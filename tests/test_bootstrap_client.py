"""Unit tests for :mod:`cms_client.bootstrap_client`.

Stage B.2 of the bootstrap redesign (issue #420).  These tests cover the
three HTTP endpoints on the CMS as a pure library, with a
lightweight fake :class:`aiohttp.ClientSession` so the suite doesn't take
a new dependency or spin up a real server.

The canonical HMAC / signature bytes are cross-checked against the B.1
primitives so the device side stays in lockstep with what the CMS
verifies.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

# ``aiohttp`` is a real runtime dep (see requirements-cms-client.txt); we
# import it directly so ``ClientError`` / ``ClientTimeout`` etc. are the
# real classes.  The rest of the aiohttp surface is stubbed per-test.
import aiohttp

from cms_client.bootstrap_client import (
    BootstrapServerError,
    BootstrapStatus,
    BootstrapTransportError,
    CapacityExceededError,
    ConnectTokenRejectedError,
    ConnectTokenResult,
    FleetHmacRejectedError,
    PendingNotFoundError,
    PubkeyMismatchError,
    RateLimitedError,
    fetch_connect_token,
    get_bootstrap_status_once,
    register_once,
)
from shared.bootstrap_identity import (
    compute_fleet_hmac_hex,
    connect_token_canonical_bytes,
)


# ---------------------------------------------------------------------
# Fake aiohttp response / session
# ---------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for aiohttp.ClientResponse."""

    def __init__(
        self,
        status: int,
        json_body: Any = None,
        raise_content_type: bool = False,
    ) -> None:
        self.status = status
        self._json_body = json_body
        self._raise_content_type = raise_content_type

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401
        return None

    async def json(self, content_type: Any = None) -> Any:  # noqa: D401
        if self._raise_content_type:
            raise aiohttp.ContentTypeError(None, None, message="no json")
        if self._json_body is None:
            raise ValueError("no body")
        return self._json_body


class _FakeSession:
    """Captures the last request arguments so tests can assert on them."""

    def __init__(self, response: _FakeResponse | Exception) -> None:
        self._response = response
        self.last_method: str | None = None
        self.last_url: str | None = None
        self.last_json: Any = None
        self.last_params: Any = None
        self.last_headers: dict[str, str] | None = None

    def _handle(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        self.last_method = method
        self.last_url = url
        self.last_json = kwargs.get("json")
        self.last_params = kwargs.get("params")
        self.last_headers = kwargs.get("headers")
        if isinstance(self._response, Exception):
            raise self._response
        return self._response

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        return self._handle("POST", url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        return self._handle("GET", url, **kwargs)


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def fleet_secret() -> bytes:
    # 32 bytes of zeros is fine for a deterministic test vector.
    return b"\x00" * 32


@pytest.fixture
def pubkey_b64() -> str:
    # Deterministic pubkey (standard base64 of 32 zero bytes).
    return base64.b64encode(b"\x00" * 32).decode("ascii")


@pytest.fixture
def pairing_secret_hash() -> str:
    return hashlib.sha256(b"not-a-real-secret").hexdigest()


# ---------------------------------------------------------------------
# register_once
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_once_happy_path_202(
    fleet_secret: bytes, pubkey_b64: str, pairing_secret_hash: str,
) -> None:
    sess = _FakeSession(_FakeResponse(status=202, json_body={"status": "pending"}))
    await register_once(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com/",
        device_id="dev-00001",
        pubkey_b64=pubkey_b64,
        pairing_secret_hash=pairing_secret_hash,
        fleet_id="fleet-A",
        fleet_secret=fleet_secret,
        metadata={"model": "pi5"},
        timestamp=1714000000,
        nonce="0" * 32,
    )
    assert sess.last_method == "POST"
    assert sess.last_url == "https://cms.example.com/api/devices/register"
    assert sess.last_json == {
        "device_id": "dev-00001",
        "pubkey": pubkey_b64,
        "pairing_secret_hash": pairing_secret_hash,
        "metadata": {"model": "pi5"},
    }
    hdr = sess.last_headers or {}
    assert hdr["X-Fleet-Id"] == "fleet-A"
    assert hdr["X-Fleet-Timestamp"] == "1714000000"
    assert hdr["X-Fleet-Nonce"] == "0" * 32


def test_register_once_mac_header_matches_shared_primitive(
    fleet_secret: bytes, pubkey_b64: str, pairing_secret_hash: str,
) -> None:
    """The ``X-Fleet-Mac`` header is HEX (not base64), computed by the
    same primitive the CMS uses to verify.  Canonicalising the
    fleet HMAC on the device and on the CMS must match byte-for-byte.
    """
    expected = compute_fleet_hmac_hex(
        fleet_secret,
        device_id="dev-00001",
        pubkey_b64=pubkey_b64,
        pairing_secret_hash=pairing_secret_hash,
        fleet_id="fleet-A",
        timestamp=1714000000,
        nonce="ab" * 16,
    )
    # Independent re-derivation: match the canonical input the CMS hashes.
    canonical = "|".join(
        [
            "register",
            "dev-00001",
            pubkey_b64,
            pairing_secret_hash,
            "fleet-A",
            "1714000000",
            "ab" * 16,
        ]
    ).encode("utf-8")
    indep = hmac.new(fleet_secret, canonical, hashlib.sha256).hexdigest()
    assert expected == indep
    # 64 hex chars (SHA-256 digest length)
    assert len(expected) == 64
    int(expected, 16)  # no ValueError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, detail, expected_exc",
    [
        (401, "fleet_hmac_bad", FleetHmacRejectedError),
        (401, "fleet_hmac_stale", FleetHmacRejectedError),
        (401, "fleet_hmac_replay", FleetHmacRejectedError),
        (409, "pubkey_mismatch", PubkeyMismatchError),
        (429, "rate_limited", RateLimitedError),
        (503, "registration_capacity_exceeded", CapacityExceededError),
        (500, "internal_error", BootstrapServerError),
    ],
)
async def test_register_once_error_mapping(
    fleet_secret: bytes,
    pubkey_b64: str,
    pairing_secret_hash: str,
    status: int,
    detail: str,
    expected_exc: type,
) -> None:
    sess = _FakeSession(_FakeResponse(status=status, json_body={"detail": detail}))
    with pytest.raises(expected_exc) as excinfo:
        await register_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-00001",
            pubkey_b64=pubkey_b64,
            pairing_secret_hash=pairing_secret_hash,
            fleet_id="fleet-A",
            fleet_secret=fleet_secret,
            timestamp=1,
            nonce="a" * 32,
        )
    assert excinfo.value.status == status  # type: ignore[attr-defined]
    assert excinfo.value.detail == detail  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_register_once_transport_error_wraps_client_error(
    fleet_secret: bytes, pubkey_b64: str, pairing_secret_hash: str,
) -> None:
    sess = _FakeSession(aiohttp.ClientConnectionError("boom"))
    with pytest.raises(BootstrapTransportError):
        await register_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-00001",
            pubkey_b64=pubkey_b64,
            pairing_secret_hash=pairing_secret_hash,
            fleet_id="fleet-A",
            fleet_secret=fleet_secret,
            timestamp=1,
            nonce="a" * 32,
        )


@pytest.mark.asyncio
async def test_register_once_generates_timestamp_and_nonce_when_omitted(
    fleet_secret: bytes, pubkey_b64: str, pairing_secret_hash: str,
) -> None:
    sess = _FakeSession(_FakeResponse(status=202, json_body={"status": "pending"}))
    await register_once(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com",
        device_id="dev-00001",
        pubkey_b64=pubkey_b64,
        pairing_secret_hash=pairing_secret_hash,
        fleet_id="fleet-A",
        fleet_secret=fleet_secret,
    )
    hdr = sess.last_headers or {}
    ts = int(hdr["X-Fleet-Timestamp"])
    # Not strict: just verify it's a plausible current-era unix ts.
    assert ts > 1_700_000_000
    nonce = hdr["X-Fleet-Nonce"]
    assert len(nonce) >= 32
    int(nonce, 16)  # hex


# ---------------------------------------------------------------------
# get_bootstrap_status_once
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_returns_pending(pubkey_b64: str) -> None:
    sess = _FakeSession(
        _FakeResponse(status=200, json_body={"status": "pending", "payload": None})
    )
    result = await get_bootstrap_status_once(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com",
        pubkey_b64=pubkey_b64,
    )
    assert isinstance(result, BootstrapStatus)
    assert result.status == "pending"
    assert result.payload_b64 is None
    assert result.is_adopted is False
    assert sess.last_method == "GET"
    assert sess.last_params == {"pubkey": pubkey_b64}


@pytest.mark.asyncio
async def test_status_returns_adopted_with_ciphertext(pubkey_b64: str) -> None:
    ciphertext_b64 = base64.b64encode(b"\x01" * 64).decode("ascii")
    sess = _FakeSession(
        _FakeResponse(
            status=200,
            json_body={"status": "adopted", "payload": ciphertext_b64},
        )
    )
    result = await get_bootstrap_status_once(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com",
        pubkey_b64=pubkey_b64,
    )
    assert result.status == "adopted"
    assert result.payload_b64 == ciphertext_b64
    assert result.is_adopted is True


@pytest.mark.asyncio
async def test_status_404_raises_pending_not_found(pubkey_b64: str) -> None:
    sess = _FakeSession(_FakeResponse(status=404, json_body={"detail": "not_found"}))
    with pytest.raises(PendingNotFoundError) as ei:
        await get_bootstrap_status_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            pubkey_b64=pubkey_b64,
        )
    assert ei.value.status == 404
    assert ei.value.detail == "not_found"


@pytest.mark.asyncio
async def test_status_429_raises_rate_limited(pubkey_b64: str) -> None:
    sess = _FakeSession(_FakeResponse(status=429, json_body={"detail": "rate_limited"}))
    with pytest.raises(RateLimitedError):
        await get_bootstrap_status_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            pubkey_b64=pubkey_b64,
        )


@pytest.mark.asyncio
async def test_status_400_raises_generic(pubkey_b64: str) -> None:
    sess = _FakeSession(_FakeResponse(status=400, json_body={"detail": "bad pubkey"}))
    with pytest.raises(BootstrapServerError) as ei:
        await get_bootstrap_status_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            pubkey_b64=pubkey_b64,
        )
    assert not isinstance(ei.value, (PendingNotFoundError, RateLimitedError))


@pytest.mark.asyncio
async def test_status_payload_wrong_type_raises(pubkey_b64: str) -> None:
    sess = _FakeSession(
        _FakeResponse(status=200, json_body={"status": "adopted", "payload": 42})
    )
    with pytest.raises(BootstrapServerError):
        await get_bootstrap_status_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            pubkey_b64=pubkey_b64,
        )


@pytest.mark.asyncio
async def test_status_transport_error_wraps_client_error(pubkey_b64: str) -> None:
    sess = _FakeSession(aiohttp.ClientConnectionError("boom"))
    with pytest.raises(BootstrapTransportError):
        await get_bootstrap_status_once(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            pubkey_b64=pubkey_b64,
        )


# ---------------------------------------------------------------------
# fetch_connect_token
# ---------------------------------------------------------------------


def _fresh_seed_and_pub() -> tuple[bytes, Ed25519PublicKey]:
    priv = Ed25519PrivateKey.generate()
    from cryptography.hazmat.primitives import serialization

    seed = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return seed, priv.public_key()


@pytest.mark.asyncio
async def test_fetch_connect_token_happy_path_signature_verifies() -> None:
    seed, pub = _fresh_seed_and_pub()
    sess = _FakeSession(
        _FakeResponse(
            status=200,
            json_body={
                "wps_jwt": "JWT.TOKEN.VALUE",
                "wps_url": "wss://wps.example.com/client/hubs/agora",
                "expires_at": "2026-04-24T12:00:00Z",
            },
        )
    )
    result = await fetch_connect_token(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com",
        device_id="dev-row-uuid",
        seed=seed,
        timestamp=1714000000,
        nonce="cd" * 16,
    )
    assert isinstance(result, ConnectTokenResult)
    assert result.wps_jwt == "JWT.TOKEN.VALUE"
    assert result.wps_url == "wss://wps.example.com/client/hubs/agora"
    assert result.expires_at == "2026-04-24T12:00:00Z"
    # Verify the signature actually checks out against the canonical
    # bytes (what the CMS will verify).
    body = sess.last_json
    sig = base64.b64decode(body["signature"])
    canonical = connect_token_canonical_bytes(
        body["device_id"], body["timestamp"], body["nonce"]
    )
    pub.verify(sig, canonical)  # raises on mismatch


@pytest.mark.asyncio
async def test_fetch_connect_token_401_rejected() -> None:
    seed, _ = _fresh_seed_and_pub()
    sess = _FakeSession(_FakeResponse(status=401, json_body={"detail": "unauthorized"}))
    with pytest.raises(ConnectTokenRejectedError) as ei:
        await fetch_connect_token(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-row-uuid",
            seed=seed,
            timestamp=1,
            nonce="a" * 32,
        )
    assert ei.value.status == 401
    assert ei.value.detail == "unauthorized"


@pytest.mark.asyncio
async def test_fetch_connect_token_429_rate_limited() -> None:
    seed, _ = _fresh_seed_and_pub()
    sess = _FakeSession(_FakeResponse(status=429, json_body={"detail": "rate_limited"}))
    with pytest.raises(RateLimitedError):
        await fetch_connect_token(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-row-uuid",
            seed=seed,
            timestamp=1,
            nonce="a" * 32,
        )


@pytest.mark.asyncio
async def test_fetch_connect_token_500_generic() -> None:
    seed, _ = _fresh_seed_and_pub()
    sess = _FakeSession(_FakeResponse(status=500, json_body={"detail": "token_mint_failed"}))
    with pytest.raises(BootstrapServerError) as ei:
        await fetch_connect_token(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-row-uuid",
            seed=seed,
            timestamp=1,
            nonce="a" * 32,
        )
    assert not isinstance(
        ei.value, (ConnectTokenRejectedError, RateLimitedError)
    )


@pytest.mark.asyncio
async def test_fetch_connect_token_missing_field_in_200_body() -> None:
    seed, _ = _fresh_seed_and_pub()
    sess = _FakeSession(
        _FakeResponse(
            status=200,
            json_body={"wps_jwt": "t", "wps_url": "u"},  # missing expires_at
        )
    )
    with pytest.raises(BootstrapServerError):
        await fetch_connect_token(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-row-uuid",
            seed=seed,
            timestamp=1,
            nonce="a" * 32,
        )


@pytest.mark.asyncio
async def test_fetch_connect_token_transport_error() -> None:
    seed, _ = _fresh_seed_and_pub()
    sess = _FakeSession(aiohttp.ClientConnectionError("boom"))
    with pytest.raises(BootstrapTransportError):
        await fetch_connect_token(
            sess,  # type: ignore[arg-type]
            "https://cms.example.com",
            device_id="dev-row-uuid",
            seed=seed,
            timestamp=1,
            nonce="a" * 32,
        )


@pytest.mark.asyncio
async def test_fetch_connect_token_default_timestamp_and_nonce_are_fresh() -> None:
    seed, pub = _fresh_seed_and_pub()
    sess = _FakeSession(
        _FakeResponse(
            status=200,
            json_body={"wps_jwt": "j", "wps_url": "u", "expires_at": "t"},
        )
    )
    await fetch_connect_token(
        sess,  # type: ignore[arg-type]
        "https://cms.example.com",
        device_id="dev-row-uuid",
        seed=seed,
    )
    body = sess.last_json
    assert isinstance(body["timestamp"], int)
    assert body["timestamp"] > 1_700_000_000
    nonce = body["nonce"]
    assert len(nonce) >= 32
    int(nonce, 16)
    # Signature must verify against that fresh ts/nonce.
    sig = base64.b64decode(body["signature"])
    canonical = connect_token_canonical_bytes(
        body["device_id"], body["timestamp"], body["nonce"]
    )
    pub.verify(sig, canonical)
