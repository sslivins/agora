"""Device-side HTTPS client for the CMS bootstrap endpoints (issue #420).

Wraps the three anonymous bootstrap HTTP endpoints on the CMS:

* ``POST /api/devices/register``         — fleet-HMAC gated, announce to CMS.
* ``GET  /api/devices/bootstrap-status``  — poll until the operator adopts.
* ``POST /api/devices/connect-token``     — ed25519-signed, mint fresh WPS JWT.

Stage B.2 of the bootstrap redesign.  This is a **pure library**: it has no
dependency on :mod:`api.config`, touches no disk state, and does NOT drive
the service lifecycle.  Callers (Stage B.3, ``cms_client.service``) own
timing / retry / persistence policy.

Crypto primitives (fleet HMAC, ed25519 signing, ECIES wire format) live in
:mod:`shared.bootstrap_identity`; this module only marshals arguments and
encodes them onto the wire.  In particular, this module does NOT decrypt
the bootstrap payload — the caller holds the ed25519 seed and is the only
party that can safely do that.

Design notes:

* Errors are typed.  Callers can catch :class:`FleetHmacRejectedError` vs
  :class:`PubkeyMismatchError` vs :class:`CapacityExceededError` vs the
  catch-all :class:`BootstrapServerError` and choose different policies
  (retry with fresh nonce, refuse to re-register, back off hard).
* ``timestamp`` and ``nonce`` are injectable on every signed call so tests
  can pin canonical bytes deterministically.
* No argument is ever logged at INFO or above.  Seeds, fleet secrets,
  HMACs, signatures, JWTs, and ECIES payloads never appear in logs.
"""

from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import aiohttp

from shared.bootstrap_identity import (
    compute_fleet_hmac_hex,
    sign_connect_token_request,
)

logger = logging.getLogger("agora.cms_client.bootstrap")

# ---------------------------------------------------------------------
# Default timeouts.  Callers can override by passing a pre-configured
# ``aiohttp.ClientSession`` with its own timeout, or by supplying
# ``request_timeout`` explicitly.
# ---------------------------------------------------------------------

DEFAULT_REQUEST_TIMEOUT_S = 30.0


# ---------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------


class BootstrapClientError(Exception):
    """Base class for every error raised by this module."""


class BootstrapTransportError(BootstrapClientError):
    """Network/transport-layer failure (connection reset, DNS, TLS, etc.)."""


class BootstrapServerError(BootstrapClientError):
    """CMS returned a non-success HTTP status.

    ``status`` is the HTTP status code.  ``detail`` is the server's
    ``detail`` field from the standard FastAPI error JSON, or ``None``
    if the body was missing/unparseable.  Use the typed subclasses for
    the specific failure modes the caller is expected to act on.
    """

    def __init__(self, status: int, detail: Optional[str] = None) -> None:
        self.status = int(status)
        self.detail = detail
        super().__init__(f"bootstrap server returned {self.status} ({detail!r})")


class FleetHmacRejectedError(BootstrapServerError):
    """``POST /register`` returned 401 (fleet HMAC missing / bad / stale / replay).

    Callers should treat this as a **configuration problem** (bad fleet
    secret, clock skew > 300s, or nonce reuse) and NOT retry in a tight
    loop.  The ``detail`` field distinguishes the subcases:
    ``fleet_hmac_missing``, ``fleet_hmac_bad_timestamp``,
    ``fleet_hmac_stale``, ``fleet_hmac_bad``, ``fleet_hmac_replay``.
    """


class PubkeyMismatchError(BootstrapServerError):
    """``POST /register`` returned 409 ``pubkey_mismatch``.

    Another device has already registered under the same ``device_id``
    with a different pubkey.  The caller MUST NOT overwrite its key;
    the operator has to reset state on the CMS side or the device has
    to mint a fresh ``device_id``.
    """


class CapacityExceededError(BootstrapServerError):
    """``POST /register`` returned 503 ``registration_capacity_exceeded``.

    CMS is in back-pressure; back off and retry later.
    """


class RateLimitedError(BootstrapServerError):
    """Any endpoint returned 429 ``rate_limited``.  Back off and retry."""


class PendingNotFoundError(BootstrapServerError):
    """``GET /bootstrap-status`` returned 404 ``not_found``.

    Either the device has not yet called ``/register`` for this pubkey,
    or the pending row has been reaped/reset.  The caller's policy
    decides whether to re-register or fail hard.
    """


class ConnectTokenRejectedError(BootstrapServerError):
    """``POST /connect-token`` returned 401 (signature/adoption check failed).

    Subcases surface via the ``detail`` field (``unauthorized`` — the
    CMS intentionally does NOT distinguish further so that a probe
    can't tell valid-device-wrong-signature from non-adopted-device).
    """


# ---------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class BootstrapStatus:
    """Raw result of ``GET /bootstrap-status``.

    Does NOT decrypt the payload — callers own the ed25519 seed and
    pass it into :func:`shared.bootstrap_identity.decrypt_adopt_payload`
    themselves.
    """

    status: str  # "pending" or "adopted"
    payload_b64: Optional[str]  # base64 ECIES ciphertext when adopted; else None

    @property
    def is_adopted(self) -> bool:
        return self.status == "adopted" and bool(self.payload_b64)


@dataclass(frozen=True)
class ConnectTokenResult:
    """Successful response from ``POST /connect-token``."""

    wps_jwt: str
    wps_url: str
    expires_at: str  # RFC3339 UTC string; parsing is the caller's job


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def register_once(
    session: aiohttp.ClientSession,
    api_base: str,
    *,
    device_id: str,
    pubkey_b64: str,
    pairing_secret_hash: str,
    fleet_id: str,
    fleet_secret: bytes,
    metadata: Optional[Mapping[str, Any]] = None,
    timestamp: Optional[int] = None,
    nonce: Optional[str] = None,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT_S,
) -> None:
    """POST to ``/api/devices/register``.

    On success (CMS returns 202 ``{"status":"pending"}`` unconditionally
    so probes can't distinguish valid from invalid MACs), this function
    returns ``None``.

    :param session: pre-built ``aiohttp.ClientSession`` (caller-owned).
    :param api_base: CMS root URL, e.g. ``"https://cms.example.com"``.
        Trailing slash optional.
    :param device_id: advisory device_id the CMS will pin the pending
        row to.  The CMS key is ``(pubkey, pairing_secret_hash)`` — this
        field lets the operator see a familiar ID in the adopt UI.
    :param pubkey_b64: standard-base64 of the raw 32-byte ed25519 pubkey.
    :param pairing_secret_hash: 64-char hex SHA-256 of the raw pairing
        secret the device shows in its QR code.  Use
        :func:`shared.bootstrap_identity.pairing_secret_hash_hex`.
    :param fleet_id: fleet identifier (goes in the ``X-Fleet-Id`` header
        and the HMAC canonical input).
    :param fleet_secret: raw bytes of the fleet HMAC key (NOT base64).
    :param metadata: optional advisory metadata dict (model / serial /
        firmware version etc.).  The CMS stores but does not trust it.
    :param timestamp: unix seconds.  Defaults to ``int(time.time())``.
        Injectable for deterministic tests.
    :param nonce: 32+ hex chars.  Defaults to ``secrets.token_hex(16)``.
        Injectable for deterministic tests.
    :param request_timeout: per-request timeout in seconds.

    :raises FleetHmacRejectedError: 401 (any ``fleet_hmac_*`` detail).
    :raises PubkeyMismatchError: 409 ``pubkey_mismatch``.
    :raises CapacityExceededError: 503 ``registration_capacity_exceeded``.
    :raises RateLimitedError: 429.
    :raises BootstrapServerError: any other non-2xx.
    :raises BootstrapTransportError: network-layer failure.
    """
    ts = int(timestamp) if timestamp is not None else int(time.time())
    n = nonce if nonce is not None else secrets.token_hex(16)

    mac_hex = compute_fleet_hmac_hex(
        fleet_secret,
        device_id=device_id,
        pubkey_b64=pubkey_b64,
        pairing_secret_hash=pairing_secret_hash,
        fleet_id=fleet_id,
        timestamp=ts,
        nonce=n,
    )

    url = _join(api_base, "/api/devices/register")
    headers = {
        "X-Fleet-Id": fleet_id,
        "X-Fleet-Timestamp": str(ts),
        "X-Fleet-Nonce": n,
        # CMS verifies hex despite the ``mac_b64`` name in the router.
        # The wire format is hex SHA-256.
        "X-Fleet-Mac": mac_hex,
    }
    body = {
        "device_id": device_id,
        "pubkey": pubkey_b64,
        "pairing_secret_hash": pairing_secret_hash,
        "metadata": dict(metadata or {}),
    }

    logger.debug(
        "bootstrap.register_once device_id=%s fleet_id=%s ts=%d", device_id, fleet_id, ts
    )

    try:
        async with session.post(
            url,
            json=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=request_timeout),
        ) as resp:
            if resp.status in (200, 202):
                return
            detail = await _read_detail(resp)
            _raise_register_error(resp.status, detail)
    except aiohttp.ClientError as e:
        raise BootstrapTransportError(f"register transport error: {e!r}") from e


async def get_bootstrap_status_once(
    session: aiohttp.ClientSession,
    api_base: str,
    *,
    pubkey_b64: str,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT_S,
) -> BootstrapStatus:
    """GET ``/api/devices/bootstrap-status?pubkey=...``.

    Returns a :class:`BootstrapStatus` with the raw status string and
    the base64 ciphertext (when adopted).  Does NOT decrypt — the
    caller owns the ed25519 seed.

    :raises PendingNotFoundError: 404.  Device hasn't registered yet
        for this pubkey, or the pending row was reaped.
    :raises RateLimitedError: 429.
    :raises BootstrapServerError: any other non-2xx (400 on a malformed
        pubkey, 500 on CMS bugs, etc.).
    :raises BootstrapTransportError: network-layer failure.
    """
    url = _join(api_base, "/api/devices/bootstrap-status")
    try:
        async with session.get(
            url,
            params={"pubkey": pubkey_b64},
            timeout=aiohttp.ClientTimeout(total=request_timeout),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                status = str(data.get("status") or "")
                payload = data.get("payload")
                if payload is not None and not isinstance(payload, str):
                    raise BootstrapServerError(
                        200, f"bootstrap-status payload had type {type(payload).__name__}"
                    )
                return BootstrapStatus(status=status, payload_b64=payload)
            detail = await _read_detail(resp)
            _raise_status_error(resp.status, detail)
            # _raise_status_error always raises; unreachable.
            raise BootstrapServerError(resp.status, detail)
    except aiohttp.ClientError as e:
        raise BootstrapTransportError(f"bootstrap-status transport error: {e!r}") from e


async def fetch_connect_token(
    session: aiohttp.ClientSession,
    api_base: str,
    *,
    device_id: str,
    seed: bytes,
    timestamp: Optional[int] = None,
    nonce: Optional[str] = None,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT_S,
) -> ConnectTokenResult:
    """POST to ``/api/devices/connect-token`` with an ed25519-signed request.

    The signature is over ``f"{device_id}|{timestamp}|{nonce}"`` as
    UTF-8 bytes, matching
    :func:`shared.bootstrap_identity.connect_token_canonical_bytes`.

    :param device_id: the device_id the CMS has recorded for this pubkey
        (for agora, the Raspberry Pi CPU serial the device used at
        ``/register``).  Must match ``Device.id`` on the CMS side, which
        is pinned at adoption time.
    :param seed: raw 32-byte ed25519 seed.  Sensitive — never logged.
    :param timestamp: unix seconds.  Defaults to ``int(time.time())``.
    :param nonce: 32+ hex chars.  Defaults to ``secrets.token_hex(16)``.

    :raises ConnectTokenRejectedError: 401 (bad signature, stale /
        replayed nonce, device not adopted).  The caller MUST NOT retry
        with the same nonce; a fresh one is mandatory.
    :raises RateLimitedError: 429.
    :raises BootstrapServerError: any other non-2xx (500 ``token_mint_failed``).
    :raises BootstrapTransportError: network-layer failure.
    """
    ts = int(timestamp) if timestamp is not None else int(time.time())
    n = nonce if nonce is not None else secrets.token_hex(16)

    signature_b64 = sign_connect_token_request(seed, device_id, ts, n)

    url = _join(api_base, "/api/devices/connect-token")
    body = {
        "device_id": device_id,
        "timestamp": ts,
        "nonce": n,
        "signature": signature_b64,
    }

    logger.debug("bootstrap.fetch_connect_token device_id=%s ts=%d", device_id, ts)

    try:
        async with session.post(
            url,
            json=body,
            timeout=aiohttp.ClientTimeout(total=request_timeout),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                try:
                    return ConnectTokenResult(
                        wps_jwt=str(data["wps_jwt"]),
                        wps_url=str(data["wps_url"]),
                        expires_at=str(data["expires_at"]),
                    )
                except (KeyError, TypeError) as e:
                    raise BootstrapServerError(
                        200, f"connect-token response missing field: {e!r}"
                    ) from e
            detail = await _read_detail(resp)
            _raise_connect_token_error(resp.status, detail)
            raise BootstrapServerError(resp.status, detail)
    except aiohttp.ClientError as e:
        raise BootstrapTransportError(f"connect-token transport error: {e!r}") from e


# ---------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------


def _join(base: str, path: str) -> str:
    """Join an API base and a leading-slash path, trimming a trailing slash."""
    return base.rstrip("/") + path


async def _read_detail(resp: aiohttp.ClientResponse) -> Optional[str]:
    """Parse a FastAPI error body and return its ``detail`` field, or None."""
    try:
        data = await resp.json(content_type=None)
    except (aiohttp.ContentTypeError, ValueError):
        return None
    if isinstance(data, dict):
        d = data.get("detail")
        if isinstance(d, str):
            return d
    return None


def _raise_register_error(status: int, detail: Optional[str]) -> None:
    if status == 401:
        raise FleetHmacRejectedError(status, detail)
    if status == 409:
        raise PubkeyMismatchError(status, detail)
    if status == 429:
        raise RateLimitedError(status, detail)
    if status == 503:
        raise CapacityExceededError(status, detail)
    raise BootstrapServerError(status, detail)


def _raise_status_error(status: int, detail: Optional[str]) -> None:
    if status == 404:
        raise PendingNotFoundError(status, detail)
    if status == 429:
        raise RateLimitedError(status, detail)
    raise BootstrapServerError(status, detail)


def _raise_connect_token_error(status: int, detail: Optional[str]) -> None:
    if status == 401:
        raise ConnectTokenRejectedError(status, detail)
    if status == 429:
        raise RateLimitedError(status, detail)
    raise BootstrapServerError(status, detail)


__all__ = [
    "BootstrapClientError",
    "BootstrapServerError",
    "BootstrapStatus",
    "BootstrapTransportError",
    "CapacityExceededError",
    "ConnectTokenRejectedError",
    "ConnectTokenResult",
    "FleetHmacRejectedError",
    "PendingNotFoundError",
    "PubkeyMismatchError",
    "RateLimitedError",
    "fetch_connect_token",
    "get_bootstrap_status_once",
    "register_once",
]
