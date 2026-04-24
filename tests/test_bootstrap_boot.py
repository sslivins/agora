"""Unit tests for :mod:`cms_client.bootstrap_boot`.

Stage B.3 of the bootstrap redesign (issue #420).  The orchestration
layer is exercised offline by monkeypatching the three
``bootstrap_client`` HTTP primitives (``register_once``,
``get_bootstrap_status_once``, ``fetch_connect_token``) so no real
network / ``aiohttp`` is involved.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# ``shared.bootstrap_identity.load_or_create_device_identity`` uses
# ``os.O_NOFOLLOW`` which doesn't exist on Windows.  CI runs on Linux;
# on Windows we skip the identity-dependent classes below but still
# exercise the pure state helpers.
_POSIX = sys.platform != "win32"
posix_only = pytest.mark.skipif(
    not _POSIX,
    reason="bootstrap identity primitives are POSIX-only (fd-based invariants)",
)

from cms_client import bootstrap_boot
from cms_client.bootstrap_client import (
    BootstrapTransportError,
    ConnectTokenRejectedError,
    ConnectTokenResult,
    PendingNotFoundError,
    BootstrapStatus,
)
from shared.bootstrap_identity import (
    load_or_create_device_identity,
    load_or_create_pairing_secret,
)


# --------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------


@pytest.fixture
def tmp_persist(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def identity(tmp_persist: Path):
    return load_or_create_device_identity(tmp_persist / "device_key")


@pytest.fixture
def pairing_secret(tmp_persist: Path) -> str:
    return load_or_create_pairing_secret(tmp_persist / "pairing_secret")


def _rfc3339(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fresh_token(minutes: int = 60) -> ConnectTokenResult:
    return ConnectTokenResult(
        wps_url="wss://wps.example.com/agent",
        wps_jwt="jwt-abc",
        expires_at=_rfc3339(datetime.now(timezone.utc) + timedelta(minutes=minutes)),
    )


# --------------------------------------------------------------------
# State helpers
# --------------------------------------------------------------------


class TestState:
    def test_save_load_roundtrip(self, tmp_path):
        p = tmp_path / "s.json"
        data = {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://cms.example.com",
            "device_id": "pi-serial-xyz",
            "wps_url": "wss://wps/x",
            "wps_jwt": "jwt",
            "expires_at": _rfc3339(datetime.now(timezone.utc) + timedelta(hours=1)),
        }
        bootstrap_boot.save_state(p, data)
        assert bootstrap_boot.load_state(p) == data

    def test_save_state_is_atomic_no_partial_file(self, tmp_path, monkeypatch):
        """If the rename is interrupted, the on-disk file stays intact."""
        p = tmp_path / "s.json"
        bootstrap_boot.save_state(p, {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://a", "device_id": "d",
            "wps_url": "w", "wps_jwt": "j",
            "expires_at": _rfc3339(datetime.now(timezone.utc) + timedelta(hours=1)),
        })
        good = p.read_text()

        real_replace = os.replace
        def boom(*a, **kw):
            raise OSError("disk full")
        monkeypatch.setattr(bootstrap_boot.os, "replace", boom)
        with pytest.raises(OSError):
            bootstrap_boot.save_state(p, {
                "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
                "cms_api_base": "https://b", "device_id": "d2",
                "wps_url": "w2", "wps_jwt": "j2", "expires_at": "x",
            })
        # Original file unchanged.
        assert p.read_text() == good
        # No stray temp files.
        leftover = [x for x in tmp_path.iterdir()
                    if x.name.startswith(".bootstrap_state.")]
        assert leftover == [], leftover

    def test_load_ignores_unknown_schema(self, tmp_path):
        p = tmp_path / "s.json"
        p.write_text(json.dumps({"schema_version": 99, "anything": 1}))
        assert bootstrap_boot.load_state(p) is None

    def test_load_ignores_corrupt_json(self, tmp_path):
        p = tmp_path / "s.json"
        p.write_text("not json at all{")
        assert bootstrap_boot.load_state(p) is None

    def test_clear_state_missing_is_ok(self, tmp_path):
        bootstrap_boot.clear_state(tmp_path / "missing.json")  # no exception

    def test_clear_state_deletes_existing(self, tmp_path):
        p = tmp_path / "s.json"
        p.write_text("x")
        bootstrap_boot.clear_state(p)
        assert not p.exists()


class TestParseExpiresAt:
    def test_z_suffix(self):
        dt = bootstrap_boot._parse_expires_at("2026-01-01T00:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_offset(self):
        dt = bootstrap_boot._parse_expires_at("2026-01-01T00:00:00+00:00")
        assert dt is not None

    def test_naive_treated_as_utc(self):
        dt = bootstrap_boot._parse_expires_at("2026-01-01T00:00:00")
        assert dt is not None
        assert dt.tzinfo is timezone.utc

    def test_empty(self):
        assert bootstrap_boot._parse_expires_at("") is None

    def test_garbage(self):
        assert bootstrap_boot._parse_expires_at("nope") is None


class TestIsStateFresh:
    def _state(self, *, base="https://cms.example.com", expires):
        return {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": base, "device_id": "d",
            "wps_url": "w", "wps_jwt": "j",
            "expires_at": expires,
        }

    def test_fresh_returns_true(self):
        exp = _rfc3339(datetime.now(timezone.utc) + timedelta(hours=1))
        assert bootstrap_boot._is_state_fresh(
            self._state(expires=exp),
            "https://cms.example.com",
            time.time(),
        ) is True

    def test_different_cms_base_is_stale(self):
        exp = _rfc3339(datetime.now(timezone.utc) + timedelta(hours=1))
        assert bootstrap_boot._is_state_fresh(
            self._state(base="https://old.example.com", expires=exp),
            "https://cms.example.com",
            time.time(),
        ) is False

    def test_within_early_refresh_window_is_stale(self):
        # Expires in JWT_EARLY_REFRESH_SEC - 1 seconds → stale.
        exp = _rfc3339(
            datetime.now(timezone.utc)
            + timedelta(seconds=bootstrap_boot.JWT_EARLY_REFRESH_SEC - 1)
        )
        assert bootstrap_boot._is_state_fresh(
            self._state(expires=exp),
            "https://cms.example.com",
            time.time(),
        ) is False

    def test_garbage_expires_is_stale(self):
        assert bootstrap_boot._is_state_fresh(
            self._state(expires="nope"),
            "https://cms.example.com",
            time.time(),
        ) is False


# --------------------------------------------------------------------
# ensure_wps_credentials — three branches
# --------------------------------------------------------------------


class TestEnsureWpsCredentialsFresh:
    """Branch 1: cached state is fresh — no HTTP calls."""

    pytestmark = posix_only

    @pytest.mark.asyncio
    async def test_returns_cached(self, tmp_path, identity, pairing_secret, monkeypatch):
        state_path = tmp_path / "bootstrap_state.json"
        exp = _rfc3339(datetime.now(timezone.utc) + timedelta(hours=2))
        bootstrap_boot.save_state(state_path, {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://cms.example.com",
            "device_id": "pi-1234",
            "wps_url": "wss://wps/x",
            "wps_jwt": "cached-jwt",
            "expires_at": exp,
        })

        # Any HTTP primitive being called would be a bug on the fast path.
        called = {"n": 0}
        async def boom(*a, **kw):
            called["n"] += 1
            raise AssertionError("HTTP primitive invoked on fresh-cache path")
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", boom)
        monkeypatch.setattr(bootstrap_boot, "register_once", boom)
        monkeypatch.setattr(bootstrap_boot, "get_bootstrap_status_once", boom)

        creds = await bootstrap_boot.ensure_wps_credentials(
            session=object(),
            cms_api_base="https://cms.example.com",
            device_id="pi-1234",
            identity=identity,
            pairing_secret=pairing_secret,
            state_path=state_path,
            fleet_id="",  # empty OK — we're on the fast path
            fleet_secret=b"",
            metadata=None,
        )
        assert creds.wps_jwt == "cached-jwt"
        assert creds.device_id == "pi-1234"
        assert called["n"] == 0


class TestEnsureWpsCredentialsStaleRefresh:
    """Branch 2: cached state matches CMS but JWT stale — signed refresh."""

    pytestmark = posix_only

    @pytest.mark.asyncio
    async def test_signed_refresh_persists_new_state(
        self, tmp_path, identity, pairing_secret, monkeypatch,
    ):
        state_path = tmp_path / "bootstrap_state.json"
        stale_exp = _rfc3339(datetime.now(timezone.utc) + timedelta(seconds=10))
        bootstrap_boot.save_state(state_path, {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://cms.example.com",
            "device_id": "pi-1234",
            "wps_url": "wss://old", "wps_jwt": "old",
            "expires_at": stale_exp,
        })

        new_token = _fresh_token(minutes=55)
        captured = {}
        async def fake_fetch(session, base, *, device_id, seed):
            captured["base"] = base
            captured["device_id"] = device_id
            return new_token
        async def no_register(*a, **kw):
            raise AssertionError("register_once should not be called")
        async def no_status(*a, **kw):
            raise AssertionError("get_bootstrap_status_once should not be called")
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", fake_fetch)
        monkeypatch.setattr(bootstrap_boot, "register_once", no_register)
        monkeypatch.setattr(bootstrap_boot, "get_bootstrap_status_once", no_status)

        creds = await bootstrap_boot.ensure_wps_credentials(
            session=object(),
            cms_api_base="https://cms.example.com",
            device_id="pi-DIFFERENT",  # should be overridden by cached device_id
            identity=identity,
            pairing_secret=pairing_secret,
            state_path=state_path,
            fleet_id="",
            fleet_secret=b"",
        )
        assert creds.wps_jwt == new_token.wps_jwt
        assert captured["device_id"] == "pi-1234"  # cached wins
        assert captured["base"] == "https://cms.example.com"

        # State persisted with new token.
        reloaded = bootstrap_boot.load_state(state_path)
        assert reloaded is not None
        assert reloaded["wps_jwt"] == new_token.wps_jwt
        assert reloaded["device_id"] == "pi-1234"


class TestEnsureWpsCredentialsFirstBoot:
    """Branch 3: no cached state — register, poll, decrypt, connect."""

    pytestmark = posix_only

    @pytest.mark.asyncio
    async def test_requires_fleet_credentials(
        self, tmp_path, identity, pairing_secret,
    ):
        with pytest.raises(bootstrap_boot.BootstrapConfigError):
            await bootstrap_boot.ensure_wps_credentials(
                session=object(),
                cms_api_base="https://cms.example.com",
                device_id="pi-1234",
                identity=identity,
                pairing_secret=pairing_secret,
                state_path=tmp_path / "bootstrap_state.json",
                fleet_id="",
                fleet_secret=b"",  # missing → error
            )

    @pytest.mark.asyncio
    async def test_first_boot_happy_path(
        self, tmp_path, identity, pairing_secret, monkeypatch,
    ):
        # We don't have a CMS-side encrypt helper handy, so bypass the
        # decrypt primitive directly.
        monkeypatch.setattr(
            bootstrap_boot, "decrypt_adopt_payload",
            lambda seed, payload_b64: json.dumps({"device_id": "pi-9999"}).encode("utf-8"),
        )

        reg_calls = {"n": 0}
        async def fake_register(session, base, **kw):
            reg_calls["n"] += 1
        monkeypatch.setattr(bootstrap_boot, "register_once", fake_register)

        # Two status polls: first pending, then adopted.
        status_calls = {"n": 0}
        async def fake_status(session, base, *, pubkey_b64):
            status_calls["n"] += 1
            if status_calls["n"] == 1:
                return BootstrapStatus(status="pending", payload_b64=None)
            return BootstrapStatus(status="adopted", payload_b64="ignored-by-mock")
        monkeypatch.setattr(
            bootstrap_boot, "get_bootstrap_status_once", fake_status,
        )

        new_token = _fresh_token(60)
        fetch_calls = {"device_id": None}
        async def fake_fetch(session, base, *, device_id, seed):
            fetch_calls["device_id"] = device_id
            return new_token
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", fake_fetch)

        # Short-circuit the poll sleep.
        async def no_sleep(*a, **kw):
            return None
        monkeypatch.setattr(bootstrap_boot.asyncio, "sleep", no_sleep)

        state_path = tmp_path / "bootstrap_state.json"
        creds = await bootstrap_boot.ensure_wps_credentials(
            session=object(),
            cms_api_base="https://cms.example.com",
            device_id="pi-initial",
            identity=identity,
            pairing_secret=pairing_secret,
            state_path=state_path,
            fleet_id="fleet-A",
            fleet_secret=b"\x00" * 32,
            metadata={"firmware_version": "1.0.0"},
        )
        assert reg_calls["n"] == 1
        assert status_calls["n"] == 3  # pending + adopted in poll loop, then fetch-payload
        assert fetch_calls["device_id"] == "pi-9999"  # from decrypted outbox
        assert creds.device_id == "pi-9999"
        assert creds.wps_jwt == new_token.wps_jwt

        reloaded = bootstrap_boot.load_state(state_path)
        assert reloaded["device_id"] == "pi-9999"
        assert reloaded["cms_api_base"] == "https://cms.example.com"

    @pytest.mark.asyncio
    async def test_first_boot_404_reregisters(
        self, tmp_path, identity, pairing_secret, monkeypatch,
    ):
        """If /bootstrap-status returns 404 (row reaped), we re-register."""
        monkeypatch.setattr(
            bootstrap_boot, "decrypt_adopt_payload",
            lambda seed, payload_b64: b'{"device_id": "pi-xyz"}',
        )

        reg_calls = {"n": 0}
        async def fake_register(session, base, **kw):
            reg_calls["n"] += 1
        monkeypatch.setattr(bootstrap_boot, "register_once", fake_register)

        call_seq = {"n": 0}
        async def fake_status(session, base, *, pubkey_b64):
            call_seq["n"] += 1
            if call_seq["n"] == 1:
                raise PendingNotFoundError(404, "reaped")
            # After re-register, next poll shows adopted.
            return BootstrapStatus(status="adopted", payload_b64="x")
        monkeypatch.setattr(
            bootstrap_boot, "get_bootstrap_status_once", fake_status,
        )

        async def fake_fetch(session, base, *, device_id, seed):
            return _fresh_token(60)
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", fake_fetch)

        async def no_sleep(*a, **kw):
            return None
        monkeypatch.setattr(bootstrap_boot.asyncio, "sleep", no_sleep)

        await bootstrap_boot.ensure_wps_credentials(
            session=object(),
            cms_api_base="https://cms.example.com",
            device_id="pi-abc",
            identity=identity,
            pairing_secret=pairing_secret,
            state_path=tmp_path / "s.json",
            fleet_id="fleet-A",
            fleet_secret=b"\x00" * 32,
        )
        # Initial + re-register after 404.
        assert reg_calls["n"] == 2

    @pytest.mark.asyncio
    async def test_first_boot_cancel_event(
        self, tmp_path, identity, pairing_secret, monkeypatch,
    ):
        async def fake_register(session, base, **kw):
            return None
        monkeypatch.setattr(bootstrap_boot, "register_once", fake_register)

        async def always_pending(session, base, *, pubkey_b64):
            return BootstrapStatus(status="pending", payload_b64=None)
        monkeypatch.setattr(
            bootstrap_boot, "get_bootstrap_status_once", always_pending,
        )

        cancel = asyncio.Event()
        cancel.set()  # cancel before first poll

        with pytest.raises(bootstrap_boot.BootstrapCancelledError):
            await bootstrap_boot.ensure_wps_credentials(
                session=object(),
                cms_api_base="https://cms.example.com",
                device_id="pi-abc",
                identity=identity,
                pairing_secret=pairing_secret,
                state_path=tmp_path / "s.json",
                fleet_id="fleet-A",
                fleet_secret=b"\x00" * 32,
                poll_cancel_event=cancel,
            )


# --------------------------------------------------------------------
# refresh_wps_jwt
# --------------------------------------------------------------------


class TestRefreshWpsJwt:
    pytestmark = posix_only

    @pytest.mark.asyncio
    async def test_refresh_uses_cached_device_id(
        self, tmp_path, identity, monkeypatch,
    ):
        state_path = tmp_path / "s.json"
        bootstrap_boot.save_state(state_path, {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://cms.example.com",
            "device_id": "pi-cached",
            "wps_url": "w", "wps_jwt": "old-jwt",
            "expires_at": _rfc3339(datetime.now(timezone.utc) + timedelta(seconds=5)),
        })

        captured = {}
        async def fake_fetch(session, base, *, device_id, seed):
            captured["device_id"] = device_id
            return _fresh_token(60)
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", fake_fetch)

        creds = await bootstrap_boot.refresh_wps_jwt(
            session=object(),
            cms_api_base="https://cms.example.com",
            identity=identity,
            state_path=state_path,
        )
        assert captured["device_id"] == "pi-cached"
        assert creds.device_id == "pi-cached"
        reloaded = bootstrap_boot.load_state(state_path)
        assert reloaded["wps_jwt"] == creds.wps_jwt

    @pytest.mark.asyncio
    async def test_refresh_401_bubbles_up(
        self, tmp_path, identity, monkeypatch,
    ):
        state_path = tmp_path / "s.json"
        bootstrap_boot.save_state(state_path, {
            "schema_version": bootstrap_boot.STATE_SCHEMA_VERSION,
            "cms_api_base": "https://cms.example.com",
            "device_id": "pi-cached",
            "wps_url": "w", "wps_jwt": "j",
            "expires_at": _rfc3339(datetime.now(timezone.utc) + timedelta(seconds=5)),
        })
        async def fake_fetch(session, base, *, device_id, seed):
            raise ConnectTokenRejectedError(401, "nope")
        monkeypatch.setattr(bootstrap_boot, "fetch_connect_token", fake_fetch)

        with pytest.raises(ConnectTokenRejectedError):
            await bootstrap_boot.refresh_wps_jwt(
                session=object(),
                cms_api_base="https://cms.example.com",
                identity=identity,
                state_path=state_path,
            )
        # State file NOT cleared — policy decision lives in the caller.
        assert state_path.exists()

    @pytest.mark.asyncio
    async def test_refresh_no_state_no_device_id_raises(
        self, tmp_path, identity,
    ):
        with pytest.raises(LookupError):
            await bootstrap_boot.refresh_wps_jwt(
                session=object(),
                cms_api_base="https://cms.example.com",
                identity=identity,
                state_path=tmp_path / "missing.json",
            )


# --------------------------------------------------------------------
# ensure_identity
# --------------------------------------------------------------------


class TestEnsureIdentity:
    pytestmark = posix_only

    def test_generates_keys_on_first_call(self, tmp_path):
        ident, secret = bootstrap_boot.ensure_identity(
            device_key_path=tmp_path / "device_key",
            pairing_secret_path=tmp_path / "pairing_secret",
        )
        assert (tmp_path / "device_key").exists()
        assert (tmp_path / "pairing_secret").exists()
        assert ident.pubkey_b64
        assert secret

    def test_second_call_is_stable(self, tmp_path):
        ident1, sec1 = bootstrap_boot.ensure_identity(
            device_key_path=tmp_path / "device_key",
            pairing_secret_path=tmp_path / "pairing_secret",
        )
        ident2, sec2 = bootstrap_boot.ensure_identity(
            device_key_path=tmp_path / "device_key",
            pairing_secret_path=tmp_path / "pairing_secret",
        )
        assert ident1.pubkey_b64 == ident2.pubkey_b64
        assert sec1 == sec2
