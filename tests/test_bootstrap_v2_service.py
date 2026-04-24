"""Service-level tests for the bootstrap v2 wire-up in CMSClient.

These tests verify the `_connect_and_run` path selected when
``settings.bootstrap_v2`` is true and ``cms_transport == "wps"``:

* ``_mint_wps_credentials_v2`` is invoked (instead of the legacy
  api-key path) and its returned (url, jwt) tuple is threaded to
  ``open_transport`` as ``pre_minted_url`` / ``pre_minted_token``.
* The ``http_session`` returned alongside the creds is closed in
  the ``finally`` block, even if ``open_transport`` raises.
* When the flag is off, the legacy api-key path is used unchanged.

The tests stub out the websocket handshake + inner message loop
so only the bootstrap-selection branch executes.
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy deps before importing the service module.
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient  # noqa: E402
from cms_client.transport import TransportError  # noqa: E402


def _make_client(tmp_path, *, bootstrap_v2: bool, transport: str = "wps"):
    """Construct a minimally-initialized CMSClient for wire-up tests."""
    settings = MagicMock()
    settings.agora_base = tmp_path
    settings.assets_dir = tmp_path / "assets"
    settings.assets_dir.mkdir()
    settings.videos_dir = tmp_path / "assets" / "videos"
    settings.videos_dir.mkdir()
    settings.images_dir = tmp_path / "assets" / "images"
    settings.images_dir.mkdir()
    settings.splash_dir = tmp_path / "assets" / "splash"
    settings.splash_dir.mkdir()
    settings.manifest_path = tmp_path / "state" / "assets.json"
    settings.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    settings.schedule_path = tmp_path / "state" / "schedule.json"
    settings.desired_state_path = tmp_path / "state" / "desired.json"
    settings.asset_budget_mb = 100
    settings.cms_status_path = tmp_path / "state" / "cms_status.json"
    settings.cms_transport = transport
    settings.bootstrap_v2 = bootstrap_v2
    settings.cms_api_url = "https://cms.example.com"
    settings.device_name = "test-device"
    settings.fleet_id = "fleet-01"
    settings.fleet_secret_hex = "00" * 32
    settings.jwt_refresh_lead_seconds = 60
    settings.auth_token_path = tmp_path / "state" / "auth_token"
    settings.device_key_path = tmp_path / "state" / "device_key"
    settings.pairing_secret_path = tmp_path / "state" / "pairing_secret"
    settings.bootstrap_state_path = tmp_path / "state" / "bootstrap_state.json"

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    client.device_id = "pi-01"
    client._ws = None
    client._bootstrap_identity = None
    client._bootstrap_pairing_secret = None
    client._jwt_refresh_401_count = 0
    client._bootstrap_poll_cancel = None
    client._get_cms_url = lambda: "wss://cms.example.com/ws/device"
    client._active_cms_url = None
    client._write_cms_status = MagicMock()
    return client


class TestBootstrapV2Enabled:
    def test_flag_off_by_default(self, tmp_path):
        c = _make_client(tmp_path, bootstrap_v2=False)
        assert c._bootstrap_v2_enabled() is False

    def test_flag_on(self, tmp_path):
        c = _make_client(tmp_path, bootstrap_v2=True)
        assert c._bootstrap_v2_enabled() is True


class TestMintWpsCredentialsV2:
    @pytest.mark.asyncio
    async def test_returns_creds_session_and_api_base(self, tmp_path):
        """Happy path: ensure_identity + ensure_wps_credentials are called,
        session is returned alive for the renewal task."""
        c = _make_client(tmp_path, bootstrap_v2=True)

        fake_identity = MagicMock()
        fake_secret = b"pair-secret-bytes"
        fake_creds = MagicMock()
        fake_creds.wps_url = "wss://wps.example.com/client"
        fake_creds.wps_jwt = "jwt-abc"
        fake_creds.expires_at = "2026-01-01T00:00:00Z"

        import aiohttp as aiohttp_mod  # the MagicMock stub from sys.modules

        with patch("cms_client.bootstrap_boot.ensure_identity") as ensure_identity, \
             patch("cms_client.bootstrap_boot.ensure_wps_credentials", new=AsyncMock(return_value=fake_creds)) as ensure_wps:
            ensure_identity.return_value = (fake_identity, fake_secret)

            fake_session = MagicMock()
            fake_session.close = AsyncMock()
            aiohttp_mod.ClientSession = MagicMock(return_value=fake_session)

            creds, session, api_base = await c._mint_wps_credentials_v2(
                "wss://cms.example.com/ws/device"
            )

        assert creds is fake_creds
        assert session is fake_session
        assert api_base == "https://cms.example.com"
        fake_session.close.assert_not_called()
        ensure_identity.assert_called_once()
        ensure_wps.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bad_fleet_secret_hex_raises_transport_error(self, tmp_path):
        c = _make_client(tmp_path, bootstrap_v2=True)
        c.settings.fleet_secret_hex = "not-hex!!"

        with patch("cms_client.bootstrap_boot.ensure_identity") as ensure_identity:
            ensure_identity.return_value = (MagicMock(), b"")

            with pytest.raises(TransportError, match="not valid hex"):
                await c._mint_wps_credentials_v2(
                    "wss://cms.example.com/ws/device"
                )

    @pytest.mark.asyncio
    async def test_mint_failure_closes_session(self, tmp_path):
        """If ensure_wps_credentials raises, the http_session must be closed
        so we don't leak an aiohttp ClientSession on every failed reconnect."""
        c = _make_client(tmp_path, bootstrap_v2=True)

        import aiohttp as aiohttp_mod

        with patch("cms_client.bootstrap_boot.ensure_identity") as ensure_identity, \
             patch(
                 "cms_client.bootstrap_boot.ensure_wps_credentials",
                 new=AsyncMock(side_effect=RuntimeError("mint exploded")),
             ):
            ensure_identity.return_value = (MagicMock(), b"secret")
            fake_session = MagicMock()
            fake_session.close = AsyncMock()
            aiohttp_mod.ClientSession = MagicMock(return_value=fake_session)

            with pytest.raises(RuntimeError, match="mint exploded"):
                await c._mint_wps_credentials_v2(
                    "wss://cms.example.com/ws/device"
                )

        fake_session.close.assert_awaited_once()


class TestConnectAndRunRouting:
    """Verify _connect_and_run selects the bootstrap v2 vs legacy path."""

    @pytest.mark.asyncio
    async def test_v2_path_passes_pre_minted_to_open_transport(self, tmp_path):
        c = _make_client(tmp_path, bootstrap_v2=True, transport="wps")

        fake_creds = MagicMock()
        fake_creds.wps_url = "wss://wps.example.com/client"
        fake_creds.wps_jwt = "jwt-abc"
        fake_creds.expires_at = "2026-01-01T00:00:00Z"
        fake_session = MagicMock()
        fake_session.close = AsyncMock()

        c._mint_wps_credentials_v2 = AsyncMock(
            return_value=(fake_creds, fake_session, "https://cms.example.com")
        )

        # Abort _connect_and_run after open_transport is invoked — we only
        # care about HOW it was called.
        sentinel = RuntimeError("stop-here")
        open_transport_mock = AsyncMock(side_effect=sentinel)

        with patch("cms_client.service.open_transport", open_transport_mock):
            with pytest.raises(RuntimeError, match="stop-here"):
                await c._connect_and_run()

        open_transport_mock.assert_awaited_once()
        kwargs = open_transport_mock.await_args.kwargs
        assert kwargs["mode"] == "wps"
        assert kwargs["pre_minted_url"] == "wss://wps.example.com/client"
        assert kwargs["pre_minted_token"] == "jwt-abc"
        assert kwargs["api_key"] == ""
        # Session must be closed in the finally when open_transport fails.
        fake_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_legacy_path_unchanged_when_flag_off(self, tmp_path):
        c = _make_client(tmp_path, bootstrap_v2=False, transport="wps")
        c._mint_wps_credentials_v2 = AsyncMock(
            side_effect=AssertionError("must not mint v2 when flag is off")
        )

        sentinel = RuntimeError("stop-here")
        open_transport_mock = AsyncMock(side_effect=sentinel)

        with patch("cms_client.service.open_transport", open_transport_mock), \
             patch(
                 "cms_client.service._resolve_device_api_key",
                 return_value="legacy_api_key_abc",
             ):
            with pytest.raises(RuntimeError, match="stop-here"):
                await c._connect_and_run()

        c._mint_wps_credentials_v2.assert_not_awaited()
        kwargs = open_transport_mock.await_args.kwargs
        assert kwargs["api_key"] == "legacy_api_key_abc"
        assert kwargs["pre_minted_url"] == ""
        assert kwargs["pre_minted_token"] == ""

    @pytest.mark.asyncio
    async def test_v2_flag_ignored_when_transport_direct(self, tmp_path):
        """bootstrap_v2=True + cms_transport=direct → still direct mode,
        no minting happens (flag only activates for WPS)."""
        c = _make_client(tmp_path, bootstrap_v2=True, transport="direct")
        c._mint_wps_credentials_v2 = AsyncMock(
            side_effect=AssertionError("must not mint v2 in direct mode")
        )

        sentinel = RuntimeError("stop-here")
        open_transport_mock = AsyncMock(side_effect=sentinel)

        with patch("cms_client.service.open_transport", open_transport_mock):
            with pytest.raises(RuntimeError, match="stop-here"):
                await c._connect_and_run()

        c._mint_wps_credentials_v2.assert_not_awaited()
        kwargs = open_transport_mock.await_args.kwargs
        assert kwargs["mode"] == "direct"
        assert kwargs["pre_minted_url"] == ""
        assert kwargs["pre_minted_token"] == ""

    @pytest.mark.asyncio
    async def test_v2_creates_fresh_poll_cancel_event(self, tmp_path):
        """A fresh asyncio.Event is created per _connect_and_run, and it's
        threaded through to ensure_wps_credentials so first-boot polling
        can be interrupted by CMS URL change / shutdown."""
        c = _make_client(tmp_path, bootstrap_v2=True, transport="wps")
        # Pre-existing stale event from a previous connect — must be replaced.
        stale = asyncio.Event()
        stale.set()
        c._bootstrap_poll_cancel = stale

        captured = {}

        async def fake_mint(cms_url):
            captured["cancel_event"] = c._bootstrap_poll_cancel
            # Propagate to what ensure_wps_credentials would see.
            raise RuntimeError("stop-here")

        c._mint_wps_credentials_v2 = fake_mint

        with patch("cms_client.service.open_transport"):
            with pytest.raises(Exception):
                await c._connect_and_run()

        ev = captured["cancel_event"]
        assert ev is not stale, "must create a fresh event, not reuse stale one"
        assert not ev.is_set(), "fresh event must start unset"
