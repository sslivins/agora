"""Integration tests for CMS URL change → reconnect.

Spins up real WebSocket servers and verifies the CMS client detects a
config file change and reconnects to the new URL.
"""

import asyncio
import json
from pathlib import Path

import pytest
import websockets
from websockets.asyncio.server import serve

from cms_client.service import CMSClient, CONFIG_POLL_INTERVAL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_cms_config(config_path: Path, ws_url: str) -> None:
    """Write a cms_config.json with the given ws URL."""
    config_path.write_text(json.dumps({"cms_url": ws_url}))


def _make_settings(tmp_path: Path, initial_cms_url: str = "") -> object:
    """Build a minimal Settings-like object backed by tmp_path."""
    persist = tmp_path / "persist"
    persist.mkdir(exist_ok=True)
    state = tmp_path / "state"
    state.mkdir(exist_ok=True)
    assets = tmp_path / "assets"
    for d in ["videos", "images", "splash"]:
        (assets / d).mkdir(parents=True, exist_ok=True)

    class _Settings:
        agora_base = tmp_path
        persist_dir = persist
        cms_config_path = persist / "cms_config.json"
        auth_token_path = persist / "auth_token"
        cms_status_path = state / "cms_status.json"
        manifest_path = state / "assets.json"
        schedule_path = state / "schedule.json"
        desired_state_path = state / "desired.json"
        assets_dir = assets
        videos_dir = assets / "videos"
        images_dir = assets / "images"
        splash_dir = assets / "splash"
        asset_budget_mb = 100
        device_name = "test-device"

    _Settings.cms_url = initial_cms_url
    return _Settings()


class _ServerTracker:
    """Lightweight WS server that records which devices connected."""

    def __init__(self):
        self.connections: list[str] = []
        self._registered = asyncio.Event()

    async def handler(self, ws):
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") == "register":
                self.connections.append(msg["device_id"])
                self._registered.set()
                # Reply with a sync so the client stays happy
                await ws.send(json.dumps({
                    "type": "sync",
                    "device_status": "adopted",
                    "schedules": [],
                    "assets": [],
                }))

    async def wait_for_register(self, timeout: float = 10.0):
        await asyncio.wait_for(self._registered.wait(), timeout=timeout)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCmsUrlReconnectIntegration:
    """Full integration: real WS servers, real CMSClient, config file swap."""

    @pytest.mark.asyncio
    async def test_reconnects_when_config_file_changes(self, tmp_path):
        """Client connects to server A, config changes, client reconnects to server B."""
        tracker_a = _ServerTracker()
        tracker_b = _ServerTracker()

        async with serve(tracker_a.handler, "127.0.0.1", 0) as server_a:
            port_a = server_a.sockets[0].getsockname()[1]
            url_a = f"ws://127.0.0.1:{port_a}/ws/device"

            settings = _make_settings(tmp_path, initial_cms_url=url_a)
            _write_cms_config(settings.cms_config_path, url_a)

            client = CMSClient(settings)

            # Run client in background
            client_task = asyncio.create_task(client.run())

            # Wait for initial registration on server A
            await tracker_a.wait_for_register(timeout=10)
            assert len(tracker_a.connections) >= 1

            # Now start server B and update the config file
            async with serve(tracker_b.handler, "127.0.0.1", 0) as server_b:
                port_b = server_b.sockets[0].getsockname()[1]
                url_b = f"ws://127.0.0.1:{port_b}/ws/device"

                _write_cms_config(settings.cms_config_path, url_b)

                # Wait for registration on server B
                await tracker_b.wait_for_register(
                    timeout=CONFIG_POLL_INTERVAL + 10,
                )
                assert len(tracker_b.connections) >= 1

            # Clean up
            await client.stop()
            client_task.cancel()
            try:
                await client_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_clears_auth_token_on_url_change(self, tmp_path):
        """Auth token file should be cleared when the CMS URL changes."""
        tracker_a = _ServerTracker()
        tracker_b = _ServerTracker()

        async with serve(tracker_a.handler, "127.0.0.1", 0) as server_a:
            port_a = server_a.sockets[0].getsockname()[1]
            url_a = f"ws://127.0.0.1:{port_a}/ws/device"

            settings = _make_settings(tmp_path, initial_cms_url=url_a)
            _write_cms_config(settings.cms_config_path, url_a)
            settings.auth_token_path.write_text("old-secret-token")

            client = CMSClient(settings)
            client_task = asyncio.create_task(client.run())

            await tracker_a.wait_for_register(timeout=10)

            async with serve(tracker_b.handler, "127.0.0.1", 0) as server_b:
                port_b = server_b.sockets[0].getsockname()[1]
                url_b = f"ws://127.0.0.1:{port_b}/ws/device"

                _write_cms_config(settings.cms_config_path, url_b)
                await tracker_b.wait_for_register(
                    timeout=CONFIG_POLL_INTERVAL + 10,
                )

                # Auth token should have been cleared
                token = settings.auth_token_path.read_text().strip()
                assert token == "", f"Expected empty token, got {token!r}"

            await client.stop()
            client_task.cancel()
            try:
                await client_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_no_reconnect_when_url_unchanged(self, tmp_path):
        """Client should NOT reconnect when config file is rewritten with same URL."""
        tracker = _ServerTracker()

        async with serve(tracker.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}/ws/device"

            settings = _make_settings(tmp_path, initial_cms_url=url)
            _write_cms_config(settings.cms_config_path, url)

            client = CMSClient(settings)
            client_task = asyncio.create_task(client.run())

            await tracker.wait_for_register(timeout=10)

            # Rewrite config with the SAME URL
            _write_cms_config(settings.cms_config_path, url)

            # Wait a couple poll intervals — should NOT get a second register
            await asyncio.sleep(CONFIG_POLL_INTERVAL * 2.5)
            assert len(tracker.connections) == 1, (
                f"Expected exactly 1 connection, got {len(tracker.connections)}"
            )

            await client.stop()
            client_task.cancel()
            try:
                await client_task
            except asyncio.CancelledError:
                pass
