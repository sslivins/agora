"""Tests for the wipe_assets command handler.

Covers:
- Asset files and manifest are removed on wipe_assets
- Schedule state is cleared
- Wipe_assets_ack is sent back to CMS
- Asset manager is reinitialised after wipe
"""

import asyncio
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient


def _make_settings(tmp_path: Path) -> MagicMock:
    """Build a mock Settings pointing at temp directories."""
    settings = MagicMock()
    settings.agora_base = tmp_path
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    videos_dir = assets_dir / "videos"
    videos_dir.mkdir()
    images_dir = assets_dir / "images"
    images_dir.mkdir()
    splash_dir = assets_dir / "splash"
    splash_dir.mkdir()
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    persist_dir = tmp_path / "persist"
    persist_dir.mkdir()

    settings.assets_dir = assets_dir
    settings.videos_dir = videos_dir
    settings.images_dir = images_dir
    settings.splash_dir = splash_dir
    settings.state_dir = state_dir
    settings.persist_dir = persist_dir
    settings.manifest_path = state_dir / "assets.json"
    settings.schedule_path = state_dir / "schedule.json"
    settings.current_state_path = state_dir / "current.json"
    settings.desired_state_path = state_dir / "desired.json"
    settings.splash_config_path = state_dir / "splash.txt"
    settings.cms_config_path = persist_dir / "cms_config.json"
    settings.auth_token_path = persist_dir / "cms_auth_token"
    settings.storage_budget_mb = 500
    settings.asset_budget_mb = 500
    return settings


def _build_client(settings) -> CMSClient:
    """Instantiate CMSClient with mocked dependencies."""
    with patch("cms_client.service._get_device_id", return_value="test-pi"):
        client = CMSClient(settings)
    return client


class TestWipeAssetsHandler:
    @pytest.mark.asyncio
    async def test_wipe_removes_asset_files(self, tmp_path):
        """Video and image files should be deleted on wipe."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        # Create dummy asset files
        (settings.videos_dir / "promo.mp4").write_bytes(b"video-data")
        (settings.images_dir / "logo.png").write_bytes(b"image-data")

        ws = AsyncMock()
        await client._handle_wipe_assets({"reason": "adopted"}, ws)

        assert not list(settings.videos_dir.iterdir())
        assert not list(settings.images_dir.iterdir())

    @pytest.mark.asyncio
    async def test_wipe_removes_manifest_and_schedule(self, tmp_path):
        """assets.json and schedule.json should be deleted on wipe."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        settings.manifest_path.write_text("{}")
        settings.schedule_path.write_text("{}")

        # Stub rebuild_from_disk to avoid scanning empty dirs
        client.asset_manager.rebuild_from_disk = MagicMock()

        ws = AsyncMock()
        await client._handle_wipe_assets({"reason": "deleted"}, ws)

        assert not settings.manifest_path.exists()
        assert not settings.schedule_path.exists()

    @pytest.mark.asyncio
    async def test_wipe_sends_ack(self, tmp_path):
        """Device should send wipe_assets_ack back to CMS."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        ws = MagicMock()
        sent = []

        async def capture_send(data):
            sent.append(json.loads(data))

        ws.send = capture_send

        await client._handle_wipe_assets({"reason": "adopted"}, ws)

        assert len(sent) == 1
        assert sent[0]["type"] == "wipe_assets_ack"
        assert sent[0]["reason"] == "adopted"
        assert sent[0]["device_id"] == "test-pi"

    @pytest.mark.asyncio
    async def test_wipe_reinitialises_asset_manager(self, tmp_path):
        """After wipe, asset_manager.rebuild_from_disk should be called."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        client.asset_manager.rebuild_from_disk = MagicMock()

        ws = AsyncMock()
        await client._handle_wipe_assets({"reason": "adopted"}, ws)

        client.asset_manager.rebuild_from_disk.assert_called_once_with(
            settings.videos_dir, settings.images_dir, settings.splash_dir,
        )

    @pytest.mark.asyncio
    async def test_wipe_preserves_provisioning(self, tmp_path):
        """Wipe should NOT remove provisioning/auth files (unlike factory_reset)."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        auth_file = settings.auth_token_path
        auth_file.write_text("my-token")
        cms_config = settings.cms_config_path
        cms_config.write_text('{"cms_url": "http://cms:8080"}')

        ws = AsyncMock()
        await client._handle_wipe_assets({"reason": "deleted"}, ws)

        assert auth_file.exists(), "Auth token should be preserved"
        assert cms_config.exists(), "CMS config should be preserved"

    @pytest.mark.asyncio
    async def test_wipe_handles_empty_dirs(self, tmp_path):
        """Wipe should succeed even if no assets exist."""
        settings = _make_settings(tmp_path)
        client = _build_client(settings)

        ws = AsyncMock()
        await client._handle_wipe_assets({"reason": "adopted"}, ws)

        # Should not raise — just completes cleanly
        assert not list(settings.videos_dir.iterdir())
