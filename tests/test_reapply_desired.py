"""Tests for re-applying desired state after asset download.

When the CMS client downloads an asset that the player is waiting for,
it must re-write desired.json with a **new timestamp** so the player
detects the change (the player skips processing if the timestamp is identical).
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient  # noqa: E402
from shared.models import DesiredState, PlaybackMode  # noqa: E402
from shared.state import read_state, write_state  # noqa: E402


@pytest.fixture
def cms_client(tmp_path):
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
    settings.manifest_path.parent.mkdir(parents=True)
    settings.schedule_path = tmp_path / "state" / "schedule.json"
    settings.desired_state_path = tmp_path / "state" / "desired.json"
    settings.asset_budget_mb = 100

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    client.device_id = "test-device"
    client.asset_manager = MagicMock()
    client._ws = AsyncMock()
    client._current_schedule_id = None
    client._current_schedule_name = None
    client._current_asset = None
    return client


class _AsyncIterChunks:
    """Helper to make an async iterator from a list of byte chunks."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._chunks:
            return self._chunks.pop(0)
        raise StopAsyncIteration


def _mock_aiohttp_download(content: bytes):
    """Return a context-manager mock that simulates aiohttp downloading *content*."""
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.content.iter_chunked.return_value = _AsyncIterChunks([content])

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_cls


class TestReapplyDesiredTimestamp:
    """After downloading an asset the player is waiting for,
    desired.json must be re-written with a new timestamp."""

    @pytest.mark.asyncio
    async def test_reapply_updates_timestamp(self, cms_client):
        """Re-applying desired state must change the timestamp so the player
        picks up the change instead of skipping it as a duplicate."""
        original_ts = datetime(2026, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
        desired = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="splash.png",
            loop=True,
            timestamp=original_ts,
        )
        write_state(cms_client.settings.desired_state_path, desired)

        fake_content = b"fake image data"
        cms_client.asset_manager.has_asset.return_value = False
        cms_client.asset_manager.ensure_budget.return_value = True
        cms_client._get_scheduled_asset_names = MagicMock(return_value=set())
        cms_client._read_schedule_cache = MagicMock(return_value=None)

        ws = AsyncMock()

        # aiohttp is imported locally inside _handle_fetch_asset;
        # inject our mock into sys.modules so the import picks it up.
        mock_aiohttp = MagicMock()
        mock_aiohttp.ClientSession = _mock_aiohttp_download(fake_content)
        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            await cms_client._handle_fetch_asset(
                {
                    "asset_name": "splash.png",
                    "download_url": "http://example.com/splash.png",
                    "checksum": "",
                    "size_bytes": len(fake_content),
                },
                ws,
            )

        # Read back desired.json and verify timestamp was updated
        updated = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert updated.asset == "splash.png"
        assert updated.timestamp != original_ts, (
            "Timestamp must be updated so the player detects the change"
        )
        assert updated.timestamp > original_ts

    @pytest.mark.asyncio
    async def test_no_reapply_for_different_asset(self, cms_client):
        """If the desired asset differs from the downloaded one,
        desired.json should NOT be touched."""
        original_ts = datetime(2026, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
        desired = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="other_video.mp4",
            loop=True,
            timestamp=original_ts,
        )
        write_state(cms_client.settings.desired_state_path, desired)

        fake_content = b"fake image data"
        cms_client.asset_manager.has_asset.return_value = False
        cms_client.asset_manager.ensure_budget.return_value = True
        cms_client._get_scheduled_asset_names = MagicMock(return_value=set())
        cms_client._read_schedule_cache = MagicMock(return_value=None)

        ws = AsyncMock()

        mock_aiohttp = MagicMock()
        mock_aiohttp.ClientSession = _mock_aiohttp_download(fake_content)
        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            await cms_client._handle_fetch_asset(
                {
                    "asset_name": "splash.png",
                    "download_url": "http://example.com/splash.png",
                    "checksum": "",
                    "size_bytes": len(fake_content),
                },
                ws,
            )

        # Timestamp should remain the same — different asset
        updated = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert updated.timestamp == original_ts
