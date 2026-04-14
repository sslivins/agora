"""Tests for device API key header on asset downloads.

Verifies that _handle_fetch_asset sends X-Device-API-Key header when
a key is available, and omits it when no key exists.
"""

import asyncio
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
from shared.state import write_state  # noqa: E402


class _AsyncIterChunks:
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
    return mock_cls, mock_session


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
    settings.persist_dir = tmp_path / "persist"
    settings.persist_dir.mkdir()
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
    client._eval_wake = asyncio.Event()
    client._last_player_mode = None
    return client


class TestDownloadAuthHeader:
    @pytest.mark.asyncio
    async def test_sends_api_key_header(self, cms_client):
        """When an API key is stored, the download request includes the header."""
        api_key = "my-secret-device-key"
        (cms_client.settings.persist_dir / "api_key").write_text(api_key)

        cms_client.asset_manager.has_asset.return_value = False
        cms_client._get_scheduled_asset_names = MagicMock(return_value=set())
        cms_client._read_schedule_cache = MagicMock(return_value=None)

        # Write desired state so re-apply logic doesn't fail
        desired = DesiredState(
            mode=PlaybackMode.STOP,
            asset="",
            loop=False,
            timestamp=datetime.now(timezone.utc),
        )
        write_state(cms_client.settings.desired_state_path, desired)

        fake_content = b"fake video data"
        mock_cls, mock_session = _mock_aiohttp_download(fake_content)

        mock_aiohttp = MagicMock()
        mock_aiohttp.ClientSession = mock_cls

        ws = AsyncMock()
        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            await cms_client._handle_fetch_asset(
                {
                    "asset_name": "test.mp4",
                    "download_url": "http://cms.local/api/assets/123/download",
                    "checksum": "",
                    "size_bytes": len(fake_content),
                },
                ws,
            )

        # Verify the header was passed
        mock_session.get.assert_called_once()
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert headers.get("X-Device-API-Key") == api_key

    @pytest.mark.asyncio
    async def test_no_header_when_no_key(self, cms_client):
        """When no API key file exists, the download should still proceed without the header."""
        # No api_key file written — persist_dir/api_key doesn't exist
        cms_client.asset_manager.has_asset.return_value = False
        cms_client._get_scheduled_asset_names = MagicMock(return_value=set())
        cms_client._read_schedule_cache = MagicMock(return_value=None)

        desired = DesiredState(
            mode=PlaybackMode.STOP,
            asset="",
            loop=False,
            timestamp=datetime.now(timezone.utc),
        )
        write_state(cms_client.settings.desired_state_path, desired)

        fake_content = b"fake image data"
        mock_cls, mock_session = _mock_aiohttp_download(fake_content)

        mock_aiohttp = MagicMock()
        mock_aiohttp.ClientSession = mock_cls

        ws = AsyncMock()
        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            await cms_client._handle_fetch_asset(
                {
                    "asset_name": "splash.png",
                    "download_url": "http://cms.local/api/assets/456/download",
                    "checksum": "",
                    "size_bytes": len(fake_content),
                },
                ws,
            )

        # Verify the request was made with empty headers (no key)
        mock_session.get.assert_called_once()
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert "X-Device-API-Key" not in headers

    def test_read_api_key_returns_key(self, cms_client):
        """_read_api_key returns the stored key."""
        (cms_client.settings.persist_dir / "api_key").write_text("test-key-123")
        assert cms_client._read_api_key() == "test-key-123"

    def test_read_api_key_returns_empty_when_missing(self, cms_client):
        """_read_api_key returns empty string when no key file exists."""
        assert cms_client._read_api_key() == ""

    def test_read_api_key_strips_whitespace(self, cms_client):
        """_read_api_key strips trailing newlines/whitespace."""
        (cms_client.settings.persist_dir / "api_key").write_text("my-key\n  ")
        assert cms_client._read_api_key() == "my-key"
