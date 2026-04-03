"""Tests for CMS client connection behaviour.

Covers:
- Polling for cms_url when not yet configured (mDNS not ready)
- Auth rejection retry timing (AUTH_REJECTED_RETRY = 10s)
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import (
    AUTH_REJECTED_RETRY,
    CMSClient,
)


class TestAuthRejectedRetry:
    """AUTH_REJECTED_RETRY should be 10 seconds (fast enough for re-flash UX)."""

    def test_auth_rejected_retry_value(self):
        assert AUTH_REJECTED_RETRY == 10


class TestCmsUrlPolling:
    """When cms_url is not yet configured, run() should poll instead of exiting."""

    @pytest.mark.asyncio
    async def test_polls_until_cms_url_appears(self, tmp_path):
        """run() waits for _get_cms_url to return a URL, then proceeds."""
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
        settings.cms_status_path = tmp_path / "state" / "cms_status.json"

        with patch.object(CMSClient, "__init__", lambda self, s: None):
            client = CMSClient(settings)
        client.settings = settings
        client.device_id = "test-device"
        client.asset_manager = MagicMock()
        client._running = False

        # _get_cms_url returns "" twice, then a real URL
        call_count = 0
        def fake_get_cms_url():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return ""
            return "ws://cms.local:8080/ws/device"

        client._get_cms_url = fake_get_cms_url

        # Mock _connect_and_run to stop the loop after cms_url is found
        async def stop_after_connect():
            client._running = False

        client._connect_and_run = stop_after_connect
        client._write_cms_status = MagicMock()
        client._schedule_eval_loop = AsyncMock()
        client._fetch_loop = AsyncMock()

        # Patch asyncio.sleep to not actually wait
        with patch("cms_client.service.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await client.run()

        # Should have polled at least twice (the two empty returns)
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        poll_sleeps = [s for s in sleep_calls if s == 5]
        assert len(poll_sleeps) >= 2, f"Expected at least 2 poll sleeps of 5s, got {sleep_calls}"
        assert call_count >= 3  # 2 empty + 1 with URL

    @pytest.mark.asyncio
    async def test_immediate_url_skips_polling(self, tmp_path):
        """If cms_url is configured from the start, no polling occurs."""
        settings = MagicMock()
        settings.agora_base = tmp_path
        settings.cms_status_path = tmp_path / "cms_status.json"

        with patch.object(CMSClient, "__init__", lambda self, s: None):
            client = CMSClient(settings)
        client.settings = settings
        client.device_id = "test-device"
        client._running = False

        client._get_cms_url = lambda: "ws://cms.local:8080/ws/device"

        async def stop_after_connect():
            client._running = False

        client._connect_and_run = stop_after_connect
        client._write_cms_status = MagicMock()
        client._schedule_eval_loop = AsyncMock()
        client._fetch_loop = AsyncMock()

        with patch("cms_client.service.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await client.run()

        # No 5-second polling sleeps should have occurred
        poll_sleeps = [c for c in mock_sleep.call_args_list if c.args[0] == 5]
        assert len(poll_sleeps) == 0
