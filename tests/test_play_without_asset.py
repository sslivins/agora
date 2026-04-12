"""Tests for the play-without-asset bug.

When a schedule is active NOW but the asset is not yet on the device,
_evaluate_schedule must NOT write a PLAY desired state. Instead it should
request the asset and wait for it to arrive before playing.

Bug: creating a schedule where the start time has already passed (but end
time is in the future) causes immediate play attempt. If the asset isn't
on the device, the player fails and the device never retries fetching.
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from cms_client.service import CMSClient  # noqa: E402


def _make_schedule_data(
    schedules: list[dict],
    default_asset: str | None = None,
    default_asset_checksum: str | None = None,
    tz: str = "UTC",
) -> dict:
    return {
        "schedules": schedules,
        "default_asset": default_asset,
        "default_asset_checksum": default_asset_checksum,
        "timezone": tz,
    }


def _active_entry(
    asset: str,
    checksum: str | None = None,
    priority: int = 0,
    loop_count: int | None = None,
) -> dict:
    """Build a schedule entry that is active right now."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = (now - timedelta(minutes=30)).strftime("%H:%M")
    end = (now + timedelta(minutes=30)).strftime("%H:%M")
    entry = {
        "id": "sched-1",
        "name": "Test",
        "asset": asset,
        "start_time": start,
        "end_time": end,
        "priority": priority,
    }
    if checksum is not None:
        entry["asset_checksum"] = checksum
    if loop_count is not None:
        entry["loop_count"] = loop_count
    return entry


@pytest.fixture
def cms_client(tmp_path):
    """Create a CMSClient with tmp dirs and a mocked asset_manager."""
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
    settings.current_state_path = tmp_path / "state" / "current.json"
    settings.asset_budget_mb = 100

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    client.device_id = "test-device"
    client.asset_manager = MagicMock()
    client._ws = AsyncMock()
    client._last_eval_state = None
    client._current_schedule_id = None
    client._current_schedule_name = None
    client._current_asset = None
    client._eval_wake = asyncio.Event()
    client._last_player_mode = None
    return client


class TestPlayWithoutAsset:
    """_evaluate_schedule must not play an asset that isn't on the device."""

    def test_missing_asset_shows_splash_instead_of_play(self, cms_client):
        """If the scheduled asset is not on disk, desired state should NOT be PLAY."""
        from shared.models import DesiredState
        from shared.state import read_state

        # Asset is NOT on device
        cms_client.asset_manager.has_asset.return_value = False

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)

        desired = read_state(cms_client.settings.desired_state_path, DesiredState)
        # Should NOT be playing the missing asset
        assert desired.mode != "play" or desired.asset != "video.mp4", \
            "Should not attempt to play an asset that isn't on the device"

    def test_present_asset_plays_normally(self, cms_client):
        """If the scheduled asset IS on disk, play as usual."""
        from shared.models import DesiredState
        from shared.state import read_state

        # Asset IS on device
        cms_client.asset_manager.has_asset.return_value = True

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)

        desired = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert desired.mode == "play"
        assert desired.asset == "video.mp4"

    def test_missing_asset_requests_fetch(self, cms_client):
        """If the asset is missing, _evaluate_schedule should request it via WebSocket."""
        cms_client.asset_manager.has_asset.return_value = False

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])

        # _request_asset_fetch uses fire-and-forget (create_task), so we
        # need a running event loop to flush the scheduled coroutine.
        async def eval_and_flush():
            cms_client._evaluate_schedule(sync_data)
            await asyncio.sleep(0)

        asyncio.run(eval_and_flush())

        # Should have sent a fetch_request
        assert cms_client._ws.send.call_count >= 1
        sent = json.loads(cms_client._ws.send.call_args[0][0])
        assert sent["type"] == "fetch_request"
        assert sent["asset"] == "video.mp4"

    def test_missing_asset_does_not_cache_play_state(self, cms_client):
        """If the asset is missing, eval state should NOT be cached as 'play'."""
        cms_client.asset_manager.has_asset.return_value = False

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)

        # Eval state must not be a "play" tuple — must not think we're playing
        assert cms_client._last_eval_state is None or cms_client._last_eval_state[0] != "play"

    def test_missing_default_asset_shows_splash(self, cms_client):
        """If the default asset is not on disk, show splash instead."""
        from shared.models import DesiredState
        from shared.state import read_state

        cms_client.asset_manager.has_asset.return_value = False

        sync_data = _make_schedule_data(
            schedules=[],
            default_asset="default.jpg",
            default_asset_checksum="def456",
        )
        cms_client._evaluate_schedule(sync_data)

        desired = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert desired.mode != "play" or desired.asset != "default.jpg", \
            "Should not attempt to play a default asset that isn't on the device"

    def test_asset_arrives_then_plays(self, cms_client):
        """After the asset arrives on device, next eval should play it."""
        from shared.models import DesiredState
        from shared.state import read_state

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])

        # First eval: asset missing
        cms_client.asset_manager.has_asset.return_value = False
        cms_client._evaluate_schedule(sync_data)

        # Asset arrives (fetch completes)
        cms_client.asset_manager.has_asset.return_value = True
        cms_client._evaluate_schedule(sync_data)

        desired = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert desired.mode == "play"
        assert desired.asset == "video.mp4"

    def test_missing_asset_no_ws_does_not_crash(self, cms_client):
        """If asset is missing and WebSocket is None, don't crash."""
        cms_client.asset_manager.has_asset.return_value = False
        cms_client._ws = None

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)
        # Should not raise
