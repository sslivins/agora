"""Tests for checksum-aware proactive asset fetching.

When the CMS re-transcodes an asset, the device may have the old file under
the same name.  _check_and_fetch_missing() must detect the checksum mismatch
and request a fresh copy.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from cms_client.service import CMSClient  # noqa: E402


def _make_schedule_data(
    schedules: list[dict],
    default_asset: str | None = None,
    default_asset_checksum: str | None = None,
    timezone: str = "UTC",
) -> dict:
    return {
        "schedules": schedules,
        "default_asset": default_asset,
        "default_asset_checksum": default_asset_checksum,
        "timezone": timezone,
    }


def _active_entry(
    asset: str,
    checksum: str | None = None,
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
        "priority": 0,
    }
    if checksum is not None:
        entry["asset_checksum"] = checksum
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
    settings.asset_budget_mb = 100

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    client.device_id = "test-device"
    client.asset_manager = MagicMock()
    client._ws = AsyncMock()
    return client


class TestChecksumMismatchFetch:
    """_check_and_fetch_missing must re-request assets with wrong checksums."""

    @pytest.mark.asyncio
    async def test_matching_checksum_skips_fetch(self, cms_client):
        """Asset on disk with correct checksum — no fetch_request sent."""
        cms_client.asset_manager.has_asset.return_value = True
        data = _make_schedule_data([_active_entry("video.mp4", "correct_hash")])
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()

        cms_client.asset_manager.has_asset.assert_called_with("video.mp4", "correct_hash")
        cms_client._ws.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_mismatched_checksum_triggers_fetch(self, cms_client):
        """Asset on disk but checksum differs — must send fetch_request."""
        # has_asset returns False when checksum doesn't match
        cms_client.asset_manager.has_asset.return_value = False
        data = _make_schedule_data([_active_entry("video.mp4", "new_hash")])
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()

        cms_client.asset_manager.has_asset.assert_called_with("video.mp4", "new_hash")
        # Should have sent exactly one fetch_request
        assert cms_client._ws.send.call_count == 1
        sent = json.loads(cms_client._ws.send.call_args[0][0])
        assert sent["type"] == "fetch_request"
        assert sent["asset"] == "video.mp4"
        assert sent["device_id"] == "test-device"

    @pytest.mark.asyncio
    async def test_missing_asset_triggers_fetch(self, cms_client):
        """Asset not on disk at all — must send fetch_request."""
        cms_client.asset_manager.has_asset.return_value = False
        data = _make_schedule_data([_active_entry("video.mp4")])
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()

        # No checksum in schedule entry → called with None
        cms_client.asset_manager.has_asset.assert_called_with("video.mp4", None)
        assert cms_client._ws.send.call_count == 1

    @pytest.mark.asyncio
    async def test_default_asset_checksum_mismatch(self, cms_client):
        """Default asset with wrong checksum triggers re-fetch."""
        cms_client.asset_manager.has_asset.return_value = False
        data = _make_schedule_data(
            schedules=[],
            default_asset="splash.jpg",
            default_asset_checksum="new_splash_hash",
        )
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()

        cms_client.asset_manager.has_asset.assert_called_with("splash.jpg", "new_splash_hash")
        assert cms_client._ws.send.call_count == 1
        sent = json.loads(cms_client._ws.send.call_args[0][0])
        assert sent["asset"] == "splash.jpg"

    @pytest.mark.asyncio
    async def test_default_asset_matching_checksum_skips(self, cms_client):
        """Default asset with correct checksum — no fetch needed."""
        cms_client.asset_manager.has_asset.return_value = True
        data = _make_schedule_data(
            schedules=[],
            default_asset="splash.jpg",
            default_asset_checksum="correct_hash",
        )
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()

        cms_client._ws.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_ws_connection_skips_silently(self, cms_client):
        """If WebSocket is disconnected, don't crash."""
        cms_client._ws = None
        cms_client.asset_manager.has_asset.return_value = False
        data = _make_schedule_data([_active_entry("video.mp4", "hash")])
        cms_client.settings.schedule_path.write_text(json.dumps(data))

        await cms_client._check_and_fetch_missing()
        # Should not raise

    @pytest.mark.asyncio
    async def test_no_schedule_file_skips_silently(self, cms_client):
        """Missing schedule file — just return without error."""
        # Don't write any schedule file
        await cms_client._check_and_fetch_missing()
        cms_client._ws.send.assert_not_called()


class TestEvalRecoveryOnPlayerError:
    """_evaluate_schedule must retry when the player reports an error in current.json."""

    def test_player_error_clears_eval_cache(self, cms_client, tmp_path):
        """When current.json has an error, eval cache is cleared and desired.json rewritten."""
        from shared.models import CurrentState
        from shared.state import write_state

        state_dir = tmp_path / "state"
        state_dir.mkdir(exist_ok=True)
        cms_client.settings.current_state_path = state_dir / "current.json"
        cms_client.settings.desired_state_path = state_dir / "desired.json"
        cms_client._last_eval_state = None

        # First eval — should write desired.json and set cache
        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)
        assert cms_client._last_eval_state is not None
        first_state = cms_client._last_eval_state

        # Second eval with same data — cache hit, should skip
        cms_client._evaluate_schedule(sync_data)
        # desired.json should not change timestamp — it's the same write

        # Now simulate a player error in current.json
        error_state = CurrentState(error="Checksum mismatch: video.mp4")
        write_state(cms_client.settings.current_state_path, error_state)

        # Read desired.json before re-eval to compare timestamps
        from shared.models import DesiredState
        from shared.state import read_state
        before = read_state(cms_client.settings.desired_state_path, DesiredState)

        # Third eval — should detect error, clear cache, and rewrite desired.json
        cms_client._evaluate_schedule(sync_data)

        after = read_state(cms_client.settings.desired_state_path, DesiredState)
        # Timestamp should be different (new write)
        assert after.timestamp != before.timestamp
        assert after.asset == "video.mp4"
        assert after.expected_checksum == "abc123"

    def test_no_player_error_keeps_cache(self, cms_client, tmp_path):
        """When current.json has no error, eval cache is honored (no rewrite)."""
        from shared.models import CurrentState, DesiredState
        from shared.state import read_state, write_state

        state_dir = tmp_path / "state"
        state_dir.mkdir(exist_ok=True)
        cms_client.settings.current_state_path = state_dir / "current.json"
        cms_client.settings.desired_state_path = state_dir / "desired.json"
        cms_client._last_eval_state = None

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)
        before = read_state(cms_client.settings.desired_state_path, DesiredState)

        # Write a healthy current.json (no error)
        healthy_state = CurrentState(mode="play", asset="video.mp4")
        write_state(cms_client.settings.current_state_path, healthy_state)

        # Re-eval — cache should hold, no rewrite
        cms_client._evaluate_schedule(sync_data)
        after = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert after.timestamp == before.timestamp

    def test_missing_current_json_keeps_cache(self, cms_client, tmp_path):
        """If current.json doesn't exist yet, don't crash — keep cache."""
        state_dir = tmp_path / "state"
        state_dir.mkdir(exist_ok=True)
        cms_client.settings.current_state_path = state_dir / "current.json"
        cms_client.settings.desired_state_path = state_dir / "desired.json"
        cms_client._last_eval_state = None

        sync_data = _make_schedule_data([_active_entry("video.mp4", "abc123")])
        cms_client._evaluate_schedule(sync_data)

        from shared.models import DesiredState
        from shared.state import read_state
        before = read_state(cms_client.settings.desired_state_path, DesiredState)

        # No current.json exists — re-eval should keep cache
        cms_client._evaluate_schedule(sync_data)
        after = read_state(cms_client.settings.desired_state_path, DesiredState)
        assert after.timestamp == before.timestamp
