"""Tests for screen flicker prevention.

When the CMS re-sends a sync (e.g. after reconnect) that evaluates to the
same asset already playing, neither the CMS client nor the player should
trigger a pipeline rebuild — that causes a visible screen flicker.
"""

import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# Mock heavy dependencies before importing CMS client
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from shared.models import DesiredState, PlaybackMode
from shared.state import read_state, write_state


# ── CMS Client: _handle_sync should not re-write desired.json for same result ──


class TestSyncDedup:
    """_handle_sync must not write desired.json when the schedule result is unchanged."""

    def _make_client(self, tmp_path, assets=None):
        from api.config import Settings
        from cms_client.service import CMSClient

        settings = Settings(
            agora_base=tmp_path,
            api_key="test",
            web_username="admin",
            web_password="test",
            secret_key="test",
            device_name="test",
        )
        settings.ensure_dirs()
        client = CMSClient(settings)
        # Pre-register assets so has_asset() returns True
        for name, checksum in (assets or []):
            client.asset_manager.register(name, f"videos/{name}", 1024, checksum or "")
        return client

    def _sync_data(self, default_asset="splash.jpg", default_asset_checksum=None, schedules=None):
        return {
            "type": "sync",
            "timezone": "UTC",
            "schedules": schedules or [],
            "default_asset": default_asset,
            "default_asset_checksum": default_asset_checksum,
        }

    @pytest.mark.asyncio
    async def test_duplicate_sync_no_rewrite(self, tmp_path):
        """Two identical syncs should only write desired.json once."""
        client = self._make_client(tmp_path, assets=[("image.jpg", None)])
        sync = self._sync_data(default_asset="image.jpg")

        # First sync — should write desired.json
        await client._handle_sync(sync)
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        assert state1.asset == "image.jpg"
        ts1 = state1.timestamp

        # Second identical sync — should NOT write (same result)
        await client._handle_sync(sync)
        state2 = read_state(client.settings.desired_state_path, DesiredState)
        assert state2.timestamp == ts1, "desired.json was rewritten with new timestamp"

    @pytest.mark.asyncio
    async def test_changed_sync_does_rewrite(self, tmp_path):
        """A sync that changes the default asset should write desired.json."""
        client = self._make_client(tmp_path, assets=[("image_a.jpg", None), ("image_b.jpg", None)])

        await client._handle_sync(self._sync_data(default_asset="image_a.jpg"))
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        ts1 = state1.timestamp

        await client._handle_sync(self._sync_data(default_asset="image_b.jpg"))
        state2 = read_state(client.settings.desired_state_path, DesiredState)
        assert state2.asset == "image_b.jpg"
        assert state2.timestamp != ts1, "desired.json should have been rewritten"

    @pytest.mark.asyncio
    async def test_schedule_winner_unchanged_no_rewrite(self, tmp_path):
        """Sync with same schedule winner should not rewrite desired.json."""
        client = self._make_client(tmp_path, assets=[("video.mp4", None)])
        schedules = [
            {
                "id": "s1",
                "name": "Test",
                "asset": "video.mp4",
                "start_time": "00:00",
                "end_time": "23:59",
                "start_date": None,
                "end_date": None,
                "days_of_week": None,
                "priority": 10,
            }
        ]
        sync = self._sync_data(schedules=schedules)

        await client._handle_sync(sync)
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        ts1 = state1.timestamp

        await client._handle_sync(sync)
        state2 = read_state(client.settings.desired_state_path, DesiredState)
        assert state2.timestamp == ts1, "desired.json was rewritten for same schedule winner"

    @pytest.mark.asyncio
    async def test_replaced_default_asset_plays_through_then_swaps(self, tmp_path):
        """Replacing the bytes of the currently-displayed default asset must
        NOT flicker to splash. The device plays the current version through
        the background download and swaps once the new checksum lands.

        (Previously this asserted an immediate splash rewrite; play-through-
        update changed that — see tests/test_play_through_update.py.)"""
        client = self._make_client(tmp_path, assets=[("image.jpg", "aaa")])

        await client._handle_sync(
            self._sync_data(default_asset="image.jpg", default_asset_checksum="aaa")
        )
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        assert state1.mode == PlaybackMode.PLAY
        ts1 = state1.timestamp

        # Same name, new checksum (asset replaced on CMS); new bytes not on
        # disk yet. The old version is still on disk and on screen.
        await client._handle_sync(
            self._sync_data(default_asset="image.jpg", default_asset_checksum="bbb")
        )
        state2 = read_state(client.settings.desired_state_path, DesiredState)
        # No flicker — keep playing the old bytes, desired.json untouched.
        assert state2.mode == PlaybackMode.PLAY
        assert state2.asset == "image.jpg"
        assert state2.timestamp == ts1, "must not flip to splash mid-download"

        # Download completes — new checksum now on disk → swap.
        client.asset_manager.register("image.jpg", "videos/image.jpg", 1024, "bbb")
        data = json.loads(client.settings.schedule_path.read_text())
        client._evaluate_schedule(data)
        state3 = read_state(client.settings.desired_state_path, DesiredState)
        assert state3.mode == PlaybackMode.PLAY
        assert state3.expected_checksum == "bbb"
        assert state3.timestamp != ts1, "should swap once new bytes land"

    @pytest.mark.asyncio
    async def test_replaced_schedule_asset_plays_through_then_swaps(self, tmp_path):
        """Replacing the bytes of the currently-displayed scheduled asset
        must NOT flicker to splash; it plays through then swaps."""
        client = self._make_client(tmp_path, assets=[("video.mp4", "checksum_old")])
        schedules_v1 = [
            {
                "id": "s1", "name": "Test", "asset": "video.mp4",
                "asset_checksum": "checksum_old",
                "start_time": "00:00", "end_time": "23:59",
                "start_date": None, "end_date": None,
                "days_of_week": None, "priority": 10,
            }
        ]
        schedules_v2 = [
            {
                "id": "s1", "name": "Test", "asset": "video.mp4",
                "asset_checksum": "checksum_new",
                "start_time": "00:00", "end_time": "23:59",
                "start_date": None, "end_date": None,
                "days_of_week": None, "priority": 10,
            }
        ]

        await client._handle_sync(self._sync_data(schedules=schedules_v1))
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        assert state1.mode == PlaybackMode.PLAY
        ts1 = state1.timestamp

        # New checksum, bytes not on disk yet → play through, no splash.
        await client._handle_sync(self._sync_data(schedules=schedules_v2))
        state2 = read_state(client.settings.desired_state_path, DesiredState)
        assert state2.mode == PlaybackMode.PLAY
        assert state2.asset == "video.mp4"
        assert state2.timestamp == ts1, "must not flip to splash mid-download"

        # New bytes land → swap.
        client.asset_manager.register("video.mp4", "videos/video.mp4", 2048, "checksum_new")
        data = json.loads(client.settings.schedule_path.read_text())
        client._evaluate_schedule(data)
        state3 = read_state(client.settings.desired_state_path, DesiredState)
        assert state3.mode == PlaybackMode.PLAY
        assert state3.expected_checksum == "checksum_new"
        assert state3.timestamp != ts1, "should swap once new bytes land"

    @pytest.mark.asyncio
    async def test_eval_loop_no_rewrite(self, tmp_path):
        """The 15s eval loop should not rewrite desired.json for unchanged result."""
        client = self._make_client(tmp_path, assets=[("image.jpg", None)])
        sync = self._sync_data(default_asset="image.jpg")

        await client._handle_sync(sync)
        state1 = read_state(client.settings.desired_state_path, DesiredState)
        ts1 = state1.timestamp

        # Simulate what the eval loop does — read cached schedule and re-evaluate
        data = json.loads(client.settings.schedule_path.read_text())
        client._evaluate_schedule(data)

        state2 = read_state(client.settings.desired_state_path, DesiredState)
        assert state2.timestamp == ts1


# ── Player: apply_desired should skip rebuild for same content ──


class TestPlayerSkipRebuild:
    """Player should not tear down the pipeline when mode+asset+loop are unchanged."""

    def _make_player(self, tmp_path):
        """Create a player with mocked GStreamer."""
        # Mock gi and GStreamer before importing player.service
        mock_gi = MagicMock()
        mock_gst = MagicMock()
        mock_gst.State.NULL = "NULL"
        mock_gst.State.PLAYING = "PLAYING"
        mock_gst.init = MagicMock()
        mock_glib = MagicMock()

        sys.modules["gi"] = mock_gi
        sys.modules["gi.repository"] = MagicMock(Gst=mock_gst, GLib=mock_glib)

        mock_gi.require_version = MagicMock()

        # Force re-import of player.service with mocked gi
        if "player.service" in sys.modules:
            del sys.modules["player.service"]

        import player.service as ps

        # These tests validate the mpv/GStreamer pipeline rebuild logic
        # specifically, so force the legacy mpv backend regardless of
        # the global default (chromium is now the default backend).
        with patch.dict(os.environ, {"AGORA_PLAYER_BACKEND": "mpv"}):
            player = ps.AgoraPlayer(base_path=str(tmp_path))
        player.state_dir.mkdir(parents=True, exist_ok=True)
        # Create asset directories and a test image
        (tmp_path / "assets" / "images").mkdir(parents=True, exist_ok=True)
        (tmp_path / "assets" / "images" / "test.jpg").write_bytes(b"fake")
        # Create default splash so _find_splash() can find it
        (tmp_path / "assets" / "splash").mkdir(parents=True, exist_ok=True)
        (tmp_path / "assets" / "splash" / "default.png").write_bytes(b"fake-png")
        return player, mock_gst

    def test_same_content_skips_rebuild(self, tmp_path):
        """Writing desired.json with same mode+asset+loop but new timestamp
        should NOT trigger a pipeline teardown/rebuild."""
        player, mock_gst = self._make_player(tmp_path)

        # Simulate an active pipeline playing test.jpg
        asset_path = tmp_path / "assets" / "images" / "test.jpg"
        player.pipeline = MagicMock()
        player._current_path = asset_path
        player._current_mtime = asset_path.stat().st_mtime
        prior = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True
        )
        player.current_desired = prior
        # Prove the renderer applied this exact desired previously so
        # the equivalence short-circuit can fire.
        player._applied_desired = prior.model_copy(deep=True)

        # Write desired.json with same content but new timestamp
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True
        )
        write_state(player.desired_path, new_desired)

        # apply_desired should skip rebuild
        player.apply_desired()

        # Pipeline should NOT have been torn down
        player.pipeline.set_state.assert_not_called()
        # But the stored timestamp should be updated
        assert player.current_desired.timestamp == new_desired.timestamp

    def test_different_asset_does_rebuild(self, tmp_path):
        """Changing the asset should trigger a pipeline rebuild."""
        player, mock_gst = self._make_player(tmp_path)

        mock_pipeline = MagicMock()
        mock_gst.parse_launch.return_value = mock_pipeline
        mock_pipeline.get_bus.return_value = MagicMock()
        mock_pipeline.get_state.return_value = (None, MagicMock(value_nick="playing"), None)

        player.pipeline = MagicMock()
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="old.jpg", loop=True,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        # Create the new asset
        (tmp_path / "assets" / "images" / "new.jpg").write_bytes(b"\x00" * 16)

        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="new.jpg", loop=True
        )
        write_state(player.desired_path, new_desired)

        player.apply_desired()

        # Old pipeline should have been torn down (set to NULL)
        player.pipeline.set_state.assert_called()

    def test_no_pipeline_does_build(self, tmp_path):
        """If there's no active pipeline, a rebuild should happen even for same content."""
        player, mock_gst = self._make_player(tmp_path)

        mock_pipeline = MagicMock()
        mock_gst.parse_launch.return_value = mock_pipeline
        mock_pipeline.get_bus.return_value = MagicMock()
        mock_pipeline.get_state.return_value = (None, MagicMock(value_nick="playing"), None)

        player.pipeline = None  # No active pipeline
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
        )
        write_state(player.desired_path, new_desired)

        # Ensure the file has enough content for the readability check
        (tmp_path / "assets" / "images" / "test.jpg").write_bytes(b"\x00" * 16)

        player.apply_desired()

        # Should build a new pipeline
        mock_gst.parse_launch.assert_called_once()

    def test_mode_change_does_rebuild(self, tmp_path):
        """Changing from play to splash should trigger rebuild."""
        player, mock_gst = self._make_player(tmp_path)

        old_pipeline = MagicMock()
        player.pipeline = old_pipeline
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        new_desired = DesiredState(mode=PlaybackMode.SPLASH)
        write_state(player.desired_path, new_desired)

        player.apply_desired()

        # Old pipeline should have been torn down (set to NULL via _teardown)
        old_pipeline.set_state.assert_called()

    def test_file_replaced_on_disk_triggers_rebuild(self, tmp_path):
        """Same filename but new content (different mtime) should rebuild."""
        player, mock_gst = self._make_player(tmp_path)

        mock_pipeline = MagicMock()
        mock_gst.parse_launch.return_value = mock_pipeline
        mock_pipeline.get_bus.return_value = MagicMock()
        mock_pipeline.get_state.return_value = (
            None, MagicMock(value_nick="playing"), None,
        )

        asset_path = tmp_path / "assets" / "images" / "test.jpg"
        old_mtime = asset_path.stat().st_mtime

        # Simulate pipeline playing the old version
        player.pipeline = MagicMock()
        player._current_path = asset_path
        player._current_mtime = old_mtime
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
        )

        # CMS downloads new content — same filename, new bytes
        import time
        time.sleep(0.05)  # ensure mtime differs
        asset_path.write_bytes(b"new content")

        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
        )
        write_state(player.desired_path, new_desired)

        player.apply_desired()

        # Should rebuild because mtime changed
        mock_gst.parse_launch.assert_called_once()

    def test_splash_to_play_same_file_does_rebuild(self, tmp_path):
        """SPLASH→PLAY for the same image MUST rebuild — the prior
        renderer state was a splash, not the requested PLAY, so we
        cannot claim the new desired is already satisfied just because
        the file path happens to match.  This is the regression guard
        for the ``apply_desired`` postcondition rewrite.
        """
        player, mock_gst = self._make_player(tmp_path)

        asset_path = tmp_path / "assets" / "images" / "test.jpg"
        # Make the file pass the >=8-byte header sanity check so we
        # actually exercise the rebuild path, not the splash fallback.
        asset_path.write_bytes(b"\x00" * 16)

        # Simulate splash playing test.jpg
        player.pipeline = MagicMock()
        player._current_path = asset_path
        player._current_mtime = asset_path.stat().st_mtime
        prior_splash = DesiredState(
            mode=PlaybackMode.SPLASH,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        player.current_desired = prior_splash
        # Splash branch sets _applied_desired on success — replicate
        # that so the new contract sees an honest prior state.
        player._applied_desired = prior_splash.model_copy(deep=True)

        # CMS pushes PLAY for same file
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.jpg", loop=True,
        )
        write_state(player.desired_path, new_desired)

        player.apply_desired()

        # MUST rebuild — different mode means renderer must commit to
        # PLAY (loop counter, watchdog, etc.).  The path-equality
        # short-circuit that previously skipped this is the root cause
        # of the slideshow-stuck bug.
        assert mock_gst.parse_launch.call_count >= 1
        assert player.current_desired.mode == PlaybackMode.PLAY
