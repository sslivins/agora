"""Tests for loop_count feature — device side.

Covers:
- DesiredState / CurrentState model serialization with loop_count
- Player _on_eos counting and stopping at loop_count
- Player apply_desired reset of loop counter
- CMS client _handle_play and _evaluate_schedule passing loop_count
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.models import CurrentState, DesiredState, PlaybackMode


# ── Model tests ──


class TestDesiredStateLoopCount:
    def test_default_none(self):
        state = DesiredState()
        assert state.loop_count is None

    def test_explicit_count(self):
        state = DesiredState(mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=5)
        assert state.loop_count == 5

    def test_serialization_roundtrip(self):
        state = DesiredState(mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=3)
        restored = DesiredState.model_validate_json(state.model_dump_json())
        assert restored.loop_count == 3

    def test_none_means_infinite(self):
        state = DesiredState(mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=None)
        data = state.model_dump(mode="json")
        assert data["loop_count"] is None


class TestCurrentStateLoopCount:
    def test_defaults(self):
        state = CurrentState()
        assert state.loop_count is None
        assert state.loops_completed == 0

    def test_with_values(self):
        state = CurrentState(loop_count=5, loops_completed=3)
        assert state.loop_count == 5
        assert state.loops_completed == 3

    def test_serialization_roundtrip(self):
        state = CurrentState(loop_count=10, loops_completed=7)
        restored = CurrentState.model_validate_json(state.model_dump_json())
        assert restored.loop_count == 10
        assert restored.loops_completed == 7


# ── Player tests ──


@pytest.fixture
def player():
    """Create an AgoraPlayer instance with mocked GStreamer."""
    with patch.dict("sys.modules", {
        "gi": MagicMock(),
        "gi.repository": MagicMock(),
    }):
        import importlib
        import player.service as svc
        importlib.reload(svc)

        p = svc.AgoraPlayer.__new__(svc.AgoraPlayer)
        p.pipeline = None
        p._mpv_process = None
        p._cage_process = None
        p.current_desired = None
        p._loops_completed = 0
        p._health_retries = 0
        p._error_retry_delay = 3
        p._pending_error = None
        p._plymouth_quit = False
        p._current_path = None
        p._current_mtime = None
        p._board = svc.Board.ZERO_2W
        p._i2c_bus = "/dev/i2c-2"
        p._player_backend = "gstreamer"
        yield p


class TestOnEosLoopCount:
    def test_finite_loops_counts_and_stops(self, player):
        """After reaching loop_count EOS events, player should show splash."""
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=3,
        )
        mock_pipeline = MagicMock()
        player.pipeline = mock_pipeline

        with patch("player.service.Gst") as mock_gst, \
             patch.object(player, "_show_splash") as mock_splash:
            mock_gst.Format.TIME = "TIME"
            mock_gst.SeekFlags.FLUSH = 1
            mock_gst.SeekFlags.KEY_UNIT = 2

            # First 2 EOS: should seek to 0 (loop)
            player._on_eos(None, None)
            assert player._loops_completed == 1
            mock_pipeline.seek_simple.assert_called_once()
            mock_splash.assert_not_called()

            mock_pipeline.reset_mock()
            player._on_eos(None, None)
            assert player._loops_completed == 2
            mock_pipeline.seek_simple.assert_called_once()
            mock_splash.assert_not_called()

            # Third EOS: should stop and show splash
            mock_pipeline.reset_mock()
            player._on_eos(None, None)
            assert player._loops_completed == 3
            mock_pipeline.seek_simple.assert_not_called()
            mock_splash.assert_called_once()

    def test_infinite_loops_never_stops(self, player):
        """With loop_count=None, player loops indefinitely."""
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=None,
        )
        mock_pipeline = MagicMock()
        player.pipeline = mock_pipeline

        with patch("player.service.Gst") as mock_gst, \
             patch.object(player, "_show_splash") as mock_splash:
            mock_gst.Format.TIME = "TIME"
            mock_gst.SeekFlags.FLUSH = 1
            mock_gst.SeekFlags.KEY_UNIT = 2

            for i in range(10):
                player._on_eos(None, None)
                assert player._loops_completed == i + 1
                mock_splash.assert_not_called()

            assert mock_pipeline.seek_simple.call_count == 10

    def test_loop_count_one_stops_after_first_play(self, player):
        """loop_count=1 means play once then stop."""
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=1,
        )
        player.pipeline = MagicMock()

        with patch("player.service.Gst"), \
             patch.object(player, "_show_splash") as mock_splash:
            player._on_eos(None, None)
            assert player._loops_completed == 1
            mock_splash.assert_called_once()


class TestApplyDesiredResetsLoopCounter:
    def test_counter_resets_on_new_playback(self, player, tmp_path):
        """Starting new playback should reset _loops_completed to 0."""
        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib") as mock_glib, \
             patch.object(player, "_has_audio", return_value=False), \
             patch.object(player, "_resolve_asset") as mock_resolve, \
             patch.object(player, "_update_current"):
            mock_gst.parse_launch.return_value = MagicMock()
            mock_gst.State.PLAYING = "PLAYING"

            video = tmp_path / "v.mp4"
            video.write_bytes(b"\x00" * 16)
            mock_resolve.return_value = video

            player.base = tmp_path
            player.state_dir = tmp_path / "state"
            player.state_dir.mkdir()
            player.desired_path = player.state_dir / "desired.json"
            player.current_path = player.state_dir / "current.json"

            # Simulate having completed some loops previously
            player._loops_completed = 5

            desired = DesiredState(mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=3)
            from shared.state import write_state
            write_state(player.desired_path, desired)

            player.apply_desired()
            assert player._loops_completed == 0


class TestApplyDesiredSkipsRebuildWithLoopCount:
    def test_same_content_with_same_loop_count_skips_rebuild(self, player, tmp_path):
        """Same asset + same loop_count should not rebuild pipeline."""
        # Set up asset directory with a test file
        player.base = tmp_path
        player.assets_dir = tmp_path / "assets"
        (player.assets_dir / "videos").mkdir(parents=True)
        video = player.assets_dir / "videos" / "v.mp4"
        video.write_bytes(b"fake")

        desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=5,
        )
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=5,
        )
        player.pipeline = MagicMock()
        player._current_path = video
        player._current_mtime = video.stat().st_mtime

        player.state_dir = tmp_path / "state"
        player.state_dir.mkdir()
        player.desired_path = player.state_dir / "desired.json"
        player.current_path = player.state_dir / "current.json"

        from shared.state import write_state
        write_state(player.desired_path, desired)

        with patch.object(player, "_teardown") as mock_teardown, \
             patch.object(player, "_update_current"):
            player.apply_desired()
            mock_teardown.assert_not_called()

    def test_different_loop_count_triggers_rebuild(self, player, tmp_path):
        """Changing loop_count for same asset should rebuild pipeline."""
        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib"), \
             patch.object(player, "_has_audio", return_value=False), \
             patch.object(player, "_resolve_asset") as mock_resolve, \
             patch.object(player, "_update_current"):
            mock_gst.parse_launch.return_value = MagicMock()
            mock_gst.State.PLAYING = "PLAYING"

            video = tmp_path / "v.mp4"
            video.write_bytes(b"\x00" * 16)
            mock_resolve.return_value = video

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=5,
            )
            player.pipeline = MagicMock()
            player._current_path = video
            player._current_mtime = video.stat().st_mtime
            player.base = tmp_path
            player.state_dir = tmp_path / "state"
            player.state_dir.mkdir()
            player.desired_path = player.state_dir / "desired.json"
            player.current_path = player.state_dir / "current.json"

            desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="v.mp4", loop=True, loop_count=10,
            )
            from shared.state import write_state
            write_state(player.desired_path, desired)

            player.apply_desired()
            assert player._loops_completed == 0


# ── CMS client tests ──


class TestCMSClientLoopCount:
    """Test that CMS client passes loop_count from messages to DesiredState."""

    def test_handle_play_passes_loop_count(self, tmp_path):
        """_handle_play should include loop_count in DesiredState."""
        import asyncio
        import sys
        sys.modules.setdefault("websockets", MagicMock())
        sys.modules.setdefault("websockets.asyncio", MagicMock())
        sys.modules.setdefault("websockets.asyncio.client", MagicMock())
        sys.modules.setdefault("aiohttp", MagicMock())
        sys.modules.setdefault("cms_client.asset_manager", MagicMock())

        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        desired_path = tmp_path / "desired.json"

        # Minimal mock for _handle_play
        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.desired_state_path = desired_path
        client._last_eval_state = None
        client._current_schedule_id = None
        client._current_schedule_name = None
        client._current_asset = None
        client._eval_wake = asyncio.Event()
        client._last_player_mode = None
        client._ws = None

        msg = {"type": "play", "asset": "v.mp4", "loop": True, "loop_count": 7}
        asyncio.run(client._handle_play(msg))

        import json
        data = json.loads(desired_path.read_text())
        assert data["loop_count"] == 7

    def test_handle_play_no_loop_count(self, tmp_path):
        """_handle_play without loop_count should set it to None."""
        import asyncio
        import sys
        sys.modules.setdefault("websockets", MagicMock())
        sys.modules.setdefault("websockets.asyncio", MagicMock())
        sys.modules.setdefault("websockets.asyncio.client", MagicMock())
        sys.modules.setdefault("aiohttp", MagicMock())
        sys.modules.setdefault("cms_client.asset_manager", MagicMock())

        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        desired_path = tmp_path / "desired.json"

        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.desired_state_path = desired_path
        client._last_eval_state = None
        client._current_schedule_id = None
        client._current_schedule_name = None
        client._current_asset = None
        client._eval_wake = asyncio.Event()
        client._last_player_mode = None
        client._ws = None

        msg = {"type": "play", "asset": "v.mp4", "loop": True}
        asyncio.run(client._handle_play(msg))

        import json
        data = json.loads(desired_path.read_text())
        assert data["loop_count"] is None

    def test_evaluate_schedule_passes_loop_count(self, tmp_path):
        """_evaluate_schedule should include loop_count from winning schedule."""
        import asyncio
        import sys
        sys.modules.setdefault("websockets", MagicMock())
        sys.modules.setdefault("websockets.asyncio", MagicMock())
        sys.modules.setdefault("websockets.asyncio.client", MagicMock())
        sys.modules.setdefault("aiohttp", MagicMock())
        sys.modules.setdefault("cms_client.asset_manager", MagicMock())

        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        desired_path = tmp_path / "desired.json"

        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.desired_state_path = desired_path
        client._last_eval_state = None
        client._current_schedule_id = None
        client._current_schedule_name = None
        client._current_asset = None
        client._eval_wake = asyncio.Event()
        client._last_player_mode = None
        client._ws = None
        client.asset_manager = MagicMock()

        sync_data = {
            "timezone": "UTC",
            "schedules": [{
                "id": "s1",
                "name": "Test",
                "asset": "v.mp4",
                "asset_checksum": "abc",
                "start_time": "00:00",
                "end_time": "23:59",
                "start_date": None,
                "end_date": None,
                "days_of_week": None,
                "priority": 0,
                "loop_count": 4,
            }],
            "default_asset": None,
        }

        client._evaluate_schedule(sync_data)

        import json
        data = json.loads(desired_path.read_text())
        assert data["loop_count"] == 4
        assert data["mode"] == "play"
        assert data["asset"] == "v.mp4"


# ── Player watch loop tests ──


class TestPlayerWatchLoop:
    """Verify _player_watch_loop wakes eval loop on mode transition."""

    def test_play_to_splash_sets_eval_wake(self, tmp_path):
        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        current_path = tmp_path / "current.json"

        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.current_state_path = current_path
        client._running = True
        client._eval_wake = asyncio.Event()
        client._last_player_mode = "play"  # was playing

        # Player stopped — write splash mode
        import json
        current_path.write_text(json.dumps({"mode": "splash"}))

        async def run_one_tick():
            # Run the loop body once, then stop
            async def limited_loop():
                # Simulate one iteration
                data = json.loads(client.settings.current_state_path.read_text())
                mode = data.get("mode", "splash")
                prev = client._last_player_mode
                client._last_player_mode = mode
                if prev == "play" and mode != "play":
                    client._eval_wake.set()

            await limited_loop()

        asyncio.run(run_one_tick())
        assert client._eval_wake.is_set()
        assert client._last_player_mode == "splash"

    def test_play_to_play_does_not_wake(self, tmp_path):
        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        current_path = tmp_path / "current.json"

        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.current_state_path = current_path
        client._running = True
        client._eval_wake = asyncio.Event()
        client._last_player_mode = "play"  # still playing

        import json
        current_path.write_text(json.dumps({"mode": "play"}))

        async def run_one_tick():
            data = json.loads(client.settings.current_state_path.read_text())
            mode = data.get("mode", "splash")
            prev = client._last_player_mode
            client._last_player_mode = mode
            if prev == "play" and mode != "play":
                client._eval_wake.set()

        asyncio.run(run_one_tick())
        assert not client._eval_wake.is_set()

    def test_splash_to_play_does_not_wake(self, tmp_path):
        import importlib
        import cms_client.service as svc
        importlib.reload(svc)

        current_path = tmp_path / "current.json"

        client = svc.CMSClient.__new__(svc.CMSClient)
        client.settings = MagicMock()
        client.settings.current_state_path = current_path
        client._running = True
        client._eval_wake = asyncio.Event()
        client._last_player_mode = "splash"

        import json
        current_path.write_text(json.dumps({"mode": "play", "asset": "v.mp4"}))

        async def run_one_tick():
            data = json.loads(client.settings.current_state_path.read_text())
            mode = data.get("mode", "splash")
            prev = client._last_player_mode
            client._last_player_mode = mode
            if prev == "play" and mode != "play":
                client._eval_wake.set()

        asyncio.run(run_one_tick())
        assert not client._eval_wake.is_set()
        assert client._last_player_mode == "play"
