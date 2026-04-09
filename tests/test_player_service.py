"""Tests for player service — pipeline selection logic."""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock, call

import pytest

from shared.models import PlaybackMode, DesiredState


@pytest.fixture
def player():
    """Create an AgoraPlayer instance with mocked GStreamer."""
    with patch.dict("sys.modules", {
        "gi": MagicMock(),
        "gi.repository": MagicMock(),
    }):
        # Must patch before importing since player imports Gst at module level
        import importlib
        import player.service as svc
        importlib.reload(svc)

        p = svc.AgoraPlayer.__new__(svc.AgoraPlayer)
        p.pipeline = None
        p.current_desired = None
        p._plymouth_quit = False
        p._current_path = None
        p._current_mtime = None
        p._health_retries = 0
        p._error_retry_delay = 3
        p._pending_error = None
        p._loops_completed = 0
        yield p


class TestPipelineSelection:
    """Verify _build_pipeline picks the correct pipeline string based on audio presence."""

    def test_video_with_audio_uses_audio_pipeline(self, player, tmp_path):
        video = tmp_path / "video.mp4"
        video.touch()

        with patch.object(type(player), "_has_audio", return_value=True):
            with patch("player.service.Gst") as mock_gst:
                mock_gst.parse_launch.return_value = MagicMock()
                player._build_pipeline(video, is_video=True)

                pipeline_str = mock_gst.parse_launch.call_args[0][0]
                assert "alsasink" in pipeline_str
                assert "dmux.audio_0" in pipeline_str
                assert "sync=true" in pipeline_str

    def test_video_without_audio_uses_no_audio_pipeline(self, player, tmp_path):
        video = tmp_path / "video.mp4"
        video.touch()

        with patch.object(type(player), "_has_audio", return_value=False):
            with patch("player.service.Gst") as mock_gst:
                mock_gst.parse_launch.return_value = MagicMock()
                player._build_pipeline(video, is_video=True)

                pipeline_str = mock_gst.parse_launch.call_args[0][0]
                assert "alsasink" not in pipeline_str
                assert "dmux.audio_0" not in pipeline_str
                assert "sync=false" in pipeline_str

    def test_image_ignores_audio_check(self, player, tmp_path):
        img = tmp_path / "image.png"
        img.touch()

        with patch.object(type(player), "_has_audio") as mock_has_audio:
            with patch("player.service.Gst") as mock_gst:
                mock_gst.parse_launch.return_value = MagicMock()
                player._build_pipeline(img, is_video=False)

                mock_has_audio.assert_not_called()
                pipeline_str = mock_gst.parse_launch.call_args[0][0]
                assert "imagefreeze" in pipeline_str


class TestPipelineHealthCheck:
    """Verify _check_pipeline_health retries with rebuild before giving up."""

    def test_no_error_when_pipeline_is_playing(self, player):
        """Pipeline in PLAYING state should not report an error."""
        with patch("player.service.Gst") as mock_gst:
            playing_state = MagicMock()
            playing_state.value_nick = "playing"
            mock_gst.State.PLAYING = playing_state

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, playing_state, None)
            player.pipeline = mock_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update:
                player._check_pipeline_health("test.mp4")
                mock_update.assert_not_called()

    def test_skips_check_when_asset_changed(self, player):
        """Health check should be a no-op if a different asset is now playing."""
        player.pipeline = MagicMock()
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="other.mp4", loop=True
        )

        with patch.object(player, "_update_current") as mock_update:
            player._check_pipeline_health("test.mp4")
            mock_update.assert_not_called()

    def test_first_failure_rebuilds_pipeline(self, player, tmp_path):
        """First health check failure should teardown, rebuild, and schedule another check."""
        video = tmp_path / "test.mp4"
        video.write_bytes(b"\x00" * 100)
        player._current_path = video

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib") as mock_glib:
            mock_gst.State.PLAYING = "PLAYING"
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            mock_state = MagicMock()
            mock_state.value_nick = "ready"
            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, mock_state, None)
            player.pipeline = mock_pipeline

            new_pipeline = MagicMock()
            mock_gst.parse_launch.return_value = new_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update, \
                 patch.object(player, "_show_splash") as mock_splash:
                player._check_pipeline_health("test.mp4")

                # Should NOT show splash or report error on first failure
                mock_splash.assert_not_called()
                mock_update.assert_not_called()
                # Should have rebuilt the pipeline
                assert player.pipeline == new_pipeline
                new_pipeline.set_state.assert_called_with("PLAYING")
                # Should schedule another health check
                mock_glib.timeout_add_seconds.assert_called_once()
                assert player._health_retries == 1

    def test_retries_exhaust_then_fails(self, player, tmp_path):
        """After max retries, should teardown, report error, and show splash."""
        video = tmp_path / "test.mp4"
        video.write_bytes(b"\x00" * 100)
        player._current_path = video
        player._health_retries = 3  # Already at max

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib") as mock_glib:
            mock_gst.State.PLAYING = "PLAYING"
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            mock_state = MagicMock()
            mock_state.value_nick = "ready"
            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, mock_state, None)
            player.pipeline = mock_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update, \
                 patch.object(player, "_show_splash"):
                player._check_pipeline_health("test.mp4")

                # Should report error with retry count
                mock_update.assert_called_once()
                error_msg = mock_update.call_args[1]["error"]
                assert "3 retries" in error_msg
                # Pipeline should be torn down
                assert player.pipeline is None
                assert player._health_retries == 0
                # Should schedule splash
                mock_glib.timeout_add_seconds.assert_called_once()

    def test_success_after_retry_logs_and_resets(self, player):
        """If pipeline reaches PLAYING after retries, counter should reset."""
        player._health_retries = 2  # Had 2 prior failures

        with patch("player.service.Gst") as mock_gst:
            playing_state = MagicMock()
            playing_state.value_nick = "playing"
            mock_gst.State.PLAYING = playing_state

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, playing_state, None)
            player.pipeline = mock_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update:
                player._check_pipeline_health("test.mp4")
                mock_update.assert_not_called()
                assert player._health_retries == 0

    def test_new_playback_resets_retry_counter(self, player, tmp_path):
        """Starting a new playback should reset _health_retries."""
        player._health_retries = 2

        video = tmp_path / "videos" / "new.mp4"
        video.parent.mkdir(parents=True)
        video.write_bytes(b"\x00" * 100)

        state_dir = tmp_path / "state"
        state_dir.mkdir()
        player.state_dir = state_dir
        player.desired_path = state_dir / "desired.json"
        player.current_path = state_dir / "current.json"
        player.base = tmp_path
        player.assets_dir = tmp_path

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib"):
            mock_gst.State.NULL = "NULL"
            mock_gst.State.PLAYING = "PLAYING"
            mock_gst.CLOCK_TIME_NONE = 0
            mock_gst.parse_launch.return_value = MagicMock()

            # Different timestamp so we don't hit the "unchanged" early return
            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="old.mp4", loop=True,
                timestamp="2026-01-01T00:00:00Z",
            )

            desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="new.mp4", loop=True,
                timestamp="2026-01-01T00:00:01Z",
            )

            with patch.object(player, "_update_current"), \
                 patch("player.service.read_state", return_value=desired):
                player.desired_path.write_text("{}")
                player.apply_desired()
                assert player._health_retries == 0


class TestTeardownSync:
    """Verify _teardown waits for NULL state before returning."""

    def test_teardown_waits_for_null_state(self, player):
        """_teardown should call get_state to wait for NULL transition."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            mock_pipeline = MagicMock()
            player.pipeline = mock_pipeline

            player._teardown()

            mock_pipeline.set_state.assert_called_once_with("NULL")
            mock_pipeline.get_state.assert_called_once_with(0)
            assert player.pipeline is None


class TestHasAudio:
    """Verify _has_audio detects audio streams via qtdemux pad inspection."""

    def _make_player(self):
        """Create a minimal player instance for _has_audio testing."""
        with patch.dict("sys.modules", {
            "gi": MagicMock(),
            "gi.repository": MagicMock(),
        }):
            import importlib
            import player.service as svc
            importlib.reload(svc)
            return svc.AgoraPlayer.__new__(svc.AgoraPlayer), svc

    def test_returns_true_when_audio_pad_found(self):
        player, svc = self._make_player()

        mock_pipe = MagicMock()
        mock_dmux = MagicMock()
        mock_pipe.get_by_name.return_value = mock_dmux

        # Capture the signal handlers when dmux.connect() is called
        handlers = {}
        def capture_connect(signal_name, handler):
            handlers[signal_name] = handler
        mock_dmux.connect.side_effect = capture_connect

        mock_ctx = MagicMock()
        def fire_pads(*_args, **_kwargs):
            # Simulate qtdemux discovering pads
            mock_pad = MagicMock()
            mock_pad.get_name.return_value = "video_0"
            handlers["pad-added"](mock_dmux, mock_pad)
            mock_pad2 = MagicMock()
            mock_pad2.get_name.return_value = "audio_0"
            handlers["pad-added"](mock_dmux, mock_pad2)
            handlers["no-more-pads"](mock_dmux)
            return True
        mock_ctx.iteration.side_effect = fire_pads

        with patch.object(svc, "Gst") as mock_gst, \
             patch.object(svc, "GLib") as mock_glib:
            mock_gst.parse_launch.return_value = mock_pipe
            mock_glib.MainContext.default.return_value = mock_ctx

            assert player._has_audio(Path("/fake/video.mp4")) is True
            mock_pipe.set_state.assert_any_call(mock_gst.State.NULL)

    def test_returns_false_when_no_audio_pad(self):
        player, svc = self._make_player()

        mock_pipe = MagicMock()
        mock_dmux = MagicMock()
        mock_pipe.get_by_name.return_value = mock_dmux

        handlers = {}
        def capture_connect(signal_name, handler):
            handlers[signal_name] = handler
        mock_dmux.connect.side_effect = capture_connect

        mock_ctx = MagicMock()
        def fire_pads(*_args, **_kwargs):
            mock_pad = MagicMock()
            mock_pad.get_name.return_value = "video_0"
            handlers["pad-added"](mock_dmux, mock_pad)
            handlers["no-more-pads"](mock_dmux)
            return True
        mock_ctx.iteration.side_effect = fire_pads

        with patch.object(svc, "Gst") as mock_gst, \
             patch.object(svc, "GLib") as mock_glib:
            mock_gst.parse_launch.return_value = mock_pipe
            mock_glib.MainContext.default.return_value = mock_ctx

            assert player._has_audio(Path("/fake/video.mp4")) is False

    def test_returns_true_on_exception(self):
        """If qtdemux fails, assume audio exists as a safe default."""
        player, svc = self._make_player()

        with patch.object(svc, "Gst") as mock_gst:
            mock_gst.parse_launch.side_effect = Exception("pipeline error")

            assert player._has_audio(Path("/fake/video.mp4")) is True


class TestStateChanged:
    """Verify _on_state_changed updates current.json with accurate pipeline state."""

    def test_updates_state_when_pipeline_reaches_playing(self, player):
        """When pipeline reaches PLAYING, current.json should reflect that with started_at."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.PLAYING = "PLAYING"

            mock_pipeline = MagicMock()
            player.pipeline = mock_pipeline
            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            # Build a mock bus message from the pipeline itself
            mock_message = MagicMock()
            mock_message.src = mock_pipeline
            new_state = MagicMock()
            new_state.value_nick = "playing"
            old_state = MagicMock()
            old_state.value_nick = "paused"
            mock_message.parse_state_changed.return_value = (old_state, new_state, None)

            # new state == Gst.State.PLAYING
            new_state.__eq__ = lambda self, other: other == "PLAYING"

            with patch.object(player, "_update_current") as mock_update:
                player._on_state_changed(None, mock_message)
                mock_update.assert_called_once()
                call_kwargs = mock_update.call_args[1]
                assert call_kwargs["mode"] == PlaybackMode.PLAY
                assert call_kwargs["asset"] == "test.mp4"
                assert call_kwargs["started_at"] is not None

    def test_ignores_element_state_changes(self, player):
        """State changes from child elements (not the pipeline) should be ignored."""
        mock_pipeline = MagicMock()
        player.pipeline = mock_pipeline

        mock_message = MagicMock()
        mock_message.src = MagicMock()  # Different object than pipeline

        with patch.object(player, "_update_current") as mock_update:
            player._on_state_changed(None, mock_message)
            mock_update.assert_not_called()

    def test_ignores_non_playing_transitions(self, player):
        """Transitions to states other than PLAYING should not update current.json."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.PLAYING = "PLAYING"

            mock_pipeline = MagicMock()
            player.pipeline = mock_pipeline
            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            mock_message = MagicMock()
            mock_message.src = mock_pipeline
            new_state = MagicMock()
            new_state.value_nick = "paused"
            old_state = MagicMock()
            old_state.value_nick = "ready"
            mock_message.parse_state_changed.return_value = (old_state, new_state, None)

            # new state != Gst.State.PLAYING
            new_state.__eq__ = lambda self, other: False

            with patch.object(player, "_update_current") as mock_update:
                player._on_state_changed(None, mock_message)
                mock_update.assert_not_called()


class TestPlaybackPosition:
    """Verify playback position querying and periodic updates."""

    def test_query_position_ms_returns_milliseconds(self, player):
        """Position in nanoseconds should be converted to milliseconds."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.Format.TIME = "TIME"
            mock_pipeline = MagicMock()
            # 5 seconds = 5_000_000_000 nanoseconds
            mock_pipeline.query_position.return_value = (True, 5_000_000_000)
            player.pipeline = mock_pipeline

            assert player._query_position_ms() == 5000

    def test_query_position_ms_returns_none_when_no_pipeline(self, player):
        """Should return None when no pipeline exists."""
        player.pipeline = None
        assert player._query_position_ms() is None

    def test_query_position_ms_returns_none_on_failure(self, player):
        """Should return None when query fails."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.Format.TIME = "TIME"
            mock_pipeline = MagicMock()
            mock_pipeline.query_position.return_value = (False, -1)
            player.pipeline = mock_pipeline

            assert player._query_position_ms() is None

    def test_update_position_stops_when_not_playing(self, player):
        """Timer should stop (return False) when not in PLAY mode."""
        player.pipeline = MagicMock()
        player.current_desired = DesiredState(
            mode=PlaybackMode.SPLASH, asset=None, loop=False
        )
        assert player._update_position() is False

    def test_update_position_writes_to_state_file(self, player, tmp_path):
        """Timer should update playback_position_ms in current.json."""
        state_file = tmp_path / "current.json"
        player.current_path = state_file

        from shared.models import CurrentState
        from shared.state import write_state
        initial = CurrentState(
            mode=PlaybackMode.PLAY, asset="test.mp4",
            pipeline_state="PLAYING", playback_position_ms=1000,
        )
        write_state(state_file, initial)

        player.pipeline = MagicMock()
        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
        )

        with patch.object(player, "_query_position_ms", return_value=5000):
            result = player._update_position()

        assert result is True
        import json
        data = json.loads(state_file.read_text())
        assert data["playback_position_ms"] == 5000


class TestSplashStateConsistency:
    """Verify _show_splash updates current_desired for both image and video splash."""

    def test_image_splash_updates_current_desired(self, player, tmp_path):
        """When showing an image splash, current_desired must reflect SPLASH mode.

        Regression: if current_desired is not updated for image splash, a
        subsequent _on_state_changed callback will use stale desired state and
        overwrite current.json with the old (failed) PLAY mode, making the CMS
        think the device is playing when it's actually showing splash.
        """
        splash_img = tmp_path / "assets" / "splash" / "default.png"
        splash_img.parent.mkdir(parents=True)
        splash_img.touch()

        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="failed_video.mp4", loop=True
        )

        with patch.object(player, "_find_splash", return_value=splash_img), \
             patch.object(player, "_teardown"), \
             patch.object(player, "_build_pipeline") as mock_build, \
             patch.object(player, "_update_current"):
            mock_pipeline = MagicMock()
            mock_build.return_value = mock_pipeline

            player._show_splash()

            # current_desired must be SPLASH, not the stale PLAY mode
            assert player.current_desired.mode == PlaybackMode.SPLASH
            assert player.current_desired.asset is None

    def test_video_splash_updates_current_desired(self, player, tmp_path):
        """Video splash should also set current_desired to SPLASH with loop=True."""
        splash_vid = tmp_path / "assets" / "splash" / "default.mp4"
        splash_vid.parent.mkdir(parents=True)
        splash_vid.touch()

        player.current_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="failed_video.mp4", loop=True
        )

        with patch.object(player, "_find_splash", return_value=splash_vid), \
             patch.object(player, "_teardown"), \
             patch.object(player, "_build_pipeline") as mock_build, \
             patch.object(player, "_update_current"):
            mock_pipeline = MagicMock()
            mock_build.return_value = mock_pipeline

            player._show_splash()

            assert player.current_desired.mode == PlaybackMode.SPLASH
            assert player.current_desired.loop is True


class TestAssetNotFoundDesiredState:
    """Verify apply_desired does not clobber current_desired when asset is missing."""

    def test_asset_not_found_preserves_current_desired(self, player, tmp_path):
        """When an asset is not found, current_desired should NOT be updated.

        Regression: if current_desired is set before asset resolution, the old
        running pipeline's _on_state_changed callback may use it to write
        incorrect state to current.json.
        """
        player.desired_path = tmp_path / "desired.json"
        player.current_path = tmp_path / "current.json"
        player.assets_dir = tmp_path / "assets"
        player.assets_dir.mkdir()
        player._loops_completed = 0

        # Previous state: showing splash (with an older timestamp)
        from datetime import datetime, timezone, timedelta
        old_desired = DesiredState(
            mode=PlaybackMode.SPLASH,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        player.current_desired = old_desired

        # New desired state wants to play a non-existent asset (different timestamp)
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="missing.mp4", loop=True,
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        from shared.state import write_state
        write_state(player.desired_path, new_desired)

        with patch.object(player, "_update_current"), \
             patch.object(player, "_show_splash"):
            player.apply_desired()

        # current_desired should still be the old splash state, not the new play state
        assert player.current_desired.mode == PlaybackMode.SPLASH


class TestChecksumValidation:
    """Verify player validates asset checksum before building pipeline."""

    def test_matching_checksum_allows_playback(self, player, tmp_path):
        """Player should proceed when file checksum matches expected."""
        player.desired_path = tmp_path / "desired.json"
        player.current_path = tmp_path / "current.json"
        player.assets_dir = tmp_path / "assets"
        (player.assets_dir / "videos").mkdir(parents=True)
        player._loops_completed = 0
        player._plymouth_quit = True

        video = player.assets_dir / "videos" / "test.mp4"
        content = b"\x00\x00\x00\x20ftypisom" + b"\x00" * 100
        video.write_bytes(content)

        import hashlib
        checksum = hashlib.sha256(content).hexdigest()

        from datetime import datetime, timezone
        desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.mp4", loop=True,
            expected_checksum=checksum,
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        from shared.state import write_state
        write_state(player.desired_path, desired)

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib"), \
             patch.object(player, "_has_audio", return_value=False), \
             patch.object(player, "_update_current"):
            mock_gst.parse_launch.return_value = MagicMock()
            mock_gst.State.PLAYING = "PLAYING"

            player.apply_desired()

            # Pipeline should have been built
            mock_gst.parse_launch.assert_called_once()

    def test_mismatched_checksum_blocks_playback(self, player, tmp_path):
        """Player should refuse to play when checksum doesn't match."""
        player.desired_path = tmp_path / "desired.json"
        player.current_path = tmp_path / "current.json"
        player.assets_dir = tmp_path / "assets"
        (player.assets_dir / "videos").mkdir(parents=True)
        player._loops_completed = 0

        video = player.assets_dir / "videos" / "bad.mp4"
        video.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\x00" * 100)

        from datetime import datetime, timezone
        desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="bad.mp4", loop=True,
            expected_checksum="0000000000000000000000000000000000000000000000000000000000000000",
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        from shared.state import write_state
        write_state(player.desired_path, desired)

        with patch("player.service.Gst") as mock_gst, \
             patch.object(player, "_update_current") as mock_update, \
             patch.object(player, "_show_splash") as mock_splash:
            mock_gst.State.PLAYING = "PLAYING"

            player.apply_desired()

            # Pipeline should NOT have been built
            mock_gst.parse_launch.assert_not_called()
            # Error should have been reported
            mock_update.assert_called()
            call_kwargs = mock_update.call_args[1]
            assert "Checksum mismatch" in call_kwargs["error"]
            # Should fall back to splash
            mock_splash.assert_called_once()

    def test_no_checksum_skips_validation(self, player, tmp_path):
        """When no expected_checksum is set, skip validation and play."""
        player.desired_path = tmp_path / "desired.json"
        player.current_path = tmp_path / "current.json"
        player.assets_dir = tmp_path / "assets"
        (player.assets_dir / "videos").mkdir(parents=True)
        player._loops_completed = 0
        player._plymouth_quit = True

        video = player.assets_dir / "videos" / "test.mp4"
        video.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\x00" * 100)

        from datetime import datetime, timezone
        desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="test.mp4", loop=True,
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        from shared.state import write_state
        write_state(player.desired_path, desired)

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib"), \
             patch.object(player, "_has_audio", return_value=False), \
             patch.object(player, "_update_current"):
            mock_gst.parse_launch.return_value = MagicMock()
            mock_gst.State.PLAYING = "PLAYING"

            player.apply_desired()

            # Pipeline should still be built
            mock_gst.parse_launch.assert_called_once()


class TestStartupDesiredRace:
    """Verify desired.json written during splash startup is not missed."""

    def test_desired_written_during_splash_is_applied(self, player, tmp_path):
        """If desired.json arrives between the first apply_desired() and inotify
        setup, the player must still process it — not remain stuck in splash.

        Reproduces: device boots, player starts, apply_desired sees no desired.json
        and shows splash (taking several seconds), CMS writes desired.json during
        that window, inotify is set up after splash is ready — change is missed.
        """
        player.desired_path = tmp_path / "desired.json"
        player.current_path = tmp_path / "current.json"
        player.assets_dir = tmp_path / "assets"
        player.state_dir = tmp_path
        player.persist_dir = tmp_path / "persist"
        player.persist_dir.mkdir()
        player.splash_config_path = player.persist_dir / "splash"
        (player.assets_dir / "images").mkdir(parents=True)
        (player.assets_dir / "splash").mkdir(parents=True)
        player._loops_completed = 0
        player._plymouth_quit = True
        player._running = True

        # Create a default splash and the target asset (same image, different paths)
        splash_img = player.assets_dir / "splash" / "default.png"
        splash_img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        target_img = player.assets_dir / "images" / "target.png"
        target_img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 200)

        import hashlib
        checksum = hashlib.sha256(target_img.read_bytes()).hexdigest()

        from datetime import datetime, timezone
        from shared.state import write_state

        # Track apply_desired calls to simulate writing desired.json after the
        # first call but before inotify is ready.
        original_apply = type(player).apply_desired
        call_count = [0]

        def patched_apply(self_inner):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: desired.json doesn't exist yet → shows splash.
                original_apply(self_inner)
                # NOW write desired.json (simulates CMS writing during splash).
                desired = DesiredState(
                    mode=PlaybackMode.PLAY,
                    asset="target.png",
                    loop=True,
                    expected_checksum=checksum,
                    timestamp=datetime(2026, 4, 4, 22, 27, 44, tzinfo=timezone.utc),
                )
                write_state(player.desired_path, desired)
            else:
                original_apply(self_inner)

        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib") as mock_glib, \
             patch.object(player, "_has_audio", return_value=False), \
             patch.object(player, "_quit_plymouth"), \
             patch("player.service.signal"):
            mock_pipeline = MagicMock()
            mock_gst.parse_launch.return_value = mock_pipeline
            mock_gst.State.PLAYING = "PLAYING"
            mock_glib.MainLoop.return_value = MagicMock()

            # Make inotify unavailable so we don't need real fs events
            with patch.object(player, "_setup_inotify", return_value=True):
                with patch.object(type(player), "apply_desired", patched_apply):
                    player.loop = MagicMock()
                    player.run()

        # apply_desired should have been called at least twice
        assert call_count[0] >= 2, (
            f"apply_desired called {call_count[0]} time(s); expected >=2 to "
            f"catch desired.json written during splash startup"
        )
        # Player should end up with the play desired state, not splash
        assert player.current_desired is not None
        assert player.current_desired.mode == PlaybackMode.PLAY
        assert player.current_desired.asset == "target.png"


class TestTeardownBusCleanup:
    """Verify _teardown removes bus signal watch to prevent GSource leaks."""

    def test_teardown_removes_signal_watch(self, player):
        """_teardown must call bus.remove_signal_watch() before setting NULL."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            mock_bus = MagicMock()
            mock_pipeline = MagicMock()
            mock_pipeline.get_bus.return_value = mock_bus
            player.pipeline = mock_pipeline

            player._teardown()

            mock_pipeline.get_bus.assert_called_once()
            mock_bus.remove_signal_watch.assert_called_once()
            mock_pipeline.set_state.assert_called_once_with("NULL")
            mock_pipeline.get_state.assert_called_once_with(0)
            assert player.pipeline is None

    def test_teardown_handles_no_bus_gracefully(self, player):
        """_teardown should not crash if get_bus() returns None."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            mock_pipeline = MagicMock()
            mock_pipeline.get_bus.return_value = None
            player.pipeline = mock_pipeline

            player._teardown()

            assert player.pipeline is None

    def test_no_signal_watch_leak_after_error_loop(self, player):
        """Simulating multiple error→teardown→rebuild cycles should not accumulate bus watches."""
        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib"):
            mock_gst.State.NULL = "NULL"
            mock_gst.CLOCK_TIME_NONE = 0

            buses = []
            for i in range(5):
                mock_bus = MagicMock(name=f"bus_{i}")
                mock_pipeline = MagicMock(name=f"pipeline_{i}")
                mock_pipeline.get_bus.return_value = mock_bus
                player.pipeline = mock_pipeline

                player._teardown()

                mock_bus.remove_signal_watch.assert_called_once()
                buses.append(mock_bus)

            # All 5 buses should have had their signal watch removed
            assert all(b.remove_signal_watch.called for b in buses)


class TestErrorTranslation:
    """Verify _translate_error maps GStreamer errors to friendly messages."""

    def test_drm_set_plane_error(self, player):
        raw = "drmModeSetPlane failed: Permission denied (13)"
        result = player._translate_error(raw)
        assert result == "No display connected \u2014 check the HDMI cable"

    def test_audio_device_error(self, player):
        raw = "Could not open audio device for playback"
        result = player._translate_error(raw)
        assert result == "No audio output \u2014 check the HDMI cable"

    def test_memory_allocation_error(self, player):
        raw = "Failed to allocate required memory"
        result = player._translate_error(raw)
        assert result == "Not enough memory to decode this video"

    def test_unknown_error_passes_through(self, player):
        raw = "Some totally unexpected GStreamer error"
        result = player._translate_error(raw)
        assert result == "Playback error: Some totally unexpected GStreamer error"

    def test_drm_error_in_debug_string(self, player):
        """Real GStreamer wraps the specific error in debug, not err.message."""
        raw = "GStreamer encountered a general resource error."
        debug = "gst_kms_sink_show_frame: drmModeSetPlane failed: Permission denied (13)"
        result = player._translate_error(raw, debug)
        assert result == "No display connected \u2014 check the HDMI cable"

    def test_audio_error_in_message_only(self, player):
        """Audio errors appear directly in err.message."""
        raw = "Could not open audio device for playback"
        debug = "gstalsa: some alsa debug info"
        result = player._translate_error(raw, debug)
        assert result == "No audio output \u2014 check the HDMI cable"

    def test_generic_resource_error_without_details(self, player):
        """Generic resource error with no detail in debug passes through."""
        raw = "GStreamer encountered a general resource error."
        debug = "some unrelated debug info"
        result = player._translate_error(raw, debug)
        assert result == "Playback error: GStreamer encountered a general resource error."

    def test_is_display_error_true_for_drm(self, player):
        assert player._is_display_error("drmModeSetPlane failed: Permission denied (13)") is True

    def test_is_display_error_true_for_audio(self, player):
        assert player._is_display_error("Could not open audio device for playback") is True

    def test_is_display_error_false_for_other(self, player):
        assert player._is_display_error("Failed to allocate required memory") is False

    def test_is_display_error_via_debug_string(self, player):
        """_is_display_error should detect display errors in the debug string."""
        raw = "GStreamer encountered a general resource error."
        debug = "drmModeSetPlane failed: Permission denied (13)"
        assert player._is_display_error(raw, debug) is True


class TestErrorBackoff:
    """Verify _on_error uses exponential backoff for display errors."""

    def test_display_error_increases_retry_delay(self, player):
        """Display errors should double the retry delay each time."""
        with patch("player.service.GLib") as mock_glib:
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "drmModeSetPlane failed: Permission denied (13)"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash"):
                assert player._error_retry_delay == 3

                player._on_error(None, mock_message)
                delay1 = mock_glib.timeout_add_seconds.call_args[0][0]
                assert delay1 == 3
                assert player._error_retry_delay == 6

                mock_glib.reset_mock()
                player._on_error(None, mock_message)
                delay2 = mock_glib.timeout_add_seconds.call_args[0][0]
                assert delay2 == 6
                assert player._error_retry_delay == 12

    def test_display_error_caps_at_max_delay(self, player):
        """Retry delay should not exceed _RETRY_DELAY_MAX (60s)."""
        with patch("player.service.GLib") as mock_glib:
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "drmModeSetPlane failed: Permission denied (13)"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            player._error_retry_delay = 48

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash"):
                player._on_error(None, mock_message)
                delay = mock_glib.timeout_add_seconds.call_args[0][0]
                assert delay == 48
                assert player._error_retry_delay == 60  # capped

                mock_glib.reset_mock()
                player._on_error(None, mock_message)
                delay = mock_glib.timeout_add_seconds.call_args[0][0]
                assert delay == 60
                assert player._error_retry_delay == 60  # stays capped

    def test_non_display_error_resets_delay(self, player):
        """Non-display errors should use 3s and reset the backoff counter."""
        with patch("player.service.GLib") as mock_glib:
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "Failed to allocate required memory"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            player._error_retry_delay = 30  # Previously backed off

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash"):
                player._on_error(None, mock_message)
                delay = mock_glib.timeout_add_seconds.call_args[0][0]
                assert delay == 3
                assert player._error_retry_delay == 3

    def test_on_error_reports_friendly_message(self, player):
        """_on_error should pass the translated message to _update_current."""
        with patch("player.service.GLib"):
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "drmModeSetPlane failed: Permission denied (13)"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current") as mock_update, \
                 patch.object(player, "_show_splash"):
                player._on_error(None, mock_message)
                mock_update.assert_called_once_with(
                    error="No display connected \u2014 check the HDMI cable"
                )

    def test_on_error_shows_splash_immediately(self, player):
        """_on_error should show splash immediately as visual fallback."""
        with patch("player.service.GLib"):
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "drmModeSetPlane failed"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash") as mock_splash:
                player._on_error(None, mock_message)
                mock_splash.assert_called_once()

    def test_on_error_schedules_retry_desired(self, player):
        """_on_error should schedule _retry_desired after backoff delay."""
        with patch("player.service.GLib") as mock_glib:
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "Could not open audio device"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash"):
                player._on_error(None, mock_message)
                mock_glib.timeout_add_seconds.assert_called_once_with(
                    3, player._retry_desired,
                )

    def test_on_error_sets_pending_error_for_splash(self, player):
        """_on_error should set _pending_error so splash preserves it."""
        with patch("player.service.GLib"):
            mock_message = MagicMock()
            mock_err = MagicMock()
            mock_err.message = "Could not open audio device"
            mock_message.parse_error.return_value = (mock_err, "debug info")

            with patch.object(player, "_teardown"), \
                 patch.object(player, "_update_current"), \
                 patch.object(player, "_show_splash"):
                player._on_error(None, mock_message)
                # _show_splash will have consumed it, but _pending_error
                # was set to the friendly message before the call
                # Verify by checking the call happened after _pending_error was set
                # (splash mock doesn't clear it)
                assert player._pending_error == "No audio output \u2014 check the HDMI cable"

    def test_retry_desired_calls_apply_desired(self, player):
        """_retry_desired should re-read desired.json via apply_desired."""
        with patch.object(player, "apply_desired") as mock_apply:
            result = player._retry_desired()
            mock_apply.assert_called_once()
            assert result is False  # one-shot timer

    def test_splash_preserves_pending_error(self, player, tmp_path):
        """_show_splash should include _pending_error in current.json."""
        splash_img = tmp_path / "assets" / "splash" / "default.png"
        splash_img.parent.mkdir(parents=True)
        splash_img.touch()

        player._pending_error = "No audio output \u2014 check the HDMI cable"

        with patch.object(player, "_find_splash", return_value=splash_img), \
             patch.object(player, "_teardown"), \
             patch.object(player, "_build_pipeline") as mock_build, \
             patch.object(player, "_update_current") as mock_update:
            mock_build.return_value = MagicMock()
            player._show_splash()
            mock_update.assert_called_once_with(
                mode=PlaybackMode.SPLASH,
                asset="default.png",
                error="No audio output \u2014 check the HDMI cable",
            )
            assert player._pending_error is None  # consumed

    def test_splash_clears_error_when_no_pending(self, player, tmp_path):
        """_show_splash should pass error=None when no pending error."""
        splash_img = tmp_path / "assets" / "splash" / "default.png"
        splash_img.parent.mkdir(parents=True)
        splash_img.touch()

        player._pending_error = None

        with patch.object(player, "_find_splash", return_value=splash_img), \
             patch.object(player, "_teardown"), \
             patch.object(player, "_build_pipeline") as mock_build, \
             patch.object(player, "_update_current") as mock_update:
            mock_build.return_value = MagicMock()
            player._show_splash()
            mock_update.assert_called_once_with(
                mode=PlaybackMode.SPLASH,
                asset="default.png",
                error=None,
            )

    def test_successful_playback_resets_backoff(self, player):
        """Pipeline reaching PLAYING should reset _error_retry_delay to 3."""
        with patch("player.service.Gst") as mock_gst:
            playing_state = MagicMock()
            playing_state.value_nick = "playing"
            mock_gst.State.PLAYING = playing_state

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, playing_state, None)
            player.pipeline = mock_pipeline
            player._error_retry_delay = 30  # Previously backed off
            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            mock_message = MagicMock()
            mock_message.src = mock_pipeline
            new_state = MagicMock()
            new_state.value_nick = "playing"
            new_state.__eq__ = lambda self, other: other == playing_state
            old_state = MagicMock()
            old_state.value_nick = "paused"
            mock_message.parse_state_changed.return_value = (old_state, new_state, None)

            with patch.object(player, "_update_current"):
                player._on_state_changed(None, mock_message)
                assert player._error_retry_delay == 3

    def test_splash_playing_does_not_reset_backoff(self, player):
        """Splash reaching PLAYING should NOT reset backoff delay."""
        with patch("player.service.Gst") as mock_gst:
            playing_state = MagicMock()
            playing_state.value_nick = "playing"
            mock_gst.State.PLAYING = playing_state

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, playing_state, None)
            player.pipeline = mock_pipeline
            player._error_retry_delay = 12  # Previously backed off
            player.current_desired = DesiredState(
                mode=PlaybackMode.SPLASH, loop=False
            )

            mock_message = MagicMock()
            mock_message.src = mock_pipeline
            new_state = MagicMock()
            new_state.value_nick = "playing"
            new_state.__eq__ = lambda self, other: other == playing_state
            old_state = MagicMock()
            old_state.value_nick = "paused"
            mock_message.parse_state_changed.return_value = (old_state, new_state, None)

            with patch.object(player, "_update_current"):
                player._on_state_changed(None, mock_message)
                assert player._error_retry_delay == 12  # NOT reset
