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
    """Verify _check_pipeline_health reports errors when pipeline fails to start."""

    def test_reports_error_and_recovers_when_not_playing(self, player, tmp_path):
        """Pipeline stuck in PAUSED should report an error and recover to splash."""
        with patch("player.service.Gst") as mock_gst, \
             patch("player.service.GLib") as mock_glib:
            mock_gst.State.PLAYING = "PLAYING"

            mock_state = MagicMock()
            mock_state.value_nick = "paused"

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, mock_state, None)
            player.pipeline = mock_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update, \
                 patch.object(player, "_show_splash"):
                player._check_pipeline_health("test.mp4")
                mock_update.assert_called_once()
                call_kwargs = mock_update.call_args[1]
                assert call_kwargs["error"] is not None
                assert "PLAYING" in call_kwargs["error"]
                # Should schedule splash recovery
                mock_glib.timeout_add_seconds.assert_called_once()

    def test_no_error_when_pipeline_is_playing(self, player, tmp_path):
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
