"""Tests for player service — pipeline selection logic."""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

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

    def test_reports_error_when_not_playing(self, player, tmp_path):
        """Pipeline stuck in PAUSED should report an error."""
        with patch("player.service.Gst") as mock_gst:
            mock_gst.State.PLAYING = "PLAYING"

            mock_state = MagicMock()
            mock_state.value_nick = "paused"

            mock_pipeline = MagicMock()
            mock_pipeline.get_state.return_value = (None, mock_state, None)
            player.pipeline = mock_pipeline

            player.current_desired = DesiredState(
                mode=PlaybackMode.PLAY, asset="test.mp4", loop=True
            )

            with patch.object(player, "_update_current") as mock_update:
                player._check_pipeline_health("test.mp4")
                mock_update.assert_called_once()
                call_kwargs = mock_update.call_args[1]
                assert call_kwargs["error"] is not None
                assert "PLAYING" in call_kwargs["error"]

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
