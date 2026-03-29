"""Tests for shared models."""

from datetime import datetime, timezone

from shared.models import (
    AssetInfo,
    CurrentState,
    DesiredState,
    HealthResponse,
    PlaybackMode,
    PlayRequest,
    StatusResponse,
)


class TestPlaybackMode:
    def test_values(self):
        assert PlaybackMode.PLAY == "play"
        assert PlaybackMode.STOP == "stop"
        assert PlaybackMode.SPLASH == "splash"


class TestDesiredState:
    def test_defaults(self):
        state = DesiredState()
        assert state.mode == PlaybackMode.SPLASH
        assert state.asset is None
        assert state.loop is False
        assert state.timestamp is not None

    def test_play_state(self):
        state = DesiredState(mode=PlaybackMode.PLAY, asset="video.mp4", loop=True)
        assert state.mode == PlaybackMode.PLAY
        assert state.asset == "video.mp4"
        assert state.loop is True

    def test_serialization(self):
        state = DesiredState(mode=PlaybackMode.STOP)
        data = state.model_dump(mode="json")
        assert data["mode"] == "stop"

    def test_roundtrip(self):
        state = DesiredState(mode=PlaybackMode.PLAY, asset="test.mp4", loop=True)
        json_str = state.model_dump_json()
        restored = DesiredState.model_validate_json(json_str)
        assert restored.mode == state.mode
        assert restored.asset == state.asset
        assert restored.loop == state.loop


class TestCurrentState:
    def test_defaults(self):
        state = CurrentState()
        assert state.mode == PlaybackMode.SPLASH
        assert state.pipeline_state == "NULL"
        assert state.error is None

    def test_with_error(self):
        state = CurrentState(error="Pipeline crashed")
        assert state.error == "Pipeline crashed"


class TestAssetInfo:
    def test_creation(self):
        info = AssetInfo(
            name="video.mp4",
            size=1234567,
            modified_at=datetime.now(timezone.utc),
            asset_type="video",
        )
        assert info.name == "video.mp4"
        assert info.asset_type == "video"


class TestPlayRequest:
    def test_defaults(self):
        req = PlayRequest(asset="video.mp4")
        assert req.loop is False

    def test_with_loop(self):
        req = PlayRequest(asset="video.mp4", loop=True)
        assert req.loop is True
