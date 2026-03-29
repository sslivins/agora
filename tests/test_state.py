"""Tests for state file I/O."""

from pathlib import Path

from shared.models import CurrentState, DesiredState, PlaybackMode
from shared.state import atomic_write, read_state, write_state


class TestAtomicWrite:
    def test_creates_file(self, tmp_path):
        path = tmp_path / "test.json"
        atomic_write(path, '{"key": "value"}')
        assert path.exists()
        assert path.read_text() == '{"key": "value"}'

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "file.json"
        atomic_write(path, "data")
        assert path.read_text() == "data"

    def test_overwrites_existing(self, tmp_path):
        path = tmp_path / "test.json"
        atomic_write(path, "first")
        atomic_write(path, "second")
        assert path.read_text() == "second"

    def test_no_temp_files_on_success(self, tmp_path):
        path = tmp_path / "test.json"
        atomic_write(path, "data")
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert files[0].name == "test.json"


class TestReadState:
    def test_reads_valid_state(self, tmp_path):
        path = tmp_path / "desired.json"
        path.write_text('{"mode": "play", "asset": "video.mp4", "loop": true}')
        state = read_state(path, DesiredState)
        assert state.mode == PlaybackMode.PLAY
        assert state.asset == "video.mp4"
        assert state.loop is True

    def test_returns_default_on_missing(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        state = read_state(path, DesiredState)
        assert state.mode == PlaybackMode.SPLASH
        assert state.asset is None

    def test_returns_default_on_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json at all")
        state = read_state(path, DesiredState)
        assert state.mode == PlaybackMode.SPLASH

    def test_reads_current_state(self, tmp_path):
        path = tmp_path / "current.json"
        path.write_text('{"mode": "stop", "pipeline_state": "NULL"}')
        state = read_state(path, CurrentState)
        assert state.mode == PlaybackMode.STOP


class TestWriteState:
    def test_writes_and_reads_roundtrip(self, tmp_path):
        path = tmp_path / "state.json"
        original = DesiredState(mode=PlaybackMode.PLAY, asset="test.mp4", loop=True)
        write_state(path, original)

        restored = read_state(path, DesiredState)
        assert restored.mode == original.mode
        assert restored.asset == original.asset
        assert restored.loop == original.loop

    def test_json_is_indented(self, tmp_path):
        path = tmp_path / "state.json"
        write_state(path, DesiredState())
        content = path.read_text()
        assert "\n" in content  # indented = multi-line
