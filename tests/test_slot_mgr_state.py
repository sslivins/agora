"""Unit tests for ``slot_mgr.state``: SlotState model + persistence."""

from __future__ import annotations

import json

from slot_mgr.state import SCHEMA_VERSION, SlotState, load_state, save_state


class TestSlotStateModel:
    def test_defaults(self):
        state = SlotState()
        assert state.schema_version == SCHEMA_VERSION
        assert state.strikes == {"1": 0, "2": 0}
        assert state.last_tryboot_target is None
        assert state.last_tryboot_at is None
        assert state.last_success_at is None
        assert state.pinned is False
        assert state.pinned_reason is None

    def test_get_set_strikes(self):
        state = SlotState()
        state.set_strikes(1, 2)
        assert state.get_strikes(1) == 2
        assert state.get_strikes(2) == 0
        state.set_strikes(2, 1)
        assert state.get_strikes(2) == 1

    def test_extra_keys_ignored_on_load(self):
        """A future schema-version with extra keys must round-trip through
        a current-schema reader without raising."""
        raw = json.dumps(
            {
                "schema_version": 1,
                "strikes": {"1": 0, "2": 0},
                "future_field": "ignored",
            }
        )
        state = SlotState.model_validate_json(raw)
        assert state.schema_version == 1


class TestLoadSave:
    def test_load_returns_default_when_file_missing(self, tmp_path):
        missing = tmp_path / "does-not-exist.json"
        state = load_state(missing)
        assert state.strikes == {"1": 0, "2": 0}
        assert not state.pinned

    def test_load_returns_default_on_corrupt_json(self, tmp_path):
        path = tmp_path / "slot-state.json"
        path.write_text("not json")
        state = load_state(path)
        assert state.strikes == {"1": 0, "2": 0}

    def test_save_then_load_round_trip(self, tmp_path):
        path = tmp_path / "slot-state.json"
        state = SlotState()
        state.set_strikes(2, 1)
        state.last_tryboot_target = 2
        state.last_tryboot_at = "2026-05-13T03:32:00Z"
        save_state(state, path)

        reloaded = load_state(path)
        assert reloaded.get_strikes(2) == 1
        assert reloaded.last_tryboot_target == 2
        assert reloaded.last_tryboot_at == "2026-05-13T03:32:00Z"

    def test_save_is_atomic_uses_rename(self, tmp_path, monkeypatch):
        """Verify save_state writes via a tempfile (no partial file on crash)."""
        path = tmp_path / "slot-state.json"
        # Pre-populate so we can verify it's not truncated during save.
        path.write_text('{"schema_version": 1, "strikes": {"1": 5, "2": 5}}')

        original_replace = __import__("os").replace
        captured = []

        def spying_replace(src, dst):
            captured.append((str(src), str(dst)))
            return original_replace(src, dst)

        monkeypatch.setattr("os.replace", spying_replace)

        state = SlotState()
        state.set_strikes(1, 7)
        save_state(state, path)

        # The replace was used (atomic rename), and the temp file landed at
        # the right path
        assert any(dst == str(path) for _, dst in captured)
        assert load_state(path).get_strikes(1) == 7

    def test_save_creates_parent_directory(self, tmp_path):
        """If /data/agora/ doesn't exist yet, save should create it."""
        path = tmp_path / "new-subdir" / "slot-state.json"
        state = SlotState()
        save_state(state, path)
        assert path.exists()
