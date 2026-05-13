"""Tests for :mod:`os_updater.state`.

Covers the FSM legality table, atomic persistence, busy interlock, and
the ``transition`` side-effects (``IDLE`` clears dispatch fields,
``FAILED`` stamps ``last_failure_reason``).
"""

from __future__ import annotations

import json

import pytest

from os_updater.state import (
    BUSY_STATES,
    LEGAL_TRANSITIONS,
    SCHEMA_VERSION,
    TransitionError,
    UpdaterFSMState,
    UpdaterState,
    is_busy,
    load_state,
    save_state,
    transition,
)


class TestUpdaterStateModel:
    def test_defaults(self):
        s = UpdaterState()
        assert s.schema_version == SCHEMA_VERSION
        assert s.fsm is UpdaterFSMState.IDLE
        assert s.release_id is None
        assert s.target_version is None
        assert s.staging_dir is None
        assert s.last_failure_reason is None
        assert s.last_event_id == 0
        assert isinstance(s.updated_at, str) and s.updated_at.endswith("Z")

    def test_extra_keys_ignored(self):
        raw = json.dumps({"schema_version": 1, "future_field": 42})
        s = UpdaterState.model_validate_json(raw)
        assert s.schema_version == 1


class TestLoadSave:
    def test_load_returns_default_when_missing(self, tmp_path):
        s = load_state(tmp_path / "missing.json")
        assert s.fsm is UpdaterFSMState.IDLE
        assert s.last_event_id == 0

    def test_load_returns_default_on_corrupt_json(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("not json")
        s = load_state(path)
        assert s.fsm is UpdaterFSMState.IDLE

    def test_round_trip(self, tmp_path):
        path = tmp_path / "state.json"
        s = UpdaterState(
            fsm=UpdaterFSMState.DOWNLOADING,
            release_id="rel-1",
            target_version="1.2.3",
            staging_dir="/data/.update/staging/rel-1",
            last_event_id=7,
        )
        save_state(s, path)
        r = load_state(path)
        assert r.fsm is UpdaterFSMState.DOWNLOADING
        assert r.release_id == "rel-1"
        assert r.target_version == "1.2.3"
        assert r.staging_dir == "/data/.update/staging/rel-1"
        assert r.last_event_id == 7

    def test_save_creates_parent_dir(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "state.json"
        save_state(UpdaterState(), path)
        assert path.exists()

    def test_save_uses_atomic_rename(self, tmp_path, monkeypatch):
        """A torn write would leave the daemon with no state. ``save_state``
        must rename a tempfile into place."""
        path = tmp_path / "state.json"
        # Pre-populate so we can confirm the file isn't truncated mid-write.
        path.write_text('{"schema_version": 1, "fsm": "idle"}')

        import os as _os

        original_replace = _os.replace
        captured: list[tuple[str, str]] = []

        def spying_replace(src, dst):
            captured.append((str(src), str(dst)))
            return original_replace(src, dst)

        monkeypatch.setattr("os.replace", spying_replace)

        s = UpdaterState(last_event_id=42)
        save_state(s, path)
        assert any(dst == str(path) for _, dst in captured)
        assert load_state(path).last_event_id == 42


class TestTransition:
    def test_legal_transitions_table_covers_every_state(self):
        # If we add a new state to the enum, we MUST add it to the table —
        # otherwise transitioning out of it would raise with a misleading
        # "no legal targets" rather than a "you forgot to wire this up" error.
        assert set(LEGAL_TRANSITIONS.keys()) == set(UpdaterFSMState)

    def test_failed_reachable_from_every_non_terminal_state(self):
        for src in UpdaterFSMState:
            if src is UpdaterFSMState.FAILED:
                continue
            if src is UpdaterFSMState.IDLE:
                # Per plan: pre-admission rejections leave the FSM in IDLE.
                # IDLE -> FAILED is intentionally NOT legal.
                continue
            assert UpdaterFSMState.FAILED in LEGAL_TRANSITIONS[src], (
                f"{src.value} cannot fail; that breaks the orchestrator"
            )

    def test_idle_cannot_reach_failed(self):
        """The pre-admission rejection path leaves the FSM in IDLE — so
        IDLE -> FAILED must NOT be a legal transition (otherwise we'd be
        tempted to mutate the FSM for things like version_floor and lose
        the "FSM means in-flight" invariant)."""
        assert UpdaterFSMState.FAILED not in LEGAL_TRANSITIONS[UpdaterFSMState.IDLE]

    def test_happy_path_is_legal_end_to_end(self):
        s = UpdaterState()
        path = [
            UpdaterFSMState.DOWNLOADING,
            UpdaterFSMState.STAGED,
            UpdaterFSMState.TRYBOOT_PENDING,
            UpdaterFSMState.TRYBOOT_RUNNING,
            UpdaterFSMState.PROMOTED_PENDING_MIGRATION,
            UpdaterFSMState.MIGRATING,
            UpdaterFSMState.IDLE,
        ]
        for target in path:
            transition(s, target)
            assert s.fsm is target

    def test_illegal_transition_raises(self):
        s = UpdaterState()
        with pytest.raises(TransitionError):
            transition(s, UpdaterFSMState.MIGRATING)
        # State left unchanged on raise
        assert s.fsm is UpdaterFSMState.IDLE

    def test_transition_to_failed_stamps_reason(self):
        s = UpdaterState(fsm=UpdaterFSMState.DOWNLOADING)
        transition(s, UpdaterFSMState.FAILED, reason="signature_invalid")
        assert s.fsm is UpdaterFSMState.FAILED
        assert s.last_failure_reason == "signature_invalid"

    def test_transition_to_idle_clears_dispatch_fields(self):
        s = UpdaterState(
            fsm=UpdaterFSMState.MIGRATING,
            release_id="rel-1",
            target_version="1.2.3",
            staging_dir="/tmp/staging/rel-1",
            last_failure_reason="prior",
        )
        transition(s, UpdaterFSMState.IDLE)
        assert s.fsm is UpdaterFSMState.IDLE
        assert s.release_id is None
        assert s.target_version is None
        assert s.staging_dir is None
        assert s.last_failure_reason is None

    def test_failed_can_only_go_to_downloading(self):
        """FAILED is sticky until a new dispatch arrives, which restarts
        the chain at DOWNLOADING."""
        assert LEGAL_TRANSITIONS[UpdaterFSMState.FAILED] == frozenset(
            {UpdaterFSMState.DOWNLOADING}
        )


class TestIsBusy:
    def test_idle_not_busy(self):
        assert not is_busy(UpdaterState(fsm=UpdaterFSMState.IDLE))

    def test_failed_not_busy(self):
        assert not is_busy(UpdaterState(fsm=UpdaterFSMState.FAILED))

    def test_every_other_state_is_busy(self):
        for st in UpdaterFSMState:
            if st in (UpdaterFSMState.IDLE, UpdaterFSMState.FAILED):
                continue
            assert is_busy(UpdaterState(fsm=st)), f"{st.value} should be busy"

    def test_busy_states_set_matches_is_busy(self):
        # Belt-and-suspenders: BUSY_STATES is what plan #23 calls out as
        # the rejection set; is_busy should mirror it exactly.
        assert {st for st in UpdaterFSMState if is_busy(UpdaterState(fsm=st))} == set(
            BUSY_STATES
        )
