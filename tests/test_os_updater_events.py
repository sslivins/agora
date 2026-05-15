"""Tests for :mod:`os_updater.events` — the lifecycle event scaffold.

Acceptance hooks (plan §"Phase 2 — Deliverables" + plan #33):

* Monotonic ``event_id`` across emissions.
* Persistence across "daemon restart" (re-load state, ids don't repeat).
* JSONL day-file layout under :data:`DEFAULT_OUTBOX_DIR`.
* ``iter_events`` round-trips events that were written.
* Malformed lines are skipped, not raised on.
* ``emit_event`` stamps release_id / target_version from state.
"""

from __future__ import annotations

import json
from pathlib import Path

from os_updater.events import (
    LifecycleEvent,
    LifecycleEventType,
    OutboxEventSink,
    emit_event,
    next_event_id,
)
from os_updater.state import (
    UpdaterFSMState,
    UpdaterState,
    load_state,
)


class _FixedNow:
    """``now_fn`` substitute that returns the same Zulu timestamp every
    time. ``OutboxEventSink._path_for`` slices the first 10 chars off the
    timestamp to pick a day-file, so any 10-char-prefix date works.
    """

    def __init__(self, ts: str = "2026-05-07T03:30:00Z") -> None:
        self.ts = ts

    def __call__(self) -> str:
        return self.ts


class _ListSink:
    """In-memory EventSink — keeps every event so tests can assert."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def put(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class TestNextEventId:
    def test_increments_from_zero(self):
        s = UpdaterState()
        assert next_event_id(s) == 1
        assert next_event_id(s) == 2
        assert next_event_id(s) == 3
        assert s.last_event_id == 3

    def test_resumes_from_persisted_counter(self):
        s = UpdaterState(last_event_id=42)
        assert next_event_id(s) == 43


class TestEmitEvent:
    def test_stamps_id_release_and_target_from_state(self, tmp_path):
        state_path = tmp_path / "state.json"
        s = UpdaterState(
            fsm=UpdaterFSMState.DOWNLOADING,
            release_id="rel-1",
            target_version="1.2.3",
        )
        sink = _ListSink()

        event = emit_event(
            s,
            LifecycleEventType.DOWNLOAD_STARTED,
            sink,
            state_path=state_path,
            now_fn=_FixedNow(),
        )

        assert event.event_id == 1
        assert event.event_type is LifecycleEventType.DOWNLOAD_STARTED
        assert event.release_id == "rel-1"
        assert event.target_version == "1.2.3"
        assert event.occurred_at == "2026-05-07T03:30:00Z"
        assert event.reason is None
        assert event.payload == {}
        assert sink.events == [event]

    def test_persists_event_id_before_sink_put(self, tmp_path):
        state_path = tmp_path / "state.json"
        s = UpdaterState(release_id="rel-1", target_version="1.2.3")

        class _BoomSink:
            def put(self, event):
                raise RuntimeError("sink down")

        # emit_event raises through the sink failure, but state is already
        # persisted with last_event_id=1 — so on retry we don't re-use the id.
        try:
            emit_event(
                s,
                LifecycleEventType.DOWNLOAD_STARTED,
                _BoomSink(),
                state_path=state_path,
                now_fn=_FixedNow(),
            )
        except RuntimeError:
            pass

        reloaded = load_state(state_path)
        assert reloaded.last_event_id == 1

    def test_failed_event_carries_reason_and_payload(self, tmp_path):
        s = UpdaterState(release_id="rel-x", target_version="1.0.0")
        sink = _ListSink()
        emit_event(
            s,
            LifecycleEventType.FAILED,
            sink,
            reason="signature_invalid",
            payload={"detail": "bad sig"},
            state_path=tmp_path / "state.json",
            now_fn=_FixedNow(),
        )
        ev = sink.events[0]
        assert ev.reason == "signature_invalid"
        assert ev.payload == {"detail": "bad sig"}

    def test_monotonic_ids_across_emissions(self, tmp_path):
        s = UpdaterState()
        sink = _ListSink()
        for _ in range(5):
            emit_event(
                s,
                LifecycleEventType.DOWNLOAD_STARTED,
                sink,
                state_path=tmp_path / "state.json",
                now_fn=_FixedNow(),
            )
        ids = [ev.event_id for ev in sink.events]
        assert ids == [1, 2, 3, 4, 5]

    def test_ids_survive_simulated_restart(self, tmp_path):
        state_path = tmp_path / "state.json"
        s = UpdaterState()
        sink = _ListSink()
        emit_event(
            s,
            LifecycleEventType.DOWNLOAD_STARTED,
            sink,
            state_path=state_path,
            now_fn=_FixedNow(),
        )
        emit_event(
            s,
            LifecycleEventType.STAGED,
            sink,
            state_path=state_path,
            now_fn=_FixedNow(),
        )
        # Simulate daemon restart: reload state from disk, emit more.
        s2 = load_state(state_path)
        emit_event(
            s2,
            LifecycleEventType.PROMOTED,
            sink,
            state_path=state_path,
            now_fn=_FixedNow(),
        )
        ids = [ev.event_id for ev in sink.events]
        assert ids == [1, 2, 3]


class TestOutboxEventSink:
    def test_writes_to_day_file(self, tmp_path):
        outbox = tmp_path / "buffer"
        sink = OutboxEventSink(outbox_dir=outbox)
        ev = LifecycleEvent(
            event_id=1,
            event_type=LifecycleEventType.DOWNLOAD_STARTED,
            release_id="rel-1",
            target_version="1.0.0",
            occurred_at="2026-05-07T03:30:00Z",
        )
        sink.put(ev)
        day_file = outbox / "2026-05-07.jsonl"
        assert day_file.exists()
        line = day_file.read_text().strip()
        obj = json.loads(line)
        assert obj["event_type"] == "download_started"
        assert obj["event_id"] == 1

    def test_iter_events_round_trip(self, tmp_path):
        outbox = tmp_path / "buffer"
        sink = OutboxEventSink(outbox_dir=outbox)
        events = [
            LifecycleEvent(
                event_id=i,
                event_type=LifecycleEventType.STAGED,
                release_id="rel-1",
                target_version="1.0.0",
                occurred_at="2026-05-07T03:30:00Z",
            )
            for i in range(1, 4)
        ]
        for ev in events:
            sink.put(ev)
        round_tripped = list(sink.iter_events())
        assert [ev.event_id for ev in round_tripped] == [1, 2, 3]
        assert all(ev.event_type is LifecycleEventType.STAGED for ev in round_tripped)

    def test_iter_events_skips_malformed_lines(self, tmp_path):
        outbox = tmp_path / "buffer"
        outbox.mkdir(parents=True)
        day_file = outbox / "2026-05-07.jsonl"
        day_file.write_text(
            "\n".join(
                [
                    "not json",
                    json.dumps(
                        {
                            "event_id": 1,
                            "event_type": "download_started",
                            "release_id": "rel-1",
                            "target_version": "1.0.0",
                            "occurred_at": "2026-05-07T03:30:00Z",
                            "reason": None,
                            "payload": {},
                        }
                    ),
                    "",  # blank line — also skipped
                    "{}",  # parseable but missing keys
                ]
            )
            + "\n"
        )
        sink = OutboxEventSink(outbox_dir=outbox)
        evs = list(sink.iter_events())
        assert len(evs) == 1
        assert evs[0].event_id == 1

    def test_iter_events_when_outbox_missing(self, tmp_path):
        sink = OutboxEventSink(outbox_dir=tmp_path / "does-not-exist")
        assert list(sink.iter_events()) == []

    def test_events_from_different_days_in_separate_files(self, tmp_path):
        outbox = tmp_path / "buffer"
        sink = OutboxEventSink(outbox_dir=outbox)
        sink.put(
            LifecycleEvent(
                event_id=1,
                event_type=LifecycleEventType.STAGED,
                release_id="r",
                target_version="1.0.0",
                occurred_at="2026-05-07T23:59:00Z",
            )
        )
        sink.put(
            LifecycleEvent(
                event_id=2,
                event_type=LifecycleEventType.STAGED,
                release_id="r",
                target_version="1.0.0",
                occurred_at="2026-05-08T00:01:00Z",
            )
        )
        assert (outbox / "2026-05-07.jsonl").exists()
        assert (outbox / "2026-05-08.jsonl").exists()



class TestStageProgressEnum:
    """Pins the public contract that the STAGE_PROGRESS enum value
    is stable (agora#202). CMS dispatches lifecycle-event handlers off
    this string; renaming it without coordinating the CMS change would
    silently lose progress events."""

    def test_enum_value_is_stable_wire_format(self):
        assert LifecycleEventType.STAGE_PROGRESS.value == "stage_progress"

    def test_event_round_trips_through_sink_with_phase_payload(self, tmp_path):
        """emit_event must accept payload={'phase': ...} and the sink
        must receive a LifecycleEvent whose payload preserves it. This
        is the exact shape the service-side closure produces."""
        state_path = tmp_path / "state.json"
        s = UpdaterState(release_id="rel-1", target_version="1.0.0")
        sink = _ListSink()

        event = emit_event(
            s,
            LifecycleEventType.STAGE_PROGRESS,
            sink,
            payload={"phase": "extracting_rootfs"},
            state_path=state_path,
            now_fn=_FixedNow(),
        )

        assert event.event_type is LifecycleEventType.STAGE_PROGRESS
        assert event.payload == {"phase": "extracting_rootfs"}
        assert sink.events == [event]
