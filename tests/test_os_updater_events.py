"""Tests for :mod:`os_updater.events` — the lifecycle event scaffold.

Acceptance hooks (plan §"Phase 2 — Deliverables" + plan #33):

* Monotonic ``event_id`` across emissions.
* Persistence across "daemon restart" (re-load state, ids don't repeat).
* JSONL day-file layout under :data:`DEFAULT_OUTBOX_DIR`.
* ``iter_events`` round-trips events that were written.
* Malformed lines are skipped, not raised on.
* ``emit_event`` stamps release_id / target_version from state.

Plus agora#215 additions (download/extract progress, live WPS sink):

* :class:`WpsEventSink` drops when no transport, swallows send errors.
* :class:`RateLimitedProgress` first-call fires, rate-limits, force bypass.
* :class:`LifecycleEventType.DOWNLOAD_PROGRESS` / ``EXTRACT_PROGRESS``
  wire values are stable.
"""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

import pytest

from os_updater.events import (
    PROGRESS_MIN_INTERVAL_S,
    LifecycleEvent,
    LifecycleEventType,
    OutboxEventSink,
    RateLimitedProgress,
    WpsEventSink,
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


# --- agora#215: progress events + WPS live sink -----------------------------


class TestProgressEnumValues:
    """Wire-format pinning for the two new progress event types
    (agora#215). The CMS-side parser in ``cms/services/device_inbound.py``
    switches on these string values; any rename here is a coordinated
    schema migration."""

    def test_download_progress_value(self):
        assert LifecycleEventType.DOWNLOAD_PROGRESS.value == "download_progress"

    def test_extract_progress_value(self):
        assert LifecycleEventType.EXTRACT_PROGRESS.value == "extract_progress"

    def test_progress_min_interval_is_2_seconds(self):
        # Test pins the default cadence the CMS UX was designed against.
        assert PROGRESS_MIN_INTERVAL_S == 2.0


class _FakeClock:
    """Manually advanceable monotonic-time source for RateLimitedProgress."""

    def __init__(self) -> None:
        self.now: float = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class TestRateLimitedProgress:
    def test_first_call_always_fires(self):
        calls: list[tuple] = []
        rl = RateLimitedProgress(lambda *a: calls.append(a), time_source=_FakeClock())
        rl(10, 100)
        assert calls == [(10, 100)]

    def test_second_call_within_interval_drops(self):
        calls: list[tuple] = []
        clock = _FakeClock()
        rl = RateLimitedProgress(lambda *a: calls.append(a), time_source=clock)
        rl(10, 100)
        clock.advance(0.5)
        rl(20, 100)
        assert calls == [(10, 100)]

    def test_call_after_interval_fires(self):
        calls: list[tuple] = []
        clock = _FakeClock()
        rl = RateLimitedProgress(lambda *a: calls.append(a), time_source=clock)
        rl(10, 100)
        clock.advance(PROGRESS_MIN_INTERVAL_S + 0.01)
        rl(20, 100)
        assert calls == [(10, 100), (20, 100)]

    def test_force_bypasses_rate_limit(self):
        calls: list[tuple] = []
        clock = _FakeClock()
        rl = RateLimitedProgress(lambda *a: calls.append(a), time_source=clock)
        rl(10, 100)
        clock.advance(0.1)
        rl(100, 100, force=True)
        assert calls == [(10, 100), (100, 100)]

    def test_force_resets_clock_for_subsequent_calls(self):
        """After a force call, the rate-limit window restarts from that
        emit so the next non-force still has to wait PROGRESS_MIN_INTERVAL_S."""
        calls: list[tuple] = []
        clock = _FakeClock()
        rl = RateLimitedProgress(lambda *a: calls.append(a), time_source=clock)
        rl(50, 100, force=True)
        clock.advance(0.1)
        rl(60, 100)
        assert calls == [(50, 100)]

    def test_callback_exception_is_swallowed(self):
        def boom(*a, **kw):
            raise RuntimeError("intentional")

        rl = RateLimitedProgress(boom, time_source=_FakeClock())
        rl(10, 100)  # must not raise


class _FakeTransport:
    """Captures every payload sent over the transport."""

    def __init__(self, raise_on_send: bool = False) -> None:
        self.sent: list[str] = []
        self.raise_on_send = raise_on_send

    async def send(self, payload: str) -> None:
        if self.raise_on_send:
            raise RuntimeError("transport-dead")
        self.sent.append(payload)


def _drain_pending(loop: asyncio.AbstractEventLoop) -> None:
    """Run the loop briefly so any pending ``create_task`` calls complete."""
    loop.run_until_complete(asyncio.sleep(0))
    loop.run_until_complete(asyncio.sleep(0))


def _make_event(event_id: int = 1, event_type=LifecycleEventType.DOWNLOAD_PROGRESS) -> LifecycleEvent:
    return LifecycleEvent(
        event_id=event_id,
        event_type=event_type,
        release_id="rel-1",
        target_version="1.0.0",
        occurred_at="2026-05-07T03:30:00Z",
        payload={"bytes_done": 10, "bytes_total": 100},
    )


class TestWpsEventSink:
    def test_drops_when_no_transport(self):
        sink = WpsEventSink(transport_provider=lambda: None)
        # Should NOT raise even though there is no loop and no transport.
        sink.put(_make_event())

    def test_drops_when_no_running_loop(self):
        # Transport is present but no loop is running — must not raise.
        sink = WpsEventSink(transport_provider=lambda: _FakeTransport())
        sink.put(_make_event())

    def test_fires_send_on_active_transport(self):
        transport = _FakeTransport()
        sink = WpsEventSink(transport_provider=lambda: transport)

        async def go():
            sink.put(_make_event())
            # Yield so the create_task'd send actually runs.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(go())

        assert len(transport.sent) == 1
        decoded = json.loads(transport.sent[0])
        assert decoded["type"] == "lifecycle_event"
        assert decoded["event_type"] == "download_progress"
        assert decoded["event_id"] == 1
        assert decoded["release_id"] == "rel-1"
        assert decoded["payload"] == {"bytes_done": 10, "bytes_total": 100}

    def test_swallows_send_exception(self):
        transport = _FakeTransport(raise_on_send=True)
        sink = WpsEventSink(transport_provider=lambda: transport)

        async def go():
            sink.put(_make_event())
            # Two yields so the failing send task actually runs through
            # its except branch.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        # Must not propagate — lifecycle events are advisory.
        asyncio.run(go())

    def test_reads_transport_lazily_each_put(self):
        """The sink must re-query the provider on every put so a
        post-reconnect transport replacement Just Works."""
        # State: current transport, mutated between puts to simulate
        # a reconnect cycle (first put sees None -> drops; second put
        # sees a live transport -> sends).
        current: dict[str, Any] = {"t": None}

        def provider():
            return current["t"]

        sink = WpsEventSink(transport_provider=provider)

        async def go():
            sink.put(_make_event(1))
            await asyncio.sleep(0)
            current["t"] = _FakeTransport()
            sink.put(_make_event(2))
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(go())

        transport = current["t"]
        assert len(transport.sent) == 1
        decoded = json.loads(transport.sent[0])
        assert decoded["event_id"] == 2


class TestWpsEventSinkCrossThread:
    """Regression coverage for agora#219: ``WpsEventSink.put`` must work
    when called from a worker thread (not just the asyncio main loop).

    The original implementation called ``asyncio.get_running_loop`` in
    ``put`` itself, which raises ``RuntimeError`` on a worker thread.
    Result: every ``extract_progress`` / ``stage_progress`` event was
    silently dropped at DEBUG level because those callbacks fire from
    the ``asyncio.to_thread`` worker that runs ``SlotStager._stage_sync``
    and the sidecar fdinfo poll thread.  See ``WpsEventSink.put`` for
    the cross-thread scheduling fix.
    """

    def test_bind_loop_then_put_from_worker_thread(self):
        transport = _FakeTransport()
        sink = WpsEventSink(transport_provider=lambda: transport)

        async def go():
            sink.bind_loop(asyncio.get_running_loop())

            done = threading.Event()

            def worker():
                # Off-loop call site -- mirrors the extract-poller
                # thread.  Must not raise and must schedule the send
                # via run_coroutine_threadsafe.
                sink.put(_make_event(event_id=42))
                done.set()

            t = threading.Thread(target=worker)
            t.start()
            t.join(timeout=2.0)
            assert done.is_set()
            # Yield enough times for the threadsafe-scheduled task to
            # actually run through ``await transport.send``.
            for _ in range(5):
                await asyncio.sleep(0)

        asyncio.run(go())

        assert len(transport.sent) == 1
        decoded = json.loads(transport.sent[0])
        assert decoded["event_id"] == 42

    def test_put_from_worker_thread_without_explicit_bind_loop(self):
        """If the caller never calls ``bind_loop`` but DOES call ``put``
        from the main loop at least once before any worker-thread call,
        the lazy capture in ``_resolve_loop`` should kick in and make
        subsequent worker-thread puts work.
        """
        transport = _FakeTransport()
        sink = WpsEventSink(transport_provider=lambda: transport)

        async def go():
            # First put: on-loop, captures the loop lazily.
            sink.put(_make_event(event_id=1))
            await asyncio.sleep(0)

            # Second put: from a worker thread.  Must use the loop
            # captured in the first call.
            done = threading.Event()

            def worker():
                sink.put(_make_event(event_id=2))
                done.set()

            t = threading.Thread(target=worker)
            t.start()
            t.join(timeout=2.0)
            assert done.is_set()
            for _ in range(5):
                await asyncio.sleep(0)

        asyncio.run(go())

        assert len(transport.sent) == 2
        ids = sorted(json.loads(p)["event_id"] for p in transport.sent)
        assert ids == [1, 2]

    def test_worker_thread_put_with_no_bound_loop_drops(self):
        """If the sink has never seen a loop AND the worker thread
        doesn't have a running loop, the event is dropped (DEBUG log)
        without raising.  This is the same defensive behavior as the
        pre-#219 ``put`` -- we just guarantee it for worker-thread
        callers too.
        """
        transport = _FakeTransport()
        sink = WpsEventSink(transport_provider=lambda: transport)

        done = threading.Event()
        err: list[BaseException] = []

        def worker():
            try:
                sink.put(_make_event(event_id=99))
            except BaseException as exc:  # pragma: no cover - defensive
                err.append(exc)
            done.set()

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=2.0)
        assert done.is_set()
        assert err == []
        assert transport.sent == []

    def test_bind_loop_idempotent_for_same_loop(self):
        sink = WpsEventSink(transport_provider=lambda: None)

        async def go():
            loop = asyncio.get_running_loop()
            sink.bind_loop(loop)
            sink.bind_loop(loop)  # second bind same loop -- no-op
            return sink._loop is loop

        assert asyncio.run(go()) is True

    def test_rebind_to_new_loop_replaces_previous(self):
        """Tests that run multiple ``asyncio.run`` cycles back-to-back
        get a fresh loop each time.  ``bind_loop`` MUST accept the new
        loop; otherwise a stale-loop-pointer would cause every
        subsequent run-cycle's worker-thread puts to drop.
        """
        sink = WpsEventSink(transport_provider=lambda: None)

        async def first():
            sink.bind_loop(asyncio.get_running_loop())
            return id(asyncio.get_running_loop())

        async def second():
            sink.bind_loop(asyncio.get_running_loop())
            return id(asyncio.get_running_loop())

        first_id = asyncio.run(first())
        second_id = asyncio.run(second())
        # Sanity: two ``asyncio.run`` cycles -> two distinct loops.
        assert first_id != second_id
        # And bind_loop tracked the rebind.
        assert id(sink._loop) == second_id
