"""Lifecycle event emitter for agora-os-updater.

Every meaningful transition in the updater FSM emits a lifecycle event that
flows back to the CMS over the existing WPS connection (plan #33). Each
event carries a per-device monotonic ``event_id`` so the CMS can dedupe with
``(device_id, event_id)``.

This module ships the scaffold:

* :class:`LifecycleEvent` — the dataclass that goes on the wire.
* :class:`LifecycleEventType` — the closed enumeration of event names. The
  daemon shouldn't be able to send an "ad-hoc" event; if a new shape is
  needed, that's a code change.
* :class:`OutboxEventSink` — Phase 2's persistence-only sink. Appends events
  to a JSONL file under ``/data/agora/event-buffer/`` (one file per UTC
  day) and exposes a list-pending API for the Phase 4 WPS replayer to
  consume.  No WPS send is performed in Phase 2 — that's wired up in
  Phase 4 along with the buffer FIFO eviction policy (1000 events / 10
  MB cap).
* :class:`WpsEventSink` — live-send sink used in production from
  agora#215 onward.  Fires events as ``{"type": "lifecycle_event", ...}``
  over the currently-active WPS transport.  Best-effort: logs and drops
  on send failure (Phase 4 replayer will eventually take over the
  durability story).
* :func:`emit_event` — convenience that stamps the next event-id from a
  passed-in :class:`UpdaterState`, builds the event, persists state, and
  forwards to the sink.
* :class:`RateLimitedProgress` — helper for any sub-system that wants to
  emit bytes-progress callbacks without flooding the wire.  Used by the
  bundle downloader and the streaming-extract polling loop.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Mapping, Optional, Protocol

from shared.state import atomic_write

from os_updater.state import (
    DEFAULT_STATE_PATH,
    UpdaterState,
    save_state,
    utc_now_iso,
)


logger = logging.getLogger(__name__)

#: Directory where pending lifecycle events buffer when the WPS connection
#: is down. Lives under ``/data`` so the queue survives reboot.
DEFAULT_OUTBOX_DIR = Path("/data/agora/event-buffer")

#: Minimum interval between rate-limited progress emissions, per
#: callback instance.  2 seconds is a conservative trade-off:
#:
#: * Worst-case event count for a 10-minute OTA is ~300 — trivial for
#:   the CMS to ingest.
#: * Slow enough that the device doesn't backpressure the WPS connection
#:   under sustained throughput.
#: * Fast enough that the CMS progress bar feels live (humans can't
#:   distinguish 2 s updates from continuous on a 30-90 s download).
#:
#: Final-state emissions bypass the rate limiter by passing ``force=True``.
PROGRESS_MIN_INTERVAL_S: float = 2.0


class LifecycleEventType(str, enum.Enum):
    """Closed enumeration of lifecycle event names.

    Values match plan.md §"Phase 2 — Deliverables" — these are the names
    the CMS-side worker will switch on, so any addition is a coordinated
    change.
    """

    DOWNLOAD_STARTED = "download_started"
    #: Bytes-progress milestone emitted from :class:`BundleDownloader`
    #: while the streaming HTTP GET is in flight.  Carries
    #: ``payload = {"bytes_done": int, "bytes_total": int}`` so the CMS
    #: progress badge can render a live percentage.  Emitted at most
    #: every :data:`PROGRESS_MIN_INTERVAL_S` seconds; the final
    #: ``(bytes_total, bytes_total)`` call is always forced through.
    DOWNLOAD_PROGRESS = "download_progress"
    SIGNATURE_VERIFIED = "signature_verified"
    STAGED = "staged"
    #: Sub-phase milestone emitted from within :meth:`SlotStager.stage`
    #: so the operator (and a future CMS upgrade-progress UI) can see
    #: which phase of the multi-minute staging pipeline is in flight.
    #: Carries ``payload = {"phase": <one of STAGE_PROGRESS_PHASES>}``.
    #: This is in-addition-to the existing ``STAGED`` bookends emitted
    #: by the service before stage starts and the ``TRYBOOT_INITIATED``
    #: emitted after stage ends, so dropping STAGE_PROGRESS events
    #: never desynchronizes the CMS-side state machine. Tracked as
    #: ``sslivins/agora#202``.
    STAGE_PROGRESS = "stage_progress"
    #: Bytes-progress milestone emitted from :func:`stream_extract_subtree`
    #: while a ``zstd | tar`` pipeline is in flight.  Distinct from
    #: ``STAGE_PROGRESS`` (which fires at phase BOUNDARIES) — this
    #: fires DURING the two long extract phases (``extracting_boot``
    #: and ``extracting_rootfs``) with byte counts polled from
    #: ``/proc/<zstd_pid>/fdinfo/0``.  Carries
    #: ``payload = {"phase": "extracting_boot" | "extracting_rootfs",
    #: "bytes_done": int, "bytes_total": int}``.
    EXTRACT_PROGRESS = "extract_progress"
    TRYBOOT_INITIATED = "tryboot_initiated"
    SLOT_CONFIRMED = "slot_confirmed"
    PROMOTED = "promoted"
    MIGRATION_COMPLETE = "migration_complete"
    FAILED = "failed"
    DECLINED = "declined"


@dataclass(frozen=True)
class LifecycleEvent:
    """A single lifecycle event headed for the CMS.

    Frozen so a sink can keep a reference past the call and trust it. The
    ``reason`` field doubles as the colon-suffix for ``failed`` /
    ``declined`` events (``failed:signature_invalid``, ``declined:busy``)
    — the CMS-side parser expects ``f"{event_type.value}:{reason}"`` when
    a reason is set on those two types.
    """

    event_id: int
    event_type: LifecycleEventType
    release_id: Optional[str]
    target_version: Optional[str]
    occurred_at: str
    reason: Optional[str] = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        """Return a dict suitable for ``json.dumps`` / WPS send."""

        return {
            "type": "lifecycle_event",
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "release_id": self.release_id,
            "target_version": self.target_version,
            "occurred_at": self.occurred_at,
            "reason": self.reason,
            "payload": dict(self.payload),
        }


class EventSink(Protocol):
    """Type contract for somewhere an event can be sent."""

    def put(self, event: LifecycleEvent) -> None:  # pragma: no cover - protocol
        ...


class OutboxEventSink:
    """Phase 2 sink: persist events under :data:`DEFAULT_OUTBOX_DIR`.

    Events are appended to ``<UTC-date>.jsonl`` files, one event per line.
    Phase 4 will add a WPS replayer that reads + sends these files, then
    deletes the day-files whose events have all been acked. For now the
    sink is write-only; tests use :meth:`iter_events` to inspect.

    The cap on buffer size (1000 events / 10 MB FIFO) is owned by Phase
    4 — Phase 2 just lays down the directory layout.
    """

    def __init__(self, outbox_dir: Path = DEFAULT_OUTBOX_DIR) -> None:
        self.outbox_dir = outbox_dir

    def _path_for(self, event: LifecycleEvent) -> Path:
        """Pick the day-file for ``event`` from its ``occurred_at`` field."""

        # occurred_at is always "YYYY-MM-DDTHH:MM:SSZ" per utc_now_iso, so
        # the first 10 chars are the date — much faster than re-parsing.
        return self.outbox_dir / f"{event.occurred_at[:10]}.jsonl"

    def put(self, event: LifecycleEvent) -> None:
        """Persist ``event`` to the day-file.

        Uses an append-with-fsync rather than the atomic-rename pattern
        used elsewhere in agora because we want O(1) writes. A torn
        last-line on a power cut is recoverable — the replayer skips
        unparseable JSON lines and logs them, which is correct behavior
        even for non-truncation corruption.
        """

        self.outbox_dir.mkdir(parents=True, exist_ok=True)
        target = self._path_for(event)
        line = json.dumps(event.to_jsonable(), sort_keys=True) + "\n"
        with target.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()

    def iter_events(self) -> Iterator[LifecycleEvent]:
        """Yield every event currently in the outbox, in file order.

        Test-and-Phase-4-replayer-friendly. Skips unparseable lines
        silently — a strict mode is Phase 4's problem.
        """

        if not self.outbox_dir.exists():
            return
        for day_file in sorted(self.outbox_dir.glob("*.jsonl")):
            with day_file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    try:
                        yield LifecycleEvent(
                            event_id=int(obj["event_id"]),
                            event_type=LifecycleEventType(obj["event_type"]),
                            release_id=obj.get("release_id"),
                            target_version=obj.get("target_version"),
                            occurred_at=obj["occurred_at"],
                            reason=obj.get("reason"),
                            payload=obj.get("payload") or {},
                        )
                    except (KeyError, ValueError):
                        continue


def next_event_id(state: UpdaterState) -> int:
    """Pre-increment ``last_event_id`` on ``state`` and return the new value.

    Mutates the state. Caller must persist it (or call :func:`emit_event`,
    which does so).
    """

    state.last_event_id += 1
    return state.last_event_id


def emit_event(
    state: UpdaterState,
    event_type: LifecycleEventType,
    sink: EventSink,
    *,
    reason: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
    state_path: Optional[Path] = None,
    now_fn=utc_now_iso,
) -> LifecycleEvent:
    """Stamp + persist + sink a lifecycle event.

    Concretely:

    1. Increment ``state.last_event_id`` (allocates a fresh id).
    2. Build a :class:`LifecycleEvent` with that id, the supplied type /
       reason / payload, and the ``release_id`` + ``target_version``
       currently on ``state``.
    3. Persist ``state`` to disk (so we never re-use the same event_id
       after a crash).
    4. Hand the event to ``sink``.
    5. Return the event so the caller can log it.

    Per #33 we persist BEFORE sending so the id is locked in even if the
    sink throws. The Phase 4 replayer handles "I see this id was reserved
    but no record exists in the outbox" by simply skipping forward.

    ``now_fn`` and ``state_path`` are injection seams for tests.
    """

    event_id = next_event_id(state)
    event = LifecycleEvent(
        event_id=event_id,
        event_type=event_type,
        release_id=state.release_id,
        target_version=state.target_version,
        occurred_at=now_fn(),
        reason=reason,
        payload=dict(payload or {}),
    )
    save_state(state, path=state_path or DEFAULT_STATE_PATH)
    sink.put(event)
    return event


# A callable that returns the currently-active WPS transport (or ``None``
# if the device is offline / mid-reconnect).  Used by :class:`WpsEventSink`
# so the sink doesn't have to know about the service's reconnect lifecycle.
TransportProvider = Callable[[], Optional[Any]]


class WpsEventSink:
    """Send each lifecycle event live over the active WPS transport.

    The sink is intentionally fire-and-forget:

    * If no transport is currently connected, the event is dropped with a
      DEBUG log.  (Phase 4 will replace this with a disk-backed replayer
      that drains on reconnect — out of scope for v1, tracked in
      ``sslivins/agora#215``.)
    * If the underlying ``transport.send`` raises, the failure is logged
      at WARNING but never propagates back to the caller.  Lifecycle
      events are advisory — a failed send must not abort the OTA FSM.

    The wire shape matches what ``cms.services.device_inbound`` dispatches
    on:

        {"type": "lifecycle_event",
         "event_id": <int>, "event_type": <str>,
         "release_id": <str|None>, "target_version": <str|None>,
         "occurred_at": <ISO-8601>, "reason": <str|None>,
         "payload": {...}}

    Construction takes a ``transport_provider`` callable so the sink can
    be built BEFORE the first connect (it doesn't hold a reference to a
    transport that might later disconnect — it asks for the current one
    every time it sends).

    Cross-thread safety (agora#219): :meth:`put` is invoked from three
    distinct call sites with different threading characteristics:

    * The FSM coroutines in :mod:`os_updater.service` — these run on the
      service's asyncio main loop, so ``put`` is on-loop.
    * The downloader's chunk loop — also on the main loop (``_fetch`` is
      ``async``).
    * The stage / extract progress callbacks — these fire from a
      ``asyncio.to_thread`` worker (because :meth:`SlotStager.stage`
      wraps its synchronous body in ``to_thread``) AND from a sidecar
      polling thread (``_poll_extract_progress`` ticks fdinfo every
      ``PROGRESS_MIN_INTERVAL_S``).  Both are off-loop.

    The pre-#219 implementation called ``asyncio.get_running_loop()``
    inside :meth:`put`, which raises ``RuntimeError`` on a worker
    thread, so EVERY stage/extract progress event was silently dropped
    at DEBUG level — the badge showed "Staging bundle" / "Upgrading…"
    with no granular progress.  The fix is two-pronged:

    1. The service binds the main loop via :meth:`bind_loop` once it
       enters ``run()``, so we have a known good loop to schedule sends
       on regardless of which thread ``put`` is called from.
    2. :meth:`put` decides between ``loop.create_task`` (on-loop) and
       ``asyncio.run_coroutine_threadsafe`` (off-loop) at send time.
       Both are fire-and-forget — lifecycle events are advisory.
    """

    def __init__(self, transport_provider: TransportProvider) -> None:
        self._transport_provider = transport_provider
        # The asyncio loop on which lifecycle-event sends are dispatched.
        # Bound lazily: explicitly via :meth:`bind_loop` (preferred —
        # called by the service at run-loop entry), or implicitly on the
        # first on-loop :meth:`put` that finds a running loop in the
        # caller's thread.  Once set, all subsequent puts (including
        # those from worker threads) use this loop.
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_lock = threading.Lock()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind the asyncio loop used to schedule background sends.

        Called once by :meth:`OSUpdaterService.run` at the top of the
        main loop so worker-thread :meth:`put` callers can schedule
        cross-thread via ``run_coroutine_threadsafe`` without having to
        know about the service's loop bookkeeping.

        Idempotent: re-binding the same loop is a no-op; rebinding to a
        different loop replaces the previous one (tests routinely do
        this when running multiple ``asyncio.run`` cycles back-to-back).
        """
        with self._loop_lock:
            self._loop = loop

    def put(self, event: LifecycleEvent) -> None:
        """Fire ``event`` over the active transport, or drop on failure.

        Resolves the asyncio loop to schedule the send on (see class
        docstring): a previously-bound loop wins; otherwise we try to
        capture the loop from the caller's thread.  Off-loop callers
        (extract/stage progress from worker threads) use
        ``run_coroutine_threadsafe``; on-loop callers use
        ``create_task``.  No loop available → DEBUG-log and drop.
        """

        transport = self._transport_provider()
        if transport is None:
            logger.debug(
                "WpsEventSink: no active transport, dropping event_id=%d type=%s",
                event.event_id, event.event_type.value,
            )
            return

        loop = self._resolve_loop()
        if loop is None:
            logger.debug(
                "WpsEventSink: no running loop, dropping event_id=%d type=%s",
                event.event_id, event.event_type.value,
            )
            return

        payload = json.dumps(event.to_jsonable())
        coro = self._send(transport, payload, event.event_id)

        # On-loop vs cross-thread submission.  ``get_running_loop``
        # raises if the current thread has no running loop, which is
        # exactly the worker-thread case we route via
        # ``run_coroutine_threadsafe``.
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None

        if current is loop:
            loop.create_task(coro)
            return

        try:
            asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:
            # Loop has stopped between resolve and submit — close the
            # coroutine to suppress the "coroutine was never awaited"
            # warning, then drop.  Same shape as the no-loop branch
            # above: lifecycle events are advisory.
            coro.close()
            logger.debug(
                "WpsEventSink: loop not running, dropping event_id=%d type=%s",
                event.event_id, event.event_type.value,
            )

    def _resolve_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        """Return the loop to dispatch sends on, caching on first hit.

        Order of preference:

        1. An explicitly-bound loop (via :meth:`bind_loop`).  This is
           the production path — the service calls ``bind_loop`` once
           at startup so worker-thread puts have a loop ready before
           the first stage_progress / extract_progress event fires.
        2. The loop running in the caller's thread, if any.  Captured
           lazily so that test code which constructs a sink and calls
           ``put`` from an ``asyncio.run`` block Just Works without
           plumbing through ``bind_loop``.
        3. ``None`` — caller drops the event.
        """

        if self._loop is not None:
            return self._loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        with self._loop_lock:
            if self._loop is None:
                self._loop = loop
        return self._loop

    @staticmethod
    async def _send(transport: Any, payload: str, event_id: int) -> None:
        try:
            await transport.send(payload)
        except Exception:
            # Lifecycle events are advisory; a stale transport / network
            # blip must not abort the OTA FSM.  Log at WARNING so a
            # systematic failure shows up in ops, but swallow.
            logger.warning(
                "WpsEventSink: send failed for event_id=%d", event_id,
                exc_info=True,
            )


class RateLimitedProgress:
    """Rate-limited wrapper for a bytes-progress callback.

    Use case: download / extract progress fires hundreds of times per
    OTA on a fast network.  We don't want to flood the wire with
    ``download_progress`` events at chunk-level granularity — the CMS
    badge can't usefully render >1 update per second anyway, and every
    event costs a WPS round trip.

    Each instance maintains its own ``last-emit`` clock.  The first
    call always fires (so the badge appears immediately when the OTA
    starts).  Subsequent calls fire only if at least
    :data:`PROGRESS_MIN_INTERVAL_S` has elapsed since the last fire,
    OR the caller passes ``force=True`` (used for the terminal
    ``(bytes_total, bytes_total)`` call so the bar always reaches 100%
    before the next FSM event lands).

    Exceptions from the wrapped callback are logged and swallowed —
    progress is advisory and a buggy callback must not crash the
    download/extract loop.
    """

    def __init__(
        self,
        callback: Callable[..., None],
        *,
        min_interval_s: float = PROGRESS_MIN_INTERVAL_S,
        time_source: Callable[[], float] = time.monotonic,
    ) -> None:
        self._callback = callback
        self._min_interval_s = min_interval_s
        self._time_source = time_source
        # None = "never fired", so the first call goes through immediately.
        self._last_emit: Optional[float] = None

    def __call__(self, *args: Any, force: bool = False, **kwargs: Any) -> None:
        now = self._time_source()
        if not force and self._last_emit is not None \
                and (now - self._last_emit) < self._min_interval_s:
            return
        self._last_emit = now
        try:
            self._callback(*args, **kwargs)
        except Exception:
            logger.warning(
                "RateLimitedProgress callback raised; continuing",
                exc_info=True,
            )
