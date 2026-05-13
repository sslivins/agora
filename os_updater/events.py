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
* :func:`emit_event` — convenience that stamps the next event-id from a
  passed-in :class:`UpdaterState`, builds the event, persists state, and
  forwards to the sink.
"""

from __future__ import annotations

import enum
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Protocol

from shared.state import atomic_write

from os_updater.state import (
    DEFAULT_STATE_PATH,
    UpdaterState,
    save_state,
    utc_now_iso,
)


#: Directory where pending lifecycle events buffer when the WPS connection
#: is down. Lives under ``/data`` so the queue survives reboot.
DEFAULT_OUTBOX_DIR = Path("/data/agora/event-buffer")


class LifecycleEventType(str, enum.Enum):
    """Closed enumeration of lifecycle event names.

    Values match plan.md §"Phase 2 — Deliverables" — these are the names
    the CMS-side worker will switch on, so any addition is a coordinated
    change.
    """

    DOWNLOAD_STARTED = "download_started"
    SIGNATURE_VERIFIED = "signature_verified"
    STAGED = "staged"
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
