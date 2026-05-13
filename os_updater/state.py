"""Persistent updater state: ``/data/agora/updater-state.json``.

The agora-os-updater daemon is a finite state machine. Persisting the state
on every transition lets the daemon recover after a crash, daemon restart, or
even a system reboot mid-update (the post-tryboot reboot is the normal case —
we come back up in ``tryboot_running`` and slot-confirm decides what to do).

The 8 states (plan.md §"Phase 2 — Deliverables"):

* ``idle`` — no update in progress. Accepts new dispatches.
* ``downloading`` — bundle download in flight to ``/data/.update/staging/``.
* ``staged`` — bundle downloaded, signature verified, sha256 manifest
  checked, NOT yet written to the inactive slot.
* ``tryboot_pending`` — staged content rsynced to the inactive slot;
  about to call ``agora-slot-mgr trigger-tryboot``.
* ``tryboot_running`` — ``trigger-tryboot`` returned successfully; the
  device is now (or imminently) running on the inactive slot in
  tentative mode. Set just before issuing the post-tryboot reboot;
  survives that reboot via the on-disk state file.
* ``promoted_pending_migration`` — slot-confirm passed, ``agora-slot-mgr
  promote`` succeeded. Forward-migration scripts have not run yet.
* ``migrating`` — forward-migration runner is executing scripts under
  ``/etc/agora/migrations/`` (post-promote only, gated by the
  migration-allowed sentinel from Phase 1).
* ``failed`` — terminal failure for this dispatch. Stays in ``failed``
  until cleared by a fresh dispatch arriving (which transitions back
  to ``downloading``) or operator intervention.

Concurrency interlock (plan #23): only ``idle`` and ``failed`` accept a new
dispatch. Anything else returns ``UpdaterBusyError`` and emits
``declined:busy`` over WPS.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.state import atomic_write

SCHEMA_VERSION = 1

#: On-disk location of the persistent updater state. Lives under ``/data``
#: so it survives slot switches — the new slot's first boot still knows
#: which dispatch was in flight.
DEFAULT_STATE_PATH = Path("/data/agora/updater-state.json")


class UpdaterFSMState(str, enum.Enum):
    """Explicit finite state machine values.

    String-valued so the state survives a round-trip through JSON without
    needing custom serialization.
    """

    IDLE = "idle"
    DOWNLOADING = "downloading"
    STAGED = "staged"
    TRYBOOT_PENDING = "tryboot_pending"
    TRYBOOT_RUNNING = "tryboot_running"
    PROMOTED_PENDING_MIGRATION = "promoted_pending_migration"
    MIGRATING = "migrating"
    FAILED = "failed"


#: Legal transitions: ``{from_state: set_of_to_states}``.
#:
#: Validated at write time by :func:`transition` so a programming error
#: ("we somehow went straight from downloading to migrating") fails loud
#: in tests rather than producing a wedged on-disk state.
LEGAL_TRANSITIONS: dict[UpdaterFSMState, frozenset[UpdaterFSMState]] = {
    UpdaterFSMState.IDLE: frozenset({UpdaterFSMState.DOWNLOADING}),
    UpdaterFSMState.DOWNLOADING: frozenset(
        {UpdaterFSMState.STAGED, UpdaterFSMState.FAILED}
    ),
    UpdaterFSMState.STAGED: frozenset(
        {UpdaterFSMState.TRYBOOT_PENDING, UpdaterFSMState.FAILED}
    ),
    UpdaterFSMState.TRYBOOT_PENDING: frozenset(
        {UpdaterFSMState.TRYBOOT_RUNNING, UpdaterFSMState.FAILED}
    ),
    UpdaterFSMState.TRYBOOT_RUNNING: frozenset(
        {
            UpdaterFSMState.PROMOTED_PENDING_MIGRATION,
            UpdaterFSMState.FAILED,
            UpdaterFSMState.IDLE,
        }
    ),
    UpdaterFSMState.PROMOTED_PENDING_MIGRATION: frozenset(
        {UpdaterFSMState.MIGRATING, UpdaterFSMState.FAILED}
    ),
    UpdaterFSMState.MIGRATING: frozenset(
        {UpdaterFSMState.IDLE, UpdaterFSMState.FAILED}
    ),
    UpdaterFSMState.FAILED: frozenset({UpdaterFSMState.DOWNLOADING}),
}

#: States in which a new dispatch is REFUSED (per #23).
BUSY_STATES: frozenset[UpdaterFSMState] = frozenset(
    {
        UpdaterFSMState.DOWNLOADING,
        UpdaterFSMState.STAGED,
        UpdaterFSMState.TRYBOOT_PENDING,
        UpdaterFSMState.TRYBOOT_RUNNING,
        UpdaterFSMState.PROMOTED_PENDING_MIGRATION,
        UpdaterFSMState.MIGRATING,
    }
)


def utc_now_iso() -> str:
    """Return the current UTC time as a Zulu ISO-8601 string."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TransitionError(Exception):
    """Raised when :func:`transition` is asked to make an illegal jump.

    Programmer error — a routine failure path should target
    :data:`UpdaterFSMState.FAILED` explicitly, which is reachable from every
    non-terminal state.
    """


class UpdaterState(BaseModel):
    """Persistent on-disk state for the agora-os-updater daemon."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION

    #: Current FSM position.
    fsm: UpdaterFSMState = UpdaterFSMState.IDLE

    #: Server-assigned release identifier of the currently-in-flight
    #: dispatch, or ``None`` when idle. Mirrors the dispatch payload's
    #: ``release_id`` field so events emitted later can be correlated
    #: back to the originating dispatch.
    release_id: Optional[str] = None

    #: Target semantic version of the current dispatch. Used for the
    #: ``target_version`` field of lifecycle events.
    target_version: Optional[str] = None

    #: Path to the staging directory for the current dispatch. Set when
    #: we enter ``downloading``; cleared on ``idle`` / ``failed``.
    staging_dir: Optional[str] = None

    #: Last reason string captured at the most recent ``failed`` transition.
    #: Surfaces in lifecycle events and helps an operator diagnose without
    #: cross-referencing journald.
    last_failure_reason: Optional[str] = None

    #: ISO-8601 timestamp of the last successful state change.
    updated_at: str = Field(default_factory=utc_now_iso)

    #: Monotonic per-device event sequence. Lifecycle events are stamped
    #: with this counter pre-increment (so the first event sent is
    #: ``event_id=1``). Persisted so we survive daemon restart without
    #: re-using ids — critical for the ``(device_id, event_id)`` dedupe
    #: contract with the CMS event-buffer (D33).
    last_event_id: int = 0


def load_state(path: Optional[Path] = None) -> UpdaterState:
    """Read ``updater-state.json``, returning a fresh state if missing.

    A missing file is the normal first-boot case. Parse errors fall back
    to a fresh state too — losing a stale state record is better than
    refusing to boot. Real disk-level corruption is rare enough that we
    accept the data loss rather than complicating recovery.
    """

    p = path or DEFAULT_STATE_PATH
    try:
        return UpdaterState.model_validate_json(p.read_text())
    except (FileNotFoundError, ValueError):
        return UpdaterState()


def save_state(state: UpdaterState, path: Optional[Path] = None) -> None:
    """Persist ``state`` to disk atomically.

    Refreshes ``updated_at`` to "now" before writing so callers don't have
    to remember.
    """

    p = path or DEFAULT_STATE_PATH
    state.updated_at = utc_now_iso()
    atomic_write(p, state.model_dump_json(indent=2))


def is_busy(state: UpdaterState) -> bool:
    """Return ``True`` if a fresh dispatch should be refused."""

    return state.fsm in BUSY_STATES


def transition(
    state: UpdaterState,
    to: UpdaterFSMState,
    *,
    reason: Optional[str] = None,
) -> UpdaterState:
    """Move ``state`` from its current FSM value to ``to``.

    Mutates ``state`` in place AND returns it (for fluent chaining and to
    match how callers typically use it).

    Raises :class:`TransitionError` if the requested move isn't in
    :data:`LEGAL_TRANSITIONS`. Use ``UpdaterFSMState.FAILED`` from any
    non-terminal state to signal a failure path — that edge is always
    legal.

    Side-effects on the state:

    * ``reason`` is stored as ``last_failure_reason`` when transitioning
      into ``FAILED``.
    * Transitioning to ``IDLE`` clears all dispatch-specific fields
      (``release_id``, ``target_version``, ``staging_dir``,
      ``last_failure_reason``) so the next dispatch starts clean.
    """

    legal = LEGAL_TRANSITIONS.get(state.fsm, frozenset())
    if to not in legal:
        raise TransitionError(
            f"illegal FSM transition: {state.fsm.value} -> {to.value} "
            f"(legal targets: {sorted(s.value for s in legal)!r})"
        )

    state.fsm = to
    if to is UpdaterFSMState.FAILED:
        state.last_failure_reason = reason
    if to is UpdaterFSMState.IDLE:
        state.release_id = None
        state.target_version = None
        state.staging_dir = None
        state.last_failure_reason = None
    return state
