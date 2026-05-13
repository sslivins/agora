"""Forward-migration fence reader - implementation.

See :mod:`migration_fence` for the high-level rationale. This module is
the no-magic implementation: read the sentinel, parse it, compare against
the running slot, return a structured result.

Every IO seam is injectable so the test suite can drive every branch
without touching ``/data``, ``slot_mgr``, or ``os``.

Sentinel format
---------------

The sentinel is the file written by :func:`slot_mgr.promote_slot`. It is
plain UTF-8 text with one ``key=value`` per line::

    slot=2
    promoted_at=2026-04-22T18:30:00+00:00

Only ``slot`` is required. Any additional lines are tolerated and exposed
verbatim in :attr:`FenceStatus.measurement`. Lines without ``=`` or whose
key/value is empty after stripping are skipped silently - we'd rather let
``promote_slot()`` add fields than have the reader reject sentinels from
slightly-newer firmware.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional


#: Default location of the sentinel file. Matches
#: ``slot_mgr.paths.migration_allowed_sentinel_path()`` on the device.
#: Re-declared here as a string so this package does not have a hard
#: import dependency on slot_mgr - tests and dev hosts can use this
#: library without slot_mgr being installed.
DEFAULT_SENTINEL_PATH = "/data/agora/migration-allowed"


class MigrationFenceError(RuntimeError):
    """Programmer-error class (bad arguments, broken plumbing).

    The fence reader *never* raises this for operational situations
    (sentinel missing, slot mismatch, malformed file): those are reported
    via :class:`FenceStatus` so service code can keep going without a
    try/except wrapper.
    """


@dataclass(frozen=True)
class FenceStatus:
    """Snapshot of the forward-migration fence.

    ``allowed``
        ``True`` iff the caller is permitted to run forward-migrations on
        ``/data``. ``False`` for every other case (sentinel missing,
        slot mismatch, IO error reading the sentinel, indeterminate
        running slot).

    ``reason``
        Human-readable explanation of the decision. Stable enough to be
        useful in logs but not part of the public contract for
        programmatic use - branch on the booleans / slot fields, not on
        the string.

    ``allowed_slot``
        Slot number named by the sentinel's ``slot=N`` line, or ``None``
        if the sentinel was missing or unparseable.

    ``running_slot``
        Slot the device is currently running, sourced from
        :func:`slot_mgr.slot_state` (or the injected ``slot_state_fn``).
        ``None`` if the cmdline did not name a slot - which on a real Pi
        only happens during the very first boot off a freshly-flashed
        card.

    ``sentinel_present``
        ``True`` if the file at the sentinel path was readable. False
        when the file is missing *or* unreadable (the distinction lives
        in :attr:`reason`).

    ``measurement``
        Verbatim parsed key/value pairs from the sentinel. ``slot`` is
        always a string here even though :attr:`allowed_slot` is an int -
        this dict is for telemetry; the typed field is for logic.
    """

    allowed: bool
    reason: str
    allowed_slot: Optional[int]
    running_slot: Optional[int]
    sentinel_present: bool
    measurement: Mapping[str, Any] = field(default_factory=dict)


def parse_sentinel(text: str) -> Mapping[str, str]:
    """Parse a sentinel body into a dict of string key/values.

    Forgiving on purpose. Lines without ``=`` are skipped; surrounding
    whitespace is stripped from both key and value. Returns an empty dict
    for the empty string.

    Used by :func:`check_migration_fence` but exposed publicly so callers
    can inspect a sentinel file without re-implementing the format.
    """
    out: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        out[key] = value
    return out


def _default_sentinel_reader(path: Path) -> Optional[str]:
    """Read the sentinel file. Returns ``None`` when it doesn't exist."""
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def _default_slot_state():
    """Lazy import of :func:`slot_mgr.slot_state`.

    Kept as a function (not a module-level import) so this package stays
    installable in environments that don't have ``slot_mgr`` - tests, dev
    laptops, and the os-image build pipeline before slot_mgr is installed
    can all import :mod:`migration_fence` without failing.
    """
    from slot_mgr import slot_state  # imported lazily on first call

    return slot_state()


def is_migration_allowed(
    *,
    sentinel_path: Optional[Path] = None,
    sentinel_reader: Optional[Callable[[Path], Optional[str]]] = None,
    slot_state_fn: Optional[Callable[[], Any]] = None,
) -> bool:
    """Convenience wrapper around :func:`check_migration_fence`.

    Returns just the boolean. Equivalent to::

        check_migration_fence(...).allowed

    Service code that already routes through structured logging should
    prefer :func:`check_migration_fence` so the :attr:`FenceStatus.reason`
    string ends up in the log line.
    """
    return check_migration_fence(
        sentinel_path=sentinel_path,
        sentinel_reader=sentinel_reader,
        slot_state_fn=slot_state_fn,
    ).allowed


def check_migration_fence(
    *,
    sentinel_path: Optional[Path] = None,
    sentinel_reader: Optional[Callable[[Path], Optional[str]]] = None,
    slot_state_fn: Optional[Callable[[], Any]] = None,
) -> FenceStatus:
    """Read the sentinel + running slot and decide whether to migrate.

    Algorithm:

    1. Locate sentinel file (``sentinel_path`` or ``DEFAULT_SENTINEL_PATH``).
    2. Read it (``sentinel_reader`` or ``Path.read_text``).
       Missing file -> ``allowed=False, reason="sentinel absent"``.
    3. Parse with :func:`parse_sentinel`.
       No ``slot`` key, or non-integer value -> ``allowed=False, reason="malformed"``.
    4. Resolve the running slot (``slot_state_fn`` or
       :func:`slot_mgr.slot_state`).
       ``None`` -> ``allowed=False, reason="running slot unknown"``.
    5. Compare. Equal -> ``allowed=True``. Otherwise -> ``allowed=False,
       reason="sentinel for slot X, running on Y"``.

    Any exception raised by the reader or ``slot_state_fn`` is caught and
    reported as ``allowed=False`` - never propagated. This keeps callers
    from having to wrap the fence check in a try/except: an unreadable
    sentinel is operationally identical to a missing one.
    """
    path = Path(sentinel_path) if sentinel_path is not None else Path(DEFAULT_SENTINEL_PATH)
    reader = sentinel_reader or _default_sentinel_reader
    state_fn = slot_state_fn or _default_slot_state

    try:
        text = reader(path)
    except Exception as exc:  # pragma: no cover - covered by injected reader tests
        return FenceStatus(
            allowed=False,
            reason=f"sentinel unreadable: {exc.__class__.__name__}: {exc}",
            allowed_slot=None,
            running_slot=_safe_running_slot(state_fn),
            sentinel_present=False,
            measurement={},
        )

    if text is None:
        return FenceStatus(
            allowed=False,
            reason=f"sentinel absent at {path}",
            allowed_slot=None,
            running_slot=_safe_running_slot(state_fn),
            sentinel_present=False,
            measurement={},
        )

    measurement = dict(parse_sentinel(text))
    slot_str = measurement.get("slot")
    if slot_str is None or slot_str == "":
        return FenceStatus(
            allowed=False,
            reason="sentinel malformed: missing 'slot' key",
            allowed_slot=None,
            running_slot=_safe_running_slot(state_fn),
            sentinel_present=True,
            measurement=measurement,
        )

    try:
        allowed_slot = int(slot_str)
    except ValueError:
        return FenceStatus(
            allowed=False,
            reason=f"sentinel malformed: slot={slot_str!r} is not an integer",
            allowed_slot=None,
            running_slot=_safe_running_slot(state_fn),
            sentinel_present=True,
            measurement=measurement,
        )

    if allowed_slot not in (1, 2):
        return FenceStatus(
            allowed=False,
            reason=f"sentinel slot={allowed_slot} is not a valid A/B slot",
            allowed_slot=allowed_slot,
            running_slot=_safe_running_slot(state_fn),
            sentinel_present=True,
            measurement=measurement,
        )

    try:
        status = state_fn()
        running_slot = getattr(status, "running_slot", None)
    except Exception as exc:
        return FenceStatus(
            allowed=False,
            reason=f"slot_state unavailable: {exc.__class__.__name__}: {exc}",
            allowed_slot=allowed_slot,
            running_slot=None,
            sentinel_present=True,
            measurement=measurement,
        )

    if running_slot is None:
        return FenceStatus(
            allowed=False,
            reason="running slot unknown (no PARTLABEL in /proc/cmdline)",
            allowed_slot=allowed_slot,
            running_slot=None,
            sentinel_present=True,
            measurement=measurement,
        )

    if running_slot != allowed_slot:
        return FenceStatus(
            allowed=False,
            reason=(
                f"sentinel names slot {allowed_slot} but device is running "
                f"slot {running_slot} (tryboot-revert window)"
            ),
            allowed_slot=allowed_slot,
            running_slot=running_slot,
            sentinel_present=True,
            measurement=measurement,
        )

    return FenceStatus(
        allowed=True,
        reason=f"sentinel matches running slot {running_slot}",
        allowed_slot=allowed_slot,
        running_slot=running_slot,
        sentinel_present=True,
        measurement=measurement,
    )


def _safe_running_slot(state_fn: Callable[[], Any]) -> Optional[int]:
    """Best-effort running-slot lookup that never raises.

    Used by the early-exit paths in :func:`check_migration_fence` so that
    even when we already know we're going to deny, the returned
    ``FenceStatus`` still has the running_slot filled in when possible
    (it's useful for log lines and the CLI).
    """
    try:
        status = state_fn()
        return getattr(status, "running_slot", None)
    except Exception:
        return None
