"""Core slot-management primitives used by both the library API and the CLI.

This module implements the Phase 1 deliverables:

* :func:`slot_state` - derive the current active slot + tentative flag
* :func:`trigger_tryboot` - stage a tryboot of the inactive slot and reboot
* :func:`promote_slot` - make a slot the permanent default (resets strikes,
  writes the forward-migration sentinel)
* :func:`record_tryboot_strike` - increment the failure counter for a slot
  and pin the device if the counter hits ``STRIKE_LIMIT``
* :func:`unpin` - clear the pinned flag (operator escape hatch)

All disk side-effects route through configurable paths in :mod:`slot_mgr.paths`
so the whole module is testable on a developer laptop without a Pi.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from slot_mgr import autoboot as ab
from slot_mgr import paths
from slot_mgr.state import (
    STRIKE_LIMIT,
    SlotState,
    load_state,
    save_state,
    utc_now_iso,
)

# Public re-exports for type-completeness
SLOT_A = ab.SLOT_A
SLOT_B = ab.SLOT_B
VALID_SLOTS = (SLOT_A, SLOT_B)

#: ``reboot '0 tryboot'`` is the Pi 5 syntax that tells the bootloader to use
#: ``[tryboot]`` for exactly one boot. The single-token argument matters - the
#: bootloader sees it via the kernel "reboot reason" field.
REBOOT_TRYBOOT_CMD = ("sudo", "reboot", "0 tryboot")

#: When the kernel cmdline says we booted via ``root=PARTLABEL=root-A|B`` we
#: map the label to a slot number.
_PARTLABEL_RE = re.compile(r"root=PARTLABEL=root-([AB])\b", re.IGNORECASE)


class SlotMgrError(RuntimeError):
    """Base class for all expected slot-mgr failures."""


class PinnedError(SlotMgrError):
    """trigger_tryboot refused because the device is pinned to last-known-good."""


class InvalidSlotError(SlotMgrError):
    """A non-{1, 2} slot identifier was supplied."""


@dataclass(frozen=True)
class SlotStatus:
    """Snapshot returned by :func:`slot_state`.

    ``running_slot``
        The slot we actually booted into (parsed from /proc/cmdline). ``None``
        if we cannot determine it (typically: dev host without a real cmdline).

    ``default_slot``
        The slot the bootloader would pick by default on the next normal
        boot (parsed from ``[all] boot_partition`` of autoboot.txt). ``None``
        if autoboot.txt is missing or malformed.

    ``tentative``
        ``True`` iff ``running_slot != default_slot`` - i.e., we're on a
        tryboot. ``slot_state()`` derives this rather than reading any marker
        file; the kernel cmdline and autoboot.txt are the single source of
        truth (no extra state to get out of sync).

    ``pinned`` / ``strikes`` / ``last_tryboot_target``
        Pulled straight from slot-state.json for convenience.
    """

    running_slot: Optional[int]
    default_slot: Optional[int]
    tentative: bool
    pinned: bool
    strikes: dict[int, int]
    last_tryboot_target: Optional[int]
    last_tryboot_at: Optional[str]
    last_success_at: Optional[str]


def _validate_slot(slot: int) -> int:
    if slot not in VALID_SLOTS:
        raise InvalidSlotError(f"slot must be 1 or 2, got {slot!r}")
    return slot


def _read_cmdline(path: Optional[Path] = None) -> str:
    p = path or paths.proc_cmdline_path()
    try:
        return p.read_text()
    except FileNotFoundError:
        return ""


def _detect_running_slot(cmdline_text: str) -> Optional[int]:
    """Parse ``/proc/cmdline`` for ``root=PARTLABEL=root-A|B``."""
    m = _PARTLABEL_RE.search(cmdline_text)
    if not m:
        return None
    return SLOT_A if m.group(1).upper() == "A" else SLOT_B


def _read_default_slot(autoboot_path: Optional[Path] = None) -> Optional[int]:
    """Parse ``[all] boot_partition=N`` from autoboot.txt."""
    p = autoboot_path or paths.autoboot_path()
    try:
        parsed = ab.read_autoboot(p)
    except (FileNotFoundError, ab.AutobootError):
        return None
    return parsed.default_slot()


def slot_state() -> SlotStatus:
    """Derive the current slot configuration.

    Reads /proc/cmdline + /boot/firmware/autoboot.txt + /data/agora/slot-state.json.
    Cheap; safe to call repeatedly.
    """
    state = load_state()
    running = _detect_running_slot(_read_cmdline())
    default = _read_default_slot()
    return SlotStatus(
        running_slot=running,
        default_slot=default,
        tentative=(running is not None and default is not None and running != default),
        pinned=state.pinned,
        strikes={int(k): int(v) for k, v in state.strikes.items()},
        last_tryboot_target=state.last_tryboot_target,
        last_tryboot_at=state.last_tryboot_at,
        last_success_at=state.last_success_at,
    )


def _autoboot_mirrors() -> tuple[Path, ...]:
    """Return the mirror paths that should also receive autoboot.txt writes.

    A mirror is only included when its parent directory exists - on a dev
    host or in a test fixture with no boot-B mountpoint we don't want to
    blow up the operation; the primary write is what guards against bricking.
    """
    mirror = paths.autoboot_mirror_path()
    if mirror.parent.exists():
        return (mirror,)
    return ()


def trigger_tryboot(
    target_slot: int,
    *,
    reboot: bool = True,
    reboot_fn: Optional[Callable[[], None]] = None,
) -> SlotState:
    """Stage a one-shot tryboot of ``target_slot`` and (by default) reboot.

    Sequence (in order, with persistence between steps so a crash mid-flow
    leaves the device in a recoverable state):

    1. Refuse if the device is pinned. Operator must ``unpin`` first.
    2. Rewrite ``[tryboot] boot_partition`` in autoboot.txt to the target
       slot's boot partition. Mirror the file to boot-B if mounted.
    3. Record ``last_tryboot_target`` + ``last_tryboot_at`` in slot-state.json
       *before* issuing the reboot, so on next boot ``record_tryboot_strike``
       can attribute a failed boot to this target.
    4. Execute ``sudo reboot '0 tryboot'`` (unless ``reboot=False``).

    Returns the updated :class:`SlotState` (for tests and CLI output).

    Parameters
    ----------
    reboot:
        When ``False``, every disk side-effect happens but the actual reboot
        is skipped. Useful for unit tests and for ``--dry-run`` flows.
    reboot_fn:
        Override the reboot invocation - tests pass a recorder. Defaults to
        ``_default_reboot`` which execs ``sudo reboot '0 tryboot'``.
    """
    _validate_slot(target_slot)
    state = load_state()
    if state.pinned:
        raise PinnedError(
            "device is pinned to last-known-good after repeated tryboot failures; "
            f"reason={state.pinned_reason!r}; run `agora-slot-mgr unpin` to clear"
        )

    parsed = ab.read_autoboot(paths.autoboot_path())
    parsed.set_tryboot_partition(ab.SLOT_TO_BOOT_PARTITION[target_slot])
    ab.write_autoboot(parsed, paths.autoboot_path(), mirrors=_autoboot_mirrors())

    state.last_tryboot_target = target_slot
    state.last_tryboot_at = utc_now_iso()
    save_state(state)

    if reboot:
        (reboot_fn or _default_reboot)()

    return state


def _default_reboot() -> None:
    """Issue ``sudo reboot '0 tryboot'`` to the host kernel."""
    subprocess.run(REBOOT_TRYBOOT_CMD, check=True)


def promote_slot(target_slot: int) -> SlotState:
    """Make ``target_slot`` the permanent default.

    Steps:

    1. Rewrite ``[all] boot_partition`` in autoboot.txt to the target slot's
       boot partition. Mirror to boot-B if available.
    2. Clear ``[tryboot] boot_partition`` so a stray ``reboot '0 tryboot'``
       can't accidentally re-enter the old tentative configuration. (We
       point ``[tryboot]`` at the *other* slot, the new candidate, so the
       file remains valid and future trybooot calls behave.)
    3. Reset the strike counter for the promoted slot.
    4. Update ``last_success_at`` and clear ``last_tryboot_target``.
    5. Write ``/data/agora/migration-allowed`` so the new agora.service can
       run forward migrations on /data. (The forward-migration fence reader
       is implemented by ``p1-forward-migration-fence``.)
    """
    _validate_slot(target_slot)
    other_slot = SLOT_B if target_slot == SLOT_A else SLOT_A

    parsed = ab.read_autoboot(paths.autoboot_path())
    parsed.set_default_partition(ab.SLOT_TO_BOOT_PARTITION[target_slot])
    parsed.set_tryboot_partition(ab.SLOT_TO_BOOT_PARTITION[other_slot])
    ab.write_autoboot(parsed, paths.autoboot_path(), mirrors=_autoboot_mirrors())

    state = load_state()
    state.set_strikes(target_slot, 0)
    state.last_success_at = utc_now_iso()
    state.last_tryboot_target = None
    state.last_tryboot_at = None
    save_state(state)

    _write_migration_sentinel(target_slot)
    return state


def _write_migration_sentinel(slot: int) -> None:
    sentinel = paths.migration_allowed_sentinel_path()
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    body = (
        f"slot={slot}\n"
        f"promoted_at={utc_now_iso()}\n"
    )
    from shared.state import atomic_write

    atomic_write(sentinel, body)


def record_tryboot_strike(
    slot: int,
    *,
    reason: str = "tryboot failed",
) -> SlotState:
    """Increment the strike counter for ``slot``; pin the device if it hits the limit.

    Called by ``p1-slot-confirm`` when slot-confirm fails on a tentative boot,
    and by ``p1-watchdog`` when a watchdog reset rolls a tryboot back to the
    previous slot.

    Returns the updated state.
    """
    _validate_slot(slot)
    state = load_state()
    new_count = state.get_strikes(slot) + 1
    state.set_strikes(slot, new_count)

    if new_count >= STRIKE_LIMIT and not state.pinned:
        state.pinned = True
        state.pinned_at = utc_now_iso()
        state.pinned_reason = (
            f"slot {slot} reached {new_count} consecutive tryboot failures: {reason}"
        )

    save_state(state)
    return state


def unpin(*, reason: str = "operator unpin") -> SlotState:
    """Clear the pinned flag and reset both strike counters.

    Equivalent to "operator has decided the device is recovered." Resets
    both counters because the pin condition was a global "we don't trust
    tryboot right now" - re-trusting one slot means re-trusting the
    mechanism, not just that slot's history.
    """
    state = load_state()
    state.pinned = False
    state.pinned_at = None
    state.pinned_reason = None
    state.strikes = {"1": 0, "2": 0}
    state.last_tryboot_target = None
    state.last_tryboot_at = None
    save_state(state)
    return state


def format_status_human(status: SlotStatus) -> str:
    """Human-readable rendering for ``agora-slot-mgr status``."""
    lines = []
    lines.append(f"running slot:        {status.running_slot}")
    lines.append(f"default slot:        {status.default_slot}")
    lines.append(f"tentative tryboot:   {status.tentative}")
    lines.append(f"pinned:              {status.pinned}")
    lines.append(
        f"strikes:             slot1={status.strikes.get(1, 0)} "
        f"slot2={status.strikes.get(2, 0)}"
    )
    lines.append(f"last_tryboot_target: {status.last_tryboot_target}")
    lines.append(f"last_tryboot_at:     {status.last_tryboot_at}")
    lines.append(f"last_success_at:     {status.last_success_at}")
    return "\n".join(lines)


def quote_cmd(cmd: tuple[str, ...]) -> str:
    """Render a command tuple as a shell-quoted string (for log lines)."""
    return " ".join(shlex.quote(part) for part in cmd)
