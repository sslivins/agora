"""Concrete chaos scenarios for the Phase 1 ≥15-point software-reboot subset.

Each scenario uses :func:`chaos.core.inject_after_nth_call` to fault at a
specific seam between disk operations in ``slot_mgr``, then verifies that:

* the on-disk artifacts (autoboot.txt, slot-state.json, migration sentinel)
  are either unchanged or in a documented partially-committed state,
* re-running the interrupted operation completes successfully (idempotency),
* the system can still derive a valid :class:`slot_mgr.SlotStatus`.

The Phase 1 plan requires ≥15 software-reboot injection points; this module
ships 17.

Naming convention
-----------------

``<operation>/<seam>`` where ``<operation>`` is one of ``trigger-tryboot``,
``promote``, ``strike``, ``unpin``, ``storm``, ``recovery`` and ``<seam>``
is a short label for *where* in the operation the fault fires.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, List

from slot_mgr import autoboot as ab
from slot_mgr.autoboot import BOOT_PARTITION_TO_SLOT
from slot_mgr import core as sm_core
from slot_mgr import state as sm_state
from slot_mgr.state import STRIKE_LIMIT, SlotState, save_state, load_state

from chaos.core import (
    ChaosEnv,
    ChaosPowerCut,
    Scenario,
    inject_after_nth_call,
    inject_first_call,
    set_cmdline_slot,
)


# ---------------------------------------------------------------------------
# Invariant helpers shared across scenarios
# ---------------------------------------------------------------------------


def _parse_autoboot_default(path: Path) -> int | None:
    """Return the ``[all] boot_partition`` slot number for ``path``."""
    parsed = ab.parse_autoboot(path.read_text())
    return parsed.default_slot()


def _parse_autoboot_tryboot(path: Path) -> int | None:
    """Return the ``[tryboot] boot_partition`` slot number for ``path``."""
    parsed = ab.parse_autoboot(path.read_text())
    part = parsed.tryboot_partition()
    return None if part is None else BOOT_PARTITION_TO_SLOT.get(part)


def _assert_autoboot_parseable(env: ChaosEnv) -> None:
    """Both autoboot files must be parseable (no half-written content)."""
    primary = ab.parse_autoboot(env.autoboot_path.read_text())
    assert primary.default_slot() in (1, 2), (
        f"primary autoboot.txt has invalid [all] slot: {primary.default_slot()!r}"
    )
    if env.autoboot_mirror_path.exists():
        mirror = ab.parse_autoboot(env.autoboot_mirror_path.read_text())
        assert mirror.default_slot() in (1, 2), (
            f"mirror autoboot.txt has invalid [all] slot: {mirror.default_slot()!r}"
        )


def _assert_slot_state_parseable(env: ChaosEnv) -> None:
    """slot-state.json must be parseable as a :class:`SlotState`."""
    if not env.slot_state_path.exists():
        # Missing is fine — load_state() returns a fresh default.
        return
    SlotState.model_validate_json(env.slot_state_path.read_text())


def _assert_no_orphan_tempfiles(env: ChaosEnv) -> None:
    """No ``.tmp`` files left behind in the directories we wrote to."""
    for d in (
        env.autoboot_path.parent,
        env.autoboot_mirror_path.parent,
        env.data_dir,
    ):
        if not d.exists():
            continue
        orphans = [p.name for p in d.iterdir() if p.suffix == ".tmp"]
        assert not orphans, f"orphan tempfiles in {d}: {orphans!r}"


def _assert_slot_state_recoverable(env: ChaosEnv) -> None:
    """``slot_state()`` must return a valid :class:`SlotStatus`."""
    status = sm_core.slot_state()
    assert status.running_slot in (None, 1, 2), (
        f"running_slot={status.running_slot!r} out of band"
    )
    assert status.default_slot in (None, 1, 2), (
        f"default_slot={status.default_slot!r} out of band"
    )


def _assert_baseline(env: ChaosEnv) -> None:
    """The full baseline invariant suite (all four checks)."""
    _assert_autoboot_parseable(env)
    _assert_slot_state_parseable(env)
    _assert_no_orphan_tempfiles(env)
    _assert_slot_state_recoverable(env)


def _silent_reboot() -> None:
    """A no-op reboot function — used when scenarios drive ``trigger_tryboot``
    with ``reboot=True`` to verify the reboot fires *last* (after all disk
    side-effects are committed).
    """
    return None


def _exploding_reboot() -> Callable[[], None]:
    """Return a reboot function that raises :class:`ChaosPowerCut`.

    Used by the "after-state-before-reboot" scenarios that verify a power-cut
    during ``subprocess.run`` of ``sudo reboot`` leaves the device in a
    consistent state (state + autoboot fully committed; just no actual reboot).
    """

    def _r() -> None:
        raise ChaosPowerCut("reboot command interrupted")

    return _r


# ---------------------------------------------------------------------------
# 1. trigger_tryboot scenarios
# ---------------------------------------------------------------------------


def _s_trigger_before_any_write(env: ChaosEnv) -> None:
    """Power-cut before any autoboot write: state must be unchanged."""
    original_default = _parse_autoboot_default(env.autoboot_path)

    with inject_first_call(
        ab, "_atomic_write_text", message="before any autoboot write"
    ):
        try:
            sm_core.trigger_tryboot(2, reboot=False)
        except ChaosPowerCut:
            pass

    # Invariants: no slot-state.json written, autoboot unchanged.
    assert not env.slot_state_path.exists(), (
        "slot-state.json should not exist — write should not have begun"
    )
    assert _parse_autoboot_default(env.autoboot_path) == original_default, (
        "autoboot [all] slot must be unchanged before any write"
    )
    _assert_baseline(env)


def _s_trigger_between_primary_and_mirror(env: ChaosEnv) -> None:
    """Primary autoboot written, mirror failed.

    The primary now has the tryboot section pointing at slot 2 (the freshly
    written value); the mirror was untouched by the failing call. The state
    save did not run (autoboot write raised before completing).
    """
    # Sanity: capture the mirror text before the chaos run so we can assert
    # it's unchanged afterwards. The fresh env seeds both files identically.
    mirror_before = env.autoboot_mirror_path.read_text()

    with inject_after_nth_call(
        ab, "_atomic_write_text", 1, message="between primary and mirror autoboot"
    ):
        try:
            sm_core.trigger_tryboot(2, reboot=False)
        except ChaosPowerCut:
            pass

    primary = ab.parse_autoboot(env.autoboot_path.read_text())
    assert _parse_autoboot_tryboot(env.autoboot_path) == 2, (
        "primary [tryboot] should point at slot 2 after partial write"
    )
    assert env.autoboot_mirror_path.read_text() == mirror_before, (
        "mirror autoboot.txt must be byte-unchanged when its write failed"
    )
    assert not env.slot_state_path.exists(), (
        "slot-state.json should not exist — write_autoboot raised before save"
    )
    _assert_baseline(env)


def _s_trigger_between_autoboot_and_state(env: ChaosEnv) -> None:
    """Both autoboot files written; slot-state.json save failed.

    On next boot we'd try to tryboot to slot 2 (autoboot says so) but no
    state was saved so ``record_tryboot_strike`` would have nothing to
    attribute. ``slot_state()`` still reports a valid status.
    """
    with inject_first_call(
        sm_state, "atomic_write", message="between autoboot and state save"
    ):
        try:
            sm_core.trigger_tryboot(2, reboot=False)
        except ChaosPowerCut:
            pass

    assert _parse_autoboot_tryboot(env.autoboot_path) == 2, (
        "primary [tryboot] slot should be set"
    )
    assert _parse_autoboot_tryboot(env.autoboot_mirror_path) == 2, (
        "mirror [tryboot] slot should match primary"
    )
    assert not env.slot_state_path.exists(), (
        "slot-state.json save should have failed before any bytes hit disk"
    )
    _assert_baseline(env)


def _s_trigger_after_state_before_reboot(env: ChaosEnv) -> None:
    """All disk side-effects committed; reboot itself failed.

    Equivalent to power-cut during ``sudo reboot``. The device is fully
    prepared for a tryboot but didn't actually reboot. On a real reboot
    later, the bootloader picks up the existing autoboot and trybooots.
    """
    try:
        sm_core.trigger_tryboot(2, reboot=True, reboot_fn=_exploding_reboot())
    except ChaosPowerCut:
        pass

    assert _parse_autoboot_tryboot(env.autoboot_path) == 2
    assert _parse_autoboot_tryboot(env.autoboot_mirror_path) == 2
    assert env.slot_state_path.exists(), "state should be fully committed"
    state = load_state()
    assert state.last_tryboot_target == 2, (
        "state should record we asked for a tryboot to slot 2"
    )
    _assert_baseline(env)


# ---------------------------------------------------------------------------
# 2. promote_slot scenarios
# ---------------------------------------------------------------------------


def _stage_post_tryboot(env: ChaosEnv) -> None:
    """Seed env to look like we just landed on slot 2 after a successful tryboot.

    Used by promote scenarios so the system thinks tryboot already happened
    (cmdline says slot 2, slot-state has last_tryboot_target=2).
    """
    set_cmdline_slot(env, 2)
    seed = SlotState()
    seed.last_tryboot_target = 2
    seed.last_tryboot_at = "2026-01-01T00:00:00Z"
    save_state(seed)


def _s_promote_before_any_write(env: ChaosEnv) -> None:
    """Power-cut before any autoboot write during promote."""
    _stage_post_tryboot(env)
    original_default = _parse_autoboot_default(env.autoboot_path)

    with inject_first_call(
        ab, "_atomic_write_text", message="promote: before any write"
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    assert _parse_autoboot_default(env.autoboot_path) == original_default, (
        "autoboot [all] must be unchanged"
    )
    assert not env.sentinel_path.exists(), "sentinel must not exist"
    _assert_baseline(env)


def _s_promote_between_primary_and_mirror(env: ChaosEnv) -> None:
    """Primary autoboot promoted to slot 2; mirror not.

    Device will boot slot 2 normally (primary autoboot is authoritative when
    the bootloader reads boot-A). State + sentinel never written.
    """
    _stage_post_tryboot(env)

    with inject_after_nth_call(
        ab,
        "_atomic_write_text",
        1,
        message="promote: between primary and mirror",
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    assert _parse_autoboot_default(env.autoboot_path) == 2, (
        "primary should be promoted to slot 2"
    )
    # Mirror still has the pre-chaos default (slot 1).
    assert _parse_autoboot_default(env.autoboot_mirror_path) == 1, (
        "mirror should be unchanged"
    )
    assert not env.sentinel_path.exists(), (
        "sentinel must not exist — promote raised before sentinel write"
    )
    _assert_baseline(env)


def _s_promote_between_autoboot_and_state(env: ChaosEnv) -> None:
    """Autoboot fully promoted; slot-state save failed.

    Stale ``last_tryboot_target`` survives but autoboot is correctly promoted.
    Recovery: re-run promote_slot, which is idempotent.
    """
    _stage_post_tryboot(env)

    with inject_first_call(
        sm_state, "atomic_write", message="promote: between autoboot and state"
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    assert _parse_autoboot_default(env.autoboot_path) == 2
    assert _parse_autoboot_default(env.autoboot_mirror_path) == 2
    # State was the seed we saved in _stage_post_tryboot — last_tryboot_target=2.
    state = load_state()
    assert state.last_tryboot_target == 2, (
        "state save during promote failed, so seed survives"
    )
    assert not env.sentinel_path.exists()
    _assert_baseline(env)


def _s_promote_between_state_and_sentinel(env: ChaosEnv) -> None:
    """Autoboot + state promoted; sentinel write failed.

    This is the critical bug surface: agora.service refuses to migrate
    without the sentinel, so the device sits in a "promoted but not
    migration-allowed" state until promote_slot is re-run.
    """
    _stage_post_tryboot(env)

    # The sentinel write uses shared.state.atomic_write via a local import
    # inside _write_migration_sentinel. Patch the shared module directly.
    from shared import state as shared_state

    with inject_first_call(
        shared_state, "atomic_write", message="promote: between state and sentinel"
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    assert _parse_autoboot_default(env.autoboot_path) == 2
    state = load_state()
    assert state.last_tryboot_target is None, "promote did update state"
    assert state.last_success_at is not None, "promote recorded success timestamp"
    assert not env.sentinel_path.exists(), "sentinel was the step that failed"
    _assert_baseline(env)


def _s_promote_rerun_recovers_after_state_crash(env: ChaosEnv) -> None:
    """Re-running promote after a state-save crash completes the operation."""
    _stage_post_tryboot(env)

    with inject_first_call(
        sm_state, "atomic_write", message="state save crash for rerun test"
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    # Re-run with no chaos injection.
    sm_core.promote_slot(2)

    assert _parse_autoboot_default(env.autoboot_path) == 2
    state = load_state()
    assert state.last_tryboot_target is None, (
        "rerun should have cleared the stale last_tryboot_target"
    )
    assert state.get_strikes(2) == 0, "rerun should have reset strikes for slot 2"
    assert env.sentinel_path.exists(), "rerun should have written the sentinel"
    sentinel_body = env.sentinel_path.read_text()
    assert "slot=2" in sentinel_body
    _assert_baseline(env)


def _s_promote_rerun_recovers_after_sentinel_crash(env: ChaosEnv) -> None:
    """Re-running promote after a sentinel-write crash writes the sentinel."""
    _stage_post_tryboot(env)

    from shared import state as shared_state

    with inject_first_call(
        shared_state, "atomic_write", message="sentinel crash for rerun test"
    ):
        try:
            sm_core.promote_slot(2)
        except ChaosPowerCut:
            pass

    assert not env.sentinel_path.exists()

    # Re-run with no chaos injection.
    sm_core.promote_slot(2)

    assert env.sentinel_path.exists(), "rerun should have written the sentinel"
    sentinel_body = env.sentinel_path.read_text()
    assert "slot=2" in sentinel_body
    _assert_baseline(env)


# ---------------------------------------------------------------------------
# 3. record_tryboot_strike scenarios
# ---------------------------------------------------------------------------


def _s_strike_before_save(env: ChaosEnv) -> None:
    """Power-cut before strike count is saved: no strike credited."""
    # Seed an existing state so load_state returns something to mutate.
    seed = SlotState()
    seed.set_strikes(2, 1)
    save_state(seed)

    # Only the next atomic_write call (the strike's save) faults; the seed
    # save above already ran outside the injection context.
    with inject_first_call(
        sm_state, "atomic_write", message="strike: before save"
    ):
        try:
            sm_core.record_tryboot_strike(2)
        except ChaosPowerCut:
            pass

    state = load_state()
    assert state.get_strikes(2) == 1, (
        f"strike count must be unchanged (still 1, got {state.get_strikes(2)})"
    )
    assert not state.pinned, "device must not be pinned"
    _assert_baseline(env)


def _s_strike_at_pin_threshold_crash(env: ChaosEnv) -> None:
    """Power-cut at pin threshold: device not pinned, strikes unchanged."""
    seed = SlotState()
    seed.set_strikes(2, STRIKE_LIMIT - 1)
    save_state(seed)

    with inject_first_call(
        sm_state, "atomic_write", message="strike: at pin threshold"
    ):
        try:
            sm_core.record_tryboot_strike(2)
        except ChaosPowerCut:
            pass

    state = load_state()
    assert state.get_strikes(2) == STRIKE_LIMIT - 1, (
        f"strike count must be unchanged (still {STRIKE_LIMIT - 1})"
    )
    assert not state.pinned, "pin must not be committed"
    _assert_baseline(env)


# ---------------------------------------------------------------------------
# 4. unpin scenarios
# ---------------------------------------------------------------------------


def _s_unpin_before_save(env: ChaosEnv) -> None:
    """Power-cut before unpin save: device stays pinned."""
    seed = SlotState()
    seed.pinned = True
    seed.pinned_at = "2026-01-01T00:00:00Z"
    seed.pinned_reason = "test seed"
    seed.set_strikes(2, STRIKE_LIMIT)
    save_state(seed)

    with inject_first_call(
        sm_state, "atomic_write", message="unpin: before save"
    ):
        try:
            sm_core.unpin()
        except ChaosPowerCut:
            pass

    state = load_state()
    assert state.pinned, "device must still be pinned"
    assert state.get_strikes(2) == STRIKE_LIMIT, "strikes must not be reset"
    _assert_baseline(env)


def _s_unpin_rerun_recovers(env: ChaosEnv) -> None:
    """Re-running unpin after a save crash clears the pin."""
    seed = SlotState()
    seed.pinned = True
    seed.pinned_at = "2026-01-01T00:00:00Z"
    seed.pinned_reason = "test seed"
    seed.set_strikes(2, STRIKE_LIMIT)
    save_state(seed)

    with inject_first_call(
        sm_state, "atomic_write", message="unpin: crash before rerun"
    ):
        try:
            sm_core.unpin()
        except ChaosPowerCut:
            pass

    sm_core.unpin()

    state = load_state()
    assert not state.pinned, "rerun should have cleared the pin"
    assert state.get_strikes(2) == 0, "rerun should have reset strikes"
    _assert_baseline(env)


# ---------------------------------------------------------------------------
# 5. Multi-step storm scenarios
# ---------------------------------------------------------------------------


def _s_storm_three_failed_trybooots_pin(env: ChaosEnv) -> None:
    """Three sequential failed trybooots pin the device.

    No chaos injection — verifies the pinning pipeline as an integration
    sanity check that lives alongside the chaos cases (a stable baseline
    that *should* always work; if this turns red, something's wrong with
    the strike/pin logic that the chaos cases also depend on).
    """
    for _ in range(STRIKE_LIMIT):
        sm_core.record_tryboot_strike(2)

    state = load_state()
    assert state.pinned, "device must be pinned after STRIKE_LIMIT consecutive strikes"
    assert state.get_strikes(2) == STRIKE_LIMIT
    _assert_baseline(env)


def _s_storm_unpin_restores_functionality(env: ChaosEnv) -> None:
    """After pinning, unpin clears state and lets trigger_tryboot run again."""
    for _ in range(STRIKE_LIMIT):
        sm_core.record_tryboot_strike(2)
    assert load_state().pinned

    sm_core.unpin()

    state = load_state()
    assert not state.pinned
    # Should not raise PinnedError now.
    sm_core.trigger_tryboot(2, reboot=False)
    assert _parse_autoboot_tryboot(env.autoboot_path) == 2
    _assert_baseline(env)


def _s_recovery_tryboot_revert_on_simulated_reset(env: ChaosEnv) -> None:
    """Simulated watchdog reset: cmdline reverts to default; strike credited.

    Sequence:
    1. trigger_tryboot(2) with reboot=False — state records ``last_tryboot_target=2``.
    2. Simulate next boot: cmdline still says slot 1 (the watchdog reset
       caused the bootloader to fall back since [tryboot] is single-shot).
    3. slot_confirm would notice tentative=False on slot 1 and call
       record_tryboot_strike for the failed target.
    4. Assert the strike landed.
    """
    sm_core.trigger_tryboot(2, reboot=False)
    # Simulated next boot: still on slot 1 (watchdog reset, didn't actually
    # tryboot). cmdline path is already slot 1 in the fresh env.
    set_cmdline_slot(env, 1)

    status = sm_core.slot_state()
    assert status.running_slot == 1
    assert status.last_tryboot_target == 2, (
        "state still remembers we asked to tryboot slot 2"
    )
    assert not status.tentative, "default is 1, running is 1 — not tentative"

    sm_core.record_tryboot_strike(2, reason="watchdog reverted")
    state = load_state()
    assert state.get_strikes(2) == 1
    _assert_baseline(env)


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------


SCENARIOS: List[Scenario] = [
    Scenario(
        name="trigger-tryboot/before-any-write",
        description=(
            "Power-cut before any autoboot write during trigger_tryboot. "
            "Autoboot.txt and slot-state.json must be unchanged."
        ),
        run=_s_trigger_before_any_write,
    ),
    Scenario(
        name="trigger-tryboot/between-primary-and-mirror",
        description=(
            "Power-cut after primary autoboot.txt write, before mirror. "
            "Primary holds the new [tryboot] section; mirror is unchanged; "
            "slot-state.json was not written."
        ),
        run=_s_trigger_between_primary_and_mirror,
    ),
    Scenario(
        name="trigger-tryboot/between-autoboot-and-state",
        description=(
            "Power-cut after both autoboot files are written, before "
            "slot-state.json save. Autoboot is fully staged; slot-state.json "
            "does not exist."
        ),
        run=_s_trigger_between_autoboot_and_state,
    ),
    Scenario(
        name="trigger-tryboot/after-state-before-reboot",
        description=(
            "All disk side-effects committed; reboot itself failed (e.g. "
            "power-cut during `sudo reboot`). State is fully consistent; the "
            "next physical boot will pick up the staged tryboot."
        ),
        run=_s_trigger_after_state_before_reboot,
    ),
    Scenario(
        name="promote/before-any-write",
        description=(
            "Power-cut before any disk write during promote_slot. Nothing "
            "must change on disk."
        ),
        run=_s_promote_before_any_write,
    ),
    Scenario(
        name="promote/between-primary-and-mirror",
        description=(
            "Promote: primary autoboot.txt rewritten to the new [all] slot; "
            "mirror write failed. Device will boot the promoted slot since "
            "the primary is authoritative."
        ),
        run=_s_promote_between_primary_and_mirror,
    ),
    Scenario(
        name="promote/between-autoboot-and-state",
        description=(
            "Promote: both autoboot files written; slot-state.json save "
            "failed. Stale last_tryboot_target survives; recovery via "
            "re-running promote_slot (idempotent)."
        ),
        run=_s_promote_between_autoboot_and_state,
    ),
    Scenario(
        name="promote/between-state-and-sentinel",
        description=(
            "Promote: autoboot + slot-state committed; migration-allowed "
            "sentinel write failed. agora.service migration is blocked "
            "until promote is re-run."
        ),
        run=_s_promote_between_state_and_sentinel,
    ),
    Scenario(
        name="promote/rerun-recovers-after-state-crash",
        description=(
            "After a state-save crash mid-promote, re-running promote_slot "
            "is idempotent and completes successfully."
        ),
        run=_s_promote_rerun_recovers_after_state_crash,
    ),
    Scenario(
        name="promote/rerun-recovers-after-sentinel-crash",
        description=(
            "After a sentinel-write crash mid-promote, re-running "
            "promote_slot writes the sentinel and unblocks migration."
        ),
        run=_s_promote_rerun_recovers_after_sentinel_crash,
    ),
    Scenario(
        name="strike/before-save",
        description=(
            "Power-cut during record_tryboot_strike before the state save "
            "completes. Strike count is unchanged."
        ),
        run=_s_strike_before_save,
    ),
    Scenario(
        name="strike/at-pin-threshold-crash",
        description=(
            "Power-cut during record_tryboot_strike at the pin threshold. "
            "The pin is not committed (next strike will land it)."
        ),
        run=_s_strike_at_pin_threshold_crash,
    ),
    Scenario(
        name="unpin/before-save",
        description=(
            "Power-cut during unpin before the state save completes. The "
            "device remains pinned; strikes are not reset."
        ),
        run=_s_unpin_before_save,
    ),
    Scenario(
        name="unpin/rerun-recovers",
        description=(
            "After an unpin save crash, re-running unpin clears the pin "
            "and resets both strike counters."
        ),
        run=_s_unpin_rerun_recovers,
    ),
    Scenario(
        name="storm/three-failed-trybooots-pin",
        description=(
            "Three consecutive record_tryboot_strike calls (no chaos) pin "
            "the device — control case verifying the pinning pipeline."
        ),
        run=_s_storm_three_failed_trybooots_pin,
    ),
    Scenario(
        name="storm/unpin-restores-functionality",
        description=(
            "After a pin, unpin clears state and trigger_tryboot becomes "
            "callable again — end-to-end recovery from a stuck device."
        ),
        run=_s_storm_unpin_restores_functionality,
    ),
    Scenario(
        name="recovery/tryboot-revert-on-simulated-reset",
        description=(
            "Simulate a watchdog reset reverting the bootloader to the "
            "default slot. slot_state() reports non-tentative; strike is "
            "credited to the failed target."
        ),
        run=_s_recovery_tryboot_revert_on_simulated_reset,
    ),
]


def list_scenarios() -> list[dict[str, str]]:
    """Return scenario names + descriptions (used by ``python -m chaos list``)."""
    return [{"name": s.name, "description": s.description} for s in SCENARIOS]


def get_scenario(name: str) -> Scenario:
    """Look up one scenario by name; raises :class:`KeyError` if absent."""
    for s in SCENARIOS:
        if s.name == name:
            return s
    raise KeyError(f"unknown scenario: {name!r}")
