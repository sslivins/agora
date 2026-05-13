"""Unit tests for ``slot_mgr.core`` - the slot-state derivation, tryboot
trigger, promote, strike, and unpin logic.

All filesystem interactions point at tmp_path via the ``slot_env`` fixture so
we never touch /boot/firmware or /data on the dev host.
"""

from __future__ import annotations

import textwrap

import pytest

from slot_mgr import autoboot as ab
from slot_mgr import core
from slot_mgr.state import STRIKE_LIMIT, load_state


AUTOBOOT_A_DEFAULT = textwrap.dedent("""\
    # autoboot.txt - managed by agora-slot-mgr

    [all]
    tryboot_a_b=1
    boot_partition=1

    [tryboot]
    boot_partition=3
""")

AUTOBOOT_B_DEFAULT = textwrap.dedent("""\
    # autoboot.txt - managed by agora-slot-mgr

    [all]
    tryboot_a_b=1
    boot_partition=3

    [tryboot]
    boot_partition=1
""")


@pytest.fixture
def slot_env(tmp_path, monkeypatch):
    """Point every slot_mgr path at tmp_path and return the layout for assertions."""
    boot_a = tmp_path / "boot-A"
    boot_b = tmp_path / "boot-B"
    data = tmp_path / "data"
    proc = tmp_path / "proc"

    boot_a.mkdir()
    boot_b.mkdir()
    data.mkdir()
    proc.mkdir()

    autoboot_a = boot_a / "autoboot.txt"
    autoboot_b = boot_b / "autoboot.txt"
    cmdline = proc / "cmdline"

    autoboot_a.write_text(AUTOBOOT_A_DEFAULT)
    autoboot_b.write_text(AUTOBOOT_A_DEFAULT)
    cmdline.write_text(
        "console=tty1 root=PARTLABEL=root-A rootfstype=ext4 rootwait\n"
    )

    monkeypatch.setenv("AGORA_AUTOBOOT_PATH", str(autoboot_a))
    monkeypatch.setenv("AGORA_AUTOBOOT_MIRROR_PATH", str(autoboot_b))
    monkeypatch.setenv("AGORA_SLOT_DATA_DIR", str(data))
    monkeypatch.setenv("AGORA_PROC_CMDLINE", str(cmdline))

    return {
        "autoboot_a": autoboot_a,
        "autoboot_b": autoboot_b,
        "cmdline": cmdline,
        "data": data,
        "tmp_path": tmp_path,
    }


def _set_cmdline(env, slot_letter: str):
    env["cmdline"].write_text(
        f"console=tty1 root=PARTLABEL=root-{slot_letter} rootfstype=ext4 rootwait\n"
    )


def _set_autoboot_default_partition(env, partition: int):
    parsed = ab.read_autoboot(env["autoboot_a"])
    parsed.set_default_partition(partition)
    ab.write_autoboot(parsed, env["autoboot_a"], mirrors=[env["autoboot_b"]])


class TestSlotState:
    def test_steady_state_on_slot_a(self, slot_env):
        status = core.slot_state()
        assert status.running_slot == 1
        assert status.default_slot == 1
        assert status.tentative is False
        assert status.pinned is False
        assert status.strikes == {1: 0, 2: 0}

    def test_tentative_when_running_differs_from_default(self, slot_env):
        # autoboot.txt still says boot from slot A, but we booted on B (tryboot)
        _set_cmdline(slot_env, "B")
        status = core.slot_state()
        assert status.running_slot == 2
        assert status.default_slot == 1
        assert status.tentative is True

    def test_no_tentative_when_cmdline_missing_partlabel(self, slot_env):
        slot_env["cmdline"].write_text("console=tty1 root=/dev/mmcblk0p3 rootwait\n")
        status = core.slot_state()
        assert status.running_slot is None
        assert status.tentative is False  # cannot derive tentative -> default False

    def test_no_default_when_autoboot_missing(self, slot_env):
        slot_env["autoboot_a"].unlink()
        status = core.slot_state()
        assert status.default_slot is None
        assert status.tentative is False


class TestTriggerTryboot:
    def test_writes_tryboot_section_and_persists_state(self, slot_env):
        reboot_called = []

        def fake_reboot():
            reboot_called.append(True)

        state = core.trigger_tryboot(2, reboot_fn=fake_reboot)

        # autoboot.txt now has [tryboot] boot_partition=3
        parsed = ab.read_autoboot(slot_env["autoboot_a"])
        assert parsed.tryboot_partition() == ab.PART_BOOT_B
        # [all] unchanged - still slot A
        assert parsed.default_partition() == ab.PART_BOOT_A

        # Mirror updated
        mirror_parsed = ab.read_autoboot(slot_env["autoboot_b"])
        assert mirror_parsed.tryboot_partition() == ab.PART_BOOT_B

        # State persisted
        assert state.last_tryboot_target == 2
        assert state.last_tryboot_at is not None
        # Reboot was invoked
        assert reboot_called == [True]

    def test_no_reboot_kwarg_skips_reboot(self, slot_env):
        reboot_called = []

        def fake_reboot():
            reboot_called.append(True)

        core.trigger_tryboot(2, reboot=False, reboot_fn=fake_reboot)
        assert reboot_called == []
        # But state and autoboot.txt still updated
        assert load_state().last_tryboot_target == 2

    def test_refuses_when_pinned(self, slot_env):
        # Force three strikes on slot 2 to trigger the pin
        for _ in range(STRIKE_LIMIT):
            core.record_tryboot_strike(2)

        with pytest.raises(core.PinnedError):
            core.trigger_tryboot(2, reboot=False)

    def test_invalid_slot_rejected(self, slot_env):
        with pytest.raises(core.InvalidSlotError):
            core.trigger_tryboot(0, reboot=False)
        with pytest.raises(core.InvalidSlotError):
            core.trigger_tryboot(3, reboot=False)

    def test_mirror_skipped_when_missing(self, slot_env, tmp_path, monkeypatch):
        # Point mirror at a path whose parent dir doesn't exist
        monkeypatch.setenv("AGORA_AUTOBOOT_MIRROR_PATH", str(tmp_path / "nope" / "autoboot.txt"))
        # Should not raise - the mirror is best-effort
        core.trigger_tryboot(2, reboot=False)
        parsed = ab.read_autoboot(slot_env["autoboot_a"])
        assert parsed.tryboot_partition() == ab.PART_BOOT_B


class TestPromoteSlot:
    def test_rewrites_all_section_and_resets_strikes(self, slot_env):
        # Set up a tentative state: booted on B, strikes accumulated on B
        _set_cmdline(slot_env, "B")
        core.record_tryboot_strike(2)
        core.record_tryboot_strike(2)
        # Pre-promote: [all] still slot A
        assert ab.read_autoboot(slot_env["autoboot_a"]).default_partition() == ab.PART_BOOT_A

        state = core.promote_slot(2)

        # [all] flipped to slot B's boot partition
        parsed = ab.read_autoboot(slot_env["autoboot_a"])
        assert parsed.default_partition() == ab.PART_BOOT_B
        # [tryboot] now points at the OTHER slot so a future tryboot is well-formed
        assert parsed.tryboot_partition() == ab.PART_BOOT_A

        # Mirror updated
        mirror_parsed = ab.read_autoboot(slot_env["autoboot_b"])
        assert mirror_parsed.default_partition() == ab.PART_BOOT_B
        assert mirror_parsed.tryboot_partition() == ab.PART_BOOT_A

        # Strikes for the promoted slot reset; last_success_at set
        assert state.get_strikes(2) == 0
        assert state.last_success_at is not None
        assert state.last_tryboot_target is None

    def test_writes_migration_allowed_sentinel(self, slot_env):
        core.promote_slot(2)
        sentinel = slot_env["data"] / "migration-allowed"
        assert sentinel.exists()
        content = sentinel.read_text()
        assert "slot=2" in content
        assert "promoted_at=" in content

    def test_invalid_slot_rejected(self, slot_env):
        with pytest.raises(core.InvalidSlotError):
            core.promote_slot(99)


class TestRecordStrike:
    def test_increments_per_slot(self, slot_env):
        s1 = core.record_tryboot_strike(2)
        assert s1.get_strikes(2) == 1
        s2 = core.record_tryboot_strike(2)
        assert s2.get_strikes(2) == 2
        # Slot 1 untouched
        assert s2.get_strikes(1) == 0

    def test_pins_at_strike_limit(self, slot_env):
        for _ in range(STRIKE_LIMIT - 1):
            state = core.record_tryboot_strike(2)
            assert state.pinned is False

        state = core.record_tryboot_strike(2, reason="frame render hung")
        assert state.pinned is True
        assert state.pinned_at is not None
        assert "frame render hung" in (state.pinned_reason or "")
        assert state.get_strikes(2) == STRIKE_LIMIT

    def test_invalid_slot_rejected(self, slot_env):
        with pytest.raises(core.InvalidSlotError):
            core.record_tryboot_strike(5)


class TestUnpin:
    def test_clears_pin_and_resets_strikes(self, slot_env):
        for _ in range(STRIKE_LIMIT):
            core.record_tryboot_strike(2)
        assert load_state().pinned is True

        state = core.unpin()
        assert state.pinned is False
        assert state.pinned_at is None
        assert state.pinned_reason is None
        assert state.strikes == {"1": 0, "2": 0}
        assert state.last_tryboot_target is None
        assert state.last_tryboot_at is None

    def test_unpin_is_idempotent(self, slot_env):
        core.unpin()
        state = core.unpin()  # no-op
        assert state.pinned is False
