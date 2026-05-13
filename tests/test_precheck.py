"""Unit tests for :mod:`precheck`.

The pre-flight check library is built around four injectable seams
(``statvfs_fn``, ``stat_fn``, ``runner``, ``slot_state_fn``) so every
test runs on a developer laptop without ``/dev/watchdog0``, ``/proc``,
``timedatectl``, or a slot-mgr-compatible environment. The tests below
exercise the happy path, every documented failure path, and the
aggregator's contract.
"""

from __future__ import annotations

import errno
import os
import subprocess
from collections import namedtuple
from typing import Any, List, Sequence
from unittest.mock import patch

import pytest

from precheck import (
    CheckResult,
    DEFAULT_DATA_FREE_BYTES,
    DEFAULT_DATA_PATH,
    DEFAULT_WATCHDOG_DEVICE,
    DEFAULT_WATCHDOG_SERVICE,
    PrecheckError,
    check_data_free_space,
    check_inactive_slot_clean,
    check_ntp_fresh,
    check_watchdog_responsive,
    run_checks,
)
from precheck import core as precheck_core


# ── Helpers ─────────────────────────────────────────────────────────────────


# A minimal statvfs_result-style namedtuple — the real ``os.statvfs_result``
# only takes positional ints from the kernel and we just need .f_bavail,
# .f_frsize, .f_blocks for our caller. A namedtuple matches the access
# pattern exactly.
FakeStatvfs = namedtuple("FakeStatvfs", ["f_bavail", "f_frsize", "f_blocks"])


def _statvfs_yielding(result: FakeStatvfs):
    """Build a fake ``statvfs_fn`` that returns ``result`` and records calls."""
    calls: List[str] = []

    def fn(path: str) -> FakeStatvfs:
        calls.append(path)
        return result

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _statvfs_raising(exc: Exception):
    """Build a fake ``statvfs_fn`` that always raises ``exc``."""
    calls: List[str] = []

    def fn(path: str) -> FakeStatvfs:
        calls.append(path)
        raise exc

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _stat_present(path: str) -> os.stat_result:
    """Stand-in ``stat_fn`` for a device that exists."""
    # We never look at fields; only the absence of an exception matters.
    return os.stat_result((0o020666, 1, 1, 1, 0, 0, 0, 0, 0, 0))


def _stat_missing(path: str) -> os.stat_result:
    raise FileNotFoundError(errno.ENOENT, "No such file", path)


def _completed(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["fake"], returncode=returncode, stdout=stdout, stderr=stderr
    )


class FakeRunner:
    """Recording stand-in for :func:`subprocess.run`.

    Optionally returns a queued result or raises a queued exception
    on each call. Lets a test assert exact arg vectors without
    monkeypatching the real ``subprocess`` module.
    """

    def __init__(self, *, results: Sequence[Any] = (), raises: Sequence[Any] = ()):
        self._results = list(results)
        self._raises = list(raises)
        self.calls: List[dict[str, Any]] = []

    def __call__(self, args, **kwargs):
        self.calls.append({"args": list(args), **kwargs})
        if self._raises:
            exc = self._raises.pop(0)
            if exc is not None:
                raise exc
        if self._results:
            return self._results.pop(0)
        return _completed()


# ── check_data_free_space ───────────────────────────────────────────────────


class TestCheckDataFreeSpace:
    def test_pass_when_free_meets_threshold(self):
        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=5_000, f_frsize=1024 * 1024, f_blocks=10_000)
        )  # 5 GiB free of 10 GiB total

        result = check_data_free_space(
            path="/data",
            min_bytes=4 * 1024 * 1024 * 1024,
            statvfs_fn=statvfs_fn,
        )

        assert result.ok is True
        assert result.name == "data_free_space"
        assert result.measurement["path"] == "/data"
        assert result.measurement["free_bytes"] == 5_000 * 1024 * 1024
        assert result.measurement["total_bytes"] == 10_000 * 1024 * 1024
        assert result.measurement["min_bytes"] == 4 * 1024 * 1024 * 1024
        assert statvfs_fn.calls == ["/data"]  # type: ignore[attr-defined]

    def test_pass_exactly_at_threshold(self):
        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=1024, f_frsize=1024 * 1024, f_blocks=2048)
        )  # exactly 1 GiB free

        result = check_data_free_space(
            min_bytes=1024 * 1024 * 1024, statvfs_fn=statvfs_fn
        )

        assert result.ok is True

    def test_fail_when_below_threshold(self):
        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=1024, f_frsize=1024 * 1024, f_blocks=10_000)
        )  # 1 GiB free; need 4 GiB

        result = check_data_free_space(
            min_bytes=4 * 1024 * 1024 * 1024, statvfs_fn=statvfs_fn
        )

        assert result.ok is False
        assert "only" in result.detail.lower()
        assert "1.0 gib" in result.detail.lower()
        assert "4.0 gib" in result.detail.lower()

    def test_path_not_found(self):
        statvfs_fn = _statvfs_raising(
            FileNotFoundError(errno.ENOENT, "No such file", "/data")
        )

        result = check_data_free_space(path="/data", statvfs_fn=statvfs_fn)

        assert result.ok is False
        assert "not found" in result.detail
        assert result.measurement["errno"] == errno.ENOENT

    def test_oserror_propagates_as_failure(self):
        statvfs_fn = _statvfs_raising(OSError(errno.EIO, "I/O error", "/data"))

        result = check_data_free_space(path="/data", statvfs_fn=statvfs_fn)

        assert result.ok is False
        assert "i/o error" in result.detail.lower()
        assert result.measurement["errno"] == errno.EIO

    def test_negative_min_bytes_raises(self):
        with pytest.raises(PrecheckError, match="non-negative"):
            check_data_free_space(min_bytes=-1)

    def test_zero_min_bytes_passes(self):
        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=0, f_frsize=4096, f_blocks=0)
        )

        result = check_data_free_space(min_bytes=0, statvfs_fn=statvfs_fn)

        assert result.ok is True

    def test_default_path_is_data(self):
        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=10**9, f_frsize=4096, f_blocks=10**9)
        )

        check_data_free_space(statvfs_fn=statvfs_fn, min_bytes=0)

        assert statvfs_fn.calls == [DEFAULT_DATA_PATH]  # type: ignore[attr-defined]
        assert DEFAULT_DATA_PATH == "/data"

    def test_path_can_be_pathlike(self):
        from pathlib import PurePosixPath

        statvfs_fn = _statvfs_yielding(
            FakeStatvfs(f_bavail=10**6, f_frsize=4096, f_blocks=10**6)
        )

        result = check_data_free_space(
            path=PurePosixPath("/var/data"),
            min_bytes=0,
            statvfs_fn=statvfs_fn,
        )

        assert result.ok is True
        assert statvfs_fn.calls == ["/var/data"]  # type: ignore[attr-defined]


# ── check_ntp_fresh ─────────────────────────────────────────────────────────


class TestCheckNtpFresh:
    def test_yes_means_ok(self):
        runner = FakeRunner(results=[_completed(stdout="yes\n")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is True
        assert result.name == "ntp_fresh"
        assert result.measurement["ntp_synchronized"] == "yes"
        # Confirm we called the canonical command, not something else.
        assert runner.calls[0]["args"] == [
            "timedatectl",
            "show",
            "-p",
            "NTPSynchronized",
            "--value",
        ]

    def test_yes_case_insensitive(self):
        runner = FakeRunner(results=[_completed(stdout="YES\n")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is True

    def test_no_means_fail(self):
        runner = FakeRunner(results=[_completed(stdout="no\n")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "'no'" in result.detail
        assert result.measurement["ntp_synchronized"] == "no"

    def test_unexpected_output_means_fail(self):
        runner = FakeRunner(results=[_completed(stdout="maybe\n")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "'maybe'" in result.detail

    def test_missing_command(self):
        runner = FakeRunner(raises=[FileNotFoundError("no timedatectl")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "not installed" in result.detail
        assert result.measurement["reason"] == "missing_command"

    def test_timeout(self):
        runner = FakeRunner(
            raises=[subprocess.TimeoutExpired(cmd="timedatectl", timeout=5.0)]
        )

        result = check_ntp_fresh(runner=runner, timeout_s=5.0)

        assert result.ok is False
        assert "timed out" in result.detail
        assert result.measurement["reason"] == "timeout"
        assert result.measurement["timeout_s"] == 5.0

    def test_nonzero_exit(self):
        runner = FakeRunner(
            results=[_completed(returncode=1, stderr="Failed to connect\n")]
        )

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "exited 1" in result.detail
        assert "Failed to connect" in result.detail
        assert result.measurement["reason"] == "nonzero_exit"
        assert result.measurement["returncode"] == 1

    def test_nonzero_exit_no_stderr(self):
        runner = FakeRunner(results=[_completed(returncode=2, stderr="")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "exited 2" in result.detail

    def test_oserror(self):
        runner = FakeRunner(raises=[OSError(errno.EACCES, "Permission denied")])

        result = check_ntp_fresh(runner=runner)

        assert result.ok is False
        assert "permission denied" in result.detail.lower()
        assert result.measurement["reason"] == "oserror"
        assert result.measurement["errno"] == errno.EACCES


# ── check_inactive_slot_clean ───────────────────────────────────────────────


class FakeSlotStatus:
    """Stands in for :class:`slot_mgr.SlotStatus`. Only ``running_slot``
    is read by the precheck library."""

    def __init__(self, running_slot):
        self.running_slot = running_slot


class TestCheckInactiveSlotClean:
    def test_running_slot_1_checks_root_b(self, tmp_path):
        # Pre-create both partlabel symlinks so the device-exists guard
        # passes; we want to verify the inactive selection logic.
        (tmp_path / "root-A").write_text("")
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(results=[_completed(returncode=0)])

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
        )

        assert result.ok is True
        assert result.name == "inactive_slot_clean"
        assert result.measurement["inactive_slot"] == 2
        assert result.measurement["running_slot"] == 1
        assert str(tmp_path / "root-B") in runner.calls[0]["args"]
        # Confirm read-only flag is set so the test inadvertently can't
        # write to /dev.
        assert runner.calls[0]["args"][:2] == ["fsck", "-n"]

    def test_running_slot_2_checks_root_a(self, tmp_path):
        (tmp_path / "root-A").write_text("")
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(results=[_completed(returncode=0)])

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(2),
            runner=runner,
        )

        assert result.ok is True
        assert result.measurement["inactive_slot"] == 1
        assert str(tmp_path / "root-A") in runner.calls[0]["args"]

    def test_running_slot_none_fails(self, tmp_path):
        runner = FakeRunner()

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(None),
            runner=runner,
        )

        assert result.ok is False
        assert "running slot unknown" in result.detail
        assert result.measurement["reason"] == "running_slot_unknown"
        assert runner.calls == []  # never tried fsck

    def test_running_slot_invalid_int_fails(self, tmp_path):
        runner = FakeRunner()

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(7),
            runner=runner,
        )

        assert result.ok is False
        assert result.measurement["running_slot"] == 7
        assert runner.calls == []

    def test_partlabel_missing(self, tmp_path):
        # Don't create root-B
        runner = FakeRunner()

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
        )

        assert result.ok is False
        assert "device missing" in result.detail
        assert result.measurement["reason"] == "device_missing"
        assert runner.calls == []

    def test_fsck_missing_command(self, tmp_path):
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(raises=[FileNotFoundError("no fsck")])

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
        )

        assert result.ok is False
        assert "fsck not installed" in result.detail
        assert result.measurement["reason"] == "missing_command"

    def test_fsck_timeout(self, tmp_path):
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(
            raises=[subprocess.TimeoutExpired(cmd="fsck", timeout=120.0)]
        )

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
            timeout_s=120.0,
        )

        assert result.ok is False
        assert "timed out" in result.detail
        assert result.measurement["reason"] == "timeout"
        assert result.measurement["timeout_s"] == 120.0

    def test_fsck_oserror(self, tmp_path):
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(raises=[OSError(errno.EACCES, "Permission denied")])

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
        )

        assert result.ok is False
        assert "permission denied" in result.detail.lower()
        assert result.measurement["reason"] == "oserror"

    def test_fsck_nonzero_exit_is_failure(self, tmp_path):
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(
            results=[
                _completed(returncode=4, stdout="root-B: UNEXPECTED INCONSISTENCY\n")
            ]
        )

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=lambda: FakeSlotStatus(1),
            runner=runner,
        )

        assert result.ok is False
        assert "returned 4" in result.detail
        assert "UNEXPECTED INCONSISTENCY" in result.detail
        assert result.measurement["returncode"] == 4

    def test_slot_state_raises(self, tmp_path):
        def bad():
            raise RuntimeError("can't read /proc/cmdline")

        result = check_inactive_slot_clean(
            partlabel_base=tmp_path,
            slot_state_fn=bad,
            runner=FakeRunner(),
        )

        assert result.ok is False
        assert "slot_state() raised RuntimeError" in result.detail
        assert "can't read" in result.detail
        assert result.measurement["reason"] == "slot_state_error"

    def test_default_slot_state_lazy_import(self, tmp_path):
        # Verify the default slot_state_fn actually imports slot_mgr and
        # calls slot_state(). We don't want a real read of /proc/cmdline
        # in the test, so we patch slot_mgr.slot_state to a no-op.
        (tmp_path / "root-B").write_text("")
        runner = FakeRunner(results=[_completed(returncode=0)])

        with patch("slot_mgr.slot_state", return_value=FakeSlotStatus(1)) as m:
            result = check_inactive_slot_clean(
                partlabel_base=tmp_path,
                runner=runner,
                # slot_state_fn omitted on purpose — exercise default
            )

        assert m.called
        assert result.ok is True


# ── check_watchdog_responsive ───────────────────────────────────────────────


class TestCheckWatchdogResponsive:
    def test_device_and_service_active(self):
        runner = FakeRunner(results=[_completed(returncode=0, stdout="active\n")])

        result = check_watchdog_responsive(
            device="/dev/watchdog0",
            service="agora-watchdog.service",
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is True
        assert result.name == "watchdog_responsive"
        assert result.measurement["service_state"] == "active"
        assert result.measurement["returncode"] == 0
        assert runner.calls[0]["args"] == [
            "systemctl",
            "is-active",
            "agora-watchdog.service",
        ]

    def test_device_missing(self):
        runner = FakeRunner()

        result = check_watchdog_responsive(
            device="/dev/watchdog0",
            stat_fn=_stat_missing,
            runner=runner,
        )

        assert result.ok is False
        assert "watchdog device missing" in result.detail
        assert result.measurement["reason"] == "device_missing"
        assert runner.calls == []  # short-circuited before systemctl

    def test_device_stat_other_oserror(self):
        def stat_eio(path):
            raise OSError(errno.EIO, "I/O error", path)

        runner = FakeRunner()

        result = check_watchdog_responsive(
            device="/dev/watchdog0",
            stat_fn=stat_eio,
            runner=runner,
        )

        assert result.ok is False
        assert "i/o error" in result.detail.lower()
        assert result.measurement["reason"] == "device_stat_error"
        assert runner.calls == []

    def test_service_inactive(self):
        runner = FakeRunner(
            results=[_completed(returncode=3, stdout="inactive\n")]
        )

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is False
        assert "'inactive'" in result.detail
        assert "returncode=3" in result.detail
        assert result.measurement["service_state"] == "inactive"

    def test_service_active_but_systemctl_nonzero(self):
        # systemctl is-active normally returns 0 for "active" and 3 for
        # "inactive" / "failed". A 0 with stdout "active" is the only
        # combination we accept.
        runner = FakeRunner(
            results=[_completed(returncode=3, stdout="active\n")]
        )

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is False

    def test_systemctl_missing(self):
        runner = FakeRunner(raises=[FileNotFoundError("no systemctl")])

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is False
        assert "systemctl not installed" in result.detail
        assert result.measurement["reason"] == "missing_command"

    def test_systemctl_timeout(self):
        runner = FakeRunner(
            raises=[subprocess.TimeoutExpired(cmd="systemctl", timeout=5.0)]
        )

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
            timeout_s=5.0,
        )

        assert result.ok is False
        assert "timed out" in result.detail
        assert result.measurement["reason"] == "timeout"
        assert result.measurement["timeout_s"] == 5.0

    def test_systemctl_oserror(self):
        runner = FakeRunner(raises=[OSError(errno.EACCES, "Permission denied")])

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is False
        assert "permission denied" in result.detail.lower()
        assert result.measurement["reason"] == "oserror"

    def test_default_device_and_service(self):
        runner = FakeRunner(results=[_completed(returncode=0, stdout="active\n")])

        result = check_watchdog_responsive(
            stat_fn=_stat_present,
            runner=runner,
        )

        assert result.ok is True
        assert result.measurement["device"] == DEFAULT_WATCHDOG_DEVICE
        assert result.measurement["service"] == DEFAULT_WATCHDOG_SERVICE
        assert DEFAULT_WATCHDOG_DEVICE == "/dev/watchdog0"
        assert DEFAULT_WATCHDOG_SERVICE == "agora-watchdog.service"


# ── run_checks aggregator ───────────────────────────────────────────────────


def _ok(name="x") -> CheckResult:
    return CheckResult(name=name, ok=True, detail="fine")


def _bad(name="x") -> CheckResult:
    return CheckResult(name=name, ok=False, detail="broken")


class TestRunChecks:
    def test_empty_returns_all_ok(self):
        all_ok, results = run_checks([])
        assert all_ok is True
        assert results == []

    def test_all_pass(self):
        all_ok, results = run_checks(
            [lambda: _ok("a"), lambda: _ok("b"), lambda: _ok("c")]
        )
        assert all_ok is True
        assert [r.name for r in results] == ["a", "b", "c"]

    def test_one_failure_flips_all_ok_false(self):
        all_ok, results = run_checks(
            [lambda: _ok("a"), lambda: _bad("b"), lambda: _ok("c")]
        )
        assert all_ok is False
        # All results are collected even when one fails — no short-circuit.
        assert [r.name for r in results] == ["a", "b", "c"]
        assert results[1].ok is False

    def test_checks_run_in_order(self):
        order: List[str] = []

        def mk(name):
            def fn():
                order.append(name)
                return _ok(name)

            return fn

        run_checks([mk("first"), mk("second"), mk("third")])
        assert order == ["first", "second", "third"]

    def test_non_check_result_raises_programmer_error(self):
        with pytest.raises(PrecheckError, match="expected CheckResult"):
            run_checks([lambda: "not a result"])  # type: ignore[list-item]

    def test_callable_raising_propagates(self):
        # A check raising an unexpected exception is a bug; we don't
        # swallow it into ok=False.
        def boom():
            raise ValueError("oops")

        with pytest.raises(ValueError, match="oops"):
            run_checks([lambda: _ok(), boom, lambda: _ok()])


# ── Misc invariants ─────────────────────────────────────────────────────────


class TestModuleSurface:
    def test_check_result_is_frozen(self):
        r = _ok()
        with pytest.raises(Exception):
            r.ok = False  # type: ignore[misc]

    def test_check_result_measurement_defaults_to_empty(self):
        r = CheckResult(name="x", ok=True, detail="")
        assert r.measurement == {}

    def test_precheck_error_is_runtimeerror(self):
        assert issubclass(PrecheckError, RuntimeError)

    def test_defaults_exported_at_package_level(self):
        # If we forget to re-export a default in __init__.py, callers
        # break silently. Lock the public surface.
        import precheck

        assert precheck.DEFAULT_DATA_PATH == "/data"
        assert precheck.DEFAULT_DATA_FREE_BYTES == 4 * 1024**3
        assert precheck.DEFAULT_WATCHDOG_DEVICE == "/dev/watchdog0"
        assert precheck.DEFAULT_WATCHDOG_SERVICE == "agora-watchdog.service"
        assert precheck.DEFAULT_PARTLABEL_BASE == "/dev/disk/by-partlabel"

    def test_humanize_bytes_units(self):
        from precheck.core import _humanize_bytes

        assert _humanize_bytes(0) == "0.0 B"
        assert _humanize_bytes(1023) == "1023.0 B"
        assert _humanize_bytes(1024) == "1.0 KiB"
        assert _humanize_bytes(1024 * 1024) == "1.0 MiB"
        assert _humanize_bytes(4 * 1024**3) == "4.0 GiB"
        assert _humanize_bytes(2 * 1024**4) == "2.0 TiB"

    def test_last_nonblank_line(self):
        from precheck.core import _last_nonblank_line

        assert _last_nonblank_line("") == ""
        assert _last_nonblank_line("\n\n") == ""
        assert _last_nonblank_line("only one") == "only one"
        assert _last_nonblank_line("first\nsecond\n\n") == "second"
        assert _last_nonblank_line("a  \n  b  \n") == "b"
