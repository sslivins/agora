"""Tests for the :mod:`slot_confirm` package.

The 4 checks each have their own ``Test*`` class; the aggregator
:func:`slot_confirm.slot_confirm` has its own class; the CLI has
its own class. Every test runs without root, without /data, without
/dev/fb0, and without a real systemctl — every external surface is
injected via the documented seams (``runner``, ``opener``,
``writer``, ``reader``, ``slot_state_fn``).
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Sequence
from unittest.mock import patch

import pytest

from slot_confirm import (
    DEFAULT_AGORA_SERVICES,
    CheckResult,
    ConfirmStatus,
    SlotConfirmError,
    __version__,
    check_agora_services_active,
    check_data_writable,
    check_framebuffer,
    check_wps_connected,
    slot_confirm,
)
from slot_confirm import __main__ as cli
from slot_confirm import core as scc


# ── Fixtures / helpers ─────────────────────────────────────────────────────


FakeStatus = namedtuple("FakeStatus", ["running_slot", "default_slot", "tentative"])


def _fixed_now(seconds_ago: int = 0) -> datetime:
    base = datetime(2026, 4, 22, 19, 30, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds_ago)


def _show_output(
    active_state: str,
    entered_at: str = "",
    monotonic_us: str = "0",
) -> str:
    return (
        f"ActiveState={active_state}\n"
        f"ActiveEnterTimestamp={entered_at}\n"
        f"ActiveEnterTimestampMonotonic={monotonic_us}\n"
    )


def _systemctl_runner(
    per_service: dict[str, tuple[str, ...]],
) -> scc.Runner:
    """Build a runner that returns canned ``systemctl show`` output per service.

    ``per_service`` is ``{unit: (active_state, entered_raw)}`` or
    ``{unit: (active_state, entered_raw, monotonic_us)}``. The 2-tuple
    form is preserved for backward compatibility with the existing
    wallclock-only tests; missing monotonic defaults to ``"0"`` (the
    systemd sentinel for "never activated" — handled as wallclock
    fallback by :func:`check_agora_services_active`).
    """

    def runner(
        args: Sequence[str],
        *,
        check: bool = False,
        capture_output: bool = True,
        text: bool = True,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        unit = args[2]
        triple = per_service.get(unit, ("inactive", "", "0"))
        active = triple[0]
        entered = triple[1] if len(triple) > 1 else ""
        monotonic = triple[2] if len(triple) > 2 else "0"
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=0,
            stdout=_show_output(active, entered, monotonic),
            stderr="",
        )

    return runner


# ── _parse_systemd_timestamp ───────────────────────────────────────────────


class TestParseSystemdTimestamp:
    def test_dow_prefix_with_utc(self) -> None:
        ts = scc._parse_systemd_timestamp("Mon 2026-04-22 19:30:11 UTC")
        assert ts == datetime(2026, 4, 22, 19, 30, 11, tzinfo=timezone.utc)

    def test_no_dow_prefix_with_utc(self) -> None:
        ts = scc._parse_systemd_timestamp("2026-04-22 19:30:11 UTC")
        assert ts == datetime(2026, 4, 22, 19, 30, 11, tzinfo=timezone.utc)

    def test_no_timezone_suffix_treated_as_utc(self) -> None:
        ts = scc._parse_systemd_timestamp("2026-04-22 19:30:11")
        assert ts is not None
        assert ts.tzinfo is timezone.utc

    def test_empty_returns_none(self) -> None:
        assert scc._parse_systemd_timestamp("") is None

    def test_zero_returns_none(self) -> None:
        assert scc._parse_systemd_timestamp("0") is None

    def test_na_returns_none(self) -> None:
        assert scc._parse_systemd_timestamp("n/a") is None

    def test_malformed_returns_none(self) -> None:
        assert scc._parse_systemd_timestamp("not a date") is None


# ── check_agora_services_active ────────────────────────────────────────────


class TestCheckAgoraServicesActive:
    def test_happy_path(self) -> None:
        now = _fixed_now(0)
        entered = (now - timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S UTC")
        runner = _systemctl_runner(
            {svc: ("active", entered) for svc in DEFAULT_AGORA_SERVICES}
        )
        result = check_agora_services_active(runner=runner, now_fn=lambda: now)
        assert result.ok is True
        assert result.name == "agora_services_active"
        assert len(result.measurement["per_service"]) == len(DEFAULT_AGORA_SERVICES)

    def test_inactive_service_fails(self) -> None:
        now = _fixed_now(0)
        entered = (now - timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S UTC")
        # First service inactive, others fine.
        per = {svc: ("active", entered) for svc in DEFAULT_AGORA_SERVICES}
        per[DEFAULT_AGORA_SERVICES[0]] = ("inactive", entered)
        runner = _systemctl_runner(per)
        result = check_agora_services_active(runner=runner, now_fn=lambda: now)
        assert result.ok is False
        assert "inactive" in result.detail
        assert result.measurement["service"] == DEFAULT_AGORA_SERVICES[0]

    def test_too_young_fails(self) -> None:
        now = _fixed_now(0)
        entered = (now - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S UTC")
        runner = _systemctl_runner(
            {svc: ("active", entered) for svc in DEFAULT_AGORA_SERVICES}
        )
        result = check_agora_services_active(
            runner=runner, now_fn=lambda: now, min_active_seconds=300
        )
        assert result.ok is False
        assert "60s" in result.detail
        assert result.measurement["active_for_seconds"] == pytest.approx(60.0)

    def test_missing_timestamp_fails(self) -> None:
        now = _fixed_now(0)
        runner = _systemctl_runner(
            {svc: ("active", "") for svc in DEFAULT_AGORA_SERVICES}
        )
        result = check_agora_services_active(runner=runner, now_fn=lambda: now)
        assert result.ok is False
        assert "no ActiveEnterTimestamp" in result.detail

    def test_custom_service_list(self) -> None:
        now = _fixed_now(0)
        entered = (now - timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S UTC")
        runner = _systemctl_runner({"agora-api.service": ("active", entered)})
        result = check_agora_services_active(
            services=("agora-api.service",),
            runner=runner,
            now_fn=lambda: now,
        )
        assert result.ok is True
        assert result.measurement["services"] == ["agora-api.service"]

    def test_systemctl_missing(self) -> None:
        def runner(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError(2, "No such file or directory", "systemctl")

        result = check_agora_services_active(runner=runner, now_fn=_fixed_now)
        assert result.ok is False
        assert "systemctl not available" in result.detail

    def test_systemctl_timeout(self) -> None:
        def runner(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd="systemctl", timeout=10)

        result = check_agora_services_active(runner=runner, now_fn=_fixed_now)
        assert result.ok is False
        assert "timed out" in result.detail

    def test_negative_threshold_raises(self) -> None:
        with pytest.raises(SlotConfirmError, match="non-negative"):
            check_agora_services_active(min_active_seconds=-1, runner=lambda *a, **k: None)

    def test_empty_services_raises(self) -> None:
        with pytest.raises(SlotConfirmError, match="at least one"):
            check_agora_services_active(services=(), runner=lambda *a, **k: None)

    def test_default_runner_is_real_subprocess(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
            captured["args"] = args
            return subprocess.CompletedProcess(
                args=list(args[0]),
                returncode=0,
                stdout=_show_output("inactive"),
                stderr="",
            )

        monkeypatch.setattr(scc.subprocess, "run", fake_run)
        result = check_agora_services_active(
            services=("agora-api.service",), now_fn=_fixed_now
        )
        assert result.ok is False  # because we returned "inactive"
        assert captured["args"][0][0] == "systemctl"

    # ── monotonic clock path (bug #197) ────────────────────────────────────

    def test_uses_monotonic_clock_when_available(self) -> None:
        """Happy path: systemd reports ``ActiveEnterTimestampMonotonic``,
        clock-source is recorded as ``monotonic``, age math uses the
        injected monotonic-now-fn rather than the wallclock now-fn."""
        wall_now = _fixed_now(0)
        # 600s elapsed in monotonic clock; wallclock irrelevant.
        monotonic_now_s = 3600.0
        entered_monotonic_us = (3600.0 - 600.0) * 1_000_000
        entered_at = (wall_now - timedelta(seconds=600)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        runner = _systemctl_runner(
            {
                svc: ("active", entered_at, str(int(entered_monotonic_us)))
                for svc in DEFAULT_AGORA_SERVICES
            }
        )
        result = check_agora_services_active(
            runner=runner,
            now_fn=lambda: wall_now,
            monotonic_now_fn=lambda: monotonic_now_s,
        )
        assert result.ok is True
        per_service = result.measurement["per_service"]
        for svc in DEFAULT_AGORA_SERVICES:
            assert per_service[svc]["clock_source"] == "monotonic"
            assert per_service[svc]["active_for_seconds"] == pytest.approx(600.0)

    def test_clock_skew_immunity_via_monotonic(self) -> None:
        """The bug #197 repro: wallclock-derived age is negative because
        NTP stepped the clock forward after the service registered its
        ``ActiveEnterTimestamp``. With the monotonic property populated,
        the gate must ignore the corrupt wallclock and report passing."""
        wall_now = _fixed_now(0)
        # The corrupt wallclock entered_at is 1h *ahead* of now, which
        # would yield active_for_seconds=-3600 on the wallclock path
        # (exactly the symptom we saw on Pi100 v0.0.13).
        bad_entered_at = (wall_now + timedelta(seconds=3600)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        # Monotonic clock — independently — says 600s of uptime, which
        # is a clean pass against the 300s default threshold.
        monotonic_now_s = 1200.0
        entered_monotonic_us = 600 * 1_000_000
        runner = _systemctl_runner(
            {
                svc: ("active", bad_entered_at, str(entered_monotonic_us))
                for svc in DEFAULT_AGORA_SERVICES
            }
        )
        result = check_agora_services_active(
            runner=runner,
            now_fn=lambda: wall_now,
            monotonic_now_fn=lambda: monotonic_now_s,
        )
        assert result.ok is True, (
            f"monotonic path must mask wallclock skew, got {result}"
        )
        per_service = result.measurement["per_service"]
        for svc in DEFAULT_AGORA_SERVICES:
            assert per_service[svc]["clock_source"] == "monotonic"
            assert per_service[svc]["active_for_seconds"] == pytest.approx(600.0)

    def test_falls_back_to_wallclock_when_monotonic_zero(self) -> None:
        """systemd reports ``ActiveEnterTimestampMonotonic=0`` for units
        that have never activated (or for transient bugs we shouldn't
        amplify). The gate must fall back to the wallclock path and
        report ``clock_source="wallclock"`` so the caller can audit."""
        wall_now = _fixed_now(0)
        entered_at = (wall_now - timedelta(seconds=600)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        runner = _systemctl_runner(
            {
                svc: ("active", entered_at, "0")
                for svc in DEFAULT_AGORA_SERVICES
            }
        )
        result = check_agora_services_active(
            runner=runner,
            now_fn=lambda: wall_now,
            monotonic_now_fn=lambda: 9999.0,  # ignored
        )
        assert result.ok is True
        per_service = result.measurement["per_service"]
        for svc in DEFAULT_AGORA_SERVICES:
            assert per_service[svc]["clock_source"] == "wallclock"
            assert per_service[svc]["active_for_seconds"] == pytest.approx(600.0)

    def test_negative_age_treated_as_failure(self) -> None:
        """If the monotonic clock disagrees with the property (e.g. the
        property was sampled *after* the now-fn returned), the gate
        must not silently pass — it must report failure with
        ``measurement.reason='negative_age'``. This is the safety net
        in case there's an unanticipated clock-source ordering bug."""
        wall_now = _fixed_now(0)
        entered_at = (wall_now - timedelta(seconds=600)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        # Monotonic now < monotonic entered: -100s age.
        runner = _systemctl_runner(
            {
                svc: ("active", entered_at, str(1000 * 1_000_000))
                for svc in DEFAULT_AGORA_SERVICES
            }
        )
        result = check_agora_services_active(
            runner=runner,
            now_fn=lambda: wall_now,
            monotonic_now_fn=lambda: 900.0,
        )
        assert result.ok is False
        assert result.measurement["reason"] == "negative_age"
        assert result.measurement["clock_source"] == "monotonic"
        assert result.measurement["active_for_seconds"] < 0


# ── _parse_systemd_monotonic_us ────────────────────────────────────────────


class TestParseSystemdMonotonicUs:
    def test_zero_returns_none(self) -> None:
        assert scc._parse_systemd_monotonic_us("0") is None

    def test_empty_returns_none(self) -> None:
        assert scc._parse_systemd_monotonic_us("") is None

    def test_whitespace_returns_none(self) -> None:
        assert scc._parse_systemd_monotonic_us("   ") is None

    def test_non_integer_returns_none(self) -> None:
        assert scc._parse_systemd_monotonic_us("not-a-number") is None

    def test_negative_returns_none(self) -> None:
        assert scc._parse_systemd_monotonic_us("-1") is None

    def test_positive_returns_int(self) -> None:
        # 10s after boot expressed as microseconds.
        assert scc._parse_systemd_monotonic_us("10000000") == 10_000_000

    def test_strips_whitespace(self) -> None:
        assert scc._parse_systemd_monotonic_us("  12345  ") == 12345


# ── check_framebuffer ──────────────────────────────────────────────────────


class _FakeFile:
    def __init__(self, *, write_raises: BaseException | None = None) -> None:
        self.write_raises = write_raises
        self.written: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> int:
        if self.write_raises is not None:
            raise self.write_raises
        self.written.append(data)
        return len(data)

    def close(self) -> None:
        self.closed = True


class TestCheckFramebuffer:
    def test_happy_path(self) -> None:
        fake = _FakeFile()
        result = check_framebuffer(opener=lambda path: fake)
        assert result.ok is True
        assert fake.written == [b"\x00"]
        assert fake.closed is True

    def test_device_not_found(self) -> None:
        def opener(path: str) -> Any:
            raise FileNotFoundError(2, "No such file or directory", path)

        result = check_framebuffer(opener=opener)
        assert result.ok is False
        assert "not found" in result.detail
        assert result.measurement["errno"] == 2

    def test_permission_denied(self) -> None:
        def opener(path: str) -> Any:
            raise PermissionError(13, "Permission denied", path)

        result = check_framebuffer(opener=opener)
        assert result.ok is False
        assert "not writable" in result.detail
        assert result.measurement["errno"] == 13

    def test_open_oserror(self) -> None:
        def opener(path: str) -> Any:
            raise OSError(5, "Input/output error", path)

        result = check_framebuffer(opener=opener)
        assert result.ok is False
        assert "open failed" in result.detail

    def test_write_oserror_still_closes(self) -> None:
        fake = _FakeFile(write_raises=OSError(5, "I/O"))
        result = check_framebuffer(opener=lambda path: fake)
        assert result.ok is False
        assert "write failed" in result.detail
        assert fake.closed is True

    def test_custom_device(self) -> None:
        fake = _FakeFile()
        captured: dict[str, str] = {}

        def opener(path: str) -> Any:
            captured["path"] = path
            return fake

        result = check_framebuffer(device="/dev/fb1", opener=opener)
        assert captured["path"] == "/dev/fb1"
        assert result.measurement["device"] == "/dev/fb1"
        assert result.ok is True


# ── check_data_writable ────────────────────────────────────────────────────


class TestCheckDataWritable:
    def test_happy_path_with_writer_stub(self) -> None:
        captured: list[tuple[str, bytes]] = []

        def writer(path: str, payload: bytes) -> None:
            captured.append((path, payload))

        result = check_data_writable(writer=writer)
        assert result.ok is True
        assert len(captured) == 1
        # os.path.join uses backslashes on Windows, so just match the leaf.
        assert ".slot-confirm-probe-" in captured[0][0]
        assert b"slot-confirm" in captured[0][1]

    def test_dir_not_found(self) -> None:
        def writer(path: str, payload: bytes) -> None:
            raise FileNotFoundError(2, "No such file or directory", path)

        result = check_data_writable(writer=writer)
        assert result.ok is False
        assert "not found" in result.detail

    def test_permission_denied(self) -> None:
        def writer(path: str, payload: bytes) -> None:
            raise PermissionError(13, "Permission denied", path)

        result = check_data_writable(writer=writer)
        assert result.ok is False
        assert "write failed" in result.detail
        assert result.measurement["errno"] == 13

    def test_readonly_oserror(self) -> None:
        def writer(path: str, payload: bytes) -> None:
            raise OSError(30, "Read-only file system", path)

        result = check_data_writable(writer=writer)
        assert result.ok is False
        assert result.measurement["errno"] == 30

    def test_default_writer_real_file_roundtrip(self, tmp_path: Any) -> None:
        result = check_data_writable(probe_dir=str(tmp_path))
        assert result.ok is True
        # Probe file should have been cleaned up.
        assert list(tmp_path.iterdir()) == []

    def test_default_writer_cleanup_failure_still_ok(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If unlink raises, the check should still return ok=True — the
        # write succeeded, which is what we're testing.
        def _fail_unlink(path: Any) -> None:
            raise OSError(1, "cannot remove")

        monkeypatch.setattr(scc.os, "unlink", _fail_unlink)
        result = check_data_writable(probe_dir=str(tmp_path))
        assert result.ok is True
        # And the file is still there because unlink "failed".
        assert len(list(tmp_path.iterdir())) == 1


# ── check_wps_connected ────────────────────────────────────────────────────


def _reader_returning(content: str):
    def reader(path: str) -> str:
        return content

    return reader


def _reader_raising(exc: BaseException):
    def reader(path: str) -> str:
        raise exc

    return reader


class TestCheckWpsConnected:
    def test_happy_path(self) -> None:
        result = check_wps_connected(
            reader=_reader_returning(
                json.dumps({"state": "connected", "registration": "registered"})
            )
        )
        assert result.ok is True
        assert "registered" in result.detail
        assert result.measurement["state"] == "connected"

    def test_disconnected_fails(self) -> None:
        result = check_wps_connected(
            reader=_reader_returning(json.dumps({"state": "disconnected"}))
        )
        assert result.ok is False
        assert "'disconnected'" in result.detail

    def test_connecting_fails(self) -> None:
        result = check_wps_connected(
            reader=_reader_returning(json.dumps({"state": "connecting"}))
        )
        assert result.ok is False

    def test_error_state_fails(self) -> None:
        result = check_wps_connected(
            reader=_reader_returning(json.dumps({"state": "error", "error": "boom"}))
        )
        assert result.ok is False

    def test_missing_state_field_fails(self) -> None:
        result = check_wps_connected(
            reader=_reader_returning(json.dumps({"registration": "pending"}))
        )
        assert result.ok is False
        assert "None" in result.detail

    def test_file_not_found(self) -> None:
        result = check_wps_connected(
            reader=_reader_raising(FileNotFoundError(2, "no such file", "x"))
        )
        assert result.ok is False
        assert "not found" in result.detail

    def test_permission_denied(self) -> None:
        result = check_wps_connected(
            reader=_reader_raising(PermissionError(13, "denied", "x"))
        )
        assert result.ok is False
        assert "unreadable" in result.detail

    def test_invalid_json_fails(self) -> None:
        result = check_wps_connected(reader=_reader_returning("not json {{{"))
        assert result.ok is False
        assert "not valid JSON" in result.detail

    def test_non_object_json_fails(self) -> None:
        result = check_wps_connected(reader=_reader_returning('"connected"'))
        assert result.ok is False
        assert "not a JSON object" in result.detail

    def test_default_reader_real_file(self, tmp_path: Any) -> None:
        status_file = tmp_path / "cms_status.json"
        status_file.write_text(json.dumps({"state": "connected"}))
        result = check_wps_connected(status_path=str(status_file))
        assert result.ok is True


# ── slot_confirm aggregator ────────────────────────────────────────────────


def _ok_check(name: str) -> CheckResult:
    return CheckResult(name=name, ok=True, detail="ok")


def _fail_check(name: str) -> CheckResult:
    return CheckResult(name=name, ok=False, detail="bad")


class TestSlotConfirm:
    def test_skipped_when_not_tentative(self) -> None:
        status = slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=1, default_slot=1, tentative=False),
            agora_service_fn=lambda: pytest.fail("should not run"),
            framebuffer_fn=lambda: pytest.fail("should not run"),
            data_writable_fn=lambda: pytest.fail("should not run"),
            wps_connected_fn=lambda: pytest.fail("should not run"),
        )
        assert status.ok is True
        assert status.next_action == "skipped"
        assert status.checks == ()
        assert status.running_slot == 1
        assert status.tentative is False

    def test_error_when_slot_state_raises(self) -> None:
        def bad() -> Any:
            raise RuntimeError("boom")

        status = slot_confirm(
            slot_state_fn=bad,
            agora_service_fn=lambda: pytest.fail("should not run"),
            framebuffer_fn=lambda: pytest.fail("should not run"),
            data_writable_fn=lambda: pytest.fail("should not run"),
            wps_connected_fn=lambda: pytest.fail("should not run"),
        )
        assert status.ok is False
        assert status.next_action == "error"
        assert "boom" in status.error

    def test_promote_on_all_pass(self) -> None:
        status = slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=2, default_slot=1, tentative=True),
            agora_service_fn=lambda: _ok_check("agora_services_active"),
            framebuffer_fn=lambda: _ok_check("framebuffer"),
            data_writable_fn=lambda: _ok_check("data_writable"),
            wps_connected_fn=lambda: _ok_check("wps_connected"),
        )
        assert status.ok is True
        assert status.next_action == "promote"
        assert len(status.checks) == 4
        assert all(c.ok for c in status.checks)
        assert status.running_slot == 2
        assert status.tentative is True

    def test_strike_on_any_fail(self) -> None:
        status = slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=2, default_slot=1, tentative=True),
            agora_service_fn=lambda: _ok_check("agora_services_active"),
            framebuffer_fn=lambda: _ok_check("framebuffer"),
            data_writable_fn=lambda: _fail_check("data_writable"),
            wps_connected_fn=lambda: _ok_check("wps_connected"),
        )
        assert status.ok is False
        assert status.next_action == "strike"
        assert sum(1 for c in status.checks if not c.ok) == 1

    def test_strike_when_multiple_fail(self) -> None:
        status = slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=2, default_slot=1, tentative=True),
            agora_service_fn=lambda: _fail_check("agora_services_active"),
            framebuffer_fn=lambda: _fail_check("framebuffer"),
            data_writable_fn=lambda: _ok_check("data_writable"),
            wps_connected_fn=lambda: _ok_check("wps_connected"),
        )
        assert status.ok is False
        assert status.next_action == "strike"
        assert sum(1 for c in status.checks if not c.ok) == 2

    def test_runs_all_4_checks_in_order(self) -> None:
        order: list[str] = []

        def mk(name: str) -> Any:
            def fn() -> CheckResult:
                order.append(name)
                return _ok_check(name)

            return fn

        slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=2, default_slot=1, tentative=True),
            agora_service_fn=mk("services"),
            framebuffer_fn=mk("fb"),
            data_writable_fn=mk("data"),
            wps_connected_fn=mk("wps"),
        )
        assert order == ["services", "fb", "data", "wps"]

    def test_tentative_none_still_runs(self) -> None:
        # When tentative is None (unknown), we still want to run the gate
        # and emit a recommendation so the caller can decide.
        status = slot_confirm(
            slot_state_fn=lambda: FakeStatus(running_slot=1, default_slot=1, tentative=None),
            agora_service_fn=lambda: _ok_check("agora_services_active"),
            framebuffer_fn=lambda: _ok_check("framebuffer"),
            data_writable_fn=lambda: _ok_check("data_writable"),
            wps_connected_fn=lambda: _ok_check("wps_connected"),
        )
        assert status.next_action == "promote"
        assert status.tentative is None

    def test_default_slot_state_fn_consults_slot_mgr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_mod = SimpleNamespace(
            slot_state=lambda: FakeStatus(running_slot=1, default_slot=1, tentative=False)
        )
        monkeypatch.setitem(sys.modules, "slot_mgr", fake_mod)
        status = slot_confirm()
        assert status.next_action == "skipped"
        assert status.running_slot == 1


# ── CLI ────────────────────────────────────────────────────────────────────


class TestCli:
    def _capture(self, argv: list[str]) -> tuple[int, dict[str, Any]]:
        with patch.object(sys, "stdout", new=io.StringIO()) as buf:
            rc = cli.main(argv)
        return rc, json.loads(buf.getvalue())

    def test_default_run_exits_0_on_promote(self) -> None:
        promote_status = ConfirmStatus(
            ok=True,
            next_action="promote",
            checks=(_ok_check("framebuffer"),),
            running_slot=2,
            tentative=True,
        )
        with patch.object(cli, "slot_confirm", return_value=promote_status):
            rc, payload = self._capture([])
        assert rc == 0
        assert payload["next_action"] == "promote"
        assert payload["ok"] is True
        assert "action_taken" not in payload

    def test_default_run_exits_0_on_strike(self) -> None:
        strike_status = ConfirmStatus(
            ok=False,
            next_action="strike",
            checks=(_fail_check("framebuffer"),),
            running_slot=2,
            tentative=True,
        )
        with patch.object(cli, "slot_confirm", return_value=strike_status):
            rc, payload = self._capture([])
        assert rc == 0
        assert payload["next_action"] == "strike"

    def test_check_flag_exits_1_on_failure(self) -> None:
        strike_status = ConfirmStatus(
            ok=False, next_action="strike", running_slot=2, tentative=True
        )
        with patch.object(cli, "slot_confirm", return_value=strike_status):
            rc, _ = self._capture(["--check"])
        assert rc == 1

    def test_check_flag_exits_0_on_success(self) -> None:
        promote_status = ConfirmStatus(
            ok=True, next_action="promote", running_slot=2, tentative=True
        )
        with patch.object(cli, "slot_confirm", return_value=promote_status):
            rc, _ = self._capture(["--check"])
        assert rc == 0

    def test_check_flag_exits_0_when_skipped(self) -> None:
        skipped_status = ConfirmStatus(
            ok=True, next_action="skipped", running_slot=1, tentative=False
        )
        with patch.object(cli, "slot_confirm", return_value=skipped_status):
            rc, _ = self._capture(["--check"])
        assert rc == 0

    def test_auto_promote_calls_slot_mgr(self) -> None:
        promote_status = ConfirmStatus(
            ok=True, next_action="promote", running_slot=2, tentative=True
        )
        fake_promote: list[int] = []
        fake_mgr = SimpleNamespace(
            promote_slot=lambda slot: fake_promote.append(slot),
            record_tryboot_strike=lambda slot, *, reason: pytest.fail(
                "should not strike"
            ),
        )
        with patch.object(cli, "slot_confirm", return_value=promote_status), patch.dict(
            sys.modules, {"slot_mgr": fake_mgr}
        ):
            rc, payload = self._capture(["--auto"])
        assert rc == 0
        assert fake_promote == [2]
        assert payload["action_taken"] == "promote"
        assert payload["action_error"] == ""

    def test_auto_strike_calls_slot_mgr(self) -> None:
        strike_status = ConfirmStatus(
            ok=False, next_action="strike", running_slot=2, tentative=True
        )
        fake_strikes: list[tuple[int, str]] = []
        fake_mgr = SimpleNamespace(
            promote_slot=lambda slot: pytest.fail("should not promote"),
            record_tryboot_strike=lambda slot, *, reason: fake_strikes.append(
                (slot, reason)
            ),
        )
        with patch.object(cli, "slot_confirm", return_value=strike_status), patch.dict(
            sys.modules, {"slot_mgr": fake_mgr}
        ):
            rc, payload = self._capture(["--auto"])
        # strike still exits 0 — the gate ran successfully, the slot just
        # happened to fail. Only `next_action="error"` exits 2.
        assert rc == 0
        assert len(fake_strikes) == 1
        assert fake_strikes[0][0] == 2
        assert payload["action_taken"] == "strike"

    def test_auto_skipped_is_noop(self) -> None:
        skipped_status = ConfirmStatus(
            ok=True, next_action="skipped", running_slot=1, tentative=False
        )
        fake_mgr = SimpleNamespace(
            promote_slot=lambda slot: pytest.fail("noop"),
            record_tryboot_strike=lambda slot, *, reason: pytest.fail("noop"),
        )
        with patch.object(cli, "slot_confirm", return_value=skipped_status), patch.dict(
            sys.modules, {"slot_mgr": fake_mgr}
        ):
            rc, payload = self._capture(["--auto"])
        assert rc == 0
        assert payload["action_taken"] == "skipped"
        assert payload["action_error"] == ""

    def test_auto_error_exits_2(self) -> None:
        error_status = ConfirmStatus(
            ok=False,
            next_action="error",
            running_slot=None,
            tentative=None,
            error="boom",
        )
        with patch.object(cli, "slot_confirm", return_value=error_status):
            rc, payload = self._capture(["--auto"])
        assert rc == 2
        assert payload["action_taken"] == "none"
        assert "boom" in payload["action_error"]

    def test_auto_slot_mgr_raises_recorded(self) -> None:
        promote_status = ConfirmStatus(
            ok=True, next_action="promote", running_slot=2, tentative=True
        )

        def boom(slot: int) -> None:
            raise RuntimeError("disk full")

        fake_mgr = SimpleNamespace(
            promote_slot=boom,
            record_tryboot_strike=lambda slot, *, reason: pytest.fail("noop"),
        )
        with patch.object(cli, "slot_confirm", return_value=promote_status), patch.dict(
            sys.modules, {"slot_mgr": fake_mgr}
        ):
            rc, payload = self._capture(["--auto"])
        # rc=0 because gate completed; we just couldn't act.
        assert rc == 0
        assert payload["action_taken"] == "none"
        assert "disk full" in payload["action_error"]

    def test_payload_includes_check_details(self) -> None:
        promote_status = ConfirmStatus(
            ok=True,
            next_action="promote",
            checks=(
                CheckResult(
                    name="framebuffer",
                    ok=True,
                    detail="written",
                    measurement={"device": "/dev/fb0", "bytes_written": 1},
                ),
            ),
            running_slot=2,
            tentative=True,
        )
        with patch.object(cli, "slot_confirm", return_value=promote_status):
            _, payload = self._capture([])
        assert payload["checks"][0]["name"] == "framebuffer"
        assert payload["checks"][0]["measurement"]["device"] == "/dev/fb0"

    def test_version_flag(self) -> None:
        with patch.object(sys, "stdout", new=io.StringIO()) as buf:
            with pytest.raises(SystemExit) as exc_info:
                cli.main(["--version"])
        assert exc_info.value.code == 0
        assert __version__ in buf.getvalue()


# ── Module surface ─────────────────────────────────────────────────────────


class TestModuleSurface:
    def test_version_is_string(self) -> None:
        assert isinstance(__version__, str)
        assert __version__.count(".") == 2  # MAJOR.MINOR.PATCH

    def test_all_exports_resolve(self) -> None:
        import slot_confirm

        for name in slot_confirm.__all__:
            assert hasattr(slot_confirm, name), f"__all__ lists {name!r} but it's missing"

    def test_check_result_is_frozen(self) -> None:
        result = CheckResult(name="x", ok=True, detail="y")
        with pytest.raises(Exception):  # FrozenInstanceError subclass of AttributeError
            result.ok = False  # type: ignore[misc]

    def test_confirm_status_is_frozen(self) -> None:
        status = ConfirmStatus(ok=True, next_action="skipped")
        with pytest.raises(Exception):
            status.ok = False  # type: ignore[misc]

    def test_defaults_match_documented_values(self) -> None:
        assert scc.DEFAULT_MIN_ACTIVE_SECONDS == 300
        assert scc.DEFAULT_FRAMEBUFFER_DEVICE == "/dev/fb0"
        assert scc.DEFAULT_CMS_STATUS_PATH == "/opt/agora/state/cms_status.json"
        assert scc.DEFAULT_DATA_PROBE_DIR == "/data/agora"
        assert "agora-api.service" in DEFAULT_AGORA_SERVICES
