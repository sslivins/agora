"""Tests for the update-tester synthetic-load battery."""

from __future__ import annotations

import io
import json
import os
import subprocess
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from unittest import mock

from update_tester import core
from update_tester.core import (
    DEFAULT_RENDER_DURATION_SECONDS,
    DEFAULT_RENDER_FPS_TOLERANCE,
    DEFAULT_RENDER_TARGET_FPS,
    DEFAULT_STRESS_DURATION_SECONDS,
    DEFAULT_THERMAL_THROTTLE_CELSIUS,
    DEFAULT_WPS_FRESHNESS_SECONDS,
    TestBatteryResult,
    TestResult,
    UpdateTesterError,
    battery_to_dict,
    check_data_integrity,
    check_render_canary,
    check_stress,
    check_wps_synthetic,
    run_test_battery,
)

# Stop pytest from trying to collect these as test classes — they're dataclasses
# named "TestResult" / "TestBatteryResult" from the library under test.
TestResult.__test__ = False  # type: ignore[attr-defined]
TestBatteryResult.__test__ = False  # type: ignore[attr-defined]


# ── Helpers ────────────────────────────────────────────────────────────────


def _utc(year: int = 2026, month: int = 5, day: int = 13, hour: int = 12) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


class _TickingNow:
    """``now_fn`` substitute that ticks forward a fixed delta per call.

    Returns ``start`` on the first call, then ``start + step`` on the
    second, and so on. Lets the render canary write frames without
    real sleeps while keeping wall-clock arithmetic exact.
    """

    def __init__(self, start: datetime, step_seconds: float) -> None:
        self._next = start
        self._step = timedelta(seconds=step_seconds)
        self.calls = 0

    def __call__(self) -> datetime:
        out = self._next
        self._next = self._next + self._step
        self.calls += 1
        return out


class _FixedSequenceNow:
    """``now_fn`` that returns a fixed list of timestamps, then the last."""

    def __init__(self, values: list[datetime]) -> None:
        assert values, "need at least one value"
        self._values = list(values)
        self._last = values[-1]
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        if self._values:
            return self._values.pop(0)
        return self._last


class _FakeFile:
    """In-memory file stub supporting write/read/close/flush/fileno."""

    def __init__(self, name: str = "<fake>", mode: str = "wb") -> None:
        self.name = name
        self.mode = mode
        self.buffer = bytearray()
        self.write_calls = 0
        self.read_calls = 0
        self.closed = False
        self._read_offset = 0

    def write(self, data: bytes) -> int:
        if self.closed:
            raise ValueError("write on closed file")
        self.buffer.extend(data)
        self.write_calls += 1
        return len(data)

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("read on closed file")
        self.read_calls += 1
        if size < 0 or size > len(self.buffer) - self._read_offset:
            chunk = bytes(self.buffer[self._read_offset :])
            self._read_offset = len(self.buffer)
        else:
            chunk = bytes(
                self.buffer[self._read_offset : self._read_offset + size]
            )
            self._read_offset += size
        return chunk

    def flush(self) -> None:
        return None

    def fileno(self) -> int:
        raise OSError("no fileno in fake")

    def close(self) -> None:
        self.closed = True


def _make_runner(responses: dict[str, "subprocess.CompletedProcess[str]"]):
    """Build a fake runner keyed by the first arg of each invocation.

    Each response is reusable. Calls to unknown commands raise
    ``AssertionError`` so tests catch unexpected subprocess use.
    """

    def runner(
        args, *, check=False, capture_output=True, text=True, timeout=None
    ):
        first = args[0]
        if first not in responses:
            raise AssertionError(f"unexpected runner call: {args!r}")
        return responses[first]

    return runner


def _completed(
    returncode: int = 0, stdout: str = "", stderr: str = ""
) -> "subprocess.CompletedProcess[str]":
    return subprocess.CompletedProcess(
        args=["fake"], returncode=returncode, stdout=stdout, stderr=stderr
    )


# ── 1. Render canary ───────────────────────────────────────────────────────


class TestCheckRenderCanary(unittest.TestCase):
    def test_happy_path_hits_target_fps(self) -> None:
        # 10 fps target × 10 s with _TickingNow step 0.1 s yields 99 frames
        # written in ~10.1 s of mocked wall time (1 extra now() call for
        # `finished`), giving ~9.80 fps — well inside the 8.5–11.5 fps
        # tolerance band. Using a longer mocked duration keeps the
        # off-by-one edge effect of the loop's elapsed check tiny.
        now = _TickingNow(_utc(), 1.0 / 10)
        fake = _FakeFile(name="/dev/fb0")
        sleeps: list[float] = []
        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=10.0,
            target_fps=10.0,
            opener=lambda path: fake,
            sleeper=sleeps.append,
            now_fn=now,
        )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.name, "render_canary")
        # Loop exits when elapsed >= duration; 99 writes precede that exit.
        self.assertEqual(result.measurement["frames_written"], 99)
        self.assertGreaterEqual(
            result.measurement["achieved_fps"], result.measurement["fps_low"]
        )
        self.assertLessEqual(
            result.measurement["achieved_fps"], result.measurement["fps_high"]
        )
        self.assertEqual(len(sleeps), 99)
        for s in sleeps:
            self.assertAlmostEqual(s, 0.1)

    def test_missing_device_fails(self) -> None:
        def opener(_path: str) -> Any:
            raise FileNotFoundError(2, "no such device", "/dev/fb0")

        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            opener=opener,
            sleeper=lambda _s: None,
            now_fn=_TickingNow(_utc(), 0.05),
        )
        self.assertFalse(result.ok)
        self.assertIn("not found", result.detail)
        self.assertEqual(result.measurement["errno"], 2)

    def test_permission_denied_fails(self) -> None:
        def opener(_path: str) -> Any:
            raise PermissionError(13, "denied", "/dev/fb0")

        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            opener=opener,
            sleeper=lambda _s: None,
            now_fn=_TickingNow(_utc(), 0.05),
        )
        self.assertFalse(result.ok)
        self.assertIn("not writable", result.detail)
        self.assertEqual(result.measurement["errno"], 13)

    def test_generic_oserror_on_open_fails(self) -> None:
        def opener(_path: str) -> Any:
            raise OSError(5, "I/O error")

        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            opener=opener,
            sleeper=lambda _s: None,
            now_fn=_TickingNow(_utc(), 0.05),
        )
        self.assertFalse(result.ok)
        self.assertIn("open failed", result.detail)
        self.assertEqual(result.measurement["errno"], 5)

    def test_write_error_mid_stream_fails(self) -> None:
        class BadWrite(_FakeFile):
            def write(self, data: bytes) -> int:
                if self.write_calls >= 2:
                    raise OSError(5, "EIO mid-write")
                return super().write(data)

        bad = BadWrite()
        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            opener=lambda _p: bad,
            sleeper=lambda _s: None,
            now_fn=_TickingNow(_utc(), 0.05),
        )
        self.assertFalse(result.ok)
        self.assertIn("write failed", result.detail)
        self.assertEqual(result.measurement["errno"], 5)
        self.assertTrue(bad.closed)

    def test_too_slow_fps_fails(self) -> None:
        # Tick by 1.0 s/call → only 1 frame in 1 s → 1 fps, target 10.
        now = _TickingNow(_utc(), 1.0)
        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            fps_tolerance=0.15,
            opener=lambda _p: _FakeFile(),
            sleeper=lambda _s: None,
            now_fn=now,
        )
        self.assertFalse(result.ok)
        self.assertIn("too slow", result.detail)
        self.assertLess(
            result.measurement["achieved_fps"], result.measurement["fps_low"]
        )

    def test_too_fast_fps_fails(self) -> None:
        # Tick by 0.001 s/call → 1000 frames in 1 s → 1000 fps, target 10.
        now = _TickingNow(_utc(), 0.001)
        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=1.0,
            target_fps=10.0,
            fps_tolerance=0.15,
            opener=lambda _p: _FakeFile(),
            sleeper=lambda _s: None,
            now_fn=now,
        )
        self.assertFalse(result.ok)
        self.assertIn("too fast", result.detail)

    def test_zero_duration_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_render_canary(duration_seconds=0)

    def test_zero_target_fps_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_render_canary(target_fps=0)

    def test_tolerance_out_of_range_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_render_canary(fps_tolerance=1.0)
        with self.assertRaises(UpdateTesterError):
            check_render_canary(fps_tolerance=-0.1)

    def test_empty_frame_bytes_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_render_canary(frame_bytes=b"")

    def test_measurement_records_frame_byte_size(self) -> None:
        now = _TickingNow(_utc(), 0.1)
        result = check_render_canary(
            device="/dev/fb0",
            duration_seconds=10.0,
            target_fps=10.0,
            frame_bytes=b"\x00\xff\xaa",
            opener=lambda _p: _FakeFile(),
            sleeper=lambda _s: None,
            now_fn=now,
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.measurement["frame_bytes"], 3)


# ── 2. WPS end-to-end ──────────────────────────────────────────────────────


def _wps_status_text(
    *, state: str = "connected", last_seen_at: str = "2026-05-13T11:59:59Z"
) -> str:
    return json.dumps({"state": state, "last_seen_at": last_seen_at})


class TestCheckWpsSynthetic(unittest.TestCase):
    def test_happy_path(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now - timedelta(seconds=5)).isoformat()
        )
        result = check_wps_synthetic(
            status_path="/opt/agora/state/cms_status.json",
            reader=lambda _p: text,
            now_fn=lambda: now,
        )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.name, "wps_synthetic")
        self.assertEqual(result.measurement["state"], "connected")
        self.assertAlmostEqual(
            result.measurement["last_seen_age_seconds"], 5.0, places=1
        )

    def test_missing_status_file_fails(self) -> None:
        def reader(_p: str) -> str:
            raise FileNotFoundError(2, "no such file")

        result = check_wps_synthetic(
            reader=reader, now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("not found", result.detail)
        self.assertEqual(result.measurement["errno"], 2)

    def test_permission_denied_fails(self) -> None:
        def reader(_p: str) -> str:
            raise PermissionError(13, "denied")

        result = check_wps_synthetic(
            reader=reader, now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("read failed", result.detail)
        self.assertEqual(result.measurement["errno"], 13)

    def test_invalid_json_fails(self) -> None:
        result = check_wps_synthetic(
            reader=lambda _p: "not json {{{",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("JSON invalid", result.detail)

    def test_non_object_json_fails(self) -> None:
        result = check_wps_synthetic(
            reader=lambda _p: "[1,2,3]", now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("not an object", result.detail)

    def test_disconnected_state_fails(self) -> None:
        text = _wps_status_text(state="disconnected")
        result = check_wps_synthetic(
            reader=lambda _p: text, now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("disconnected", result.detail)
        self.assertEqual(result.measurement["state"], "disconnected")

    def test_missing_last_seen_fails(self) -> None:
        text = json.dumps({"state": "connected"})
        result = check_wps_synthetic(
            reader=lambda _p: text, now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("last_seen_at", result.detail)

    def test_unparseable_last_seen_fails(self) -> None:
        text = _wps_status_text(last_seen_at="garbage")
        result = check_wps_synthetic(
            reader=lambda _p: text, now_fn=lambda: _utc()
        )
        self.assertFalse(result.ok)
        self.assertIn("last_seen_at", result.detail)

    def test_stale_heartbeat_fails(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now - timedelta(seconds=120)).isoformat()
        )
        result = check_wps_synthetic(
            freshness_seconds=30, reader=lambda _p: text, now_fn=lambda: now
        )
        self.assertFalse(result.ok)
        self.assertIn("stale", result.detail)

    def test_future_heartbeat_fails(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now + timedelta(seconds=600)).isoformat()
        )
        result = check_wps_synthetic(
            freshness_seconds=30, reader=lambda _p: text, now_fn=lambda: now
        )
        self.assertFalse(result.ok)
        self.assertIn("future", result.detail)

    def test_dispatcher_invoked_and_recorded(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now - timedelta(seconds=5)).isoformat()
        )
        seen: list[bool] = []

        def dispatcher() -> Mapping[str, Any]:
            seen.append(True)
            return {"id": "synth-42", "ack": True}

        result = check_wps_synthetic(
            reader=lambda _p: text,
            now_fn=lambda: now,
            dispatcher=dispatcher,
        )
        self.assertTrue(result.ok)
        self.assertEqual(seen, [True])
        self.assertEqual(
            result.measurement["dispatched"], {"id": "synth-42", "ack": True}
        )

    def test_dispatcher_nonmapping_wrapped(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now - timedelta(seconds=5)).isoformat()
        )
        result = check_wps_synthetic(
            reader=lambda _p: text,
            now_fn=lambda: now,
            dispatcher=lambda: "ok",
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.measurement["dispatched"], {"value": "ok"})

    def test_dispatcher_raise_fails(self) -> None:
        now = _utc()
        text = _wps_status_text(
            last_seen_at=(now - timedelta(seconds=5)).isoformat()
        )

        def dispatcher() -> Mapping[str, Any]:
            raise RuntimeError("websocket gone")

        result = check_wps_synthetic(
            reader=lambda _p: text,
            now_fn=lambda: now,
            dispatcher=dispatcher,
        )
        self.assertFalse(result.ok)
        self.assertIn("dispatcher raised", result.detail)

    def test_zero_freshness_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_wps_synthetic(freshness_seconds=0)

    def test_handles_naive_datetime_string(self) -> None:
        now = _utc()
        # Naive ISO string (no offset) — module treats as UTC.
        naive = (now - timedelta(seconds=5)).replace(tzinfo=None).isoformat()
        text = _wps_status_text(last_seen_at=naive)
        result = check_wps_synthetic(
            reader=lambda _p: text, now_fn=lambda: now
        )
        self.assertTrue(result.ok, result.detail)


# ── 3. Memory/CPU stress ───────────────────────────────────────────────────


class TestCheckStress(unittest.TestCase):
    def _runner(
        self,
        *,
        stress_rc: int = 0,
        stress_stdout: str = "",
        stress_stderr: str = "",
        dmesg_text: str = "",
        timeout: bool = False,
        missing: bool = False,
    ):
        def runner(args, **kwargs):
            if args[0] == "dmesg":
                return _completed(0, dmesg_text, "")
            if args[0] == "stress-ng":
                if missing:
                    raise FileNotFoundError(2, "no such file", "stress-ng")
                if timeout:
                    raise subprocess.TimeoutExpired(args, kwargs.get("timeout", 60))
                return _completed(stress_rc, stress_stdout, stress_stderr)
            raise AssertionError(f"unexpected runner call: {args!r}")

        return runner

    def test_happy_path(self) -> None:
        result = check_stress(
            duration_seconds=10,
            cpu_workers=1,
            vm_workers=0,
            runner=self._runner(stress_rc=0, dmesg_text="kernel boot OK\n"),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.name, "stress")
        self.assertEqual(result.measurement["returncode"], 0)
        self.assertEqual(result.measurement["new_oom"], 0)
        self.assertEqual(result.measurement["post_thermal_celsius"], 55.0)

    def test_missing_binary_fails(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(missing=True),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("not found", result.detail)
        self.assertEqual(result.measurement["errno"], 2)

    def test_nonzero_returncode_fails(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(
                stress_rc=2, stress_stderr="vm: unable to mmap\nbailing\n"
            ),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("returncode=2", result.detail)
        self.assertIn("stderr_excerpt", result.measurement)

    def test_thermal_throttle_fails(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(),
            thermal_reader=lambda _p: "82000",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("thermal", result.detail)
        self.assertGreaterEqual(
            result.measurement["post_thermal_celsius"],
            DEFAULT_THERMAL_THROTTLE_CELSIUS,
        )

    def test_thermal_just_below_throttle_passes(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(),
            thermal_reader=lambda _p: "79900",
            now_fn=lambda: _utc(),
        )
        self.assertTrue(result.ok, result.detail)

    def test_oom_detected_fails(self) -> None:
        dmesg = (
            "[12345.0] all good\n"
            "[12345.5] kernel: Out of memory: Killed process 999 (stress-ng)\n"
        )
        # First runner call (pre-dmesg) has zero OOM hits; second call
        # (post-dmesg) has one. Use a stateful runner.
        calls = {"n": 0}

        def runner(args, **kwargs):
            if args[0] == "dmesg":
                calls["n"] += 1
                if calls["n"] == 1:
                    return _completed(0, "[12300.0] clean boot\n", "")
                return _completed(0, dmesg, "")
            if args[0] == "stress-ng":
                return _completed(0, "", "")
            raise AssertionError(args)

        result = check_stress(
            duration_seconds=10,
            runner=runner,
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("OOM", result.detail)
        self.assertEqual(result.measurement["new_oom"], 1)

    def test_preexisting_oom_doesnt_fail(self) -> None:
        # Same OOM line in both pre- and post-dmesg → new_oom = 0.
        oom_text = "[1.0] Out of memory: Killed process 99 (oldproc)\n"

        def runner(args, **_kwargs):
            if args[0] == "dmesg":
                return _completed(0, oom_text, "")
            if args[0] == "stress-ng":
                return _completed(0, "", "")
            raise AssertionError(args)

        result = check_stress(
            duration_seconds=10,
            runner=runner,
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.measurement["new_oom"], 0)
        self.assertEqual(result.measurement["pre_dmesg_oom"], 1)
        self.assertEqual(result.measurement["post_dmesg_oom"], 1)

    def test_dmesg_unavailable_is_nonfatal(self) -> None:
        def runner(args, **_kwargs):
            if args[0] == "dmesg":
                raise FileNotFoundError(2, "no dmesg")
            if args[0] == "stress-ng":
                return _completed(0, "", "")
            raise AssertionError(args)

        result = check_stress(
            duration_seconds=10,
            runner=runner,
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.measurement["pre_dmesg_oom"], 0)
        self.assertEqual(result.measurement["post_dmesg_oom"], 0)

    def test_stress_timeout_fails(self) -> None:
        def runner(args, **kwargs):
            if args[0] == "dmesg":
                return _completed(0, "", "")
            if args[0] == "stress-ng":
                raise subprocess.TimeoutExpired(args, kwargs.get("timeout", 60))
            raise AssertionError(args)

        result = check_stress(
            duration_seconds=10,
            runner=runner,
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("wall-clock", result.detail)

    def test_duration_zero_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_stress(duration_seconds=0)

    def test_negative_worker_count_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_stress(cpu_workers=-1)
        with self.assertRaises(UpdateTesterError):
            check_stress(vm_workers=-1)

    def test_zero_workers_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_stress(cpu_workers=0, vm_workers=0)

    def test_invalid_thermal_returns_none(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(),
            thermal_reader=lambda _p: "not-a-number",
            now_fn=lambda: _utc(),
        )
        # Stress check passes — thermal None means we can't enforce.
        self.assertTrue(result.ok, result.detail)
        self.assertIsNone(result.measurement["post_thermal_celsius"])

    def test_thermal_plain_float_string(self) -> None:
        result = check_stress(
            duration_seconds=10,
            runner=self._runner(),
            thermal_reader=lambda _p: "55.0",
            now_fn=lambda: _utc(),
        )
        # 55.0 in the file is parsed as 55 °C (not 55 mC), since the
        # int parse fails and float parse succeeds.
        self.assertTrue(result.ok)
        self.assertEqual(result.measurement["post_thermal_celsius"], 55.0)

    def test_args_list_records_workers(self) -> None:
        result = check_stress(
            duration_seconds=300,
            cpu_workers=4,
            vm_workers=2,
            vm_bytes="256M",
            runner=self._runner(),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        args = result.measurement["args"]
        self.assertIn("--cpu", args)
        self.assertIn("4", args)
        self.assertIn("--vm", args)
        self.assertIn("2", args)
        self.assertIn("--vm-bytes", args)
        self.assertIn("256M", args)
        self.assertIn("--timeout", args)
        self.assertIn("300s", args)

    def test_cpu_workers_zero_omits_cpu_flag(self) -> None:
        result = check_stress(
            duration_seconds=10,
            cpu_workers=0,
            vm_workers=2,
            runner=self._runner(),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        args = result.measurement["args"]
        self.assertNotIn("--cpu", args)
        self.assertIn("--vm", args)

    def test_vm_workers_zero_omits_vm_flag(self) -> None:
        result = check_stress(
            duration_seconds=10,
            cpu_workers=1,
            vm_workers=0,
            runner=self._runner(),
            thermal_reader=lambda _p: "55000",
            now_fn=lambda: _utc(),
        )
        args = result.measurement["args"]
        self.assertNotIn("--vm", args)
        self.assertIn("--cpu", args)


# ── 4. /data integrity sweep ───────────────────────────────────────────────


class _Scratch:
    """Stateful scratch opener that backs files in memory by path."""

    def __init__(self) -> None:
        self.files: dict[str, _FakeFile] = {}
        self.unlinked: list[str] = []
        self.open_calls: list[tuple[str, str]] = []

    def __call__(self, path: str, mode: str) -> _FakeFile:
        self.open_calls.append((path, mode))
        if "w" in mode:
            fh = _FakeFile(name=path, mode=mode)
            self.files[path] = fh
            return fh
        if "r" in mode:
            existing = self.files.get(path)
            if existing is None:
                raise FileNotFoundError(2, "no such file", path)
            existing._read_offset = 0
            existing.closed = False
            existing.mode = mode
            return existing
        raise AssertionError(f"unexpected mode {mode!r}")


class TestCheckDataIntegrity(unittest.TestCase):
    def test_happy_path_round_trip(self) -> None:
        scratch = _Scratch()
        unlinks: list[str] = []
        with mock.patch.object(
            core.os, "unlink", side_effect=unlinks.append
        ):
            result = check_data_integrity(
                schema_version_path="/data/agora/SCHEMA_VERSION",
                scratch_dir="/data/agora",
                scratch_size_bytes=4096,
                chunk_bytes=1024,
                schema_reader=lambda _p: "1\n",
                scratch_opener=scratch,
                now_fn=lambda: _utc(),
            )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.name, "data_integrity")
        self.assertEqual(result.measurement["schema_version"], "1")
        self.assertEqual(result.measurement["bytes_written"], 4096)
        self.assertEqual(result.measurement["bytes_read"], 4096)
        self.assertEqual(
            result.measurement["write_sha256"],
            result.measurement["read_sha256"],
        )
        self.assertEqual(len(unlinks), 1)

    def test_missing_schema_version_fails(self) -> None:
        def reader(_p: str) -> str:
            raise FileNotFoundError(2, "missing")

        result = check_data_integrity(
            schema_reader=reader,
            scratch_opener=_Scratch(),
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("SCHEMA_VERSION not found", result.detail)

    def test_empty_schema_version_fails(self) -> None:
        result = check_data_integrity(
            schema_reader=lambda _p: "   \n",
            scratch_opener=_Scratch(),
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("empty", result.detail)

    def test_schema_permission_denied_fails(self) -> None:
        def reader(_p: str) -> str:
            raise PermissionError(13, "denied")

        result = check_data_integrity(
            schema_reader=reader,
            scratch_opener=_Scratch(),
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("read failed", result.detail)
        self.assertEqual(result.measurement["errno"], 13)

    def test_scratch_open_wb_failure_fails(self) -> None:
        def opener(path: str, mode: str) -> _FakeFile:
            raise PermissionError(13, "denied", path)

        result = check_data_integrity(
            schema_reader=lambda _p: "1",
            scratch_opener=opener,
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("open(wb)", result.detail)
        self.assertEqual(result.measurement["errno"], 13)

    def test_scratch_write_oserror_fails(self) -> None:
        class BadWrite(_FakeFile):
            def write(self, data: bytes) -> int:
                raise OSError(28, "ENOSPC")

        def opener(path: str, mode: str) -> _FakeFile:
            return BadWrite(name=path, mode=mode)

        result = check_data_integrity(
            schema_reader=lambda _p: "1",
            scratch_size_bytes=1024,
            chunk_bytes=512,
            scratch_opener=opener,
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("write failed", result.detail)
        self.assertEqual(result.measurement["errno"], 28)

    def test_scratch_open_rb_failure_fails(self) -> None:
        # write succeeds, but on re-open in read mode the opener raises.
        scratch = _Scratch()

        def opener(path: str, mode: str) -> _FakeFile:
            if "r" in mode:
                raise PermissionError(13, "denied", path)
            return scratch(path, mode)

        unlinks: list[str] = []
        with mock.patch.object(
            core.os, "unlink", side_effect=unlinks.append
        ):
            result = check_data_integrity(
                schema_reader=lambda _p: "1",
                scratch_size_bytes=1024,
                chunk_bytes=512,
                scratch_opener=opener,
                now_fn=lambda: _utc(),
            )
        self.assertFalse(result.ok)
        self.assertIn("open(rb)", result.detail)
        self.assertEqual(len(unlinks), 1)

    def test_scratch_read_oserror_fails(self) -> None:
        class BadRead(_FakeFile):
            def read(self, size: int = -1) -> bytes:
                raise OSError(5, "EIO")

        scratch_state: dict[str, _FakeFile] = {}

        def opener(path: str, mode: str) -> _FakeFile:
            if "w" in mode:
                fh = _FakeFile(name=path, mode=mode)
                scratch_state[path] = fh
                return fh
            # Read returns BadRead which raises on read().
            return BadRead(name=path, mode=mode)

        result = check_data_integrity(
            schema_reader=lambda _p: "1",
            scratch_size_bytes=1024,
            chunk_bytes=512,
            scratch_opener=opener,
            now_fn=lambda: _utc(),
        )
        self.assertFalse(result.ok)
        self.assertIn("read failed", result.detail)
        self.assertEqual(result.measurement["errno"], 5)

    def test_checksum_mismatch_fails(self) -> None:
        # Corrupt one byte on read: wrap _Scratch so the read file
        # returns a flipped first byte.
        scratch = _Scratch()

        def opener(path: str, mode: str) -> _FakeFile:
            fh = scratch(path, mode)
            if "r" in mode and fh.buffer:
                fh.buffer[0] ^= 0xFF
            return fh

        unlinks: list[str] = []
        with mock.patch.object(core.os, "unlink", side_effect=unlinks.append):
            result = check_data_integrity(
                schema_reader=lambda _p: "1",
                scratch_size_bytes=1024,
                chunk_bytes=512,
                scratch_opener=opener,
                now_fn=lambda: _utc(),
            )
        self.assertFalse(result.ok)
        self.assertIn("checksum mismatch", result.detail)
        self.assertNotEqual(
            result.measurement["write_sha256"],
            result.measurement["read_sha256"],
        )

    def test_zero_size_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_data_integrity(scratch_size_bytes=0)

    def test_zero_chunk_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            check_data_integrity(chunk_bytes=0)

    def test_round_trip_with_uneven_chunk(self) -> None:
        # 4097 bytes in 1024-byte chunks → 4×1024 + 1×1.
        scratch = _Scratch()
        with mock.patch.object(core.os, "unlink"):
            result = check_data_integrity(
                schema_reader=lambda _p: "1",
                scratch_size_bytes=4097,
                chunk_bytes=1024,
                scratch_opener=scratch,
                now_fn=lambda: _utc(),
            )
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.measurement["bytes_written"], 4097)
        self.assertEqual(result.measurement["bytes_read"], 4097)

    def test_records_path_and_sizes(self) -> None:
        scratch = _Scratch()
        with mock.patch.object(core.os, "unlink"):
            result = check_data_integrity(
                schema_version_path="/data/agora/SCHEMA_VERSION",
                scratch_dir="/data/agora",
                scratch_size_bytes=4096,
                chunk_bytes=2048,
                schema_reader=lambda _p: "2",
                scratch_opener=scratch,
                now_fn=lambda: _utc(),
            )
        self.assertTrue(result.ok)
        self.assertIn(
            "agora",  # Windows-portable substring (no leading slash check)
            result.measurement["scratch_path"],
        )
        self.assertEqual(result.measurement["scratch_size_bytes"], 4096)
        self.assertEqual(result.measurement["chunk_bytes"], 2048)
        self.assertEqual(result.measurement["schema_version"], "2")


# ── Aggregator ─────────────────────────────────────────────────────────────


def _passing_render_kwargs() -> dict[str, Any]:
    return {
        "duration_seconds": 10.0,
        "target_fps": 10.0,
        "opener": lambda _p: _FakeFile(),
        "sleeper": lambda _s: None,
        # Render check needs its own clock to control achieved-fps math;
        # the battery aggregator's tiny-step now_fn would make it look
        # like the render check ran at ~1000 fps and fail "too fast".
        "now_fn": _TickingNow(_utc(), 0.1),
    }


def _passing_wps_kwargs(now: datetime) -> dict[str, Any]:
    text = _wps_status_text(
        last_seen_at=(now - timedelta(seconds=5)).isoformat()
    )
    return {"reader": lambda _p: text}


def _passing_stress_kwargs() -> dict[str, Any]:
    def runner(args, **_kwargs):
        if args[0] == "dmesg":
            return _completed(0, "", "")
        if args[0] == "stress-ng":
            return _completed(0, "", "")
        raise AssertionError(args)

    return {
        "duration_seconds": 10,
        "cpu_workers": 1,
        "vm_workers": 0,
        "runner": runner,
        "thermal_reader": lambda _p: "55000",
    }


def _passing_integrity_kwargs() -> dict[str, Any]:
    return {
        "scratch_size_bytes": 1024,
        "chunk_bytes": 512,
        "schema_reader": lambda _p: "1",
        "scratch_opener": _Scratch(),
    }


class TestRunTestBattery(unittest.TestCase):
    def _now(self) -> _TickingNow:
        # Step is tiny so render-canary doesn't see deadline pressure.
        return _TickingNow(_utc(), 0.001)

    def test_happy_path_all_pass(self) -> None:
        writes: list[tuple[str, dict[str, Any]]] = []
        now = self._now()
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                run_id="run-happy",
                output_dir="/data/agora/test-results",
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=now,
                json_writer=lambda p, payload: writes.append(
                    (p, dict(payload))
                ),
            )
        self.assertTrue(battery.ok, [t.detail for t in battery.tests if not t.ok])
        self.assertEqual(battery.run_id, "run-happy")
        self.assertEqual(len(battery.tests), 4)
        self.assertEqual(
            [t.name for t in battery.tests],
            ["render_canary", "wps_synthetic", "stress", "data_integrity"],
        )
        self.assertFalse(battery.deadline_hit)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][1]["ok"], True)

    def test_failure_in_one_check_fails_battery(self) -> None:
        bad_wps = dict(_passing_wps_kwargs(_utc()))
        bad_wps["reader"] = lambda _p: json.dumps(
            {"state": "disconnected", "last_seen_at": ""}
        )
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                deadline_seconds=300,
                write_output=False,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=bad_wps,
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
            )
        self.assertFalse(battery.ok)
        failed = [t for t in battery.tests if not t.ok]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].name, "wps_synthetic")

    def test_deadline_skips_remaining_tests(self) -> None:
        # First call to now() = T0 (battery start). Render check internally
        # calls now() many times for its loop. We need a now_fn that
        # makes the *second* test see deadline_exceeded()=True. The
        # cleanest path is to mock now_fn so that after render finishes,
        # the next deadline_exceeded() call jumps past 5s.
        timeline = iter([_utc() + timedelta(seconds=i) for i in range(0, 1000)])
        baseline = [next(timeline) for _ in range(5)]
        # baseline[0] start, baseline[1] render start, etc.; then a huge jump
        future = _utc() + timedelta(seconds=9999)

        sequence = [
            _utc(),  # battery start
            _utc(),  # check 1 start
            _utc() + timedelta(seconds=0.5),  # check 1 finish
            future,  # deadline check before check 2 — exceeds
            future,  # placeholder skip start
            future,  # placeholder skip finish
            future,  # next deadline check
            future,  # skip
            future,  # skip
            future,  # next deadline check
            future,
            future,
            future,
            future,  # battery finish
        ]
        now = _FixedSequenceNow(sequence)

        # The render check itself calls now() during its loop. Easier: use
        # a render check that doesn't loop (duration ~ tiny). Inject a
        # render kwargs with duration_seconds=0.01 and a sleeper that
        # never sleeps. Combine with target_fps=1, achieved_fps will be
        # near 0 — that fails the render check. So replace render with
        # a synthetic check that just returns ok=True via monkeypatch.
        with mock.patch.object(core, "check_render_canary") as m_render, \
             mock.patch.object(core, "check_wps_synthetic") as m_wps, \
             mock.patch.object(core, "check_stress") as m_stress, \
             mock.patch.object(core, "check_data_integrity") as m_int:
            def render_call(**_kw):
                t = now()
                return core._wrap_result(
                    name="render_canary",
                    ok=True,
                    detail="fake",
                    measurement={},
                    started=t,
                    finished=t,
                )
            m_render.side_effect = render_call
            m_wps.side_effect = AssertionError("wps should be skipped")
            m_stress.side_effect = AssertionError("stress should be skipped")
            m_int.side_effect = AssertionError("integrity should be skipped")

            battery = run_test_battery(
                deadline_seconds=10,
                write_output=False,
                now_fn=now,
            )

        self.assertTrue(battery.deadline_hit)
        self.assertFalse(battery.ok)
        # First test ran, rest were skipped
        self.assertTrue(battery.tests[0].ok)
        for skipped in battery.tests[1:]:
            self.assertFalse(skipped.ok)
            self.assertIn("deadline", skipped.detail)
            self.assertTrue(skipped.measurement.get("skipped_due_to_deadline"))

    def test_zero_deadline_raises(self) -> None:
        with self.assertRaises(UpdateTesterError):
            run_test_battery(deadline_seconds=0)

    def test_no_output_skips_artifact(self) -> None:
        writes: list[Any] = []
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                write_output=False,
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
                json_writer=lambda p, payload: writes.append((p, payload)),
            )
        self.assertEqual(writes, [])
        self.assertEqual(battery.output_path, "")
        self.assertTrue(battery.ok)

    def test_none_output_dir_skips_artifact(self) -> None:
        writes: list[Any] = []
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                output_dir=None,
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
                json_writer=lambda p, payload: writes.append((p, payload)),
            )
        self.assertEqual(writes, [])
        self.assertEqual(battery.output_path, "")

    def test_artifact_json_has_expected_keys(self) -> None:
        writes: list[tuple[str, dict[str, Any]]] = []
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                run_id="rid",
                output_dir="/tmp/test-results",
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
                json_writer=lambda p, payload: writes.append((p, dict(payload))),
            )
        self.assertEqual(len(writes), 1)
        path, payload = writes[0]
        # Path joining is OS-dependent; use a Windows-portable substring.
        self.assertIn("rid.json", path.replace("\\", "/"))
        for key in (
            "run_id",
            "ok",
            "started_at",
            "finished_at",
            "duration_seconds",
            "deadline_seconds",
            "deadline_hit",
            "output_path",
            "tests",
        ):
            self.assertIn(key, payload)
        self.assertEqual(len(payload["tests"]), 4)
        self.assertTrue(battery.ok)

    def test_unexpected_exception_in_check_is_wrapped(self) -> None:
        with mock.patch.object(
            core, "check_wps_synthetic", side_effect=ValueError("boom")
        ):
            with mock.patch.object(core.os, "unlink"):
                battery = run_test_battery(
                    write_output=False,
                    deadline_seconds=300,
                    render_kwargs=_passing_render_kwargs(),
                    stress_kwargs=_passing_stress_kwargs(),
                    integrity_kwargs=_passing_integrity_kwargs(),
                    now_fn=self._now(),
                )
        self.assertFalse(battery.ok)
        wps = [t for t in battery.tests if t.name == "wps_synthetic"][0]
        self.assertFalse(wps.ok)
        self.assertIn("raised unexpectedly", wps.detail)
        self.assertEqual(wps.measurement["exception_type"], "ValueError")

    def test_programmer_error_propagates(self) -> None:
        # If a check raises UpdateTesterError (programmer error), it
        # must propagate — the battery doesn't wrap it.
        with mock.patch.object(
            core,
            "check_render_canary",
            side_effect=UpdateTesterError("bad arg"),
        ):
            with self.assertRaises(UpdateTesterError):
                run_test_battery(
                    write_output=False,
                    deadline_seconds=300,
                    now_fn=self._now(),
                )

    def test_artifact_write_failure_records_persist_error(self) -> None:
        def bad_writer(_path: str, _payload: Mapping[str, Any]) -> None:
            raise OSError(28, "ENOSPC")

        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                run_id="r1",
                output_dir="/tmp",
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
                json_writer=bad_writer,
            )
        self.assertFalse(battery.ok)
        self.assertEqual(battery.output_path, "")
        last = battery.tests[-1]
        self.assertEqual(last.name, "artifact_persist")
        self.assertFalse(last.ok)
        self.assertEqual(last.measurement["errno"], 28)

    def test_battery_to_dict_is_json_serializable(self) -> None:
        with mock.patch.object(core.os, "unlink"):
            battery = run_test_battery(
                write_output=False,
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
            )
        # Should round-trip through json without raising.
        s = json.dumps(battery_to_dict(battery), sort_keys=True)
        self.assertIn(battery.run_id, s)

    def test_default_run_id_is_fresh(self) -> None:
        with mock.patch.object(core.os, "unlink"):
            b1 = run_test_battery(
                write_output=False,
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
            )
            b2 = run_test_battery(
                write_output=False,
                deadline_seconds=300,
                render_kwargs=_passing_render_kwargs(),
                wps_kwargs=_passing_wps_kwargs(_utc()),
                stress_kwargs=_passing_stress_kwargs(),
                integrity_kwargs=_passing_integrity_kwargs(),
                now_fn=self._now(),
            )
        self.assertNotEqual(b1.run_id, b2.run_id)
        # Default is uuid4 hex, length 32.
        self.assertEqual(len(b1.run_id), 32)

    def test_now_fn_injected_into_checks(self) -> None:
        # Use a now_fn that records call count; render runs in tight
        # mocked form to confirm now_fn is threaded through.
        now = _TickingNow(_utc(), 0.0001)
        with mock.patch.object(core, "check_render_canary") as m_render, \
             mock.patch.object(core, "check_wps_synthetic") as m_wps, \
             mock.patch.object(core, "check_stress") as m_stress, \
             mock.patch.object(core, "check_data_integrity") as m_int:
            t = _utc()
            for m, name in (
                (m_render, "render_canary"),
                (m_wps, "wps_synthetic"),
                (m_stress, "stress"),
                (m_int, "data_integrity"),
            ):
                m.return_value = core._wrap_result(
                    name=name,
                    ok=True,
                    detail="",
                    measurement={},
                    started=t,
                    finished=t,
                )

            run_test_battery(
                write_output=False,
                deadline_seconds=300,
                now_fn=now,
            )
            for m in (m_render, m_wps, m_stress, m_int):
                m.assert_called_once()
                kwargs = m.call_args.kwargs
                self.assertIs(kwargs["now_fn"], now)


# ── CLI ────────────────────────────────────────────────────────────────────


class TestCli(unittest.TestCase):
    def _make_passing_battery(self) -> TestBatteryResult:
        return TestBatteryResult(
            run_id="cli-run",
            ok=True,
            started_at="2026-05-13T12:00:00+00:00",
            finished_at="2026-05-13T12:00:05+00:00",
            duration_seconds=5.0,
            deadline_seconds=1800,
            deadline_hit=False,
            tests=(
                TestResult(
                    name="render_canary",
                    ok=True,
                    detail="ok",
                    started_at="",
                    finished_at="",
                    duration_seconds=1.0,
                ),
            ),
            output_path="/tmp/cli-run.json",
        )

    def _make_failing_battery(self) -> TestBatteryResult:
        return TestBatteryResult(
            run_id="cli-run-bad",
            ok=False,
            started_at="2026-05-13T12:00:00+00:00",
            finished_at="2026-05-13T12:00:05+00:00",
            duration_seconds=5.0,
            deadline_seconds=1800,
            deadline_hit=False,
            tests=(
                TestResult(
                    name="render_canary",
                    ok=False,
                    detail="too slow",
                    started_at="",
                    finished_at="",
                    duration_seconds=1.0,
                ),
            ),
            output_path="",
        )

    def test_version_exits_zero(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod.sys, "stdout", new=io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                cli_mod.main(["--version"])
        self.assertEqual(cm.exception.code, 0)

    def test_exit_zero_on_pass(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod, "run_test_battery") as m_run, \
             mock.patch.object(cli_mod.sys, "stdout", new=io.StringIO()) as out:
            m_run.return_value = self._make_passing_battery()
            rc = cli_mod.main(["--no-output"])
        self.assertEqual(rc, 0)
        text = out.getvalue()
        # JSON should be parseable.
        payload = json.loads(text)
        self.assertEqual(payload["run_id"], "cli-run")
        self.assertTrue(payload["ok"])

    def test_exit_one_on_failure(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod, "run_test_battery") as m_run, \
             mock.patch.object(cli_mod.sys, "stdout", new=io.StringIO()):
            m_run.return_value = self._make_failing_battery()
            rc = cli_mod.main(["--no-output"])
        self.assertEqual(rc, 1)

    def test_exit_two_on_programmer_error(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod, "run_test_battery") as m_run, \
             mock.patch.object(cli_mod.sys, "stderr", new=io.StringIO()) as err:
            m_run.side_effect = UpdateTesterError("bad deadline")
            rc = cli_mod.main(["--no-output"])
        self.assertEqual(rc, 2)
        self.assertIn("bad deadline", err.getvalue())

    def test_no_output_threads_through(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod, "run_test_battery") as m_run, \
             mock.patch.object(cli_mod.sys, "stdout", new=io.StringIO()):
            m_run.return_value = self._make_passing_battery()
            cli_mod.main(["--no-output", "--run-id", "x"])
            kwargs = m_run.call_args.kwargs
            self.assertEqual(kwargs["write_output"], False)
            self.assertIsNone(kwargs["output_dir"])
            self.assertEqual(kwargs["run_id"], "x")

    def test_default_output_dir(self) -> None:
        from update_tester import __main__ as cli_mod

        with mock.patch.object(cli_mod, "run_test_battery") as m_run, \
             mock.patch.object(cli_mod.sys, "stdout", new=io.StringIO()):
            m_run.return_value = self._make_passing_battery()
            cli_mod.main([])
            kwargs = m_run.call_args.kwargs
            self.assertTrue(kwargs["write_output"])
            self.assertIn("test-results", kwargs["output_dir"])


# ── _atomic_write_json ─────────────────────────────────────────────────────


class TestAtomicWriteJson(unittest.TestCase):
    def test_writes_and_replaces(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "subdir", "result.json")
            payload = {"run_id": "x", "ok": True, "nested": [1, 2, 3]}
            core._atomic_write_json(path, payload)
            with open(path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            self.assertEqual(loaded, payload)
            # No stray temp files left in the parent directory.
            kids = os.listdir(os.path.dirname(path))
            self.assertEqual(kids, ["result.json"])

    def test_cleans_up_on_write_failure(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "result.json")

            # Force json.dump to raise.
            with mock.patch.object(
                core.json, "dump", side_effect=ValueError("boom")
            ):
                with self.assertRaises(ValueError):
                    core._atomic_write_json(path, {"k": "v"})
            # Temp file shouldn't survive.
            kids = os.listdir(td)
            self.assertEqual(kids, [])


if __name__ == "__main__":
    unittest.main()
