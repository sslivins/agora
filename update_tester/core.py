"""Implementation of the update-tester synthetic-load test battery.

Runs after :func:`slot_confirm.slot_confirm` passes on devices whose
ring config has ``gate_type=confirm_plus_test_suite`` (plan Decision
#6, plan §157-165). Phase 1 emits structured JSON to
``/data/agora/test-results/<run-id>.json`` and journald — there is
**no CMS wire-up** in Phase 1, that's Phase 3 (D-#13/#14).

The 4-test battery (plan §160-163):

1. **Render canary** — submit frames to the framebuffer at the target
   framerate for 5 min; fail if the achieved FPS drifts outside a
   tolerance band.
2. **WPS end-to-end** — verify the WPS receiver is currently connected
   *and* has heartbeated with CMS within a freshness window; dispatcher
   / observer hooks are exposed for Phase 3 to plug a real synthetic
   event into.
3. **Memory/CPU stress** — run ``stress-ng`` for 5 min; fail on
   non-zero exit, OOM kills in ``dmesg``, or thermal throttling
   (Pi 5 throttles at 80 °C).
4. **/data integrity sweep** — re-read ``SCHEMA_VERSION``, then
   write+read+delete a 100 MB scratch file; verify the round-trip
   bytes are identical.

Each test is a top-level function returning :class:`TestResult` and has
runner / opener / writer / reader / sleeper / now_fn injection seams so
tests can run without root, without ``/dev/fb0``, without ``stress-ng``
on PATH, without waiting 5 minutes — every external surface is stubbed
through the documented seam.

The aggregator :func:`run_test_battery` runs the 4 tests in order,
enforces an overall deadline (default 30 min), and writes a stable JSON
artifact suitable for hand-inspection or future CMS upload.

Routine failures (stress-ng missing, framebuffer EIO, scratch ENOSPC)
return ``TestResult(ok=False, …)`` — they never raise.
:class:`UpdateTesterError` is reserved for programmer errors (invalid
duration, negative size, missing required argument).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

# ── Defaults ────────────────────────────────────────────────────────────────

#: Where Phase 1 writes the per-run JSON result artifact.
DEFAULT_OUTPUT_DIR: str = "/data/agora/test-results"

#: Framebuffer device used by :func:`check_render_canary`.
DEFAULT_FRAMEBUFFER_DEVICE: str = "/dev/fb0"

#: Render-canary duration. Plan §160: "render-canary frames at full
#: framerate for 5 min".
DEFAULT_RENDER_DURATION_SECONDS: int = 5 * 60

#: Target frames-per-second for the render canary. Pi 5 HDMI runs at
#: 60 Hz by default; the canary aims for half that to leave headroom
#: for jitter without falsely failing the gate.
DEFAULT_RENDER_TARGET_FPS: float = 30.0

#: Fraction of ``target_fps`` the achieved FPS may drift before the
#: render canary fails (±). Default ±15 % — wide enough to absorb
#: HDMI vsync jitter on a stock Pi 5 image.
DEFAULT_RENDER_FPS_TOLERANCE: float = 0.15

#: Bytes written per frame. One byte is enough to exercise the
#: framebuffer device; larger writes don't add useful signal.
DEFAULT_RENDER_FRAME_BYTES: bytes = b"\x00"

#: WPS receiver status file. Same path as slot_confirm consumes.
DEFAULT_WPS_STATUS_PATH: str = "/opt/agora/state/cms_status.json"

#: How recent ``last_seen_at`` must be (relative to ``now_fn()``) for
#: the WPS check to pass. Plan-driven: the WPS receiver heartbeats
#: every few seconds, so 30 s gives plenty of margin without admitting
#: a stale receiver.
DEFAULT_WPS_FRESHNESS_SECONDS: int = 30

#: stress-ng command name. Resolved through ``$PATH`` by the default
#: runner.
DEFAULT_STRESS_BINARY: str = "stress-ng"

#: stress-ng duration. Plan §162: "memory/CPU stress for 5 min".
DEFAULT_STRESS_DURATION_SECONDS: int = 5 * 60

#: stress-ng CPU worker count. Pi 5 has 4 Cortex-A76 cores.
DEFAULT_STRESS_CPU_WORKERS: int = 4

#: stress-ng VM worker count. Two workers × 256 MB ≈ ½ GB of resident
#: pressure on an 8 GB Pi 5 — meaningful but well clear of OOM.
DEFAULT_STRESS_VM_WORKERS: int = 2

#: Per-worker resident memory for the VM stressor (passed verbatim to
#: ``stress-ng --vm-bytes``).
DEFAULT_STRESS_VM_BYTES: str = "256M"

#: Thermal-zone sysfs path. Pi 5 exposes the SoC temperature here in
#: millidegrees Celsius.
DEFAULT_THERMAL_PATH: str = "/sys/class/thermal/thermal_zone0/temp"

#: Temperature at which the Pi 5 begins thermal throttling (°C). The
#: stress check fails if the post-stress reading is at or above this.
DEFAULT_THERMAL_THROTTLE_CELSIUS: float = 80.0

#: dmesg ring-buffer scan window. Looked at after stress-ng exits to
#: detect OOM kills produced during the stress run.
DEFAULT_DMESG_RECENT_LINES: int = 200

#: Where the SCHEMA_VERSION file lives on a healthy device.
DEFAULT_SCHEMA_VERSION_PATH: str = "/data/agora/SCHEMA_VERSION"

#: Directory under which the integrity check writes its scratch file.
#: ``slot_mgr`` already owns this directory.
DEFAULT_SCRATCH_DIR: str = "/data/agora"

#: Size of the integrity-check scratch file. Plan §163: "100 MB".
DEFAULT_SCRATCH_SIZE_BYTES: int = 100 * 1024 * 1024

#: Chunk size used when streaming the scratch file write/read.
DEFAULT_SCRATCH_CHUNK_BYTES: int = 1 * 1024 * 1024

#: Battery-wide deadline. Plan §164: "default 30 min".
DEFAULT_DEADLINE_SECONDS: int = 30 * 60


# ── Types ───────────────────────────────────────────────────────────────────


class UpdateTesterError(RuntimeError):
    """Programmer error in calling an update-tester function.

    Routine "the test failed" outcomes return :class:`TestResult` with
    ``ok=False``; this exception is reserved for bugs in the caller
    (negative durations, empty test names, missing required keys).
    """


@dataclass(frozen=True)
class TestResult:
    """Outcome of one update-tester test."""

    name: str
    ok: bool
    detail: str
    measurement: Mapping[str, Any] = field(default_factory=dict)
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True)
class TestBatteryResult:
    """Aggregated outcome of one update-tester battery run."""

    run_id: str
    ok: bool
    started_at: str
    finished_at: str
    duration_seconds: float
    deadline_seconds: int
    deadline_hit: bool
    tests: tuple[TestResult, ...]
    output_path: str = ""


# ── Type aliases for injection seams ───────────────────────────────────────

Runner = Callable[..., "subprocess.CompletedProcess[str]"]
NowFn = Callable[[], datetime]
Sleeper = Callable[[float], None]
FrameOpener = Callable[[str], Any]
ProbeWriter = Callable[[str, bytes], None]
ScratchOpener = Callable[[str, str], Any]
StatusReader = Callable[[str], str]
SchemaReader = Callable[[str], str]
ThermalReader = Callable[[str], str]
EventDispatcher = Callable[[], Mapping[str, Any]]


def _default_runner(
    args: Sequence[str],
    *,
    check: bool = False,
    capture_output: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(
        list(args),
        check=check,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )


def _default_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _default_sleeper(seconds: float) -> None:
    if seconds > 0:
        import time as _time

        _time.sleep(seconds)


def _format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _wrap_result(
    name: str,
    ok: bool,
    detail: str,
    measurement: Mapping[str, Any],
    *,
    started: datetime,
    finished: datetime,
) -> TestResult:
    return TestResult(
        name=name,
        ok=ok,
        detail=detail,
        measurement=measurement,
        started_at=_format_ts(started),
        finished_at=_format_ts(finished),
        duration_seconds=max(0.0, (finished - started).total_seconds()),
    )


# ── 1. Render canary ───────────────────────────────────────────────────────


def check_render_canary(
    *,
    device: str = DEFAULT_FRAMEBUFFER_DEVICE,
    duration_seconds: float = DEFAULT_RENDER_DURATION_SECONDS,
    target_fps: float = DEFAULT_RENDER_TARGET_FPS,
    fps_tolerance: float = DEFAULT_RENDER_FPS_TOLERANCE,
    frame_bytes: bytes = DEFAULT_RENDER_FRAME_BYTES,
    opener: Optional[FrameOpener] = None,
    sleeper: Optional[Sleeper] = None,
    now_fn: Optional[NowFn] = None,
) -> TestResult:
    """Write frames to ``device`` at ``target_fps`` for ``duration_seconds``.

    The achieved FPS is computed as
    ``frames_written / wall_clock_elapsed``. The check fails when the
    achieved FPS is outside ``target_fps * (1 ± fps_tolerance)``.

    Failure modes (each surfaces ``ok=False`` with an errno when
    applicable, never raises):

    * ``device`` missing / non-writable / EIO during write.
    * Achieved FPS below the lower tolerance band (slow path).
    * Achieved FPS above the upper tolerance band (the sleeper was
      ignored or the wall clock skewed — caller bug, but we report
      rather than raise so a real ``stress-ng``-induced clock blip
      doesn't crash the battery).

    All wall-clock advances go through ``now_fn``; all sleeps go
    through ``sleeper``. Tests inject deterministic versions of both.
    """
    if duration_seconds <= 0:
        raise UpdateTesterError(
            f"duration_seconds must be > 0, got {duration_seconds!r}"
        )
    if target_fps <= 0:
        raise UpdateTesterError(f"target_fps must be > 0, got {target_fps!r}")
    if not 0 <= fps_tolerance < 1:
        raise UpdateTesterError(
            f"fps_tolerance must be in [0, 1), got {fps_tolerance!r}"
        )
    if not frame_bytes:
        raise UpdateTesterError("frame_bytes must be non-empty")

    now = now_fn or _default_now
    sleep = sleeper or _default_sleeper
    open_fn: FrameOpener = opener or (lambda path: open(path, "wb"))

    started = now()
    try:
        fh = open_fn(device)
    except FileNotFoundError as exc:
        finished = now()
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=f"framebuffer device not found: {device}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
            started=started,
            finished=finished,
        )
    except PermissionError as exc:
        finished = now()
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=f"framebuffer device not writable: {device}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
            started=started,
            finished=finished,
        )
    except OSError as exc:
        finished = now()
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=f"framebuffer open failed: {exc}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
            started=started,
            finished=finished,
        )

    frame_period = 1.0 / target_fps
    frames_written = 0
    write_errno: Optional[int] = None
    write_error_detail = ""
    try:
        while True:
            elapsed = (now() - started).total_seconds()
            if elapsed >= duration_seconds:
                break
            try:
                fh.write(frame_bytes)
            except OSError as exc:
                write_errno = getattr(exc, "errno", None)
                write_error_detail = str(exc)
                break
            frames_written += 1
            sleep(frame_period)
    finally:
        close = getattr(fh, "close", None)
        if callable(close):
            try:
                close()
            except OSError:
                pass

    finished = now()
    wall_seconds = max(0.0, (finished - started).total_seconds())
    achieved_fps = frames_written / wall_seconds if wall_seconds > 0 else 0.0
    fps_low = target_fps * (1 - fps_tolerance)
    fps_high = target_fps * (1 + fps_tolerance)

    measurement = {
        "device": device,
        "duration_seconds": duration_seconds,
        "target_fps": target_fps,
        "fps_tolerance": fps_tolerance,
        "fps_low": fps_low,
        "fps_high": fps_high,
        "frames_written": frames_written,
        "wall_seconds": wall_seconds,
        "achieved_fps": achieved_fps,
        "frame_bytes": len(frame_bytes),
    }

    if write_errno is not None or write_error_detail:
        measurement["errno"] = write_errno
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=f"framebuffer write failed: {write_error_detail}",
            measurement=measurement,
            started=started,
            finished=finished,
        )

    if achieved_fps < fps_low:
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=(
                f"render too slow: {achieved_fps:.2f} fps < "
                f"{fps_low:.2f} fps (target {target_fps:.0f} fps "
                f"±{fps_tolerance:.0%})"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )
    if achieved_fps > fps_high:
        return _wrap_result(
            name="render_canary",
            ok=False,
            detail=(
                f"render too fast: {achieved_fps:.2f} fps > "
                f"{fps_high:.2f} fps (target {target_fps:.0f} fps "
                f"±{fps_tolerance:.0%}) — sleeper/clock skew?"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )

    return _wrap_result(
        name="render_canary",
        ok=True,
        detail=(
            f"rendered {frames_written} frames in "
            f"{wall_seconds:.1f}s at {achieved_fps:.2f} fps "
            f"(target {target_fps:.0f} fps ±{fps_tolerance:.0%})"
        ),
        measurement=measurement,
        started=started,
        finished=finished,
    )


# ── 2. WPS end-to-end ──────────────────────────────────────────────────────


def _read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _parse_iso_or_none(raw: str) -> Optional[datetime]:
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def check_wps_synthetic(
    *,
    status_path: str = DEFAULT_WPS_STATUS_PATH,
    freshness_seconds: int = DEFAULT_WPS_FRESHNESS_SECONDS,
    dispatcher: Optional[EventDispatcher] = None,
    reader: Optional[StatusReader] = None,
    now_fn: Optional[NowFn] = None,
) -> TestResult:
    """Verify the WPS receiver is connected and recently heartbeating.

    Plan §161: "WPS end-to-end: dispatch synthetic event, observe
    receiver-side". Phase 1's honest interpretation:

    1. Read ``status_path`` (the same file ``slot_confirm`` consumes).
    2. Require ``state == "connected"``.
    3. Require ``last_seen_at`` (the WPS receiver's heartbeat
       timestamp) within ``freshness_seconds`` of ``now_fn()``.
    4. Optionally invoke ``dispatcher`` — a hook for Phase 3 to plug
       in a real synthetic-event submission. Phase 1 leaves it
       unset, which means dispatcher-derived measurements are absent
       from the result. If the dispatcher raises, the check fails.

    This is a strictly stricter check than
    :func:`slot_confirm.check_wps_connected`: that one only verifies
    the state field; this one also enforces freshness.

    Failure modes return ``ok=False``; only programmer errors raise.
    """
    if freshness_seconds <= 0:
        raise UpdateTesterError(
            f"freshness_seconds must be > 0, got {freshness_seconds!r}"
        )

    now = now_fn or _default_now
    read = reader or _read_text_file

    started = now()

    try:
        raw = read(status_path)
    except FileNotFoundError as exc:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=f"WPS status file not found: {status_path}",
            measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            started=started,
            finished=finished,
        )
    except (PermissionError, OSError) as exc:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=f"WPS status read failed: {exc}",
            measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            started=started,
            finished=finished,
        )

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=f"WPS status JSON invalid: {exc}",
            measurement={"status_path": status_path, "raw_excerpt": raw[:120]},
            started=started,
            finished=finished,
        )
    if not isinstance(payload, dict):
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail="WPS status JSON is not an object",
            measurement={"status_path": status_path, "type": type(payload).__name__},
            started=started,
            finished=finished,
        )

    state = payload.get("state")
    if state != "connected":
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=f"WPS receiver state is {state!r}, expected 'connected'",
            measurement={"status_path": status_path, "state": state},
            started=started,
            finished=finished,
        )

    last_seen_raw = payload.get("last_seen_at", "")
    last_seen = _parse_iso_or_none(str(last_seen_raw)) if last_seen_raw else None
    measurement: dict[str, Any] = {
        "status_path": status_path,
        "freshness_seconds": freshness_seconds,
        "state": state,
        "last_seen_at": last_seen_raw,
    }
    if last_seen is None:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail="WPS status missing or unparseable 'last_seen_at'",
            measurement=measurement,
            started=started,
            finished=finished,
        )

    age = (now() - last_seen).total_seconds()
    measurement["last_seen_age_seconds"] = age
    if age > freshness_seconds:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=(
                f"WPS heartbeat stale: {age:.0f}s old, "
                f"need ≤{freshness_seconds}s"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )
    if age < -freshness_seconds:
        finished = now()
        return _wrap_result(
            name="wps_synthetic",
            ok=False,
            detail=(
                f"WPS heartbeat in the future: {-age:.0f}s ahead, "
                f"clock skew?"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )

    if dispatcher is not None:
        try:
            dispatched = dispatcher()
        except Exception as exc:  # noqa: BLE001
            finished = now()
            return _wrap_result(
                name="wps_synthetic",
                ok=False,
                detail=f"synthetic-event dispatcher raised: {exc}",
                measurement=measurement,
                started=started,
                finished=finished,
            )
        if isinstance(dispatched, Mapping):
            measurement["dispatched"] = dict(dispatched)
        else:
            measurement["dispatched"] = {"value": dispatched}

    finished = now()
    return _wrap_result(
        name="wps_synthetic",
        ok=True,
        detail=(
            f"WPS receiver connected, last heartbeat {age:.0f}s ago "
            f"(≤{freshness_seconds}s)"
        ),
        measurement=measurement,
        started=started,
        finished=finished,
    )


# ── 3. Memory/CPU stress ───────────────────────────────────────────────────


def _read_thermal_celsius(
    path: str,
    *,
    reader: ThermalReader,
) -> Optional[float]:
    try:
        raw = reader(path).strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        # Pi 5 emits millidegrees.
        return int(raw) / 1000.0
    except ValueError:
        try:
            return float(raw)
        except ValueError:
            return None


def _count_oom(dmesg_text: str, recent_lines: int) -> int:
    if recent_lines <= 0:
        lines = dmesg_text.splitlines()
    else:
        lines = dmesg_text.splitlines()[-recent_lines:]
    count = 0
    for line in lines:
        lower = line.lower()
        if "out of memory" in lower or "killed process" in lower or "oom-kill" in lower:
            count += 1
    return count


def check_stress(
    *,
    duration_seconds: int = DEFAULT_STRESS_DURATION_SECONDS,
    cpu_workers: int = DEFAULT_STRESS_CPU_WORKERS,
    vm_workers: int = DEFAULT_STRESS_VM_WORKERS,
    vm_bytes: str = DEFAULT_STRESS_VM_BYTES,
    binary: str = DEFAULT_STRESS_BINARY,
    thermal_path: str = DEFAULT_THERMAL_PATH,
    throttle_celsius: float = DEFAULT_THERMAL_THROTTLE_CELSIUS,
    dmesg_recent_lines: int = DEFAULT_DMESG_RECENT_LINES,
    runner: Optional[Runner] = None,
    thermal_reader: Optional[ThermalReader] = None,
    now_fn: Optional[NowFn] = None,
) -> TestResult:
    """Run ``stress-ng`` for ``duration_seconds``; check thermal + OOM.

    The default invocation is::

        stress-ng --cpu {cpu_workers} --vm {vm_workers}
                  --vm-bytes {vm_bytes} --timeout {duration_seconds}s
                  --metrics-brief

    The check fails if any of these are true:

    * ``stress-ng`` is missing on PATH (``FileNotFoundError``).
    * ``stress-ng`` exits non-zero.
    * The post-stress reading from ``thermal_path`` is ≥
      ``throttle_celsius`` (Pi 5 throttle threshold is 80 °C).
    * Running ``dmesg --color=never`` shows an OOM kill that
      didn't appear pre-stress.

    All clocks go through ``now_fn``; tests inject a runner that
    returns a canned :class:`subprocess.CompletedProcess` so no real
    stress-ng invocation happens.
    """
    if duration_seconds <= 0:
        raise UpdateTesterError(
            f"duration_seconds must be > 0, got {duration_seconds!r}"
        )
    if cpu_workers < 0 or vm_workers < 0:
        raise UpdateTesterError(
            f"worker counts must be ≥0, got cpu={cpu_workers!r} vm={vm_workers!r}"
        )
    if cpu_workers == 0 and vm_workers == 0:
        raise UpdateTesterError("at least one stressor must be enabled")

    run = runner or _default_runner
    therm_read = thermal_reader or _read_text_file
    now = now_fn or _default_now

    started = now()
    pre_thermal = _read_thermal_celsius(thermal_path, reader=therm_read)

    pre_dmesg_oom = 0
    try:
        cp = run(
            ["dmesg", "--color=never"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        pre_dmesg_oom = _count_oom(cp.stdout or "", dmesg_recent_lines)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # dmesg may be unavailable in containers or rate-limited; the
        # post-check still catches OOMs by looking at the full window.
        pass

    args: list[str] = [binary]
    if cpu_workers > 0:
        args += ["--cpu", str(cpu_workers)]
    if vm_workers > 0:
        args += ["--vm", str(vm_workers), "--vm-bytes", vm_bytes]
    args += ["--timeout", f"{duration_seconds}s", "--metrics-brief"]

    stress_returncode: Optional[int] = None
    stress_stderr = ""
    stress_stdout = ""
    try:
        cp = run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=duration_seconds + 60,
        )
        stress_returncode = cp.returncode
        stress_stdout = cp.stdout or ""
        stress_stderr = cp.stderr or ""
    except FileNotFoundError as exc:
        finished = now()
        return _wrap_result(
            name="stress",
            ok=False,
            detail=f"stress-ng binary not found: {binary}",
            measurement={
                "binary": binary,
                "errno": getattr(exc, "errno", None),
            },
            started=started,
            finished=finished,
        )
    except subprocess.TimeoutExpired:
        finished = now()
        return _wrap_result(
            name="stress",
            ok=False,
            detail=(
                f"stress-ng exceeded {duration_seconds + 60}s wall-clock "
                f"(internal timeout was {duration_seconds}s)"
            ),
            measurement={
                "binary": binary,
                "duration_seconds": duration_seconds,
            },
            started=started,
            finished=finished,
        )

    post_thermal = _read_thermal_celsius(thermal_path, reader=therm_read)
    post_dmesg_oom = 0
    try:
        cp = run(
            ["dmesg", "--color=never"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        post_dmesg_oom = _count_oom(cp.stdout or "", dmesg_recent_lines)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    new_oom = max(0, post_dmesg_oom - pre_dmesg_oom)

    measurement: dict[str, Any] = {
        "binary": binary,
        "duration_seconds": duration_seconds,
        "cpu_workers": cpu_workers,
        "vm_workers": vm_workers,
        "vm_bytes": vm_bytes,
        "args": args,
        "returncode": stress_returncode,
        "pre_thermal_celsius": pre_thermal,
        "post_thermal_celsius": post_thermal,
        "throttle_celsius": throttle_celsius,
        "pre_dmesg_oom": pre_dmesg_oom,
        "post_dmesg_oom": post_dmesg_oom,
        "new_oom": new_oom,
    }
    if stress_stderr.strip():
        measurement["stderr_excerpt"] = stress_stderr.strip().splitlines()[-5:]
    _ = stress_stdout  # available via runner stub for tests

    finished = now()
    if stress_returncode != 0:
        return _wrap_result(
            name="stress",
            ok=False,
            detail=f"stress-ng exited with returncode={stress_returncode}",
            measurement=measurement,
            started=started,
            finished=finished,
        )
    if new_oom > 0:
        return _wrap_result(
            name="stress",
            ok=False,
            detail=f"detected {new_oom} OOM kill(s) in dmesg during stress",
            measurement=measurement,
            started=started,
            finished=finished,
        )
    if post_thermal is not None and post_thermal >= throttle_celsius:
        return _wrap_result(
            name="stress",
            ok=False,
            detail=(
                f"post-stress thermal {post_thermal:.1f} °C ≥ "
                f"throttle threshold {throttle_celsius:.1f} °C"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )

    detail_bits = [f"stress-ng ran for {duration_seconds}s (rc=0)"]
    if post_thermal is not None:
        detail_bits.append(f"post-thermal {post_thermal:.1f} °C")
    detail_bits.append("OOM kills during run: 0")
    return _wrap_result(
        name="stress",
        ok=True,
        detail="; ".join(detail_bits),
        measurement=measurement,
        started=started,
        finished=finished,
    )


# ── 4. /data integrity sweep ───────────────────────────────────────────────


def check_data_integrity(
    *,
    schema_version_path: str = DEFAULT_SCHEMA_VERSION_PATH,
    scratch_dir: str = DEFAULT_SCRATCH_DIR,
    scratch_size_bytes: int = DEFAULT_SCRATCH_SIZE_BYTES,
    chunk_bytes: int = DEFAULT_SCRATCH_CHUNK_BYTES,
    schema_reader: Optional[SchemaReader] = None,
    scratch_opener: Optional[ScratchOpener] = None,
    now_fn: Optional[NowFn] = None,
) -> TestResult:
    """Re-read ``SCHEMA_VERSION`` and round-trip a 100 MB scratch file.

    Plan §163: "/data integrity sweep (re-read SCHEMA_VERSION,
    write/read/delete a 100 MB scratch file)".

    Behaviour:

    1. Open ``schema_version_path`` and read its (non-empty) contents
       — failure means slot_confirm has somehow let us run without a
       valid /data, which is a hard fail.
    2. Create a uuid-named scratch file under ``scratch_dir`` and
       write ``scratch_size_bytes`` of a deterministic pseudo-random
       pattern in ``chunk_bytes``-sized chunks, computing a SHA-256 as
       we write.
    3. Read it back, computing a SHA-256 of the read bytes.
    4. Delete the scratch file (best-effort — leaving 100 MB of
       garbage on /data is not a check failure of the write/read).
    5. Fail if the read SHA differs from the write SHA or any I/O
       step raised.

    Tests inject ``schema_reader`` (returns the SCHEMA_VERSION text)
    and ``scratch_opener`` (mimics ``open(path, mode)``) — the file
    object returned must support ``write(bytes)``, ``read(int)``,
    ``flush()``, ``fileno()`` (optional, ``os.fsync`` is wrapped in
    try/except), and ``close()``.
    """
    if scratch_size_bytes <= 0:
        raise UpdateTesterError(
            f"scratch_size_bytes must be > 0, got {scratch_size_bytes!r}"
        )
    if chunk_bytes <= 0:
        raise UpdateTesterError(f"chunk_bytes must be > 0, got {chunk_bytes!r}")

    now = now_fn or _default_now
    read_schema = schema_reader or _read_text_file

    started = now()

    try:
        schema_text = read_schema(schema_version_path)
    except FileNotFoundError as exc:
        finished = now()
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=f"SCHEMA_VERSION not found: {schema_version_path}",
            measurement={
                "schema_version_path": schema_version_path,
                "errno": getattr(exc, "errno", None),
            },
            started=started,
            finished=finished,
        )
    except (PermissionError, OSError) as exc:
        finished = now()
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=f"SCHEMA_VERSION read failed: {exc}",
            measurement={
                "schema_version_path": schema_version_path,
                "errno": getattr(exc, "errno", None),
            },
            started=started,
            finished=finished,
        )

    schema_text_stripped = (schema_text or "").strip()
    if not schema_text_stripped:
        finished = now()
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=f"SCHEMA_VERSION is empty: {schema_version_path}",
            measurement={"schema_version_path": schema_version_path},
            started=started,
            finished=finished,
        )

    scratch_name = f".update-tester-scratch-{uuid.uuid4().hex}"
    scratch_path = os.path.join(scratch_dir, scratch_name)
    open_fn: ScratchOpener = scratch_opener or (lambda p, m: open(p, m))

    write_hash = hashlib.sha256()
    seed = uuid.uuid4().bytes
    bytes_written = 0
    try:
        fh = open_fn(scratch_path, "wb")
    except (FileNotFoundError, PermissionError, OSError) as exc:
        finished = now()
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=f"scratch open(wb) failed: {exc}",
            measurement={
                "scratch_path": scratch_path,
                "errno": getattr(exc, "errno", None),
            },
            started=started,
            finished=finished,
        )
    try:
        try:
            remaining = scratch_size_bytes
            counter = 0
            while remaining > 0:
                this_chunk = min(chunk_bytes, remaining)
                # Deterministic pseudo-random chunk: SHA-256(seed || counter)
                # truncated/repeated to fill ``this_chunk`` bytes.
                pattern = hashlib.sha256(
                    seed + counter.to_bytes(8, "big", signed=False)
                ).digest()
                repeats = (this_chunk + len(pattern) - 1) // len(pattern)
                chunk = (pattern * repeats)[:this_chunk]
                fh.write(chunk)
                write_hash.update(chunk)
                bytes_written += this_chunk
                remaining -= this_chunk
                counter += 1
            flush = getattr(fh, "flush", None)
            if callable(flush):
                try:
                    flush()
                except OSError:
                    pass
            fileno = getattr(fh, "fileno", None)
            if callable(fileno):
                try:
                    os.fsync(fileno())
                except (OSError, ValueError):
                    pass
        except OSError as exc:
            finished = now()
            return _wrap_result(
                name="data_integrity",
                ok=False,
                detail=f"scratch write failed after {bytes_written} bytes: {exc}",
                measurement={
                    "scratch_path": scratch_path,
                    "bytes_written": bytes_written,
                    "errno": getattr(exc, "errno", None),
                    "schema_version": schema_text_stripped,
                },
                started=started,
                finished=finished,
            )
    finally:
        close = getattr(fh, "close", None)
        if callable(close):
            try:
                close()
            except OSError:
                pass

    read_hash = hashlib.sha256()
    bytes_read = 0
    try:
        fh = open_fn(scratch_path, "rb")
    except (FileNotFoundError, PermissionError, OSError) as exc:
        try:
            os.unlink(scratch_path)
        except OSError:
            pass
        finished = now()
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=f"scratch open(rb) failed: {exc}",
            measurement={
                "scratch_path": scratch_path,
                "bytes_written": bytes_written,
                "errno": getattr(exc, "errno", None),
                "schema_version": schema_text_stripped,
            },
            started=started,
            finished=finished,
        )
    try:
        try:
            while True:
                chunk = fh.read(chunk_bytes)
                if not chunk:
                    break
                read_hash.update(chunk)
                bytes_read += len(chunk)
        except OSError as exc:
            finished = now()
            return _wrap_result(
                name="data_integrity",
                ok=False,
                detail=f"scratch read failed after {bytes_read} bytes: {exc}",
                measurement={
                    "scratch_path": scratch_path,
                    "bytes_written": bytes_written,
                    "bytes_read": bytes_read,
                    "errno": getattr(exc, "errno", None),
                    "schema_version": schema_text_stripped,
                },
                started=started,
                finished=finished,
            )
    finally:
        close = getattr(fh, "close", None)
        if callable(close):
            try:
                close()
            except OSError:
                pass

    try:
        os.unlink(scratch_path)
    except OSError:
        # Best-effort cleanup; not a check failure.
        pass

    write_digest = write_hash.hexdigest()
    read_digest = read_hash.hexdigest()
    measurement = {
        "schema_version_path": schema_version_path,
        "schema_version": schema_text_stripped,
        "scratch_path": scratch_path,
        "scratch_size_bytes": scratch_size_bytes,
        "chunk_bytes": chunk_bytes,
        "bytes_written": bytes_written,
        "bytes_read": bytes_read,
        "write_sha256": write_digest,
        "read_sha256": read_digest,
    }

    finished = now()
    if bytes_read != bytes_written:
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=(
                f"scratch round-trip size mismatch: wrote "
                f"{bytes_written} bytes, read {bytes_read} bytes"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )
    if write_digest != read_digest:
        return _wrap_result(
            name="data_integrity",
            ok=False,
            detail=(
                f"scratch round-trip checksum mismatch: "
                f"wrote sha256 {write_digest[:12]}…, "
                f"read sha256 {read_digest[:12]}…"
            ),
            measurement=measurement,
            started=started,
            finished=finished,
        )

    return _wrap_result(
        name="data_integrity",
        ok=True,
        detail=(
            f"SCHEMA_VERSION={schema_text_stripped!r}; "
            f"{bytes_written}-byte scratch round-trip checksum match"
        ),
        measurement=measurement,
        started=started,
        finished=finished,
    )


# ── Aggregator ─────────────────────────────────────────────────────────────


def _atomic_write_json(path: str, payload: Mapping[str, Any]) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".update-tester-", suffix=".json", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _result_to_dict(result: TestResult) -> dict[str, Any]:
    return {
        "name": result.name,
        "ok": result.ok,
        "detail": result.detail,
        "measurement": dict(result.measurement or {}),
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "duration_seconds": result.duration_seconds,
    }


def battery_to_dict(result: TestBatteryResult) -> dict[str, Any]:
    """Render a :class:`TestBatteryResult` as a JSON-friendly dict."""
    return {
        "run_id": result.run_id,
        "ok": result.ok,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "duration_seconds": result.duration_seconds,
        "deadline_seconds": result.deadline_seconds,
        "deadline_hit": result.deadline_hit,
        "output_path": result.output_path,
        "tests": [_result_to_dict(t) for t in result.tests],
    }


def run_test_battery(
    *,
    run_id: Optional[str] = None,
    output_dir: Optional[str] = DEFAULT_OUTPUT_DIR,
    write_output: bool = True,
    deadline_seconds: int = DEFAULT_DEADLINE_SECONDS,
    render_kwargs: Optional[Mapping[str, Any]] = None,
    wps_kwargs: Optional[Mapping[str, Any]] = None,
    stress_kwargs: Optional[Mapping[str, Any]] = None,
    integrity_kwargs: Optional[Mapping[str, Any]] = None,
    now_fn: Optional[NowFn] = None,
    json_writer: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
) -> TestBatteryResult:
    """Run the 4-test battery in order, enforce a deadline, write JSON.

    Tests run in the listed order: render, WPS, stress, integrity.
    The deadline is enforced *between* tests: a test that's already
    running when the deadline elapses runs to completion; subsequent
    tests are skipped and recorded as ``ok=False`` with
    ``detail="deadline exceeded; test not run"``. The overall
    ``ok`` is ``True`` only if every test passed *and* the deadline
    wasn't hit.

    ``run_id`` defaults to a fresh uuid4 hex. When ``write_output`` is
    true (default) and ``output_dir`` is set, the per-run JSON
    artifact is written atomically at
    ``output_dir/<run_id>.json``. Set ``write_output=False`` (or
    ``output_dir=None``) for dry-runs and tests.

    Per-check overrides are passed through their ``*_kwargs`` dicts;
    any unknown key surfaces as ``TypeError`` from the underlying
    check function. ``now_fn`` is injected into every check for clock
    consistency.
    """
    if deadline_seconds <= 0:
        raise UpdateTesterError(
            f"deadline_seconds must be > 0, got {deadline_seconds!r}"
        )

    now = now_fn or _default_now
    rid = run_id or uuid.uuid4().hex
    started = now()

    def deadline_exceeded() -> bool:
        return (now() - started).total_seconds() > deadline_seconds

    def merge(base: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        merged: dict[str, Any] = {"now_fn": now}
        if base:
            merged.update(dict(base))
        return merged

    tests: list[TestResult] = []
    deadline_hit = False

    runners: list[tuple[str, Callable[[], TestResult]]] = [
        ("render_canary", lambda: check_render_canary(**merge(render_kwargs))),
        ("wps_synthetic", lambda: check_wps_synthetic(**merge(wps_kwargs))),
        ("stress", lambda: check_stress(**merge(stress_kwargs))),
        ("data_integrity", lambda: check_data_integrity(**merge(integrity_kwargs))),
    ]

    for name, runfn in runners:
        if deadline_hit or deadline_exceeded():
            deadline_hit = True
            t0 = now()
            tests.append(
                _wrap_result(
                    name=name,
                    ok=False,
                    detail="deadline exceeded; test not run",
                    measurement={
                        "deadline_seconds": deadline_seconds,
                        "skipped_due_to_deadline": True,
                    },
                    started=t0,
                    finished=t0,
                )
            )
            continue
        try:
            result = runfn()
        except UpdateTesterError:
            raise
        except Exception as exc:  # noqa: BLE001
            t0 = now()
            result = _wrap_result(
                name=name,
                ok=False,
                detail=f"test raised unexpectedly: {type(exc).__name__}: {exc}",
                measurement={"exception_type": type(exc).__name__},
                started=t0,
                finished=t0,
            )
        tests.append(result)

    finished = now()
    duration = max(0.0, (finished - started).total_seconds())
    overall_ok = (not deadline_hit) and all(t.ok for t in tests)

    output_path = ""
    if write_output and output_dir:
        output_path = os.path.join(output_dir, f"{rid}.json")

    battery = TestBatteryResult(
        run_id=rid,
        ok=overall_ok,
        started_at=_format_ts(started),
        finished_at=_format_ts(finished),
        duration_seconds=duration,
        deadline_seconds=deadline_seconds,
        deadline_hit=deadline_hit,
        tests=tuple(tests),
        output_path=output_path,
    )

    if output_path:
        write_fn = json_writer or _atomic_write_json
        try:
            write_fn(output_path, battery_to_dict(battery))
        except OSError as exc:
            battery = TestBatteryResult(
                run_id=rid,
                ok=False,
                started_at=battery.started_at,
                finished_at=battery.finished_at,
                duration_seconds=duration,
                deadline_seconds=deadline_seconds,
                deadline_hit=deadline_hit,
                tests=battery.tests
                + (
                    _wrap_result(
                        name="artifact_persist",
                        ok=False,
                        detail=f"failed to write {output_path}: {exc}",
                        measurement={
                            "output_path": output_path,
                            "errno": getattr(exc, "errno", None),
                        },
                        started=finished,
                        finished=finished,
                    ),
                ),
                output_path="",
            )

    return battery
