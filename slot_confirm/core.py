"""Implementation of the slot-confirm 4-check gate.

Design mirrors :mod:`precheck.core` and :mod:`migration_fence.core`:

* Each check is a top-level function returning :class:`CheckResult`.
* Every check has an injection seam (``runner``, ``opener``, ``writer``,
  ``reader``) so tests can stub the underlying syscall without
  monkey-patching ``subprocess``/``builtins.open``.
* :func:`slot_confirm` is the aggregator: it looks up the boot state
  via :mod:`slot_mgr`, runs the 4 checks, and labels the recommended
  ``next_action``.

Routine failures (service inactive, framebuffer missing, ``/data`` read-only)
return ``CheckResult(ok=False, …)`` — never raise. :class:`SlotConfirmError`
is reserved for programmer errors (negative thresholds, missing
``slot_mgr`` module).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

# ── Defaults ────────────────────────────────────────────────────────────────

#: The agora-* systemd units that must be active for the gate to pass.
#: Order is informational — every entry is checked independently.
DEFAULT_AGORA_SERVICES: tuple[str, ...] = (
    "agora-api.service",
    "agora-cms-client.service",
    "agora-player.service",
    "agora-watchdog.service",
)

#: Minimum seconds a service must have been Active for the check to pass.
#: Plan §137: "agora.service active for ≥5 min".
DEFAULT_MIN_ACTIVE_SECONDS: int = 5 * 60

#: Default framebuffer device on Pi 5 (HDMI0).
DEFAULT_FRAMEBUFFER_DEVICE: str = "/dev/fb0"

#: Directory under which the data-writable check creates its probe file.
#: ``slot_mgr`` already creates and uses this directory for slot-state.json.
DEFAULT_DATA_PROBE_DIR: str = "/data/agora"

#: WPS receiver status file. Written by ``cms_client/service.py``.
#: Schema (subset used here): ``{"state": "connected" | "connecting" |
#: "disconnected" | "error", ...}``.
DEFAULT_CMS_STATUS_PATH: str = "/opt/agora/state/cms_status.json"

#: Maximum total time the gate is allowed to keep returning
#: ``"deferred"`` before flipping over to ``"strike"`` instead.
#:
#: Background: post-tryboot, systemd fires this unit every
#: ``RestartSec`` seconds (see ``agora-slot-mgr.service``) and the gate
#: returns ``"deferred"`` while the agora-* services haven't reached the
#: ``min_active_seconds`` bar. Without an upper bound, a slot that
#: NEVER stabilises (e.g. the player crash-loops permanently) would
#: defer forever, the os-updater would stay stuck in
#: ``tryboot_running``, and CMS upgrade dispatches would be silently
#: rejected -- the failure mode that motivated this constant.
#:
#: 30 minutes gives a healthy device plenty of headroom past the 5-min
#: service-age window even when the player restarts a few times during
#: early boot. Past that, the gate concludes "this slot is broken" and
#: returns ``"strike"`` so the strike counter advances and the device
#: rolls back to the previous good slot.
DEFAULT_MAX_DEFERRAL_SECONDS: int = 30 * 60


#: Path to the kernel-maintained system-uptime file. Used by
#: :func:`_default_boot_age` to bound how long the gate will defer
#: before striking.
DEFAULT_BOOT_AGE_PATH: str = "/proc/uptime"


# ── Types ───────────────────────────────────────────────────────────────────


class SlotConfirmError(RuntimeError):
    """Programmer error in calling a slot-confirm function.

    Routine "the check failed" outcomes return
    :class:`CheckResult` (``ok=False``); this exception is reserved
    for bugs (invalid threshold, missing required module, etc.).
    """


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one slot-confirm check."""

    name: str
    ok: bool
    detail: str
    measurement: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfirmStatus:
    """Aggregated outcome of running the slot-confirm 4-check gate.

    Attributes
    ----------
    ok
        ``True`` iff all 4 checks passed *and* the device was on a
        tentative boot. ``True`` is also returned when the boot was
        **not** tentative (already on the default slot) — there's
        nothing to confirm.
    next_action
        One of ``"promote"`` (tentative + all checks passed),
        ``"deferred"`` (tentative + the *only* failure is the
        agora-* services not having reached the ≥5 min Active
        threshold yet — caller should retry rather than strike,
        see bug #209), ``"strike"`` (tentative + any other
        failure), or ``"skipped"`` (not tentative). The CLI's
        ``--auto`` mode uses this to decide which :mod:`slot_mgr`
        verb to invoke (and ``"deferred"`` triggers a clean retry
        via systemd ``Restart=on-failure``).
    checks
        Tuple of all :class:`CheckResult` instances, in the order they
        were run. Empty when ``next_action == "skipped"``.
    running_slot
        The slot the device booted into (1 or 2). ``None`` if the
        running slot could not be determined (e.g. ``slot_mgr`` raised).
    tentative
        ``True`` if the device booted via ``[tryboot]`` (default slot
        differs from running slot). ``None`` if the state could not
        be determined.
    error
        Free-form detail when ``next_action == "error"`` or the boot
        state was indeterminate. Empty otherwise.
    """

    ok: bool
    next_action: str
    checks: tuple[CheckResult, ...] = ()
    running_slot: Optional[int] = None
    tentative: Optional[bool] = None
    error: str = ""


# ── Runner / opener / reader aliases ───────────────────────────────────────

Runner = Callable[..., "subprocess.CompletedProcess[str]"]
NowFn = Callable[[], datetime]
MonotonicNowFn = Callable[[], float]
BootAgeFn = Callable[[], float]
FrameOpener = Callable[[str], Any]
ProbeWriter = Callable[[str, bytes], None]
StatusReader = Callable[[str], str]
SlotStateFn = Callable[[], Any]


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


def _default_monotonic_now() -> float:
    """Default source for the monotonic "now" used by the service-age math.

    Returns ``time.monotonic()`` (CLOCK_MONOTONIC on Linux), which is
    immune to NTP step corrections. Pi 5 has no battery-backed RTC, so
    on a fresh boot the wallclock can jump forward by minutes-to-hours
    once ``systemd-timesyncd`` syncs — making
    ``datetime.now() - systemd_ActiveEnterTimestamp`` produce a
    negative age and a spurious slot-confirm strike. See bug #197.
    """
    return time.monotonic()


def _default_boot_age(path: str = DEFAULT_BOOT_AGE_PATH) -> float:
    """Default source for "how long has this kernel been up?" in seconds.

    Reads ``/proc/uptime``, whose first whitespace-separated field is
    seconds-since-boot as a float. Used by :func:`slot_confirm` to bound
    how long the gate may keep returning ``"deferred"`` before flipping
    over to ``"strike"`` (see :data:`DEFAULT_MAX_DEFERRAL_SECONDS`).

    On any I/O or parse failure we return ``0.0`` rather than raising:
    "we just booted" is the lenient interpretation and keeps the gate
    deferring (the systemd Restart loop will fire us again). The
    catastrophic failure mode this whole code path defends against is
    deferring *forever*; a transient read failure that leaves us
    deferring for one more cycle is not interesting.
    """
    try:
        with open(path, "r", encoding="ascii") as fh:
            first = fh.readline().split()
        if not first:
            return 0.0
        return float(first[0])
    except (OSError, ValueError):
        return 0.0


def _parse_systemd_monotonic_us(raw: str) -> Optional[int]:
    """Parse a systemd ``ActiveEnterTimestampMonotonic`` value.

    systemd emits the property as an integer microsecond count since
    the kernel started (i.e. against ``CLOCK_MONOTONIC``). Returns
    ``None`` for empty / ``"0"`` / unparseable values — ``0`` is the
    sentinel emitted by systemd before a unit has ever activated.
    """
    raw = (raw or "").strip()
    if not raw or raw == "0":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


# ── 1. agora services active for ≥5 min ────────────────────────────────────


def _parse_systemd_timestamp(raw: str) -> Optional[datetime]:
    """Parse a systemd ``ActiveEnterTimestamp`` (or similar) value.

    systemd emits timestamps as ``"<DOW> YYYY-MM-DD HH:MM:SS UTC"`` (e.g.
    ``"Mon 2026-04-22 19:30:11 UTC"``) when ``LANG=C`` is in effect, which
    is the case under systemd unit execution. Returns ``None`` for the
    sentinel empty value emitted before the service has ever activated.
    """
    raw = (raw or "").strip()
    if not raw or raw == "0" or raw.startswith("n/a"):
        return None
    # Strip the leading day-of-week if present ("Mon ").
    parts = raw.split(maxsplit=1)
    payload = parts[1] if len(parts) == 2 and parts[0].isalpha() else raw
    # Try "YYYY-MM-DD HH:MM:SS TZ"
    for fmt in (
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(payload, fmt)
            if dt.tzinfo is None:
                # systemd's "UTC" suffix is dropped by %Z on many platforms;
                # the timestamp itself is wall-clock UTC.
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def check_agora_services_active(
    *,
    services: Sequence[str] = DEFAULT_AGORA_SERVICES,
    min_active_seconds: int = DEFAULT_MIN_ACTIVE_SECONDS,
    runner: Optional[Runner] = None,
    now_fn: Optional[NowFn] = None,
    monotonic_now_fn: Optional[MonotonicNowFn] = None,
) -> CheckResult:
    """Verify every ``services`` entry is Active and has been so for ≥ ``min_active_seconds``.

    Returns ``ok=False`` on the first service that is not active or has
    been active for less than the threshold. The reason and offending
    service are recorded in ``detail`` and ``measurement``.

    Age math prefers ``ActiveEnterTimestampMonotonic`` (CLOCK_MONOTONIC)
    over the wallclock ``ActiveEnterTimestamp``. Pi 5 has no
    battery-backed RTC, so on a fresh boot the wallclock can jump
    forward by minutes-to-hours once ``systemd-timesyncd`` syncs,
    yielding negative wallclock-derived ages and spurious strikes.
    See bug #197. The wallclock path remains as a fallback when systemd
    reports ``ActiveEnterTimestampMonotonic=0`` (the sentinel for
    "never activated"); a negative age from either source is treated as
    a failure (with ``measurement.reason="negative_age"``) since both
    inputs must be monotonic w.r.t. their own clock.
    """
    if min_active_seconds < 0:
        raise SlotConfirmError(
            f"min_active_seconds must be non-negative, got {min_active_seconds!r}"
        )
    if not services:
        raise SlotConfirmError("services must contain at least one entry")

    run = runner or _default_runner
    now = (now_fn or _default_now)()
    monotonic_now_s = (monotonic_now_fn or _default_monotonic_now)()

    per_service: dict[str, dict[str, Any]] = {}
    for unit in services:
        try:
            cp = run(
                [
                    "systemctl",
                    "show",
                    unit,
                    "--property=ActiveState",
                    "--property=ActiveEnterTimestamp",
                    "--property=ActiveEnterTimestampMonotonic",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError as exc:
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=f"systemctl not available: {exc}",
                measurement={"service": unit, "errno": getattr(exc, "errno", None)},
            )
        except subprocess.TimeoutExpired:
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=f"systemctl timed out for {unit}",
                measurement={"service": unit},
            )

        active_state: Optional[str] = None
        enter_raw: str = ""
        enter_monotonic_raw: str = ""
        for line in (cp.stdout or "").splitlines():
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key == "ActiveState":
                active_state = value.strip()
            elif key == "ActiveEnterTimestamp":
                enter_raw = value.strip()
            elif key == "ActiveEnterTimestampMonotonic":
                enter_monotonic_raw = value.strip()

        if active_state != "active":
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=f"{unit} is {active_state!r}, expected 'active'",
                measurement={
                    "service": unit,
                    "active_state": active_state,
                    "stderr": (cp.stderr or "").strip(),
                },
            )

        entered = _parse_systemd_timestamp(enter_raw)
        if entered is None:
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=f"{unit} has no ActiveEnterTimestamp",
                measurement={
                    "service": unit,
                    "active_enter_raw": enter_raw,
                },
            )

        entered_monotonic_us = _parse_systemd_monotonic_us(enter_monotonic_raw)
        if entered_monotonic_us is not None:
            # Preferred path: monotonic clock; immune to NTP step.
            age = monotonic_now_s - (entered_monotonic_us / 1_000_000.0)
            clock_source = "monotonic"
        else:
            # Fallback: wallclock — only triggers if systemd reports
            # ``ActiveEnterTimestampMonotonic=0`` (unit never activated)
            # which would already have tripped the ``active_state`` gate.
            age = (now - entered).total_seconds()
            clock_source = "wallclock"

        per_service[unit] = {
            "active_for_seconds": age,
            "entered_at": entered.isoformat(),
            "clock_source": clock_source,
        }

        if age < 0:
            # Both clock sources are supposed to be monotonic w.r.t.
            # themselves; a negative delta means the systemd property
            # disagrees with our "now" (clock jumped backwards, or unit
            # entered ``ActiveEnterTimestampMonotonic`` after the
            # daemon read its own clock — neither can be reasoned about).
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=(
                    f"{unit} has negative active-for age ({age:.1f}s) "
                    f"via {clock_source} clock"
                ),
                measurement={
                    "service": unit,
                    "active_for_seconds": age,
                    "min_active_seconds": min_active_seconds,
                    "entered_at": entered.isoformat(),
                    "clock_source": clock_source,
                    "reason": "negative_age",
                },
            )

        if age < min_active_seconds:
            return CheckResult(
                name="agora_services_active",
                ok=False,
                detail=(
                    f"{unit} has been active for {age:.0f}s, "
                    f"need ≥{min_active_seconds}s"
                ),
                measurement={
                    "service": unit,
                    "active_for_seconds": age,
                    "min_active_seconds": min_active_seconds,
                    "entered_at": entered.isoformat(),
                    "clock_source": clock_source,
                    # ``not_yet_aged`` is the discriminator the aggregator
                    # uses to distinguish "this service is up but hasn't
                    # met the ≥5min bar yet — try me again in a moment"
                    # from "this service is broken — strike me". See
                    # :func:`_only_services_not_yet_aged` and bug #209
                    # for the failure that motivated this split.
                    "reason": "not_yet_aged",
                },
            )

    return CheckResult(
        name="agora_services_active",
        ok=True,
        detail=(
            f"all {len(services)} agora services active for "
            f"≥{min_active_seconds}s"
        ),
        measurement={
            "services": list(services),
            "min_active_seconds": min_active_seconds,
            "per_service": per_service,
        },
    )


# ── 2. framebuffer writable ────────────────────────────────────────────────


def check_framebuffer(
    *,
    device: str = DEFAULT_FRAMEBUFFER_DEVICE,
    opener: Optional[FrameOpener] = None,
) -> CheckResult:
    """Verify a test frame can be submitted to the framebuffer.

    The default implementation opens ``device`` for writing and writes
    a single zero-byte block, then closes. Any ``OSError`` (no device,
    EACCES, EIO) returns ``ok=False`` with the errno in the measurement.

    Tests pass an ``opener`` stub that mimics the ``open(path, mode)``
    signature; the function then calls ``write`` and ``close`` on the
    returned file object.
    """
    open_fn: FrameOpener = opener or (lambda path: open(path, "wb"))
    try:
        fh = open_fn(device)
    except FileNotFoundError as exc:
        return CheckResult(
            name="framebuffer",
            ok=False,
            detail=f"framebuffer device not found: {device}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
        )
    except PermissionError as exc:
        return CheckResult(
            name="framebuffer",
            ok=False,
            detail=f"framebuffer device not writable: {device}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
        )
    except OSError as exc:
        return CheckResult(
            name="framebuffer",
            ok=False,
            detail=f"framebuffer open failed: {exc}",
            measurement={"device": device, "errno": getattr(exc, "errno", None)},
        )

    try:
        try:
            fh.write(b"\x00")
        except OSError as exc:
            return CheckResult(
                name="framebuffer",
                ok=False,
                detail=f"framebuffer write failed: {exc}",
                measurement={"device": device, "errno": getattr(exc, "errno", None)},
            )
    finally:
        close = getattr(fh, "close", None)
        if callable(close):
            try:
                close()
            except OSError:
                pass

    return CheckResult(
        name="framebuffer",
        ok=True,
        detail=f"test frame written to {device}",
        measurement={"device": device, "bytes_written": 1},
    )


# ── 3. /data is mounted r/w ────────────────────────────────────────────────


def check_data_writable(
    *,
    probe_dir: str = DEFAULT_DATA_PROBE_DIR,
    writer: Optional[ProbeWriter] = None,
) -> CheckResult:
    """Verify ``probe_dir`` is writable by writing + removing a probe file.

    The default implementation:

    1. Creates a unique probe path under ``probe_dir``.
    2. Writes a small payload to it via ``open(path, "wb")``.
    3. Removes the probe file.

    Any failure (read-only mount, missing dir, ENOSPC) returns
    ``ok=False`` with the errno in the measurement.

    Tests pass a ``writer`` stub: ``writer(path, payload)`` that mimics
    the write step. Cleanup is the default's responsibility — tests
    don't need to remove anything.
    """
    probe_name = f".slot-confirm-probe-{uuid.uuid4().hex}"
    probe_path = os.path.join(probe_dir, probe_name)

    if writer is not None:
        try:
            writer(probe_path, b"slot-confirm probe\n")
        except FileNotFoundError as exc:
            return CheckResult(
                name="data_writable",
                ok=False,
                detail=f"probe directory not found: {probe_dir}",
                measurement={
                    "probe_path": probe_path,
                    "errno": getattr(exc, "errno", None),
                },
            )
        except (PermissionError, OSError) as exc:
            return CheckResult(
                name="data_writable",
                ok=False,
                detail=f"probe write failed: {exc}",
                measurement={
                    "probe_path": probe_path,
                    "errno": getattr(exc, "errno", None),
                },
            )
        return CheckResult(
            name="data_writable",
            ok=True,
            detail=f"probe write succeeded under {probe_dir}",
            measurement={"probe_path": probe_path},
        )

    try:
        with open(probe_path, "wb") as fh:
            fh.write(b"slot-confirm probe\n")
            fh.flush()
            os.fsync(fh.fileno())
    except FileNotFoundError as exc:
        return CheckResult(
            name="data_writable",
            ok=False,
            detail=f"probe directory not found: {probe_dir}",
            measurement={"probe_path": probe_path, "errno": getattr(exc, "errno", None)},
        )
    except (PermissionError, OSError) as exc:
        return CheckResult(
            name="data_writable",
            ok=False,
            detail=f"probe write failed: {exc}",
            measurement={"probe_path": probe_path, "errno": getattr(exc, "errno", None)},
        )

    try:
        os.unlink(probe_path)
    except OSError:
        # Leaving a stale probe file is not a failure of the r/w check —
        # the write itself succeeded, which is what we're testing.
        pass

    return CheckResult(
        name="data_writable",
        ok=True,
        detail=f"probe write+remove succeeded under {probe_dir}",
        measurement={"probe_path": probe_path},
    )


# ── 4. WPS receiver reports connected ──────────────────────────────────────


def check_wps_connected(
    *,
    status_path: str = DEFAULT_CMS_STATUS_PATH,
    reader: Optional[StatusReader] = None,
) -> CheckResult:
    """Verify the WPS receiver has written ``state: "connected"`` to its status file.

    Reads ``status_path`` (default ``/opt/agora/state/cms_status.json``)
    written by :mod:`cms_client.service`. ``state`` must equal
    ``"connected"``; any other value (``connecting``, ``disconnected``,
    ``error``) is a fail.

    Note: this does **not** verify CMS *reachability* — only that the
    WPS receiver process is up and reports a live connection. If the
    CMS itself is briefly unreachable, the receiver will write
    ``"disconnected"`` and this check will (correctly) fail.

    Tests pass a ``reader`` stub returning the raw JSON text.
    """
    if reader is not None:
        try:
            raw = reader(status_path)
        except FileNotFoundError as exc:
            return CheckResult(
                name="wps_connected",
                ok=False,
                detail=f"status file not found: {status_path}",
                measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            )
        except (PermissionError, OSError) as exc:
            return CheckResult(
                name="wps_connected",
                ok=False,
                detail=f"status file unreadable: {exc}",
                measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            )
    else:
        try:
            raw = Path(status_path).read_text()
        except FileNotFoundError as exc:
            return CheckResult(
                name="wps_connected",
                ok=False,
                detail=f"status file not found: {status_path}",
                measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            )
        except (PermissionError, OSError) as exc:
            return CheckResult(
                name="wps_connected",
                ok=False,
                detail=f"status file unreadable: {exc}",
                measurement={"status_path": status_path, "errno": getattr(exc, "errno", None)},
            )

    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        return CheckResult(
            name="wps_connected",
            ok=False,
            detail=f"status file not valid JSON: {exc}",
            measurement={"status_path": status_path},
        )

    if not isinstance(payload, dict):
        return CheckResult(
            name="wps_connected",
            ok=False,
            detail="status file payload is not a JSON object",
            measurement={"status_path": status_path, "payload_type": type(payload).__name__},
        )

    state = payload.get("state")
    registration = payload.get("registration", "")
    if state == "connected":
        return CheckResult(
            name="wps_connected",
            ok=True,
            detail=(
                f"WPS receiver reports connected (registration={registration!r})"
                if registration
                else "WPS receiver reports connected"
            ),
            measurement={
                "status_path": status_path,
                "state": state,
                "registration": registration,
            },
        )

    return CheckResult(
        name="wps_connected",
        ok=False,
        detail=f"WPS receiver state is {state!r}, expected 'connected'",
        measurement={
            "status_path": status_path,
            "state": state,
            "registration": registration,
        },
    )


# ── Aggregator ─────────────────────────────────────────────────────────────


def _default_slot_state() -> Any:
    """Lazily import :mod:`slot_mgr` so tests can stub it.

    Tests inject a fake by ``monkeypatch.setitem(sys.modules, "slot_mgr", fake)``
    or by passing ``slot_state_fn=`` to :func:`slot_confirm` directly.
    """
    from slot_mgr import slot_state  # noqa: WPS433 (intentional lazy import)

    return slot_state()


def _safe_slot_info(slot_state_fn: SlotStateFn) -> tuple[Optional[int], Optional[bool], str]:
    """Return ``(running_slot, tentative, error_detail)`` without raising."""
    try:
        status = slot_state_fn()
    except Exception as exc:  # noqa: BLE001
        return None, None, f"slot_state() raised: {exc}"

    running = getattr(status, "running_slot", None)
    tentative = getattr(status, "tentative", None)
    return running, tentative, ""


def slot_confirm(
    *,
    slot_state_fn: Optional[SlotStateFn] = None,
    agora_service_fn: Optional[Callable[[], CheckResult]] = None,
    framebuffer_fn: Optional[Callable[[], CheckResult]] = None,
    data_writable_fn: Optional[Callable[[], CheckResult]] = None,
    wps_connected_fn: Optional[Callable[[], CheckResult]] = None,
    boot_age_fn: Optional[BootAgeFn] = None,
    max_deferral_seconds: int = DEFAULT_MAX_DEFERRAL_SECONDS,
) -> ConfirmStatus:
    """Run the slot-confirm 4-check gate against the current boot.

    Steps:

    1. Look up running_slot + tentative via :mod:`slot_mgr`.
       * If lookup fails → return ``next_action="error"``.
       * If not tentative → return ``next_action="skipped"`` (nothing to
         confirm; the device is already on the default slot).
    2. Tentative → run all 4 checks and aggregate.
       * Every check passed → ``next_action="promote"``.
       * Only failure is "services not yet aged" (bug #209) AND boot
         age ≤ ``max_deferral_seconds`` → ``next_action="deferred"``
         (systemd will fire us again).
       * Only failure is "services not yet aged" AND boot age >
         ``max_deferral_seconds`` → ``next_action="strike"``. A slot
         that has gone this long without its services stabilising is
         broken; advance the strike counter so the device rolls back
         to the previous good slot instead of sitting stuck in
         ``tryboot_running`` forever (see
         :data:`DEFAULT_MAX_DEFERRAL_SECONDS`).
       * Any other check failed → ``next_action="strike"``.

    Each ``*_fn`` parameter overrides the corresponding check; useful
    for tests and for callers that want to short-circuit one of the
    pillars (e.g. a headless test rig with no framebuffer).

    Parameters
    ----------
    boot_age_fn
        Returns seconds-since-boot as a float. Injection seam for tests
        and for callers that want a different "elapsed since tryboot"
        source. Defaults to reading ``/proc/uptime``.
    max_deferral_seconds
        See :data:`DEFAULT_MAX_DEFERRAL_SECONDS`. Pass a smaller value in
        tests to exercise the cutover branch without sleeping.
    """
    slot_state_fn = slot_state_fn or _default_slot_state
    boot_age_fn = boot_age_fn or _default_boot_age
    running, tentative, err = _safe_slot_info(slot_state_fn)

    if err:
        return ConfirmStatus(
            ok=False,
            next_action="error",
            checks=(),
            running_slot=running,
            tentative=tentative,
            error=err,
        )

    if tentative is False:
        return ConfirmStatus(
            ok=True,
            next_action="skipped",
            checks=(),
            running_slot=running,
            tentative=False,
            error="",
        )

    # Tentative (True) or unknown (None) — run the gate either way.
    # Unknown is unusual but we still want signal; the caller decides
    # what to do with ok=True/next_action="promote" when tentative is None.
    fns = (
        agora_service_fn or check_agora_services_active,
        framebuffer_fn or check_framebuffer,
        data_writable_fn or check_data_writable,
        wps_connected_fn or check_wps_connected,
    )

    results: list[CheckResult] = []
    all_ok = True
    for fn in fns:
        result = fn()
        results.append(result)
        if not result.ok:
            all_ok = False

    if all_ok:
        next_action = "promote"
        ok = True
    elif _only_services_not_yet_aged(results):
        # The agora-* services are up and healthy, they just haven't
        # been Active for the required ≥5 min yet (see bug #209). We
        # MUST NOT strike — striking burns the slot's strike budget
        # for a transient, expected condition. Recommend the caller
        # try again in a few seconds; the systemd unit's Restart=
        # policy does exactly that.
        #
        # ...unless we've been deferring for too long. If the system
        # has been up longer than `max_deferral_seconds` and we are
        # STILL only deferring, the slot is not transiently slow --
        # it's broken (e.g. the player is crash-looping and never
        # accumulates the contiguous Active time the aging check
        # requires). Flip to strike so the strike counter advances
        # and the device rolls back. Without this cutover the
        # os-updater stays in tryboot_running forever and silently
        # rejects every future upgrade dispatch.
        try:
            boot_age = float(boot_age_fn())
        except Exception:  # noqa: BLE001
            boot_age = 0.0
        if boot_age > max_deferral_seconds:
            next_action = "strike"
            ok = False
        else:
            next_action = "deferred"
            ok = False
    else:
        next_action = "strike"
        ok = False

    return ConfirmStatus(
        ok=ok,
        next_action=next_action,
        checks=tuple(results),
        running_slot=running,
        tentative=tentative,
        error="",
    )


def _only_services_not_yet_aged(results: Sequence[CheckResult]) -> bool:
    """True iff the only failing check is ``agora_services_active`` with reason ``not_yet_aged``.

    The discriminator is :func:`check_agora_services_active`'s
    ``measurement["reason"] == "not_yet_aged"`` tag, which is emitted
    only on the "service is Active but hasn't met the ≥5 min
    threshold" branch. Other failure modes of the services check
    (inactive/failed unit, missing timestamp, negative-age clock
    bug) have a different reason or no reason and therefore still
    strike — they are genuine red flags, not the boot-timing race
    that bug #209 fixes.
    """
    saw_not_yet_aged = False
    for result in results:
        if result.ok:
            continue
        if result.name != "agora_services_active":
            return False
        reason = (result.measurement or {}).get("reason")
        if reason != "not_yet_aged":
            return False
        saw_not_yet_aged = True
    return saw_not_yet_aged
