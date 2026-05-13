"""Implementation of the pre-flight checks library.

Every check is a small function with the same shape::

    def check_X(*, ...defaults..., runner=subprocess.run, ...) -> CheckResult

The injectable seams (``runner``, ``statvfs_fn``, ``stat_fn``,
``slot_state_fn``) default to real-system implementations and are
overridden in tests with fakes. This is the same pattern used by
:mod:`agora_watchdog.pinger` and :mod:`slot_mgr.core`.

Failure semantics
-----------------

A check never raises ``PrecheckError`` for a routine failure. If
``/data`` is short on space, ``check_data_free_space`` returns
``CheckResult(ok=False, detail="…")`` — not an exception. This makes it
easy to compose several checks with :func:`run_checks` and surface a
combined report.

``PrecheckError`` is reserved for *programmer* errors: invalid argument
types, a slot identifier outside ``{1, 2}``, etc. — bugs the caller
needs to fix in code, not at runtime.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence


# ── Defaults ────────────────────────────────────────────────────────────────

#: Where the persistent data partition is mounted on a Pi running agora-os.
DEFAULT_DATA_PATH: str = "/data"

#: Phase 1 free-space floor (plan.md §"Phase 1"). Phase 2 may pass a
#: smaller value (2 GiB hard floor) — the library is threshold-agnostic;
#: callers pass ``min_bytes`` explicitly.
DEFAULT_DATA_FREE_BYTES: int = 4 * 1024 * 1024 * 1024

#: Maximum acceptable NTP skew when the caller opts in to ``chronyc``
#: skew checking. Phase 2 spec: "NTP not fresh (skew > 5 min)".
DEFAULT_NTP_MAX_SKEW_SECONDS: int = 5 * 60

#: Pi 5 hardware-watchdog device. Same constant used by
#: :mod:`agora_watchdog.pinger`.
DEFAULT_WATCHDOG_DEVICE: str = "/dev/watchdog0"

#: Systemd unit that pets the hardware watchdog. Created by Phase 1
#: PR #167 (``feat: agora-watchdog Pi5 hardware watchdog pinger``).
DEFAULT_WATCHDOG_SERVICE: str = "agora-watchdog.service"

#: Directory where the kernel exposes per-partition symlinks-by-label.
#: ``root-A`` / ``root-B`` live here on a Phase-0-imaged Pi.
DEFAULT_PARTLABEL_BASE: str = "/dev/disk/by-partlabel"


# ── Types ───────────────────────────────────────────────────────────────────


class PrecheckError(RuntimeError):
    """Programmer error in calling a precheck function.

    Reserved for invalid arguments / impossible inputs. Routine
    "the check failed" outcomes are returned as
    ``CheckResult(ok=False, …)`` instead.
    """


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one pre-flight check.

    Attributes
    ----------
    name
        Short identifier for the check (``"data_free_space"``,
        ``"ntp_fresh"``, etc.). Stable across versions so log
        consumers can pattern-match.
    ok
        ``True`` iff the check passed.
    detail
        Human-readable one-line summary suitable for journald.
    measurement
        Numeric / structured side data for whoever consumes the
        result programmatically (a Prometheus exporter, an
        operator's status command, etc.). Always present, possibly
        empty.
    """

    name: str
    ok: bool
    detail: str
    measurement: Mapping[str, Any] = field(default_factory=dict)


# ── Runner alias ────────────────────────────────────────────────────────────

#: Callable type matching :func:`subprocess.run`. Tests pass a fake that
#: returns a canned :class:`subprocess.CompletedProcess`.
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def _default_runner(
    args: Sequence[str],
    *,
    check: bool = False,
    capture_output: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
) -> "subprocess.CompletedProcess[str]":
    """Thin wrapper around ``subprocess.run`` with our defaults applied."""
    return subprocess.run(
        list(args),
        check=check,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )


# ── 1. /data free space ─────────────────────────────────────────────────────


def check_data_free_space(
    *,
    path: str | os.PathLike[str] = DEFAULT_DATA_PATH,
    min_bytes: int = DEFAULT_DATA_FREE_BYTES,
    statvfs_fn: Callable[[str], os.statvfs_result] | None = None,
) -> CheckResult:
    """Verify ``path`` (default ``/data``) has at least ``min_bytes`` free.

    Uses ``os.statvfs`` for unprivileged, syscall-cheap measurement.
    The available-blocks figure (``f_bavail * f_frsize``) reflects
    space usable by an unprivileged user — same number ``df`` reports
    in the "Available" column.

    Parameters
    ----------
    path
        Mountpoint or any file/dir under the volume to inspect.
    min_bytes
        Refuse to pass if free bytes < this. Defaults to 4 GiB
        (Phase 1 threshold). Callers (os-updater, chaos tests)
        override.
    statvfs_fn
        Injection seam for tests. Defaults to :func:`os.statvfs`.

    Returns
    -------
    CheckResult
        ``ok=True`` iff free bytes ≥ ``min_bytes``. On a missing
        path (``FileNotFoundError``) or an unrelated ``OSError``,
        ``ok=False`` with the errno in ``measurement``.
    """
    if min_bytes < 0:
        raise PrecheckError(f"min_bytes must be non-negative, got {min_bytes!r}")

    statvfs_fn = statvfs_fn or getattr(os, "statvfs", None)
    if statvfs_fn is None:
        raise PrecheckError(
            "os.statvfs is unavailable on this platform; "
            "pass statvfs_fn= explicitly"
        )
    path_str = os.fspath(path)

    try:
        st = statvfs_fn(path_str)
    except FileNotFoundError as exc:
        return CheckResult(
            name="data_free_space",
            ok=False,
            detail=f"path not found: {path_str}",
            measurement={"path": path_str, "errno": exc.errno},
        )
    except OSError as exc:
        return CheckResult(
            name="data_free_space",
            ok=False,
            detail=f"statvfs({path_str}) failed: {exc.strerror or exc}",
            measurement={"path": path_str, "errno": exc.errno},
        )

    free_bytes = int(st.f_bavail) * int(st.f_frsize)
    total_bytes = int(st.f_blocks) * int(st.f_frsize)
    ok = free_bytes >= min_bytes

    if ok:
        detail = (
            f"{_humanize_bytes(free_bytes)} free on {path_str} "
            f"(>= {_humanize_bytes(min_bytes)} required)"
        )
    else:
        detail = (
            f"only {_humanize_bytes(free_bytes)} free on {path_str} "
            f"(need {_humanize_bytes(min_bytes)})"
        )

    return CheckResult(
        name="data_free_space",
        ok=ok,
        detail=detail,
        measurement={
            "path": path_str,
            "free_bytes": free_bytes,
            "total_bytes": total_bytes,
            "min_bytes": int(min_bytes),
        },
    )


def _humanize_bytes(n: int) -> str:
    """Render ``n`` bytes as e.g. ``"4.0 GiB"`` (always 1 decimal place)."""
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(f) < 1024.0:
            return f"{f:.1f} {unit}"
        f /= 1024.0
    return f"{f:.1f} PiB"


# ── 2. NTP fresh ────────────────────────────────────────────────────────────


def check_ntp_fresh(
    *,
    runner: Runner | None = None,
    timeout_s: float = 5.0,
) -> CheckResult:
    """Verify the system clock is NTP-synchronized.

    Runs ``timedatectl show -p NTPSynchronized --value`` (the
    systemd-timesyncd canonical answer) and accepts only literal
    ``yes``. Everything else (``no``, missing command, non-zero exit,
    timeout) maps to ``ok=False``.

    Phase 1 spec wants "NTP fresh"; Phase 2 spec wants "skew > 5 min ->
    fail". The synchronized-flag is sufficient for Phase 1 — when
    systemd-timesyncd has not yet drifted away from a successful sync,
    it leaves the flag set. A future enhancement can layer a skew check
    on top by parsing ``chronyc tracking`` when chrony is installed.

    Parameters
    ----------
    runner
        Injection seam for tests. Defaults to :func:`subprocess.run`
        with our standard kwargs.
    timeout_s
        Soft timeout applied to the ``timedatectl`` call.

    Returns
    -------
    CheckResult
        ``ok=True`` iff timedatectl reports ``NTPSynchronized=yes``.
    """
    runner = runner or _default_runner

    try:
        result = runner(
            ["timedatectl", "show", "-p", "NTPSynchronized", "--value"],
            timeout=timeout_s,
        )
    except FileNotFoundError:
        return CheckResult(
            name="ntp_fresh",
            ok=False,
            detail="timedatectl not installed",
            measurement={"reason": "missing_command"},
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name="ntp_fresh",
            ok=False,
            detail=f"timedatectl timed out after {timeout_s}s",
            measurement={"reason": "timeout", "timeout_s": timeout_s},
        )
    except OSError as exc:
        return CheckResult(
            name="ntp_fresh",
            ok=False,
            detail=f"timedatectl failed to launch: {exc.strerror or exc}",
            measurement={"reason": "oserror", "errno": exc.errno},
        )

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        return CheckResult(
            name="ntp_fresh",
            ok=False,
            detail=(
                f"timedatectl exited {result.returncode}: {stderr}"
                if stderr
                else f"timedatectl exited {result.returncode}"
            ),
            measurement={"reason": "nonzero_exit", "returncode": result.returncode},
        )

    raw = (result.stdout or "").strip()
    synchronized = raw.lower() == "yes"
    return CheckResult(
        name="ntp_fresh",
        ok=synchronized,
        detail=(
            "NTP-synchronized (timedatectl)"
            if synchronized
            else f"NTP not synchronized (timedatectl reported {raw!r})"
        ),
        measurement={"ntp_synchronized": raw},
    )


# ── 3. Inactive slot clean ──────────────────────────────────────────────────


_PARTLABEL_FOR_SLOT = {1: "root-A", 2: "root-B"}


def check_inactive_slot_clean(
    *,
    partlabel_base: str | os.PathLike[str] = DEFAULT_PARTLABEL_BASE,
    slot_state_fn: Callable[[], Any] | None = None,
    runner: Runner | None = None,
    timeout_s: float = 120.0,
) -> CheckResult:
    """Verify the inactive slot's rootfs is clean (``fsck -n`` is happy).

    "Inactive" means: the slot we are *not* currently running on. The
    library asks :func:`slot_mgr.slot_state` for the running slot,
    flips it (1<->2), looks up the matching ``/dev/disk/by-partlabel/root-{A,B}``
    symlink, and runs ``fsck -n <device>`` — a read-only filesystem
    check that NEVER modifies the partition.

    A return code of ``0`` means clean. ``1`` means errors were
    corrected (impossible with ``-n``, so we treat it as failure for
    safety). ``>=4`` means uncorrectable errors. Everything non-zero
    counts as "not clean."

    Parameters
    ----------
    partlabel_base
        Directory containing per-label device symlinks. Defaults to
        ``/dev/disk/by-partlabel``.
    slot_state_fn
        Callable returning an object with ``.running_slot`` attribute
        (int in ``{1, 2}`` or ``None``). Defaults to importing and
        calling :func:`slot_mgr.slot_state`. Tests inject a fake.
    runner
        Injection seam for the ``fsck`` invocation.
    timeout_s
        Soft timeout applied to ``fsck``. Default 2 min — plenty for
        a couple-of-GiB rootfs on a Pi 5's SD card.

    Returns
    -------
    CheckResult
        ``ok=True`` iff ``fsck -n`` exits ``0`` on the inactive
        partition. Edge cases (running_slot unknown, partlabel
        symlink missing, fsck not installed, timeout) all map to
        ``ok=False`` with a descriptive ``detail``.
    """
    runner = runner or _default_runner
    slot_state_fn = slot_state_fn or _default_slot_state

    try:
        status = slot_state_fn()
    except Exception as exc:  # noqa: BLE001 — any failure ⇒ inconclusive
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail=f"slot_state() raised {type(exc).__name__}: {exc}",
            measurement={"reason": "slot_state_error"},
        )

    running = getattr(status, "running_slot", None)
    if running not in (1, 2):
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail=f"running slot unknown (slot_state.running_slot={running!r})",
            measurement={"reason": "running_slot_unknown", "running_slot": running},
        )

    inactive = 2 if running == 1 else 1
    label = _PARTLABEL_FOR_SLOT[inactive]
    device = Path(os.fspath(partlabel_base)) / label

    if not device.exists():
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail=f"inactive-slot device missing: {device}",
            measurement={
                "reason": "device_missing",
                "device": str(device),
                "inactive_slot": inactive,
                "running_slot": running,
            },
        )

    try:
        result = runner(
            ["fsck", "-n", str(device)],
            timeout=timeout_s,
        )
    except FileNotFoundError:
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail="fsck not installed",
            measurement={"reason": "missing_command", "device": str(device)},
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail=f"fsck -n {device} timed out after {timeout_s}s",
            measurement={
                "reason": "timeout",
                "device": str(device),
                "timeout_s": timeout_s,
            },
        )
    except OSError as exc:
        return CheckResult(
            name="inactive_slot_clean",
            ok=False,
            detail=f"fsck failed to launch: {exc.strerror or exc}",
            measurement={
                "reason": "oserror",
                "device": str(device),
                "errno": exc.errno,
            },
        )

    ok = result.returncode == 0
    out_tail = _last_nonblank_line((result.stdout or "") + (result.stderr or ""))
    return CheckResult(
        name="inactive_slot_clean",
        ok=ok,
        detail=(
            f"inactive slot {label} clean (fsck -n returned 0)"
            if ok
            else (
                f"inactive slot {label} not clean (fsck -n returned "
                f"{result.returncode}): {out_tail}"
            )
        ),
        measurement={
            "device": str(device),
            "inactive_slot": inactive,
            "running_slot": running,
            "returncode": result.returncode,
        },
    )


def _default_slot_state() -> Any:
    """Lazy-import :func:`slot_mgr.slot_state` so the precheck library
    can be imported on a developer laptop without slot_mgr's runtime
    deps (autoboot.txt, /proc/cmdline) being satisfied.

    A bare ``from slot_mgr import slot_state`` at module top works fine
    — slot_mgr has no import-time side effects — but doing it lazily
    here keeps the dependency edge visible in code review and lets
    tests stub the function via ``slot_state_fn=`` without monkey-
    patching :mod:`slot_mgr`.
    """
    from slot_mgr import slot_state  # local import is intentional

    return slot_state()


def _last_nonblank_line(text: str) -> str:
    """Return the last non-blank line of ``text``, trimmed; ``""`` if none."""
    for line in reversed(text.splitlines()):
        s = line.strip()
        if s:
            return s
    return ""


# ── 4. Watchdog responsive ──────────────────────────────────────────────────


def check_watchdog_responsive(
    *,
    device: str | os.PathLike[str] = DEFAULT_WATCHDOG_DEVICE,
    service: str = DEFAULT_WATCHDOG_SERVICE,
    stat_fn: Callable[[str], os.stat_result] | None = None,
    runner: Runner | None = None,
    timeout_s: float = 5.0,
) -> CheckResult:
    """Verify the hardware watchdog is being pet.

    Two conditions, both required:

    1. ``device`` (default ``/dev/watchdog0``) exists.
    2. ``systemctl is-active <service>`` returns ``"active"`` (exit 0).

    We deliberately do NOT open ``/dev/watchdog0`` ourselves: on Linux
    that device is single-open. Opening here would either (a) fail with
    EBUSY because :mod:`agora_watchdog.pinger` already holds it (which
    is exactly the healthy state we are trying to verify), or (b)
    succeed because the pinger is dead — at which point we would have
    to remember to magic-close it ourselves before exiting or the
    watchdog would fire on us. Both outcomes are worse than asking
    systemd whether the daemon is active.

    Parameters
    ----------
    device
        Watchdog character device path.
    service
        Systemd unit name (without ``.service`` is fine too; pass the
        full name for clarity).
    stat_fn
        Injection seam, defaults to :func:`os.stat`.
    runner
        Injection seam, defaults to :func:`subprocess.run`.
    timeout_s
        Soft timeout for the ``systemctl`` call.

    Returns
    -------
    CheckResult
        ``ok=True`` iff both conditions hold.
    """
    runner = runner or _default_runner
    stat_fn = stat_fn or os.stat
    device_str = os.fspath(device)

    # (1) device present
    try:
        stat_fn(device_str)
    except FileNotFoundError:
        return CheckResult(
            name="watchdog_responsive",
            ok=False,
            detail=f"watchdog device missing: {device_str}",
            measurement={
                "reason": "device_missing",
                "device": device_str,
                "service": service,
            },
        )
    except OSError as exc:
        return CheckResult(
            name="watchdog_responsive",
            ok=False,
            detail=f"stat({device_str}) failed: {exc.strerror or exc}",
            measurement={
                "reason": "device_stat_error",
                "device": device_str,
                "errno": exc.errno,
            },
        )

    # (2) service active
    try:
        result = runner(
            ["systemctl", "is-active", service],
            timeout=timeout_s,
        )
    except FileNotFoundError:
        return CheckResult(
            name="watchdog_responsive",
            ok=False,
            detail="systemctl not installed",
            measurement={"reason": "missing_command"},
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name="watchdog_responsive",
            ok=False,
            detail=f"systemctl is-active {service} timed out after {timeout_s}s",
            measurement={
                "reason": "timeout",
                "service": service,
                "timeout_s": timeout_s,
            },
        )
    except OSError as exc:
        return CheckResult(
            name="watchdog_responsive",
            ok=False,
            detail=f"systemctl failed to launch: {exc.strerror or exc}",
            measurement={"reason": "oserror", "errno": exc.errno},
        )

    state = (result.stdout or "").strip()
    ok = result.returncode == 0 and state == "active"
    if ok:
        detail = f"watchdog active (device={device_str}, {service}=active)"
    else:
        detail = (
            f"agora-watchdog not active "
            f"(state={state!r}, returncode={result.returncode})"
        )
    return CheckResult(
        name="watchdog_responsive",
        ok=ok,
        detail=detail,
        measurement={
            "device": device_str,
            "service": service,
            "service_state": state,
            "returncode": result.returncode,
        },
    )


# ── Aggregator ──────────────────────────────────────────────────────────────


def run_checks(
    checks: Sequence[Callable[[], CheckResult]],
) -> tuple[bool, list[CheckResult]]:
    """Execute ``checks`` in order, return ``(all_ok, results)``.

    Every callable in ``checks`` is invoked with no arguments — wrap
    your specific check with :func:`functools.partial` or a lambda
    when you need to bind a particular threshold::

        from functools import partial
        ok, results = run_checks([
            partial(check_data_free_space, min_bytes=2 * 1024**3),
            check_ntp_fresh,
            check_inactive_slot_clean,
            check_watchdog_responsive,
        ])

    Aggregation is "all-or-nothing": ``all_ok`` is ``True`` iff every
    :class:`CheckResult` has ``ok=True``. If a check callable itself
    raises (a bug), the exception is *not* suppressed — that's a
    :class:`PrecheckError`-class condition we want to be noisy about.
    """
    results: list[CheckResult] = []
    for check in checks:
        result = check()
        if not isinstance(result, CheckResult):
            raise PrecheckError(
                f"check {check!r} returned {type(result).__name__}, "
                f"expected CheckResult"
            )
        results.append(result)
    return (all(r.ok for r in results), results)
