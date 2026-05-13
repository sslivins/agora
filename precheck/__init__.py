"""Pre-flight checks library used by slot-mgr, os-updater, and chaos tests.

This is a *library* — there is no CLI and no systemd unit. Callers compose
the individual ``check_*`` functions and (optionally) aggregate with
:func:`run_checks` to decide whether it is safe to begin a slot transition
or an OS update.

The Phase 1 spec (plan.md §"Phase 1 — Bootloader integration") lists four
checks; this module ships one function per check, plus an aggregator:

* :func:`check_data_free_space` — refuse if /data has less than the
  caller's threshold of free bytes
* :func:`check_ntp_fresh` — refuse if the system clock is not
  ``timedatectl``-NTP-synchronized
* :func:`check_inactive_slot_clean` — refuse if ``fsck -n`` on the
  inactive slot's rootfs reports any filesystem error
* :func:`check_watchdog_responsive` — refuse if ``/dev/watchdog0`` is
  missing OR ``agora-watchdog.service`` is not active
* :func:`run_checks` — execute a sequence of checks and report
  ``(all_ok, [CheckResult])``

Every check accepts injection seams (``runner``, ``statvfs_fn``, etc.)
so the whole module is exercised by unit tests on a developer laptop
without a Pi, without ``/proc``, without ``/dev/watchdog0``, and without
``timedatectl``. The defaults talk to the real syscalls / commands on a
production Pi.

A check NEVER raises on a failed check — it returns
``CheckResult(ok=False, ...)``. The only exception class
(:class:`PrecheckError`) is reserved for *programmer* errors (invalid
arguments) so a buggy caller is loud, while a routine "free space is
low" outcome stays a normal return value the caller can format / log.
"""

from precheck.core import (
    CheckResult,
    DEFAULT_DATA_FREE_BYTES,
    DEFAULT_DATA_PATH,
    DEFAULT_NTP_MAX_SKEW_SECONDS,
    DEFAULT_PARTLABEL_BASE,
    DEFAULT_WATCHDOG_DEVICE,
    DEFAULT_WATCHDOG_SERVICE,
    PrecheckError,
    check_data_free_space,
    check_inactive_slot_clean,
    check_ntp_fresh,
    check_watchdog_responsive,
    run_checks,
)

__version__ = "0.1.0"

__all__ = [
    "CheckResult",
    "DEFAULT_DATA_FREE_BYTES",
    "DEFAULT_DATA_PATH",
    "DEFAULT_NTP_MAX_SKEW_SECONDS",
    "DEFAULT_PARTLABEL_BASE",
    "DEFAULT_WATCHDOG_DEVICE",
    "DEFAULT_WATCHDOG_SERVICE",
    "PrecheckError",
    "check_data_free_space",
    "check_inactive_slot_clean",
    "check_ntp_fresh",
    "check_watchdog_responsive",
    "run_checks",
]
