"""Slot-confirm gate for the Pi 5 A/B tryboot update path.

Runs immediately after a tentative boot to decide whether the new slot
is healthy enough to promote (write the new ``[all]`` boot_partition)
or whether the device should revert by recording a strike against the
tryboot target.

The 4-check gate (per Phase 1 plan §137-141):

1. ``agora-*.service`` units have been active for ≥5 min
2. The framebuffer is writable (a test frame can be submitted)
3. ``/data`` is mounted r/w (a probe file can be created and removed)
4. The WPS receiver process reports connected (``cms_status.json``
   state field == ``"connected"``)

Note: CMS *reachability* is deliberately **not** in the gate. The
device only needs to know its own WPS receiver is alive — being
asked to slot-confirm a boot where the upstream CMS is briefly
unreachable would falsely fail the gate.

Public API:

    slot_confirm() -> ConfirmStatus
        Aggregated result. Looks up running_slot + tentative via
        :func:`slot_mgr.slot_state`, runs the 4 checks, and indicates
        the recommended ``next_action`` (``"promote"``, ``"strike"``,
        or ``"skipped"`` when not on a tentative boot).

    check_agora_services_active() -> CheckResult
    check_framebuffer() -> CheckResult
    check_data_writable() -> CheckResult
    check_wps_connected() -> CheckResult
        Individual checks; useful for tests, status commands, and
        ad-hoc CLI invocations.

    CheckResult / ConfirmStatus
        Frozen dataclasses; the result types.

CLI:

    python -m slot_confirm                # run checks, print JSON, always exit 0
    python -m slot_confirm --check        # exit 1 if any check failed
    python -m slot_confirm --auto         # run checks, then promote / strike via slot_mgr
"""

from slot_confirm.core import (
    DEFAULT_AGORA_SERVICES,
    DEFAULT_BOOT_AGE_PATH,
    DEFAULT_CMS_STATUS_PATH,
    DEFAULT_DATA_PROBE_DIR,
    DEFAULT_FRAMEBUFFER_DEVICE,
    DEFAULT_MAX_DEFERRAL_SECONDS,
    DEFAULT_MIN_ACTIVE_SECONDS,
    CheckResult,
    ConfirmStatus,
    SlotConfirmError,
    check_agora_services_active,
    check_data_writable,
    check_framebuffer,
    check_wps_connected,
    slot_confirm,
)

__version__ = "0.1.0"
__all__ = [
    "DEFAULT_AGORA_SERVICES",
    "DEFAULT_BOOT_AGE_PATH",
    "DEFAULT_CMS_STATUS_PATH",
    "DEFAULT_DATA_PROBE_DIR",
    "DEFAULT_FRAMEBUFFER_DEVICE",
    "DEFAULT_MAX_DEFERRAL_SECONDS",
    "DEFAULT_MIN_ACTIVE_SECONDS",
    "CheckResult",
    "ConfirmStatus",
    "SlotConfirmError",
    "__version__",
    "check_agora_services_active",
    "check_data_writable",
    "check_framebuffer",
    "check_wps_connected",
    "slot_confirm",
]
