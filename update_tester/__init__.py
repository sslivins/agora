"""Update-tester synthetic-load test battery for tentative slots.

Public entry point: :func:`run_test_battery`. Runs a 4-test battery
(render canary, WPS end-to-end, memory/CPU stress, /data integrity)
on a device that has just passed :func:`slot_confirm.slot_confirm`
in a ring whose ``gate_type`` is ``confirm_plus_test_suite``
(plan Decision #6, plan §157-165).

Phase 1 emits structured JSON to
``/data/agora/test-results/<run-id>.json`` and journald. There is
no CMS wire-up in Phase 1 — that lands in Phase 3.
"""

from update_tester.core import (
    DEFAULT_DEADLINE_SECONDS,
    DEFAULT_DMESG_RECENT_LINES,
    DEFAULT_FRAMEBUFFER_DEVICE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RENDER_DURATION_SECONDS,
    DEFAULT_RENDER_FPS_TOLERANCE,
    DEFAULT_RENDER_FRAME_BYTES,
    DEFAULT_RENDER_TARGET_FPS,
    DEFAULT_SCHEMA_VERSION_PATH,
    DEFAULT_SCRATCH_CHUNK_BYTES,
    DEFAULT_SCRATCH_DIR,
    DEFAULT_SCRATCH_SIZE_BYTES,
    DEFAULT_STRESS_BINARY,
    DEFAULT_STRESS_CPU_WORKERS,
    DEFAULT_STRESS_DURATION_SECONDS,
    DEFAULT_STRESS_VM_BYTES,
    DEFAULT_STRESS_VM_WORKERS,
    DEFAULT_THERMAL_PATH,
    DEFAULT_THERMAL_THROTTLE_CELSIUS,
    DEFAULT_WPS_FRESHNESS_SECONDS,
    DEFAULT_WPS_STATUS_PATH,
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

__all__ = [
    "DEFAULT_DEADLINE_SECONDS",
    "DEFAULT_DMESG_RECENT_LINES",
    "DEFAULT_FRAMEBUFFER_DEVICE",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_RENDER_DURATION_SECONDS",
    "DEFAULT_RENDER_FPS_TOLERANCE",
    "DEFAULT_RENDER_FRAME_BYTES",
    "DEFAULT_RENDER_TARGET_FPS",
    "DEFAULT_SCHEMA_VERSION_PATH",
    "DEFAULT_SCRATCH_CHUNK_BYTES",
    "DEFAULT_SCRATCH_DIR",
    "DEFAULT_SCRATCH_SIZE_BYTES",
    "DEFAULT_STRESS_BINARY",
    "DEFAULT_STRESS_CPU_WORKERS",
    "DEFAULT_STRESS_DURATION_SECONDS",
    "DEFAULT_STRESS_VM_BYTES",
    "DEFAULT_STRESS_VM_WORKERS",
    "DEFAULT_THERMAL_PATH",
    "DEFAULT_THERMAL_THROTTLE_CELSIUS",
    "DEFAULT_WPS_FRESHNESS_SECONDS",
    "DEFAULT_WPS_STATUS_PATH",
    "TestBatteryResult",
    "TestResult",
    "UpdateTesterError",
    "battery_to_dict",
    "check_data_integrity",
    "check_render_canary",
    "check_stress",
    "check_wps_synthetic",
    "run_test_battery",
]

__version__ = "0.1.0"
