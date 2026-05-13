"""A/B slot manager for the Pi 5 tryboot-based update path.

Public API:

    slot_state() -> SlotStatus
        Report the currently-running slot, the default slot, and whether
        the device is on a tentative tryboot.

    trigger_tryboot(target_slot, *, reboot=True) -> None
        Stage a tryboot to ``target_slot`` and (by default) reboot the
        device so the bootloader picks up the [tryboot] section once.

    promote_slot(target_slot) -> None
        Make ``target_slot`` the new permanent default. Resets the strike
        counter for that slot, updates last_success_at, and writes the
        forward-migration-allowed sentinel.

    unpin() -> None
        Clear the "pinned" flag set after three consecutive failed
        trybooots. Operator-only escape hatch.

    SlotStatus
        Frozen snapshot returned by ``slot_state()``.

    SlotState
        Pydantic model persisted to /data/agora/slot-state.json.
"""

from slot_mgr.core import (
    PinnedError,
    SlotStatus,
    promote_slot,
    record_tryboot_strike,
    slot_state,
    trigger_tryboot,
    unpin,
)
from slot_mgr.state import SlotState, load_state, save_state

__version__ = "0.1.0"
__all__ = [
    "PinnedError",
    "SlotState",
    "SlotStatus",
    "load_state",
    "promote_slot",
    "record_tryboot_strike",
    "save_state",
    "slot_state",
    "trigger_tryboot",
    "unpin",
]
