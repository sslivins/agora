"""Persistent slot-manager state: ``/data/agora/slot-state.json``.

Lives on the shared ``/data`` partition so it survives a slot switch (the
whole point of the strike counter is that slot B's first boot still knows
how many times it has already failed).

Schema is versioned. v1 fields:

``schema_version``
    Bump on any breaking change. We migrate forward at load time.

``strikes``
    ``{slot: count}`` map. A "strike" is one consecutive failed tryboot to
    that slot. Resets to 0 on a successful ``promote_slot``. Three strikes
    pin the device (see ``pinned``).

``last_tryboot_target`` / ``last_tryboot_at``
    The slot we most recently asked the bootloader to try and the ISO-8601
    timestamp of the request.

``last_success_at``
    ISO-8601 timestamp of the most recent ``promote_slot`` call.

``pinned`` / ``pinned_at`` / ``pinned_reason``
    Set when ``record_tryboot_strike`` raises the count for a slot to 3.
    While pinned, ``trigger_tryboot`` refuses to run; an operator must call
    ``agora-slot-mgr unpin`` to clear.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from shared.state import atomic_write
from slot_mgr import paths

SCHEMA_VERSION = 1

# Maximum consecutive failed tryboots before we pin the device and require
# operator unpin. (Per Phase 1 acceptance: "3 consecutive failed tryboots
# leave the device pinned to last-known-good".)
STRIKE_LIMIT = 3


def utc_now_iso() -> str:
    """Return the current UTC time formatted as a Zulu ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SlotState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION
    strikes: dict[str, int] = Field(default_factory=lambda: {"1": 0, "2": 0})
    last_tryboot_target: Optional[int] = None
    last_tryboot_at: Optional[str] = None
    last_success_at: Optional[str] = None
    pinned: bool = False
    pinned_at: Optional[str] = None
    pinned_reason: Optional[str] = None

    def get_strikes(self, slot: int) -> int:
        return int(self.strikes.get(str(slot), 0))

    def set_strikes(self, slot: int, value: int) -> None:
        self.strikes[str(slot)] = int(value)


def load_state(path: Optional[Path] = None) -> SlotState:
    """Read slot-state.json, returning a fresh ``SlotState`` if missing."""
    p = path or paths.slot_state_path()
    try:
        return SlotState.model_validate_json(p.read_text())
    except (FileNotFoundError, ValueError):
        return SlotState()


def save_state(state: SlotState, path: Optional[Path] = None) -> None:
    """Write slot-state.json atomically."""
    p = path or paths.slot_state_path()
    atomic_write(p, state.model_dump_json(indent=2))
