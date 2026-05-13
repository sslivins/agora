"""File paths used by slot_mgr.

Every path is resolved at call time from an environment variable so that
unit tests can point the library at a fake /boot/firmware and /data tree
without monkey-patching module-level constants.

Defaults match the Bookworm / Phase 0 layout:

* /boot/firmware/autoboot.txt - the one we mutate to switch slots
* /boot/firmware-b/autoboot.txt - the mirror on boot-B (kept in sync)
* /data/agora/slot-state.json - persistent strike counter + history
* /data/agora/migration-allowed - forward-migration fence sentinel
* /proc/cmdline - inspected to detect the running slot
"""

from __future__ import annotations

import os
from pathlib import Path

#: Layout of the two boot partitions when both are mounted simultaneously.
#: Phase 0 mounts boot-A at /boot/firmware (the kernel-standard path); the
#: B-side mirror is exposed at /boot/firmware-b so slot_mgr can write the
#: same autoboot.txt to both copies (F6).
DEFAULT_AUTOBOOT_PATH = "/boot/firmware/autoboot.txt"
DEFAULT_AUTOBOOT_MIRROR_PATH = "/boot/firmware-b/autoboot.txt"

DEFAULT_DATA_DIR = "/data/agora"
DEFAULT_PROC_CMDLINE = "/proc/cmdline"


def autoboot_path() -> Path:
    """Primary autoboot.txt on the active boot partition (boot-A by default)."""
    return Path(os.environ.get("AGORA_AUTOBOOT_PATH", DEFAULT_AUTOBOOT_PATH))


def autoboot_mirror_path() -> Path:
    """Mirror autoboot.txt on the inactive boot partition (boot-B by default).

    Phase 0 ships byte-identical copies on both boot partitions so the device
    isn't single-point-of-failure on boot-A; slot_mgr is responsible for
    keeping the two in sync on every promote/tryboot rewrite.
    """
    return Path(os.environ.get("AGORA_AUTOBOOT_MIRROR_PATH", DEFAULT_AUTOBOOT_MIRROR_PATH))


def data_dir() -> Path:
    """Persistent slot-mgr state directory (lives on /data so it survives slot switches)."""
    return Path(os.environ.get("AGORA_SLOT_DATA_DIR", DEFAULT_DATA_DIR))


def slot_state_path() -> Path:
    """JSON file holding the strike counter and tryboot history."""
    return data_dir() / "slot-state.json"


def migration_allowed_sentinel_path() -> Path:
    """Sentinel written by ``promote_slot()`` to tell agora.service that running
    forward-migrations on /data is safe (the running slot is now the permanent one).
    """
    return data_dir() / "migration-allowed"


def proc_cmdline_path() -> Path:
    """The kernel command line - parsed to detect the running slot via PARTLABEL."""
    return Path(os.environ.get("AGORA_PROC_CMDLINE", DEFAULT_PROC_CMDLINE))
