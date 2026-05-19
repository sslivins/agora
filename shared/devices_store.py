"""Slot-keyed credential store for multi-display Pis.

Foundation for the multi-display work: each physical Pi can host up to
two virtual devices (one per HDMI), and each needs its own
``device_id`` / ``api_key`` pair.  This module owns the
``persist/devices.json`` file that stores those credentials by slot.

File shape::

    {
      "A": {"device_id": "...", "api_key": "..."},
      "B": {"device_id": "...", "api_key": "..."}     # absent if unbound
    }

File lives in ``persist_dir`` (flash) so credentials survive reboot;
``state_dir`` is tmpfs in production and would lose them.

PR 1 (multi-display) wires this module in as a dual-write layer:
credential read sites prefer ``devices.json`` slot A and fall back to
the legacy ``persist/api_key`` file; credential write sites update
both.  PR 3 will drop the legacy writes once the fleet has stabilised.

This module is intentionally tiny: no schema validation beyond key
presence, no migration logic (callers handle the fallback), no
concurrency primitives (writes are atomic via ``shared.state``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Optional

from shared.state import atomic_write

DEVICES_FILENAME = "devices.json"

# Slot identifiers used today.  Slot A is the default / single-display
# case; slot B is the opt-in second HDMI on multi-display Pis.
SLOT_A = "A"
SLOT_B = "B"
KNOWN_SLOTS = (SLOT_A, SLOT_B)


def devices_path(persist_dir: Path) -> Path:
    """Return the absolute path to the devices.json file."""
    return Path(persist_dir) / DEVICES_FILENAME


def _load_all(persist_dir: Path) -> Dict[str, dict]:
    """Return the full slot->credentials dict, or {} if no file/invalid."""
    path = devices_path(persist_dir)
    try:
        raw = path.read_text()
    except (FileNotFoundError, OSError):
        return {}
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    # Discard anything that isn't a dict-valued slot.
    return {k: v for k, v in data.items() if isinstance(v, dict)}


def _save_all(persist_dir: Path, slots: Dict[str, dict]) -> None:
    """Persist the slot dict atomically to devices.json."""
    path = devices_path(persist_dir)
    atomic_write(path, json.dumps(slots, indent=2, sort_keys=True))


def read_slot(persist_dir: Path, slot: str) -> Optional[dict]:
    """Return the credentials dict for ``slot`` or ``None`` if absent.

    The returned dict is a copy; callers may mutate it without affecting
    the on-disk file.
    """
    slots = _load_all(persist_dir)
    entry = slots.get(slot)
    if entry is None:
        return None
    return dict(entry)


def write_slot(persist_dir: Path, slot: str, payload: dict) -> None:
    """Upsert credentials for ``slot``.

    Any keys present in ``payload`` are persisted as-is; this is a
    full-replace at the slot level (not a per-field merge), matching
    the way today CMS rotates api_key by handing back the full key
    string rather than a diff.
    """
    if not isinstance(payload, dict):
        raise TypeError(f"write_slot payload must be a dict, got {type(payload).__name__}")
    slots = _load_all(persist_dir)
    slots[slot] = dict(payload)
    _save_all(persist_dir, slots)


def remove_slot(persist_dir: Path, slot: str) -> bool:
    """Remove ``slot`` from devices.json.

    Returns True if the slot was present and removed; False if it was
    already absent or the file did not exist.  If removing the slot
    leaves the file empty, the file itself is deleted.
    """
    path = devices_path(persist_dir)
    slots = _load_all(persist_dir)
    if slot not in slots:
        return False
    del slots[slot]
    if not slots:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return True
    _save_all(persist_dir, slots)
    return True


def list_slots(persist_dir: Path) -> Iterable[str]:
    """Return the slot identifiers currently populated in devices.json."""
    return tuple(_load_all(persist_dir).keys())


def wipe(persist_dir: Path) -> None:
    """Remove the entire devices.json file.

    Idempotent: returns without error if the file does not exist.  Used
    by the factory-reset path alongside the legacy api_key wipe.
    """
    path = devices_path(persist_dir)
    try:
        path.unlink()
    except FileNotFoundError:
        pass


# ── Read helpers with legacy fallback ────────────────────────────────

def read_api_key_with_fallback(
    persist_dir: Path,
    slot: str = SLOT_A,
    legacy_key_path: Optional[Path] = None,
) -> str:
    """Return the api_key for ``slot``, falling back to a legacy file.

    Read order:
      1. ``devices.json`` slot's ``api_key`` (if file present and field set);
      2. ``legacy_key_path`` (defaults to ``persist/api_key``) read as text.

    Returns empty string if neither source yields a key.  Always
    strips trailing whitespace so callers don't have to.
    """
    entry = read_slot(persist_dir, slot)
    if entry:
        key = entry.get("api_key") or ""
        if isinstance(key, str) and key.strip():
            return key.strip()
    legacy = legacy_key_path if legacy_key_path is not None else Path(persist_dir) / "api_key"
    try:
        return legacy.read_text().strip()
    except (FileNotFoundError, OSError):
        return ""
