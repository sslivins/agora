"""Tests for AgoraPlayer's slot-B reconciliation tick.

The cms_client persists bind/unbind decisions to ``devices.json``;
AgoraPlayer reconciles the actual Coordinator state to match on a
periodic GLib tick. These tests exercise that logic without spinning
up a real player.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Match the import-time mocks used by other player_service tests.
sys.modules.setdefault("gi", MagicMock())
sys.modules.setdefault("gi.repository", MagicMock())

from shared.devices_store import SLOT_B, write_slot  # noqa: E402


def _make_player_with_coordinator(tmp_path: Path, *, has_b: bool, available=("A", "B")):
    from player.service import AgoraPlayer

    # Bypass __init__; the reconciler only touches a few attrs.
    player = AgoraPlayer.__new__(AgoraPlayer)
    player.persist_dir = tmp_path / "persist"
    player.persist_dir.mkdir(parents=True, exist_ok=True)

    coord = MagicMock()
    coord.available_slots = available
    coord.has_slot.return_value = has_b
    player._coordinator = coord
    return player, coord


def test_reconcile_activates_slot_b_when_creds_appear(tmp_path: Path):
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False)
    write_slot(player.persist_dir, SLOT_B, {"device_id": "d", "api_key": "k"})
    player._reconcile_slot_b()
    coord.activate_slot.assert_called_once_with("B")
    coord.deactivate_slot.assert_not_called()


def test_reconcile_deactivates_slot_b_when_creds_vanish(tmp_path: Path):
    player, coord = _make_player_with_coordinator(tmp_path, has_b=True)
    # No write_slot -- devices.json absent.
    player._reconcile_slot_b()
    coord.deactivate_slot.assert_called_once_with("B")
    coord.activate_slot.assert_not_called()


def test_reconcile_noop_when_creds_and_slot_b_already_up(tmp_path: Path):
    player, coord = _make_player_with_coordinator(tmp_path, has_b=True)
    write_slot(player.persist_dir, SLOT_B, {"device_id": "d", "api_key": "k"})
    player._reconcile_slot_b()
    coord.activate_slot.assert_not_called()
    coord.deactivate_slot.assert_not_called()


def test_reconcile_noop_when_no_creds_and_slot_b_down(tmp_path: Path):
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False)
    player._reconcile_slot_b()
    coord.activate_slot.assert_not_called()
    coord.deactivate_slot.assert_not_called()


def test_reconcile_ignores_when_slot_b_unavailable(tmp_path: Path):
    """Single-output boards: never touch slot B regardless of devices.json."""
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False, available=("A",))
    write_slot(player.persist_dir, SLOT_B, {"device_id": "d", "api_key": "k"})
    player._reconcile_slot_b()
    coord.activate_slot.assert_not_called()
    coord.deactivate_slot.assert_not_called()


def test_reconcile_treats_empty_api_key_as_no_creds(tmp_path: Path):
    """Defensive: a slot entry with empty api_key isn't real creds."""
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False)
    write_slot(player.persist_dir, SLOT_B, {"device_id": "d", "api_key": ""})
    player._reconcile_slot_b()
    coord.activate_slot.assert_not_called()


def test_reconcile_tick_swallows_exceptions(tmp_path: Path):
    """The GLib timeout callback must never raise -- it would break the loop."""
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False)
    coord.activate_slot.side_effect = RuntimeError("boom")
    write_slot(player.persist_dir, SLOT_B, {"device_id": "d", "api_key": "k"})
    # Returns True so GLib re-arms it.
    assert player._reconcile_slots_tick() is True


def test_reconcile_tick_returns_true_for_glib_rearm(tmp_path: Path):
    player, coord = _make_player_with_coordinator(tmp_path, has_b=False)
    assert player._reconcile_slots_tick() is True


def test_reconcile_skipped_when_no_coordinator(tmp_path: Path):
    from player.service import AgoraPlayer
    player = AgoraPlayer.__new__(AgoraPlayer)
    player.persist_dir = tmp_path / "persist"
    player.persist_dir.mkdir(parents=True, exist_ok=True)
    player._coordinator = None
    # Must not raise:
    player._reconcile_slot_b()
