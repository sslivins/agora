"""Tests for ``player/coordinator.py``.

The Coordinator wires SwayManager + per-slot ChromiumPlayer instances
together. These tests use a mocked ChromiumPlayer (no subprocesses, no
sockets) and a mocked SwayManager (no sway lifecycle).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from player.coordinator import PORT_FOR_SLOT, Coordinator, _port_for_slot
from player.slot_state import SlotPaths


# ── port_for_slot ────────────────────────────────────────────────────


def test_port_map_has_both_slots() -> None:
    assert PORT_FOR_SLOT["A"] == 8780
    assert PORT_FOR_SLOT["B"] == 8781


def test_port_for_slot_unknown_raises() -> None:
    with pytest.raises(ValueError):
        _port_for_slot("Z")


# ── constructor ──────────────────────────────────────────────────────


def test_coordinator_rejects_invalid_slot_a_paths_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Coordinator(
            base_path=tmp_path, assets_dir=tmp_path / "assets",
            slot_a_paths_mode="invalid_mode",
        )


def test_coordinator_constructor_wires_sway_with_available_slots(tmp_path: Path) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    assert coord.sway_manager._slots == ("A", "B")
    assert coord.available_slots == ("A", "B")


# ── activate_slot ────────────────────────────────────────────────────


@pytest.fixture
def fake_chromium_class():
    """Patch ChromiumPlayer where Coordinator imports it; track constructor calls."""
    with patch("player.chromium_backend.ChromiumPlayer") as cls:
        instances: list[MagicMock] = []

        def make(*args, **kwargs):
            inst = MagicMock()
            inst.port = kwargs.get("port")
            inst.is_alive.return_value = True
            instances.append(inst)
            return inst
        cls.side_effect = make
        cls.instances = instances  # type: ignore[attr-defined]
        yield cls


def test_activate_slot_a_uses_legacy_paths_by_default(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
    )
    with patch.object(coord.sway_manager, "start"):
        state = coord.activate_slot("A")
    assert state is not None
    assert state.paths == SlotPaths.legacy(tmp_path)


def test_activate_slot_a_per_slot_paths_when_mode_per_slot(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
        slot_a_paths_mode="per_slot",
    )
    state = coord.activate_slot("A")
    assert state is not None
    assert state.paths == SlotPaths.for_slot(tmp_path, "A")


def test_activate_slot_b_always_uses_per_slot_paths(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    state = coord.activate_slot("B")
    assert state is not None
    assert state.paths == SlotPaths.for_slot(tmp_path, "B")


def test_activate_slot_uses_correct_port_per_slot(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    state_a = coord.activate_slot("A")
    state_b = coord.activate_slot("B")
    assert state_a is not None and state_a.chromium_player.port == 8780
    assert state_b is not None and state_b.chromium_player.port == 8781


def test_activate_slot_passes_sway_manager_and_app_id(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    coord.activate_slot("B")
    last_call = fake_chromium_class.call_args
    assert last_call.kwargs["sway_manager"] is coord.sway_manager
    assert last_call.kwargs["app_id"] == "agora-shell-B"
    assert last_call.kwargs["port"] == 8781


def test_activate_slot_idempotent(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
    )
    s1 = coord.activate_slot("A")
    s2 = coord.activate_slot("A")
    assert s1 is s2
    # ChromiumPlayer should only be constructed once.
    assert len(fake_chromium_class.instances) == 1  # type: ignore[attr-defined]


def test_activate_slot_unsupported_returns_none(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
    )
    result = coord.activate_slot("B")
    assert result is None
    assert "B" not in coord.slots


# ── deactivate_slot ──────────────────────────────────────────────────


def test_deactivate_slot_stops_and_removes(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    coord.activate_slot("B")
    cp_b = coord.slots["B"].chromium_player
    coord.deactivate_slot("B")
    cp_b.stop.assert_called_once()
    assert "B" not in coord.slots


def test_deactivate_slot_noop_when_never_activated(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    coord.deactivate_slot("B")  # must not raise


# ── start / stop ─────────────────────────────────────────────────────


def test_start_brings_up_sway_and_slot_a(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
    )
    with patch.object(coord.sway_manager, "start") as sm_start:
        coord.start()
    sm_start.assert_called_once()
    assert "A" in coord.slots


def test_start_activates_slot_b_when_devices_json_has_creds(tmp_path: Path, fake_chromium_class) -> None:
    """If devices.json slot B has creds at startup, Coordinator brings B up."""
    persist = tmp_path / "persist"
    persist.mkdir()
    (persist / "devices.json").write_text(
        '{"B": {"device_id": "d2", "api_key": "k2"}}'
    )
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    with patch.object(coord.sway_manager, "start"):
        coord.start()
    assert "A" in coord.slots
    assert "B" in coord.slots


def test_start_does_not_activate_slot_b_when_no_creds(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    with patch.object(coord.sway_manager, "start"):
        coord.start()
    assert "A" in coord.slots
    assert "B" not in coord.slots


def test_start_does_not_activate_slot_b_when_unavailable(tmp_path: Path, fake_chromium_class) -> None:
    """Even with slot B creds present, do nothing on a single-output board."""
    persist = tmp_path / "persist"
    persist.mkdir()
    (persist / "devices.json").write_text(
        '{"B": {"device_id": "d2", "api_key": "k2"}}'
    )
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),  # single-output board
    )
    with patch.object(coord.sway_manager, "start"):
        coord.start()
    assert "B" not in coord.slots


def test_stop_tears_down_all_slots_then_sway(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    with patch.object(coord.sway_manager, "start"):
        coord.activate_slot("A")
        coord.activate_slot("B")
    cps = [coord.slots[s].chromium_player for s in ("A", "B")]
    with patch.object(coord.sway_manager, "stop") as sm_stop:
        coord.stop()
    for cp in cps:
        cp.stop.assert_called_once()
    sm_stop.assert_called_once()
    assert coord.slots == {}


# ── event callback ───────────────────────────────────────────────────


def test_event_callback_passes_slot_id(tmp_path: Path, fake_chromium_class) -> None:
    """The Coordinator wraps the slot-aware callback to look like the per-slot one."""
    events: list[tuple[str, dict]] = []
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
        on_chromium_event=lambda slot, payload: events.append((slot, payload)),
    )
    coord.activate_slot("A")
    coord.activate_slot("B")
    # Capture the on_event callbacks passed to each ChromiumPlayer.
    call_a, call_b = fake_chromium_class.call_args_list
    cb_a = call_a.kwargs["on_event"]
    cb_b = call_b.kwargs["on_event"]
    cb_a({"event": "ended"})
    cb_b({"event": "error", "msg": "oops"})
    assert events == [
        ("A", {"event": "ended"}),
        ("B", {"event": "error", "msg": "oops"}),
    ]


def test_event_callback_swallows_exceptions(tmp_path: Path, fake_chromium_class) -> None:
    """A raising callback must not propagate into the chromium event handler."""
    def bad(slot: str, payload: dict) -> None:
        raise RuntimeError("intentional")

    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
        on_chromium_event=bad,
    )
    coord.activate_slot("A")
    cb = fake_chromium_class.call_args.kwargs["on_event"]
    # Should not raise:
    cb({"event": "anything"})


def test_event_callback_none_when_no_handler(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A",),
    )
    coord.activate_slot("A")
    assert fake_chromium_class.call_args.kwargs["on_event"] is None


# ── has_slot ─────────────────────────────────────────────────────────


def test_has_slot_tracks_activation(tmp_path: Path, fake_chromium_class) -> None:
    coord = Coordinator(
        base_path=tmp_path, assets_dir=tmp_path / "assets",
        available_slots=("A", "B"),
    )
    assert not coord.has_slot("A")
    coord.activate_slot("A")
    assert coord.has_slot("A")
    coord.deactivate_slot("A")
    assert not coord.has_slot("A")
