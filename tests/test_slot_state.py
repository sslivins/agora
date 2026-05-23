"""Tests for ``player/slot_state.py``.

These tests exercise the dataclass + lifecycle without spinning up a
real chromium player. We mock ``ChromiumPlayer`` to a MagicMock with the
``start`` / ``stop`` / ``is_alive`` / ``port`` surface we depend on.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from player.slot_state import SlotPaths, SlotState


# ── SlotPaths.legacy ─────────────────────────────────────────────────


def test_slot_paths_legacy_layout(tmp_path: Path) -> None:
    p = SlotPaths.legacy(tmp_path)
    assert p.desired == tmp_path / "state" / "desired.json"
    assert p.current == tmp_path / "state" / "current.json"
    assert p.cms_status == tmp_path / "state" / "cms_status.json"
    assert p.schedule == tmp_path / "state" / "schedule.json"
    assert p.splash == tmp_path / "persist" / "splash"


def test_slot_paths_legacy_is_frozen(tmp_path: Path) -> None:
    p = SlotPaths.legacy(tmp_path)
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        p.desired = Path("/whatever")  # type: ignore[misc]


# ── SlotPaths.for_slot ───────────────────────────────────────────────


def test_slot_paths_for_slot_a(tmp_path: Path) -> None:
    p = SlotPaths.for_slot(tmp_path, "A")
    assert p.desired == tmp_path / "state" / "displays" / "A" / "desired.json"
    assert p.current == tmp_path / "state" / "displays" / "A" / "current.json"
    assert p.cms_status == tmp_path / "state" / "displays" / "A" / "cms_status.json"
    assert p.schedule == tmp_path / "state" / "displays" / "A" / "schedule.json"
    assert p.splash == tmp_path / "persist" / "displays" / "A" / "splash"


def test_slot_paths_for_slot_b(tmp_path: Path) -> None:
    p = SlotPaths.for_slot(tmp_path, "B")
    assert "displays" in str(p.desired)
    assert "B" in str(p.desired)
    assert "B" in str(p.splash)


def test_slot_paths_a_and_b_do_not_collide(tmp_path: Path) -> None:
    a = SlotPaths.for_slot(tmp_path, "A")
    b = SlotPaths.for_slot(tmp_path, "B")
    for attr in ("desired", "current", "cms_status", "schedule", "splash"):
        assert getattr(a, attr) != getattr(b, attr), f"{attr} collides"


# ── SlotState lifecycle ──────────────────────────────────────────────


def _make_chromium_mock() -> MagicMock:
    cp = MagicMock()
    cp.port = 8780
    cp.is_alive.return_value = True
    return cp


def test_slot_state_ensure_dirs_creates_state_and_persist(tmp_path: Path) -> None:
    paths = SlotPaths.for_slot(tmp_path, "B")
    state = SlotState(slot="B", paths=paths, chromium_player=_make_chromium_mock())
    state.ensure_dirs()
    assert (tmp_path / "state" / "displays" / "B").is_dir()
    assert (tmp_path / "persist" / "displays" / "B").is_dir()


def test_slot_state_ensure_dirs_idempotent(tmp_path: Path) -> None:
    paths = SlotPaths.for_slot(tmp_path, "A")
    state = SlotState(slot="A", paths=paths, chromium_player=_make_chromium_mock())
    state.ensure_dirs()
    state.ensure_dirs()  # second call must not raise
    assert (tmp_path / "state" / "displays" / "A").is_dir()


def test_slot_state_start_calls_chromium_start(tmp_path: Path) -> None:
    cp = _make_chromium_mock()
    state = SlotState(
        slot="A",
        paths=SlotPaths.legacy(tmp_path),
        chromium_player=cp,
    )
    state.start()
    cp.start.assert_called_once()


def test_slot_state_start_ensures_dirs(tmp_path: Path) -> None:
    cp = _make_chromium_mock()
    state = SlotState(
        slot="B",
        paths=SlotPaths.for_slot(tmp_path, "B"),
        chromium_player=cp,
    )
    state.start()
    assert (tmp_path / "state" / "displays" / "B").is_dir()


def test_slot_state_stop_calls_chromium_stop(tmp_path: Path) -> None:
    cp = _make_chromium_mock()
    state = SlotState(
        slot="A",
        paths=SlotPaths.legacy(tmp_path),
        chromium_player=cp,
    )
    state.stop()
    cp.stop.assert_called_once()


def test_slot_state_is_alive_delegates_to_chromium(tmp_path: Path) -> None:
    cp = _make_chromium_mock()
    state = SlotState(
        slot="A",
        paths=SlotPaths.legacy(tmp_path),
        chromium_player=cp,
    )
    cp.is_alive.return_value = True
    assert state.is_alive() is True
    cp.is_alive.return_value = False
    assert state.is_alive() is False


def test_slot_state_repr_includes_slot_and_port(tmp_path: Path) -> None:
    cp = _make_chromium_mock()
    cp.port = 8781
    state = SlotState(
        slot="B",
        paths=SlotPaths.for_slot(tmp_path, "B"),
        chromium_player=cp,
    )
    text = repr(state)
    assert "B" in text
    assert "8781" in text
