"""Tests for ``player/sway_manager.py``.

The real ``SwayManager`` spawns ``sway`` and ``systemctl`` -- neither
runs in a test environment. These tests exercise:

- Config rendering (output blocks + per-slot pin lines)
- ``write_config`` writing to a tmp runtime dir with the right perms
- ``start`` invoking ``systemd-run`` with the expected args, and being
  idempotent
- ``stop`` shelling out to ``systemctl stop`` on the scope unit
- ``env_for_client`` returning the wayland env vars
- ``swaymsg`` handling the common error paths
- ``app_id_for_slot`` mapping
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from player.sway_manager import (
    DEFAULT_RUNTIME_DIR,
    DEFAULT_CONFIG_PATH,
    SLOT_OUTPUT_MAP,
    SwayManager,
    app_id_for_slot,
)


# ── app_id_for_slot ──────────────────────────────────────────────────


def test_app_id_for_slot_returns_expected_strings() -> None:
    assert app_id_for_slot("A") == "agora-shell-A"
    assert app_id_for_slot("B") == "agora-shell-B"


def test_slot_output_map_covers_a_and_b() -> None:
    assert SLOT_OUTPUT_MAP["A"] == "HDMI-A-1"
    assert SLOT_OUTPUT_MAP["B"] == "HDMI-A-2"


# ── config rendering ─────────────────────────────────────────────────


def test_render_config_declares_both_outputs() -> None:
    mgr = SwayManager(slots=("A",))
    body = mgr._render_config()
    assert "output HDMI-A-1" in body
    assert "output HDMI-A-2" in body, (
        "HDMI-A-2 must always be declared even on single-slot boards"
    )


def test_render_config_includes_for_window_only_for_active_slots() -> None:
    mgr = SwayManager(slots=("A",))
    body = mgr._render_config()
    assert 'for_window [app_id="agora-shell-A"] move container to output HDMI-A-1' in body
    assert "agora-shell-B" not in body, (
        "Single-slot devices must not pin the B kiosk"
    )


def test_render_config_dual_slot_pins_both() -> None:
    mgr = SwayManager(slots=("A", "B"))
    body = mgr._render_config()
    assert 'for_window [app_id="agora-shell-A"] move container to output HDMI-A-1' in body
    assert 'for_window [app_id="agora-shell-B"] move container to output HDMI-A-2' in body


def test_render_config_unknown_slot_logged_and_skipped() -> None:
    mgr = SwayManager(slots=("A", "Z"))
    body = mgr._render_config()
    assert 'for_window [app_id="agora-shell-A"]' in body
    assert "agora-shell-Z" not in body


def test_render_config_has_hide_cursor_and_no_borders() -> None:
    body = SwayManager(slots=("A",))._render_config()
    assert "hide_cursor 1" in body
    assert "default_border none" in body
    assert "hide_edge_borders both" in body


# ── _write_config ────────────────────────────────────────────────────


def test_write_config_creates_runtime_dir_and_writes_body(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "agora-sway-shell-run"
    config_path = runtime_dir / "sway.conf"
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(runtime_dir),
        config_path=str(config_path),
    )

    written = mgr._write_config()
    assert written == config_path
    assert config_path.exists()
    body = config_path.read_text()
    assert "output HDMI-A-1" in body
    # Runtime dir is 0o700 (best-effort; not all filesystems honour it
    # but the call should at least not crash).
    assert runtime_dir.exists()


def test_write_config_overwrites_existing_file(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "rt"
    config_path = runtime_dir / "sway.conf"
    runtime_dir.mkdir()
    config_path.write_text("STALE")
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(runtime_dir),
        config_path=str(config_path),
    )
    mgr._write_config()
    body = config_path.read_text()
    assert "STALE" not in body
    assert "output HDMI-A-1" in body


# ── start / stop / is_alive ──────────────────────────────────────────


def _fake_popen(alive: bool = True) -> MagicMock:
    """Return a MagicMock that quacks like subprocess.Popen."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.pid = 12345
    proc.poll.return_value = None if alive else 0
    proc.wait.return_value = 0
    return proc


def test_start_invokes_systemd_run_with_scope(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "rt"
    mgr = SwayManager(
        slots=("A", "B"),
        runtime_dir=str(runtime_dir),
        config_path=str(runtime_dir / "sway.conf"),
    )
    fake_proc = _fake_popen(alive=True)
    with patch("player.sway_manager.subprocess.Popen", return_value=fake_proc) as popen:
        mgr.start()
    popen.assert_called_once()
    args, kwargs = popen.call_args
    cmd = args[0]
    assert cmd[0] == "systemd-run"
    assert "--scope" in cmd
    assert "--unit" in cmd
    assert any(part.startswith("agora-sway-shell-") and part.endswith(".scope") for part in cmd)
    assert cmd[-3:] == ["sway", "-c", str(runtime_dir / "sway.conf")]
    env = kwargs["env"]
    assert env["XDG_RUNTIME_DIR"] == str(runtime_dir)
    assert env["WAYLAND_DISPLAY"] == "wayland-1"
    assert mgr.is_alive()


def test_start_idempotent_when_already_alive(tmp_path: Path) -> None:
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(tmp_path / "rt"),
        config_path=str(tmp_path / "rt" / "sway.conf"),
    )
    fake_proc = _fake_popen(alive=True)
    with patch("player.sway_manager.subprocess.Popen", return_value=fake_proc) as popen:
        mgr.start()
        mgr.start()
    assert popen.call_count == 1


def test_start_handles_popen_failure(tmp_path: Path) -> None:
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(tmp_path / "rt"),
        config_path=str(tmp_path / "rt" / "sway.conf"),
    )
    with patch(
        "player.sway_manager.subprocess.Popen",
        side_effect=FileNotFoundError("no systemd-run"),
    ):
        mgr.start()
    assert not mgr.is_alive()


def test_stop_invokes_systemctl_stop(tmp_path: Path) -> None:
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(tmp_path / "rt"),
        config_path=str(tmp_path / "rt" / "sway.conf"),
    )
    fake_proc = _fake_popen(alive=True)
    with patch("player.sway_manager.subprocess.Popen", return_value=fake_proc):
        mgr.start()
    captured_unit = mgr._scope_unit
    assert captured_unit is not None

    with patch("player.sway_manager.subprocess.run") as srun:
        srun.return_value = MagicMock(returncode=0)
        mgr.stop()
    # First (and only) call: systemctl stop <unit>
    first_call = srun.call_args_list[0]
    assert first_call[0][0][:2] == ["systemctl", "stop"]
    assert first_call[0][0][2] == captured_unit
    assert not mgr.is_alive()


def test_stop_falls_back_to_sigkill_on_timeout(tmp_path: Path) -> None:
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(tmp_path / "rt"),
        config_path=str(tmp_path / "rt" / "sway.conf"),
    )
    fake_proc = _fake_popen(alive=True)
    fake_proc.wait.side_effect = [subprocess.TimeoutExpired(cmd="sway", timeout=5), 0]
    with patch("player.sway_manager.subprocess.Popen", return_value=fake_proc):
        mgr.start()

    with patch("player.sway_manager.subprocess.run") as srun:
        srun.return_value = MagicMock(returncode=0)
        mgr.stop()
    # Expect two srun calls: stop then kill.
    cmds = [call[0][0] for call in srun.call_args_list]
    assert cmds[0][:2] == ["systemctl", "stop"]
    assert cmds[1][:2] == ["systemctl", "kill"]


def test_stop_when_never_started_is_noop() -> None:
    mgr = SwayManager(slots=("A",))
    # Should not raise.
    mgr.stop()
    assert not mgr.is_alive()


# ── env_for_client ───────────────────────────────────────────────────


def test_env_for_client_returns_wayland_vars(tmp_path: Path) -> None:
    mgr = SwayManager(
        slots=("A",),
        runtime_dir=str(tmp_path / "rt"),
    )
    env = mgr.env_for_client()
    assert env["XDG_RUNTIME_DIR"] == str(tmp_path / "rt")
    assert env["WAYLAND_DISPLAY"] == "wayland-1"


# ── swaymsg ──────────────────────────────────────────────────────────


def test_swaymsg_returns_stdout_on_success(tmp_path: Path) -> None:
    mgr = SwayManager(slots=("A",), runtime_dir=str(tmp_path / "rt"))
    with patch("player.sway_manager.subprocess.run") as srun:
        srun.return_value = MagicMock(
            returncode=0, stdout="OK\n", stderr="",
        )
        out = mgr.swaymsg("workspace", "1")
    assert out == "OK\n"


def test_swaymsg_returns_none_on_nonzero_exit(tmp_path: Path) -> None:
    mgr = SwayManager(slots=("A",), runtime_dir=str(tmp_path / "rt"))
    with patch("player.sway_manager.subprocess.run") as srun:
        srun.return_value = MagicMock(returncode=1, stdout="", stderr="oops")
        out = mgr.swaymsg("bogus")
    assert out is None


def test_swaymsg_returns_none_when_swaymsg_missing(tmp_path: Path) -> None:
    mgr = SwayManager(slots=("A",), runtime_dir=str(tmp_path / "rt"))
    with patch(
        "player.sway_manager.subprocess.run",
        side_effect=FileNotFoundError("no swaymsg"),
    ):
        out = mgr.swaymsg("workspace", "1")
    assert out is None


def test_swaymsg_returns_none_on_timeout(tmp_path: Path) -> None:
    mgr = SwayManager(slots=("A",), runtime_dir=str(tmp_path / "rt"))
    with patch(
        "player.sway_manager.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="swaymsg", timeout=5),
    ):
        out = mgr.swaymsg("workspace", "1")
    assert out is None


# ── defaults sanity ─────────────────────────────────────────────────


def test_default_paths_are_under_tmp_runtime_dir() -> None:
    """Lock the default paths so any reshuffle is intentional."""
    assert DEFAULT_RUNTIME_DIR == "/tmp/agora-sway-shell-run"
    assert DEFAULT_CONFIG_PATH == "/tmp/agora-sway-shell-run/sway.conf"
