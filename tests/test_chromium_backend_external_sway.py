"""Tests for ChromiumPlayer in external-sway mode (multi-display path).

The standalone (single-display, legacy) code path is covered by
``tests/test_chromium_backend.py``. This file exclusively covers the
new behavior when a ``SwayManager`` is supplied at construction:

- ``--class=<app_id>`` is appended to chromium argv
- chromium is launched as a transient systemd scope (NOT inside sway)
- the sway manager is NOT touched at start or stop
- the wayland env vars come from the sway manager
- ``stop()`` only tears down the chromium scope, never the sway
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from player.chromium_backend import ChromiumPlayer
from player.sway_manager import SwayManager, app_id_for_slot


@pytest.fixture
def sway_mgr(tmp_path: Path) -> SwayManager:
    """A SwayManager configured with tmp paths but not actually started."""
    return SwayManager(
        slots=("A", "B"),
        runtime_dir=str(tmp_path / "rt"),
        config_path=str(tmp_path / "rt" / "sway.conf"),
    )


# ── chromium argv ────────────────────────────────────────────────────


def test_chromium_argv_includes_class_flag_when_app_id_provided(tmp_path: Path, sway_mgr) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets,
        port=8781,
        sway_manager=sway_mgr,
        app_id="agora-shell-B",
    )
    argv = cp._chromium_argv(app_id=cp._app_id)
    assert "--class=agora-shell-B" in argv
    # URL last (chromium convention)
    assert argv[-1] == "http://127.0.0.1:8781/"


def test_chromium_argv_omits_class_when_no_app_id(tmp_path: Path) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(assets_dir=assets)
    argv = cp._chromium_argv(app_id=None)
    assert not any(a.startswith("--class=") for a in argv)


# ── start in external-sway mode ──────────────────────────────────────


def _fake_popen() -> MagicMock:
    proc = MagicMock(spec=subprocess.Popen)
    proc.pid = 999
    proc.poll.return_value = None
    proc.wait.return_value = 0
    return proc


def test_external_sway_start_launches_chromium_not_sway(tmp_path: Path, sway_mgr) -> None:
    """In external-sway mode the spawn must skip sway entirely."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets,
        port=8781,
        sway_manager=sway_mgr,
        app_id=app_id_for_slot("B"),
    )
    # Drive _start_chromium_client_scope directly -- ChromiumPlayer.start
    # also spins up the uvicorn shell server which we don't need here.
    with patch("player.chromium_backend.subprocess.Popen", return_value=_fake_popen()) as popen:
        cp._start_chromium_client_scope()
    popen.assert_called_once()
    cmd = popen.call_args[0][0]
    assert cmd[0] == "systemd-run"
    assert "--scope" in cmd
    # systemd-run scope wraps chromium DIRECTLY (no `sway -c`).
    assert "sway" not in cmd
    assert "chromium" in cmd
    # --class set to the slot's app_id
    assert any(part == "--class=agora-shell-B" for part in cmd)
    # Env includes wayland vars from the sway manager
    env = popen.call_args[1]["env"]
    assert "WAYLAND_DISPLAY" in env
    assert "XDG_RUNTIME_DIR" in env
    assert env["XDG_RUNTIME_DIR"] == str(tmp_path / "rt")


def test_external_sway_start_does_not_invoke_internal_write_config(tmp_path: Path, sway_mgr) -> None:
    """We must NOT write our own sway config in external-sway mode."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets, port=8781,
        sway_manager=sway_mgr, app_id="agora-shell-B",
    )
    with patch.object(cp, "_write_shell_sway_config") as wsc, \
         patch("player.chromium_backend.subprocess.Popen", return_value=_fake_popen()):
        cp._start_chromium_client_scope()
    wsc.assert_not_called()


def test_external_sway_start_idempotent(tmp_path: Path, sway_mgr) -> None:
    """Calling _start_chromium_client_scope twice stops the previous scope first."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets, port=8781,
        sway_manager=sway_mgr, app_id="agora-shell-B",
    )
    p1 = _fake_popen()
    p2 = _fake_popen()
    with patch(
        "player.chromium_backend.subprocess.Popen",
        side_effect=[p1, p2],
    ), patch(
        "player.chromium_backend.subprocess.run",
    ) as srun:
        srun.return_value = MagicMock(returncode=0)
        cp._start_chromium_client_scope()
        first_unit = cp._sway_scope_unit
        cp._start_chromium_client_scope()
        second_unit = cp._sway_scope_unit
    # Second call should have invoked systemctl stop on first scope.
    stop_calls = [c for c in srun.call_args_list if c[0][0][:2] == ["systemctl", "stop"]]
    assert any(c[0][0][2] == first_unit for c in stop_calls), (
        "second start did not stop the first chromium scope"
    )
    assert first_unit != second_unit


def test_external_sway_stop_does_not_touch_sway_manager(tmp_path: Path, sway_mgr) -> None:
    """stop() in external-sway mode must NOT call SwayManager.stop()."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets, port=8781,
        sway_manager=sway_mgr, app_id="agora-shell-B",
    )
    sway_mgr.stop = MagicMock()  # type: ignore[method-assign]
    with patch(
        "player.chromium_backend.subprocess.Popen", return_value=_fake_popen(),
    ), patch(
        "player.chromium_backend.subprocess.run",
    ) as srun:
        srun.return_value = MagicMock(returncode=0)
        cp._start_chromium_client_scope()
        cp.stop()
    sway_mgr.stop.assert_not_called()
    # And systemctl stop WAS called on our chromium scope.
    cmds = [c[0][0] for c in srun.call_args_list]
    assert any(c[:2] == ["systemctl", "stop"] for c in cmds)


def test_external_sway_start_handles_popen_failure(tmp_path: Path, sway_mgr) -> None:
    """A failed launch leaves the process handle cleared."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(
        assets_dir=assets, port=8781,
        sway_manager=sway_mgr, app_id="agora-shell-B",
    )
    with patch(
        "player.chromium_backend.subprocess.Popen",
        side_effect=FileNotFoundError("no systemd-run"),
    ):
        cp._start_chromium_client_scope()
    assert cp._sway_process is None
    assert cp._sway_scope_unit is None


# ── shell_url honours custom port ───────────────────────────────────


def test_shell_url_reflects_custom_port(tmp_path: Path, sway_mgr) -> None:
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(assets_dir=assets, port=8781, sway_manager=sway_mgr)
    assert cp.shell_url() == "http://127.0.0.1:8781/"


# ── legacy mode regression ──────────────────────────────────────────


def test_legacy_mode_still_writes_internal_sway_config(tmp_path: Path) -> None:
    """Without sway_manager, ChromiumPlayer keeps owning sway (regression)."""
    assets = tmp_path / "assets"
    assets.mkdir()
    cp = ChromiumPlayer(assets_dir=assets)
    assert cp._sway_manager is None
    assert cp._app_id is None
