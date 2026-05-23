"""Shared sway compositor lifecycle for the chromium-backend player.

Before multi-display, ``ChromiumPlayer`` owned its own sway scope; sway
ran with a single output declared and a single chromium kiosk wrapped
inside it. That model breaks down when we want two chromium kiosks --
one per HDMI output -- on the same Pi5: sway can only own the DRM
device once, and ``swaymsg reload`` does NOT pick up a hot-added
``output`` block. The fix is to bake **both** ``output HDMI-A-1`` and
``output HDMI-A-2`` blocks into the sway config at boot and never
restart sway at runtime. Per-slot chromium kiosks are then launched as
separate systemd scopes that connect to the running sway as wayland
clients; sway pins each kiosk's window to the correct output via
``for_window [app_id="agora-shell-<slot>"] move container to output
HDMI-A-<n>``.

This module owns:
  * Rendering the sway config with both outputs + per-slot ``for_window``
    pins baked in.
  * Starting / stopping the single sway scope.
  * Exposing the ``XDG_RUNTIME_DIR`` + ``WAYLAND_DISPLAY`` env vars that
    per-slot chromium subprocesses need to connect to the running sway.
  * Running ``swaymsg`` commands against the live sway (e.g. to move a
    window between outputs if a runtime re-pin is ever needed).

The per-slot ``ChromiumPlayer`` no longer launches sway when a
``SwayManager`` is provided -- it just runs its chromium kiosk as a
client of the sway managed here. See ``ChromiumPlayer.__init__``.

Single sway instance only. This class is **not** thread safe at the
start/stop boundary; treat it as a singleton owned by the
``Coordinator``.
"""
from __future__ import annotations

import logging
import os
import shlex
import subprocess
import uuid
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger("agora.player.sway_manager")

DEFAULT_RUNTIME_DIR = "/tmp/agora-sway-shell-run"
DEFAULT_CONFIG_PATH = "/tmp/agora-sway-shell-run/sway.conf"
DEFAULT_WAYLAND_DISPLAY = "wayland-1"

# Map slot id -> sway output name. The CM5 / Pi5 wire HDMI-0 to
# ``HDMI-A-1`` and HDMI-1 to ``HDMI-A-2`` (see ``shared/board.py``).
# Slot A always corresponds to the primary HDMI; slot B to the
# secondary.
SLOT_OUTPUT_MAP = {
    "A": "HDMI-A-1",
    "B": "HDMI-A-2",
}


def app_id_for_slot(slot: str) -> str:
    """Return the wayland app_id used to identify a slot's chromium kiosk.

    Chromium picks up its wayland app_id from the ``--class`` flag (yes,
    despite the X11-sounding name -- on Wayland this maps to xdg-shell
    ``app_id``). We pin each kiosk to its target output by app_id in the
    baked sway config, so the same string must be used here, on the
    chromium command line, and inside the sway config.
    """
    return f"agora-shell-{slot}"


class SwayManager:
    """Single sway compositor with both HDMI outputs declared at boot.

    Lifecycle::

        mgr = SwayManager(slots=("A", "B"))
        mgr.start()
        # ... chromium kiosks launched as separate scopes here ...
        mgr.stop()

    The runtime dir + wayland display socket are exposed via
    :meth:`env_for_client` so per-slot kiosk subprocesses can connect
    to this sway instance as wayland clients.

    ``slots`` controls which ``for_window`` pin lines are emitted; declare
    both ``("A", "B")`` on dual-HDMI Pi5 boards, just ``("A",)`` on
    single-output boards. The corresponding ``output`` blocks are always
    emitted for both HDMIs regardless -- sway happily ignores a declared
    output that the kernel doesn't expose, and this lets us bake one
    config that works on either board (a missing HDMI-A-2 just stays
    "disconnected" until something is plugged in).
    """

    def __init__(
        self,
        slots: Iterable[str] = ("A", "B"),
        runtime_dir: str = DEFAULT_RUNTIME_DIR,
        config_path: str = DEFAULT_CONFIG_PATH,
        wayland_display: str = DEFAULT_WAYLAND_DISPLAY,
    ) -> None:
        self._slots = tuple(slots)
        self._runtime_dir = runtime_dir
        self._config_path = config_path
        self._wayland_display = wayland_display

        self._process: Optional[subprocess.Popen] = None
        self._scope_unit: Optional[str] = None

    # ── Public lifecycle ──

    def start(self) -> None:
        """Start sway in a transient systemd scope.

        Idempotent: a successful call followed by another (without
        ``stop()``) is a no-op.
        """
        if self.is_alive():
            logger.debug("SwayManager.start: already running")
            return
        # If a previous run left a dead process handle, clear it first
        # so the scope unit gets a fresh uuid suffix.
        self._process = None
        self._scope_unit = None

        try:
            conf = self._write_config()
        except OSError as e:
            logger.error("SwayManager: could not write sway config: %s", e)
            return

        env = os.environ.copy()
        env["XDG_RUNTIME_DIR"] = self._runtime_dir
        env["WAYLAND_DISPLAY"] = self._wayland_display

        self._scope_unit = f"agora-sway-shell-{uuid.uuid4().hex[:8]}.scope"
        cmd = [
            "systemd-run", "--scope", "--quiet",
            "--unit", self._scope_unit, "--collect",
            "sway", "-c", str(conf),
        ]
        logger.info(
            "SwayManager: launching sway scope=%s (slots=%s, outputs=%s)",
            self._scope_unit,
            ",".join(self._slots),
            ",".join(SLOT_OUTPUT_MAP[s] for s in self._slots if s in SLOT_OUTPUT_MAP),
        )
        try:
            self._process = subprocess.Popen(
                cmd, env=env,
                start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except (FileNotFoundError, OSError) as e:
            logger.error("SwayManager: failed to launch sway: %s", e)
            self._process = None
            self._scope_unit = None

    def stop(self) -> None:
        """Stop the sway scope if running.

        Uses ``systemctl stop`` on the transient scope so the cgroup
        signals every descendant -- including any chromium kiosks that
        were started as separate scopes but parented to the wayland
        socket. Falls back to a cgroup-wide SIGKILL on timeout.
        """
        proc = self._process
        unit = self._scope_unit
        if proc and proc.poll() is None and unit:
            logger.info(
                "SwayManager: stopping scope=%s pid=%d", unit, proc.pid,
            )
            subprocess.run(
                ["systemctl", "stop", unit],
                timeout=8, check=False,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                subprocess.run(
                    ["systemctl", "kill", "-s", "SIGKILL", unit],
                    timeout=5, check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        self._process = None
        self._scope_unit = None

    def is_alive(self) -> bool:
        """True iff the sway subprocess is still running."""
        return bool(self._process and self._process.poll() is None)

    # ── Client env ──

    def env_for_client(self) -> dict[str, str]:
        """Env vars a per-slot chromium subprocess needs to connect to this sway.

        Returned dict can be merged into ``os.environ.copy()`` before
        ``subprocess.Popen``.
        """
        return {
            "XDG_RUNTIME_DIR": self._runtime_dir,
            "WAYLAND_DISPLAY": self._wayland_display,
        }

    # ── Runtime control ──

    def swaymsg(self, *args: str, timeout: float = 5.0) -> Optional[str]:
        """Run ``swaymsg <args>`` against the live sway. Returns stdout or None.

        Used by callers that need to move a window between outputs or
        otherwise reconfigure layout at runtime. Returns ``None`` on
        failure (sway not running, swaymsg missing, timeout, non-zero
        exit) -- callers should treat that as best-effort: the baked
        ``for_window`` pin should handle the common case so swaymsg
        calls are a runtime-fixup safety net.
        """
        env = os.environ.copy()
        env.update(self.env_for_client())
        try:
            result = subprocess.run(
                ["swaymsg", *args],
                env=env, timeout=timeout, check=False,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as e:
            logger.debug("SwayManager.swaymsg(%s) failed: %s", args, e)
            return None
        if result.returncode != 0:
            logger.debug(
                "SwayManager.swaymsg(%s) exit=%d stderr=%s",
                args, result.returncode, result.stderr.strip(),
            )
            return None
        return result.stdout

    # ── Internals ──

    def _write_config(self) -> Path:
        """Render the sway config and write it under the runtime dir.

        Same security posture as ``ChromiumPlayer._write_shell_sway_config``:
        runtime dir is 0o700, config opened ``O_NOFOLLOW`` so a
        pre-existing symlink can't redirect the write.
        """
        body = self._render_config()
        runtime_dir = Path(self._runtime_dir)
        runtime_dir.mkdir(mode=0o700, exist_ok=True)
        try:
            os.chmod(runtime_dir, 0o700)
        except OSError:
            pass
        conf_path = Path(self._config_path)
        o_nofollow = getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(
            str(conf_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | o_nofollow,
            0o600,
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(body)
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        return conf_path

    def _render_config(self) -> str:
        """Return the sway config body as a single string.

        Output blocks for **both** HDMI-A-1 and HDMI-A-2 are always
        emitted so single-output boards work with the same baked config
        as dual-output boards (sway treats the absent output as
        disconnected). Per-slot ``for_window`` pin lines are only
        emitted for slots in ``self._slots`` so a single-display device
        doesn't pin a non-existent app_id.
        """
        lines: list[str] = [
            "# Generated by agora SwayManager",
            "",
            "# Both HDMI outputs declared up-front: swaymsg reload",
            "# does NOT pick up hot-added output blocks, so we have",
            "# to bake them in at boot.",
            "output HDMI-A-1 bg #000000 solid_color",
            "output HDMI-A-2 bg #000000 solid_color",
            "",
            "seat * hide_cursor 1",
            "default_border none",
            "default_floating_border none",
            "hide_edge_borders both",
            "",
            "# Per-slot kiosk window pinning. Each chromium subprocess",
            "# is launched with --class=agora-shell-<slot> (which maps",
            "# to xdg-shell app_id on Wayland) and is moved to its",
            "# target HDMI output here.",
        ]
        for slot in self._slots:
            output = SLOT_OUTPUT_MAP.get(slot)
            if output is None:
                logger.warning(
                    "SwayManager: unknown slot %r, skipping for_window pin",
                    slot,
                )
                continue
            app_id = app_id_for_slot(slot)
            lines.append(
                'for_window [app_id="%s"] move container to output %s, '
                'fullscreen enable, border none'
                % (app_id, output),
            )
        lines.append("")
        return "\n".join(lines)
