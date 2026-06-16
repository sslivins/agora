"""
Chromium-based playback backend (demo).

A small FastAPI/uvicorn server bound to 127.0.0.1 serves a static "player
shell" SPA and the device's asset directory, plus a control WebSocket.
A single sway+chromium subprocess (wrapped in a transient systemd scope)
is launched in kiosk mode pointing at that server, and the daemon drives
the shell over the WebSocket with JSON commands.

This is the default playback backend on production Pis, intended to
make rich image transitions and unified image/video rendering easy to
iterate on.
Opt out via environment variable AGORA_PLAYER_BACKEND=mpv to fall back
to the legacy mpv path.

The protocol is documented in ``player/shell/README.md``.

Note: this module owns its own sway+chromium kiosk pointed at the shell
URL. A separate sway is spawned by ``AgoraPlayer._start_sway`` for
webpage-asset rendering. The two cannot coexist (single DRM device),
so mode-mixing chromium-backed playback with webpage assets is a known
follow-up — when a webpage URL arrives, the shell-kiosk sway is torn
down, and apply_desired will need to bring it back when returning to
chromium-backed playback.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import subprocess
import threading
import uuid
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from player.sway_manager import SwayManager  # noqa: F401  (forward ref)

logger = logging.getLogger("agora.player.chromium")

# `from __future__ import annotations` makes all annotations strings, and
# FastAPI resolves `WebSocket` annotations on closure-defined handlers via
# `get_type_hints()`, which looks them up in the module's globals (NOT the
# function's local scope). Without this top-level import the WS handler in
# `_build_app` is misinterpreted as expecting a `websocket` query param,
# and every connect closes with code 1008. The try/except keeps the module
# importable on hosts that don't have fastapi installed (unit tests).
try:
    from fastapi import WebSocket as WebSocket  # noqa: F401
except ImportError:
    pass

DEFAULT_BIND_HOST = "127.0.0.1"
DEFAULT_BIND_PORT = 8780
DEFAULT_TRANSITION_MS = 600

SHELL_DIR = Path(__file__).resolve().parent / "shell"

# Sway runtime paths for the chromium-shell kiosk. Kept distinct from the
# webpage-asset path's ``/tmp/agora-sway-run`` so the two can't trample
# each other's config file or runtime state — but only ONE sway can be
# alive at a time on the actual DRM device, so the coordination problem
# is real even with split paths. See module docstring.
_SHELL_SWAY_RUNTIME_DIR = "/tmp/agora-sway-shell-run"
_SHELL_SWAY_CONFIG_PATH = "/tmp/agora-sway-shell-run/sway.conf"

# Process-wide flag so we only fire `plymouth quit` once per agora-player
# lifetime. Subsequent kiosk restarts (e.g. after a webpage-mode bounce)
# don't need to re-quit a plymouth that already exited.
_plymouth_quit_done = False


def _quit_plymouth() -> None:
    """Tell Plymouth to release the DRM device before sway claims it.

    Mirrors ``AgoraPlayer._quit_plymouth`` in player/service.py: under the
    mpv backend that helper is called before every GStreamer/mpv pipeline
    build, but the chromium backend never went through those code paths,
    so plymouth was left holding DRM master and sway would silently fail
    to come up — visible as a long-running splash that never advances.

    ``--retain-splash`` keeps plymouth's last frame on the framebuffer
    until chromium paints its first frame so there is no flash to black
    during the handoff.
    """
    global _plymouth_quit_done
    if _plymouth_quit_done:
        return
    _plymouth_quit_done = True
    try:
        subprocess.run(
            ["/usr/bin/plymouth", "quit", "--retain-splash"],
            timeout=5,
            capture_output=True,
        )
        logger.info("ChromiumPlayer: plymouth quit (retained splash)")
    except FileNotFoundError:
        # Plymouth not installed (e.g. dev workstation, unit-test host).
        pass
    except Exception as e:
        logger.debug("ChromiumPlayer: plymouth quit skipped: %s", e)


class ChromiumPlayer:
    """Manage a persistent sway+chromium kiosk + shell control channel.

    Lifecycle:
        player = ChromiumPlayer(assets_dir=Path("/opt/agora/assets"))
        player.start()
        player.show_image("/opt/agora/assets/images/foo.jpg")
        ...
        player.stop()

    Threading model:
        * The uvicorn server runs in its own thread with a dedicated asyncio
          event loop. All WebSocket I/O happens on that loop.
        * Callers invoke ``show_*`` / ``stop_playback`` from any thread; the
          method pushes the command into the loop via
          ``run_coroutine_threadsafe``.
        * The sway+chromium subprocess is unmanaged by the asyncio loop —
          it's wrapped in a transient systemd scope (mirroring
          ``AgoraPlayer._start_sway``) so the cgroup tracks chromium
          grandchildren that sway's ``exec`` double-forks out of any
          process group. Without the scope, ``killpg`` on the sway pid
          would leak chromium and the next start would fail to acquire
          ``/dev/dri/cardX``.

    Two operating modes:

      * **Standalone (single-display, legacy)**: ``sway_manager`` is
        ``None``. This instance owns its own sway scope and chromium
        runs as an ``exec`` line inside that sway. Existing single-
        display callers stay on this path.

      * **External sway (multi-display)**: ``sway_manager`` is a
        :class:`player.sway_manager.SwayManager` that has already been
        started by the ``Coordinator``. This instance does NOT spawn
        sway; instead it launches its chromium subprocess directly as
        a transient systemd scope, attached to the running sway as a
        Wayland client via ``WAYLAND_DISPLAY`` / ``XDG_RUNTIME_DIR``.
        ``app_id`` is passed to chromium as ``--class=<app_id>`` so the
        sway config's ``for_window`` rules can pin the kiosk to its
        target HDMI output. ``stop()`` only tears down the chromium
        scope -- the shared sway stays up.
    """

    def __init__(
        self,
        assets_dir: Path,
        shell_dir: Path = SHELL_DIR,
        host: str = DEFAULT_BIND_HOST,
        port: int = DEFAULT_BIND_PORT,
        on_event: Optional[Callable[[dict], None]] = None,
        spawn_kiosk: bool = True,
        sway_manager: Optional["SwayManager"] = None,
        app_id: Optional[str] = None,
    ) -> None:
        self.assets_dir = Path(assets_dir)
        self.shell_dir = Path(shell_dir)
        self.host = host
        self.port = port
        self._on_event = on_event
        self._spawn_kiosk = spawn_kiosk
        self._sway_manager = sway_manager
        self._app_id = app_id

        self._server_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._server = None  # uvicorn.Server
        self._stop_event: Optional[asyncio.Event] = None
        self._ws_state = _WebSocketState()

        # Process handle: either the standalone sway scope (when this
        # instance owns sway) or the chromium-only scope (when external
        # sway is provided). ``_scope_unit`` always names whichever
        # scope is alive.
        self._sway_process: Optional[subprocess.Popen] = None
        self._sway_scope_unit: Optional[str] = None

    # ── Public lifecycle ──

    def start(self) -> None:
        """Start the shell server and spawn sway+chromium."""
        if self._server_thread and self._server_thread.is_alive():
            logger.debug("ChromiumPlayer.start: already running")
            return
        ready = threading.Event()
        self._server_thread = threading.Thread(
            target=self._run_server_thread,
            args=(ready,),
            name="agora-chromium-shell",
            daemon=True,
        )
        self._server_thread.start()
        # Wait up to 10s for uvicorn to bind. Don't block forever — the demo
        # would rather fail loudly than hang.
        if not ready.wait(timeout=10.0):
            logger.error("ChromiumPlayer: shell server failed to start within 10s")
            return
        logger.info(
            "ChromiumPlayer shell server listening on http://%s:%d",
            self.host, self.port,
        )
        if self._spawn_kiosk:
            if self._sway_manager is not None:
                self._start_chromium_client_scope()
            else:
                self._start_sway_kiosk()

    def stop(self) -> None:
        """Stop sway+chromium and the shell server.

        In external-sway mode, ``_stop_sway_kiosk`` stops *our* scope
        (the chromium-only one). The shared sway is owned by the
        ``SwayManager`` -- callers there are responsible for shutting
        it down independently.
        """
        self._stop_sway_kiosk()
        if self._loop and self._stop_event:
            try:
                self._loop.call_soon_threadsafe(self._stop_event.set)
            except RuntimeError:
                pass
        if self._server_thread:
            self._server_thread.join(timeout=5.0)
        self._server_thread = None
        self._loop = None
        self._stop_event = None
        self._server = None

    def is_alive(self) -> bool:
        """True iff both the shell server thread and the kiosk are alive."""
        server_ok = bool(self._server_thread and self._server_thread.is_alive())
        if not self._spawn_kiosk:
            return server_ok
        kiosk_ok = bool(
            self._sway_process and self._sway_process.poll() is None
        )
        return server_ok and kiosk_ok

    def shell_url(self) -> str:
        """The HTTP URL the kiosk should be pointed at."""
        return f"http://{self.host}:{self.port}/"

    def asset_url(self, path: Path) -> Optional[str]:
        """Return the ``/assets/<rel>`` URL the shell will load for ``path``.

        Returns ``None`` if ``path`` is not under ``assets_dir``. Callers
        use this to compute the expected URL string for matching against
        shell ``ended`` events.
        """
        return self._asset_url(path)

    # ── Command API ──

    def show_image(
        self,
        path: Path,
        transition: str = "cut",
        duration_ms: int = DEFAULT_TRANSITION_MS,
        fit: Optional[str] = None,
        effect: Optional[str] = None,
        effect_duration_ms: Optional[int] = None,
        effect_direction: Optional[str] = None,
    ) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_image: not under assets dir: %s", path)
            return
        payload: dict = {
            "cmd": "show_image",
            "url": url,
            "transition": transition,
            "duration_ms": duration_ms,
        }
        # Per-slide object-fit (cover|contain) from the slideshow builder.
        # Emit whenever provided — the shell's legacy default is
        # object-fit:contain, so even "contain" must be sent to be explicit
        # and "cover" must be sent to override. Omit when None so older
        # callers stay byte-for-byte backward-compatible.
        if fit:
            payload["fit"] = fit
        # Ken Burns slow-pan/zoom. Only emit a real effect (skip "none")
        # so the default render path is unchanged. effect_duration_ms is
        # the slide's on-screen dwell so the animation spans the slide;
        # the shell falls back to a sane default when it's absent/zero.
        if effect and effect != "none":
            payload["effect"] = effect
            if effect_duration_ms and int(effect_duration_ms) > 0:
                payload["effect_duration_ms"] = int(effect_duration_ms)
            # Ken Burns direction (manifest schema 1.4). "in" is the shell's
            # base keyframe, so only emit a real non-"in" direction — older
            # decks (and the absent case) stay byte-for-byte compatible and
            # the shell plays the default zoom-in.
            if effect_direction and effect_direction != "in":
                payload["effect_direction"] = effect_direction
        self._enqueue(payload)

    def show_video(
        self,
        path: Path,
        loop: bool = False,
        muted: bool = False,
        transition: str = "cut",
        duration_ms: int = DEFAULT_TRANSITION_MS,
        loop_count: Optional[int] = None,
        start_offset_ms: int = 0,
        fit: Optional[str] = None,
    ) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_video: not under assets dir: %s", path)
            return
        payload: dict = {
            "cmd": "show_video",
            "url": url,
            "loop": bool(loop),
            "muted": bool(muted),
            "transition": transition,
            "duration_ms": duration_ms,
        }
        # Per-slide object-fit (cover|contain). No Ken Burns on video.
        if fit:
            payload["fit"] = fit
        # loop_count drives finite-loop playback in player.js: the shell
        # counts down on each video.ended, replays in-place (no layer
        # swap, no fade hiccup) until the count is exhausted, then emits
        # a terminal {event:"ended"} so the daemon can transition out.
        # Only emit the field when the caller actually wants finite
        # looping — otherwise the protocol stays backward-compatible.
        if loop_count is not None and loop_count > 0:
            payload["loop_count"] = int(loop_count)
        # start_offset_ms drives wall-clock anchored seek-on-resume in
        # the shell: the slideshow engine computes how far into the
        # current video slide we should be (based on manifest
        # ``started_at`` + per-slide durations) and the shell sets
        # ``v.currentTime`` after loadedmetadata. Omit when zero so the
        # protocol stays backward-compatible with older shell builds.
        if start_offset_ms and start_offset_ms > 0:
            payload["start_offset_ms"] = int(start_offset_ms)
        self._enqueue(payload)

    def show_splash(self, path: Path) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_splash: not under assets dir: %s", path)
            return
        self._enqueue({
            "cmd": "show_splash",
            "url": url,
            "transition": "cut",
            "duration_ms": 0,
        })

    def show_html(
        self,
        path: Path,
        transition: str = "cut",
        duration_ms: int = DEFAULT_TRANSITION_MS,
    ) -> None:
        """Render a local HTML bundle (e.g. composed slide) in an iframe."""
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_html: not under assets dir: %s", path)
            return
        self._enqueue({
            "cmd": "show_html",
            "url": url,
            "transition": transition,
            "duration_ms": duration_ms,
        })

    def stop_playback(self) -> None:
        self._enqueue({"cmd": "stop"})

    # ── Internals ──

    def _asset_url(self, path: Path) -> Optional[str]:
        """Return ``/assets/<relpath>`` if ``path`` is inside ``assets_dir``."""
        path = Path(path)
        try:
            rel = path.resolve().relative_to(self.assets_dir.resolve())
        except (ValueError, OSError):
            return None
        return "/assets/" + rel.as_posix()

    def _enqueue(self, command: dict) -> None:
        """Push a command to the WS handler from any thread."""
        if not self._loop:
            logger.debug("ChromiumPlayer: dropping command before server ready: %s", command)
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._ws_state.publish(command), self._loop,
            )
        except RuntimeError:
            logger.debug("ChromiumPlayer: event loop closed; dropping %s", command)

    def _build_app(self):
        """Construct the FastAPI app with WS + static routes in the right order.

        Route registration order matters: the WebSocket route at /ws MUST be
        registered BEFORE the catch-all StaticFiles mount on "/", otherwise
        Starlette dispatches the WS upgrade into StaticFiles which then
        asserts scope["type"] == "http" and raises AssertionError on every
        connection attempt. Likewise /assets must be mounted before the "/"
        catch-all. Extracted as a method so tests can exercise routing
        without spinning up uvicorn.
        """
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import FileResponse
        from fastapi.staticfiles import StaticFiles

        app = FastAPI(title="Agora Player Shell")

        @app.get("/")
        async def root() -> FileResponse:
            return FileResponse(self.shell_dir / "index.html")

        @app.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket) -> None:
            await websocket.accept()
            await self._ws_state.attach(websocket)
            try:
                while True:
                    msg_text = await websocket.receive_text()
                    try:
                        payload = json.loads(msg_text)
                    except json.JSONDecodeError:
                        logger.debug("shell sent bad json: %s", msg_text)
                        continue
                    if self._on_event:
                        try:
                            self._on_event(payload)
                        except Exception:
                            logger.exception("shell on_event handler raised")
            except WebSocketDisconnect:
                pass
            except Exception:
                logger.exception("shell ws unexpected error")
            finally:
                await self._ws_state.detach(websocket)

        if self.assets_dir.is_dir():
            app.mount(
                "/assets", StaticFiles(directory=str(self.assets_dir)),
                name="assets",
            )

        if self.shell_dir.is_dir():
            app.mount(
                "/", StaticFiles(directory=str(self.shell_dir), html=True),
                name="shell",
            )

        return app

    def _run_server_thread(self, ready: threading.Event) -> None:
        """Thread target: build asyncio loop + uvicorn server + run."""
        # Lazy import uvicorn here, and fastapi inside _build_app, so the
        # rest of the player stays importable on environments without
        # those packages installed (e.g. unit tests that mock the backend).
        try:
            import uvicorn
            import fastapi  # noqa: F401  (probe so we fail before _build_app)
        except ImportError as e:
            logger.error("ChromiumPlayer: fastapi/uvicorn not available: %s", e)
            ready.set()
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._stop_event = asyncio.Event()
        # Bind the WS state to this loop now that one exists.
        self._ws_state.bind(loop)

        app = self._build_app()

        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
            loop="asyncio",
        )
        self._server = uvicorn.Server(config)

        async def _serve() -> None:
            # uvicorn.Server.serve() blocks until shutdown; install our
            # own stop signal so .stop() can break it cleanly.
            serve_task = asyncio.create_task(self._server.serve())
            # Wait until uvicorn has finished binding.
            for _ in range(100):
                if self._server.started:
                    break
                await asyncio.sleep(0.05)
            ready.set()
            await self._stop_event.wait()
            self._server.should_exit = True
            await serve_task

        try:
            loop.run_until_complete(_serve())
        except Exception:
            logger.exception("shell server crashed")
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
            ready.set()  # in case we crashed before binding

    # ── Sway kiosk subprocess ──

    def _chromium_argv(self, *, app_id: Optional[str] = None) -> list[str]:
        """Build the chromium command line used by both launch modes.

        ``--class=<app_id>`` is set when ``app_id`` is provided so the
        wayland xdg-shell ``app_id`` matches the ``for_window`` rules in
        the external sway's baked config. (Yes, ``--class`` on Wayland
        sets the app_id, despite the X11-era flag name.)
        """
        argv = [
            "chromium", "--no-sandbox", "--kiosk", "--noerrdialogs",
            # Force the wayland ozone backend; without these chromium
            # defaults to X11 and exits immediately on systems with no
            # X server (i.e. every agora device).
            "--ozone-platform=wayland",
            "--enable-features=UseOzonePlatform",
            "--disable-translate", "--disable-infobars", "--incognito",
            "--hide-scrollbars",
            "--autoplay-policy=no-user-gesture-required",
            "--load-extension=/opt/agora/src/player/extensions/hide-cursor",
        ]
        if app_id:
            argv.append(f"--class={app_id}")
        argv.append(self.shell_url())
        return argv

    def _write_shell_sway_config(self) -> Path:
        """Render a minimal sway config that execs chromium at the shell URL.

        Lives inside the same 0o700 runtime dir used for ``XDG_RUNTIME_DIR``,
        written with ``O_NOFOLLOW`` so a pre-existing symlink can't redirect
        the (URL-containing) config body.

        Only used in standalone (legacy) mode. In external-sway mode
        the ``SwayManager`` writes its own config; this method is never
        called.
        """
        chromium_cmd = self._chromium_argv(app_id=None)
        exec_line = "exec " + shlex.join(chromium_cmd)
        body = (
            "output * bg #000000 solid_color\n"
            "seat * hide_cursor 1\n"
            "default_border none\n"
            "default_floating_border none\n"
            "hide_edge_borders both\n"
            'for_window [shell=".*"] fullscreen enable, border none\n'
            "\n" + exec_line + "\n"
        )
        runtime_dir = Path(_SHELL_SWAY_RUNTIME_DIR)
        runtime_dir.mkdir(mode=0o700, exist_ok=True)
        try:
            os.chmod(runtime_dir, 0o700)
        except OSError:
            pass
        conf_path = Path(_SHELL_SWAY_CONFIG_PATH)
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

    def _start_sway_kiosk(self) -> None:
        """Launch sway + chromium in kiosk mode pointed at the shell URL.

        Mirrors ``AgoraPlayer._start_sway`` — wraps sway in a transient
        systemd scope so the cgroup catches chromium grandchildren that
        sway's ``exec`` double-forks + setsids out of any process group.
        """
        self._stop_sway_kiosk()

        # Plymouth holds DRM master across boot; sway will silently fail
        # to acquire /dev/dri/cardX (looking like a frozen splash) until
        # plymouth is told to release it. Idempotent across kiosk
        # restarts.
        _quit_plymouth()

        env = os.environ.copy()
        env["XDG_RUNTIME_DIR"] = _SHELL_SWAY_RUNTIME_DIR
        # _write_shell_sway_config ensures the runtime dir exists at 0o700.

        try:
            conf = self._write_shell_sway_config()
        except OSError as e:
            logger.error("ChromiumPlayer: could not write sway config: %s", e)
            return

        # Per-invocation unique scope unit name — a stable name would
        # collide if a previous _stop_sway_kiosk ever failed to fully
        # reap the scope, trapping retries with "unit already exists".
        self._sway_scope_unit = f"agora-sway-shell-{uuid.uuid4().hex[:8]}.scope"

        cmd = [
            "systemd-run", "--scope", "--quiet",
            "--unit", self._sway_scope_unit, "--collect",
            "sway", "-c", str(conf),
        ]

        logger.info(
            "ChromiumPlayer: launching sway+chromium kiosk → %s (scope=%s)",
            self.shell_url(), self._sway_scope_unit,
        )
        try:
            self._sway_process = subprocess.Popen(
                cmd, env=env,
                start_new_session=True,
                # stderr inherited (→ journal) so silent launch failures
                # surface in `journalctl -u agora-player`.
                stdout=subprocess.DEVNULL,
            )
        except (FileNotFoundError, OSError) as e:
            logger.error(
                "ChromiumPlayer: failed to launch sway+chromium: %s", e,
            )
            self._sway_process = None
            self._sway_scope_unit = None

    def _stop_sway_kiosk(self) -> None:
        """Stop the sway+chromium kiosk scope if running.

        Tears down via ``systemctl stop`` on the transient scope so the
        cgroup signals every descendant — including double-forked
        chromium grandchildren that pgrp-based kills can't see.

        Works for both standalone and external-sway modes: the scope
        contains different processes (sway+chromium vs chromium-only)
        but the scope-teardown logic is identical.
        """
        proc = self._sway_process
        unit = self._sway_scope_unit
        if proc and proc.poll() is None and unit:
            logger.info(
                "ChromiumPlayer: stopping kiosk scope=%s pid=%d",
                unit, proc.pid,
            )
            subprocess.run(
                ["systemctl", "stop", unit],
                timeout=8, check=False,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Cgroup KILL fallback — reaches double-forked grandchildren
                # that pgrp-based kills can't see.
                subprocess.run(
                    ["systemctl", "kill", "-s", "SIGKILL", unit],
                    timeout=5, check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        self._sway_process = None
        self._sway_scope_unit = None

    def _start_chromium_client_scope(self) -> None:
        """Launch chromium as a wayland client of the external SwayManager.

        Used only when ``sway_manager`` is provided (multi-display
        mode). Skips sway entirely -- the chromium subprocess connects
        to the running sway via ``WAYLAND_DISPLAY`` /
        ``XDG_RUNTIME_DIR`` from :meth:`SwayManager.env_for_client`.

        Sets ``--class=<app_id>`` on chromium so the sway config's
        ``for_window`` rules can pin the kiosk window to the correct
        HDMI output. Wrapped in a transient systemd scope (same
        rationale as standalone mode) so cgroup teardown reaches
        chromium's double-forked grandchildren.
        """
        # Idempotency: if a previous client scope is still alive,
        # tear it down first so we never have two chromiums fighting
        # for the same shell URL.
        self._stop_sway_kiosk()

        assert self._sway_manager is not None  # checked by caller
        env = os.environ.copy()
        env.update(self._sway_manager.env_for_client())

        self._sway_scope_unit = (
            f"agora-shell-{(self._app_id or 'kiosk').replace('agora-shell-', '')}"
            f"-{uuid.uuid4().hex[:8]}.scope"
        )

        argv = self._chromium_argv(app_id=self._app_id)
        cmd = [
            "systemd-run", "--scope", "--quiet",
            "--unit", self._sway_scope_unit, "--collect",
            *argv,
        ]
        logger.info(
            "ChromiumPlayer: launching chromium client kiosk → %s "
            "(app_id=%s, scope=%s)",
            self.shell_url(), self._app_id, self._sway_scope_unit,
        )
        try:
            self._sway_process = subprocess.Popen(
                cmd, env=env,
                start_new_session=True,
                # stderr inherited (→ journal) so silent launch failures
                # surface in `journalctl -u agora-player`.
                stdout=subprocess.DEVNULL,
            )
        except (FileNotFoundError, OSError) as e:
            logger.error(
                "ChromiumPlayer: failed to launch chromium client: %s", e,
            )
            self._sway_process = None
            self._sway_scope_unit = None


class _WebSocketState:
    """Tracks at most one connected shell WebSocket plus a small command buffer.

    All public methods are coroutines and must be awaited on the same
    asyncio loop the state was ``bind``-ed to. ``publish`` is the only entry
    point used from outside the loop (via ``run_coroutine_threadsafe``).
    """

    _MAX_BUFFER = 4

    def __init__(self) -> None:
        self._ws = None  # current WebSocket
        self._buffer: deque[dict] = deque(maxlen=self._MAX_BUFFER)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def attach(self, ws) -> None:
        # If we already had a socket, drop the old. Newest wins.
        if self._ws is not None and self._ws is not ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = ws
        # Flush any commands that were sent while the shell wasn't connected.
        # Keep only the latest show_* command — older ones are stale.
        flushed = self._coalesce_buffer()
        for cmd in flushed:
            try:
                await ws.send_text(json.dumps(cmd))
            except Exception:
                logger.exception("ChromiumPlayer: failed sending buffered cmd")
                break

    async def detach(self, ws) -> None:
        if self._ws is ws:
            self._ws = None

    async def publish(self, command: dict) -> None:
        ws = self._ws
        if ws is None:
            self._buffer.append(command)
            return
        try:
            await ws.send_text(json.dumps(command))
        except Exception:
            logger.exception("ChromiumPlayer: ws send failed; buffering")
            self._buffer.append(command)
            self._ws = None

    def _coalesce_buffer(self) -> list[dict]:
        """Squash buffered commands down to "latest show_* + a stop if newer"."""
        if not self._buffer:
            return []
        items = list(self._buffer)
        self._buffer.clear()
        # Keep only the latest show_* and any stop that was issued after it.
        latest_show = None
        latest_stop = None
        for i, cmd in enumerate(items):
            if cmd.get("cmd") in ("show_image", "show_video", "show_splash"):
                latest_show = (i, cmd)
            elif cmd.get("cmd") == "stop":
                latest_stop = (i, cmd)
        result: list[dict] = []
        if latest_show is not None:
            result.append(latest_show[1])
        if latest_stop is not None and (
            latest_show is None or latest_stop[0] > latest_show[0]
        ):
            result.append(latest_stop[1])
        return result
