"""
Chromium-based playback backend (demo).

A small FastAPI/uvicorn server bound to 127.0.0.1 serves a static "player
shell" SPA and the device's asset directory, plus a control WebSocket.
A single cage+chromium subprocess is launched in kiosk mode pointing at
that server, and the daemon drives the shell over the WebSocket with
JSON commands.

This is an opt-in alternative to mpv, intended to make rich image
transitions and unified image/video rendering easy to iterate on.
Enable by setting environment variable AGORA_PLAYER_BACKEND=chromium.

The protocol is documented in ``player/shell/README.md``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import subprocess
import threading
from collections import deque
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("agora.player.chromium")

DEFAULT_BIND_HOST = "127.0.0.1"
DEFAULT_BIND_PORT = 8780
DEFAULT_TRANSITION_MS = 600

SHELL_DIR = Path(__file__).resolve().parent / "shell"


class ChromiumPlayer:
    """Manage a persistent cage+chromium kiosk + shell control channel.

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
        * The cage+chromium subprocess is unmanaged by the asyncio loop —
          it's just a child process polled the same way ``_sway_process``
          is in ``service.py``.
    """

    def __init__(
        self,
        assets_dir: Path,
        shell_dir: Path = SHELL_DIR,
        host: str = DEFAULT_BIND_HOST,
        port: int = DEFAULT_BIND_PORT,
        on_event: Optional[Callable[[dict], None]] = None,
        spawn_chromium: bool = True,
    ) -> None:
        self.assets_dir = Path(assets_dir)
        self.shell_dir = Path(shell_dir)
        self.host = host
        self.port = port
        self._on_event = on_event
        self._spawn_chromium = spawn_chromium

        self._server_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._server = None  # uvicorn.Server
        self._stop_event: Optional[asyncio.Event] = None
        self._ws_state = _WebSocketState()

        self._chromium_proc: Optional[subprocess.Popen] = None

    # ── Public lifecycle ──

    def start(self) -> None:
        """Start the shell server and spawn cage+chromium."""
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
        if self._spawn_chromium:
            self._start_chromium()

    def stop(self) -> None:
        """Stop cage+chromium and the shell server."""
        self._stop_chromium()
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
        """True iff both the shell server thread and chromium are alive."""
        server_ok = bool(self._server_thread and self._server_thread.is_alive())
        if not self._spawn_chromium:
            return server_ok
        chromium_ok = bool(
            self._chromium_proc and self._chromium_proc.poll() is None
        )
        return server_ok and chromium_ok

    # ── Command API ──

    def show_image(
        self,
        path: Path,
        transition: str = "fade",
        duration_ms: int = DEFAULT_TRANSITION_MS,
    ) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_image: not under assets dir: %s", path)
            return
        self._enqueue({
            "cmd": "show_image",
            "url": url,
            "transition": transition,
            "duration_ms": duration_ms,
        })

    def show_video(
        self,
        path: Path,
        loop: bool = False,
        muted: bool = False,
        transition: str = "fade",
        duration_ms: int = DEFAULT_TRANSITION_MS,
    ) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_video: not under assets dir: %s", path)
            return
        self._enqueue({
            "cmd": "show_video",
            "url": url,
            "loop": bool(loop),
            "muted": bool(muted),
            "transition": transition,
            "duration_ms": duration_ms,
        })

    def show_splash(self, path: Path) -> None:
        url = self._asset_url(path)
        if url is None:
            logger.warning("ChromiumPlayer.show_splash: not under assets dir: %s", path)
            return
        self._enqueue({
            "cmd": "show_splash",
            "url": url,
            "transition": "none",
            "duration_ms": 0,
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

    def _run_server_thread(self, ready: threading.Event) -> None:
        """Thread target: build asyncio loop + uvicorn server + run."""
        # Lazy import: keeps the rest of the player importable on
        # environments without fastapi (e.g. unit tests that don't need
        # the chromium backend).
        try:
            import uvicorn
            from fastapi import FastAPI, WebSocket, WebSocketDisconnect
            from fastapi.responses import FileResponse
            from fastapi.staticfiles import StaticFiles
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

        app = FastAPI(title="Agora Player Shell")

        @app.get("/")
        async def root() -> FileResponse:
            return FileResponse(self.shell_dir / "index.html")

        # Shell static (player.js, player.css, etc.) served at /shell/.
        # / itself is the bootstrap above so the relative URLs in index.html
        # resolve naturally.
        if self.shell_dir.is_dir():
            app.mount(
                "/", StaticFiles(directory=str(self.shell_dir), html=True),
                name="shell",
            )

        # Asset library at /assets.
        if self.assets_dir.is_dir():
            app.mount(
                "/assets", StaticFiles(directory=str(self.assets_dir)),
                name="assets",
            )

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

    # ── Chromium subprocess ──

    def _start_chromium(self) -> None:
        """Launch cage + chromium in kiosk mode pointed at the shell."""
        env = os.environ.copy()
        env["XDG_RUNTIME_DIR"] = "/tmp/cage-run"
        try:
            os.makedirs("/tmp/cage-run", exist_ok=True)
        except OSError as e:
            logger.warning("ChromiumPlayer: could not create cage runtime dir: %s", e)

        url = f"http://{self.host}:{self.port}/"
        cmd = [
            "cage", "-d", "--",
            "chromium", "--no-sandbox", "--kiosk", "--noerrdialogs",
            "--disable-translate", "--disable-infobars", "--incognito",
            "--hide-scrollbars",
            "--autoplay-policy=no-user-gesture-required",
            # Loading a local URL → no need to fight a low-mem prompt or
            # disable site isolation. Pi 5 has enough RAM for defaults.
            "--load-extension=/opt/agora/src/player/extensions/hide-cursor",
            url,
        ]

        logger.info("ChromiumPlayer: launching kiosk → %s", url)
        try:
            self._chromium_proc = subprocess.Popen(
                cmd, env=env,
                start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except (FileNotFoundError, OSError) as e:
            logger.error("ChromiumPlayer: failed to launch cage+chromium: %s", e)

    def _stop_chromium(self) -> None:
        proc = self._chromium_proc
        if proc and proc.poll() is None:
            logger.info("ChromiumPlayer: stopping kiosk (PID %d)", proc.pid)
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        self._chromium_proc = None


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
