"""Agora Player Service — watches desired state and manages media playback."""

import itertools
import json
import logging
import os
import queue
import signal
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst  # noqa: E402

from shared.board import Board, alsa_device_string, alsa_device_string_gst, get_board, player_backend, supported_codecs  # noqa: E402
from hardware.display import PortStatus, get_display_probe  # noqa: E402
from shared.models import CurrentState, DesiredState, PlaybackMode  # noqa: E402
from shared.state import read_state, write_state  # noqa: E402

logger = logging.getLogger("agora.player")


def _build_video_pipeline_str(board: Board) -> str:
    """Return the GStreamer pipeline string for video playback with audio.

    Used only on boards with player_backend='gstreamer' (Zero 2 W).
    """
    codecs = supported_codecs()
    if "hevc" in codecs:
        decode = "h265parse ! v4l2h265dec"
    else:
        decode = "h264parse ! v4l2h264dec"

    _gst_dev = alsa_device_string_gst()
    return (
        'filesrc location="{path}" ! '
        "qtdemux name=dmux "
        f"dmux.video_0 ! queue ! {decode} ! kmssink driver-name=vc4 sync=true "
        "dmux.audio_0 ! queue ! decodebin ! audioconvert ! audioresample ! "
        f'alsasink device="{_gst_dev}"'
    )


def _build_video_pipeline_no_audio_str(board: Board) -> str:
    """Return the GStreamer pipeline string for video playback without audio.

    Used only on boards with player_backend='gstreamer' (Zero 2 W).
    """
    codecs = supported_codecs()
    if "hevc" in codecs:
        decode = "h265parse ! v4l2h265dec"
    else:
        decode = "h264parse ! v4l2h264dec"

    return (
        'filesrc location="{path}" ! '
        "qtdemux name=dmux "
        f"dmux.video_0 ! queue ! {decode} ! kmssink driver-name=vc4 sync=false"
    )


MPV_IPC_SOCKET = "/tmp/mpv-socket"


def _build_mpv_command(path: Path, *, muted: bool = True, loop: bool = False) -> list[str]:
    """Build the mpv command for media playback via DRM output.

    Used on Pi 4 and Pi 5 for both video and image playback.
    For video: uses drm-copy hwdec for hardware decoding.
    For images: uses image-display-duration=inf to hold the frame.
    Includes IPC socket for seamless content switching via loadfile.

    The ALSA HDMI audio device is always bound at launch so that later
    IPC ``loadfile`` swaps can carry audio (mpv cannot add an audio
    output after the process is running). Mute state is runtime-toggleable
    via ``set_property mute …`` — splash is muted, scheduled assets are
    unmuted.
    """
    is_image = path.suffix.lower() in (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp")
    cmd = [
        "mpv",
        "--vo=drm",
        "--drm-connector=HDMI-A-1",
        "--fullscreen",
        "--no-terminal",
        "--no-input-terminal",
        "--no-osc",
        f"--input-ipc-server={MPV_IPC_SOCKET}",
        "--ao=alsa",
        f"--audio-device={alsa_device_string()}",
    ]
    if is_image:
        cmd.append("--image-display-duration=inf")
    else:
        cmd.append("--hwdec=drm-copy")
    if muted:
        cmd.append("--mute=yes")
    if loop:
        cmd.append("--loop=inf")
    cmd.append(str(path))
    return cmd


def _build_stream_command(url: str) -> list[str]:
    """Build the mpv command for streaming video playback (HLS, DASH, RTMP, etc.).

    mpv handles adaptive bitrate (ABR) streaming natively via FFmpeg's
    demuxers. For live streams we disable looping; for VOD streams the
    caller can pass --loop=inf via IPC later if needed.
    """
    cmd = [
        "mpv",
        "--vo=drm",
        "--drm-connector=HDMI-A-1",
        "--fullscreen",
        "--no-terminal",
        "--no-input-terminal",
        "--no-osc",
        f"--input-ipc-server={MPV_IPC_SOCKET}",
        "--hwdec=drm-copy",
        # Stream-specific cache settings for smooth playback
        "--cache=yes",
        "--demuxer-max-bytes=50MiB",
        "--demuxer-max-back-bytes=25MiB",
        # Loop for VOD streams (harmless for live — they don't end)
        "--loop=inf",
        "--ao=alsa",
        f"--audio-device={alsa_device_string()}",
        url,
    ]
    return cmd


class AgoraPlayer:
    """Manages media playback driven by desired state file changes.

    Uses GStreamer pipelines on Zero 2 W (and for images/splash on all boards),
    and mpv subprocess on Pi 4/Pi 5 for video playback with hardware decoding.
    """

    # Class-level default so tests that bypass __init__ still see this
    # attribute as None (matches "not in a slideshow").
    _slideshow: Optional[dict] = None

    IMAGE_PIPELINE_JPEG = (
        'filesrc location="{path}" ! '
        "jpegparse ! jpegdec ! videoconvert ! videoscale add-borders=true ! "
        "video/x-raw,width=1920,height=1080,pixel-aspect-ratio=1/1 ! "
        "imagefreeze ! kmssink driver-name=vc4 sync=false"
    )

    IMAGE_PIPELINE_OTHER = (
        'filesrc location="{path}" ! '
        "decodebin ! videoconvert ! videoscale add-borders=true ! "
        "video/x-raw,width=1920,height=1080,pixel-aspect-ratio=1/1 ! "
        "imagefreeze ! kmssink driver-name=vc4 sync=false"
    )

    DEFAULT_SPLASH_CONFIG = "splash/default.png"

    # Class-level default so bypass-init test fixtures see a sane value
    # before _start_mpv increments it. Real instances overwrite via
    # __init__ for clarity, but the int is immutable so the class default
    # poses no shared-state hazard.
    _mpv_generation: int = 0

    def __init__(self, base_path: str = "/opt/agora"):
        self.base = Path(base_path)
        self.state_dir = self.base / "state"
        self.persist_dir = self.base / "persist"
        self.assets_dir = self.base / "assets"
        self.desired_path = self.state_dir / "desired.json"
        self.current_path = self.state_dir / "current.json"
        self.splash_config_path = self.persist_dir / "splash"

        self._board = get_board()
        self._display_probe = get_display_probe()
        self._player_backend = player_backend()

        self.pipeline: Optional[Gst.Pipeline] = None
        self._mpv_process: Optional[subprocess.Popen] = None
        self._cage_process: Optional[subprocess.Popen] = None
        self.loop = GLib.MainLoop()
        self.current_desired: Optional[DesiredState] = None
        self._current_path: Optional[Path] = None  # file being played
        self._current_mtime: Optional[float] = None  # mtime when pipeline was built
        self._loops_completed: int = 0
        self._health_retries: int = 0
        self._error_retry_delay: int = 3
        self._pending_error: Optional[str] = None
        self._plymouth_quit: bool = False
        self._running = True
        # Debounce state for display port connection changes (issue #178).
        # Maps port name -> (candidate_new_value, consecutive_count).
        # Only flips between True<->False require confirmation; transitions
        # involving None (indeterminate) commit immediately.
        self._display_pending: dict[str, tuple[Optional[bool], int]] = {}

        # Slideshow sequencer state. None when not playing a slideshow.
        # Populated by _start_slideshow; cleared by _clear_slideshow.
        self._slideshow: Optional[dict] = None

        # ── mpv IPC event listener (Phase 1) ──
        #
        # A persistent background thread connects to ``MPV_IPC_SOCKET`` and
        # reads events broadcast by mpv (start-file, end-file, etc.) onto
        # ``_mpv_event_queue``. Events are drained on the GLib main loop
        # via ``GLib.idle_add(self._drain_mpv_events)`` so all dispatch
        # happens single-threaded.
        #
        # ``_mpv_generation`` is bumped every time a fresh mpv subprocess is
        # spawned. Each event is stamped with the generation that was
        # current when the listener was connected, so consumers (slideshow,
        # loop_count) can ignore stale events from a previous mpv instance.
        self._mpv_event_thread: Optional[threading.Thread] = None
        self._mpv_event_stop = threading.Event()
        self._mpv_event_queue: "queue.Queue[dict]" = queue.Queue()
        self._mpv_event_connected = threading.Event()
        self._mpv_generation: int = 0
        self._mpv_drain_lock = threading.Lock()
        self._mpv_drain_pending: bool = False

        Gst.init(None)

    # ── Asset resolution ──

    def _resolve_asset(self, name: str) -> Optional[Path]:
        for subdir in ["videos", "images", "splash"]:
            path = self.assets_dir / subdir / name
            if path.is_file():
                return path
        return None

    # ── Slideshow sequencer ──

    def _read_slideshow_manifest(self, name: str) -> Optional[dict]:
        """Read and validate a slideshow manifest from the assets dir.

        Returns the parsed dict or None if missing/invalid.
        """
        path = self.assets_dir / "slideshows" / f"{name}.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.error("Slideshow manifest %s unreadable: %s", path, e)
            return None
        if not isinstance(data, dict):
            return None
        slides = data.get("slides")
        if not isinstance(slides, list) or not slides:
            return None
        return data

    def _cancel_slide_timeout(self) -> None:
        """Cancel any pending GLib slide-advance timeout."""
        ss = self._slideshow
        if ss and ss.get("timeout_id"):
            try:
                GLib.source_remove(ss["timeout_id"])
            except Exception:
                pass
            ss["timeout_id"] = None

    def _clear_slideshow(self) -> None:
        """Tear down slideshow state (cancel timeout, drop manifest)."""
        self._cancel_slide_timeout()
        self._slideshow = None

    def _start_slideshow(self, name: str, loop_count: Optional[int]) -> None:
        """Begin sequencing slides from the named slideshow manifest."""
        manifest = self._read_slideshow_manifest(name)
        if not manifest:
            logger.error("Slideshow not playable: %s — showing splash", name)
            self._update_current(error=f"Slideshow not found: {name}")
            self._show_splash()
            return
        self._cancel_slide_timeout()
        slides = manifest["slides"]
        self._slideshow = {
            "name": name,
            "slides": slides,
            "index": 0,
            "loops_completed": 0,
            "loop_count": loop_count,
            "timeout_id": None,
        }
        self._loops_completed = 0
        logger.info(
            "Slideshow start: name=%s slides=%d loop_count=%s",
            name, len(slides), loop_count,
        )
        self._play_next_slide()

    def _play_next_slide(self) -> bool:
        """Advance to the next slide in the active slideshow.

        Loops back to the first slide when end is reached, honouring the
        slideshow-level loop_count.  Returns False so it can also be used
        as a one-shot GLib timeout callback.
        """
        ss = self._slideshow
        if not ss:
            return False

        if ss["index"] >= len(ss["slides"]):
            ss["loops_completed"] += 1
            self._loops_completed = ss["loops_completed"]
            target = ss.get("loop_count")
            if target is not None and ss["loops_completed"] >= target:
                logger.info(
                    "Slideshow %s: completed %d/%d loops, → splash",
                    ss["name"], ss["loops_completed"], target,
                )
                self._clear_slideshow()
                self._show_splash()
                return False
            ss["index"] = 0  # next loop

        slide = ss["slides"][ss["index"]]
        ss["index"] += 1
        slide_name = slide.get("name") or ""
        path = self._resolve_asset(slide_name)
        if not path:
            logger.error(
                "Slideshow %s: slide %d (%s) missing on disk — skipping",
                ss["name"], ss["index"] - 1, slide_name,
            )
            return self._play_next_slide()

        self._cancel_slide_timeout()
        is_video_slide = (slide.get("asset_type") == "video")
        play_to_end = bool(slide.get("play_to_end")) and is_video_slide

        logger.info(
            "Slideshow %s: slide %d/%d %s (play_to_end=%s)",
            ss["name"], ss["index"], len(ss["slides"]),
            slide_name, play_to_end,
        )

        if play_to_end:
            # Video, run to end. We need a fresh mpv subprocess (not IPC
            # loadfile) so the process actually exits on EOF and
            # _monitor_mpv detects it to advance to the next slide.
            self._stop_mpv()
            self._start_mpv(path, loop=False)
        else:
            # Image, or video with fixed duration: load via IPC if possible
            # then schedule a timed advance.  Loop the source so static
            # images/short videos don't black out before the timeout fires.
            if not self._loadfile_mpv(path, loop=True, muted=False):
                self._start_mpv(path, loop=True)
            duration_ms = int(slide.get("duration_ms") or 0)
            if duration_ms <= 0:
                duration_ms = 10000  # safe default
            ss["timeout_id"] = GLib.timeout_add(
                duration_ms, self._on_slide_timeout,
            )
            self._update_current(
                mode=PlaybackMode.PLAY,
                asset=ss["name"],
                started_at=datetime.now(timezone.utc),
            )
        return False

    def _on_slide_timeout(self) -> bool:
        """GLib timeout callback for image slide expiry."""
        ss = self._slideshow
        if ss:
            ss["timeout_id"] = None
        self._play_next_slide()
        return False  # one-shot

    def _find_splash(self) -> Optional[Path]:
        # 1. Check user-configured splash in state/splash
        if self.splash_config_path.is_file():
            name = self.splash_config_path.read_text().strip()
            if name:
                path = self._resolve_asset(name)
                if path:
                    return path
                logger.warning("Configured splash '%s' not found, using default", name)

        # 2. Fall back to default_splash from boot config
        default = self.DEFAULT_SPLASH_CONFIG
        boot_config = Path("/boot/agora-config.json")
        if boot_config.is_file():
            try:
                cfg = json.loads(boot_config.read_text())
                default = cfg.get("default_splash", default)
            except (json.JSONDecodeError, OSError):
                pass

        path = self.assets_dir / default
        if path.is_file():
            return path

        logger.warning("No splash asset found")
        return None

    # ── Pipeline management ──

    @staticmethod
    def _has_audio(path: Path) -> bool:
        """Return True if the video file contains an audio stream.

        Uses qtdemux to inspect container pads instead of GstPbutils Discoverer,
        which allocates a v4l2 hardware decoder and exhausts the single decoder
        slot on Pi Zero 2W, causing 'Failed to allocate required memory' errors.
        """
        import time

        try:
            pipe = Gst.parse_launch(
                f'filesrc location="{path}" ! qtdemux name=dmux'
            )
            dmux = pipe.get_by_name("dmux")

            found_audio = [False]
            no_more = [False]

            def on_pad_added(_element, pad):
                if "audio" in pad.get_name():
                    found_audio[0] = True

            def on_no_more_pads(_element):
                no_more[0] = True

            dmux.connect("pad-added", on_pad_added)
            dmux.connect("no-more-pads", on_no_more_pads)

            pipe.set_state(Gst.State.PAUSED)

            start = time.monotonic()
            ctx = GLib.MainContext.default()
            while not no_more[0] and (time.monotonic() - start) < 3:
                ctx.iteration(False)
                time.sleep(0.01)

            pipe.set_state(Gst.State.NULL)
            return found_audio[0]
        except Exception:
            logger.warning("Audio detection failed, assuming audio present")
            return True

    def _teardown(self) -> None:
        self._stop_mpv_event_listener()
        self._stop_mpv()
        self._stop_cage()
        if self.pipeline:
            bus = self.pipeline.get_bus()
            if bus:
                bus.remove_signal_watch()
            self.pipeline.set_state(Gst.State.NULL)
            # Wait for NULL state to complete so hardware resources (V4L2
            # decoder, KMS/DRM plane, ALSA) are fully released before
            # building a new pipeline.
            self.pipeline.get_state(Gst.CLOCK_TIME_NONE)
            self.pipeline = None
        self._current_path = None
        self._current_mtime = None

    # ── Cage+Chromium (webpage rendering) ──

    # Boards where aggressive Chromium memory-saver flags are applied.
    # These boards either have ≤1 GB RAM (Zero 2 W) or are memory-constrained
    # variants in the supported line (Pi 4 — the 1 GB model is still in use).
    # Pi 5 is excluded: it has ≥4 GB and hardware-accelerated compositing.
    # UNKNOWN is included defensively (same rationale as gstreamer fallback).
    _LOWMEM_BOARDS = frozenset({Board.ZERO_2W, Board.PI_4, Board.UNKNOWN})

    @staticmethod
    def _chromium_lowmem_flags() -> list[str]:
        """Chromium flags that reduce RAM footprint on memory-constrained boards.

        Empirically validated on a Pi Zero 2 W (416 MB usable RAM) rendering
        wikipedia, google, bbc and similar pages without swap thrash. Full
        rationale and test results are in the PR that introduced this helper.
        """
        return [
            # Bypass the Raspberry Pi OS chromium wrapper's low-RAM zenity
            # dialog that blocks launch on boards with MemTotal ≤ 512 MB.
            "--no-memcheck",
            # Collapse all same-site frames into one renderer and cap it at 1.
            "--disable-features=site-per-process,IsolateOrigins,SpareRendererForSitePerProcess",
            "--process-per-site",
            "--renderer-process-limit=1",
            # Skip disk cache writes (slow SD, small wins, bloats dirty pages).
            "--disk-cache-size=1", "--media-cache-size=1",
            # Don't proactively free memory on pressure events; we're always
            # under pressure, and the discards cause visible redraw churn.
            "--memory-pressure-off",
            # Chromium auto-disables GPU rasterization below 512 MB anyway,
            # but explicit flags also free upfront EGL/GBM surface allocations
            # and skip the GPU process, which measurably reduces RSS.
            "--disable-gpu", "--disable-gpu-compositing", "--disable-accelerated-2d-canvas",
            # Background chatter that's pointless for signage.
            "--disable-background-networking", "--disable-sync", "--disable-default-apps",
            "--disable-component-update", "--disable-domain-reliability",
            # Cap V8 heap so JS-heavy sites don't eat all RAM before GC.
            "--js-flags=--max-old-space-size=96 --max-semi-space-size=2",
        ]

    def _start_cage(self, url: str) -> None:
        """Launch Cage + Chromium in kiosk mode to render a URL."""
        self._stop_cage()
        self._teardown()  # Stop any mpv/gstreamer pipeline

        env = os.environ.copy()
        env["XDG_RUNTIME_DIR"] = "/tmp/cage-run"
        os.makedirs("/tmp/cage-run", exist_ok=True)

        cmd = [
            "cage", "-d", "--",
            "chromium", "--no-sandbox", "--kiosk", "--noerrdialogs",
            "--disable-translate", "--disable-infobars", "--incognito",
            "--hide-scrollbars", "--autoplay-policy=no-user-gesture-required",
        ]
        if get_board() in self._LOWMEM_BOARDS:
            cmd.extend(self._chromium_lowmem_flags())
        cmd.extend([
            "--load-extension=/opt/agora/src/player/extensions/hide-cursor",
            url,
        ])

        logger.info("Starting Cage+Chromium for URL: %s", url)
        try:
            self._cage_process = subprocess.Popen(
                cmd, env=env,
                start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self._current_path = None
            self._current_mtime = None
            self._update_current(mode=PlaybackMode.PLAY, asset=url)
            # Schedule periodic monitoring to detect Cage/Chromium crashes
            GLib.timeout_add_seconds(3, self._monitor_cage, url)
        except Exception as e:
            logger.error("Failed to start Cage+Chromium: %s", e)
            self._update_current(error=f"Cage startup failed: {e}")
            self._show_splash()

    def _stop_cage(self) -> None:
        """Stop Cage+Chromium process if running."""
        proc = self._cage_process
        if proc and proc.poll() is None:
            logger.info("Stopping Cage+Chromium (PID %d)", proc.pid)
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
        self._cage_process = None

    def _monitor_cage(self, url: str) -> bool:
        """Periodic check for Cage process exit. Returns False to stop timer."""
        if self._cage_process is None:
            return False

        # Stop monitoring if desired state no longer wants this URL
        if (
            not self.current_desired
            or self.current_desired.url != url
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False

        retcode = self._cage_process.poll()
        if retcode is None:
            # Still running
            return True

        # Cage exited unexpectedly
        self._cage_process = None
        error_msg = f"Cage exited unexpectedly (code {retcode})"
        logger.error("Cage crashed for %s: %s", url, error_msg)
        self._update_current(error=error_msg)

        # Retry with exponential backoff (same pattern as mpv)
        delay = self._error_retry_delay
        self._error_retry_delay = min(
            self._error_retry_delay * 2, self._RETRY_DELAY_MAX,
        )
        self._pending_error = error_msg
        self._show_splash()
        GLib.timeout_add_seconds(delay, self._retry_desired)
        return False

    def _build_pipeline(self, path: Path, is_video: bool) -> Gst.Pipeline:
        # Quit Plymouth before GStreamer claims the DRM device
        self._quit_plymouth()

        if is_video:
            if self._has_audio(path):
                pipeline_str = _build_video_pipeline_str(self._board).format(path=path)
            else:
                logger.info("No audio track detected, using video-only pipeline")
                pipeline_str = _build_video_pipeline_no_audio_str(self._board).format(path=path)
        elif path.suffix.lower() in (".jpg", ".jpeg"):
            pipeline_str = self.IMAGE_PIPELINE_JPEG.format(path=path)
        else:
            pipeline_str = self.IMAGE_PIPELINE_OTHER.format(path=path)

        logger.info("Building pipeline: %s", pipeline_str)
        pipeline = Gst.parse_launch(pipeline_str)

        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message::eos", self._on_eos)
        bus.connect("message::error", self._on_error)
        bus.connect("message::state-changed", self._on_state_changed)

        return pipeline

    # ── mpv subprocess management ──

    _IMAGE_EXTS = frozenset((".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"))

    # Per-command IPC timeout (seconds). mpv responds promptly under
    # normal conditions; we'd rather log + fail than block forever.
    _IPC_CMD_TIMEOUT_S = 1.5

    def _ipc_call(self, sock, recv_buf: bytes, command: list, *,
                  timeout_s: Optional[float] = None):
        """Send a JSON IPC command stamped with a request_id and wait for the
        matching response. Demultiplexes events that interleave on the same
        socket (mpv broadcasts events to every connected client).

        Returns ``(success: bool, data, new_recv_buf: bytes)``. On success
        the response had ``error == "success"``; on parse / timeout / socket
        error returns ``(False, None, recv_buf)``.

        ``recv_buf`` carries any bytes already read but not yet parsed across
        successive calls on the same socket so partial JSON lines are not
        lost.
        """
        req_id = next(self._mpv_ipc_counter)
        msg = json.dumps({"command": command, "request_id": req_id}).encode() + b"\n"
        if timeout_s is None:
            timeout_s = self._IPC_CMD_TIMEOUT_S
        try:
            sock.sendall(msg)
        except OSError:
            return False, None, recv_buf

        deadline = time.monotonic() + timeout_s
        while True:
            # Drain any complete lines we already have buffered
            while b"\n" in recv_buf:
                line, recv_buf = recv_buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    resp = json.loads(line.decode())
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if "event" in resp:
                    # mpv broadcasts events on every IPC client; drop
                    continue
                if resp.get("request_id") == req_id:
                    return resp.get("error") == "success", resp.get("data"), recv_buf
                # response for a different request_id — drop and keep looking

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, None, recv_buf
            try:
                sock.settimeout(min(remaining, 0.5))
                chunk = sock.recv(4096)
            except OSError:
                # socket.timeout is an OSError subclass since Python 3.10
                return False, None, recv_buf
            if not chunk:
                return False, None, recv_buf
            recv_buf += chunk

    def _loadfile_mpv(self, path: Path, *, loop: bool = False, muted: bool = True) -> bool:
        """Switch content in a running mpv via IPC socket. Returns True on success.

        ``muted`` controls the runtime ``mute`` property after the file is
        loaded. Splash calls pass ``muted=True``; scheduled-asset calls pass
        ``muted=False``. Because the underlying mpv process always launches
        with ``--ao=alsa --audio-device=…`` bound (see ``_build_mpv_command``),
        toggling mute via IPC is sufficient — no respawn needed.

        Each command is correlated by ``request_id`` so events that interleave
        on the same socket (mpv broadcasts events to every connected client)
        cannot be mistaken for the response.
        """
        if self._mpv_process is None or self._mpv_process.poll() is not None:
            return False
        is_image = path.suffix.lower() in self._IMAGE_EXTS

        # Fresh request_id sequence per IPC session — responses on this
        # socket are single-shot, no need for global uniqueness.
        self._mpv_ipc_counter = itertools.count()

        sock = None
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect(MPV_IPC_SOCKET)
            recv_buf = b""

            loop_val = "inf" if loop else "no"
            ok, _, recv_buf = self._ipc_call(sock, recv_buf, ["set_property", "loop-file", loop_val])
            if not ok:
                logger.warning("mpv IPC: set loop-file failed")
                return False

            ok, _, recv_buf = self._ipc_call(sock, recv_buf, ["set_property", "mute", bool(muted)])
            if not ok:
                logger.warning("mpv IPC: set mute (pre-load) failed")
                return False

            if is_image:
                ok, _, recv_buf = self._ipc_call(sock, recv_buf,
                    ["set_property", "image-display-duration", "inf"])
                if not ok:
                    logger.warning("mpv IPC: set image-display-duration failed")
                    return False
                ok, _, recv_buf = self._ipc_call(sock, recv_buf,
                    ["set_property", "hwdec", "no"])
            else:
                ok, _, recv_buf = self._ipc_call(sock, recv_buf,
                    ["set_property", "hwdec", "drm-copy"])
            if not ok:
                logger.warning("mpv IPC: set hwdec failed")
                return False

            ok, _, recv_buf = self._ipc_call(sock, recv_buf,
                ["loadfile", str(path), "replace"])
            if not ok:
                logger.warning("mpv IPC: loadfile failed for %s", path.name)
                return False

            # Re-assert mute after loadfile — mpv can reset per-file audio
            ok, _, recv_buf = self._ipc_call(sock, recv_buf,
                ["set_property", "mute", bool(muted)])
            if not ok:
                logger.warning("mpv IPC: set mute (post-load) failed (continuing)")

            # When loading an image, toggle fullscreen to force DRM plane refresh.
            # Don't fail the whole call if a toggle drops; best-effort.
            if is_image:
                for _ in range(3):
                    _, _, recv_buf = self._ipc_call(sock, recv_buf,
                        ["set_property", "fullscreen", False])
                    _, _, recv_buf = self._ipc_call(sock, recv_buf,
                        ["set_property", "fullscreen", True])

            logger.info(
                "mpv IPC loadfile succeeded for %s (mute=%s)", path.name, muted,
            )
            return True
        except (OSError, json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning("mpv IPC loadfile failed: %s — will restart mpv", e)
            return False
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    # ── mpv IPC event listener (Phase 1) ──
    #
    # Long-lived thread that subscribes to mpv's IPC event stream so the
    # main loop can react to ``end-file`` / ``start-file`` / etc. without
    # having to poll ``_mpv_process.poll()`` or run a watchdog timer per
    # asset. This is the foundation for identity-based slideshow EOF
    # tracking (Phase 2) and mpv-native finite-loop accounting (Phase 3).

    # Reconnect cadence when the IPC socket is unavailable (e.g. mpv not
    # yet up, or restarting between schedules). Kept short so a freshly
    # spawned mpv is picked up promptly.
    _MPV_EVENT_RECONNECT_DELAY_S = 0.3

    # Read timeout so the listener checks ``_mpv_event_stop`` regularly
    # and exits promptly during shutdown without abandoning a recv() call.
    _MPV_EVENT_READ_TIMEOUT_S = 0.5

    def _start_mpv_event_listener(self) -> None:
        """Start the persistent mpv IPC event listener thread. Idempotent.

        The thread reconnects to ``MPV_IPC_SOCKET`` whenever it becomes
        available, so it is safe to call before mpv has been spawned.
        """
        # Lazy-init threading primitives so bypass-init test fixtures
        # that go through run() don't have to set them up by hand.
        if getattr(self, "_mpv_event_stop", None) is None:
            self._mpv_event_stop = threading.Event()
        if getattr(self, "_mpv_event_connected", None) is None:
            self._mpv_event_connected = threading.Event()
        if getattr(self, "_mpv_event_queue", None) is None:
            self._mpv_event_queue = queue.Queue()
        if getattr(self, "_mpv_drain_lock", None) is None:
            self._mpv_drain_lock = threading.Lock()
        if not hasattr(self, "_mpv_drain_pending"):
            self._mpv_drain_pending = False
        existing = getattr(self, "_mpv_event_thread", None)
        if existing is not None and existing.is_alive():
            return
        self._mpv_event_stop.clear()
        self._mpv_event_thread = threading.Thread(
            target=self._mpv_event_loop,
            name="mpv-event-listener",
            daemon=True,
        )
        self._mpv_event_thread.start()
        logger.info("mpv event listener thread started")

    def _stop_mpv_event_listener(self) -> None:
        """Signal the listener to stop and wait briefly for it to exit."""
        # Defensive: this is called from _teardown, which can run in test
        # contexts where the listener fields were never initialised
        # (bypass-init fixtures). Treat missing fields as "no listener".
        stop_evt = getattr(self, "_mpv_event_stop", None)
        if stop_evt is None:
            return
        stop_evt.set()
        connected_evt = getattr(self, "_mpv_event_connected", None)
        if connected_evt is not None:
            connected_evt.clear()
        t = getattr(self, "_mpv_event_thread", None)
        if t is not None and t.is_alive():
            t.join(timeout=2.0)
            if t.is_alive():
                logger.warning("mpv event listener did not exit within 2s")
        self._mpv_event_thread = None

    def is_mpv_event_listener_ready(self) -> bool:
        """Return True if the listener is currently connected to a running mpv.

        Consumers (slideshow ``play_to_end``, loop_count) check this before
        relying on event-driven transitions; if False, they fall back to
        the legacy duration/respawn paths so we never get stuck.
        """
        return self._mpv_event_connected.is_set()

    def _mpv_event_loop(self) -> None:
        """Background thread body: connect, read events, dispatch via GLib.

        Runs until ``self._mpv_event_stop`` is set. Auto-reconnects on
        ENOENT (mpv not up) and on connection close (mpv respawned).
        Each event dict is stamped with ``_generation`` matching the mpv
        instance that emitted it.
        """
        while not self._mpv_event_stop.is_set():
            sock = self._mpv_event_connect()
            if sock is None:
                # No mpv yet — wait briefly, then retry. wait() returns
                # True if stop was set during the sleep so we exit cleanly.
                if self._mpv_event_stop.wait(self._MPV_EVENT_RECONNECT_DELAY_S):
                    return
                continue

            gen = self._mpv_generation
            self._mpv_event_connected.set()
            logger.debug("mpv event listener connected (gen %d)", gen)
            try:
                self._mpv_event_read_loop(sock, gen)
            finally:
                self._mpv_event_connected.clear()
                try:
                    sock.close()
                except OSError:
                    pass
                logger.debug("mpv event listener disconnected; will retry")

    def _mpv_event_connect(self) -> Optional[socket.socket]:
        """Open a non-blocking-ish AF_UNIX connection to MPV_IPC_SOCKET.

        Returns the connected socket on success, or None if mpv isn't up.
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self._MPV_EVENT_READ_TIMEOUT_S)
            sock.connect(MPV_IPC_SOCKET)
            return sock
        except OSError:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            return None

    def _mpv_event_read_loop(self, sock: socket.socket, generation: int) -> None:
        """Inner loop: read newline-delimited JSON events until disconnect."""
        recv_buf = b""
        while not self._mpv_event_stop.is_set():
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                # Expected — gives us a chance to check the stop flag.
                continue
            except OSError:
                return
            if not chunk:
                # mpv closed its end of the socket
                return
            recv_buf += chunk
            while b"\n" in recv_buf:
                line, recv_buf = recv_buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode())
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if "event" not in msg:
                    # Command responses go to the requesting client only
                    # in practice, but be defensive in case mpv changes.
                    continue
                msg["_generation"] = generation
                self._mpv_event_queue.put(msg)
                self._schedule_drain()

    def _schedule_drain(self) -> None:
        """Schedule a single GLib.idle_add of the drain callback.

        If a drain is already pending, do nothing — the existing callback
        will dequeue everything currently buffered when it runs. This
        prevents flooding the main loop with idle callbacks on bursty
        event streams.
        """
        with self._mpv_drain_lock:
            if self._mpv_drain_pending:
                return
            self._mpv_drain_pending = True
        GLib.idle_add(self._drain_mpv_events)

    def _drain_mpv_events(self) -> bool:
        """GLib idle callback: drain the queue and dispatch each event.

        Always runs on the main loop thread, so consumers don't need
        their own locks. Returns False (one-shot).
        """
        with self._mpv_drain_lock:
            self._mpv_drain_pending = False
        while True:
            try:
                event = self._mpv_event_queue.get_nowait()
            except queue.Empty:
                break
            try:
                self._on_mpv_event(event)
            except Exception:  # pragma: no cover — defensive
                logger.exception("Error handling mpv event")
        return False

    def _on_mpv_event(self, event: dict) -> None:
        """Dispatch a single mpv IPC event on the main loop thread.

        Phase 1 leaves this as a no-op pass-through (the listener still
        runs and is observable for tests / readiness gating). Phase 2
        wires slideshow ``play_to_end`` advancement here, and Phase 3
        wires ``loop_count`` accounting.
        """
        # Phase 2/3 will populate this. Keep the no-op explicit so unit
        # tests can patch / observe dispatches without subclassing.
        return None

    def _start_mpv(self, path: Path, *, loop: bool = False) -> None:
        """Launch mpv subprocess for media playback via DRM output.

        Used on Pi 4 and Pi 5 for both video and image playback.
        Tries IPC loadfile first for seamless switching; falls back to
        full restart if IPC is unavailable.
        """
        # Try seamless switch via IPC if mpv is already running.
        # Scheduled assets always play unmuted (policy: only scheduled
        # content may produce audio; splash is always silent).
        if self._loadfile_mpv(path, loop=loop, muted=False):
            self._current_path = path
            self._current_mtime = path.stat().st_mtime
            started = datetime.now(timezone.utc)
            self._update_current(
                mode=PlaybackMode.PLAY,
                asset=self.current_desired.asset if self.current_desired else path.name,
                started_at=started,
            )
            return

        self._quit_plymouth()
        self._stop_mpv()

        cmd = _build_mpv_command(path, muted=False, loop=loop)
        logger.info("Starting mpv: %s", " ".join(cmd))
        try:
            self._mpv_generation += 1
            self._mpv_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self._current_path = path
            self._current_mtime = path.stat().st_mtime
            started = datetime.now(timezone.utc)
            self._update_current(
                mode=PlaybackMode.PLAY,
                asset=self.current_desired.asset if self.current_desired else path.name,
                started_at=started,
            )
            logger.info("mpv started (pid %d) for %s", self._mpv_process.pid, path.name)
            # Schedule periodic monitoring to detect EOS / errors
            GLib.timeout_add_seconds(2, self._monitor_mpv, path.name)
        except FileNotFoundError:
            logger.error("mpv not found — is it installed?")
            self._update_current(error="mpv not installed")
            self._show_splash()
        except Exception as e:
            logger.error("Failed to start mpv: %s", e)
            self._update_current(error=f"mpv start failed: {e}")
            self._show_splash()

    def _start_stream(self, url: str) -> None:
        """Launch mpv for streaming video playback (HLS, DASH, RTMP, etc.).

        Stops any existing player (mpv, Cage, GStreamer) and starts mpv
        with stream-optimised settings.  Monitoring reuses _monitor_mpv
        with the URL as the asset name for retry/splash fallback.
        """
        self._quit_plymouth()
        self._teardown()
        self._stop_mpv()

        cmd = _build_stream_command(url)
        logger.info("Starting stream: %s", " ".join(cmd))
        try:
            self._mpv_generation += 1
            self._mpv_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self._current_path = None
            self._current_mtime = None
            started = datetime.now(timezone.utc)
            self._update_current(
                mode=PlaybackMode.PLAY,
                asset=self.current_desired.asset if self.current_desired else url,
                started_at=started,
            )
            logger.info("mpv stream started (pid %d) for %s", self._mpv_process.pid, url)
            GLib.timeout_add_seconds(2, self._monitor_mpv, url)
        except FileNotFoundError:
            logger.error("mpv not found — is it installed?")
            self._update_current(error="mpv not installed")
            self._show_splash()
        except Exception as e:
            logger.error("Failed to start stream: %s", e)
            self._update_current(error=f"Stream start failed: {e}")
            self._show_splash()

    def _stop_mpv(self) -> None:
        """Stop the mpv subprocess if running."""
        if self._mpv_process is None:
            return
        if self._mpv_process.poll() is None:
            logger.info("Stopping mpv (pid %d)", self._mpv_process.pid)
            self._mpv_process.terminate()
            try:
                self._mpv_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("mpv did not stop, killing")
                self._mpv_process.kill()
                self._mpv_process.wait(timeout=3)
        self._mpv_process = None
        # Clean up IPC socket
        try:
            os.unlink(MPV_IPC_SOCKET)
        except FileNotFoundError:
            pass

    def _monitor_mpv(self, asset_name: str) -> bool:
        """Periodic check for mpv process exit. Returns False to stop timer."""
        if self._mpv_process is None:
            return False

        # Check if still supposed to be playing this asset.
        # Slideshow mode: current_desired.asset is the slideshow name (e.g.
        # "Test Slideshow"), but mpv was launched for an individual slide
        # file. Skip the asset_name match in that case — exit handling
        # routes through _play_next_slide which knows the slideshow state.
        if self._slideshow is None and (
            not self.current_desired
            or self.current_desired.asset != asset_name
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False
        if self._slideshow is not None and (
            not self.current_desired
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False

        retcode = self._mpv_process.poll()
        if retcode is None:
            # Still running — keep monitoring
            return True

        # mpv exited
        stderr_output = ""
        try:
            stderr_output = self._mpv_process.stderr.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        self._mpv_process = None

        if retcode == 0:
            # Normal exit — EOS
            logger.info("mpv finished playing %s", asset_name)
            # Slideshow: advance to next slide regardless of slide loop flag.
            if self._slideshow:
                self._play_next_slide()
                return False
            # Live streams: auto-restart on EOS (stream may have dropped)
            if (
                self.current_desired
                and getattr(self.current_desired, "asset_type", None) == "stream"
            ):
                logger.info("Stream ended, restarting: %s", asset_name)
                delay = 5  # brief pause before reconnecting
                GLib.timeout_add_seconds(delay, self._retry_desired)
            elif self.current_desired and self.current_desired.loop:
                self._loops_completed += 1
                if (
                    self.current_desired.loop_count is not None
                    and self._loops_completed >= self.current_desired.loop_count
                ):
                    logger.info(
                        "Completed %d/%d loops, switching to splash",
                        self._loops_completed, self.current_desired.loop_count,
                    )
                    self._show_splash()
                    return False
                # Infinite loop but mpv exited (shouldn't happen with --loop=inf)
                # Restart playback
                path = self._resolve_asset(asset_name)
                if path:
                    self._start_mpv(path, loop=True)
                else:
                    self._show_splash()
            else:
                logger.info("Playback complete (no loop), switching to splash")
                self._show_splash()
        else:
            # Error exit
            error_msg = f"mpv exited with code {retcode}"
            if stderr_output:
                # Take last meaningful line from stderr
                lines = [l.strip() for l in stderr_output.splitlines() if l.strip()]
                if lines:
                    error_msg = f"mpv error: {lines[-1]}"
            logger.error("mpv error for %s: %s", asset_name, error_msg)
            self._update_current(error=error_msg)
            delay = self._error_retry_delay
            self._error_retry_delay = min(
                self._error_retry_delay * 2, self._RETRY_DELAY_MAX,
            )
            self._pending_error = error_msg
            self._show_splash()
            GLib.timeout_add_seconds(delay, self._retry_desired)

        return False

    def _on_state_changed(self, bus, message) -> None:
        """Track pipeline state transitions and update current.json."""
        # Only react to pipeline-level state changes, not individual elements
        if message.src != self.pipeline:
            return
        old, new, _pending = message.parse_state_changed()
        new_name = new.value_nick.upper()
        logger.debug("Pipeline state: %s -> %s", old.value_nick, new_name)

        if new == Gst.State.PLAYING and self.current_desired:
            # Only reset backoff when desired content starts playing,
            # not when splash fallback reaches PLAYING
            if self.current_desired.mode == PlaybackMode.PLAY:
                self._error_retry_delay = 3
            started = datetime.now(timezone.utc)
            mode = self.current_desired.mode
            asset = self.current_desired.asset
            self._update_current(
                mode=mode, asset=asset, started_at=started,
            )
            logger.info("Pipeline reached PLAYING for %s", asset)

    def _on_eos(self, bus, message) -> None:
        logger.info("EOS received")
        if self.current_desired and self.current_desired.loop:
            self._loops_completed += 1
            # Finite loop count: stop after N loops
            if (
                self.current_desired.loop_count is not None
                and self._loops_completed >= self.current_desired.loop_count
            ):
                logger.info(
                    "Completed %d/%d loops, switching to splash",
                    self._loops_completed, self.current_desired.loop_count,
                )
                self._show_splash()
                return
            # Seamless loop: seek to beginning
            self.pipeline.seek_simple(
                Gst.Format.TIME, Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT, 0
            )
        else:
            logger.info("Playback complete, switching to splash")
            self._show_splash()

    _ERROR_TRANSLATIONS = [
        ("drmModeSetPlane failed", "No display connected — check the HDMI cable"),
        ("Could not open audio device", "No audio output — check the HDMI cable"),
        ("Failed to allocate required memory", "Not enough memory to decode this video"),
    ]

    _DISPLAY_ERROR_MARKERS = ("drmModeSetPlane", "Could not open audio device")

    @classmethod
    def _translate_error(cls, raw: str, debug: str = "") -> str:
        """Translate raw GStreamer error into a user-friendly message.

        Checks both the error message and debug string, since GStreamer
        often wraps the real cause (e.g. drmModeSetPlane) in debug info
        while the message is a generic 'resource error'.
        """
        combined = f"{raw} {debug}"
        for marker, friendly in cls._ERROR_TRANSLATIONS:
            if marker in combined:
                return friendly
        return f"Playback error: {raw}"

    @classmethod
    def _is_display_error(cls, raw: str, debug: str = "") -> bool:
        """Return True if the error indicates a missing/broken display."""
        combined = f"{raw} {debug}"
        return any(m in combined for m in cls._DISPLAY_ERROR_MARKERS)

    def _probe_display(self) -> tuple[Optional[bool], list[PortStatus]]:
        """Probe all HDMI ports via the board-specific display probe.

        Returns ``(primary_connected, all_ports)`` where ``primary_connected``
        is the ``connected`` value for port 0 (or ``None`` if the board has
        no ports or probing failed for it).
        """
        try:
            ports = self._display_probe.probe_all()
        except Exception:
            logger.debug("Display probe failed", exc_info=True)
            return None, []
        primary = ports[0].connected if ports else None
        return primary, ports

    def _debounce_display(
        self, ports: list[PortStatus], previous: Optional[list[PortStatus]]
    ) -> list[PortStatus]:
        """Require two consecutive matching probes before flipping True<->False.

        Transitions involving ``None`` (indeterminate) commit immediately.
        Only applies in periodic polling; explicit ``_update_current`` calls
        pass the raw probe result through.
        """
        prev_by_name: dict[str, Optional[bool]] = {}
        if previous:
            for p in previous:
                prev_by_name[p.name] = p.connected
        committed: list[PortStatus] = []
        for port in ports:
            prev = prev_by_name.get(port.name)
            new = port.connected
            if new == prev or new is None or prev is None:
                # Instantaneous commit: no change, or a None endpoint.
                self._display_pending.pop(port.name, None)
                committed.append(port)
                continue
            # True<->False flip: require a second matching reading.
            pending = self._display_pending.get(port.name)
            if pending and pending[0] == new:
                self._display_pending.pop(port.name, None)
                committed.append(port)
            else:
                self._display_pending[port.name] = (new, 1)
                committed.append(PortStatus(name=port.name, connected=prev))
        return committed

    _RETRY_DELAY_MAX = 15

    def _on_error(self, bus, message) -> None:
        err, debug = message.parse_error()
        debug_str = debug or ""
        friendly = self._translate_error(err.message, debug_str)
        logger.error("Pipeline error: %s (%s)", err.message, debug_str)
        self._teardown()
        self._update_current(error=friendly)
        # Exponential backoff for display errors to avoid burning CPU/memory
        if self._is_display_error(err.message, debug_str):
            delay = self._error_retry_delay
            self._error_retry_delay = min(
                self._error_retry_delay * 2, self._RETRY_DELAY_MAX,
            )
        else:
            delay = 3
            self._error_retry_delay = 3
        # Show splash immediately as visual fallback (preserves error)
        self._pending_error = friendly
        self._show_splash()
        # Schedule retry of the original desired content after backoff
        logger.info("Retrying in %ds", delay)
        GLib.timeout_add_seconds(delay, self._retry_desired)

    # ── Splash ──

    def _show_splash(self) -> bool:
        """Show splash screen. Returns False to cancel GLib timeout repeat."""
        self._stop_cage()
        error = self._pending_error
        self._pending_error = None
        splash = self._find_splash()
        if splash:
            is_video = splash.suffix.lower() == ".mp4"
            self._current_path = splash
            self._current_mtime = splash.stat().st_mtime
            # Use mpv on Pi 4/5 for both video and image splash, GStreamer on Zero 2 W
            if self._player_backend == "mpv":
                # Try seamless IPC switch first. Splash is always muted,
                # regardless of whether the splash asset is an image or a
                # video — only scheduled assets are allowed to produce audio.
                if self._loadfile_mpv(splash, loop=True, muted=True):
                    logger.info("Showing splash via mpv IPC: %s", splash.name)
                else:
                    self._teardown()
                    self._quit_plymouth()
                    cmd = _build_mpv_command(splash, muted=True, loop=True)
                    logger.info("Showing splash via mpv: %s", splash.name)
                    try:
                        self._mpv_generation += 1
                        self._mpv_process = subprocess.Popen(
                            cmd,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE,
                        )
                    except (FileNotFoundError, OSError) as e:
                        logger.error("mpv splash failed: %s — falling back to GStreamer", e)
                        self.pipeline = self._build_pipeline(splash, is_video)
                        self.pipeline.set_state(Gst.State.PLAYING)
            else:
                self._teardown()
                self.pipeline = self._build_pipeline(splash, is_video)
                self.pipeline.set_state(Gst.State.PLAYING)
            # Update desired state so _on_state_changed uses correct mode
            self.current_desired = DesiredState(
                mode=PlaybackMode.SPLASH, loop=is_video
            )
            self._update_current(mode=PlaybackMode.SPLASH, asset=splash.name, error=error)
            logger.info("Showing splash: %s", splash.name)
        else:
            logger.warning("No splash asset found")
            self._update_current(mode=PlaybackMode.STOP, error=error)
        return False

    def _retry_desired(self) -> bool:
        """Re-read desired.json and attempt playback. Returns False (one-shot)."""
        logger.info("Retrying desired state")
        self.apply_desired()
        return False

    # ── State management ──

    def _query_position_ms(self) -> Optional[int]:
        """Query current playback position from the GStreamer pipeline."""
        if not self.pipeline:
            return None
        try:
            ok, pos = self.pipeline.query_position(Gst.Format.TIME)
            if ok and pos >= 0:
                return pos // 1_000_000  # nanoseconds → milliseconds
        except Exception:
            pass
        return None

    def _update_current(
        self,
        mode: PlaybackMode = PlaybackMode.STOP,
        asset: Optional[str] = None,
        error: Optional[str] = None,
        started_at: Optional[datetime] = None,
    ) -> None:
        pipeline_state = "NULL"
        if self._cage_process and self._cage_process.poll() is None:
            pipeline_state = "PLAYING"
        elif self._mpv_process and self._mpv_process.poll() is None:
            pipeline_state = "PLAYING"
        elif self.pipeline:
            try:
                _, state, _ = self.pipeline.get_state(0)
                pipeline_state = state.value_nick.upper()
            except Exception:
                pipeline_state = "ERROR"

        primary_connected, ports = self._probe_display()
        state = CurrentState(
            mode=mode,
            asset=asset,
            loop=self.current_desired.loop if self.current_desired else False,
            loop_count=self.current_desired.loop_count if self.current_desired else None,
            loops_completed=self._loops_completed,
            started_at=started_at,
            playback_position_ms=self._query_position_ms(),
            pipeline_state=pipeline_state,
            display_connected=primary_connected,
            display_ports=ports or None,
            error=error,
        )
        write_state(self.current_path, state)

    def _update_position(self) -> bool:
        """Periodic callback to update playback position in current.json."""
        is_active = (
            self.pipeline
            or (self._mpv_process and self._mpv_process.poll() is None)
            or (self._cage_process and self._cage_process.poll() is None)
        )
        if (
            not is_active
            or not self.current_desired
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False  # Stop the timer
        try:
            current = read_state(self.current_path, CurrentState)
            pos = self._query_position_ms()
            _, raw_ports = self._probe_display()
            ports = self._debounce_display(raw_ports, current.display_ports)
            primary = ports[0].connected if ports else None
            changed = False
            if pos is not None and current.playback_position_ms != pos:
                current.playback_position_ms = pos
                changed = True
            if current.display_connected != primary:
                if primary is not None and current.display_connected is not None:
                    logger.warning(
                        "Display %s",
                        "connected" if primary else "disconnected",
                    )
                current.display_connected = primary
                changed = True
            old_ports = current.display_ports or []
            old_map = {p.name: p.connected for p in old_ports}
            new_map = {p.name: p.connected for p in ports}
            if old_map != new_map:
                current.display_ports = ports or None
                changed = True
            if changed:
                current.updated_at = datetime.now(timezone.utc)
                write_state(self.current_path, current)
        except Exception:
            logger.debug("Failed to update playback position")
        return True  # Keep the timer running

    def _probe_display_tick(self) -> bool:
        """Periodic display probe that runs regardless of playback state.

        `_update_position` only runs while a pipeline is active, so during
        splash/idle the display connection state in current.json would go
        stale and the CMS would keep reporting whatever was last observed on
        a playback transition. This tick re-probes HDMI at a steady cadence
        and writes updates to current.json so the next heartbeat reflects
        reality. Always returns True to keep the GLib timer running for the
        lifetime of the service.
        """
        try:
            current = read_state(self.current_path, CurrentState)
            _, raw_ports = self._probe_display()
            ports = self._debounce_display(raw_ports, current.display_ports)
            primary = ports[0].connected if ports else None
            changed = False
            if current.display_connected != primary:
                if primary is not None and current.display_connected is not None:
                    logger.warning(
                        "Display %s",
                        "connected" if primary else "disconnected",
                    )
                current.display_connected = primary
                changed = True
            old_map = {p.name: p.connected for p in (current.display_ports or [])}
            new_map = {p.name: p.connected for p in ports}
            if old_map != new_map:
                current.display_ports = ports or None
                changed = True
            if changed:
                current.updated_at = datetime.now(timezone.utc)
                write_state(self.current_path, current)
        except Exception:
            logger.debug("Display probe tick failed", exc_info=True)
        return True  # Keep the timer running for the lifetime of the service

    def apply_desired(self) -> None:
        """Read desired state and apply it to the player."""
        if not self.desired_path.exists():
            if self.current_desired is None:
                self._show_splash()
                self.current_desired = DesiredState(mode=PlaybackMode.SPLASH)
            return

        desired = read_state(self.desired_path, DesiredState)

        # Skip if unchanged (same timestamp)
        if (
            self.current_desired
            and desired.timestamp == self.current_desired.timestamp
        ):
            return

        # Skip pipeline rebuild if the same file is already being displayed.
        # Covers CMS re-syncs, mode changes (SPLASH→PLAY) for the same image,
        # and timestamp-only updates.  Avoids a visible black flash.
        # Compare resolved file path + mtime to detect content changes even if
        # the filename is reused.  Also compares loop_count since that affects
        # video playback behaviour.
        is_active = (
            self.pipeline
            or (self._mpv_process and self._mpv_process.poll() is None)
            or (self._cage_process and self._cage_process.poll() is None)
        )
        if is_active and self._current_path and desired.asset:
            new_path = self._resolve_asset(desired.asset)
            cur_loop_count = self.current_desired.loop_count if self.current_desired else None
            if (
                new_path and new_path == self._current_path
                and desired.loop_count == cur_loop_count
            ):
                # Same path — verify file hasn't been replaced (mtime check)
                try:
                    current_mtime = self._current_path.stat().st_mtime
                except OSError:
                    current_mtime = None
                if current_mtime == self._current_mtime:
                    logger.info("Same file already playing (%s), skipping rebuild", new_path.name)
                    self.current_desired = desired
                    self._update_current(mode=desired.mode, asset=desired.asset)
                    return

        logger.info("Applying desired state: %s", desired.model_dump_json())

        if desired.mode == PlaybackMode.STOP:
            self._clear_slideshow()
            self.current_desired = desired
            self._show_splash()
            return

        if desired.mode == PlaybackMode.SPLASH:
            self._clear_slideshow()
            self.current_desired = desired
            self._show_splash()
            return

        if desired.mode == PlaybackMode.PLAY and desired.url:
            self._clear_slideshow()
            # Stream assets → mpv (handles HLS/DASH/RTMP natively)
            if desired.asset_type == "stream":
                mpv_proc = self._mpv_process
                if mpv_proc and mpv_proc.poll() is None:
                    if self.current_desired and self.current_desired.url == desired.url:
                        logger.info("Same stream already playing (%s), skipping", desired.url)
                        self.current_desired = desired
                        return
                self.current_desired = desired
                self._start_stream(desired.url)
                return

            # Webpage rendering via Cage+Chromium
            cage_proc = self._cage_process
            if cage_proc and cage_proc.poll() is None:
                # Cage is still running — skip if same URL
                if self.current_desired and self.current_desired.url == desired.url:
                    logger.info("Same webpage already rendering (%s), skipping", desired.url)
                    self.current_desired = desired
                    return
            # Cage not running or different URL — (re)start
            self.current_desired = desired
            self._start_cage(desired.url)
            return

        if desired.mode == PlaybackMode.PLAY and desired.asset:
            # Slideshow: read manifest from assets/slideshows/<name>.json
            # and sequence slides ourselves.  Bypass single-file resolution.
            if desired.asset_type == "slideshow":
                # If the same slideshow is already running, leave it alone;
                # otherwise (re)start.  Manifest changes show up via the
                # CMS-side checksum and will arrive as a fresh fetch.
                ss = self._slideshow
                if ss and ss.get("name") == desired.asset:
                    self.current_desired = desired
                    return
                self.current_desired = desired
                self._health_retries = 0
                self._loops_completed = 0
                self._start_slideshow(desired.asset, desired.loop_count)
                return

            # Leaving any in-flight slideshow before single-asset playback.
            self._clear_slideshow()

            path = self._resolve_asset(desired.asset)
            if not path:
                logger.error("Asset not found: %s — showing splash", desired.asset)
                self._update_current(error=f"Asset not found: {desired.asset}")
                self._show_splash()
                return
            # Verify file is readable, non-empty, and checksum matches
            try:
                size = path.stat().st_size
                if size == 0:
                    logger.error("Asset is empty (0 bytes): %s — showing splash", desired.asset)
                    self._update_current(error=f"Asset is empty: {desired.asset}")
                    self._show_splash()
                    return
                with open(path, "rb") as f:
                    header = f.read(8)
                if len(header) < 8:
                    logger.error("Asset too small to be valid: %s — showing splash", desired.asset)
                    self._update_current(error=f"Asset too small: {desired.asset}")
                    self._show_splash()
                    return
            except OSError as e:
                logger.error("Asset not readable: %s (%s) — showing splash", desired.asset, e)
                self._update_current(error=f"Asset not readable: {desired.asset}")
                self._show_splash()
                return
            self.current_desired = desired
            self._health_retries = 0
            is_video = path.suffix.lower() == ".mp4"
            self._loops_completed = 0

            # Dispatch to mpv on Pi 4/5 (video and images), GStreamer on Zero 2 W
            if self._player_backend == "mpv":
                loop = bool(desired.loop)
                # For finite loop count, let mpv handle it naturally
                if is_video and desired.loop_count is not None and desired.loop_count > 0:
                    loop = False  # Don't use --loop=inf; monitor exits instead
                self._start_mpv(path, loop=loop)
                GLib.timeout_add_seconds(10, self._update_position)
            else:
                self._teardown()
                self._current_path = path
                self._current_mtime = path.stat().st_mtime
                self.pipeline = self._build_pipeline(path, is_video)
                self.pipeline.set_state(Gst.State.PLAYING)
                self._update_current(mode=PlaybackMode.PLAY, asset=desired.asset)
                # Schedule a health check to verify the pipeline actually started
                GLib.timeout_add_seconds(
                    5, self._check_pipeline_health, desired.asset,
                )
                # Periodic position updates for CMS status reporting
                GLib.timeout_add_seconds(10, self._update_position)

    _HEALTH_CHECK_MAX_RETRIES = 3

    def _check_pipeline_health(self, asset_name: str) -> bool:
        """Verify the pipeline reached PLAYING state. Returns False (no repeat)."""
        if not self.pipeline:
            return False
        # Only check if we're still supposed to be playing this asset
        if (
            not self.current_desired
            or self.current_desired.asset != asset_name
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False

        _, state, _ = self.pipeline.get_state(0)
        if state == Gst.State.PLAYING:
            if self._health_retries > 0:
                logger.info(
                    "Pipeline reached PLAYING for %s after %d retry(ies)",
                    asset_name, self._health_retries,
                )
                self._health_retries = 0
            return False

        # Not PLAYING — retry with a full rebuild
        if self._health_retries < self._HEALTH_CHECK_MAX_RETRIES:
            self._health_retries += 1
            logger.warning(
                "Pipeline health check failed for %s: state is %s — "
                "rebuilding (retry %d/%d)",
                asset_name, state.value_nick if state else "NULL",
                self._health_retries, self._HEALTH_CHECK_MAX_RETRIES,
            )
            path = self._current_path or self._resolve_asset(asset_name)
            self._teardown()
            if path and path.is_file():
                is_video = path.suffix.lower() == ".mp4"
                self._current_path = path
                self._current_mtime = path.stat().st_mtime
                self.pipeline = self._build_pipeline(path, is_video)
                self._loops_completed = 0
                self.pipeline.set_state(Gst.State.PLAYING)
                GLib.timeout_add_seconds(
                    5, self._check_pipeline_health, asset_name,
                )
            else:
                logger.error("Asset file not found for retry: %s", asset_name)
                self._health_retries = 0
                self._update_current(
                    error=f"Asset not found during retry: {asset_name}",
                )
                GLib.timeout_add_seconds(3, self._show_splash)
            return False

        # All retries exhausted
        logger.error(
            "Pipeline health check failed for %s after %d retries: "
            "state is %s (expected PLAYING)",
            asset_name, self._HEALTH_CHECK_MAX_RETRIES,
            state.value_nick if state else "NULL",
        )
        self._health_retries = 0
        self._teardown()
        self._update_current(
            error=f"Pipeline failed to reach PLAYING state after {self._HEALTH_CHECK_MAX_RETRIES} retries",
        )
        GLib.timeout_add_seconds(3, self._show_splash)
        return False

    # ── State file watcher ──

    def _setup_inotify(self) -> bool:
        """Watch desired.json via inotify. Returns True on success."""
        try:
            from inotify_simple import INotify, flags as inotify_flags

            inotify = INotify()
            inotify.add_watch(
                str(self.state_dir),
                inotify_flags.CLOSE_WRITE | inotify_flags.MOVED_TO,
            )

            def on_inotify_event(fd, condition):
                for event in inotify.read():
                    if event.name == "desired.json":
                        logger.debug("desired.json changed (inotify)")
                        GLib.idle_add(self.apply_desired)
                return True

            GLib.io_add_watch(inotify.fd, GLib.IO_IN, on_inotify_event)
            logger.info("Watching state dir via inotify")
            return True
        except ImportError:
            return False

    def _poll_state(self) -> bool:
        """Poll-based fallback for state changes."""
        self.apply_desired()
        return self._running

    # ── Main loop ──

    @staticmethod
    def _suppress_console() -> None:
        """Disable VT console so text doesn't bleed through during transitions."""
        try:
            # Unbind VT console from framebuffer
            vtcon = Path("/sys/class/vtconsole/vtcon1/bind")
            if vtcon.exists():
                vtcon.write_text("0")
                logger.info("Unbound VT console from framebuffer")

            # Blank all virtual terminals
            for tty_num in range(1, 7):
                tty_path = f"/dev/tty{tty_num}"
                if os.path.exists(tty_path):
                    subprocess.run(
                        ["/usr/bin/setterm", "--blank", "force", "--term", "linux"],
                        stdin=open(tty_path),
                        stdout=open(tty_path, "w"),
                        stderr=subprocess.DEVNULL,
                    )
        except Exception as e:
            logger.warning("Could not suppress console: %s", e)

    def _quit_plymouth(self) -> None:
        """Tell Plymouth to quit, retaining its last frame on the framebuffer.

        Called once before the first GStreamer pipeline build so kmssink can
        claim the DRM device.  The --retain-splash flag keeps the boot splash
        visible until GStreamer renders its first frame.
        """
        if self._plymouth_quit:
            return
        self._plymouth_quit = True
        try:
            subprocess.run(
                ["/usr/bin/plymouth", "quit", "--retain-splash"],
                timeout=5, capture_output=True,
            )
            logger.info("Plymouth quit (retained splash)")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.debug("Plymouth quit skipped: %s", e)

    @staticmethod
    def _clear_framebuffer() -> None:
        """Clear the framebuffer to black (used during pipeline transitions)."""
        try:
            fb_path = Path("/dev/fb0")
            if fb_path.exists():
                with open(fb_path, "wb") as fb:
                    # 1920x1080 @ 16bpp = 4,147,200 bytes
                    chunk = b"\x00" * 65536
                    total = 1920 * 1080 * 2
                    written = 0
                    while written < total:
                        to_write = min(len(chunk), total - written)
                        fb.write(chunk[:to_write])
                        written += to_write
                logger.info("Cleared framebuffer to black")
        except Exception as e:
            logger.warning("Could not clear framebuffer: %s", e)

    def run(self) -> None:
        logger.info("Agora Player starting")
        self.state_dir.mkdir(parents=True, exist_ok=True)

        # Suppress VT console text (preserves Plymouth retained splash on framebuffer)
        self._suppress_console()

        # Start the persistent mpv IPC event listener early. It auto-
        # reconnects, so it's safe to run before the first mpv is spawned.
        self._start_mpv_event_listener()

        # Apply initial state (may show splash, which can take seconds)
        self.apply_desired()

        # Set up file watcher (inotify preferred, poll fallback)
        if not self._setup_inotify():
            logger.warning("inotify unavailable, falling back to 2s polling")
            GLib.timeout_add_seconds(2, self._poll_state)

        # Re-apply: desired.json may have been written while the initial splash
        # pipeline was loading (before inotify was watching).
        self.apply_desired()

        # Periodic display probe: runs regardless of playback state so
        # display_connected in current.json stays fresh during splash/idle.
        GLib.timeout_add_seconds(10, self._probe_display_tick)

        # Signal handlers for clean shutdown
        def on_shutdown(signum, frame):
            logger.info("Received signal %d, shutting down", signum)
            self._running = False
            self._teardown()
            self.loop.quit()

        signal.signal(signal.SIGTERM, on_shutdown)
        signal.signal(signal.SIGINT, on_shutdown)

        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            self._teardown()
            logger.info("Agora Player stopped")
