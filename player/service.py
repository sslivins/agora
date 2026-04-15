"""Agora Player Service — watches desired state and manages media playback."""

import json
import logging
import os
import signal
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst  # noqa: E402

from shared.board import Board, alsa_card, get_board, get_i2c_bus, player_backend, supported_codecs  # noqa: E402
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

    _card = alsa_card()
    return (
        'filesrc location="{path}" ! '
        "qtdemux name=dmux "
        f"dmux.video_0 ! queue ! {decode} ! kmssink driver-name=vc4 sync=true "
        "dmux.audio_0 ! queue ! decodebin ! audioconvert ! audioresample ! "
        f'alsasink device="hdmi:CARD={_card},DEV=0"'
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


def _build_mpv_command(path: Path, *, audio: bool = True, loop: bool = False) -> list[str]:
    """Build the mpv command for media playback via DRM output.

    Used on Pi 4 and Pi 5 for both video and image playback.
    For video: uses drm-copy hwdec for hardware decoding.
    For images: uses image-display-duration=inf to hold the frame.
    Includes IPC socket for seamless content switching via loadfile.
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
    ]
    if is_image:
        cmd.append("--image-display-duration=inf")
        cmd.append("--no-audio")
    else:
        cmd.append("--hwdec=drm-copy")
        if not audio:
            cmd.append("--no-audio")
        else:
            cmd.extend(["--ao=alsa", f"--audio-device=alsa/hdmi:CARD={alsa_card()},DEV=0"])
    if loop:
        cmd.append("--loop=inf")
    cmd.append(str(path))
    return cmd


class AgoraPlayer:
    """Manages media playback driven by desired state file changes.

    Uses GStreamer pipelines on Zero 2 W (and for images/splash on all boards),
    and mpv subprocess on Pi 4/Pi 5 for video playback with hardware decoding.
    """

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

    # HDMI display detection via DDC/EDID I2C probe
    _EDID_ADDR = 0x50
    _I2C_SLAVE = 0x0703  # ioctl request code

    DEFAULT_SPLASH_CONFIG = "splash/default.png"

    def __init__(self, base_path: str = "/opt/agora"):
        self.base = Path(base_path)
        self.state_dir = self.base / "state"
        self.persist_dir = self.base / "persist"
        self.assets_dir = self.base / "assets"
        self.desired_path = self.state_dir / "desired.json"
        self.current_path = self.state_dir / "current.json"
        self.splash_config_path = self.persist_dir / "splash"

        self._board = get_board()
        self._i2c_bus = get_i2c_bus()
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

        Gst.init(None)

    # ── Asset resolution ──

    def _resolve_asset(self, name: str) -> Optional[Path]:
        for subdir in ["videos", "images", "splash"]:
            path = self.assets_dir / subdir / name
            if path.is_file():
                return path
        return None

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
            "--load-extension=/opt/agora/src/player/extensions/hide-cursor",
            url,
        ]

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

    def _loadfile_mpv(self, path: Path, *, loop: bool = False) -> bool:
        """Switch content in a running mpv via IPC socket. Returns True on success."""
        if self._mpv_process is None or self._mpv_process.poll() is not None:
            return False
        is_image = path.suffix.lower() in self._IMAGE_EXTS
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect(MPV_IPC_SOCKET)
            # Set loop property before loading
            loop_val = "inf" if loop else "no"
            sock.sendall(json.dumps({"command": ["set_property", "loop-file", loop_val]}).encode() + b"\n")
            sock.recv(512)  # read response
            # Set image-display-duration based on content type
            if is_image:
                sock.sendall(json.dumps({"command": ["set_property", "image-display-duration", "inf"]}).encode() + b"\n")
                sock.recv(512)
                sock.sendall(json.dumps({"command": ["set_property", "hwdec", "no"]}).encode() + b"\n")
                sock.recv(512)
            else:
                sock.sendall(json.dumps({"command": ["set_property", "hwdec", "drm-copy"]}).encode() + b"\n")
                sock.recv(512)
            # Load the new file (replace = stop current, play new)
            sock.sendall(json.dumps({"command": ["loadfile", str(path), "replace"]}).encode() + b"\n")
            time.sleep(0.3)  # allow mpv to process and queue response + events
            resp = sock.recv(4096)
            # Parse response lines — look for the loadfile result, skip events
            success = False
            for line in resp.decode().strip().split("\n"):
                try:
                    msg = json.loads(line)
                    if "event" not in msg and msg.get("error") == "success":
                        success = True
                        break
                except json.JSONDecodeError:
                    continue
            if not success:
                sock.close()
                logger.warning("mpv IPC loadfile — no success in response: %s", resp[:200])
                return False
            # When loading an image, toggle fullscreen to force DRM plane refresh
            if is_image:
                time.sleep(0.2)
                for _ in range(3):
                    sock.sendall(json.dumps({"command": ["set_property", "fullscreen", False]}).encode() + b"\n")
                    sock.recv(512)
                    sock.sendall(json.dumps({"command": ["set_property", "fullscreen", True]}).encode() + b"\n")
                    sock.recv(512)
            sock.close()
            logger.info("mpv IPC loadfile succeeded for %s", path.name)
            return True
        except (OSError, json.JSONDecodeError, IndexError, KeyError) as e:
            logger.warning("mpv IPC loadfile failed: %s — will restart mpv", e)
            return False

    def _start_mpv(self, path: Path, *, loop: bool = False) -> None:
        """Launch mpv subprocess for media playback via DRM output.

        Used on Pi 4 and Pi 5 for both video and image playback.
        Tries IPC loadfile first for seamless switching; falls back to
        full restart if IPC is unavailable.
        """
        # Try seamless switch via IPC if mpv is already running
        if self._loadfile_mpv(path, loop=loop):
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

        cmd = _build_mpv_command(path, audio=True, loop=loop)
        logger.info("Starting mpv: %s", " ".join(cmd))
        try:
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

        # Check if still supposed to be playing this asset
        if (
            not self.current_desired
            or self.current_desired.asset != asset_name
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
            if self.current_desired and self.current_desired.loop:
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

    @classmethod
    def _is_display_connected(cls) -> Optional[bool]:
        """Probe the HDMI DDC/EDID I2C bus to detect a connected display.

        Reads one byte from the EDID EEPROM at address 0x50 on the board's
        primary HDMI I2C bus.  Returns True if the device responds (display
        connected), False if it fails with an I/O error (no display), or
        None if the I2C bus is not available.
        """
        i2c_bus = get_i2c_bus()
        try:
            fd = os.open(i2c_bus, os.O_RDWR)
        except OSError:
            return None
        try:
            import fcntl
            fcntl.ioctl(fd, cls._I2C_SLAVE, cls._EDID_ADDR)
            os.read(fd, 1)
            return True
        except OSError:
            return False
        finally:
            os.close(fd)

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
                # Try seamless IPC switch first
                if self._loadfile_mpv(splash, loop=True):
                    logger.info("Showing splash via mpv IPC: %s", splash.name)
                else:
                    self._teardown()
                    self._quit_plymouth()
                    cmd = _build_mpv_command(splash, audio=False, loop=True)
                    logger.info("Showing splash via mpv: %s", splash.name)
                    try:
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

        state = CurrentState(
            mode=mode,
            asset=asset,
            loop=self.current_desired.loop if self.current_desired else False,
            loop_count=self.current_desired.loop_count if self.current_desired else None,
            loops_completed=self._loops_completed,
            started_at=started_at,
            playback_position_ms=self._query_position_ms(),
            pipeline_state=pipeline_state,
            display_connected=self._is_display_connected(),
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
            display = self._is_display_connected()
            changed = False
            if pos is not None and current.playback_position_ms != pos:
                current.playback_position_ms = pos
                changed = True
            if current.display_connected != display:
                if display is not None and current.display_connected is not None:
                    logger.warning("Display %s", "connected" if display else "disconnected")
                current.display_connected = display
                changed = True
            if changed:
                current.updated_at = datetime.now(timezone.utc)
                write_state(self.current_path, current)
        except Exception:
            logger.debug("Failed to update playback position")
        return True  # Keep the timer running

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
            self.current_desired = desired
            self._show_splash()
            return

        if desired.mode == PlaybackMode.SPLASH:
            self.current_desired = desired
            self._show_splash()
            return

        if desired.mode == PlaybackMode.PLAY and desired.url:
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

        # Apply initial state (may show splash, which can take seconds)
        self.apply_desired()

        # Set up file watcher (inotify preferred, poll fallback)
        if not self._setup_inotify():
            logger.warning("inotify unavailable, falling back to 2s polling")
            GLib.timeout_add_seconds(2, self._poll_state)

        # Re-apply: desired.json may have been written while the initial splash
        # pipeline was loading (before inotify was watching).
        self.apply_desired()

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
