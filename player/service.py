"""Agora Player Service — watches desired state and manages GStreamer pipelines."""

import json
import logging
import os
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst  # noqa: E402

from shared.models import CurrentState, DesiredState, PlaybackMode  # noqa: E402
from shared.state import read_state, write_state  # noqa: E402

logger = logging.getLogger("agora.player")


class AgoraPlayer:
    """Manages GStreamer pipelines driven by desired state file changes."""

    VIDEO_PIPELINE = (
        'filesrc location="{path}" ! '
        "qtdemux name=dmux "
        "dmux.video_0 ! queue ! h264parse ! v4l2h264dec ! kmssink driver-name=vc4 sync=true "
        "dmux.audio_0 ! queue ! decodebin ! audioconvert ! audioresample ! "
        'alsasink device="hdmi:CARD=vc4hdmi,DEV=0"'
    )

    VIDEO_PIPELINE_NO_AUDIO = (
        'filesrc location="{path}" ! '
        "qtdemux name=dmux "
        "dmux.video_0 ! queue ! h264parse ! v4l2h264dec ! kmssink driver-name=vc4 sync=false"
    )

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

    def __init__(self, base_path: str = "/opt/agora"):
        self.base = Path(base_path)
        self.state_dir = self.base / "state"
        self.persist_dir = self.base / "persist"
        self.assets_dir = self.base / "assets"
        self.desired_path = self.state_dir / "desired.json"
        self.current_path = self.state_dir / "current.json"
        self.splash_config_path = self.persist_dir / "splash"

        self.pipeline: Optional[Gst.Pipeline] = None
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

    def _build_pipeline(self, path: Path, is_video: bool) -> Gst.Pipeline:
        # Quit Plymouth before GStreamer claims the DRM device
        self._quit_plymouth()

        if is_video:
            if self._has_audio(path):
                pipeline_str = self.VIDEO_PIPELINE.format(path=path)
            else:
                logger.info("No audio track detected, using video-only pipeline")
                pipeline_str = self.VIDEO_PIPELINE_NO_AUDIO.format(path=path)
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
        self._teardown()
        error = self._pending_error
        self._pending_error = None
        splash = self._find_splash()
        if splash:
            is_video = splash.suffix.lower() == ".mp4"
            self._current_path = splash
            self._current_mtime = splash.stat().st_mtime
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
        if self.pipeline:
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
            error=error,
        )
        write_state(self.current_path, state)

    def _update_position(self) -> bool:
        """Periodic callback to update playback position in current.json."""
        if (
            not self.pipeline
            or not self.current_desired
            or self.current_desired.mode != PlaybackMode.PLAY
        ):
            return False  # Stop the timer
        try:
            current = read_state(self.current_path, CurrentState)
            pos = self._query_position_ms()
            if pos is not None and current.playback_position_ms != pos:
                current.playback_position_ms = pos
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
        if self.pipeline and self._current_path and desired.asset:
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
            if desired.expected_checksum:
                import hashlib
                sha = hashlib.sha256()
                with open(path, "rb") as f:
                    for chunk in iter(lambda: f.read(65536), b""):
                        sha.update(chunk)
                actual = sha.hexdigest()
                if actual != desired.expected_checksum:
                    logger.error(
                        "Checksum mismatch for %s: expected %s, got %s — showing splash",
                        desired.asset, desired.expected_checksum, actual,
                    )
                    self._update_current(
                        error=f"Checksum mismatch: {desired.asset}",
                    )
                    self._show_splash()
                    return
            self.current_desired = desired
            self._teardown()
            self._health_retries = 0
            is_video = path.suffix.lower() == ".mp4"
            self._current_path = path
            self._current_mtime = path.stat().st_mtime
            self.pipeline = self._build_pipeline(path, is_video)
            self._loops_completed = 0
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
