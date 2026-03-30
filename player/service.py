"""Agora Player Service — watches desired state and manages GStreamer pipelines."""

import json
import logging
import os
import signal
import subprocess
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
        "dmux.video_0 ! queue ! h264parse ! v4l2h264dec ! kmssink sync=true "
        "dmux.audio_0 ! queue ! decodebin ! audioconvert ! audioresample ! "
        'alsasink device="hdmi:CARD=vc4hdmi,DEV=0"'
    )

    IMAGE_PIPELINE_JPEG = (
        'filesrc location="{path}" ! '
        "jpegparse ! jpegdec ! videoconvert ! videoscale add-borders=true ! "
        "video/x-raw,width=1920,height=1080,pixel-aspect-ratio=1/1 ! "
        "imagefreeze ! kmssink sync=false"
    )

    IMAGE_PIPELINE_OTHER = (
        'filesrc location="{path}" ! '
        "decodebin ! videoconvert ! videoscale add-borders=true ! "
        "video/x-raw,width=1920,height=1080,pixel-aspect-ratio=1/1 ! "
        "imagefreeze ! kmssink sync=false"
    )

    DEFAULT_SPLASH_CONFIG = "splash/default.png"

    def __init__(self, base_path: str = "/opt/agora"):
        self.base = Path(base_path)
        self.state_dir = self.base / "state"
        self.assets_dir = self.base / "assets"
        self.desired_path = self.state_dir / "desired.json"
        self.current_path = self.state_dir / "current.json"
        self.splash_config_path = self.state_dir / "splash"

        self.pipeline: Optional[Gst.Pipeline] = None
        self.loop = GLib.MainLoop()
        self.current_desired: Optional[DesiredState] = None
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

    def _teardown(self) -> None:
        if self.pipeline:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None

    def _build_pipeline(self, path: Path, is_video: bool) -> Gst.Pipeline:
        if is_video:
            pipeline_str = self.VIDEO_PIPELINE.format(path=path)
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

        return pipeline

    def _on_eos(self, bus, message) -> None:
        logger.info("EOS received")
        if self.current_desired and self.current_desired.loop:
            # Seamless loop: seek to beginning
            self.pipeline.seek_simple(
                Gst.Format.TIME, Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT, 0
            )
        else:
            logger.info("Playback complete, switching to splash")
            self._show_splash()

    def _on_error(self, bus, message) -> None:
        err, debug = message.parse_error()
        logger.error("Pipeline error: %s (%s)", err.message, debug)
        self._teardown()
        self._update_current(error=err.message)
        # Recover by showing splash after a brief delay
        GLib.timeout_add_seconds(3, self._show_splash)

    # ── Splash ──

    def _show_splash(self) -> bool:
        """Show splash screen. Returns False to cancel GLib timeout repeat."""
        self._teardown()
        splash = self._find_splash()
        if splash:
            is_video = splash.suffix.lower() == ".mp4"
            self.pipeline = self._build_pipeline(splash, is_video)
            self.pipeline.set_state(Gst.State.PLAYING)
            # Loop splash videos
            if is_video:
                self.current_desired = DesiredState(
                    mode=PlaybackMode.SPLASH, loop=True
                )
            self._update_current(mode=PlaybackMode.SPLASH, asset=splash.name)
            logger.info("Showing splash: %s", splash.name)
        else:
            logger.warning("No splash asset found")
            self._update_current(mode=PlaybackMode.STOP)
        return False

    # ── State management ──

    def _update_current(
        self,
        mode: PlaybackMode = PlaybackMode.STOP,
        asset: Optional[str] = None,
        error: Optional[str] = None,
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
            pipeline_state=pipeline_state,
            error=error,
        )
        write_state(self.current_path, state)

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

        # Skip pipeline rebuild if the same content is already playing.
        # A new timestamp alone (e.g. from a CMS re-sync) should not
        # cause a teardown/rebuild — that creates a visible screen flicker.
        if (
            self.current_desired
            and self.pipeline
            and desired.mode == self.current_desired.mode
            and desired.asset == self.current_desired.asset
            and desired.loop == self.current_desired.loop
        ):
            logger.debug("Same content already playing, skipping rebuild")
            self.current_desired = desired
            return

        logger.info("Applying desired state: %s", desired.model_dump_json())
        self.current_desired = desired

        if desired.mode == PlaybackMode.STOP:
            self._show_splash()
            return

        if desired.mode == PlaybackMode.SPLASH:
            self._show_splash()
            return

        if desired.mode == PlaybackMode.PLAY and desired.asset:
            path = self._resolve_asset(desired.asset)
            if not path:
                logger.error("Asset not found: %s", desired.asset)
                self._update_current(error=f"Asset not found: {desired.asset}")
                return
            self._teardown()
            is_video = path.suffix.lower() == ".mp4"
            self.pipeline = self._build_pipeline(path, is_video)
            self.pipeline.set_state(Gst.State.PLAYING)
            self._update_current(mode=PlaybackMode.PLAY, asset=desired.asset)

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
    def _blank_console() -> None:
        """Disable VT console and clear framebuffer so nothing bleeds through during transitions."""
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

            # Clear the framebuffer to black
            fb_path = Path("/dev/fb0")
            if fb_path.exists():
                with open(fb_path, "wb") as fb:
                    # 1920x1080 @ 16bpp = 4,147,200 bytes
                    # Write in chunks to avoid large memory allocation
                    chunk = b"\x00" * 65536
                    total = 1920 * 1080 * 2
                    written = 0
                    while written < total:
                        to_write = min(len(chunk), total - written)
                        fb.write(chunk[:to_write])
                        written += to_write
                logger.info("Cleared framebuffer to black")
        except Exception as e:
            logger.warning("Could not blank console: %s", e)

    def run(self) -> None:
        logger.info("Agora Player starting")
        self.state_dir.mkdir(parents=True, exist_ok=True)

        # Blank the Linux VT console so it doesn't show during transitions
        self._blank_console()

        # Apply initial state
        self.apply_desired()

        # Set up file watcher (inotify preferred, poll fallback)
        if not self._setup_inotify():
            logger.warning("inotify unavailable, falling back to 2s polling")
            GLib.timeout_add_seconds(2, self._poll_state)

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
