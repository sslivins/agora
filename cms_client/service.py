"""CMS WebSocket client service.

Maintains a persistent WebSocket connection to the CMS.
Handles registration, auth token management, state sync, and command execution.
Reconnects automatically on disconnect.
"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import socket
import subprocess
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import websockets

from api.config import Settings
from cms_client.asset_manager import AssetManager
from shared.board import get_cpu_temp, supported_codecs
from shared.models import CurrentState, DesiredState, PlaybackMode
from shared.state import atomic_write, read_state, write_state

logger = logging.getLogger("agora.cms_client")

PROTOCOL_VERSION = 1

# Reconnect backoff: 2s, 4s, 8s, ... capped at 60s
RECONNECT_BASE = 2
RECONNECT_MAX = 60
CONFIG_POLL_INTERVAL = 5  # seconds between CMS config file checks

STATUS_INTERVAL = 30    # seconds between heartbeat status messages
RAPID_STATUS_INTERVAL = 3  # seconds between status messages after a state change
RAPID_STATUS_DURATION = 15 # how long rapid status mode lasts
EVAL_INTERVAL = 15      # seconds between local schedule evaluations
PLAYER_WATCH_INTERVAL = 2  # seconds between player-mode checks for end-of-stream
FETCH_INTERVAL = 60     # seconds between proactive fetch checks
FETCH_LOOKAHEAD_HOURS = 24  # how far ahead to look for missing assets
AUTH_REJECTED_RETRY = 10    # seconds to wait before retrying after auth rejection


class AuthRejectedError(Exception):
    """Raised when the CMS rejects this device's credentials."""


def _get_device_id() -> str:
    """Read the Pi CPU serial number as device identity."""
    from shared.identity import get_device_serial
    serial = get_device_serial()
    if serial == "unknown":
        logger.error("Cannot determine device serial number")
    return serial


def _get_storage_mb(path: Path) -> tuple[int, int]:
    """Return (capacity_mb, used_mb) for the filesystem containing path."""
    try:
        stat = shutil.disk_usage(path)
        return int(stat.total / (1024 * 1024)), int(stat.used / (1024 * 1024))
    except OSError:
        return 0, 0


def _get_cpu_temp() -> float | None:
    """Read CPU temperature. Uses vcgencmd with sysfs fallback for Pi 5."""
    return get_cpu_temp()


def _is_ssh_enabled() -> bool | None:
    """Check if the SSH service is enabled. Returns True/False or None if unknown."""
    try:
        result = subprocess.run(
            ["systemctl", "is-enabled", "ssh"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() == "enabled"
    except (OSError, subprocess.TimeoutExpired):
        return None


def _is_local_api_enabled(persist_dir: Path) -> bool:
    """Check if the local REST API is enabled.

    Returns False only if the flag file explicitly contains 'false'.
    Defaults to True (enabled) when no flag file exists.
    """
    flag_path = persist_dir / "local_api_enabled"
    try:
        return flag_path.read_text().strip().lower() != "false"
    except (FileNotFoundError, OSError):
        return True


def _safe_unlink(path: Path) -> None:
    """Delete a file without raising on missing."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _get_device_type() -> str:
    """Read device model from /proc/device-tree/model (standard on Raspberry Pi)."""
    try:
        return Path("/proc/device-tree/model").read_text().strip().rstrip("\x00")
    except (FileNotFoundError, OSError):
        return ""


def _get_local_ip() -> str:
    """Return the device's LAN IP address by connecting to a remote endpoint."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return ""


def _read_auth_token(path: Path) -> str:
    try:
        return path.read_text().strip()
    except (FileNotFoundError, OSError):
        return ""


def _save_auth_token(path: Path, token: str) -> None:
    atomic_write(path, token)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


# ── Schedule evaluation helpers ──

def _parse_time(s: str) -> tuple[int, int, int]:
    """Parse 'HH:MM' or 'HH:MM:SS' string to (hour, minute, second)."""
    parts = s.split(":")
    sec = int(parts[2]) if len(parts) > 2 else 0
    return int(parts[0]), int(parts[1]), sec


def _schedule_matches_now(entry: dict, now: datetime) -> bool:
    """Check if a schedule entry is active at the given local datetime."""
    start_date = entry.get("start_date")
    if start_date and now.date() < date.fromisoformat(start_date):
        return False
    end_date = entry.get("end_date")
    if end_date and now.date() > date.fromisoformat(end_date):
        return False

    days = entry.get("days_of_week")
    if days and now.isoweekday() not in days:
        return False

    sh, sm, ss = _parse_time(entry["start_time"])
    eh, em, es = _parse_time(entry["end_time"])
    start_secs = sh * 3600 + sm * 60 + ss
    end_secs = eh * 3600 + em * 60 + es
    cur_secs = now.hour * 3600 + now.minute * 60 + now.second

    if start_secs <= end_secs:
        if not (start_secs <= cur_secs < end_secs):
            return False
    else:
        if not (cur_secs >= start_secs or cur_secs < end_secs):
            return False

    return True


def _schedule_starts_within_hours(entry: dict, now: datetime, hours: int) -> bool:
    """Check if a schedule could run within the next N hours (for pre-fetch)."""
    end_date = entry.get("end_date")
    if end_date and now.date() > date.fromisoformat(end_date):
        return False
    start_date = entry.get("start_date")
    lookahead_date = (now + timedelta(hours=hours)).date()
    if start_date and lookahead_date < date.fromisoformat(start_date):
        return False

    days = entry.get("days_of_week")
    if days:
        today_dow = now.isoweekday()
        tomorrow_dow = (now + timedelta(days=1)).isoweekday()
        if today_dow not in days and tomorrow_dow not in days:
            return False

    return True


class CMSClient:
    """WebSocket client that connects to the Agora CMS."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.device_id = _get_device_id()
        self._running = False
        self._ws = None
        self._last_eval_state: tuple | None = None
        self._rapid_until: float = 0  # monotonic deadline for rapid status
        self._current_schedule_id: str | None = None
        self._current_schedule_name: str | None = None
        self._current_asset: str | None = None
        self._eval_wake = asyncio.Event()      # triggers immediate schedule re-eval
        self._last_player_mode: str | None = None
        self.asset_manager = AssetManager(
            manifest_path=settings.manifest_path,
            assets_dir=settings.assets_dir,
            budget_mb=settings.asset_budget_mb,
        )
        # Rebuild manifest from disk on startup (catches manually added/removed files)
        self.asset_manager.rebuild_from_disk(
            settings.videos_dir, settings.images_dir, settings.splash_dir,
        )

    def _get_cms_url(self) -> str:
        try:
            config = json.loads(self.settings.cms_config_path.read_text())
            url = config.get("cms_url", "")
            if url:
                return url
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        return self.settings.cms_url

    def _write_cms_status(
        self,
        state: str,
        error: str = "",
        message: str = "",
        registration: str = "",
    ) -> None:
        """Write CMS connection status to a JSON file for the settings UI.

        ``state`` is one of: connected, connecting, disconnected, error.
        ``registration`` is one of: pending, registered, rejected, or "" (unknown).
        ``message`` is a user-facing coaching message (e.g. how to fix an error).
        """
        status = {
            "state": state,
            "error": error,
            "message": message,
            "registration": registration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            atomic_write(self.settings.cms_status_path, json.dumps(status, indent=2))
        except Exception:
            logger.debug("Failed to write CMS status file", exc_info=True)

    def _apply_timezone(self, tz_name: str) -> None:
        """Set the system timezone if it differs from the CMS value."""
        try:
            result = subprocess.run(
                ["timedatectl", "show", "--property=Timezone", "--value"],
                capture_output=True, text=True, timeout=5,
            )
            current_tz = result.stdout.strip()
            if current_tz == tz_name:
                return
            result = subprocess.run(
                ["sudo", "timedatectl", "set-timezone", tz_name],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                logger.info("System timezone set to %s (was %s)", tz_name, current_tz)
            else:
                logger.warning("Failed to set timezone: %s", result.stderr.strip())
        except Exception:
            logger.debug("Could not apply timezone", exc_info=True)

    async def run(self) -> None:
        """Main loop — connect, communicate, reconnect on failure."""
        cms_url = self._get_cms_url()
        if not cms_url:
            logger.info("No cms_url configured, waiting for CMS discovery…")
            while not cms_url:
                await asyncio.sleep(5)
                cms_url = self._get_cms_url()
            logger.info("CMS URL discovered: %s", cms_url)

        self._running = True
        self._active_cms_url = cms_url
        attempt = 0

        eval_task = asyncio.create_task(self._schedule_eval_loop())
        fetch_task = asyncio.create_task(self._fetch_loop())
        config_task = asyncio.create_task(self._config_watch_loop())
        watch_task = asyncio.create_task(self._player_watch_loop())

        try:
            while self._running:
                try:
                    await self._connect_and_run()
                    attempt = 0
                except AuthRejectedError:
                    logger.warning(
                        "Auth rejected by CMS — waiting %ds before retrying. "
                        "Adopt the device in the CMS to resume.",
                        AUTH_REJECTED_RETRY,
                    )
                    # Don't overwrite the rejection status — keep the coaching message visible
                    await asyncio.sleep(AUTH_REJECTED_RETRY)
                except (
                    websockets.ConnectionClosed,
                    websockets.InvalidURI,
                    websockets.InvalidHandshake,
                    OSError,
                ) as e:
                    attempt += 1
                    delay = min(RECONNECT_BASE * (2 ** (attempt - 1)), RECONNECT_MAX)
                    logger.warning("CMS connection lost (%s), reconnecting in %ds...", e, delay)
                    self._write_cms_status(
                        "disconnected",
                        error=str(e),
                        message=f"Connection lost. Retrying in {delay}s\u2026",
                    )
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    logger.info("CMS client shutting down")
                    break
                except Exception:
                    attempt += 1
                    delay = min(RECONNECT_BASE * (2 ** (attempt - 1)), RECONNECT_MAX)
                    logger.exception("Unexpected CMS client error, reconnecting in %ds...", delay)
                    self._write_cms_status(
                        "disconnected",
                        error="Unexpected error",
                        message=f"An unexpected error occurred. Retrying in {delay}s\u2026",
                    )
                    await asyncio.sleep(delay)
        finally:
            for task in [eval_task, fetch_task, config_task, watch_task]:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _config_watch_loop(self) -> None:
        """Poll cms_config.json for URL changes and trigger reconnect."""
        while self._running:
            await asyncio.sleep(CONFIG_POLL_INTERVAL)
            try:
                new_url = self._get_cms_url()
                if new_url and new_url != self._active_cms_url:
                    logger.info(
                        "CMS URL changed: %s → %s — reconnecting",
                        self._active_cms_url, new_url,
                    )
                    self._active_cms_url = new_url
                    # Clear auth token — the new CMS won't recognise it
                    _save_auth_token(self.settings.auth_token_path, "")
                    self._write_cms_status(
                        "connecting",
                        message="CMS URL changed. Reconnecting\u2026",
                    )
                    if self._ws:
                        await self._ws.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Config watch error", exc_info=True)

    async def _connect_and_run(self) -> None:
        """Single connection lifecycle: connect → register → message loop."""
        cms_url = self._get_cms_url()
        self._active_cms_url = cms_url
        logger.info("Connecting to CMS at %s", cms_url)

        async with websockets.connect(
            cms_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            logger.info("WebSocket connected")

            auth_token = _read_auth_token(self.settings.auth_token_path)
            # Always start as "pending" — actual status comes from the CMS
            # sync message (device_status field).  Don't assume "registered"
            # just because we have a local token; the device may have been
            # deleted and re-created on the CMS as pending.
            self._write_cms_status("connected", registration="pending")
            cap_mb, used_mb = _get_storage_mb(self.settings.assets_dir)

            # Name is "custom" if the user explicitly set it via captive portal
            name_is_custom = self.settings.persist_dir.joinpath("device_name").exists()
            register_msg = {
                "type": "register",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "auth_token": auth_token,
                "firmware_version": self._get_version(),
                "device_name": self.settings.device_name,
                "device_name_custom": name_is_custom,
                "device_type": _get_device_type(),
                "supported_codecs": supported_codecs(),
                "ip_address": _get_local_ip(),
                "storage_capacity_mb": cap_mb,
                "storage_used_mb": used_mb,
            }
            await ws.send(json.dumps(register_msg))
            logger.info("Sent register message (device_id=%s)", self.device_id)

            status_task = asyncio.create_task(self._status_loop(ws))

            try:
                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type")

                    if msg_type == "auth_assigned":
                        await self._handle_auth_assigned(msg)
                    elif msg_type == "sync":
                        await self._handle_sync(msg)
                    elif msg_type == "play":
                        await self._handle_play(msg)
                    elif msg_type == "stop":
                        await self._handle_stop()
                    elif msg_type == "fetch_asset":
                        await self._handle_fetch_asset(msg, ws)
                    elif msg_type == "delete_asset":
                        await self._handle_delete_asset(msg, ws)
                    elif msg_type == "config":
                        await self._handle_config(msg)
                    elif msg_type == "reboot":
                        await self._handle_reboot(ws)
                    elif msg_type == "upgrade":
                        await self._handle_upgrade(ws)
                    elif msg_type == "factory_reset":
                        await self._handle_factory_reset(ws)
                    elif msg_type == "wipe_assets":
                        await self._handle_wipe_assets(msg, ws)
                    elif msg_type == "request_logs":
                        await self._handle_request_logs(msg, ws)
                    elif "error" in msg:
                        error_text = msg["error"]
                        logger.error("CMS error: %s", error_text)
                        if "credentials" in error_text.lower():
                            logger.warning(
                                "Auth rejected — clearing stored token. "
                                "If this device was re-flashed, use 'Reset Auth' "
                                "on the CMS Devices page for device %s",
                                self.device_id,
                            )
                            _save_auth_token(self.settings.auth_token_path, "")
                            self._write_cms_status(
                                "error",
                                error="Authentication rejected",
                                registration="rejected",
                                message=(
                                    "This device's credentials were rejected by the CMS. "
                                    "Go to the CMS Devices page, find this device, "
                                    "and click \u201cAdopt\u201d to re-register it. "
                                    "The device will reconnect automatically."
                                ),
                            )
                        else:
                            self._write_cms_status(
                                "error",
                                error=error_text,
                                message=f"The CMS returned an error: {error_text}",
                            )
                            raise ConnectionError(f"CMS error: {error_text}")
                        raise AuthRejectedError(f"CMS rejected credentials for {self.device_id}")
                    else:
                        logger.warning("Unknown CMS message type: %s", msg_type)
            finally:
                status_task.cancel()
                try:
                    await status_task
                except asyncio.CancelledError:
                    pass

    async def _send_status(self) -> None:
        """Build and send a single status heartbeat."""
        try:
            current_data = json.loads(self.settings.current_state_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            current_data = {}

        _, used_mb = _get_storage_mb(self.settings.assets_dir)

        status_msg = {
            "type": "status",
            "protocol_version": PROTOCOL_VERSION,
            "device_id": self.device_id,
            "mode": current_data.get("mode", "splash"),
            "asset": current_data.get("asset"),
            "pipeline_state": current_data.get("pipeline_state", "NULL"),
            "started_at": current_data.get("started_at"),
            "playback_position_ms": current_data.get("playback_position_ms"),
            "uptime_seconds": int(time.monotonic()),
            "storage_used_mb": used_mb,
            "cpu_temp_c": _get_cpu_temp(),
            "error": current_data.get("error"),
            "error_timestamp": current_data.get("updated_at") if current_data.get("error") else None,
            "ssh_enabled": _is_ssh_enabled(),
            "local_api_enabled": _is_local_api_enabled(self.settings.persist_dir),
            "display_connected": current_data.get("display_connected"),
        }
        await self._ws.send(json.dumps(status_msg))

    async def _status_loop(self, ws) -> None:
        """Send periodic status heartbeats.

        Uses a shorter interval right after a state change so the CMS
        dashboard picks up transitions (e.g. starting → playing) quickly.
        """
        while True:
            if time.monotonic() < self._rapid_until:
                interval = RAPID_STATUS_INTERVAL
            else:
                interval = STATUS_INTERVAL
            await asyncio.sleep(interval)
            try:
                await self._send_status()
            except websockets.ConnectionClosed:
                raise
            except Exception:
                logger.exception("Error sending status heartbeat")

    async def _handle_auth_assigned(self, msg: dict) -> None:
        token = msg.get("device_auth_token", "")
        if token:
            _save_auth_token(self.settings.auth_token_path, token)
            logger.info("Device auth token received and saved")
            # Don't write "registered" here — the device may still be
            # pending on the CMS.  The sync message's device_status field
            # is the authoritative source.

    # ── Sync handling ──

    async def _handle_sync(self, msg: dict) -> None:
        """CMS sent full schedule sync — cache, evaluate, and report status."""
        logger.info("Received sync from CMS (%d schedules)", len(msg.get("schedules", [])))

        # Update registration status from authoritative CMS device_status
        device_status = msg.get("device_status", "")
        if device_status == "adopted":
            self._write_cms_status("connected", registration="registered")
        elif device_status:
            # pending, orphaned, etc. — keep as "pending" for the OOBE
            self._write_cms_status("connected", registration="pending")
        logger.info("SYNC step 1: status written")

        try:
            atomic_write(self.settings.schedule_path, json.dumps(msg, indent=2))
        except Exception:
            logger.exception("Failed to cache schedule.json")
        logger.info("SYNC step 2: schedule.json written")

        tz_name = msg.get("timezone")
        if tz_name:
            self._apply_timezone(tz_name)
        logger.info("SYNC step 3: timezone done")

        # Persist splash config BEFORE evaluate — the player reads the splash
        # config file when desired.json triggers a splash transition (via
        # inotify).  If we wrote desired.json first, the player could read a
        # stale splash config and show the wrong asset.
        await self._persist_splash(msg.get("splash"))
        logger.info("SYNC step 4: splash persist done")

        # Evaluate schedule — writes desired.json to tmpfs, triggering
        # the player immediately via inotify.
        prev_state = self._last_eval_state
        self._evaluate_schedule(msg)

        # Wake the eval loop so it picks up changes immediately
        self._eval_wake.set()
        logger.info("SYNC step 5: evaluate done, eval_wake set")

        # If the schedule evaluation changed desired state, send an immediate
        # status and enter rapid mode so the CMS dashboard picks up the
        # starting → playing transition quickly.
        if self._last_eval_state != prev_state:
            self._rapid_until = time.monotonic() + RAPID_STATUS_DURATION
            await asyncio.sleep(2)  # brief delay for player to start
            try:
                await self._send_status()
            except Exception:
                logger.debug("Failed to send post-sync status", exc_info=True)

    async def _persist_splash(self, splash: str | None) -> None:
        """Write splash config to persistent storage in a background thread.

        Skips the write when the on-disk value already matches, avoiding
        sporadic multi-second NVMe stalls caused by ext4 journal commits.
        When a write is needed, it runs in a thread pool executor so the
        asyncio event loop is not blocked.
        """
        path = self.settings.splash_config_path
        try:
            if splash:
                # Read current value — file is tiny (< 100 bytes), read is fast
                current = ""
                try:
                    current = path.read_text().strip()
                except (OSError, FileNotFoundError):
                    pass
                if current == splash:
                    logger.debug("Splash unchanged (%s), skipping persist", splash)
                    return
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, atomic_write, path, splash)
                logger.info("Splash persisted: %r → %r", current, splash)
            elif path.is_file():
                path.unlink()
                logger.info("Splash config removed")
        except Exception:
            logger.debug("Failed to update splash config", exc_info=True)

    def _evaluate_schedule(self, sync_data: dict) -> None:
        """Evaluate the cached schedule and update desired state."""
        schedules = sync_data.get("schedules", [])
        default_asset = sync_data.get("default_asset")
        tz_name = sync_data.get("timezone", "UTC")
        logger.info("EVAL: default_asset=%s, schedules=%d, last_state=%s",
                     default_asset, len(schedules), self._last_eval_state)

        # Check if the player is in an error state — if so, clear the cache
        # so we re-write desired.json and give the player another chance.
        if self._last_eval_state is not None:
            try:
                current = read_state(self.settings.current_state_path, CurrentState)
                if current.error:
                    logger.info(
                        "Player has error (%s), clearing eval cache to retry",
                        current.error,
                    )
                    self._last_eval_state = None
            except Exception:
                pass  # Can't read current.json — proceed normally

        try:
            from zoneinfo import ZoneInfo
            now_utc = datetime.now(timezone.utc)
            local_now = now_utc.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
        except Exception:
            local_now = datetime.utcnow()

        # Find the highest-priority active schedule
        winner = None
        for entry in schedules:
            if not _schedule_matches_now(entry, local_now):
                continue
            if winner is None or entry.get("priority", 0) > winner.get("priority", 0):
                winner = entry

        if winner:
            asset = winner.get("asset", "")
            checksum = winner.get("asset_checksum")
            loop_count = winner.get("loop_count")
            new_schedule_id = winner.get("id")
            new_schedule_name = winner.get("name", "")

            # Webpage schedule — render URL via Cage+Chromium, no file on disk
            if winner.get("asset_type") == "webpage" and winner.get("url"):
                url = winner["url"]

                # Validate URL scheme — only allow http/https
                from urllib.parse import urlparse
                parsed = urlparse(url)
                if parsed.scheme not in ("http", "https"):
                    logger.warning(
                        "Schedule: ignoring webpage with invalid URL scheme %r: %s",
                        parsed.scheme, url,
                    )
                    return
                # Block loopback/internal addresses (SSRF protection)
                hostname = parsed.hostname or ""
                if hostname in ("localhost", "127.0.0.1", "::1", "0.0.0.0") or hostname.endswith(".local"):
                    logger.warning(
                        "Schedule: ignoring webpage with blocked hostname %r: %s",
                        hostname, url,
                    )
                    return
                # Block URLs pointing to this device's own IP addresses
                try:
                    import socket
                    resolved = socket.gethostbyname(hostname)
                    local_ips = {
                        addr[4][0]
                        for info in socket.getaddrinfo(socket.gethostname(), None)
                        for addr in [info]
                    }
                    local_ips.update(("127.0.0.1", "::1", "0.0.0.0"))
                    if resolved in local_ips:
                        logger.warning(
                            "Schedule: ignoring webpage pointing to this device's IP %s: %s",
                            resolved, url,
                        )
                        return
                except OSError:
                    pass  # DNS resolution failed — let Chromium handle the error

                state_key = ("webpage", url)
                if self._last_eval_state == state_key:
                    return

                self._end_current_playback()

                desired = DesiredState(
                    mode=PlaybackMode.PLAY,
                    asset=url,
                    url=url,
                    loop=False,
                    loop_count=None,
                )
                write_state(self.settings.desired_state_path, desired)
                self._last_eval_state = state_key

                self._current_schedule_id = new_schedule_id
                self._current_schedule_name = new_schedule_name
                self._current_asset = url
                if new_schedule_id:
                    self._send_playback_event(
                        "playback_started", new_schedule_id, new_schedule_name, url,
                    )

                logger.info(
                    "Schedule: rendering webpage %s (priority %d)",
                    url, winner.get("priority", 0),
                )
                return

            # Stream schedule — play URL directly via mpv (HLS, DASH, RTMP, etc.)
            if winner.get("asset_type") == "stream" and winner.get("url"):
                url = winner["url"]

                # Validate URL scheme — allow streaming protocols
                from urllib.parse import urlparse
                parsed = urlparse(url)
                if parsed.scheme not in ("http", "https", "rtmp", "rtmps", "rtsp", "rtsps", "mms", "mmsh"):
                    logger.warning(
                        "Schedule: ignoring stream with invalid URL scheme %r: %s",
                        parsed.scheme, url,
                    )
                    return
                # Block loopback/internal addresses (SSRF protection)
                hostname = parsed.hostname or ""
                if hostname in ("localhost", "127.0.0.1", "::1", "0.0.0.0") or hostname.endswith(".local"):
                    logger.warning(
                        "Schedule: ignoring stream with blocked hostname %r: %s",
                        hostname, url,
                    )
                    return
                # Block URLs pointing to this device's own IP addresses
                try:
                    import socket
                    resolved = socket.gethostbyname(hostname)
                    local_ips = {
                        addr[4][0]
                        for info in socket.getaddrinfo(socket.gethostname(), None)
                        for addr in [info]
                    }
                    local_ips.update(("127.0.0.1", "::1", "0.0.0.0"))
                    if resolved in local_ips:
                        logger.warning(
                            "Schedule: ignoring stream pointing to this device's IP %s: %s",
                            resolved, url,
                        )
                        return
                except OSError:
                    pass  # DNS resolution failed — let mpv handle the error

                state_key = ("stream", url)
                if self._last_eval_state == state_key:
                    return

                self._end_current_playback()

                desired = DesiredState(
                    mode=PlaybackMode.PLAY,
                    asset=url,
                    url=url,
                    asset_type="stream",
                    loop=False,
                    loop_count=None,
                )
                write_state(self.settings.desired_state_path, desired)
                self._last_eval_state = state_key

                self._current_schedule_id = new_schedule_id
                self._current_schedule_name = new_schedule_name
                self._current_asset = url
                if new_schedule_id:
                    self._send_playback_event(
                        "playback_started", new_schedule_id, new_schedule_name, url,
                    )

                logger.info(
                    "Schedule: playing stream %s (priority %d)",
                    url, winner.get("priority", 0),
                )
                return

            if not self.asset_manager.has_asset(asset, checksum):
                # Asset not on device yet — request fetch and show splash
                logger.info(
                    "Schedule: asset %s not on device, requesting fetch", asset,
                )
                self._request_asset_fetch(asset)
                # Don't cache — retry on next eval when asset may have arrived
                if self._last_eval_state != ("waiting", asset, checksum):
                    desired = DesiredState(mode=PlaybackMode.SPLASH)
                    write_state(self.settings.desired_state_path, desired)
                    self._last_eval_state = ("waiting", asset, checksum)
                return

            state_key = ("play", asset, checksum, loop_count)
            if self._last_eval_state == state_key:
                return

            # Schedule changed — send ENDED for previous, STARTED for new
            self._end_current_playback()

            desired = DesiredState(mode=PlaybackMode.PLAY, asset=asset, loop=True, loop_count=loop_count, expected_checksum=checksum)
            write_state(self.settings.desired_state_path, desired)
            self.asset_manager.touch(asset)
            self._last_eval_state = state_key

            self._current_schedule_id = new_schedule_id
            self._current_schedule_name = new_schedule_name
            self._current_asset = asset
            if new_schedule_id:
                self._send_playback_event(
                    "playback_started", new_schedule_id, new_schedule_name, asset,
                )

            logger.info("Schedule: playing %s (priority %d, loop_count=%s)", asset, winner.get("priority", 0), loop_count)
        elif default_asset:
            default_checksum = sync_data.get("default_asset_checksum")

            if not self.asset_manager.has_asset(default_asset, default_checksum):
                logger.info(
                    "Schedule: default asset %s not on device, requesting fetch",
                    default_asset,
                )
                self._request_asset_fetch(default_asset)
                if self._last_eval_state != ("waiting", default_asset, default_checksum):
                    desired = DesiredState(mode=PlaybackMode.SPLASH)
                    write_state(self.settings.desired_state_path, desired)
                    self._last_eval_state = ("waiting", default_asset, default_checksum)
                return

            state_key = ("default", default_asset, default_checksum)
            if self._last_eval_state == state_key:
                return

            # Leaving a scheduled playback → default asset
            self._end_current_playback()

            desired = DesiredState(mode=PlaybackMode.PLAY, asset=default_asset, loop=True, expected_checksum=default_checksum)
            write_state(self.settings.desired_state_path, desired)
            self.asset_manager.touch(default_asset)
            self._last_eval_state = state_key
            logger.info("Schedule: playing default asset %s", default_asset)
        else:
            state_key = ("splash", None)
            if self._last_eval_state == state_key:
                return

            # Leaving a scheduled playback → splash
            self._end_current_playback()

            desired = DesiredState(mode=PlaybackMode.SPLASH)
            write_state(self.settings.desired_state_path, desired)
            self._last_eval_state = state_key
            logger.info("Schedule: no active schedule, showing splash")

    def _end_current_playback(self) -> None:
        """Send PLAYBACK_ENDED for the current schedule, if any."""
        if self._current_schedule_id:
            self._send_playback_event(
                "playback_ended",
                self._current_schedule_id,
                self._current_schedule_name or "",
                self._current_asset or "",
            )
        self._current_schedule_id = None
        self._current_schedule_name = None
        self._current_asset = None

    def _request_asset_fetch(self, asset_name: str) -> None:
        """Fire-and-forget a fetch_request for a missing asset via WebSocket."""
        ws = self._ws
        if not ws:
            return
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return
        msg = json.dumps({
            "type": "fetch_request",
            "protocol_version": PROTOCOL_VERSION,
            "device_id": self.device_id,
            "asset": asset_name,
        })
        loop.create_task(ws.send(msg))

    def _send_playback_event(self, event_type: str, schedule_id: str,
                             schedule_name: str, asset: str) -> None:
        """Fire-and-forget a playback_started or playback_ended event."""
        ws = self._ws
        if not ws:
            logger.warning("Cannot send %s — no WebSocket connection", event_type)
            return
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            logger.warning("Cannot send %s — no event loop", event_type)
            return
        msg = json.dumps({
            "type": event_type,
            "protocol_version": PROTOCOL_VERSION,
            "device_id": self.device_id,
            "schedule_id": schedule_id,
            "schedule_name": schedule_name,
            "asset": asset,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Sending %s for schedule %s (%s)", event_type, schedule_name, asset)

        async def _send():
            try:
                await ws.send(msg)
            except Exception:
                logger.exception("Failed to send %s event", event_type)

        loop.create_task(_send())

    async def _schedule_eval_loop(self) -> None:
        """Local schedule evaluator — re-evaluates on timer or when woken early."""
        while self._running:
            # Wait up to EVAL_INTERVAL, but wake immediately if _eval_wake is set
            self._eval_wake.clear()
            try:
                await asyncio.wait_for(self._eval_wake.wait(), timeout=EVAL_INTERVAL)
            except asyncio.TimeoutError:
                pass  # normal 15s tick
            try:
                data = json.loads(self.settings.schedule_path.read_text())
                self._evaluate_schedule(data)
            except FileNotFoundError:
                pass
            except Exception:
                logger.exception("Error in local schedule evaluator")

    async def _player_watch_loop(self) -> None:
        """Fast poll of current.json to detect player end-of-stream.

        Runs every 2s.  When the player transitions out of "play" mode
        (e.g. loop_count reached, EOS), immediately wakes the eval loop
        so PLAYBACK_ENDED is sent without waiting for the 15s eval tick.
        """
        while self._running:
            await asyncio.sleep(PLAYER_WATCH_INTERVAL)
            try:
                data = json.loads(self.settings.current_state_path.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                data = {}
            mode = data.get("mode", "splash")
            prev = self._last_player_mode
            self._last_player_mode = mode
            if prev == "play" and mode != "play":
                logger.info(
                    "Player stopped (was %s, now %s) — triggering immediate eval",
                    prev, mode,
                )
                self._eval_wake.set()

    async def _fetch_loop(self) -> None:
        """Proactively request missing assets for upcoming schedules."""
        while self._running:
            await asyncio.sleep(FETCH_INTERVAL)
            try:
                await self._check_and_fetch_missing()
            except Exception:
                logger.exception("Error in fetch loop")

    async def _check_and_fetch_missing(self) -> None:
        """Scan schedule for upcoming assets not on disk and request them.
        Also re-fetches assets whose local checksum doesn't match CMS."""
        try:
            data = json.loads(self.settings.schedule_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return

        schedules = data.get("schedules", [])
        default_asset = data.get("default_asset")
        default_asset_checksum = data.get("default_asset_checksum")
        tz_name = data.get("timezone", "UTC")

        try:
            from zoneinfo import ZoneInfo
            now_utc = datetime.now(timezone.utc)
            local_now = now_utc.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
        except Exception:
            local_now = datetime.utcnow()

        # Collect assets needed: (name, expected_checksum) — active first, then upcoming
        needed: list[tuple[str, str | None]] = []
        seen: set[str] = set()
        for entry in schedules:
            asset = entry.get("asset")
            if not asset or asset in seen:
                continue
            checksum = entry.get("asset_checksum")
            if _schedule_matches_now(entry, local_now):
                needed.insert(0, (asset, checksum))
            elif _schedule_starts_within_hours(entry, local_now, FETCH_LOOKAHEAD_HOURS):
                needed.append((asset, checksum))
            seen.add(asset)

        if default_asset and default_asset not in seen:
            needed.append((default_asset, default_asset_checksum))

        ws = self._ws
        if not ws:
            return

        for asset_name, expected_checksum in needed:
            if self.asset_manager.has_asset(asset_name, expected_checksum):
                continue

            if expected_checksum:
                logger.info("Requesting asset: %s (checksum mismatch or missing)", asset_name)
            else:
                logger.info("Requesting missing asset: %s", asset_name)
            try:
                request_msg = {
                    "type": "fetch_request",
                    "protocol_version": PROTOCOL_VERSION,
                    "device_id": self.device_id,
                    "asset": asset_name,
                }
                await ws.send(json.dumps(request_msg))
            except websockets.ConnectionClosed:
                break
            except Exception:
                logger.exception("Error requesting asset %s", asset_name)

    # ── Direct commands ──

    async def _handle_play(self, msg: dict) -> None:
        asset = msg.get("asset", "")
        loop = msg.get("loop", True)
        loop_count = msg.get("loop_count")
        self._end_current_playback()
        desired = DesiredState(mode=PlaybackMode.PLAY, asset=asset, loop=loop, loop_count=loop_count)
        write_state(self.settings.desired_state_path, desired)
        self._last_eval_state = None
        self._rapid_until = time.monotonic() + RAPID_STATUS_DURATION
        logger.info("CMS play command: %s (loop=%s, loop_count=%s)", asset, loop, loop_count)

    async def _handle_stop(self) -> None:
        self._end_current_playback()
        desired = DesiredState(mode=PlaybackMode.SPLASH)
        write_state(self.settings.desired_state_path, desired)
        self._last_eval_state = None
        self._rapid_until = time.monotonic() + RAPID_STATUS_DURATION
        logger.info("CMS stop command: showing splash")

    # ── Asset management ──

    def _read_api_key(self) -> str:
        """Read the current device API key from the persist directory."""
        key_path = self.settings.persist_dir / "api_key"
        try:
            return key_path.read_text().strip()
        except FileNotFoundError:
            return ""

    async def _handle_fetch_asset(self, msg: dict, ws) -> None:
        """CMS tells us to download an asset — with budget-aware eviction."""
        asset_name = msg.get("asset_name", "")
        download_url = msg.get("download_url", "")
        expected_checksum = msg.get("checksum", "")
        expected_size = msg.get("size_bytes", 0)

        if not asset_name or not download_url:
            logger.warning("Invalid fetch_asset message: missing fields")
            return

        # Skip if we already have it with matching checksum
        if self.asset_manager.has_asset(asset_name, expected_checksum):
            logger.info("Asset already cached: %s", asset_name)
            ack = {
                "type": "asset_ack",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "asset_name": asset_name,
                "checksum": expected_checksum,
            }
            await ws.send(json.dumps(ack))
            return

        # Determine scheduled assets (protected during eviction)
        scheduled_assets = self._get_scheduled_asset_names()
        sync_data = self._read_schedule_cache()
        default_asset = sync_data.get("default_asset") if sync_data else None

        # Evict if needed
        if expected_size > 0:
            ok = self.asset_manager.evict_for(expected_size, scheduled_assets, default_asset)
            if not ok:
                logger.error("Cannot fit asset %s (%d bytes): budget=%dMB, available=%dMB",
                             asset_name, expected_size,
                             self.asset_manager.budget_mb,
                             self.asset_manager.available_bytes // (1024 * 1024))
                fail = {
                    "type": "fetch_failed",
                    "protocol_version": PROTOCOL_VERSION,
                    "device_id": self.device_id,
                    "asset": asset_name,
                    "reason": "insufficient_storage",
                    "budget_mb": self.asset_manager.budget_mb,
                    "available_mb": self.asset_manager.available_bytes // (1024 * 1024),
                    "required_mb": expected_size // (1024 * 1024),
                }
                await ws.send(json.dumps(fail))
                return

        logger.info("Fetching asset: %s from %s", asset_name, download_url)

        try:
            import aiohttp

            ext = Path(asset_name).suffix.lower()
            if ext == ".mp4":
                target_dir = self.settings.videos_dir
            elif ext in (".jpg", ".jpeg", ".png"):
                target_dir = self.settings.images_dir
            else:
                target_dir = self.settings.assets_dir

            target_path = target_dir / asset_name

            async with aiohttp.ClientSession() as session:
                headers = {}
                api_key = self._read_api_key()
                if api_key:
                    headers["X-Device-API-Key"] = api_key
                async with session.get(download_url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.error("Failed to download %s: HTTP %d", asset_name, resp.status)
                        return

                    sha256 = hashlib.sha256()
                    tmp_path = target_path.with_suffix(".tmp")
                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(65536):
                            f.write(chunk)
                            sha256.update(chunk)
                        f.flush()
                        os.fsync(f.fileno())

                    actual_checksum = sha256.hexdigest()
                    if expected_checksum and actual_checksum != expected_checksum:
                        logger.error("Checksum mismatch for %s: expected %s, got %s",
                                     asset_name, expected_checksum, actual_checksum)
                        tmp_path.unlink(missing_ok=True)
                        return

                    os.replace(tmp_path, target_path)
                    file_size = target_path.stat().st_size
                    logger.info("Asset downloaded: %s (%d bytes)", asset_name, file_size)

            # Register in manifest
            rel_path = str(target_path.relative_to(self.settings.assets_dir))
            self.asset_manager.register(asset_name, rel_path, file_size, actual_checksum)

            # Re-trigger desired state if player is waiting for this asset
            desired = read_state(self.settings.desired_state_path, DesiredState)
            if desired.asset == asset_name:
                logger.info("Re-applying desired state for just-downloaded asset: %s", asset_name)
                desired.timestamp = datetime.now(timezone.utc)
                write_state(self.settings.desired_state_path, desired)

            ack = {
                "type": "asset_ack",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "asset_name": asset_name,
                "checksum": actual_checksum,
            }
            await ws.send(json.dumps(ack))

        except Exception:
            logger.exception("Error fetching asset %s", asset_name)

    async def _handle_delete_asset(self, msg: dict, ws) -> None:
        asset_name = msg.get("asset_name", "")
        if not asset_name:
            return

        self.asset_manager.remove(asset_name)

        # Also check disk directly in case it wasn't in manifest
        for d in [self.settings.videos_dir, self.settings.images_dir, self.settings.splash_dir]:
            target = d / asset_name
            if target.exists():
                target.unlink()
                break

        ack = {
            "type": "asset_deleted",
            "protocol_version": PROTOCOL_VERSION,
            "device_id": self.device_id,
            "asset_name": asset_name,
        }
        await ws.send(json.dumps(ack))

    # ── Config ──

    async def _handle_config(self, msg: dict) -> None:
        if "splash" in msg and msg["splash"]:
            await self._persist_splash(msg["splash"])
            logger.info("Splash updated to: %s", msg["splash"])

        if "device_name" in msg and msg["device_name"]:
            logger.info("Device name updated to: %s (requires restart)", msg["device_name"])

        if "web_password" in msg and msg["web_password"]:
            new_password = msg["web_password"]
            override_path = self.settings.state_dir / "web_password"
            atomic_write(override_path, new_password)
            try:
                os.chmod(override_path, 0o644)
            except OSError:
                pass
            boot_config = Path("/boot/agora-config.json")
            try:
                cfg = json.loads(boot_config.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                cfg = {}
            cfg["web_password"] = new_password
            atomic_write(boot_config, json.dumps(cfg, indent=2))
            logger.info("Web UI password updated")

        if "api_key" in msg and msg["api_key"]:
            new_key = msg["api_key"]
            override_path = self.settings.persist_dir / "api_key"
            atomic_write(override_path, new_key)
            try:
                os.chmod(override_path, 0o644)
            except OSError:
                pass
            boot_config = Path("/boot/agora-config.json")
            try:
                cfg = json.loads(boot_config.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                cfg = {}
            cfg["api_key"] = new_key
            atomic_write(boot_config, json.dumps(cfg, indent=2))
            logger.info("API key updated")

        if "ssh_enabled" in msg and msg["ssh_enabled"] is not None:
            enabled = msg["ssh_enabled"]
            if enabled:
                os.system("sudo systemctl enable ssh")
                os.system("sudo systemctl start ssh")
                logger.info("SSH enabled by CMS")
            else:
                os.system("sudo systemctl stop ssh")
                os.system("sudo systemctl disable ssh")
                logger.info("SSH disabled by CMS")

        if "local_api_enabled" in msg and msg["local_api_enabled"] is not None:
            enabled = msg["local_api_enabled"]
            flag_path = self.settings.persist_dir / "local_api_enabled"
            atomic_write(flag_path, "true" if enabled else "false")
            try:
                os.chmod(flag_path, 0o644)
            except OSError:
                pass
            logger.info("Local API %s by CMS", "enabled" if enabled else "disabled")

    async def _handle_reboot(self, ws) -> None:
        logger.info("Reboot requested by CMS")
        try:
            await ws.send(json.dumps({"type": "reboot_ack"}))
        except Exception:
            pass
        await asyncio.sleep(1)
        os.system("sudo reboot")

    async def _handle_factory_reset(self, ws) -> None:
        """Factory reset: wipe all data and reboot into AP mode.

        Reuses the same cleanup logic as the REST endpoint in
        api/routers/system.py but runs from the CMS client context.
        """
        logger.warning("Factory reset requested by CMS")
        try:
            await ws.send(json.dumps({"type": "factory_reset_ack"}))
        except Exception:
            pass

        persist_dir = self.settings.persist_dir
        state_dir = self.settings.state_dir

        # Remove provisioning flag
        _safe_unlink(persist_dir / "provisioned")

        # Remove CMS config and auth
        _safe_unlink(persist_dir / "cms_config.json")
        _safe_unlink(persist_dir / "cms_auth_token")
        _safe_unlink(persist_dir / "device_name")
        _safe_unlink(persist_dir / "api_key")
        _safe_unlink(persist_dir / "local_api_enabled")

        # Remove state files
        _safe_unlink(state_dir / "cms_status.json")
        _safe_unlink(state_dir / "cms_config.json")
        _safe_unlink(state_dir / "schedule.json")
        _safe_unlink(state_dir / "assets.json")

        # Wipe assets on disk
        for subdir in [self.settings.videos_dir, self.settings.images_dir]:
            if subdir.exists():
                for f in subdir.iterdir():
                    if f.is_file():
                        try:
                            f.unlink()
                        except OSError:
                            pass

        # Forget all saved Wi-Fi connections
        try:
            from provision.network import forget_all_wifi
            forget_all_wifi()
        except ImportError:
            try:
                result = subprocess.run(
                    ["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"],
                    capture_output=True, text=True, timeout=10,
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().splitlines():
                        parts = line.split(":")
                        if len(parts) >= 2 and parts[1] == "802-11-wireless":
                            subprocess.run(
                                ["nmcli", "connection", "delete", parts[0]],
                                capture_output=True, timeout=10,
                            )
            except (subprocess.SubprocessError, FileNotFoundError):
                pass

        logger.warning("Factory reset complete — rebooting")
        await asyncio.sleep(1)
        os.system("sudo reboot")

    async def _handle_wipe_assets(self, msg: dict, ws) -> None:
        """Wipe all cached assets and schedule state.

        Sent by the CMS on adoption or device deletion so the device starts
        clean.  Unlike factory_reset, this preserves provisioning, Wi-Fi, and
        auth credentials — the device stays connected and re-syncs immediately.
        """
        reason = msg.get("reason", "unknown")
        logger.warning("Wipe assets requested by CMS (reason: %s)", reason)

        state_dir = self.settings.state_dir

        # Clear schedule and asset manifest
        _safe_unlink(state_dir / "schedule.json")
        _safe_unlink(state_dir / "assets.json")

        # Wipe asset files on disk
        for subdir in [self.settings.videos_dir, self.settings.images_dir]:
            if subdir.exists():
                for f in subdir.iterdir():
                    if f.is_file():
                        try:
                            f.unlink()
                        except OSError:
                            pass

        # Reinitialise the asset manager so in-memory state matches disk
        self.asset_manager.rebuild_from_disk(
            self.settings.videos_dir, self.settings.images_dir, self.settings.splash_dir,
        )

        logger.info("Asset wipe complete (reason: %s)", reason)

        try:
            await ws.send(json.dumps({
                "type": "wipe_assets_ack",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "reason": reason,
            }))
        except Exception:
            pass

    async def _handle_request_logs(self, msg: dict, ws) -> None:
        """Collect journalctl logs for requested services and send back."""
        request_id = msg.get("request_id", "")
        services = msg.get("services") or [
            "agora-player", "agora-api", "agora-cms-client", "agora-provision",
        ]
        since = msg.get("since", "24h")
        logger.info("Log request %s: services=%s since=%s", request_id, services, since)

        logs: dict[str, str] = {}
        error = None

        for service in services:
            try:
                result = subprocess.run(
                    ["journalctl", "-u", service, f"--since={since} ago",
                     "--no-pager", "-o", "short-iso"],
                    capture_output=True, text=True, timeout=30,
                )
                logs[service] = result.stdout if result.returncode == 0 else (
                    result.stderr or f"journalctl exited with code {result.returncode}"
                )
            except subprocess.TimeoutExpired:
                logs[service] = f"[timed out reading logs for {service}]"
            except FileNotFoundError:
                error = "journalctl not available on this device"
                break
            except Exception as e:
                logs[service] = f"[error: {e}]"

        try:
            await ws.send(json.dumps({
                "type": "logs_response",
                "protocol_version": PROTOCOL_VERSION,
                "request_id": request_id,
                "device_id": self.device_id,
                "logs": logs,
                "error": error,
            }))
        except Exception:
            logger.exception("Failed to send logs response")

    async def _handle_upgrade(self, ws) -> None:
        logger.info("Upgrade requested by CMS")
        try:
            await ws.send(json.dumps({"type": "upgrade_ack"}))
        except Exception:
            pass
        # Run the upgrade in a separate systemd scope so it survives the
        # CMS client service restart triggered by the package's postinst.
        # Without this, systemd kills the upgrade commands (same cgroup)
        # when postinst calls 'systemctl restart agora-cms-client', and
        # the reboot never happens.
        #
        # Key details:
        # - Acquire::http::No-Cache bypasses GitHub Pages CDN stale metadata
        # - Version is compared before/after to only reboot on actual upgrade
        subprocess.Popen(
            ["systemd-run", "--scope",
             "bash", "-c",
             "dpkg --configure -a; "
             "OLD=$(dpkg-query -W -f='${Version}' agora 2>/dev/null); "
             "apt-get -o Acquire::http::No-Cache=True update -qq "
             "&& apt-get install -y agora; "
             "NEW=$(dpkg-query -W -f='${Version}' agora 2>/dev/null); "
             '[ "$OLD" != "$NEW" ] && reboot'],
        )

    # ── Helpers ──

    def _get_version(self) -> str:
        try:
            from api import __version__
            return __version__
        except ImportError:
            return "unknown"

    def _read_schedule_cache(self) -> dict | None:
        try:
            return json.loads(self.settings.schedule_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _get_scheduled_asset_names(self) -> set[str]:
        """Get all asset names from the cached schedule."""
        data = self._read_schedule_cache()
        if not data:
            return set()
        names = set()
        for entry in data.get("schedules", []):
            asset = entry.get("asset")
            if asset:
                names.add(asset)
        return names
