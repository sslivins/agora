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
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import websockets

from api.config import Settings
from cms_client.asset_manager import AssetManager
from shared.models import DesiredState, PlaybackMode
from shared.state import atomic_write, read_state, write_state

logger = logging.getLogger("agora.cms_client")

PROTOCOL_VERSION = 1

# Reconnect backoff: 2s, 4s, 8s, ... capped at 60s
RECONNECT_BASE = 2
RECONNECT_MAX = 60

STATUS_INTERVAL = 30    # seconds between heartbeat status messages
EVAL_INTERVAL = 15      # seconds between local schedule evaluations
FETCH_INTERVAL = 60     # seconds between proactive fetch checks
FETCH_LOOKAHEAD_HOURS = 24  # how far ahead to look for missing assets


def _get_device_id() -> str:
    """Read the Pi CPU serial number as device identity."""
    try:
        with open("/sys/firmware/devicetree/base/serial-number") as f:
            return f.read().strip().strip("\x00")
    except OSError:
        pass
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("Serial"):
                return line.split(":")[1].strip()
    except OSError:
        pass
    logger.error("Cannot determine device serial number")
    return "unknown"


def _get_storage_mb(path: Path) -> tuple[int, int]:
    """Return (capacity_mb, used_mb) for the filesystem containing path."""
    try:
        stat = shutil.disk_usage(path)
        return int(stat.total / (1024 * 1024)), int(stat.used / (1024 * 1024))
    except OSError:
        return 0, 0


def _get_device_type() -> str:
    """Read device model from /proc/device-tree/model (standard on Raspberry Pi)."""
    try:
        return Path("/proc/device-tree/model").read_text().strip().rstrip("\x00")
    except (FileNotFoundError, OSError):
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

def _parse_time(s: str) -> tuple[int, int]:
    """Parse 'HH:MM' string to (hour, minute)."""
    parts = s.split(":")
    return int(parts[0]), int(parts[1])


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

    sh, sm = _parse_time(entry["start_time"])
    eh, em = _parse_time(entry["end_time"])
    start_mins = sh * 60 + sm
    end_mins = eh * 60 + em
    cur_mins = now.hour * 60 + now.minute

    if start_mins <= end_mins:
        if not (start_mins <= cur_mins < end_mins):
            return False
    else:
        if not (cur_mins >= start_mins or cur_mins < end_mins):
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

    async def run(self) -> None:
        """Main loop — connect, communicate, reconnect on failure."""
        cms_url = self._get_cms_url()
        if not cms_url:
            logger.info("No cms_url configured, CMS client disabled")
            return

        self._running = True
        attempt = 0

        eval_task = asyncio.create_task(self._schedule_eval_loop())
        fetch_task = asyncio.create_task(self._fetch_loop())

        try:
            while self._running:
                try:
                    await self._connect_and_run()
                    attempt = 0
                except (
                    websockets.ConnectionClosed,
                    websockets.InvalidURI,
                    websockets.InvalidHandshake,
                    OSError,
                ) as e:
                    attempt += 1
                    delay = min(RECONNECT_BASE * (2 ** (attempt - 1)), RECONNECT_MAX)
                    logger.warning("CMS connection lost (%s), reconnecting in %ds...", e, delay)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    logger.info("CMS client shutting down")
                    break
                except Exception:
                    attempt += 1
                    delay = min(RECONNECT_BASE * (2 ** (attempt - 1)), RECONNECT_MAX)
                    logger.exception("Unexpected CMS client error, reconnecting in %ds...", delay)
                    await asyncio.sleep(delay)
        finally:
            for task in [eval_task, fetch_task]:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_and_run(self) -> None:
        """Single connection lifecycle: connect → register → message loop."""
        cms_url = self._get_cms_url()
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
            cap_mb, used_mb = _get_storage_mb(self.settings.assets_dir)

            register_msg = {
                "type": "register",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "auth_token": auth_token,
                "firmware_version": self._get_version(),
                "device_type": _get_device_type(),
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
                    elif "error" in msg:
                        logger.error("CMS error: %s", msg["error"])
                        return
                    else:
                        logger.warning("Unknown CMS message type: %s", msg_type)
            finally:
                status_task.cancel()
                try:
                    await status_task
                except asyncio.CancelledError:
                    pass

    async def _status_loop(self, ws) -> None:
        """Send periodic status heartbeats."""
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            try:
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
                    "uptime_seconds": int(time.monotonic()),
                    "storage_used_mb": used_mb,
                }
                await ws.send(json.dumps(status_msg))
            except websockets.ConnectionClosed:
                raise
            except Exception:
                logger.exception("Error sending status heartbeat")

    async def _handle_auth_assigned(self, msg: dict) -> None:
        token = msg.get("device_auth_token", "")
        if token:
            _save_auth_token(self.settings.auth_token_path, token)
            logger.info("Device auth token received and saved")

    # ── Sync handling ──

    async def _handle_sync(self, msg: dict) -> None:
        """CMS sent full schedule sync — cache and evaluate."""
        logger.info("Received sync from CMS (%d schedules)", len(msg.get("schedules", [])))

        try:
            atomic_write(self.settings.schedule_path, json.dumps(msg, indent=2))
        except Exception:
            logger.exception("Failed to cache schedule.json")

        # Reset eval state so next evaluation applies immediately
        self._last_eval_state = None
        self._evaluate_schedule(msg)

    def _evaluate_schedule(self, sync_data: dict) -> None:
        """Evaluate the cached schedule and update desired state."""
        schedules = sync_data.get("schedules", [])
        default_asset = sync_data.get("default_asset")
        tz_name = sync_data.get("timezone", "UTC")

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
            state_key = ("play", asset)
            if self._last_eval_state == state_key:
                return
            desired = DesiredState(mode=PlaybackMode.PLAY, asset=asset, loop=True)
            write_state(self.settings.desired_state_path, desired)
            self.asset_manager.touch(asset)
            self._last_eval_state = state_key
            logger.info("Schedule: playing %s (priority %d)", asset, winner.get("priority", 0))
        elif default_asset:
            state_key = ("default", default_asset)
            if self._last_eval_state == state_key:
                return
            desired = DesiredState(mode=PlaybackMode.PLAY, asset=default_asset, loop=True)
            write_state(self.settings.desired_state_path, desired)
            self.asset_manager.touch(default_asset)
            self._last_eval_state = state_key
            logger.info("Schedule: playing default asset %s", default_asset)
        else:
            state_key = ("splash", None)
            if self._last_eval_state == state_key:
                return
            desired = DesiredState(mode=PlaybackMode.SPLASH)
            write_state(self.settings.desired_state_path, desired)
            self._last_eval_state = state_key
            logger.info("Schedule: no active schedule, showing splash")

    async def _schedule_eval_loop(self) -> None:
        """Local schedule evaluator — re-evaluates cached schedule every 15s."""
        while self._running:
            await asyncio.sleep(EVAL_INTERVAL)
            try:
                data = json.loads(self.settings.schedule_path.read_text())
                self._evaluate_schedule(data)
            except FileNotFoundError:
                pass
            except Exception:
                logger.exception("Error in local schedule evaluator")

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
        desired = DesiredState(mode=PlaybackMode.PLAY, asset=asset, loop=loop)
        write_state(self.settings.desired_state_path, desired)
        self._last_eval_state = None
        logger.info("CMS play command: %s (loop=%s)", asset, loop)

    async def _handle_stop(self) -> None:
        desired = DesiredState(mode=PlaybackMode.SPLASH)
        write_state(self.settings.desired_state_path, desired)
        self._last_eval_state = None
        logger.info("CMS stop command: showing splash")

    # ── Asset management ──

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
                async with session.get(download_url) as resp:
                    if resp.status != 200:
                        logger.error("Failed to download %s: HTTP %d", asset_name, resp.status)
                        return

                    sha256 = hashlib.sha256()
                    tmp_path = target_path.with_suffix(".tmp")
                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(65536):
                            f.write(chunk)
                            sha256.update(chunk)

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
            atomic_write(self.settings.splash_config_path, msg["splash"])
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
            override_path = self.settings.state_dir / "api_key"
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

    async def _handle_reboot(self, ws) -> None:
        logger.info("Reboot requested by CMS")
        try:
            await ws.send(json.dumps({"type": "reboot_ack"}))
        except Exception:
            pass
        await asyncio.sleep(1)
        os.system("sudo reboot")

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
