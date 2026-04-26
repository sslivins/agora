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

try:
    import aiohttp
except ImportError:  # pragma: no cover - aiohttp is optional at import time
    aiohttp = None  # type: ignore[assignment]

from api.config import Settings
from cms_client.asset_manager import AssetManager
from cms_client.transport import TransportError, _derive_api_base, open_transport
from shared.board import get_cpu_temp, supported_codecs
from shared.models import CurrentState, DesiredState, PlaybackMode
from shared.state import atomic_write, read_state, write_state

logger = logging.getLogger("agora.cms_client")

PROTOCOL_VERSION = 2

# Firmware-advertised capability flags consumed by the CMS to gate
# features that require specific firmware behaviour. The CMS persists
# these on the device row and rejects schedule create/updates that
# would push assets the device can't render.
#
# - "slideshow_v1": this firmware understands ``asset_type=slideshow``
#   on FETCH_ASSET messages, fetches the manifest+slides, and stores
#   them under ``assets/slideshows/<name>/``.
DEVICE_CAPABILITIES = ["slideshow_v1"]

# Bootstrap v2 renewal policy (issue #420 stage B.3).
# Minimum delay between JWT refreshes on the renewal task, so a clock
# skew or an early-expired JWT doesn't spin at full speed.
JWT_REFRESH_MIN_INTERVAL = 30  # seconds
# 401s from /connect-token can mean many things (bad sig, stale nonce,
# clock skew, revoked pubkey).  Don't treat the first 401 as terminal.
# Only clear adopted state after this many consecutive 401s.
JWT_REFRESH_401_MAX = 3
# Backoff after a non-success renewal attempt before retrying.
JWT_REFRESH_RETRY_SEC = 60

# Log request transport:
# - Small payloads (<= LOGS_JSON_MAX_BYTES) ride the WS as a single
#   ``logs_response`` JSON message (legacy path, unchanged).
# - Large payloads are gzipped to a tarball and HTTP-POSTed straight
#   to the CMS at ``/api/devices/{device_id}/logs/{request_id}/upload``.
#   This path was introduced to replace the Stage-3c chunked binary
#   (LGCK) frames, which WPS transport rejects — ``ws.send(bytes)``
#   is not supported under Web PubSub.
# ~900 KiB JSON fits safely under the 1 MiB WPS ceiling after the
# WS/WPS framing overhead.  Anything larger takes the HTTP upload path.
LOGS_JSON_MAX_BYTES = 900_000
# Maximum compressed upload size the CMS accepts (matches the CMS
# ``MAX_UPLOAD_BYTES`` constant in ``cms/routers/log_requests.py``).
LOGS_UPLOAD_MAX_BYTES = 22_020_096  # 21 MiB
# HTTP timeout for the log-upload POST.  Pi-side journal tarballs are
# tens of MB at most over a local network.
LOGS_UPLOAD_TIMEOUT = 60.0

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


def _build_logs_tar_gz(logs: dict[str, str]) -> bytes:
    """Pack per-service log text into a gzipped tar archive.

    Mirrors the layout the CMS-side shim produces for the legacy
    ``LOGS_RESPONSE`` JSON path — one ``<service>.log`` entry per
    service, UTF-8 encoded.  The CMS assembler writes the result to
    blob storage untouched; consumers (the dashboard download link)
    pull it straight through.
    """
    import io
    import tarfile
    import time

    buf = io.BytesIO()
    mtime = int(time.time())
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for service_name, log_text in logs.items():
            safe_name = service_name.replace("/", "_").replace("\\", "_")
            data = (log_text or "").encode("utf-8")
            info = tarfile.TarInfo(name=f"{safe_name}.log")
            info.size = len(data)
            info.mtime = mtime
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


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


def _resolve_device_api_key(settings: Settings) -> str:
    """Return the device API key used for WPS transport auth.

    Prefers ``AGORA_DEVICE_API_KEY`` (dev/test override);
    otherwise reads ``<persist_dir>/api_key`` — the same file CMS
    rotates into via the config message and that direct-mode
    transport uses for asset downloads.
    """
    key = getattr(settings, "device_api_key", "") or ""
    if key:
        return key.strip()
    key_path = settings.persist_dir / "api_key"
    try:
        return key_path.read_text().strip()
    except (FileNotFoundError, OSError):
        return ""


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
        # In-flight asset fetch tasks, keyed by asset_name. Fetch handlers run
        # as background tasks so the WS read loop isn't blocked by multi-minute
        # downloads (tracked in agora#136). Downloads are serialized by
        # ``_fetch_lock`` so AssetManager eviction math stays correct.
        self._fetch_tasks: dict[str, asyncio.Task] = {}
        self._fetch_lock = asyncio.Lock()
        # Bootstrap v2 state (only populated when settings.bootstrap_v2 is true)
        self._bootstrap_identity = None  # shared.bootstrap_identity.DeviceIdentity
        self._bootstrap_pairing_secret: str | None = None
        self._jwt_refresh_401_count: int = 0
        # Per-connect cancel signal for first-boot polling; set by
        # _config_watch_loop when cms_url changes or by stop().
        self._bootstrap_poll_cancel: asyncio.Event | None = None
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

    def _on_bootstrap_pending_registered(self) -> None:
        """Bootstrap-v2 callback: pending row registered, awaiting adoption.

        Fired once after ``register_once`` succeeds in
        ``bootstrap_boot.ensure_wps_credentials`` (and again on
        re-registration after a pending-row reap).  Publishes
        ``connected/pending`` so provision's ``_wait_for_cms_adoption``
        can switch the device's display from the spinner to the
        pairing-QR adoption screen — even though the WebSocket isn't
        actually open yet.
        """
        self._write_cms_status("connected", registration="pending")

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
                    TransportError,
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
        # Abort any in-flight bootstrap v2 first-boot poll.
        cancel_ev = getattr(self, "_bootstrap_poll_cancel", None)
        if cancel_ev is not None:
            cancel_ev.set()
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
                    # Bootstrap v2 cached WPS JWT was minted by the old
                    # CMS (different signer/base); it's useless now.
                    # Identity keypair stays — the pubkey is still ours.
                    if self._bootstrap_v2_enabled():
                        try:
                            from cms_client import bootstrap_boot
                            bootstrap_boot.clear_state(
                                self.settings.bootstrap_state_path,
                            )
                        except Exception:
                            logger.debug(
                                "Failed to clear bootstrap_state on CMS URL change",
                                exc_info=True,
                            )
                    self._write_cms_status(
                        "connecting",
                        message="CMS URL changed. Reconnecting\u2026",
                    )
                    # Signal any in-flight bootstrap v2 first-boot poll to
                    # abort — otherwise it keeps polling the old CMS for
                    # minutes despite our URL change.
                    cancel_ev = getattr(self, "_bootstrap_poll_cancel", None)
                    if cancel_ev is not None:
                        cancel_ev.set()
                    if self._ws:
                        await self._ws.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Config watch error", exc_info=True)

    def _bootstrap_v2_enabled(self) -> bool:
        return bool(getattr(self.settings, "bootstrap_v2", False))

    async def _mint_wps_credentials_v2(self, cms_url: str):
        """Bootstrap v2: obtain (pre_minted_url, pre_minted_token) via HTTPS.

        Returns the :class:`BootstrapCredentials` and the
        ``aiohttp.ClientSession`` used to obtain them (the session is
        kept open for later renewal calls; caller is responsible for
        closing it).
        """
        import aiohttp
        from cms_client import bootstrap_boot

        # Lazy-load identity so this runs only when the flag is on.
        if self._bootstrap_identity is None or self._bootstrap_pairing_secret is None:
            identity, secret = bootstrap_boot.ensure_identity(
                device_key_path=self.settings.device_key_path,
                pairing_secret_path=self.settings.pairing_secret_path,
            )
            self._bootstrap_identity = identity
            self._bootstrap_pairing_secret = secret

        api_base = (
            getattr(self.settings, "cms_api_url", "") or _derive_api_base(cms_url)
        )

        fleet_secret_hex = getattr(self.settings, "fleet_secret_hex", "") or ""
        try:
            fleet_secret = bytes.fromhex(fleet_secret_hex) if fleet_secret_hex else b""
        except ValueError as e:
            raise TransportError(
                f"AGORA_FLEET_SECRET_HEX is not valid hex: {e}"
            ) from e
        fleet_id = getattr(self.settings, "fleet_id", "") or ""

        metadata = {
            "firmware_version": self._get_version(),
            "device_type": _get_device_type(),
            "device_name": self.settings.device_name,
            "local_ip": _get_local_ip(),
        }

        session = aiohttp.ClientSession()
        try:
            creds = await bootstrap_boot.ensure_wps_credentials(
                session,
                cms_api_base=api_base,
                device_id=self.device_id,
                identity=self._bootstrap_identity,
                pairing_secret=self._bootstrap_pairing_secret,
                state_path=self.settings.bootstrap_state_path,
                fleet_id=fleet_id,
                fleet_secret=fleet_secret,
                metadata=metadata,
                poll_cancel_event=self._bootstrap_poll_cancel,
                on_pending_registered=self._on_bootstrap_pending_registered,
            )
        except Exception:
            await session.close()
            raise
        return creds, session, api_base

    async def _connect_and_run(self) -> None:
        """Single connection lifecycle: connect → register → message loop."""
        cms_url = self._get_cms_url()
        self._active_cms_url = cms_url
        transport_mode = (getattr(self.settings, "cms_transport", "") or "direct").lower()
        logger.info(
            "Connecting to CMS at %s (transport=%s)", cms_url, transport_mode,
        )

        api_key = ""
        pre_minted_url = ""
        pre_minted_token = ""
        pre_minted_expires_at = ""
        http_session = None
        bootstrap_api_base = ""

        bootstrap_v2 = self._bootstrap_v2_enabled() and transport_mode == "wps"

        if bootstrap_v2:
            logger.info("Bootstrap v2 enabled — obtaining WPS JWT via HTTPS")
            # Fresh cancel event per connect attempt — allows
            # _config_watch_loop and stop() to interrupt slow first-boot
            # polling when cms_url changes or shutdown is requested.
            self._bootstrap_poll_cancel = asyncio.Event()
            # Publish a fresh "connecting" status BEFORE the (potentially
            # slow) bootstrap-v2 HTTPS handshake.  This overwrites any
            # stale "disconnected/error" entry from a previous run so
            # provision's _wait_for_cms_adoption doesn't latch onto it.
            self._write_cms_status(
                "connecting",
                message="Registering with CMS\u2026",
            )
            try:
                creds, http_session, bootstrap_api_base = (
                    await self._mint_wps_credentials_v2(cms_url)
                )
            except Exception as e:
                # Map orchestration errors into TransportError so the outer
                # reconnect loop backs off uniformly.
                raise TransportError(f"bootstrap v2 failed: {e!r}") from e
            pre_minted_url = creds.wps_url
            pre_minted_token = creds.wps_jwt
            pre_minted_expires_at = creds.expires_at
        elif transport_mode == "wps":
            api_key = _resolve_device_api_key(self.settings)
            if not api_key:
                raise TransportError(
                    "AGORA_CMS_TRANSPORT=wps requires AGORA_DEVICE_API_KEY "
                    "or a populated <persist_dir>/api_key file"
                )

        try:
            transport = await open_transport(
                mode=transport_mode,
                cms_url=cms_url,
                device_id=self.device_id,
                api_key=api_key,
                api_base=getattr(self.settings, "cms_api_url", "") or None,
                pre_minted_url=pre_minted_url,
                pre_minted_token=pre_minted_token,
            )
        except Exception:
            if http_session is not None:
                await http_session.close()
            raise

        async with transport as ws:
            self._ws = ws
            logger.info("WebSocket connected (transport=%s)", transport_mode)

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
                "capabilities": list(DEVICE_CAPABILITIES),
                "ip_address": _get_local_ip(),
                "storage_capacity_mb": cap_mb,
                "storage_used_mb": used_mb,
            }
            await ws.send(json.dumps(register_msg))
            logger.info("Sent register message (device_id=%s)", self.device_id)

            status_task = asyncio.create_task(self._status_loop(ws))
            renewal_task: asyncio.Task | None = None
            if bootstrap_v2:
                renewal_task = asyncio.create_task(
                    self._jwt_renewal_loop(
                        ws, http_session, bootstrap_api_base,
                        pre_minted_expires_at,
                    )
                )

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
                        self._spawn_fetch_asset(msg, ws)
                    elif msg_type == "delete_asset":
                        await self._cancel_fetch(msg.get("asset_name", ""))
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
                        await self._cancel_all_fetches()
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
                await self._cancel_all_fetches()
                status_task.cancel()
                try:
                    await status_task
                except asyncio.CancelledError:
                    pass
                if renewal_task is not None:
                    renewal_task.cancel()
                    try:
                        await renewal_task
                    except asyncio.CancelledError:
                        pass
                if http_session is not None:
                    await http_session.close()

    async def _jwt_renewal_loop(
        self, ws, session, api_base: str, expires_at: str,
    ) -> None:
        """Background: refresh the WPS JWT before it expires, then reconnect.

        Policy:

        * Sleep until ``expires_at - jwt_refresh_lead_seconds``.
        * On success: persist new state, close ``ws`` to force a
          reconnect in the outer loop (which will read the new state).
        * On :class:`ConnectTokenRejectedError` (401): do NOT clear
          adopted state on the first hit — this often means clock skew,
          replayed nonce, or a transient CMS glitch.  Only clear after
          :data:`JWT_REFRESH_401_MAX` consecutive 401s.
        * On 429 / transport error: back off and retry.
        """
        from cms_client import bootstrap_boot

        try:
            from datetime import timedelta
            lead = int(getattr(self.settings, "jwt_refresh_lead_seconds", 600) or 600)
            current_expires_at = expires_at
            while self._running:
                expires_dt = bootstrap_boot._parse_expires_at(current_expires_at)
                if expires_dt is None:
                    logger.warning(
                        "JWT expires_at unparseable (%r) — skipping renewal",
                        current_expires_at,
                    )
                    return
                now_dt = datetime.now(timezone.utc)
                sleep_seconds = (
                    expires_dt - now_dt - timedelta(seconds=lead)
                ).total_seconds()
                if sleep_seconds < JWT_REFRESH_MIN_INTERVAL:
                    sleep_seconds = JWT_REFRESH_MIN_INTERVAL
                logger.debug("JWT renewal sleeping %.0fs", sleep_seconds)
                await asyncio.sleep(sleep_seconds)

                try:
                    new_creds = await bootstrap_boot.refresh_wps_jwt(
                        session,
                        cms_api_base=api_base,
                        identity=self._bootstrap_identity,
                        state_path=self.settings.bootstrap_state_path,
                    )
                except bootstrap_boot.ConnectTokenRejectedError:
                    self._jwt_refresh_401_count += 1
                    logger.warning(
                        "WPS JWT refresh rejected (401) — consecutive=%d",
                        self._jwt_refresh_401_count,
                    )
                    if self._jwt_refresh_401_count >= JWT_REFRESH_401_MAX:
                        logger.error(
                            "Giving up on current adopted state after %d "
                            "consecutive 401s — rotating identity to force "
                            "true first-boot re-registration",
                            self._jwt_refresh_401_count,
                        )
                        # Rotate everything: JWT state, identity keypair,
                        # pairing secret, and auth token. Keeping the old
                        # pubkey would leave us re-registering with a key
                        # the CMS may have already revoked.
                        bootstrap_boot.clear_state(
                            self.settings.bootstrap_state_path,
                        )
                        for p in (
                            self.settings.device_key_path,
                            self.settings.pairing_secret_path,
                            self.settings.auth_token_path,
                        ):
                            try:
                                p.unlink()
                            except FileNotFoundError:
                                pass
                            except Exception:
                                logger.debug(
                                    "Failed to unlink %s on terminal 401", p,
                                    exc_info=True,
                                )
                        self._bootstrap_identity = None
                        self._bootstrap_pairing_secret = None
                        await ws.close()
                        return
                    await asyncio.sleep(JWT_REFRESH_RETRY_SEC)
                    continue
                except bootstrap_boot.RateLimitedError:
                    logger.info("WPS JWT refresh rate-limited — backing off")
                    await asyncio.sleep(JWT_REFRESH_RETRY_SEC)
                    continue
                except bootstrap_boot.BootstrapTransportError as e:
                    logger.warning("WPS JWT refresh transport error: %r", e)
                    await asyncio.sleep(JWT_REFRESH_RETRY_SEC)
                    continue
                except Exception:
                    logger.exception("Unexpected error refreshing WPS JWT")
                    await asyncio.sleep(JWT_REFRESH_RETRY_SEC)
                    continue

                self._jwt_refresh_401_count = 0
                logger.info(
                    "WPS JWT refreshed — forcing reconnect to pick up new token"
                )
                try:
                    await ws.close()
                except Exception:
                    pass
                return
        except asyncio.CancelledError:
            raise

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
            "display_ports": current_data.get("display_ports"),
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
        logger.debug("EVAL: default_asset=%s, schedules=%d, last_state=%s",
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
            asset_type = winner.get("asset_type")
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

            state_key = ("play", asset, checksum, loop_count, asset_type)
            if self._last_eval_state == state_key:
                return

            # Schedule changed — send ENDED for previous, STARTED for new
            self._end_current_playback()

            desired = DesiredState(
                mode=PlaybackMode.PLAY,
                asset=asset,
                asset_type=asset_type,
                loop=True,
                loop_count=loop_count,
                expected_checksum=checksum,
            )
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
                # Slideshows can be "registered" (parent JSON cached) yet have
                # one or more slide source files evicted independently. Treat
                # those as incomplete and force a refetch from CMS.
                if self._is_slideshow_asset(asset_name) and not self._has_complete_slideshow(
                    asset_name, expected_checksum,
                ):
                    logger.info(
                        "Slideshow %s registered but incomplete on disk; requesting refetch",
                        asset_name,
                    )
                else:
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

    def _spawn_fetch_asset(self, msg: dict, ws) -> None:
        """Dispatch ``fetch_asset`` as a background task.

        The WS read loop must stay responsive while large assets download
        (agora#136). Duplicate fetches for the same asset supersede the prior
        one. Actual download work is serialized by ``self._fetch_lock`` so
        AssetManager eviction accounting remains correct.
        """
        asset_name = msg.get("asset_name", "")
        if not asset_name:
            logger.warning("Invalid fetch_asset message: missing asset_name")
            return
        prior = self._fetch_tasks.get(asset_name)
        if prior is not None and not prior.done():
            prior.cancel()
        task = asyncio.create_task(self._fetch_asset_locked(msg, ws))
        self._fetch_tasks[asset_name] = task

        def _cleanup(t: asyncio.Task, name: str = asset_name) -> None:
            if self._fetch_tasks.get(name) is t:
                self._fetch_tasks.pop(name, None)

        task.add_done_callback(_cleanup)

    async def _fetch_asset_locked(self, msg: dict, ws) -> None:
        try:
            async with self._fetch_lock:
                await self._handle_fetch_asset(msg, ws)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Background fetch_asset task failed")

    async def _cancel_fetch(self, asset_name: str) -> None:
        if not asset_name:
            return
        task = self._fetch_tasks.get(asset_name)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _cancel_all_fetches(self) -> None:
        tasks = [t for t in self._fetch_tasks.values() if not t.done()]
        if not tasks:
            self._fetch_tasks.clear()
            return
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._fetch_tasks.clear()

    async def _handle_fetch_asset(self, msg: dict, ws) -> None:
        """CMS tells us to download an asset — with budget-aware eviction."""
        asset_name = msg.get("asset_name", "")
        asset_type = msg.get("asset_type", "")
        download_url = msg.get("download_url", "")
        expected_checksum = msg.get("checksum", "")
        expected_size = msg.get("size_bytes", 0)

        # Slideshow assets carry their payload in msg["slides"]; the outer
        # download_url is empty.  Dispatch BEFORE the empty-URL guard.
        if asset_type == "slideshow":
            await self._handle_fetch_slideshow(msg, ws)
            return

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

        actual_checksum = await self._download_one_asset(
            asset_name, asset_type, download_url, expected_checksum,
        )
        if actual_checksum is None:
            return  # already logged

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

    async def _download_one_asset(
        self,
        asset_name: str,
        asset_type: str,
        download_url: str,
        expected_checksum: str,
    ) -> str | None:
        """Download one asset to its target dir + register in the manifest.

        Caller is responsible for eviction and ACK. Returns the actual
        SHA-256 checksum on success or ``None`` on any failure (errors
        are logged here).
        """
        try:
            import aiohttp

            # Determine target directory using asset_type from CMS (issue #110),
            # falling back to extension-based detection for older CMS versions.
            if asset_type in ("video", "saved_stream"):
                target_dir = self.settings.videos_dir
            elif asset_type == "image":
                target_dir = self.settings.images_dir
            else:
                # Fallback: route by file extension (expanded list)
                ext = Path(asset_name).suffix.lower()
                if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi", ".ts"):
                    target_dir = self.settings.videos_dir
                elif ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"):
                    target_dir = self.settings.images_dir
                else:
                    # Default to videos/ — player searches there (never root assets/)
                    target_dir = self.settings.videos_dir

            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / asset_name

            logger.info("Fetching asset: %s from %s", asset_name, download_url)

            async with aiohttp.ClientSession() as session:
                headers = {}
                api_key = self._read_api_key()
                if api_key:
                    headers["X-Device-API-Key"] = api_key
                async with session.get(download_url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.error("Failed to download %s: HTTP %d", asset_name, resp.status)
                        return None

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
                        return None

                    os.replace(tmp_path, target_path)
                    file_size = target_path.stat().st_size
                    logger.info("Asset downloaded: %s (%d bytes)", asset_name, file_size)

            # Register in manifest
            rel_path = str(target_path.relative_to(self.settings.assets_dir))
            self.asset_manager.register(asset_name, rel_path, file_size, actual_checksum)
            return actual_checksum

        except Exception:
            logger.exception("Error fetching asset %s", asset_name)
            return None

    # ── Slideshow fetch ──

    async def _handle_fetch_slideshow(self, msg: dict, ws) -> None:
        """Fetch a slideshow: download each slide, write a local manifest.

        The slideshow's asset_manager entry is registered only after every
        slide is on disk with a verified checksum. Partial downloads are
        kept in cache (slides are independently useful) so a retry only
        re-fetches the missing ones.
        """
        asset_name = msg.get("asset_name", "")
        expected_checksum = msg.get("checksum", "")
        slides = msg.get("slides") or []

        if not asset_name:
            logger.warning("Invalid slideshow fetch_asset: missing asset_name")
            return
        if not slides or not isinstance(slides, list):
            logger.warning("Invalid slideshow fetch_asset for %s: empty or non-list slides", asset_name)
            await ws.send(json.dumps({
                "type": "fetch_failed",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "asset": asset_name,
                "reason": "invalid_slideshow_payload",
            }))
            return

        # Validate slide descriptors up front
        for i, slide in enumerate(slides):
            if not isinstance(slide, dict) or not slide.get("asset_name") or not slide.get("download_url"):
                logger.warning(
                    "Invalid slideshow fetch_asset for %s: slide %d malformed", asset_name, i,
                )
                await ws.send(json.dumps({
                    "type": "fetch_failed",
                    "protocol_version": PROTOCOL_VERSION,
                    "device_id": self.device_id,
                    "asset": asset_name,
                    "reason": "invalid_slide_descriptor",
                    "slide_position": i,
                }))
                return

        # Fast path: slideshow + every slide already cached with matching checksums.
        if self._has_complete_slideshow(asset_name, expected_checksum, slides):
            logger.info("Slideshow already cached and complete: %s", asset_name)
            await self._touch_slideshow_slides(asset_name, slides)
            await ws.send(json.dumps({
                "type": "asset_ack",
                "protocol_version": PROTOCOL_VERSION,
                "device_id": self.device_id,
                "asset_name": asset_name,
                "checksum": expected_checksum,
            }))
            return

        # Deduplicate slides by (asset_name, checksum) for budgeting and
        # downloading. The playlist itself preserves duplicates and order.
        unique_missing: dict[tuple[str, str], dict] = {}
        for slide in slides:
            key = (slide["asset_name"], slide.get("checksum", ""))
            if key in unique_missing:
                continue
            if self.asset_manager.has_asset(slide["asset_name"], slide.get("checksum") or None):
                continue
            unique_missing[key] = slide

        # Bulk eviction once for the sum of missing-slide bytes (the slideshow
        # manifest itself is sub-1 KB JSON, ignored in budgeting).
        if unique_missing:
            total_bytes = sum(s.get("size_bytes", 0) for s in unique_missing.values())
            scheduled_assets = self._get_scheduled_asset_names()
            sync_data = self._read_schedule_cache()
            default_asset = sync_data.get("default_asset") if sync_data else None
            # Slides we are about to download must also be protected during eviction
            # so the loop doesn't evict siblings out from under us.
            protected = scheduled_assets | {key[0] for key in unique_missing.keys()}

            ok = self.asset_manager.evict_for(total_bytes, protected, default_asset)
            if not ok:
                logger.error(
                    "Cannot fit slideshow %s (%d bytes total): budget=%dMB",
                    asset_name, total_bytes, self.asset_manager.budget_mb,
                )
                await ws.send(json.dumps({
                    "type": "fetch_failed",
                    "protocol_version": PROTOCOL_VERSION,
                    "device_id": self.device_id,
                    "asset": asset_name,
                    "reason": "insufficient_storage",
                    "budget_mb": self.asset_manager.budget_mb,
                    "available_mb": self.asset_manager.available_bytes // (1024 * 1024),
                    "required_mb": total_bytes // (1024 * 1024),
                }))
                return

        # Download each unique missing slide.
        for slide in unique_missing.values():
            actual = await self._download_one_asset(
                slide["asset_name"],
                slide.get("asset_type", ""),
                slide["download_url"],
                slide.get("checksum", ""),
            )
            if actual is None:
                logger.error(
                    "Slideshow %s: slide %s download failed",
                    asset_name, slide["asset_name"],
                )
                await ws.send(json.dumps({
                    "type": "fetch_failed",
                    "protocol_version": PROTOCOL_VERSION,
                    "device_id": self.device_id,
                    "asset": asset_name,
                    "reason": "slide_download_failed",
                    "slide_asset": slide["asset_name"],
                }))
                return

        # All slides verified. Write the slideshow manifest with per-slide
        # checksums so the completeness check has all the data it needs.
        manifest_payload = {
            "name": asset_name,
            "checksum": expected_checksum,
            "slides": [
                {
                    "name": s["asset_name"],
                    "asset_type": s.get("asset_type", ""),
                    "checksum": s.get("checksum", ""),
                    "size_bytes": s.get("size_bytes", 0),
                    "duration_ms": s.get("duration_ms", 0),
                    "play_to_end": bool(s.get("play_to_end", False)),
                }
                for s in slides
            ],
        }
        self.settings.slideshows_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = self.settings.slideshows_dir / f"{asset_name}.json"
        atomic_write(manifest_path, json.dumps(manifest_payload, indent=2))
        manifest_size = manifest_path.stat().st_size

        rel_path = str(manifest_path.relative_to(self.settings.assets_dir))
        self.asset_manager.register(asset_name, rel_path, manifest_size, expected_checksum)
        await self._touch_slideshow_slides(asset_name, slides)

        # Re-trigger desired state if player is waiting for this slideshow
        desired = read_state(self.settings.desired_state_path, DesiredState)
        if desired.asset == asset_name:
            logger.info("Re-applying desired state for just-downloaded slideshow: %s", asset_name)
            desired.timestamp = datetime.now(timezone.utc)
            write_state(self.settings.desired_state_path, desired)

        logger.info(
            "Slideshow fetched: %s (%d slides, %d unique downloads)",
            asset_name, len(slides), len(unique_missing),
        )
        await ws.send(json.dumps({
            "type": "asset_ack",
            "protocol_version": PROTOCOL_VERSION,
            "device_id": self.device_id,
            "asset_name": asset_name,
            "checksum": expected_checksum,
        }))

    async def _touch_slideshow_slides(self, _asset_name: str, slides: list[dict]) -> None:
        """Bump LRU timestamps for every slide source in a slideshow."""
        seen: set[str] = set()
        for slide in slides:
            name = slide.get("asset_name") or slide.get("name")
            if name and name not in seen:
                self.asset_manager.touch(name)
                seen.add(name)

    def _is_slideshow_asset(self, asset_name: str) -> bool:
        """True iff `asset_name` is registered as a slideshow on this device."""
        entry = self.asset_manager.get(asset_name)
        if not isinstance(entry, dict):
            return False
        path = entry.get("path", "")
        return isinstance(path, str) and path.startswith("slideshows/")

    def _read_slideshow_manifest(self, asset_name: str) -> dict | None:
        """Read and parse a local slideshow manifest. Returns None if missing/corrupt."""
        path = self.settings.slideshows_dir / f"{asset_name}.json"
        try:
            data = json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None
        if not isinstance(data, dict):
            return None
        if not isinstance(data.get("slides"), list):
            return None
        return data

    def _has_complete_slideshow(
        self,
        asset_name: str,
        expected_checksum: str,
        slides: list[dict] | None = None,
    ) -> bool:
        """A slideshow is complete only when:

        - the parent manifest entry is registered with a matching checksum,
        - the local slideshow JSON parses and matches that checksum,
        - every referenced slide is in the asset_manager with a matching checksum.

        If `slides` is provided (e.g. fresh from a FETCH_ASSET message), it is
        used as the authoritative slide list; otherwise the local manifest's
        slide list is used.
        """
        if not self.asset_manager.has_asset(asset_name, expected_checksum):
            return False
        manifest = self._read_slideshow_manifest(asset_name)
        if manifest is None:
            return False
        if expected_checksum and manifest.get("checksum") != expected_checksum:
            return False
        slide_list = slides if slides is not None else manifest.get("slides", [])
        for slide in slide_list:
            slide_name = slide.get("asset_name") or slide.get("name")
            slide_checksum = slide.get("checksum") or None
            if not slide_name:
                return False
            if not self.asset_manager.has_asset(slide_name, slide_checksum):
                return False
        return True

    async def _handle_delete_asset(self, msg: dict, ws) -> None:
        asset_name = msg.get("asset_name", "")
        if not asset_name:
            return

        self.asset_manager.remove(asset_name)

        # Also check disk directly in case it wasn't in manifest
        for d in [self.settings.videos_dir, self.settings.images_dir, self.settings.splash_dir, self.settings.slideshows_dir]:
            target = d / asset_name
            if target.exists():
                target.unlink()
                break
            # Slideshows live under <slideshows_dir>/<name>.json, not <name>.
            if d == self.settings.slideshows_dir:
                slideshow_target = d / f"{asset_name}.json"
                if slideshow_target.exists():
                    slideshow_target.unlink()
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

        # Bootstrap v2 identity + cached credentials.  These are safe to
        # always remove — on non-v2 builds the paths just don't exist.
        _safe_unlink(persist_dir / "device_key")
        _safe_unlink(persist_dir / "pairing_secret")
        _safe_unlink(persist_dir / "bootstrap_state.json")

        # Remove state files
        _safe_unlink(state_dir / "cms_status.json")
        _safe_unlink(state_dir / "cms_config.json")
        _safe_unlink(state_dir / "schedule.json")
        _safe_unlink(state_dir / "assets.json")

        # Wipe assets on disk
        for subdir in [self.settings.videos_dir, self.settings.images_dir, self.settings.slideshows_dir]:
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
        for subdir in [self.settings.videos_dir, self.settings.images_dir, self.settings.slideshows_dir]:
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
        """Collect journalctl logs for requested services and send back.

        Small payloads ride the WS back as a single ``logs_response``
        JSON message (legacy path).  When the journal output exceeds
        the ~1 MiB WPS frame ceiling we build a gzipped tarball and
        HTTP-POST it to the CMS log-upload endpoint; this replaces the
        earlier chunked-binary (LGCK) scheme, which WPS transport
        cannot carry because ``ws.send(bytes)`` is unsupported.
        """
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

        # journalctl-failure case always uses the small path — there's
        # no payload worth chunking.
        if error:
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
            return

        # Decide: JSON path (legacy, small) vs chunked binary path.
        response = {
            "type": "logs_response",
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "device_id": self.device_id,
            "logs": logs,
            "error": None,
        }
        json_payload = json.dumps(response)

        if len(json_payload.encode("utf-8")) <= LOGS_JSON_MAX_BYTES:
            try:
                await ws.send(json_payload)
            except Exception:
                logger.exception("Failed to send logs response")
            return

        # Large payload: gzipped tarball → HTTP POST to the CMS log
        # upload endpoint.  We used to split this into LGCK-tagged
        # binary WS frames, but the WPS transport rejects bytes sends;
        # an HTTP upload is mixed-fleet safe and bypasses both the
        # per-frame size cap and the transport's JSON-only constraint.
        try:
            tar_gz = _build_logs_tar_gz(logs)
        except Exception as e:
            logger.exception("Failed to build logs tarball for request %s", request_id)
            await self._send_logs_error(ws, request_id, f"log_pack_failed: {e}")
            return

        if len(tar_gz) > LOGS_UPLOAD_MAX_BYTES:
            logger.warning(
                "Log request %s payload %d bytes exceeds %d-byte cap; dropping",
                request_id, len(tar_gz), LOGS_UPLOAD_MAX_BYTES,
            )
            await self._send_logs_error(
                ws, request_id,
                f"logs_too_large: {len(tar_gz)} bytes > {LOGS_UPLOAD_MAX_BYTES}",
            )
            return

        logger.info(
            "Log request %s: uploading %d bytes via HTTP",
            request_id, len(tar_gz),
        )
        try:
            await self._upload_logs_bundle(request_id, tar_gz)
        except Exception as e:
            logger.exception(
                "Failed to upload logs for request %s", request_id,
            )
            await self._send_logs_error(ws, request_id, f"upload_failed: {e}")

    async def _upload_logs_bundle(self, request_id: str, tar_gz: bytes) -> None:
        """POST a gzipped log tarball to the CMS upload endpoint.

        A ``409`` response whose body carries ``status: "ready"`` means
        the request is already terminal on the CMS (e.g. an earlier
        retry succeeded); we treat that as success and do not raise.
        All other non-2xx responses raise ``RuntimeError``.
        """
        api_base = self._logs_api_base()
        if not api_base:
            raise RuntimeError("no CMS api_base configured for log upload")
        api_key = self._read_api_key()
        if not api_key:
            raise RuntimeError("no device api_key available for log upload")

        url = (
            f"{api_base.rstrip('/')}/api/devices/{self.device_id}"
            f"/logs/{request_id}/upload"
        )
        headers = {
            "X-Device-API-Key": api_key,
            "Content-Type": "application/gzip",
        }
        timeout = aiohttp.ClientTimeout(total=LOGS_UPLOAD_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, data=tar_gz) as resp:
                if resp.status == 409:
                    # Idempotent-retry case: CMS already considers the
                    # request terminal.  Accept as success so we stop
                    # bothering the user with a spurious error.
                    try:
                        body = await resp.json(content_type=None)
                    except Exception:
                        body = None
                    if isinstance(body, dict) and body.get("status") == "ready":
                        logger.info(
                            "Log request %s: CMS reports already-ready; treating as success",
                            request_id,
                        )
                        return
                    text = (
                        json.dumps(body)
                        if isinstance(body, dict)
                        else await resp.text()
                    )
                    raise RuntimeError(f"HTTP 409: {text[:200]}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {body[:200]}")
                logger.info(
                    "Log request %s uploaded successfully (HTTP %d)",
                    request_id, resp.status,
                )

    def _logs_api_base(self) -> str:
        """Resolve the CMS HTTP base URL for log uploads.

        Prefers ``settings.cms_api_url`` when explicitly configured,
        otherwise derives ``http(s)://`` from the active ws URL.
        """
        configured = (getattr(self.settings, "cms_api_url", "") or "").strip()
        if configured:
            return configured
        ws_url = self._active_cms_url or self._get_cms_url()
        if not ws_url:
            return ""
        return _derive_api_base(ws_url)

    async def _send_logs_error(self, ws, request_id: str, error: str) -> None:
        try:
            await ws.send(json.dumps({
                "type": "logs_response",
                "protocol_version": PROTOCOL_VERSION,
                "request_id": request_id,
                "device_id": self.device_id,
                "logs": {},
                "error": error,
            }))
        except Exception:
            logger.exception("Failed to send logs error response")

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
        """Get all asset names from the cached schedule, expanding any
        slideshow whose local manifest is on disk to include its slide
        sources so they are protected from LRU eviction while scheduled."""
        data = self._read_schedule_cache()
        if not data:
            return set()
        names: set[str] = set()
        for entry in data.get("schedules", []):
            asset = entry.get("asset")
            if asset:
                names.add(asset)
        default_asset = data.get("default_asset")
        if default_asset:
            names.add(default_asset)
        # Expand slideshows → slide sources
        for asset in list(names):
            if not self._is_slideshow_asset(asset):
                continue
            manifest = self._read_slideshow_manifest(asset)
            if manifest is None:
                continue
            for slide in manifest.get("slides", []):
                slide_name = slide.get("name") or slide.get("asset_name")
                if slide_name:
                    names.add(slide_name)
        if default_asset:
            # Caller may use default_asset as a separate protection axis;
            # leave it in `names` as well for safety but don't double-add.
            pass
        return names
