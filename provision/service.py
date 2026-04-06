"""Provisioning service — manages the captive portal lifecycle and OOBE display.

Boot flow:
1. Check if device is provisioned (has Wi-Fi credentials)
2. If NOT provisioned → start AP mode + captive portal immediately
3. If provisioned → try connecting to saved Wi-Fi for 60 seconds
   - Success → exit (normal boot continues)
   - Failure → start AP mode + captive portal with 10-minute timeout
     - After timeout → retry Wi-Fi → cycle repeats

OOBE display:
When running on first boot (no provisioning flag), the service drives a
framebuffer-based setup wizard on the TV via provision.display.  The display
shows progress as the user connects their phone, configures Wi-Fi, and the
device connects to the CMS.  After the CMS client confirms adoption, the
service hands off to the player.

Error recovery:
- Wi-Fi failure: retries a few times, then re-enters AP mode so the user
  can re-enter the password on their phone.
- CMS failure: retries for a period, then shows a QR code on the TV linking
  to a reconfiguration page so the user can update the CMS address/port.

Runtime disconnect (NetworkManager handles reconnection — we never re-enter AP mode).
"""

import asyncio
import json
import logging
import signal
import socket
import sys
import threading
import time
from pathlib import Path

import uvicorn

from provision.display import ProvisionDisplay
from provision.dns import install_dns_redirect, remove_dns_redirect
from provision.network import (
    connect_wifi,
    get_active_ssid,
    get_device_ip,
    get_device_serial_suffix,
    get_wifi_interface,
    is_wifi_connected,
    start_ap,
    stop_ap,
)

logger = logging.getLogger("agora.provision")

# Log to both stderr (for journald/tee) and directly to boot partition file
# The boot partition file survives SD card pulls and is readable from Windows
_log_fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=_log_fmt)
_boot_log = Path("/boot/firmware/provision.log")
try:
    _fh = logging.FileHandler(str(_boot_log), mode="a")
    _fh.setFormatter(logging.Formatter(_log_fmt))
    _fh.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(_fh)
except OSError:
    pass  # boot partition not mounted or read-only

PERSIST_DIR = Path("/opt/agora/persist")
STATE_DIR = Path("/opt/agora/state")
PROVISION_FLAG = PERSIST_DIR / "provisioned"
CMS_STATUS_PATH = STATE_DIR / "cms_status.json"

WIFI_CONNECT_TIMEOUT = 60   # seconds to wait for Wi-Fi on boot
AP_SESSION_TIMEOUT = 600    # 10 minutes in AP mode before retrying Wi-Fi
PORTAL_PORT = 80

WIFI_RETRY_COUNT = 3        # attempts before returning to AP mode
WIFI_RETRY_DELAY = 5        # seconds between Wi-Fi retries

CMS_ADOPT_TIMEOUT = 300     # 5 minutes waiting for CMS adoption
CMS_ERROR_THRESHOLD = 5     # consecutive CMS errors → trigger reconfigure

CMS_MDNS_HOST = "agora-cms.local"
CMS_MDNS_PORT = 8080

OOBE_DISPLAY_HOLD = 1       # seconds to hold static screens before advancing


def is_provisioned() -> bool:
    """Check if the device has completed initial provisioning."""
    return PROVISION_FLAG.exists()


def _ap_ssid() -> str:
    """Generate unique AP SSID like 'Agora-A1B2'."""
    suffix = get_device_serial_suffix(4)
    return f"Agora-{suffix}"


def _wait_for_wifi(timeout: int) -> bool:
    """Block until Wi-Fi is connected or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_wifi_connected():
            return True
        time.sleep(2)
    return False


async def _run_portal(
    shutdown_event: asyncio.Event,
    timeout: int | None = None,
    display: ProvisionDisplay | None = None,
    phone_spinner_stop: threading.Event | None = None,
    phone_spinner_thread: threading.Thread | None = None,
    server_ready: asyncio.Event | None = None,
) -> dict | None:
    """Run the captive portal web server.

    Returns the provision data dict when the user submits the form,
    or None if shutdown was triggered or the timeout expired.

    If *server_ready* is provided it will be set once uvicorn is listening
    on the portal port, allowing the caller to delay AP activation until
    the HTTP server can actually serve captive-portal requests.
    """
    from provision.app import app, portal_events, reset_phone_seen

    # Start with a clean slate
    reset_phone_seen()
    while not portal_events.empty():
        try:
            portal_events.get_nowait()
        except asyncio.QueueEmpty:
            break

    result_data = None

    config = uvicorn.Config(
        app, host="0.0.0.0", port=PORTAL_PORT,
        log_level="info", access_log=False,
    )
    server = uvicorn.Server(config)

    async def _watch_shutdown():
        await shutdown_event.wait()
        server.should_exit = True

    async def _watch_timeout():
        if timeout is None:
            return
        await asyncio.sleep(timeout)
        if not shutdown_event.is_set():
            logger.info("AP session timed out after %ds — will retry Wi-Fi", timeout)
            shutdown_event.set()
            server.should_exit = True

    async def _watch_portal_events():
        nonlocal result_data
        while not shutdown_event.is_set() and not server.should_exit:
            # Auto-detect Wi-Fi recovery (for already-provisioned devices)
            if is_provisioned() and is_wifi_connected():
                logger.info("Wi-Fi recovered — stopping portal")
                server.should_exit = True
                return

            try:
                event = await asyncio.wait_for(portal_events.get(), timeout=2)
            except asyncio.TimeoutError:
                continue

            if event["type"] == "phone_connected":
                logger.info("Phone connected to AP")
                # Stop the connect-phone spinner and wait for it to finish
                # before drawing the new screen (prevents race where the
                # spinner blits over the phone-connected screen)
                _stop_spinner(phone_spinner_stop, phone_spinner_thread)
                if display and display.available:
                    display.show_phone_connected()
            elif event["type"] == "provision_submitted":
                logger.info("Provision submitted — exiting portal")
                result_data = event
                server.should_exit = True
                return

    serve_task = asyncio.create_task(server.serve())

    # Signal caller once uvicorn is actually listening
    ready_task = None
    if server_ready is not None:
        async def _signal_ready():
            while not server.started:
                await asyncio.sleep(0.1)
            server_ready.set()
        ready_task = asyncio.create_task(_signal_ready())

    watch_tasks = [
        asyncio.create_task(_watch_shutdown()),
        asyncio.create_task(_watch_timeout()),
        asyncio.create_task(_watch_portal_events()),
    ]

    # Wait for the server to finish (triggered by should_exit)
    await serve_task

    # Cancel watcher tasks — they may be blocking on events that never come
    if ready_task is not None:
        ready_task.cancel()
    for t in watch_tasks:
        t.cancel()
    await asyncio.gather(*watch_tasks, return_exceptions=True)

    return result_data


def _enter_ap_mode(ssid: str) -> bool:
    """Start AP mode and DNS redirect. Returns True on success."""
    logger.info("Starting AP mode: %s", ssid)
    # Install DNS redirect BEFORE starting AP — iOS probes instantly on connect
    install_dns_redirect()
    if not start_ap(ssid):
        logger.error("Failed to start AP mode")
        remove_dns_redirect()
        return False
    return True


def _exit_ap_mode() -> None:
    """Stop AP mode and DNS redirect."""
    logger.info("Stopping AP mode...")
    try:
        stop_ap()
        logger.info("AP stopped")
    except Exception:
        logger.exception("Error stopping AP")
    try:
        remove_dns_redirect()
        logger.info("DNS redirect removed")
    except Exception:
        logger.exception("Error removing DNS redirect")
    logger.info("AP mode stopped")


def _read_cms_status() -> dict:
    """Read the CMS client status file.  Returns empty dict on failure."""
    try:
        return json.loads(CMS_STATUS_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _get_cms_host() -> str:
    """Read CMS host from the persisted config."""
    try:
        cfg = json.loads((PERSIST_DIR / "cms_config.json").read_text())
        return cfg.get("cms_host", "")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return ""


def _stop_spinner(stop_event: threading.Event, thread: threading.Thread | None) -> None:
    """Cleanly stop a spinner thread if running."""
    if thread and thread.is_alive():
        stop_event.set()
        thread.join(timeout=2)


def _try_mdns_discovery() -> bool:
    """Try mDNS auto-discovery for CMS and save config if found.

    Returns True if CMS was discovered, False otherwise.
    """
    try:
        socket.getaddrinfo(CMS_MDNS_HOST, CMS_MDNS_PORT, socket.AF_INET)
    except socket.gaierror:
        return False

    cms_config = {
        "cms_host": CMS_MDNS_HOST,
        "cms_port": CMS_MDNS_PORT,
        "cms_url": f"ws://{CMS_MDNS_HOST}:{CMS_MDNS_PORT}/ws/device",
    }
    cms_config_path = PERSIST_DIR / "cms_config.json"
    cms_config_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cms_config_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cms_config, indent=2))
    tmp.replace(cms_config_path)
    logger.info("Auto-discovered CMS at %s:%d", CMS_MDNS_HOST, CMS_MDNS_PORT)
    return True


async def _try_wifi_connect(
    display: ProvisionDisplay | None,
    wifi_ssid: str,
    wifi_password: str,
    shutdown_event: asyncio.Event,
) -> bool:
    """Attempt Wi-Fi connection with retries and display updates.

    Returns True if connected, False if all retries exhausted.
    """
    for attempt in range(1, WIFI_RETRY_COUNT + 1):
        if shutdown_event.is_set():
            return False

        logger.info(
            "Wi-Fi attempt %d/%d for '%s'", attempt, WIFI_RETRY_COUNT, wifi_ssid,
        )

        # Show spinner animation while connecting
        spinner_stop = threading.Event()
        spinner_thread = None
        if display and display.available:
            subtitle = (
                f"Attempt {attempt} of {WIFI_RETRY_COUNT}...\n"
                "Please wait while we connect to your Wi-Fi network."
            )
            spinner_thread = threading.Thread(
                target=display.animate_spinner,
                kwargs={
                    "step": "Step 3 of 5",
                    "title": "Connecting to Wi-Fi",
                    "detail": wifi_ssid,
                    "subtitle": subtitle,
                    "progress": 3,
                    "stop_event": spinner_stop,
                },
                daemon=True,
            )
            spinner_thread.start()

        success, message = await asyncio.to_thread(connect_wifi, wifi_ssid, wifi_password)

        _stop_spinner(spinner_stop, spinner_thread)

        if success:
            return True

        logger.warning("Wi-Fi attempt %d failed: %s", attempt, message)
        if attempt < WIFI_RETRY_COUNT:
            if display and display.available:
                display.show_wifi_failed(
                    wifi_ssid,
                    f"{message} — retrying ({attempt}/{WIFI_RETRY_COUNT})...",
                )
            await asyncio.sleep(WIFI_RETRY_DELAY)

    return False


async def _wait_for_cms_adoption(
    display: ProvisionDisplay | None,
    shutdown_event: asyncio.Event,
) -> str:
    """Monitor CMS status after provisioning.

    Returns:
        ``"adopted"``  — device accepted by CMS
        ``"no_cms"``   — no CMS configured (standalone mode)
        ``"failed"``   — repeated CMS errors, needs reconfiguration
        ``"timeout"``  — timed out without adoption
        ``"shutdown"`` — shutdown event triggered
    """
    cms_host = _get_cms_host()
    if not cms_host:
        logger.info("No CMS configured — standalone mode")
        return "no_cms"

    shown_connecting = False
    shown_pending = False
    spinner_stop = threading.Event()
    spinner_thread = None
    consecutive_errors = 0

    try:
        deadline = time.monotonic() + CMS_ADOPT_TIMEOUT
        while not shutdown_event.is_set() and time.monotonic() < deadline:
            status = _read_cms_status()
            state = status.get("state", "")
            registration = status.get("registration", "")

            if state == "connected" and registration == "registered":
                _stop_spinner(spinner_stop, spinner_thread)
                logger.info("Device adopted by CMS")
                return "adopted"

            if state == "connected" and registration == "pending":
                _stop_spinner(spinner_stop, spinner_thread)
                consecutive_errors = 0
                if not shown_pending and display and display.available:
                    display.show_cms_connected_pending(cms_host)
                    shown_pending = True
                    logger.info("CMS connected — waiting for adoption")

            elif state == "error":
                _stop_spinner(spinner_stop, spinner_thread)
                consecutive_errors += 1
                error_msg = status.get("error", "")
                logger.warning(
                    "CMS error (%d/%d): %s",
                    consecutive_errors, CMS_ERROR_THRESHOLD, error_msg,
                )
                if consecutive_errors >= CMS_ERROR_THRESHOLD:
                    if display and display.available:
                        display.show_cms_failed(cms_host, error_msg)
                    return "failed"
                if display and display.available:
                    display.show_cms_failed(cms_host, error_msg)

            elif state in ("connecting", "disconnected", ""):
                consecutive_errors = 0
                if not shown_connecting and display and display.available:
                    spinner_stop = threading.Event()
                    spinner_thread = threading.Thread(
                        target=display.animate_spinner,
                        kwargs={
                            "step": "Step 4 of 5",
                            "title": "Contacting Server",
                            "detail": cms_host,
                            "detail_font": "Monospace 32",
                            "subtitle": "Verifying connection to the\n"
                                        "content management server...",
                            "progress": 4,
                            "stop_event": spinner_stop,
                        },
                        daemon=True,
                    )
                    spinner_thread.start()
                    shown_connecting = True

            await asyncio.sleep(2)

        if shutdown_event.is_set():
            return "shutdown"
        return "timeout"
    finally:
        _stop_spinner(spinner_stop, spinner_thread)


async def _run_reconfigure_server(
    shutdown_event: asyncio.Event,
    display: ProvisionDisplay | None,
) -> bool:
    """Start a web server for CMS reconfiguration.

    Shows a QR code on the TV pointing to this device's IP.
    Returns True if the user submitted new CMS config, False on shutdown.
    """
    from provision.app import app, reconfigure_events

    device_ip = get_device_ip()
    if not device_ip:
        logger.error("Cannot determine device IP for reconfigure server")
        return False

    url = f"http://{device_ip}"
    logger.info("Starting reconfigure server at %s", url)

    if display and display.available:
        display.show_cms_reconfigure(url)

    # Drain stale events
    while not reconfigure_events.empty():
        try:
            reconfigure_events.get_nowait()
        except asyncio.QueueEmpty:
            break

    config = uvicorn.Config(
        app, host="0.0.0.0", port=PORTAL_PORT,
        log_level="info", access_log=False,
    )
    server = uvicorn.Server(config)
    reconfigured = False

    async def _watch_shutdown():
        await shutdown_event.wait()
        server.should_exit = True

    async def _watch_reconfigure():
        nonlocal reconfigured
        while not shutdown_event.is_set() and not server.should_exit:
            try:
                event = await asyncio.wait_for(reconfigure_events.get(), timeout=2)
            except asyncio.TimeoutError:
                continue
            if event["type"] == "cms_reconfigured":
                logger.info(
                    "CMS reconfigured to %s:%s",
                    event.get("cms_host"), event.get("cms_port"),
                )
                reconfigured = True
                server.should_exit = True
                return

    await asyncio.gather(
        server.serve(),
        _watch_shutdown(),
        _watch_reconfigure(),
    )

    return reconfigured


async def run_service(force_oobe: bool = False) -> None:
    """Main provisioning service loop."""
    ssid = _ap_ssid()
    shutdown_event = asyncio.Event()

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Ignore SIGHUP — the process must survive SSH disconnection because
    # entering AP mode changes the Wi-Fi interface and breaks any SSH session.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # Initialize display (no-op if framebuffer unavailable)
    display = ProvisionDisplay()

    run_oobe = force_oobe or not is_provisioned()

    if run_oobe:
        # ── First boot OOBE ──────────────────────────────────────────
        logger.info("Device not provisioned — starting OOBE")

        # Quit Plymouth boot splash so we can draw to the framebuffer
        proc = await asyncio.create_subprocess_exec(
            "sudo", "plymouth", "quit", "--retain-splash",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        logger.info("Plymouth quit (rc=%d)", proc.returncode)

        # Player won't auto-start (ConditionPathExists=provisioned)
        # but stop it in case it was already running
        proc = await asyncio.create_subprocess_exec(
            "sudo", "systemctl", "stop", "agora-player",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()

        # Welcome screen with animated spinner while we start up
        welcome_spinner_stop = threading.Event()
        welcome_spinner_thread = threading.Thread(
            target=display.animate_welcome,
            kwargs={"stop_event": welcome_spinner_stop},
            daemon=True,
        )
        welcome_spinner_thread.start()

        # ── Phase 1: Wi-Fi provisioning (loops on failure) ───────────
        while not shutdown_event.is_set():
            # Prepare the phone-connect spinner (started after AP is up)
            phone_spinner_stop = threading.Event()
            phone_spinner_thread = threading.Thread(
                target=display.animate_connect_phone,
                args=(ssid,),
                kwargs={"stop_event": phone_spinner_stop},
                daemon=True,
            )

            # Start the captive portal server FIRST so it's listening on
            # port 80 before the AP broadcasts the SSID.  This prevents
            # the race where a phone connects to the AP but gets
            # "connection refused" because uvicorn hasn't bound yet.
            server_ready = asyncio.Event()
            portal_task = asyncio.create_task(_run_portal(
                shutdown_event, timeout=None, display=display,
                phone_spinner_stop=phone_spinner_stop,
                phone_spinner_thread=phone_spinner_thread,
                server_ready=server_ready,
            ))

            # Wait for uvicorn to be listening (< 1s typically)
            await server_ready.wait()
            logger.info("Portal server ready on port %d", PORTAL_PORT)

            # Now start the AP — phones that connect will hit a live server
            if not _enter_ap_mode(ssid):
                logger.error("Cannot start AP mode — exiting")
                _stop_spinner(welcome_spinner_stop, welcome_spinner_thread)
                portal_task.cancel()
                await asyncio.gather(portal_task, return_exceptions=True)
                sys.exit(1)

            # Stop the welcome spinner and transition to "connect phone"
            _stop_spinner(welcome_spinner_stop, welcome_spinner_thread)
            phone_spinner_thread.start()

            # Wait for the user to submit Wi-Fi config via the portal
            provision_data = await portal_task
            _stop_spinner(phone_spinner_stop, phone_spinner_thread)
            logger.info("Portal returned: %s", provision_data)
            _exit_ap_mode()

            if not provision_data:
                if shutdown_event.is_set():
                    logger.info("Shutdown requested — exiting")
                    display.close()
                    return
                logger.info("No provision data — retrying")
                continue

            wifi_ssid = provision_data.get("wifi_ssid", "")
            wifi_password = provision_data.get("wifi_password", "")
            logger.info("Attempting Wi-Fi connection to '%s'", wifi_ssid)

            # Try Wi-Fi connection with retries
            connected = await _try_wifi_connect(
                display, wifi_ssid, wifi_password, shutdown_event,
            )

            if connected:
                logger.info("Wi-Fi connected successfully")
                # Write provisioning flag
                PROVISION_FLAG.parent.mkdir(parents=True, exist_ok=True)
                PROVISION_FLAG.write_text("1")

                # Restart the CMS client so it reconnects immediately
                # (it may have hit exponential backoff while Wi-Fi was down)
                proc = await asyncio.create_subprocess_exec(
                    "sudo", "systemctl", "restart", "agora-cms-client",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
                logger.info("CMS client restarted")

                # Pre-start the API service in the background while waiting
                # for CMS adoption.  The player is NOT started here — it will
                # steal the DRM display from the OOBE framebuffer.  The player
                # is started after Phase 2 completes successfully.
                proc = await asyncio.create_subprocess_exec(
                    "sudo", "systemctl", "start", "agora-api",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
                logger.info("Pre-started agora-api")

                # Try mDNS auto-discovery if no CMS was explicitly configured
                if not _get_cms_host():
                    _try_mdns_discovery()

                display.show_wifi_connected(get_active_ssid() or wifi_ssid)
                await asyncio.sleep(OOBE_DISPLAY_HOLD)
                break

            # Wi-Fi failed after all retries — show error and loop back to AP
            logger.warning("Wi-Fi connection failed after all retries")
            display.show_wifi_failed(wifi_ssid)
            await asyncio.sleep(OOBE_DISPLAY_HOLD)
            logger.warning(
                "Wi-Fi failed after %d attempts — restarting AP",
                WIFI_RETRY_COUNT,
            )

        # ── Phase 2: CMS adoption (loops with reconfigure) ──────────
        logger.info("Entering CMS adoption phase")
        adoption_success = False
        while not shutdown_event.is_set():
            result = await _wait_for_cms_adoption(display, shutdown_event)
            logger.info("CMS adoption result: %s", result)

            if result == "adopted":
                adoption_success = True
                break
            elif result in ("no_cms", "shutdown"):
                break
            elif result in ("failed", "timeout"):
                # CMS failed — offer reconfiguration via QR code
                logger.info("CMS connection failed — starting reconfigure server")
                reconfigured = await _run_reconfigure_server(
                    shutdown_event, display,
                )
                if reconfigured:
                    logger.info("CMS reconfigured — restarting CMS client and retrying")
                    proc = await asyncio.create_subprocess_exec(
                        "sudo", "systemctl", "restart", "agora-cms-client",
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc.wait()
                    continue
                # Shutdown triggered — loop will exit on next iteration

        if not shutdown_event.is_set() and adoption_success:
            display.show_adopted()
            await asyncio.sleep(OOBE_DISPLAY_HOLD)

        display.close()

        # Restart Plymouth so the animated boot splash covers the gap while
        # the player process imports GStreamer and builds its first pipeline.
        # The player calls `plymouth quit --retain-splash` before claiming
        # the DRM device, so the handoff is seamless.
        logger.info("Starting Plymouth splash for player handoff")
        proc = await asyncio.create_subprocess_exec(
            "sudo", "plymouthd", "--mode=boot",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        proc = await asyncio.create_subprocess_exec(
            "sudo", "plymouth", "show-splash",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        logger.info("Plymouth splash active")

        # Start the player now that OOBE is complete and the framebuffer
        # has been released.  Plymouth covers the startup time.
        proc = await asyncio.create_subprocess_exec(
            "sudo", "systemctl", "start", "agora-player",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        logger.info("OOBE complete — handing off to player")
    else:
        # ── Provisioned device boot ──────────────────────────────────
        logger.info("Device provisioned — waiting for Wi-Fi (%ds)", WIFI_CONNECT_TIMEOUT)

        iface = get_wifi_interface()
        if not iface:
            logger.error("No Wi-Fi interface found — exiting")
            sys.exit(1)

        while not shutdown_event.is_set():
            if _wait_for_wifi(WIFI_CONNECT_TIMEOUT):
                logger.info("Wi-Fi connected — provisioning service exiting")
                return

            # Wi-Fi failed — enter AP mode with timeout
            logger.warning("Wi-Fi not available — entering AP mode for %ds", AP_SESSION_TIMEOUT)

            # Start portal server before AP so it's listening when phones connect
            portal_shutdown = asyncio.Event()
            server_ready = asyncio.Event()
            portal_task = asyncio.create_task(_run_portal(
                portal_shutdown, timeout=AP_SESSION_TIMEOUT,
                server_ready=server_ready,
            ))
            await server_ready.wait()

            if not _enter_ap_mode(ssid):
                logger.error("Cannot start AP mode — retrying in 30s")
                portal_shutdown.set()
                await asyncio.gather(portal_task, return_exceptions=True)
                await asyncio.sleep(30)
                continue

            provision_data = await portal_task
            _exit_ap_mode()

            if provision_data:
                # User submitted new Wi-Fi credentials
                wifi_ssid = provision_data.get("wifi_ssid", "")
                wifi_password = provision_data.get("wifi_password", "")
                connected = await _try_wifi_connect(
                    None, wifi_ssid, wifi_password, shutdown_event,
                )
                if connected:
                    PROVISION_FLAG.parent.mkdir(parents=True, exist_ok=True)
                    PROVISION_FLAG.write_text("1")
                    return

            # Loop back to try saved Wi-Fi


def main() -> None:
    force = "--force-oobe" in sys.argv
    if force:
        logger.info("--force-oobe: skipping provisioned check")
    try:
        asyncio.run(run_service(force_oobe=force))
    except Exception:
        logger.exception("OOBE crashed — attempting Wi-Fi recovery")
        # Best-effort: tear down AP if it's up and try to reconnect
        try:
            stop_ap()
            remove_dns_redirect()
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
