"""Provisioning service — manages the captive portal lifecycle.

Boot flow:
1. Check if device is provisioned (has Wi-Fi credentials)
2. If NOT provisioned → start AP mode + captive portal immediately
3. If provisioned → try connecting to saved Wi-Fi for 60 seconds
   - Success → exit (normal boot continues)
   - Failure → start AP mode + captive portal with 10-minute timeout
     - After timeout → retry Wi-Fi → cycle repeats

Runtime disconnect (NetworkManager handles reconnection — we never re-enter AP mode).
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

import uvicorn

from provision.dns import install_dns_redirect, remove_dns_redirect
from provision.network import (
    get_device_serial_suffix,
    get_wifi_interface,
    is_wifi_connected,
    start_ap,
    stop_ap,
)

logger = logging.getLogger("agora.provision")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

PERSIST_DIR = Path("/opt/agora/persist")
PROVISION_FLAG = PERSIST_DIR / "provisioned"

WIFI_CONNECT_TIMEOUT = 60   # seconds to wait for Wi-Fi on boot
AP_SESSION_TIMEOUT = 600    # 10 minutes in AP mode before retrying Wi-Fi
PORTAL_PORT = 80


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


async def _run_portal(shutdown_event: asyncio.Event, timeout: int | None = None) -> None:
    """Run the captive portal web server until shutdown or timeout."""
    from provision.app import app

    config = uvicorn.Config(
        app, host="0.0.0.0", port=PORTAL_PORT,
        log_level="info", access_log=False,
    )
    server = uvicorn.Server(config)

    async def _watch_shutdown():
        await shutdown_event.wait()
        server.should_exit = True

    async def _watch_provisioned():
        """Stop portal once provisioning is complete."""
        while not shutdown_event.is_set():
            if PROVISION_FLAG.exists() and is_wifi_connected():
                logger.info("Provisioning complete — stopping portal")
                shutdown_event.set()
                server.should_exit = True
                return
            await asyncio.sleep(2)

    async def _watch_timeout():
        if timeout is None:
            return
        await asyncio.sleep(timeout)
        if not shutdown_event.is_set():
            logger.info("AP session timed out after %ds — will retry Wi-Fi", timeout)
            shutdown_event.set()
            server.should_exit = True

    await asyncio.gather(
        server.serve(),
        _watch_shutdown(),
        _watch_provisioned(),
        _watch_timeout(),
    )


def _enter_ap_mode(ssid: str) -> bool:
    """Start AP mode and DNS redirect. Returns True on success."""
    logger.info("Starting AP mode: %s", ssid)
    if not start_ap(ssid):
        logger.error("Failed to start AP mode")
        return False
    install_dns_redirect()
    return True


def _exit_ap_mode() -> None:
    """Stop AP mode and DNS redirect."""
    stop_ap()
    remove_dns_redirect()
    logger.info("AP mode stopped")


async def run_service() -> None:
    """Main provisioning service loop."""
    ssid = _ap_ssid()
    shutdown_event = asyncio.Event()

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    if not is_provisioned():
        # First boot — go straight to AP mode
        logger.info("Device not provisioned — entering AP mode")
        if not _enter_ap_mode(ssid):
            logger.error("Cannot start AP mode — exiting")
            sys.exit(1)
        await _run_portal(shutdown_event, timeout=None)
        _exit_ap_mode()
    else:
        # Provisioned — try connecting to saved Wi-Fi
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
            if not _enter_ap_mode(ssid):
                logger.error("Cannot start AP mode — retrying in 30s")
                await asyncio.sleep(30)
                continue

            await _run_portal(shutdown_event, timeout=AP_SESSION_TIMEOUT)
            _exit_ap_mode()

            # After AP timeout, loop back and retry Wi-Fi


def main() -> None:
    asyncio.run(run_service())


if __name__ == "__main__":
    main()
