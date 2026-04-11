"""Captive portal provisioning service.

Serves a setup page on port 80 when the device is in AP mode.
Allows the user to configure Wi-Fi, CMS address, and device name.

Also serves a reconfigure page when the device is on Wi-Fi but
CMS connection fails — allowing the user to update the CMS address
via their phone by scanning a QR code shown on the TV.

Events are emitted to ``portal_events`` (asyncio.Queue) so that the
provisioning service can react to user actions (phone connected,
provision submitted, CMS reconfigured).
"""

import asyncio
import json
import logging
import socket
import time
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from provision.network import (
    get_active_ssid,
    is_wifi_connected,
    scan_wifi,
)

logger = logging.getLogger("agora.provision")

PROVISION_DIR = Path(__file__).parent
PERSIST_DIR = Path("/opt/agora/persist")
STATE_DIR = Path("/opt/agora/state")
CMS_MDNS_HOST = "agora-cms.local"
CMS_DEFAULT_PORT = 8080

# ── Event queues (consumed by provision/service.py) ──────────────────────────

portal_events: asyncio.Queue = asyncio.Queue()
reconfigure_events: asyncio.Queue = asyncio.Queue()

# Track whether a phone has connected this session (reset by service)
_phone_seen = False

app = FastAPI(title="Agora Setup")


@app.middleware("http")
async def log_request_timing(request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    elapsed = (time.monotonic() - start) * 1000
    logger.info("%s %s -> %d (%.0fms)", request.method, request.url.path,
                response.status_code, elapsed)
    return response


# Static files (CSS reused from main app)
static_dir = PROVISION_DIR / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def _read_portal_html() -> str:
    """Read the setup portal HTML template."""
    return (PROVISION_DIR / "templates" / "setup.html").read_text()


def _read_reconfigure_html() -> str:
    """Read the CMS reconfiguration HTML template."""
    return (PROVISION_DIR / "templates" / "reconfigure.html").read_text()


def reset_phone_seen() -> None:
    """Reset the phone-seen flag (called by service between AP sessions)."""
    global _phone_seen
    _phone_seen = False


@app.get("/hotspot-detect.html")
async def apple_captive_detect():
    """iOS/macOS captive portal detection.

    Apple checks http://captive.apple.com/hotspot-detect.html and expects
    '<HTML><HEAD><TITLE>Success</TITLE></HEAD><BODY>Success</BODY></HTML>'
    when there is no captive portal. Return anything else to trigger the CNA.
    """
    return HTMLResponse(
        '<HTML><HEAD><TITLE>Agora Setup</TITLE></HEAD><BODY>'
        '<script>window.location="http://10.42.0.1/"</script></BODY></HTML>',
        status_code=200,
    )


@app.get("/generate_204")
@app.get("/gen_204")
async def android_captive_detect():
    """Android captive portal detection.

    Android checks connectivity URLs and expects a 204. Returning a 302
    redirect triggers the sign-in notification.
    """
    return RedirectResponse("/", status_code=302)


@app.get("/ncsi.txt")
@app.get("/connecttest.txt")
@app.get("/redirect")
@app.get("/canonical.html")
async def other_captive_detect():
    """Windows/other captive portal detection."""
    return RedirectResponse("/", status_code=302)


@app.get("/", response_class=HTMLResponse)
async def setup_page():
    """Serve the setup portal page and emit phone_connected on first visit."""
    global _phone_seen
    if not _phone_seen:
        _phone_seen = True
        portal_events.put_nowait({"type": "phone_connected"})
    return _read_portal_html()


@app.get("/api/wifi/scan")
async def wifi_scan():
    """Scan for available Wi-Fi networks."""
    networks = scan_wifi()
    return {"networks": [asdict(n) for n in networks]}


@app.get("/api/wifi/status")
async def wifi_status():
    """Return current Wi-Fi connection status."""
    connected = is_wifi_connected()
    ssid = get_active_ssid() if connected else None
    return {"connected": connected, "ssid": ssid}


@app.get("/api/cms/discover")
async def cms_discover():
    """Try to discover the CMS via mDNS (agora-cms.local).

    Only works when connected to Wi-Fi (not in AP mode).
    """
    try:
        socket.getaddrinfo(CMS_MDNS_HOST, CMS_DEFAULT_PORT, socket.AF_INET)
        return {
            "found": True,
            "host": CMS_MDNS_HOST,
            "port": CMS_DEFAULT_PORT,
        }
    except socket.gaierror:
        return {"found": False}


@app.post("/api/provision")
async def provision(request: Request):
    """Accept provisioning configuration.

    Saves CMS config and device name immediately, then emits a
    ``provision_submitted`` event so the service can handle the Wi-Fi
    connection (which requires stopping the AP — single radio).
    """
    body = await request.json()

    wifi_ssid = body.get("wifi_ssid", "").strip()
    wifi_password = body.get("wifi_password", "")
    cms_host = body.get("cms_host", "").strip() or CMS_MDNS_HOST
    cms_port = int(body.get("cms_port", CMS_DEFAULT_PORT))
    cms_tls = bool(body.get("cms_tls", False))
    device_name = body.get("device_name", "").strip()

    if not wifi_ssid:
        return {"success": False, "error": "Wi-Fi network is required"}

    # Save CMS config (defaults to mDNS host if not provided)
    if cms_host:
        # Strip protocol prefixes
        for prefix in ("ws://", "wss://", "http://", "https://"):
            if cms_host.startswith(prefix):
                if prefix in ("wss://", "https://"):
                    cms_tls = True
                cms_host = cms_host[len(prefix):]
        cms_host = cms_host.split("/")[0]
        if ":" in cms_host:
            parts = cms_host.rsplit(":", 1)
            cms_host = parts[0]
            try:
                cms_port = int(parts[1])
            except ValueError:
                pass

        # Default port based on TLS
        if cms_tls and cms_port == CMS_DEFAULT_PORT:
            cms_port = 443

        scheme = "wss" if cms_tls else "ws"
        if cms_tls and cms_port == 443:
            cms_url = f"{scheme}://{cms_host}/ws/device"
        else:
            cms_url = f"{scheme}://{cms_host}:{cms_port}/ws/device"

        cms_config = {
            "cms_host": cms_host,
            "cms_port": cms_port,
            "cms_tls": cms_tls,
            "cms_url": cms_url,
        }
        cms_config_path = PERSIST_DIR / "cms_config.json"
        cms_config_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cms_config_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cms_config, indent=2))
        tmp.replace(cms_config_path)

    # Save device name if provided
    if device_name:
        device_name_path = PERSIST_DIR / "device_name"
        device_name_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = device_name_path.with_suffix(".tmp")
        tmp.write_text(device_name)
        tmp.replace(device_name_path)

    # Emit event for service to handle Wi-Fi connection
    portal_events.put_nowait({
        "type": "provision_submitted",
        "wifi_ssid": wifi_ssid,
        "wifi_password": wifi_password,
        "cms_host": cms_host,
        "cms_port": cms_port,
    })

    logger.info(
        "Provision submitted: wifi=%s, cms=%s:%s, name=%s",
        wifi_ssid, cms_host or "(none)", cms_port, device_name or "(auto)",
    )

    # Build AP SSID so the phone can tell the user what to reconnect to
    from provision.network import get_device_serial_suffix
    ap_ssid = f"Agora-{get_device_serial_suffix(4)}"

    return {
        "success": True,
        "message": (
            "This hotspot will now disconnect while the device connects "
            f"to Wi-Fi. Check the TV for progress. If it fails, reconnect "
            f"to {ap_ssid} to try again."
        ),
    }


# ── Reconfigure endpoints (CMS host/port change via Wi-Fi) ──────────────────


@app.get("/reconfigure", response_class=HTMLResponse)
async def reconfigure_page():
    """Serve the CMS reconfiguration page."""
    return _read_reconfigure_html()


@app.get("/api/cms/config")
async def get_cms_config():
    """Return the current CMS configuration."""
    try:
        cfg = json.loads((PERSIST_DIR / "cms_config.json").read_text())
        return {
            "cms_host": cfg.get("cms_host", ""),
            "cms_port": cfg.get("cms_port", CMS_DEFAULT_PORT),
            "cms_tls": cfg.get("cms_tls", False),
        }
    except (FileNotFoundError, json.JSONDecodeError):
        return {"cms_host": "", "cms_port": CMS_DEFAULT_PORT, "cms_tls": False}


@app.post("/api/reconfigure")
async def reconfigure(request: Request):
    """Update CMS configuration (called from reconfigure page)."""
    body = await request.json()
    cms_host = body.get("cms_host", "").strip()
    cms_port = int(body.get("cms_port", CMS_DEFAULT_PORT))
    cms_tls = bool(body.get("cms_tls", False))

    if not cms_host:
        cms_host = CMS_MDNS_HOST

    # Strip protocol prefixes
    for prefix in ("ws://", "wss://", "http://", "https://"):
        if cms_host.startswith(prefix):
            if prefix in ("wss://", "https://"):
                cms_tls = True
            cms_host = cms_host[len(prefix):]
    cms_host = cms_host.split("/")[0]
    if ":" in cms_host:
        parts = cms_host.rsplit(":", 1)
        cms_host = parts[0]
        try:
            cms_port = int(parts[1])
        except ValueError:
            pass

    # Default port based on TLS
    if cms_tls and cms_port == CMS_DEFAULT_PORT:
        cms_port = 443

    scheme = "wss" if cms_tls else "ws"
    if cms_tls and cms_port == 443:
        cms_url = f"{scheme}://{cms_host}/ws/device"
    else:
        cms_url = f"{scheme}://{cms_host}:{cms_port}/ws/device"

    cms_config = {
        "cms_host": cms_host,
        "cms_port": cms_port,
        "cms_tls": cms_tls,
        "cms_url": cms_url,
    }
    cms_config_path = PERSIST_DIR / "cms_config.json"
    cms_config_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cms_config_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cms_config, indent=2))
    tmp.replace(cms_config_path)

    # Signal the service to retry CMS with new config
    reconfigure_events.put_nowait({
        "type": "cms_reconfigured",
        "cms_host": cms_host,
        "cms_port": cms_port,
    })

    logger.info("CMS reconfigured to %s:%s", cms_host, cms_port)
    return {"success": True, "message": f"CMS updated to {cms_host}:{cms_port}"}
