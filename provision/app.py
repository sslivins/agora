"""Captive portal provisioning service.

Serves a setup page on port 80 when the device is in AP mode.
Allows the user to configure Wi-Fi, CMS address, and device name.
"""

import json
import logging
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from provision.network import (
    connect_wifi,
    get_active_ssid,
    is_wifi_connected,
    scan_wifi,
    stop_ap,
)

logger = logging.getLogger("agora.provision")

PROVISION_DIR = Path(__file__).parent
PERSIST_DIR = Path("/opt/agora/persist")
STATE_DIR = Path("/opt/agora/state")

app = FastAPI(title="Agora Setup")

# Static files (CSS reused from main app)
static_dir = PROVISION_DIR / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def _read_portal_html() -> str:
    """Read the setup portal HTML template."""
    return (PROVISION_DIR / "templates" / "setup.html").read_text()


@app.get("/generate_204")
@app.get("/gen_204")
@app.get("/hotspot-detect.html")
@app.get("/ncsi.txt")
@app.get("/connecttest.txt")
@app.get("/redirect")
@app.get("/canonical.html")
async def captive_portal_redirect():
    """Handle captive portal detection from Android, iOS, Windows, etc."""
    return RedirectResponse("/", status_code=302)


@app.get("/", response_class=HTMLResponse)
async def setup_page():
    """Serve the setup portal page."""
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


@app.post("/api/provision")
async def provision(request: Request):
    """Apply provisioning configuration: connect to Wi-Fi and save settings."""
    body = await request.json()

    wifi_ssid = body.get("wifi_ssid", "").strip()
    wifi_password = body.get("wifi_password", "")
    cms_host = body.get("cms_host", "").strip()
    cms_port = int(body.get("cms_port", 8080))
    device_name = body.get("device_name", "").strip()

    if not wifi_ssid:
        return {"success": False, "error": "Wi-Fi network is required"}

    # Step 1: Stop AP mode and try connecting to Wi-Fi
    stop_ap()

    success, message = connect_wifi(wifi_ssid, wifi_password)
    if not success:
        return {"success": False, "error": f"Wi-Fi connection failed: {message}"}

    # Step 2: Save CMS config if provided
    if cms_host:
        # Strip protocol prefixes
        for prefix in ("ws://", "wss://", "http://", "https://"):
            if cms_host.startswith(prefix):
                cms_host = cms_host[len(prefix):]
        cms_host = cms_host.split("/")[0]
        if ":" in cms_host:
            parts = cms_host.rsplit(":", 1)
            cms_host = parts[0]
            try:
                cms_port = int(parts[1])
            except ValueError:
                pass

        cms_config = {
            "cms_host": cms_host,
            "cms_port": cms_port,
            "cms_url": f"ws://{cms_host}:{cms_port}/ws/device",
        }
        cms_config_path = PERSIST_DIR / "cms_config.json"
        cms_config_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cms_config_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cms_config, indent=2))
        tmp.replace(cms_config_path)

    # Step 3: Save device name if provided
    if device_name:
        device_name_path = PERSIST_DIR / "device_name"
        device_name_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = device_name_path.with_suffix(".tmp")
        tmp.write_text(device_name)
        tmp.replace(device_name_path)

    # Step 4: Mark provisioning complete
    provision_flag = PERSIST_DIR / "provisioned"
    provision_flag.parent.mkdir(parents=True, exist_ok=True)
    provision_flag.write_text("1")

    logger.info(
        "Provisioning complete: wifi=%s, cms=%s:%s, name=%s",
        wifi_ssid, cms_host or "(none)", cms_port, device_name or "(auto)",
    )

    return {"success": True, "message": "Device configured successfully"}
