"""CMS connection configuration API."""

import json
import subprocess

from fastapi import APIRouter, Depends, HTTPException, Request

from api.auth import get_settings, require_auth
from api.config import Settings
from shared.state import atomic_write

router = APIRouter(prefix="/cms", dependencies=[Depends(require_auth)])


CMS_WS_PATH = "/ws/device"
DEFAULT_CMS_PORT = 8080


def _read_cms_config(settings: Settings) -> dict:
    """Read runtime CMS config from state directory."""
    try:
        return json.loads(settings.cms_config_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_cms_config(settings: Settings, config: dict) -> None:
    """Write runtime CMS config to state directory atomically."""
    atomic_write(settings.cms_config_path, json.dumps(config, indent=2))


def _build_ws_url(host: str, port: int) -> str:
    """Build a WebSocket URL from host and port."""
    return f"ws://{host}:{port}{CMS_WS_PATH}"


@router.get("/config")
async def get_cms_config(settings: Settings = Depends(get_settings)):
    config = _read_cms_config(settings)

    # Check if CMS client service is running
    service_active = False
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "agora-cms-client"],
            capture_output=True, text=True, timeout=5,
        )
        service_active = result.stdout.strip() == "active"
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Check if we have an auth token (meaning we've registered)
    has_auth_token = settings.auth_token_path.exists()

    cms_host = config.get("cms_host", "")
    cms_port = config.get("cms_port", DEFAULT_CMS_PORT)

    return {
        "cms_host": cms_host,
        "cms_port": cms_port,
        "has_auth_token": has_auth_token,
        "service_active": service_active,
        "configured": bool(cms_host),
    }


@router.post("/config")
async def set_cms_config(request: Request, settings: Settings = Depends(get_settings)):
    body = await request.json()

    cms_host = body.get("cms_host", "").strip()
    cms_port = int(body.get("cms_port", DEFAULT_CMS_PORT))

    if not cms_host:
        raise HTTPException(status_code=400, detail="Server address is required")

    # Strip any accidental protocol prefix the user might paste
    for prefix in ("ws://", "wss://", "http://", "https://"):
        if cms_host.startswith(prefix):
            cms_host = cms_host[len(prefix):]
    # Strip trailing slashes or paths
    cms_host = cms_host.split("/")[0]
    # If user included port in the host string, extract it
    if ":" in cms_host:
        parts = cms_host.rsplit(":", 1)
        cms_host = parts[0]
        try:
            cms_port = int(parts[1])
        except ValueError:
            pass

    config = _read_cms_config(settings)
    config["cms_host"] = cms_host
    config["cms_port"] = cms_port
    config["cms_url"] = _build_ws_url(cms_host, cms_port)
    _write_cms_config(settings, config)

    # Try to restart the CMS client service so it picks up the new config
    try:
        subprocess.run(
            ["sudo", "systemctl", "restart", "agora-cms-client"],
            capture_output=True, timeout=10,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    return {"status": "ok", "cms_host": cms_host, "cms_port": cms_port}
