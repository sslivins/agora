import hmac
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from api.auth import (
    WebAuthRequired,
    clear_session,
    create_session,
    get_settings,
    require_web_auth,
)
from api.config import Settings
from shared.models import CurrentState, DesiredState
from shared.state import read_state

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@router.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html")


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    settings: Settings = Depends(get_settings),
):
    # Check for CMS-pushed password override, fall back to boot config
    effective_password = settings.web_password
    override_path = settings.state_dir / "web_password"
    try:
        override = override_path.read_text().strip()
        if override:
            effective_password = override
    except (FileNotFoundError, OSError):
        pass

    if hmac.compare_digest(username, settings.web_username) and hmac.compare_digest(
        password, effective_password
    ):
        response = RedirectResponse("/", status_code=303)
        create_session(response, username, settings)
        return response
    return templates.TemplateResponse(
        request,
        "login.html",
        context={"error": "Invalid credentials"},
        status_code=401,
    )


@router.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=303)
    clear_session(response)
    return response


@router.get("/")
async def dashboard(
    request: Request,
    user: str = Depends(require_web_auth),
    settings: Settings = Depends(get_settings),
):
    import shutil
    from pathlib import Path

    current = read_state(settings.current_state_path, CurrentState)
    desired = read_state(settings.desired_state_path, DesiredState)
    asset_count = 0
    for subdir in [settings.videos_dir, settings.images_dir]:
        if subdir.exists():
            asset_count += sum(1 for f in subdir.iterdir() if f.is_file())

    # Storage info
    try:
        usage = shutil.disk_usage(settings.assets_dir)
        storage_total_mb = int(usage.total / (1024 * 1024))
        storage_used_mb = int(usage.used / (1024 * 1024))
        storage_free_mb = int(usage.free / (1024 * 1024))
        storage_pct_free = round(usage.free / usage.total * 100) if usage.total else 0
    except OSError:
        storage_total_mb = storage_used_mb = storage_free_mb = storage_pct_free = 0

    # Device type
    try:
        device_type = Path("/proc/device-tree/model").read_text().strip().rstrip("\x00")
    except (FileNotFoundError, OSError):
        device_type = ""

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        context={
            "user": user,
            "current": current,
            "desired": desired,
            "asset_count": asset_count,
            "device_name": settings.device_name,
            "device_type": device_type,
            "storage_total_mb": storage_total_mb,
            "storage_used_mb": storage_used_mb,
            "storage_free_mb": storage_free_mb,
            "storage_pct_free": storage_pct_free,
        },
    )


@router.get("/assets")
async def assets_page(
    request: Request,
    user: str = Depends(require_web_auth),
    settings: Settings = Depends(get_settings),
):
    from api.routers.assets import _list_assets

    assets = _list_assets(settings)
    return templates.TemplateResponse(
        request,
        "assets.html",
        context={"user": user, "assets": assets},
    )


@router.get("/playback")
async def playback_page(
    request: Request,
    user: str = Depends(require_web_auth),
    settings: Settings = Depends(get_settings),
):
    from api.routers.assets import _list_assets

    current = read_state(settings.current_state_path, CurrentState)
    assets = _list_assets(settings)
    return templates.TemplateResponse(
        request,
        "playback.html",
        context={"user": user, "current": current, "assets": assets},
    )


@router.get("/setup")
async def setup_page(
    request: Request,
    user: str = Depends(require_web_auth),
    settings: Settings = Depends(get_settings),
):
    import json
    import subprocess

    cms_host = ""
    cms_port = ""
    try:
        config = json.loads(settings.cms_config_path.read_text())
        cms_host = config.get("cms_host", "")
        cms_port = config.get("cms_port", "")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    service_active = False
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "agora-cms-client"],
            capture_output=True, text=True, timeout=5,
        )
        service_active = result.stdout.strip() == "active"
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    has_auth_token = settings.auth_token_path.exists()
    configured = bool(cms_host)

    return templates.TemplateResponse(
        request,
        "setup.html",
        context={
            "user": user,
            "cms_host": cms_host,
            "cms_port": cms_port,
            "configured": configured,
            "service_active": service_active,
            "has_auth_token": has_auth_token,
        },
    )
