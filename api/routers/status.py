import hashlib
import time

from fastapi import APIRouter, Depends, Request

from api import __version__
from api.auth import get_settings, require_auth
from api.config import Settings
from shared.models import CurrentState, DesiredState, HealthResponse, StatusResponse
from shared.state import read_state

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(request: Request, settings: Settings = Depends(get_settings)):
    return HealthResponse(
        device_name=settings.device_name,
        version=__version__,
        uptime_seconds=time.time() - request.app.state.start_time,
    )


@router.get("/status", response_model=StatusResponse, dependencies=[Depends(require_auth)])
async def get_status(request: Request, settings: Settings = Depends(get_settings)):
    current = read_state(settings.current_state_path, CurrentState)
    desired = read_state(settings.desired_state_path, DesiredState)
    asset_count = 0
    for subdir in [settings.videos_dir, settings.images_dir]:
        if subdir.exists():
            asset_count += sum(1 for f in subdir.iterdir() if f.is_file())
    schedule_hash = ""
    try:
        raw = settings.schedule_path.read_bytes()
        schedule_hash = hashlib.md5(raw).hexdigest()
    except (FileNotFoundError, OSError):
        pass

    return StatusResponse(
        device_name=settings.device_name,
        current_state=current,
        desired_state=desired,
        asset_count=asset_count,
        schedule_hash=schedule_hash,
    )
