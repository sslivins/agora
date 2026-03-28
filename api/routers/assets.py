import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile

from api.auth import get_settings, require_auth
from api.config import Settings
from shared.models import AssetInfo

router = APIRouter(dependencies=[Depends(require_auth)])

ALLOWED_EXTENSIONS = {".mp4", ".jpg", ".jpeg", ".png"}
SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$")


def _sanitize_filename(name: str) -> str:
    """Validate and sanitize an upload filename."""
    name = Path(name).name  # strip any path components
    if not SAFE_NAME_RE.match(name):
        raise HTTPException(status_code=400, detail="Invalid filename")
    return name


def _asset_type_for(name: str) -> str:
    ext = Path(name).suffix.lower()
    if ext == ".mp4":
        return "video"
    if ext in {".jpg", ".jpeg", ".png"}:
        return "image"
    raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")


def _target_dir(asset_type: str, settings: Settings) -> Path:
    if asset_type == "video":
        return settings.videos_dir
    return settings.images_dir


def _list_assets(settings: Settings) -> List[AssetInfo]:
    """List all assets across all subdirectories."""
    # Determine which asset is currently active (the "splash")
    active_asset = None
    try:
        import json
        desired = json.loads(settings.desired_state_path.read_text())
        if desired.get("mode") == "play":
            active_asset = desired.get("asset")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass

    assets: List[AssetInfo] = []
    seen: set[str] = set()
    for subdir, atype in [
        (settings.videos_dir, "video"),
        (settings.images_dir, "image"),
        (settings.splash_dir, "image"),
    ]:
        if not subdir.exists():
            continue
        for f in subdir.iterdir():
            if f.is_file() and f.suffix.lower() in ALLOWED_EXTENSIONS and f.name not in seen:
                seen.add(f.name)
                stat = f.stat()
                # Mark the currently playing asset as "active"
                effective_type = "active" if f.name == active_asset else atype
                assets.append(
                    AssetInfo(
                        name=f.name,
                        size=stat.st_size,
                        modified_at=datetime.fromtimestamp(
                            stat.st_mtime, tz=timezone.utc
                        ),
                        asset_type=effective_type,
                    )
                )
    return sorted(assets, key=lambda a: a.name)


@router.post("/assets/upload", response_model=AssetInfo)
async def upload_asset(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    form = await request.form(max_part_size=settings.max_upload_bytes)
    try:
        file: UploadFile = form["file"]
        name = _sanitize_filename(file.filename or "upload.mp4")
        ext = Path(name).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

        atype = _asset_type_for(name)
        target_dir = _target_dir(atype, settings)
        target = target_dir / name

        # Atomic write: temp file in same directory, then rename
        fd, tmp_path = tempfile.mkstemp(dir=str(target_dir), suffix=".tmp")
        try:
            total = 0
            with os.fdopen(fd, "wb") as f:
                while chunk := await file.read(256 * 1024):
                    total += len(chunk)
                    if total > settings.max_upload_bytes:
                        raise HTTPException(status_code=413, detail="File too large")
                    f.write(chunk)
            os.replace(tmp_path, target)
        except HTTPException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise HTTPException(status_code=500, detail="Upload failed")

        stat = target.stat()
        return AssetInfo(
            name=name,
            size=stat.st_size,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            asset_type=atype,
        )
    finally:
        await form.close()


@router.get("/assets", response_model=List[AssetInfo])
async def list_assets(settings: Settings = Depends(get_settings)):
    return _list_assets(settings)


@router.delete("/assets/{name}")
async def delete_asset(name: str, settings: Settings = Depends(get_settings)):
    name = _sanitize_filename(name)
    for subdir in [settings.videos_dir, settings.images_dir, settings.splash_dir]:
        target = subdir / name
        if target.is_file():
            target.unlink()
            return {"deleted": name}
    raise HTTPException(status_code=404, detail="Asset not found")


@router.post("/assets/{name}/set-splash")
async def set_splash(name: str, settings: Settings = Depends(get_settings)):
    name = _sanitize_filename(name)
    # Verify the asset exists
    found = False
    for subdir in [settings.videos_dir, settings.images_dir, settings.splash_dir]:
        if (subdir / name).is_file():
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail="Asset not found")
    # Write the asset name to the splash config file
    settings.splash_config_path.write_text(name)
    return {"splash": name}


@router.delete("/assets/splash")
async def clear_splash(settings: Settings = Depends(get_settings)):
    """Clear the user-set splash, reverting to the default."""
    if settings.splash_config_path.is_file():
        settings.splash_config_path.unlink()
    return {"splash": None}
