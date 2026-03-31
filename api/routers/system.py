"""Factory reset API endpoint."""

import logging
import shutil
import subprocess
from pathlib import Path

from fastapi import APIRouter, Depends

from api.auth import get_settings, require_auth
from api.config import Settings

logger = logging.getLogger("agora.api.factory_reset")

router = APIRouter(prefix="/system", dependencies=[Depends(require_auth)])

PERSIST_DIR = Path("/opt/agora/persist")
STATE_DIR = Path("/opt/agora/state")


@router.post("/factory-reset")
async def factory_reset(settings: Settings = Depends(get_settings)):
    """Perform a factory reset: wipe config, assets, Wi-Fi, and reboot.

    This clears:
    - All assets (videos, images)
    - CMS config and auth token
    - Saved Wi-Fi connections
    - Provisioning flag (so captive portal runs on next boot)
    - Device name override
    """
    logger.warning("Factory reset initiated")

    # 1. Remove provisioning flag
    _safe_unlink(PERSIST_DIR / "provisioned")

    # 2. Remove CMS config and auth
    _safe_unlink(PERSIST_DIR / "cms_config.json")
    _safe_unlink(PERSIST_DIR / "cms_auth_token")
    _safe_unlink(PERSIST_DIR / "device_name")
    _safe_unlink(PERSIST_DIR / "api_key")

    # 3. Remove state files
    _safe_unlink(STATE_DIR / "cms_status.json")
    _safe_unlink(STATE_DIR / "cms_config.json")
    _safe_unlink(STATE_DIR / "schedule.json")
    _safe_unlink(STATE_DIR / "assets.json")

    # 4. Wipe assets on disk
    for subdir in [settings.videos_dir, settings.images_dir]:
        if subdir.exists():
            for f in subdir.iterdir():
                if f.is_file():
                    try:
                        f.unlink()
                    except OSError:
                        pass

    # 5. Forget all saved Wi-Fi connections
    try:
        from provision.network import forget_all_wifi
        forget_all_wifi()
    except ImportError:
        # Fallback if provision package not available
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

    # 6. Reboot the device
    logger.warning("Factory reset complete — rebooting")
    try:
        subprocess.Popen(
            ["sudo", "systemctl", "reboot"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    return {"status": "ok", "message": "Factory reset complete. Device is rebooting."}


def _safe_unlink(path: Path) -> None:
    """Delete a file without raising on missing."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
