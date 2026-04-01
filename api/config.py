import json
import secrets
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "AGORA_"}

    # Paths
    agora_base: Path = Path("/opt/agora")

    # Auth
    api_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    web_username: str = "admin"
    web_password: str = "agora"
    secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))

    # Limits
    max_upload_bytes: int = 500 * 1024 * 1024  # 500 MB

    # Device
    device_name: str = ""

    # Splash
    default_splash: str = "splash/default.png"

    # CMS connection
    cms_url: str = ""  # e.g. ws://192.168.1.100:8080/ws/device

    # Asset budget (0 = 80% of partition)
    asset_budget_mb: int = 0

    @property
    def assets_dir(self) -> Path:
        return self.agora_base / "assets"

    @property
    def videos_dir(self) -> Path:
        return self.assets_dir / "videos"

    @property
    def images_dir(self) -> Path:
        return self.assets_dir / "images"

    @property
    def splash_dir(self) -> Path:
        return self.assets_dir / "splash"

    @property
    def state_dir(self) -> Path:
        return self.agora_base / "state"

    @property
    def persist_dir(self) -> Path:
        return self.agora_base / "persist"

    @property
    def splash_config_path(self) -> Path:
        return self.persist_dir / "splash"

    @property
    def log_dir(self) -> Path:
        return self.agora_base / "logs"

    @property
    def desired_state_path(self) -> Path:
        return self.state_dir / "desired.json"

    @property
    def current_state_path(self) -> Path:
        return self.state_dir / "current.json"

    @property
    def auth_token_path(self) -> Path:
        return self.persist_dir / "cms_auth_token"

    @property
    def cms_config_path(self) -> Path:
        return self.persist_dir / "cms_config.json"

    @property
    def cms_status_path(self) -> Path:
        return self.state_dir / "cms_status.json"

    @property
    def schedule_path(self) -> Path:
        return self.state_dir / "schedule.json"

    @property
    def manifest_path(self) -> Path:
        return self.state_dir / "assets.json"

    def ensure_dirs(self) -> None:
        for d in [
            self.videos_dir,
            self.images_dir,
            self.splash_dir,
            self.state_dir,
            self.persist_dir,
            self.log_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    """Load settings from optional boot config, overlaid by env vars."""
    boot_config = Path("/boot/agora-config.json")
    overrides: dict = {}
    if boot_config.exists():
        try:
            overrides = json.loads(boot_config.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    # Check persist file (set via captive portal provisioning)
    persist_name = Path("/opt/agora/persist/device_name")
    if not overrides.get("device_name"):
        try:
            name = persist_name.read_text().strip()
            if name:
                overrides["device_name"] = name
        except (FileNotFoundError, OSError):
            pass

    settings = Settings(**overrides)

    # Generate unique default if still empty
    if not settings.device_name:
        from shared.identity import get_device_serial_suffix
        settings.device_name = f"agora-node-{get_device_serial_suffix(4)}"

    return settings
