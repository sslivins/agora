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
    device_name: str = "agora-node"

    # Splash
    default_splash: str = "splash/default.png"

    # CMS connection
    cms_url: str = ""  # e.g. ws://192.168.1.100:8080/ws/device

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
    def splash_config_path(self) -> Path:
        return self.state_dir / "splash"

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
        return self.state_dir / "cms_auth_token"

    @property
    def cms_config_path(self) -> Path:
        return self.state_dir / "cms_config.json"

    def ensure_dirs(self) -> None:
        for d in [
            self.videos_dir,
            self.images_dir,
            self.splash_dir,
            self.state_dir,
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
    return Settings(**overrides)
