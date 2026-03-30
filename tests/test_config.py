"""Tests for Settings configuration."""

import json

from api.config import Settings


class TestSettings:
    def test_defaults(self, tmp_path):
        s = Settings(agora_base=tmp_path)
        assert s.web_username == "admin"
        assert s.web_password == "agora"
        assert s.max_upload_bytes == 500 * 1024 * 1024
        assert s.device_name == "agora-node"
        assert s.cms_url == ""

    def test_derived_paths(self, tmp_path):
        s = Settings(agora_base=tmp_path)
        assert s.assets_dir == tmp_path / "assets"
        assert s.videos_dir == tmp_path / "assets" / "videos"
        assert s.images_dir == tmp_path / "assets" / "images"
        assert s.splash_dir == tmp_path / "assets" / "splash"
        assert s.state_dir == tmp_path / "state"
        assert s.persist_dir == tmp_path / "persist"
        assert s.log_dir == tmp_path / "logs"
        assert s.desired_state_path == tmp_path / "state" / "desired.json"
        assert s.current_state_path == tmp_path / "state" / "current.json"

    def test_ensure_dirs(self, tmp_path):
        s = Settings(agora_base=tmp_path)
        s.ensure_dirs()
        assert s.videos_dir.exists()
        assert s.images_dir.exists()
        assert s.splash_dir.exists()
        assert s.state_dir.exists()
        assert s.persist_dir.exists()
        assert s.log_dir.exists()

    def test_persistent_paths_on_persist_dir(self, tmp_path):
        """Auth token, CMS config, splash config, and API key must be on
        persist_dir (flash) — not on state_dir (tmpfs)."""
        s = Settings(agora_base=tmp_path)
        assert s.auth_token_path == tmp_path / "persist" / "cms_auth_token"
        assert s.cms_config_path == tmp_path / "persist" / "cms_config.json"
        assert s.splash_config_path == tmp_path / "persist" / "splash"
        # Ephemeral files stay in state_dir
        assert s.desired_state_path.parent == s.state_dir
        assert s.current_state_path.parent == s.state_dir
        assert s.schedule_path.parent == s.state_dir
        assert s.manifest_path.parent == s.state_dir

    def test_env_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AGORA_DEVICE_NAME", "my-pi")
        monkeypatch.setenv("AGORA_WEB_USERNAME", "custom")
        s = Settings(agora_base=tmp_path)
        assert s.device_name == "my-pi"
        assert s.web_username == "custom"

    def test_api_key_generated(self, tmp_path):
        s = Settings(agora_base=tmp_path)
        assert len(s.api_key) > 10  # random token generated

    def test_secret_key_generated(self, tmp_path):
        s = Settings(agora_base=tmp_path)
        assert len(s.secret_key) > 10
