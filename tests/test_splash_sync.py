"""Tests for CMS-managed splash via sync messages.

When the CMS sends a splash field in the sync message, the device should
persist it to persist/splash so the player uses the correct splash screen
on reboot (even without CMS connectivity).
"""

import json
import sys
from unittest.mock import MagicMock

import pytest

# Mock heavy dependencies before importing CMS client
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from api.config import Settings
from cms_client.service import CMSClient


def _make_client(tmp_path):
    settings = Settings(
        agora_base=tmp_path,
        api_key="test",
        web_username="admin",
        web_password="test",
        secret_key="test",
        device_name="test",
    )
    settings.ensure_dirs()
    return CMSClient(settings)


def _sync_msg(splash=None, default_asset=None):
    msg = {
        "type": "sync",
        "timezone": "UTC",
        "schedules": [],
        "default_asset": default_asset,
    }
    if splash is not None:
        msg["splash"] = splash
    return msg


class TestSplashSync:
    """_handle_sync should persist the splash field to persist/splash."""

    @pytest.mark.asyncio
    async def test_splash_written_from_sync(self, tmp_path):
        """Sync with splash field writes persist/splash."""
        client = _make_client(tmp_path)
        await client._handle_sync(_sync_msg(splash="custom.png"))

        splash_path = client.settings.splash_config_path
        assert splash_path.is_file()
        assert splash_path.read_text().strip() == "custom.png"

    @pytest.mark.asyncio
    async def test_splash_updated_on_change(self, tmp_path):
        """Subsequent sync with different splash updates persist/splash."""
        client = _make_client(tmp_path)
        await client._handle_sync(_sync_msg(splash="old.png"))
        assert client.settings.splash_config_path.read_text().strip() == "old.png"

        await client._handle_sync(_sync_msg(splash="new.png"))
        assert client.settings.splash_config_path.read_text().strip() == "new.png"

    @pytest.mark.asyncio
    async def test_splash_none_removes_config(self, tmp_path):
        """Sync with no splash removes persist/splash (fall back to stock)."""
        client = _make_client(tmp_path)

        # First set a splash
        await client._handle_sync(_sync_msg(splash="custom.png"))
        assert client.settings.splash_config_path.is_file()

        # Then clear it
        await client._handle_sync(_sync_msg(splash=None))
        assert not client.settings.splash_config_path.is_file()

    @pytest.mark.asyncio
    async def test_splash_missing_field_removes_config(self, tmp_path):
        """Sync without splash key at all removes persist/splash."""
        client = _make_client(tmp_path)

        # Set a splash first
        await client._handle_sync(_sync_msg(splash="custom.png"))
        assert client.settings.splash_config_path.is_file()

        # Sync without splash field
        await client._handle_sync({"type": "sync", "timezone": "UTC", "schedules": []})
        assert not client.settings.splash_config_path.is_file()

    @pytest.mark.asyncio
    async def test_no_splash_no_file_noop(self, tmp_path):
        """Sync with no splash when no persist/splash exists is a no-op."""
        client = _make_client(tmp_path)
        assert not client.settings.splash_config_path.is_file()

        await client._handle_sync(_sync_msg(splash=None))
        assert not client.settings.splash_config_path.is_file()

    @pytest.mark.asyncio
    async def test_splash_empty_string_removes_config(self, tmp_path):
        """Sync with empty splash string removes persist/splash."""
        client = _make_client(tmp_path)
        await client._handle_sync(_sync_msg(splash="custom.png"))
        assert client.settings.splash_config_path.is_file()

        await client._handle_sync(_sync_msg(splash=""))
        assert not client.settings.splash_config_path.is_file()
