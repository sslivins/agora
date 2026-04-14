"""Tests for CMS-managed splash via sync messages.

When the CMS sends a splash field in the sync message, the device should
persist it to persist/splash so the player uses the correct splash screen
on reboot (even without CMS connectivity).
"""

import json
import sys
from unittest.mock import MagicMock, patch

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


class TestSplashPersistOrdering:
    """Verify splash config is persisted BEFORE _evaluate_schedule writes desired.json.

    The player reacts to desired.json changes via inotify and immediately reads
    the splash config in _find_splash(). If _evaluate_schedule (which writes
    desired.json) runs before _persist_splash, the player reads a stale splash
    config and shows the wrong asset.
    """

    @pytest.mark.asyncio
    async def test_splash_persisted_before_evaluate(self, tmp_path):
        """_persist_splash must be called before _evaluate_schedule in _handle_sync."""
        client = _make_client(tmp_path)
        call_order = []

        original_persist = client._persist_splash
        original_evaluate = client._evaluate_schedule

        async def tracked_persist(splash):
            call_order.append("persist_splash")
            await original_persist(splash)

        def tracked_evaluate(msg):
            call_order.append("evaluate_schedule")
            original_evaluate(msg)

        with patch.object(client, "_persist_splash", side_effect=tracked_persist), \
             patch.object(client, "_evaluate_schedule", side_effect=tracked_evaluate):
            await client._handle_sync(_sync_msg(splash="custom.png"))

        assert "persist_splash" in call_order
        assert "evaluate_schedule" in call_order
        assert call_order.index("persist_splash") < call_order.index("evaluate_schedule"), \
            f"persist_splash must run before evaluate_schedule, got: {call_order}"

    @pytest.mark.asyncio
    async def test_splash_cleared_before_evaluate_on_removal(self, tmp_path):
        """When splash is removed, persist (delete) must happen before evaluate."""
        client = _make_client(tmp_path)
        # Set an initial splash
        await client._handle_sync(_sync_msg(splash="video.mp4"))
        assert client.settings.splash_config_path.is_file()

        call_order = []

        original_persist = client._persist_splash
        original_evaluate = client._evaluate_schedule

        async def tracked_persist(splash):
            call_order.append("persist_splash")
            await original_persist(splash)

        def tracked_evaluate(msg):
            call_order.append("evaluate_schedule")
            original_evaluate(msg)

        with patch.object(client, "_persist_splash", side_effect=tracked_persist), \
             patch.object(client, "_evaluate_schedule", side_effect=tracked_evaluate):
            await client._handle_sync(_sync_msg(splash=None))

        assert call_order.index("persist_splash") < call_order.index("evaluate_schedule")
        # Splash file should be gone
        assert not client.settings.splash_config_path.is_file()

    @pytest.mark.asyncio
    async def test_splash_config_correct_when_desired_json_written(self, tmp_path):
        """Simulate the race: verify splash config is already updated when
        desired.json is written by _evaluate_schedule."""
        client = _make_client(tmp_path)

        # Set splash to "video.mp4" initially
        await client._handle_sync(_sync_msg(splash="video.mp4", default_asset="video.mp4"))
        assert client.settings.splash_config_path.read_text().strip() == "video.mp4"

        # Track what splash config contains at the moment _evaluate_schedule runs
        splash_at_eval_time = []

        original_evaluate = client._evaluate_schedule

        def spy_evaluate(msg):
            # Read splash config at the moment evaluate fires
            path = client.settings.splash_config_path
            if path.is_file():
                splash_at_eval_time.append(path.read_text().strip())
            else:
                splash_at_eval_time.append(None)
            original_evaluate(msg)

        with patch.object(client, "_evaluate_schedule", side_effect=spy_evaluate):
            # Now sync with splash=None (removing the splash)
            await client._handle_sync(_sync_msg(splash=None))

        # At the time evaluate ran, splash config should already be gone
        assert splash_at_eval_time[0] is None, \
            f"Splash config should be removed before evaluate, but was: {splash_at_eval_time[0]}"
