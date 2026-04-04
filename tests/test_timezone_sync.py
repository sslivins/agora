"""Tests for CMS → device timezone synchronization."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from cms_client.service import CMSClient


@pytest.fixture
def client(tmp_path):
    """Create a CMSClient with mocked settings."""
    settings = MagicMock()
    settings.cms_config_path = tmp_path / "cms.json"
    settings.cms_url = ""
    settings.manifest_path = tmp_path / "manifest.json"
    settings.assets_dir = tmp_path / "assets"
    settings.videos_dir = tmp_path / "videos"
    settings.images_dir = tmp_path / "images"
    settings.splash_dir = tmp_path / "splash"
    settings.asset_budget_mb = 500
    for d in (settings.assets_dir, settings.videos_dir, settings.images_dir, settings.splash_dir):
        d.mkdir(parents=True, exist_ok=True)
    with patch.object(CMSClient, "__init__", lambda self, s: None):
        c = CMSClient(settings)
        c.settings = settings
    return c


class TestApplyTimezone:
    """Unit tests for _apply_timezone."""

    def test_sets_timezone_when_different(self, client):
        """Should call timedatectl set-timezone when current differs."""
        show_result = MagicMock(stdout="Etc/UTC\n", returncode=0)
        set_result = MagicMock(returncode=0, stderr="")
        with patch("cms_client.service.subprocess.run", side_effect=[show_result, set_result]) as mock_run:
            client._apply_timezone("America/Los_Angeles")
        assert mock_run.call_count == 2
        mock_run.assert_any_call(
            ["timedatectl", "show", "--property=Timezone", "--value"],
            capture_output=True, text=True, timeout=5,
        )
        mock_run.assert_any_call(
            ["sudo", "timedatectl", "set-timezone", "America/Los_Angeles"],
            capture_output=True, text=True, timeout=5,
        )

    def test_skips_when_same(self, client):
        """Should not call set-timezone when already correct."""
        show_result = MagicMock(stdout="America/Los_Angeles\n", returncode=0)
        with patch("cms_client.service.subprocess.run", return_value=show_result) as mock_run:
            client._apply_timezone("America/Los_Angeles")
        assert mock_run.call_count == 1

    def test_handles_set_failure(self, client):
        """Should log warning when set-timezone fails (non-zero exit)."""
        show_result = MagicMock(stdout="Etc/UTC\n", returncode=0)
        set_result = MagicMock(returncode=1, stderr="Invalid timezone")
        with patch("cms_client.service.subprocess.run", side_effect=[show_result, set_result]):
            # Should not raise
            client._apply_timezone("Invalid/Zone")

    def test_handles_exception(self, client):
        """Should handle subprocess exceptions gracefully."""
        with patch("cms_client.service.subprocess.run", side_effect=OSError("no timedatectl")):
            # Should not raise
            client._apply_timezone("America/New_York")

    def test_handles_timeout(self, client):
        """Should handle subprocess timeout gracefully."""
        with patch("cms_client.service.subprocess.run", side_effect=subprocess.TimeoutExpired("timedatectl", 5)):
            # Should not raise
            client._apply_timezone("America/New_York")


class TestHandleSyncTimezone:
    """Tests that _handle_sync triggers timezone application."""

    @pytest.mark.asyncio
    async def test_sync_applies_timezone(self, client, tmp_path):
        """_handle_sync should call _apply_timezone with the timezone from the message."""
        client._last_eval_state = None
        client._ws = None
        schedule_path = tmp_path / "schedule.json"
        client.settings.schedule_path = schedule_path
        client.settings.splash_config_path = tmp_path / "splash.txt"
        client.settings.cms_status_path = tmp_path / "cms_status.json"
        client.asset_manager = MagicMock()

        with patch.object(client, "_apply_timezone") as mock_tz, \
             patch.object(client, "_evaluate_schedule"), \
             patch.object(client, "_write_cms_status"):
            await client._handle_sync({"timezone": "America/Chicago", "schedules": []})

        mock_tz.assert_called_once_with("America/Chicago")

    @pytest.mark.asyncio
    async def test_sync_without_timezone_skips(self, client, tmp_path):
        """_handle_sync should not call _apply_timezone when timezone is absent."""
        client._last_eval_state = None
        client._ws = None
        schedule_path = tmp_path / "schedule.json"
        client.settings.schedule_path = schedule_path
        client.settings.splash_config_path = tmp_path / "splash.txt"
        client.settings.cms_status_path = tmp_path / "cms_status.json"
        client.asset_manager = MagicMock()

        with patch.object(client, "_apply_timezone") as mock_tz, \
             patch.object(client, "_evaluate_schedule"), \
             patch.object(client, "_write_cms_status"):
            await client._handle_sync({"schedules": []})

        mock_tz.assert_not_called()
