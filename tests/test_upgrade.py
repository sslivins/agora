"""Tests for CMS-triggered device upgrade handler."""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy dependencies before importing the service module
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient


class TestHandleUpgrade:
    """Tests for _handle_upgrade."""

    def _make_client(self, tmp_path):
        settings = MagicMock()
        settings.agora_base = tmp_path
        settings.assets_dir = tmp_path / "assets"
        settings.manifest_path = tmp_path / "state" / "assets.json"
        settings.schedule_path = tmp_path / "state" / "schedule.json"
        settings.desired_state_path = tmp_path / "state" / "desired.json"
        settings.cms_status_path = tmp_path / "state" / "cms_status.json"
        settings.asset_budget_mb = 100

        with patch.object(CMSClient, "__init__", lambda self, s: None):
            client = CMSClient(settings)
        client.settings = settings
        client.device_id = "test-device"
        client.asset_manager = MagicMock()
        client._write_cms_status = MagicMock()
        client._current_schedule_id = None
        client._current_schedule_name = None
        client._current_asset = None
        client._eval_wake = asyncio.Event()
        client._last_player_mode = None
        return client

    @pytest.mark.asyncio
    async def test_upgrade_bypasses_cdn_cache(self, tmp_path):
        """apt-get update must use No-Cache to bypass CDN stale metadata."""
        client = self._make_client(tmp_path)
        ws = AsyncMock()

        with patch("cms_client.service.subprocess.Popen") as mock_popen:
            await client._handle_upgrade(ws)

        mock_popen.assert_called_once()
        bash_cmd = mock_popen.call_args[0][0]
        # Find the bash -c command string
        bash_script = bash_cmd[bash_cmd.index("-c") + 1]
        assert "No-Cache=True" in bash_script or "No-Cache=true" in bash_script, (
            f"apt-get update should bypass CDN cache, got: {bash_script}"
        )

    @pytest.mark.asyncio
    async def test_upgrade_only_reboots_on_version_change(self, tmp_path):
        """Reboot should be conditional on the package actually being upgraded."""
        client = self._make_client(tmp_path)
        ws = AsyncMock()

        with patch("cms_client.service.subprocess.Popen") as mock_popen:
            await client._handle_upgrade(ws)

        bash_cmd = mock_popen.call_args[0][0]
        bash_script = bash_cmd[bash_cmd.index("-c") + 1]
        # Must compare version before/after and only reboot if changed
        assert "dpkg-query" in bash_script, "Must check package version"
        assert "reboot" in bash_script, "Must include reboot"
        # Reboot must be guarded by a version comparison, not unconditional
        # e.g. '[ "$OLD" != "$NEW" ] && reboot' rather than just 'reboot'
        reboot_idx = bash_script.index("reboot")
        before_reboot = bash_script[:reboot_idx]
        assert "!=" in before_reboot and "&&" in before_reboot, (
            f"Reboot must be conditional on version change, got: {bash_script}"
        )

    @pytest.mark.asyncio
    async def test_upgrade_sends_ack(self, tmp_path):
        """Handler sends upgrade_ack before starting the upgrade."""
        client = self._make_client(tmp_path)
        ws = AsyncMock()

        with patch("cms_client.service.subprocess.Popen"):
            await client._handle_upgrade(ws)

        ws.send.assert_called_once()
        import json
        msg = json.loads(ws.send.call_args[0][0])
        assert msg["type"] == "upgrade_ack"

    @pytest.mark.asyncio
    async def test_upgrade_runs_in_systemd_scope(self, tmp_path):
        """Upgrade must run in systemd-run --scope to survive service restart."""
        client = self._make_client(tmp_path)
        ws = AsyncMock()

        with patch("cms_client.service.subprocess.Popen") as mock_popen:
            await client._handle_upgrade(ws)

        bash_cmd = mock_popen.call_args[0][0]
        assert bash_cmd[0] == "systemd-run"
        assert "--scope" in bash_cmd
