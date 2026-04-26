"""Tests for OOBE provisioning flow logic.

Focuses on Phase 2 (CMS adoption) behavior:
- Player must NOT be pre-started during OOBE (only agora-api)
- CMS failure/timeout must loop through reconfigure, never fall through
- show_adopted only called after successful adoption
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock cairo and gi before importing provision modules (not available on CI/Windows)
sys.modules.setdefault("cairo", MagicMock())
sys.modules.setdefault("gi", MagicMock())
sys.modules.setdefault("gi.repository", MagicMock())

from provision.service import (
    _wait_for_cms_adoption,
)


class TestWaitForCmsAdoption:
    """Test _wait_for_cms_adoption return values."""

    @pytest.mark.asyncio
    async def test_returns_no_cms_when_no_host_configured(self):
        """Should return 'no_cms' when no CMS host is configured."""
        shutdown = asyncio.Event()
        with patch("provision.service._get_cms_host", return_value=""):
            result = await _wait_for_cms_adoption(None, shutdown)
        assert result == "no_cms"

    @pytest.mark.asyncio
    async def test_returns_adopted_when_registered(self):
        """Should return 'adopted' when CMS reports connected+registered."""
        shutdown = asyncio.Event()
        status_sequence = [
            {"state": "connecting"},
            {"state": "connected", "registration": "registered"},
        ]
        call_count = 0

        def mock_read_status():
            nonlocal call_count
            if call_count < len(status_sequence):
                result = status_sequence[call_count]
                call_count += 1
                return result
            return status_sequence[-1]

        with patch("provision.service._get_cms_host", return_value="192.168.1.1"), \
             patch("provision.service._read_cms_status", side_effect=mock_read_status), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = await _wait_for_cms_adoption(None, shutdown)
        assert result == "adopted"

    @pytest.mark.asyncio
    async def test_returns_failed_after_error_threshold(self):
        """Should return 'failed' after CMS_ERROR_THRESHOLD consecutive errors."""
        from datetime import datetime, timezone
        shutdown = asyncio.Event()

        def mock_read_status():
            # Fresh timestamp so the stale-negative guard doesn't filter it.
            return {
                "state": "error",
                "error": "Connection refused",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        with patch("provision.service._get_cms_host", return_value="192.168.1.1"), \
             patch("provision.service._read_cms_status", side_effect=mock_read_status), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = await _wait_for_cms_adoption(None, shutdown)
        assert result == "failed"

    @pytest.mark.asyncio
    async def test_disconnected_with_error_counts_as_failure(self):
        """A 'disconnected' state with an error field should count toward
        the error threshold (e.g. connection timeout)."""
        from datetime import datetime, timezone
        shutdown = asyncio.Event()

        def mock_read_status():
            return {
                "state": "disconnected",
                "error": "timed out during handshake",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        with patch("provision.service._get_cms_host", return_value="192.168.1.1"), \
             patch("provision.service._read_cms_status", side_effect=mock_read_status), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = await _wait_for_cms_adoption(None, shutdown)
        assert result == "failed"

    @pytest.mark.asyncio
    async def test_returns_shutdown_when_event_set(self):
        """Should return 'shutdown' when shutdown event is set."""
        shutdown = asyncio.Event()
        shutdown.set()
        with patch("provision.service._get_cms_host", return_value="192.168.1.1"), \
             patch("provision.service._read_cms_status", return_value={}):
            result = await _wait_for_cms_adoption(None, shutdown)
        assert result == "shutdown"


class TestOobeServicePrestart:
    """Verify that Phase 1 only pre-starts agora-api, not agora-player."""

    @pytest.mark.asyncio
    async def test_phase1_only_starts_api_not_player(self):
        """After Wi-Fi connects, only agora-api should be started, not agora-player."""
        from provision import service

        import inspect
        source = inspect.getsource(service.run_service)

        # Extract the section between Wi-Fi connected and entering CMS adoption
        wifi_to_phase2 = source.split("Wi-Fi connected successfully")[1].split("Entering CMS adoption phase")[0]

        # agora-api should be started, agora-player should NOT
        assert "agora-api" in wifi_to_phase2
        assert "agora-player" not in wifi_to_phase2


class TestOobePhase2NoFallthrough:
    """Verify that Phase 2 never falls through to player on CMS failure."""

    def test_phase2_no_unconditional_break_on_failure(self):
        """The failed/timeout path should not have an unconditional break."""
        import inspect
        from provision import service

        source = inspect.getsource(service.run_service)

        # Extract Phase 2 loop
        phase2 = source.split("Phase 2: CMS adoption")[1].split("display.close()")[0]

        # After "Shutdown or gave up — proceed anyway" should NOT exist
        assert "proceed anyway" not in phase2, \
            "Phase 2 should not have a fallthrough 'proceed anyway' break"

        # The adoption_success flag should gate show_adopted
        assert "adoption_success" in source, \
            "show_adopted should be gated by adoption_success flag"
