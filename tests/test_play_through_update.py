"""Tests for play-through-update: editing a currently-playing asset must
not blink to splash while the new bundle downloads.

When content that is already on screen is edited in the CMS, its checksum
changes. The schedule evaluator used to react to the now-missing checksum
by writing ``desired.json = SPLASH`` and fetching the new bytes — so the
device dropped to the splash screen for ~15s mid-download before resuming.

Desired behavior: if a prior version of the *same* logical asset is on
disk AND is what we're currently displaying, keep playing it (leave
desired.json untouched) while the new bundle downloads in the background,
then swap to the new content once it lands. A genuinely cold asset (no
prior version, or not currently on screen) still splashes while fetching.
"""

import sys
from unittest.mock import MagicMock

import pytest

# Mock heavy dependencies before importing CMS client
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from api.config import Settings
from cms_client.service import CMSClient
from shared.models import DesiredState, PlaybackMode
from shared.state import read_state, write_state


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


def _schedule_sync(asset, checksum, asset_type="video"):
    """A sync payload with one always-active schedule for ``asset``."""
    return {
        "type": "sync",
        "timezone": "UTC",
        "default_asset": None,
        "schedules": [
            {
                "id": "s1",
                "name": "always",
                "priority": 1,
                "asset": asset,
                "asset_checksum": checksum,
                "asset_type": asset_type,
                "loop_count": None,
                "start_time": "00:00:00",
                "end_time": "23:59:59",
            }
        ],
    }


def _default_sync(asset, checksum):
    """A sync payload with no schedules and ``asset`` as the default."""
    return {
        "type": "sync",
        "timezone": "UTC",
        "default_asset": asset,
        "default_asset_checksum": checksum,
        "schedules": [],
    }


def _read_desired(client):
    return read_state(client.settings.desired_state_path, DesiredState)


class TestScheduledPlayThroughUpdate:
    def test_edit_currently_playing_scheduled_asset_does_not_splash(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        # Device is currently playing v.mp4 @ checksum A.
        client.asset_manager.register("v.mp4", "videos/v.mp4", 10, "A")
        client._last_eval_state = ("play", "v.mp4", "A", None, "video", None)
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset="v.mp4",
                asset_type="video",
                loop=True,
                expected_checksum="A",
            ),
        )

        # The asset is edited: same name, new checksum B (not on disk yet).
        client._evaluate_schedule(_schedule_sync("v.mp4", "B"))

        # Must NOT drop to splash — keep playing the current bytes.
        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.asset == "v.mp4"
        # New bytes are requested.
        client._request_asset_fetch.assert_called_with("v.mp4")
        # Distinct marker so we don't thrash on repeated evals.
        assert client._last_eval_state == ("updating", "v.mp4", "B")

    def test_swaps_to_new_content_once_download_completes(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register("v.mp4", "videos/v.mp4", 10, "A")
        client._last_eval_state = ("play", "v.mp4", "A", None, "video", None)
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset="v.mp4",
                asset_type="video",
                loop=True,
                expected_checksum="A",
            ),
        )

        # Edit lands → hold (no splash).
        client._evaluate_schedule(_schedule_sync("v.mp4", "B"))
        assert _read_desired(client).mode == PlaybackMode.PLAY
        assert client._last_eval_state == ("updating", "v.mp4", "B")

        # Download completes — new checksum now on disk.
        client.asset_manager.register("v.mp4", "videos/v.mp4", 12, "B")
        client._evaluate_schedule(_schedule_sync("v.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.asset == "v.mp4"
        assert desired.expected_checksum == "B"
        assert client._last_eval_state[0] == "play"
        assert client._last_eval_state[2] == "B"

    def test_cold_scheduled_asset_still_splashes(self, tmp_path):
        """No prior version on disk → splash while fetching (unchanged)."""
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        # Nothing registered, nothing on screen.
        client._evaluate_schedule(_schedule_sync("v.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.SPLASH
        client._request_asset_fetch.assert_called_with("v.mp4")
        assert client._last_eval_state == ("waiting", "v.mp4", "B")

    def test_asset_on_disk_but_not_on_screen_still_splashes(self, tmp_path):
        """A stale prior version exists but we're not currently showing it
        (e.g. just booted, last state was splash) → splash while fetching."""
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register("v.mp4", "videos/v.mp4", 10, "A")
        client._last_eval_state = ("splash", None)

        client._evaluate_schedule(_schedule_sync("v.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.SPLASH
        assert client._last_eval_state == ("waiting", "v.mp4", "B")


class TestDefaultPlayThroughUpdate:
    def test_edit_currently_playing_default_asset_does_not_splash(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register("d.mp4", "videos/d.mp4", 10, "A")
        client._last_eval_state = ("default", "d.mp4", "A", None)
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset="d.mp4",
                loop=True,
                expected_checksum="A",
            ),
        )

        client._evaluate_schedule(_default_sync("d.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.asset == "d.mp4"
        client._request_asset_fetch.assert_called_with("d.mp4")
        assert client._last_eval_state == ("updating", "d.mp4", "B")

    def test_default_swaps_after_download(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register("d.mp4", "videos/d.mp4", 10, "A")
        client._last_eval_state = ("default", "d.mp4", "A", None)
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset="d.mp4",
                loop=True,
                expected_checksum="A",
            ),
        )

        client._evaluate_schedule(_default_sync("d.mp4", "B"))
        assert client._last_eval_state == ("updating", "d.mp4", "B")

        client.asset_manager.register("d.mp4", "videos/d.mp4", 12, "B")
        client._evaluate_schedule(_default_sync("d.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.expected_checksum == "B"
        assert client._last_eval_state == ("default", "d.mp4", "B", None)

    def test_cold_default_asset_still_splashes(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client._evaluate_schedule(_default_sync("d.mp4", "B"))

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.SPLASH
        assert client._last_eval_state == ("waiting", "d.mp4", "B")


# Composed slide bundles embed the content hash in the *filename*
# (``composed-<uuid>-<hash>.html``), so every edit yields a brand-new
# filename for the same logical slide. The play-through logic must match
# on the logical id (filename minus the hash) rather than exact filename.
_OLD_COMPOSED = "composed-83d695d2-1384-4e32-a898-0ed33233d439-601ed9272f72.html"
_NEW_COMPOSED = "composed-83d695d2-1384-4e32-a898-0ed33233d439-23e37ab5afb8.html"
_OTHER_COMPOSED = "composed-11111111-2222-3333-4444-555555555555-aaaaaaaaaaaa.html"


class TestComposedPlayThroughUpdate:
    def test_edit_currently_playing_composed_slide_does_not_splash(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        # Device is showing the old composed bundle @ checksum A.
        client.asset_manager.register(_OLD_COMPOSED, f"videos/{_OLD_COMPOSED}", 10, "A")
        client._last_eval_state = ("play", _OLD_COMPOSED, "A", None, "composed", None)
        client._displayed_asset = _OLD_COMPOSED
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset=_OLD_COMPOSED,
                asset_type="composed",
                loop=True,
                expected_checksum="A",
            ),
        )

        # Edit lands: NEW filename (new hash), checksum B, not on disk yet.
        client._evaluate_schedule(
            _schedule_sync(_NEW_COMPOSED, "B", asset_type="composed")
        )

        # Must keep playing the OLD bundle — no splash.
        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.asset == _OLD_COMPOSED
        # New bundle is requested.
        client._request_asset_fetch.assert_called_with(_NEW_COMPOSED)
        # Marker tracks the new (not-yet-on-disk) name.
        assert client._last_eval_state == ("updating", _NEW_COMPOSED, "B")
        # Still displaying the old bundle until the new one lands.
        assert client._displayed_asset == _OLD_COMPOSED

    def test_swaps_to_new_composed_bundle_once_download_completes(self, tmp_path):
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register(_OLD_COMPOSED, f"videos/{_OLD_COMPOSED}", 10, "A")
        client._last_eval_state = ("play", _OLD_COMPOSED, "A", None, "composed", None)
        client._displayed_asset = _OLD_COMPOSED
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset=_OLD_COMPOSED,
                asset_type="composed",
                loop=True,
                expected_checksum="A",
            ),
        )

        # Edit → hold (no splash).
        client._evaluate_schedule(
            _schedule_sync(_NEW_COMPOSED, "B", asset_type="composed")
        )
        assert _read_desired(client).mode == PlaybackMode.PLAY
        assert _read_desired(client).asset == _OLD_COMPOSED
        assert client._last_eval_state == ("updating", _NEW_COMPOSED, "B")

        # Download completes — new bundle on disk.
        client.asset_manager.register(_NEW_COMPOSED, f"videos/{_NEW_COMPOSED}", 12, "B")
        client._evaluate_schedule(
            _schedule_sync(_NEW_COMPOSED, "B", asset_type="composed")
        )

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.PLAY
        assert desired.asset == _NEW_COMPOSED
        assert desired.expected_checksum == "B"
        assert client._last_eval_state[0] == "play"
        assert client._last_eval_state[1] == _NEW_COMPOSED
        # Tracking field advances to the now-on-screen bundle.
        assert client._displayed_asset == _NEW_COMPOSED

    def test_cold_composed_slide_still_splashes(self, tmp_path):
        """First-ever composed slide (nothing on screen) → splash."""
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client._evaluate_schedule(
            _schedule_sync(_NEW_COMPOSED, "B", asset_type="composed")
        )

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.SPLASH
        client._request_asset_fetch.assert_called_with(_NEW_COMPOSED)
        assert client._last_eval_state == ("waiting", _NEW_COMPOSED, "B")

    def test_different_composed_slide_splashes(self, tmp_path):
        """Switching to a *different* logical slide is a cold swap → splash."""
        client = _make_client(tmp_path)
        client._request_asset_fetch = MagicMock()

        client.asset_manager.register(_OLD_COMPOSED, f"videos/{_OLD_COMPOSED}", 10, "A")
        client._last_eval_state = ("play", _OLD_COMPOSED, "A", None, "composed", None)
        client._displayed_asset = _OLD_COMPOSED
        write_state(
            client.settings.desired_state_path,
            DesiredState(
                mode=PlaybackMode.PLAY,
                asset=_OLD_COMPOSED,
                asset_type="composed",
                loop=True,
                expected_checksum="A",
            ),
        )

        # A genuinely different composed slide (different uuid).
        client._evaluate_schedule(
            _schedule_sync(_OTHER_COMPOSED, "C", asset_type="composed")
        )

        desired = _read_desired(client)
        assert desired.mode == PlaybackMode.SPLASH
        assert client._last_eval_state == ("waiting", _OTHER_COMPOSED, "C")

    def test_logical_asset_id_strips_composed_hash(self, tmp_path):
        client = _make_client(tmp_path)
        logical = "composed-83d695d2-1384-4e32-a898-0ed33233d439"
        assert client._logical_asset_id(_OLD_COMPOSED) == logical
        assert client._logical_asset_id(_NEW_COMPOSED) == logical
        # Non-composed names are returned unchanged.
        assert client._logical_asset_id("v.mp4") == "v.mp4"
        assert client._logical_asset_id("image.png") == "image.png"
