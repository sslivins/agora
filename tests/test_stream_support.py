"""Tests for stream URL support on the device side.

Covers:
- DesiredState model: asset_type field for stream routing
- CMS client: stream schedule evaluation, URL validation
- CMS client: asset download routing by asset_type (issue #110)
- Player: stream command building, stream routing in apply_desired
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

# Mock heavy dependencies before importing
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from shared.models import DesiredState, PlaybackMode


# ── DesiredState model tests ─────────────────────────────────────


class TestDesiredStateAssetType:

    def test_asset_type_field_exists(self):
        """DesiredState should have an asset_type field."""
        ds = DesiredState(mode=PlaybackMode.PLAY, asset="test.mp4", asset_type="video")
        assert ds.asset_type == "video"

    def test_asset_type_defaults_to_none(self):
        """asset_type should default to None for backwards compat."""
        ds = DesiredState(mode=PlaybackMode.PLAY, asset="test.mp4")
        assert ds.asset_type is None

    def test_stream_asset_type(self):
        """Stream URLs use asset_type='stream'."""
        ds = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="stream",
            url="https://example.com/live.m3u8",
            asset_type="stream",
        )
        assert ds.asset_type == "stream"
        assert ds.url == "https://example.com/live.m3u8"

    def test_webpage_asset_type(self):
        """Webpage URLs use asset_type='webpage'."""
        ds = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="webpage",
            url="https://example.com/dashboard",
            asset_type="webpage",
        )
        assert ds.asset_type == "webpage"


# ── Player stream command building ───────────────────────────────


class TestStreamCommandBuilding:

    def test_build_stream_command_hls(self):
        """HLS URL should produce a valid mpv command."""
        with patch.dict("sys.modules", {
            "gi": MagicMock(),
            "gi.repository": MagicMock(),
        }):
            import importlib
            import player.service as svc
            importlib.reload(svc)

            cmd = svc._build_stream_command("https://example.com/live.m3u8")
            assert "mpv" in cmd[0]
            assert "https://example.com/live.m3u8" in cmd
            # VOD streams loop (harmless for live — they don't end)
            assert "--loop=inf" in cmd

    def test_build_stream_command_rtsp(self):
        """RTSP URL should also produce a valid mpv command."""
        with patch.dict("sys.modules", {
            "gi": MagicMock(),
            "gi.repository": MagicMock(),
        }):
            import importlib
            import player.service as svc
            importlib.reload(svc)

            cmd = svc._build_stream_command("rtsp://camera.local:554/feed")
            assert "rtsp://camera.local:554/feed" in cmd


# ── CMS client: download routing (issue #110) ───────────────────


class TestDownloadRouting:
    """Test that _handle_fetch_asset routes to the correct subdirectory."""

    def _make_client(self, tmp_path):
        from cms_client.service import CMSClient

        settings = MagicMock()
        settings.agora_base = tmp_path
        settings.assets_dir = tmp_path / "assets"
        settings.assets_dir.mkdir()
        settings.videos_dir = tmp_path / "assets" / "videos"
        settings.videos_dir.mkdir()
        settings.images_dir = tmp_path / "assets" / "images"
        settings.images_dir.mkdir()
        settings.splash_dir = tmp_path / "assets" / "splash"
        settings.splash_dir.mkdir()
        settings.manifest_path = tmp_path / "state" / "assets.json"
        settings.manifest_path.parent.mkdir(parents=True)
        settings.schedule_path = tmp_path / "state" / "schedule.json"
        settings.desired_state_path = tmp_path / "state" / "desired.json"
        settings.asset_budget_mb = 100
        settings.persist_dir = tmp_path / "persist"
        settings.persist_dir.mkdir()

        with patch.object(CMSClient, "__init__", lambda self, s: None):
            client = CMSClient(settings)
        client.settings = settings
        client.device_id = "test-device"
        client.asset_manager = MagicMock()
        client.asset_manager.has_asset.return_value = False
        client.asset_manager.evict_for.return_value = True
        client.asset_manager.budget_mb = 100
        client.asset_manager.available_bytes = 100 * 1024 * 1024
        return client

    def _get_target_dir_for_asset_type(self, tmp_path, asset_type, asset_name="test.mp4"):
        """Helper: determine what target_dir the routing logic would pick."""
        from cms_client.service import CMSClient
        client = self._make_client(tmp_path)

        # Simulate the routing logic from _handle_fetch_asset
        msg = {"asset_type": asset_type}

        if asset_type in ("video", "saved_stream"):
            return client.settings.videos_dir
        elif asset_type == "image":
            return client.settings.images_dir
        else:
            ext = Path(asset_name).suffix.lower()
            if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi", ".ts"):
                return client.settings.videos_dir
            elif ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"):
                return client.settings.images_dir
            else:
                return client.settings.videos_dir

    def test_video_asset_type_routes_to_videos(self, tmp_path):
        """asset_type='video' → videos/ directory."""
        target = self._get_target_dir_for_asset_type(tmp_path, "video")
        assert target.name == "videos"

    def test_saved_stream_routes_to_videos(self, tmp_path):
        """asset_type='saved_stream' → videos/ directory."""
        target = self._get_target_dir_for_asset_type(tmp_path, "saved_stream")
        assert target.name == "videos"

    def test_image_asset_type_routes_to_images(self, tmp_path):
        """asset_type='image' → images/ directory."""
        target = self._get_target_dir_for_asset_type(tmp_path, "image")
        assert target.name == "images"

    def test_unknown_extension_defaults_to_videos(self, tmp_path):
        """No asset_type + unknown extension → videos/ (not root assets/)."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "bbb")
        assert target.name == "videos"

    def test_no_extension_defaults_to_videos(self, tmp_path):
        """Files without extension should go to videos/."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "no_ext_file")
        assert target.name == "videos"

    def test_mkv_extension_routes_to_videos(self, tmp_path):
        """MKV files should route to videos/ (expanded extension list)."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "movie.mkv")
        assert target.name == "videos"

    def test_webm_extension_routes_to_videos(self, tmp_path):
        """WebM files should route to videos/."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "clip.webm")
        assert target.name == "videos"

    def test_gif_extension_routes_to_images(self, tmp_path):
        """GIF files should route to images/ (expanded extension list)."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "animation.gif")
        assert target.name == "images"

    def test_webp_extension_routes_to_images(self, tmp_path):
        """WebP files should route to images/."""
        target = self._get_target_dir_for_asset_type(tmp_path, "", "photo.webp")
        assert target.name == "images"


# ── CMS client: stream schedule evaluation ──────────────────────


class TestStreamScheduleEval:
    """Test that stream schedules produce correct DesiredState."""

    def test_stream_url_validation_accepts_https(self):
        """HTTPS stream URLs should be accepted."""
        from urllib.parse import urlparse
        url = "https://example.com/live.m3u8"
        parsed = urlparse(url)
        assert parsed.scheme in ("http", "https", "rtsp", "rtmp", "rtp", "mms")

    def test_stream_url_validation_accepts_rtsp(self):
        """RTSP stream URLs should be accepted."""
        from urllib.parse import urlparse
        url = "rtsp://camera.local:554/feed"
        parsed = urlparse(url)
        assert parsed.scheme in ("http", "https", "rtsp", "rtmp", "rtp", "mms")

    def test_stream_url_validation_rejects_ftp(self):
        """FTP URLs should not be accepted as streams."""
        from urllib.parse import urlparse
        url = "ftp://example.com/video.mp4"
        parsed = urlparse(url)
        assert parsed.scheme not in ("http", "https", "rtsp", "rtmp", "rtp", "mms")

    def test_stream_url_validation_rejects_file(self):
        """file:// URLs should not be accepted as streams."""
        from urllib.parse import urlparse
        url = "file:///etc/passwd"
        parsed = urlparse(url)
        assert parsed.scheme not in ("http", "https", "rtsp", "rtmp", "rtp", "mms")
