"""Tests for slideshow asset fetching on the Pi (cms_client).

The CMS sends a ``fetch_asset`` message with ``asset_type="slideshow"``
and a ``slides`` list. The device must download every slide, write a
local slideshow manifest, register the slideshow in its asset manager,
and ACK with the resolved manifest checksum.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio  # noqa: F401  (registers asyncio fixtures)

from cms_client.asset_manager import AssetManager  # noqa: E402
from cms_client.service import CMSClient  # noqa: E402


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def cms_client(tmp_path):
    """CMSClient wired to a real AssetManager + tmp dirs.

    Using a real AssetManager (not a MagicMock) so the manifest, eviction,
    and has_asset semantics line up with production behaviour.
    """
    settings = MagicMock()
    settings.agora_base = tmp_path
    settings.assets_dir = tmp_path / "assets"
    settings.videos_dir = tmp_path / "assets" / "videos"
    settings.images_dir = tmp_path / "assets" / "images"
    settings.splash_dir = tmp_path / "assets" / "splash"
    settings.slideshows_dir = tmp_path / "assets" / "slideshows"
    for d in (settings.assets_dir, settings.videos_dir, settings.images_dir,
              settings.splash_dir, settings.slideshows_dir):
        d.mkdir(parents=True, exist_ok=True)
    settings.manifest_path = tmp_path / "state" / "assets.json"
    settings.manifest_path.parent.mkdir(parents=True)
    settings.schedule_path = tmp_path / "state" / "schedule.json"
    settings.desired_state_path = tmp_path / "state" / "desired.json"
    settings.persist_dir = tmp_path / "persist"
    settings.persist_dir.mkdir()
    settings.asset_budget_mb = 100

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    client.device_id = "test-device"
    client.asset_manager = AssetManager(
        settings.manifest_path, settings.assets_dir, budget_mb=100,
    )
    client._ws = AsyncMock()
    client._fetch_lock = asyncio.Lock()
    client._fetch_tasks = {}
    client._inflight_fetches = {}
    client._current_schedule_id = None
    client._current_schedule_name = None
    client._current_asset = None
    client._eval_wake = asyncio.Event()
    client._last_player_mode = None
    return client


def _make_slide(name: str, body: bytes, *, asset_type: str = "video",
                duration_ms: int = 5000, play_to_end: bool = False) -> dict:
    return {
        "asset_name": name,
        "asset_type": asset_type,
        "download_url": f"http://cms.test/{name}",
        "checksum": _sha256(body),
        "size_bytes": len(body),
        "duration_ms": duration_ms,
        "play_to_end": play_to_end,
    }


class _FakeAioHttpResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status
        self.content = self  # iter_chunked is on .content

    async def iter_chunked(self, _size):
        yield self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False


class _FakeAioHttpSession:
    """aiohttp.ClientSession() stand-in that maps URL → fixed body."""

    def __init__(self, mapping: dict[str, bytes]):
        self._mapping = mapping
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    def get(self, url, headers=None):
        self.calls.append(url)
        body = self._mapping.get(url, b"")
        status = 200 if url in self._mapping else 404
        return _FakeAioHttpResponse(body, status)


def _patch_aiohttp(mapping):
    fake_session = _FakeAioHttpSession(mapping)
    fake_module = MagicMock()
    fake_module.ClientSession = lambda: fake_session
    return fake_module, fake_session


class TestSlideshowFetch:
    @pytest.mark.asyncio
    async def test_happy_path_three_slides(self, cms_client):
        b1, b2, b3 = b"slide-one-bytes", b"slide-two-bytes", b"slide-three-bytes!"
        slides = [
            _make_slide("clip1.mp4", b1, asset_type="video", duration_ms=4000),
            _make_slide("pic1.jpg",  b2, asset_type="image", duration_ms=3000),
            _make_slide("clip2.mp4", b3, asset_type="video", duration_ms=5000, play_to_end=True),
        ]
        msg = {
            "type": "fetch_asset",
            "asset_name": "Lobby Slideshow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "manifest-hash-abc",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {s["download_url"]: body for s, body in zip(slides, [b1, b2, b3])}
        fake_aiohttp, fake_session = _patch_aiohttp(mapping)

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Each slide downloaded once
        assert len(fake_session.calls) == 3

        # Slide files on disk
        assert (cms_client.settings.videos_dir / "clip1.mp4").read_bytes() == b1
        assert (cms_client.settings.images_dir / "pic1.jpg").read_bytes() == b2
        assert (cms_client.settings.videos_dir / "clip2.mp4").read_bytes() == b3

        # Slideshow manifest on disk with per-slide metadata
        manifest_path = cms_client.settings.slideshows_dir / "Lobby Slideshow.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["checksum"] == "manifest-hash-abc"
        assert [s["name"] for s in manifest["slides"]] == ["clip1.mp4", "pic1.jpg", "clip2.mp4"]
        assert manifest["slides"][0]["asset_type"] == "video"
        assert manifest["slides"][2]["play_to_end"] is True
        assert manifest["slides"][1]["duration_ms"] == 3000

        # AssetManager registered every slide + the slideshow itself
        am = cms_client.asset_manager
        assert am.has_asset("clip1.mp4", _sha256(b1))
        assert am.has_asset("pic1.jpg",  _sha256(b2))
        assert am.has_asset("clip2.mp4", _sha256(b3))
        assert am.has_asset("Lobby Slideshow.slideshow", "manifest-hash-abc")
        slideshow_entry = am.get("Lobby Slideshow.slideshow")
        assert "slideshows" in Path(slideshow_entry["path"]).parts

        # Single ACK with the resolved manifest checksum
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        acks = [m for m in sent if m["type"] == "asset_ack"]
        assert len(acks) == 1
        assert acks[0]["asset_name"] == "Lobby Slideshow.slideshow"
        assert acks[0]["checksum"] == "manifest-hash-abc"
        assert not [m for m in sent if m["type"] == "fetch_failed"]

    @pytest.mark.asyncio
    async def test_manifest_schema_version_defaults_to_1_0_when_absent(self, cms_client):
        """agora#226 Phase 0: pre-versioning CMS payloads (no
        ``manifest_schema_version`` on the wire) get persisted as "1.0".
        """
        b1 = b"only-slide-bytes"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=2000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoVer.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
            # NOTE: no manifest_schema_version
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "NoVer.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["manifest_schema_version"] == "1.0"

    @pytest.mark.asyncio
    async def test_manifest_schema_version_propagated_from_wire(self, cms_client):
        """agora#226 Phase 0: when CMS sends a versioned manifest, the
        version is persisted verbatim so the player can branch on it.
        """
        b1 = b"only-slide-bytes"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=2000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "Versioned.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.1",
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "Versioned.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["manifest_schema_version"] == "1.1"

    @pytest.mark.asyncio
    async def test_transition_fields_default_when_absent_on_wire(self, cms_client):
        """agora#226 Phase 1a: CMS may omit ``transition``/``transition_ms``
        on a slide (older CMS, default-asset path) → persist as cut/600 so
        the chromium player sees a sane default and the mpv player ignores
        the cut as a no-op.
        """
        b1 = b"slide-bytes"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=2000)]
        # NOTE: no transition/transition_ms keys
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoTrans.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "NoTrans.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["slides"][0]["transition"] == "cut"
        assert manifest["slides"][0]["transition_ms"] == 600

    @pytest.mark.asyncio
    async def test_transition_fields_propagated_from_wire(self, cms_client):
        """agora#226 Phase 1a: when CMS provides transition/transition_ms,
        they are persisted verbatim for the chromium player to consume.
        """
        b1 = b"slide-bytes"
        slide = _make_slide("a.png", b1, asset_type="image", duration_ms=2000)
        slide["transition"] = "fade"
        slide["transition_ms"] = 850
        msg = {
            "type": "fetch_asset",
            "asset_name": "WithTrans.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": [slide],
        }
        mapping = {slide["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "WithTrans.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["slides"][0]["transition"] == "fade"
        assert manifest["slides"][0]["transition_ms"] == 850

    @pytest.mark.asyncio
    async def test_fit_effect_fields_propagated_from_wire(self, cms_client):
        """Ken Burns regression: when CMS provides per-slide ``fit``/
        ``effect`` (manifest schema 1.3), they must be persisted verbatim
        into the on-disk manifest. The player reads them from there and
        forwards them to the chromium shell; dropping them here silently
        disables the Ken Burns effect on the device.
        """
        b1 = b"slide-bytes"
        slide = _make_slide("a.png", b1, asset_type="image", duration_ms=8000)
        slide["fit"] = "cover"
        slide["effect"] = "ken_burns"
        slide["effect_direction"] = "out_down_right"
        msg = {
            "type": "fetch_asset",
            "asset_name": "KenBurns.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.4",
            "slides": [slide],
        }
        mapping = {slide["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "KenBurns.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["slides"][0]["fit"] == "cover"
        assert manifest["slides"][0]["effect"] == "ken_burns"
        assert manifest["slides"][0]["effect_direction"] == "out_down_right"

    @pytest.mark.asyncio
    async def test_clip_start_ms_propagated_from_wire(self, cms_client):
        """Per-slide source trim regression (manifest schema 1.6,
        capability ``slideshow_clip_v1``): when CMS provides
        ``clip_start_ms``, it must be persisted into the on-disk
        manifest. The player reads it from there to seek the source
        video; dropping it here silently played trimmed videos from 0
        (the symptom the user hit: duration honoured, start ignored).
        """
        b1 = b"trimmed-video-bytes"
        slide = _make_slide("Sony_AV1.mp4", b1, asset_type="video",
                            duration_ms=30000)
        slide["clip_start_ms"] = 15000
        msg = {
            "type": "fetch_asset",
            "asset_name": "Trimmed.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.6",
            "slides": [slide],
        }
        mapping = {slide["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "Trimmed.slideshow.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["slides"][0]["clip_start_ms"] == 15000

    @pytest.mark.asyncio
    async def test_clip_start_ms_defaults_to_zero_when_absent(self, cms_client):
        """Older CMS (pre-1.6) or untrimmed slides omit ``clip_start_ms``.
        Persist ``0`` so the player's seek is a no-op (plays from start).
        """
        b1 = b"sb"
        slides = [_make_slide("a.mp4", b1, asset_type="video", duration_ms=5000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoClip.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest = json.loads(
            (cms_client.settings.slideshows_dir / "NoClip.slideshow.json").read_text()
        )
        assert manifest["slides"][0]["clip_start_ms"] == 0

    @pytest.mark.asyncio
    async def test_visibility_window_fields_propagated_from_wire(self, cms_client):
        """Per-slide visibility regression (manifest schema 1.5, capability
        ``slideshow_visibility_v1``): when CMS provides window fields
        (``valid_from``/``valid_to``/``active_days``/``active_start``/
        ``active_end``) they must be persisted verbatim into the on-disk
        manifest. The player's window engine reads them from the PERSISTED
        manifest (player/service.py ``any(_slide_has_window(s) for s in
        base_slides)``); dropping them here makes every deck read as
        ``windowed=False`` so ALL slides play always (the symptom the user
        hit: a 09:10-09:15 slide showed at 09:08).
        """
        b1 = b"windowed-slide-bytes"
        slide = _make_slide("promo.png", b1, asset_type="image",
                            duration_ms=8000)
        slide["valid_from"] = "2026-12-01"
        slide["valid_to"] = "2026-12-26"
        slide["active_days"] = [0, 1, 2, 3, 4]
        slide["active_start"] = "09:10:00"
        slide["active_end"] = "09:15:00"
        msg = {
            "type": "fetch_asset",
            "asset_name": "Windowed.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.5",
            "slides": [slide],
        }
        mapping = {slide["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest_path = cms_client.settings.slideshows_dir / "Windowed.slideshow.json"
        persisted = json.loads(manifest_path.read_text())["slides"][0]
        assert persisted["valid_from"] == "2026-12-01"
        assert persisted["valid_to"] == "2026-12-26"
        assert persisted["active_days"] == [0, 1, 2, 3, 4]
        assert persisted["active_start"] == "09:10:00"
        assert persisted["active_end"] == "09:15:00"

    @pytest.mark.asyncio
    async def test_visibility_window_fields_default_none_when_absent(self, cms_client):
        """Older CMS (pre-1.5) or always-visible slides omit the window
        fields. Persist ``null`` so the player's ``parse_window`` reads an
        all-None window -> ``slide_has_window`` False -> the slide is always
        visible (byte-identical to pre-1.5 behaviour).
        """
        b1 = b"sb"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=5000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoWindow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        persisted = json.loads(
            (cms_client.settings.slideshows_dir / "NoWindow.slideshow.json").read_text()
        )["slides"][0]
        for k in ("valid_from", "valid_to", "active_days", "active_start", "active_end"):
            assert persisted[k] is None, f"{k} should persist as None when absent"

    @pytest.mark.asyncio
    async def test_fit_effect_default_when_absent(self, cms_client):
        """Older CMS (pre-1.3) omits ``fit``/``effect``. Persist ``null``
        so the player's ``slide.get(...) or None`` reads a no-op,
        preserving the legacy object-fit: contain / no-effect behavior.
        """
        b1 = b"sb"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=5000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoFx.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest = json.loads(
            (cms_client.settings.slideshows_dir / "NoFx.slideshow.json").read_text()
        )
        assert manifest["slides"][0]["fit"] is None
        assert manifest["slides"][0]["effect"] is None
        assert manifest["slides"][0]["effect_direction"] is None

    @pytest.mark.asyncio
    async def test_wall_clock_fields_absent_persist_as_none(self, cms_client):
        """agora#226 Phase 1b: older CMS (or non-slideshow paths) won't
        emit ``cycle_duration_ms``/``started_at``.  Persist as ``null``
        so the player can detect "no anchor available" and fall back
        to the legacy relative-timer chain.
        """
        b1 = b"sb"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=2000)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "NoAnchor.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest = json.loads(
            (cms_client.settings.slideshows_dir / "NoAnchor.slideshow.json").read_text()
        )
        assert manifest["cycle_duration_ms"] is None
        assert manifest["started_at"] is None

    @pytest.mark.asyncio
    async def test_wall_clock_fields_propagated_from_wire(self, cms_client):
        """agora#226 Phase 1b: when CMS sends ``cycle_duration_ms`` and
        ``started_at``, persist them verbatim into the on-disk manifest
        so Phase 2 wall-clock resync can read them after a reboot.
        """
        b1 = b"sb"
        slides = [_make_slide("a.png", b1, asset_type="image", duration_ms=2500)]
        msg = {
            "type": "fetch_asset",
            "asset_name": "Anchored.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.1",
            "cycle_duration_ms": 2500,
            "started_at": "2026-05-23T19:39:45.000Z",
            "slides": slides,
        }
        mapping = {slides[0]["download_url"]: b1}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        manifest = json.loads(
            (cms_client.settings.slideshows_dir / "Anchored.slideshow.json").read_text()
        )
        assert manifest["manifest_schema_version"] == "1.1"
        assert manifest["cycle_duration_ms"] == 2500
        assert manifest["started_at"] == "2026-05-23T19:39:45.000Z"

    @pytest.mark.asyncio
    async def test_already_cached_short_circuits(self, cms_client):
        """Slideshow + every slide already cached → ACK without re-downloading."""
        b1, b2 = b"already-have-1", b"already-have-2"
        slide1 = _make_slide("a.mp4", b1, asset_type="video")
        slide2 = _make_slide("b.jpg", b2, asset_type="image")
        # Pre-seed the cache
        (cms_client.settings.videos_dir / "a.mp4").write_bytes(b1)
        (cms_client.settings.images_dir / "b.jpg").write_bytes(b2)
        cms_client.asset_manager.register("a.mp4", "videos/a.mp4", len(b1), _sha256(b1))
        cms_client.asset_manager.register("b.jpg", "images/b.jpg", len(b2), _sha256(b2))
        # Pre-seed the slideshow manifest + asset_manager entry
        manifest_path = cms_client.settings.slideshows_dir / "MyShow.slideshow.json"
        manifest_path.write_text(json.dumps({
            "name": "MyShow.slideshow",
            "checksum": "stable-hash",
            "slides": [
                {"name": "a.mp4", "asset_type": "video", "checksum": _sha256(b1),
                 "size_bytes": len(b1), "duration_ms": 1000, "play_to_end": False},
                {"name": "b.jpg", "asset_type": "image", "checksum": _sha256(b2),
                 "size_bytes": len(b2), "duration_ms": 2000, "play_to_end": False},
            ],
        }))
        cms_client.asset_manager.register(
            "MyShow.slideshow", f"slideshows/MyShow.slideshow.json",
            manifest_path.stat().st_size, "stable-hash",
        )

        msg = {
            "type": "fetch_asset",
            "asset_name": "MyShow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "stable-hash",
            "size_bytes": 0,
            "slides": [slide1, slide2],
        }
        # Override slide checksums to match pre-seeded files
        slide1["checksum"] = _sha256(b1)
        slide2["checksum"] = _sha256(b2)
        fake_aiohttp, fake_session = _patch_aiohttp({})

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Nothing downloaded
        assert fake_session.calls == []
        # ACK still sent
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert any(m["type"] == "asset_ack" and m["checksum"] == "stable-hash" for m in sent)

    @pytest.mark.asyncio
    async def test_fast_path_rewrites_manifest_when_envelope_changes(self, cms_client):
        """Regression for the wall-clock-anchor staleness bug: a device
        that cached a slideshow under schema 1.0 (no ``started_at``)
        and then receives a re-publish under schema 1.1 must rewrite
        the manifest with the new envelope fields, even though the
        slide list (and therefore the slideshow ``checksum``) hasn't
        changed and no slide downloads are needed.

        Before the fix, the fast path returned an ACK without touching
        the manifest, leaving the device stuck on a 1.0 manifest
        forever -- which made the player's anchored-playback branch
        silently fall back to a free-running timer chain.
        """
        b1, b2 = b"img-bytes-aaaa", b"img-bytes-bbbb"
        slide1 = _make_slide("img1.jpg", b1, asset_type="image", duration_ms=60000)
        slide2 = _make_slide("img2.jpg", b2, asset_type="image", duration_ms=60000)
        # Pre-seed cache for both slide sources
        (cms_client.settings.images_dir / "img1.jpg").write_bytes(b1)
        (cms_client.settings.images_dir / "img2.jpg").write_bytes(b2)
        cms_client.asset_manager.register(
            "img1.jpg", "images/img1.jpg", len(b1), _sha256(b1),
        )
        cms_client.asset_manager.register(
            "img2.jpg", "images/img2.jpg", len(b2), _sha256(b2),
        )
        # Pre-seed a STALE schema-1.0 manifest (no envelope fields).
        manifest_path = (
            cms_client.settings.slideshows_dir / "MyShow.slideshow.json"
        )
        manifest_path.write_text(json.dumps({
            "name": "MyShow.slideshow",
            "checksum": "stable-hash",
            "slides": [
                {"name": "img1.jpg", "asset_type": "image",
                 "checksum": _sha256(b1), "size_bytes": len(b1),
                 "duration_ms": 60000, "play_to_end": False},
                {"name": "img2.jpg", "asset_type": "image",
                 "checksum": _sha256(b2), "size_bytes": len(b2),
                 "duration_ms": 60000, "play_to_end": False},
            ],
        }))
        cms_client.asset_manager.register(
            "MyShow.slideshow", "slideshows/MyShow.slideshow.json",
            manifest_path.stat().st_size, "stable-hash",
        )
        slide1["checksum"] = _sha256(b1)
        slide2["checksum"] = _sha256(b2)

        msg = {
            "type": "fetch_asset",
            "asset_name": "MyShow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "stable-hash",
            "size_bytes": 0,
            # CMS now emits schema 1.1 envelope fields
            "manifest_schema_version": "1.1",
            "cycle_duration_ms": 120000,
            "started_at": "2026-05-23T23:29:41.155Z",
            "slides": [slide1, slide2],
        }
        fake_aiohttp, fake_session = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # No slide downloads happened (fast path).
        assert fake_session.calls == []
        # ACK still sent.
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert any(
            m["type"] == "asset_ack" and m["checksum"] == "stable-hash"
            for m in sent
        )
        # Manifest on disk has been rewritten with the new envelope.
        rewritten = json.loads(manifest_path.read_text())
        assert rewritten["manifest_schema_version"] == "1.1"
        assert rewritten["started_at"] == "2026-05-23T23:29:41.155Z"
        assert rewritten["cycle_duration_ms"] == 120000

    @pytest.mark.asyncio
    async def test_fast_path_no_rewrite_when_envelope_unchanged(self, cms_client):
        """When the on-disk manifest already has the correct schema-1.1
        envelope, the fast path should NOT rewrite (avoids touching
        atime/mtime and the asset manager for every CMS sync).
        """
        b1 = b"img-bytes"
        slide1 = _make_slide("img.jpg", b1, asset_type="image", duration_ms=5000)
        (cms_client.settings.images_dir / "img.jpg").write_bytes(b1)
        cms_client.asset_manager.register(
            "img.jpg", "images/img.jpg", len(b1), _sha256(b1),
        )
        manifest_path = (
            cms_client.settings.slideshows_dir / "Stable.slideshow.json"
        )
        # Manifest already at schema 1.1 with the same envelope the CMS
        # will send -- the fast path should treat this as a no-op
        # (no rewrite).
        existing_payload = {
            "name": "Stable.slideshow",
            "checksum": "h",
            "manifest_schema_version": "1.1",
            "cycle_duration_ms": 5000,
            "started_at": "2026-05-23T23:29:41.155Z",
            "slides": [
                {"name": "img.jpg", "asset_type": "image",
                 "checksum": _sha256(b1), "size_bytes": len(b1),
                 "duration_ms": 5000, "play_to_end": False,
                 "transition": "cut", "transition_ms": 600},
            ],
        }
        manifest_path.write_text(json.dumps(existing_payload, indent=2))
        original_bytes = manifest_path.read_bytes()
        cms_client.asset_manager.register(
            "Stable.slideshow", "slideshows/Stable.slideshow.json",
            manifest_path.stat().st_size, "h",
        )
        slide1["checksum"] = _sha256(b1)

        msg = {
            "type": "fetch_asset",
            "asset_name": "Stable.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "h",
            "size_bytes": 0,
            "manifest_schema_version": "1.1",
            "cycle_duration_ms": 5000,
            "started_at": "2026-05-23T23:29:41.155Z",
            "slides": [slide1],
        }
        fake_aiohttp, _ = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # File contents identical -- no rewrite.
        assert manifest_path.read_bytes() == original_bytes

    @pytest.mark.asyncio
    async def test_partial_cache_only_fetches_missing(self, cms_client):
        b1, b2 = b"on-disk-bytes", b"need-this-bytes"
        slide1 = _make_slide("cached.mp4", b1, asset_type="video")
        slide2 = _make_slide("missing.mp4", b2, asset_type="video")
        # Pre-seed slide1 only
        (cms_client.settings.videos_dir / "cached.mp4").write_bytes(b1)
        cms_client.asset_manager.register(
            "cached.mp4", "videos/cached.mp4", len(b1), _sha256(b1),
        )
        msg = {
            "type": "fetch_asset",
            "asset_name": "Mixed.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "mixed-hash",
            "size_bytes": 0,
            "slides": [slide1, slide2],
        }
        fake_aiohttp, fake_session = _patch_aiohttp({slide2["download_url"]: b2})

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Only the missing slide was fetched
        assert fake_session.calls == [slide2["download_url"]]
        assert cms_client.asset_manager.has_asset("missing.mp4", _sha256(b2))
        assert cms_client.asset_manager.has_asset("Mixed.slideshow", "mixed-hash")

    @pytest.mark.asyncio
    async def test_slide_download_failure_aborts(self, cms_client):
        """If any slide fails to download, no slideshow manifest is written
        and a fetch_failed is sent. Already-downloaded slides remain cached."""
        b1 = b"good-slide"
        slide_ok = _make_slide("ok.mp4", b1, asset_type="video")
        slide_bad = _make_slide("bad.mp4", b"never-served", asset_type="video")
        msg = {
            "type": "fetch_asset",
            "asset_name": "Broken.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "broken-hash",
            "size_bytes": 0,
            "slides": [slide_ok, slide_bad],
        }
        # Only ok.mp4 has a valid mapping; bad.mp4 → 404
        fake_aiohttp, fake_session = _patch_aiohttp({slide_ok["download_url"]: b1})

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # ok.mp4 is still cached (kept for future retries)
        assert cms_client.asset_manager.has_asset("ok.mp4", _sha256(b1))
        # The slideshow itself was NOT registered
        assert not cms_client.asset_manager.has_asset("Broken.slideshow")
        # No manifest file written
        assert not (cms_client.settings.slideshows_dir / "Broken.slideshow.json").exists()
        # fetch_failed sent, no asset_ack
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        fails = [m for m in sent if m["type"] == "fetch_failed"]
        assert len(fails) == 1
        assert fails[0]["asset"] == "Broken.slideshow"
        assert fails[0]["reason"] == "slide_download_failed"
        assert fails[0]["slide_asset"] == "bad.mp4"
        assert not [m for m in sent if m["type"] == "asset_ack"]

    @pytest.mark.asyncio
    async def test_duplicate_slides_downloaded_once(self, cms_client):
        b = b"reused-bytes"
        slide_a = _make_slide("dup.mp4", b, asset_type="video", duration_ms=1000)
        # Same name + checksum, different position → must dedupe
        slide_b = dict(slide_a)
        slide_b["duration_ms"] = 2500
        msg = {
            "type": "fetch_asset",
            "asset_name": "Repeat.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "repeat-hash",
            "size_bytes": 0,
            "slides": [slide_a, slide_b, slide_a],
        }
        fake_aiohttp, fake_session = _patch_aiohttp({slide_a["download_url"]: b})

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Downloaded exactly once
        assert fake_session.calls == [slide_a["download_url"]]
        # Manifest preserves all three positions in order with their durations
        manifest = json.loads(
            (cms_client.settings.slideshows_dir / "Repeat.slideshow.json").read_text()
        )
        assert [s["name"] for s in manifest["slides"]] == ["dup.mp4", "dup.mp4", "dup.mp4"]
        assert [s["duration_ms"] for s in manifest["slides"]] == [1000, 2500, 1000]

    @pytest.mark.asyncio
    async def test_invalid_payload_no_slides(self, cms_client):
        msg = {
            "type": "fetch_asset",
            "asset_name": "Empty.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "empty-hash",
            "size_bytes": 0,
            "slides": [],
        }
        await cms_client._handle_fetch_asset(msg, cms_client._ws)

        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        fails = [m for m in sent if m["type"] == "fetch_failed"]
        assert len(fails) == 1
        assert fails[0]["reason"] == "invalid_slideshow_payload"


class TestScheduleProtection:
    """`_get_scheduled_asset_names` must expand slideshows so slide source
    files are protected from LRU eviction while the slideshow is scheduled."""

    def test_scheduled_slideshow_protects_slide_sources(self, cms_client):
        # Create a slideshow manifest on disk
        manifest = {
            "name": "Promo.slideshow",
            "checksum": "promo-hash",
            "slides": [
                {"name": "slideA.mp4", "asset_type": "video", "checksum": "a",
                 "size_bytes": 100, "duration_ms": 1000, "play_to_end": False},
                {"name": "slideB.jpg", "asset_type": "image", "checksum": "b",
                 "size_bytes": 200, "duration_ms": 2000, "play_to_end": False},
            ],
        }
        manifest_path = cms_client.settings.slideshows_dir / "Promo.slideshow.json"
        manifest_path.write_text(json.dumps(manifest))
        cms_client.asset_manager.register(
            "Promo.slideshow", "slideshows/Promo.slideshow.json",
            manifest_path.stat().st_size, "promo-hash",
        )
        # Schedule it
        cms_client.settings.schedule_path.write_text(json.dumps({
            "schedules": [{
                "id": "s1", "name": "promo", "asset": "Promo.slideshow",
                "asset_checksum": "promo-hash",
                "start_time": "00:00", "end_time": "23:59", "priority": 0,
            }],
            "default_asset": None,
            "timezone": "UTC",
        }))

        protected = cms_client._get_scheduled_asset_names()
        assert "Promo.slideshow" in protected
        assert "slideA.mp4" in protected
        assert "slideB.jpg" in protected

    def test_corrupt_slideshow_manifest_does_not_crash(self, cms_client):
        # Slideshow registered, but its manifest is unparseable
        bad_path = cms_client.settings.slideshows_dir / "Broken.slideshow.json"
        bad_path.write_text("{not valid json")
        cms_client.asset_manager.register(
            "Broken.slideshow", "slideshows/Broken.slideshow.json",
            bad_path.stat().st_size, "broken-hash",
        )
        cms_client.settings.schedule_path.write_text(json.dumps({
            "schedules": [{
                "id": "s1", "name": "broken", "asset": "Broken.slideshow",
                "asset_checksum": "broken-hash",
                "start_time": "00:00", "end_time": "23:59", "priority": 0,
            }],
            "default_asset": None,
            "timezone": "UTC",
        }))

        protected = cms_client._get_scheduled_asset_names()
        # Slideshow itself still protected; slides simply not expanded
        assert protected == {"Broken.slideshow"}


class TestCompletenessCheck:
    """`_has_complete_slideshow` and the proactive refetch path."""

    def test_complete_slideshow_returns_true(self, cms_client):
        b = b"x" * 32
        cms_client.asset_manager.register("s.mp4", "videos/s.mp4", len(b), _sha256(b))
        manifest_path = cms_client.settings.slideshows_dir / "OK.slideshow.json"
        manifest_path.write_text(json.dumps({
            "name": "OK.slideshow",
            "checksum": "ok-hash",
            "slides": [{"name": "s.mp4", "asset_type": "video", "checksum": _sha256(b),
                        "size_bytes": len(b), "duration_ms": 1000, "play_to_end": False}],
        }))
        cms_client.asset_manager.register(
            "OK.slideshow", "slideshows/OK.slideshow.json",
            manifest_path.stat().st_size, "ok-hash",
        )
        assert cms_client._has_complete_slideshow("OK.slideshow", "ok-hash") is True

    def test_incomplete_slideshow_missing_slide_returns_false(self, cms_client):
        # Register slideshow but never register the slide
        manifest_path = cms_client.settings.slideshows_dir / "Half.slideshow.json"
        manifest_path.write_text(json.dumps({
            "name": "Half.slideshow",
            "checksum": "half-hash",
            "slides": [{"name": "missing.mp4", "asset_type": "video",
                        "checksum": "abc", "size_bytes": 1, "duration_ms": 1000,
                        "play_to_end": False}],
        }))
        cms_client.asset_manager.register(
            "Half.slideshow", "slideshows/Half.slideshow.json",
            manifest_path.stat().st_size, "half-hash",
        )
        assert cms_client._has_complete_slideshow("Half.slideshow", "half-hash") is False

    def test_stale_slide_checksum_returns_false(self, cms_client):
        # Slide cached with different checksum than the slideshow manifest expects
        cms_client.asset_manager.register(
            "stale.mp4", "videos/stale.mp4", 100, "old-checksum",
        )
        manifest_path = cms_client.settings.slideshows_dir / "Stale.slideshow.json"
        manifest_path.write_text(json.dumps({
            "name": "Stale.slideshow",
            "checksum": "stale-hash",
            "slides": [{"name": "stale.mp4", "asset_type": "video",
                        "checksum": "new-checksum", "size_bytes": 100,
                        "duration_ms": 1000, "play_to_end": False}],
        }))
        cms_client.asset_manager.register(
            "Stale.slideshow", "slideshows/Stale.slideshow.json",
            manifest_path.stat().st_size, "stale-hash",
        )
        assert cms_client._has_complete_slideshow("Stale.slideshow", "stale-hash") is False

    @pytest.mark.asyncio
    async def test_check_and_fetch_missing_refetches_incomplete_slideshow(self, cms_client):
        """A registered slideshow whose slides are gone must trigger a fetch_request."""
        # Slideshow registered with a manifest, but slide source not in asset_manager
        manifest_path = cms_client.settings.slideshows_dir / "Incomplete.slideshow.json"
        manifest_path.write_text(json.dumps({
            "name": "Incomplete.slideshow",
            "checksum": "inc-hash",
            "slides": [{"name": "gone.mp4", "asset_type": "video",
                        "checksum": "g", "size_bytes": 10, "duration_ms": 1000,
                        "play_to_end": False}],
        }))
        cms_client.asset_manager.register(
            "Incomplete.slideshow", "slideshows/Incomplete.slideshow.json",
            manifest_path.stat().st_size, "inc-hash",
        )

        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = (now - timedelta(minutes=30)).strftime("%H:%M")
        end = (now + timedelta(minutes=30)).strftime("%H:%M")
        cms_client.settings.schedule_path.write_text(json.dumps({
            "schedules": [{
                "id": "s1", "name": "inc", "asset": "Incomplete.slideshow",
                "asset_checksum": "inc-hash",
                "start_time": start, "end_time": end, "priority": 0,
            }],
            "default_asset": None,
            "timezone": "UTC",
        }))

        await cms_client._check_and_fetch_missing()

        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        reqs = [m for m in sent if m["type"] == "fetch_request"]
        assert len(reqs) == 1
        assert reqs[0]["asset"] == "Incomplete.slideshow"
