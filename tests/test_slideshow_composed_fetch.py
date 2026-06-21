"""Tests for COMPOSED slide members inside a SLIDESHOW asset (Phase 5).

The CMS may include a slide whose ``asset_type == "composed"`` inside a
slideshow's ``slides`` list. Such a slide's ``download_url`` is the
published bundle HTML and it carries a per-slide ``siblings`` list of
referenced media (videos/images) the bundle loads from local cache.

The device must, for each composed member:

* validate + normalize sibling descriptors (shape + safe name),
* dedupe + bulk-evict for slides + composed siblings together,
* download composed siblings first, then the bundle (as a normal slide),
* write a ``.deps.json`` sidecar so cold-start completeness + eviction
  protection can reconstruct the sibling list,
* persist the sibling list in the slideshow manifest,
* honor composed completeness on the fast path.

This is gated by the ``slideshow_composed_v1`` capability so pre-feature
firmware never receives composed slideshow members.
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
from cms_client.service import (  # noqa: E402
    DEVICE_CAPABILITIES,
    CMSClient,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def cms_client(tmp_path):
    """CMSClient wired to a real AssetManager + tmp dirs.

    Mirrors the slideshow-fetch / composed-fetch fixtures, with a
    ``composed_dir`` so composed bundles + sidecars land correctly.
    """
    settings = MagicMock()
    settings.agora_base = tmp_path
    settings.assets_dir = tmp_path / "assets"
    settings.videos_dir = tmp_path / "assets" / "videos"
    settings.images_dir = tmp_path / "assets" / "images"
    settings.splash_dir = tmp_path / "assets" / "splash"
    settings.slideshows_dir = tmp_path / "assets" / "slideshows"
    settings.composed_dir = tmp_path / "assets" / "composed"
    for d in (settings.assets_dir, settings.videos_dir, settings.images_dir,
              settings.splash_dir, settings.slideshows_dir,
              settings.composed_dir):
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
                duration_ms: int = 5000, play_to_end: bool = False,
                siblings: list[dict] | None = None) -> dict:
    slide = {
        "asset_name": name,
        "asset_type": asset_type,
        "download_url": f"http://cms.test/{name}",
        "checksum": _sha256(body),
        "size_bytes": len(body),
        "duration_ms": duration_ms,
        "play_to_end": play_to_end,
    }
    if siblings is not None:
        slide["siblings"] = siblings
    return slide


def _make_sibling(name: str, body: bytes, *, asset_type: str = "video",
                  key: str = "name") -> dict:
    """Build a sibling descriptor.

    ``key`` selects which name key the CMS uses on the wire — the real
    CMS protocol Sibling model uses ``name`` (not ``asset_name``); the
    device normalizes to populate both.
    """
    return {
        key: name,
        "asset_type": asset_type,
        "download_url": f"http://cms.test/{name}",
        "checksum": _sha256(body),
        "size_bytes": len(body),
    }


class _FakeAioHttpResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status
        self.content = self

    async def iter_chunked(self, _size):
        yield self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False


class _FakeAioHttpSession:
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


class TestSlideshowComposedCapability:
    def test_capability_advertised(self):
        """Phase 5 capability flag must be in the global list so the CMS
        only emits composed slideshow members to capable firmware."""
        assert "slideshow_composed_v1" in DEVICE_CAPABILITIES

    def test_clip_capability_advertised(self):
        """Per-slide video clip (trim) capability must be advertised so
        the CMS only emits clip_start_ms slides to capable firmware."""
        assert "slideshow_clip_v1" in DEVICE_CAPABILITIES


class TestSlideshowComposedFetch:
    @pytest.mark.asyncio
    async def test_happy_path_composed_member_with_sibling(self, cms_client):
        """A slideshow with an image slide + a composed member that
        references a video sibling: device downloads all three, writes a
        sidecar for the composed member, persists siblings in the
        manifest, and ACKs once."""
        img_body = b"image-slide-bytes"
        bundle_body = b"<html><body>composed bundle</body></html>"
        vid_body = b"sibling-video-payload"

        sibling = _make_sibling("composed-clip.mp4", vid_body)
        composed = _make_slide(
            "Promo.html", bundle_body, asset_type="composed",
            duration_ms=8000, siblings=[sibling],
        )
        image = _make_slide("hero.jpg", img_body, asset_type="image",
                            duration_ms=4000)
        slides = [image, composed]

        msg = {
            "type": "fetch_asset",
            "asset_name": "Mixed Slideshow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "manifest-hash-xyz",
            "size_bytes": 0,
            "manifest_schema_version": "1.2",
            "slides": slides,
        }
        mapping = {
            image["download_url"]: img_body,
            composed["download_url"]: bundle_body,
            sibling["download_url"]: vid_body,
        }
        fake_aiohttp, fake_session = _patch_aiohttp(mapping)

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # All three assets downloaded.
        assert len(fake_session.calls) == 3

        am = cms_client.asset_manager
        assert am.has_asset("hero.jpg", _sha256(img_body))
        assert am.has_asset("Promo.html", _sha256(bundle_body))
        assert am.has_asset("composed-clip.mp4", _sha256(vid_body))
        assert am.has_asset("Mixed Slideshow.slideshow", "manifest-hash-xyz")

        # Bundle landed in composed/, sibling in videos/.
        assert (cms_client.settings.composed_dir / "Promo.html").read_bytes() == bundle_body
        assert (cms_client.settings.videos_dir / "composed-clip.mp4").read_bytes() == vid_body

        # Composed sidecar written using the persisted ``name`` key.
        sidecar_path = cms_client.settings.composed_dir / "Promo.html.deps.json"
        sidecar = json.loads(sidecar_path.read_text())
        assert sidecar["name"] == "Promo.html"
        assert [s["name"] for s in sidecar["siblings"]] == ["composed-clip.mp4"]

        # Slideshow manifest persists per-slide siblings for the composed
        # member (and nothing for the image slide).
        manifest_path = (
            cms_client.settings.slideshows_dir / "Mixed Slideshow.slideshow.json"
        )
        manifest = json.loads(manifest_path.read_text())
        slide_dicts = {s["name"]: s for s in manifest["slides"]}
        assert "siblings" not in slide_dicts["hero.jpg"]
        assert slide_dicts["Promo.html"]["asset_type"] == "composed"
        assert [s["name"] for s in slide_dicts["Promo.html"]["siblings"]] == [
            "composed-clip.mp4"
        ]

        # Single ACK, no failures.
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        acks = [m for m in sent if m["type"] == "asset_ack"]
        assert len(acks) == 1
        assert acks[0]["asset_name"] == "Mixed Slideshow.slideshow"
        assert not [m for m in sent if m["type"] == "fetch_failed"]

    @pytest.mark.asyncio
    async def test_fast_path_complete_when_sidecar_and_siblings_present(
        self, cms_client,
    ):
        """After a first successful fetch, re-sending the same slideshow
        hits the fast path (no re-download) because the composed member's
        sidecar + sibling are cached and complete."""
        bundle_body = b"<html>bundle2</html>"
        vid_body = b"sib-2-payload"
        sibling = _make_sibling("clip2.mp4", vid_body)
        composed = _make_slide(
            "Deck.html", bundle_body, asset_type="composed",
            duration_ms=6000, siblings=[sibling],
        )
        slides = [composed]
        msg = {
            "type": "fetch_asset",
            "asset_name": "Solo.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "solo-hash",
            "size_bytes": 0,
            "manifest_schema_version": "1.2",
            "slides": slides,
        }
        mapping = {
            composed["download_url"]: bundle_body,
            sibling["download_url"]: vid_body,
        }
        fake_aiohttp, fake_session = _patch_aiohttp(mapping)

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)
        assert len(fake_session.calls) == 2

        # _has_complete_slideshow must now report complete (uses the
        # composed sidecar's sibling list).
        assert cms_client._has_complete_slideshow(
            "Solo.slideshow", "solo-hash", slides,
        )

        # Second fetch: fast path, zero new downloads, still one ACK.
        cms_client._ws.reset_mock()
        fake_aiohttp2, fake_session2 = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp2}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)
        assert len(fake_session2.calls) == 0
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert [m["type"] for m in sent] == ["asset_ack"]

    @pytest.mark.asyncio
    async def test_incomplete_when_sibling_evicted(self, cms_client):
        """If a composed member's sibling is missing, the slideshow is
        not complete even though the bundle + manifest are present."""
        bundle_body = b"<html>bundle3</html>"
        vid_body = b"sib-3-payload"
        sibling = _make_sibling("clip3.mp4", vid_body)
        composed = _make_slide(
            "Show.html", bundle_body, asset_type="composed",
            duration_ms=6000, siblings=[sibling],
        )
        slides = [composed]
        msg = {
            "type": "fetch_asset",
            "asset_name": "Evict.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "evict-hash",
            "size_bytes": 0,
            "manifest_schema_version": "1.2",
            "slides": slides,
        }
        mapping = {
            composed["download_url"]: bundle_body,
            sibling["download_url"]: vid_body,
        }
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        assert cms_client._has_complete_slideshow("Evict.slideshow", "evict-hash", slides)

        # Drop the sibling from the asset manager -> no longer complete.
        cms_client.asset_manager.remove("clip3.mp4")
        assert not cms_client._has_complete_slideshow(
            "Evict.slideshow", "evict-hash", slides,
        )

    @pytest.mark.asyncio
    async def test_invalid_sibling_name_rejected(self, cms_client):
        """A composed member with a path-traversal sibling name is
        rejected with fetch_failed and nothing is downloaded."""
        bundle_body = b"<html>bad</html>"
        bad_sibling = {
            "name": "../../etc/passwd",
            "asset_type": "video",
            "download_url": "http://cms.test/evil",
            "checksum": "deadbeef",
            "size_bytes": 10,
        }
        composed = _make_slide(
            "Bad.html", bundle_body, asset_type="composed",
            duration_ms=6000, siblings=[bad_sibling],
        )
        msg = {
            "type": "fetch_asset",
            "asset_name": "BadShow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "bad-hash",
            "size_bytes": 0,
            "manifest_schema_version": "1.2",
            "slides": [composed],
        }
        fake_aiohttp, fake_session = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        failures = [m for m in sent if m["type"] == "fetch_failed"]
        assert failures
        assert failures[0]["reason"] == "invalid_sibling_name"
        assert not [m for m in sent if m["type"] == "asset_ack"]
        # Nothing downloaded.
        assert len(fake_session.calls) == 0

    @pytest.mark.asyncio
    async def test_sibling_name_key_normalized(self, cms_client):
        """The CMS wire format uses ``name`` for siblings; the device
        normalizes so the download + sidecar path works regardless."""
        bundle_body = b"<html>norm</html>"
        vid_body = b"norm-vid"
        # Use the wire-shape ``name`` key only.
        sibling = _make_sibling("normclip.mp4", vid_body, key="name")
        assert "asset_name" not in sibling
        composed = _make_slide(
            "Norm.html", bundle_body, asset_type="composed",
            duration_ms=6000, siblings=[sibling],
        )
        msg = {
            "type": "fetch_asset",
            "asset_name": "NormShow.slideshow",
            "asset_type": "slideshow",
            "download_url": "",
            "checksum": "norm-hash",
            "size_bytes": 0,
            "manifest_schema_version": "1.2",
            "slides": [composed],
        }
        mapping = {
            composed["download_url"]: bundle_body,
            sibling["download_url"]: vid_body,
        }
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        am = cms_client.asset_manager
        assert am.has_asset("normclip.mp4", _sha256(vid_body))
        sidecar = json.loads(
            (cms_client.settings.composed_dir / "Norm.html.deps.json").read_text()
        )
        assert [s["name"] for s in sidecar["siblings"]] == ["normclip.mp4"]
