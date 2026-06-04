"""Tests for composed-slide asset fetching with sibling media (Phase 1D).

The CMS sends a ``fetch_asset`` message with ``asset_type="composed"``
and an optional ``siblings`` list (videos / images the bundle HTML
loads from local cache). The device must:

* validate sibling descriptors (shape + safe name),
* dedupe + bulk-evict for bundle + siblings together,
* download siblings first, then the bundle,
* write a ``.deps.json`` sidecar so future fast-path checks and
  eviction protection can reconstruct the sibling list,
* ACK with the resolved bundle checksum.
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

    Mirrors the slideshow-fetch fixture so the test patterns line up.
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
    client._current_schedule_id = None
    client._current_schedule_name = None
    client._current_asset = None
    client._eval_wake = asyncio.Event()
    client._last_player_mode = None
    return client


def _make_sibling(name: str, body: bytes, asset_type: str = "video") -> dict:
    return {
        "asset_name": name,
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


class TestComposedCapability:
    def test_capability_advertised(self):
        """Phase 1D capability flag must be in the global list so the
        CMS gates sibling emission correctly."""
        assert "composed_siblings_v1" in DEVICE_CAPABILITIES


class TestComposedFetch:
    @pytest.mark.asyncio
    async def test_happy_path_bundle_plus_two_siblings(self, cms_client):
        bundle_body = b"<html><body>composed bundle</body></html>"
        v1_body = b"video-one-payload"
        v2_body = b"video-two-payload"
        siblings = [
            _make_sibling("ad-clip-1.mp4", v1_body),
            _make_sibling("ad-clip-2.mp4", v2_body),
        ]
        bundle_url = "http://cms.test/Composed Asset.html"
        msg = {
            "type": "fetch_asset",
            "asset_name": "Composed Asset.html",
            "asset_type": "composed",
            "download_url": bundle_url,
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": siblings,
        }
        mapping = {
            bundle_url: bundle_body,
            siblings[0]["download_url"]: v1_body,
            siblings[1]["download_url"]: v2_body,
        }
        fake_aiohttp, fake_session = _patch_aiohttp(mapping)

        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Bundle landed in composed_dir; siblings in videos_dir
        assert (cms_client.settings.composed_dir / "Composed Asset.html").read_bytes() == bundle_body
        assert (cms_client.settings.videos_dir / "ad-clip-1.mp4").read_bytes() == v1_body
        assert (cms_client.settings.videos_dir / "ad-clip-2.mp4").read_bytes() == v2_body

        # Sidecar persisted
        sidecar = json.loads((cms_client.settings.composed_dir / "Composed Asset.html.deps.json").read_text())
        assert sidecar["name"] == "Composed Asset.html"
        assert sidecar["checksum"] == _sha256(bundle_body)
        assert [s["name"] for s in sidecar["siblings"]] == ["ad-clip-1.mp4", "ad-clip-2.mp4"]
        assert sidecar["siblings"][0]["asset_type"] == "video"

        # AssetManager has all three
        am = cms_client.asset_manager
        assert am.has_asset("Composed Asset.html", _sha256(bundle_body))
        assert am.has_asset("ad-clip-1.mp4", _sha256(v1_body))
        assert am.has_asset("ad-clip-2.mp4", _sha256(v2_body))

        # ACK on bundle checksum
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        acks = [m for m in sent if m["type"] == "asset_ack"]
        assert len(acks) == 1
        assert acks[0]["asset_name"] == "Composed Asset.html"
        assert acks[0]["checksum"] == _sha256(bundle_body)
        assert not [m for m in sent if m["type"] == "fetch_failed"]

    @pytest.mark.asyncio
    async def test_happy_path_cms_wire_format_uses_name_key(self, cms_client):
        """CMS protocol (cms/schemas/protocol.py Sibling model) sends
        siblings keyed as ``name``, not ``asset_name``.  Earlier firmware
        drafts only accepted ``asset_name`` and silently dropped real
        CMS messages on the floor.  This test pins the wire format:
        siblings with only ``name`` populated must work end-to-end."""
        bundle_body = b"<html>composed wire-format test</html>"
        sib_body = b"sibling-video-payload"
        sib_url = "http://cms.test/wire-clip.mp4"
        # Real CMS wire format: key is "name", no "asset_name"
        siblings = [{
            "name": "wire-clip.mp4",
            "asset_type": "video",
            "download_url": sib_url,
            "checksum": _sha256(sib_body),
            "size_bytes": len(sib_body),
        }]
        bundle_url = "http://cms.test/wire.html"
        msg = {
            "type": "fetch_asset",
            "asset_name": "wire.html",
            "asset_type": "composed",
            "download_url": bundle_url,
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": siblings,
        }
        fake_aiohttp, _ = _patch_aiohttp({
            bundle_url: bundle_body,
            sib_url: sib_body,
        })
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Both files landed
        assert (cms_client.settings.composed_dir / "wire.html").read_bytes() == bundle_body
        assert (cms_client.settings.videos_dir / "wire-clip.mp4").read_bytes() == sib_body

        # Sidecar emitted with "name" key (matches CMS schema)
        sidecar = json.loads((cms_client.settings.composed_dir / "wire.html.deps.json").read_text())
        assert sidecar["siblings"][0]["name"] == "wire-clip.mp4"

        # ACK on bundle, no fetch_failed
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert any(m["type"] == "asset_ack" for m in sent)
        assert not [m for m in sent if m["type"] == "fetch_failed"]

    @pytest.mark.asyncio
    async def test_no_siblings_uses_simple_path(self, cms_client):
        """Composed asset with no siblings keeps the legacy
        ``_download_one_asset`` path so the change is fully backward
        compatible with a CMS that doesn't know about sibling delivery yet."""
        body = b"<html>plain composed</html>"
        bundle_url = "http://cms.test/plain.html"
        msg = {
            "type": "fetch_asset",
            "asset_name": "plain.html",
            "asset_type": "composed",
            "download_url": bundle_url,
            "checksum": _sha256(body),
            "size_bytes": len(body),
        }
        fake_aiohttp, _ = _patch_aiohttp({bundle_url: body})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Bundle present; NO sidecar (simple path doesn't write one)
        assert (cms_client.settings.composed_dir / "plain.html").read_bytes() == body
        assert not (cms_client.settings.composed_dir / "plain.html.deps.json").exists()
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert any(m["type"] == "asset_ack" and m["asset_name"] == "plain.html" for m in sent)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_name", [
        "../escape.mp4",
        "subdir/clip.mp4",
        "C:\\windows\\boom.mp4",
        "with\x00nul.mp4",
        "..",
        ".",
        "",
    ])
    async def test_path_traversal_rejected(self, cms_client, bad_name):
        bundle_body = b"<html></html>"
        sibling_body = b"v"
        siblings = [{
            "asset_name": bad_name,
            "asset_type": "video",
            "download_url": "http://cms.test/whatever",
            "checksum": _sha256(sibling_body),
            "size_bytes": len(sibling_body),
        }]
        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": "http://cms.test/bundle.html",
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": siblings,
        }
        fake_aiohttp, fake_session = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        fails = [m for m in sent if m["type"] == "fetch_failed"]
        assert len(fails) == 1
        # Empty / "." / ".." are caught as descriptor errors (no asset_name);
        # others as invalid_sibling_name.
        assert fails[0]["reason"] in {"invalid_sibling_name", "invalid_sibling_descriptor"}
        # No downloads at all
        assert fake_session.calls == []

    @pytest.mark.asyncio
    async def test_invalid_sibling_descriptor_missing_url(self, cms_client):
        bundle_body = b"<html></html>"
        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": "http://cms.test/bundle.html",
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": [{"asset_name": "vid.mp4"}],  # no download_url
        }
        fake_aiohttp, fake_session = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        fails = [m for m in sent if m["type"] == "fetch_failed"]
        assert len(fails) == 1
        assert fails[0]["reason"] == "invalid_sibling_descriptor"
        assert fake_session.calls == []

    @pytest.mark.asyncio
    async def test_fast_path_all_cached_skips_download(self, cms_client):
        """Bundle + every sibling already cached → no downloads, just ACK."""
        bundle_body = b"<html>cached</html>"
        v_body = b"video-cached"
        siblings = [_make_sibling("c.mp4", v_body)]

        # Pre-populate cache: write bundle + sibling on disk and register both.
        bundle_path = cms_client.settings.composed_dir / "bundle.html"
        bundle_path.write_bytes(bundle_body)
        cms_client.asset_manager.register(
            "bundle.html", "composed/bundle.html", len(bundle_body), _sha256(bundle_body),
        )
        sib_path = cms_client.settings.videos_dir / "c.mp4"
        sib_path.write_bytes(v_body)
        cms_client.asset_manager.register(
            "c.mp4", "videos/c.mp4", len(v_body), _sha256(v_body),
        )
        # Pre-write sidecar so the fast-path check passes
        cms_client._write_composed_sidecar("bundle.html", _sha256(bundle_body), siblings)

        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": "http://cms.test/bundle.html",
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": siblings,
        }
        fake_aiohttp, fake_session = _patch_aiohttp({})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        assert fake_session.calls == []
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        acks = [m for m in sent if m["type"] == "asset_ack"]
        assert len(acks) == 1
        assert acks[0]["checksum"] == _sha256(bundle_body)

    @pytest.mark.asyncio
    async def test_missing_sibling_triggers_slow_path(self, cms_client):
        """Bundle cached but sibling missing → slow path: download sibling
        + (re-)download bundle, write sidecar, ACK."""
        bundle_body = b"<html>bundle</html>"
        v_body = b"missing-video"
        siblings = [_make_sibling("v.mp4", v_body)]

        # Pre-cache the bundle only.
        (cms_client.settings.composed_dir / "bundle.html").write_bytes(bundle_body)
        cms_client.asset_manager.register(
            "bundle.html", "composed/bundle.html", len(bundle_body), _sha256(bundle_body),
        )
        # No sidecar yet; sibling not on disk.

        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": "http://cms.test/bundle.html",
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": siblings,
        }
        mapping = {
            msg["download_url"]: bundle_body,
            siblings[0]["download_url"]: v_body,
        }
        fake_aiohttp, fake_session = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Sibling downloaded; bundle re-downloaded (slow path always rewrites it)
        assert siblings[0]["download_url"] in fake_session.calls
        assert (cms_client.settings.videos_dir / "v.mp4").read_bytes() == v_body
        # Sidecar now persisted
        assert (cms_client.settings.composed_dir / "bundle.html.deps.json").exists()
        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        assert any(m["type"] == "asset_ack" for m in sent)

    @pytest.mark.asyncio
    async def test_sibling_download_failure_aborts(self, cms_client):
        """A sibling failing to download must short-circuit the bundle
        download and emit a precise `fetch_failed`."""
        bundle_body = b"<html>bundle</html>"
        v_body = b"video"
        sib = _make_sibling("v.mp4", v_body)
        # mapping intentionally omits the sibling URL → 404
        bundle_url = "http://cms.test/bundle.html"
        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": bundle_url,
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": [sib],
        }
        fake_aiohttp, fake_session = _patch_aiohttp({bundle_url: bundle_body})
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        sent = [json.loads(c.args[0]) for c in cms_client._ws.send.call_args_list]
        fails = [m for m in sent if m["type"] == "fetch_failed"]
        assert len(fails) == 1
        assert fails[0]["reason"] == "sibling_download_failed"
        assert fails[0]["sibling_asset"] == "v.mp4"
        # Bundle was NOT downloaded
        assert bundle_url not in fake_session.calls
        # No ACK
        assert not [m for m in sent if m["type"] == "asset_ack"]

    @pytest.mark.asyncio
    async def test_get_scheduled_asset_names_expands_composed(self, cms_client, tmp_path):
        """Scheduled composed slide must expose its siblings to the
        eviction-protection set."""
        # Register a composed bundle in the manifest
        cms_client.asset_manager.register(
            "live.html", "composed/live.html", 10, "bundle-hash",
        )
        # Write its sidecar
        cms_client._write_composed_sidecar(
            "live.html",
            "bundle-hash",
            [
                {"asset_name": "vid-a.mp4", "asset_type": "video",
                 "download_url": "x", "checksum": "ha", "size_bytes": 1},
                {"asset_name": "vid-b.mp4", "asset_type": "video",
                 "download_url": "x", "checksum": "hb", "size_bytes": 1},
            ],
        )
        # Schedule references the composed asset
        cms_client.settings.schedule_path.write_text(json.dumps({
            "schedules": [{"asset": "live.html"}],
            "default_asset": None,
        }))
        names = cms_client._get_scheduled_asset_names()
        assert "live.html" in names
        assert "vid-a.mp4" in names
        assert "vid-b.mp4" in names

    @pytest.mark.asyncio
    async def test_eviction_protects_sibling_names(self, cms_client):
        """During bulk eviction for a composed fetch, siblings about to
        be downloaded must be in the protected set so they are not
        immediately evicted to make room for themselves."""
        bundle_body = b"<html></html>"
        v_body = b"v-bytes"
        sib = _make_sibling("v.mp4", v_body)

        # Pre-register the sibling in the manifest as if a previous schedule
        # left it on disk; then verify it's still present after the fetch.
        (cms_client.settings.videos_dir / "v.mp4").write_bytes(v_body)
        cms_client.asset_manager.register(
            "v.mp4", "videos/v.mp4", len(v_body), _sha256(v_body),
        )

        msg = {
            "type": "fetch_asset",
            "asset_name": "bundle.html",
            "asset_type": "composed",
            "download_url": "http://cms.test/bundle.html",
            "checksum": _sha256(bundle_body),
            "size_bytes": len(bundle_body),
            "siblings": [sib],
        }
        mapping = {msg["download_url"]: bundle_body}
        fake_aiohttp, _ = _patch_aiohttp(mapping)
        with patch.dict(sys.modules, {"aiohttp": fake_aiohttp}):
            await cms_client._handle_fetch_asset(msg, cms_client._ws)

        # Sibling untouched (still cached with same checksum)
        assert cms_client.asset_manager.has_asset("v.mp4", _sha256(v_body))
        assert (cms_client.settings.videos_dir / "v.mp4").exists()
