"""Tests for the ``request_logs`` handler.

Covers the post-Stage-3c replacement where large log payloads are
HTTP-POSTed straight to the CMS instead of being split into LGCK
binary WS frames (WPS transport cannot carry binary frames).

- Small payloads still take the single-message JSON path, untouched.
- journalctl-unavailable error still flows through the JSON path.
- Large payloads build a gzipped tarball and POST it to
  ``/api/devices/{device_id}/logs/{request_id}/upload`` with the
  device API key, deriving the base URL from the active ws URL when
  ``cms_api_url`` is unset.
- A ``409 {"status": "ready"}`` response is treated as terminal
  success (idempotent retry), not an error.
- A non-2xx response (other than the ready-409) surfaces an
  ``upload_failed`` logs_response to the CMS.
- Oversize payloads short-circuit before any HTTP call and send a
  ``logs_too_large`` error instead.
"""

from __future__ import annotations

import gzip
import io
import json
import sys
import tarfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy deps before importing the service module (matches the
# pattern used by test_wipe_assets.py).
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import (  # noqa: E402
    CMSClient,
    LOGS_JSON_MAX_BYTES,
    LOGS_UPLOAD_MAX_BYTES,
    PROTOCOL_VERSION,
    _build_logs_tar_gz,
)


def _make_settings(tmp_path: Path) -> MagicMock:
    s = MagicMock()
    s.agora_base = tmp_path
    assets = tmp_path / "assets"; assets.mkdir()
    state = tmp_path / "state"; state.mkdir()
    persist = tmp_path / "persist"; persist.mkdir()
    s.assets_dir = assets
    s.videos_dir = assets / "videos"; s.videos_dir.mkdir()
    s.images_dir = assets / "images"; s.images_dir.mkdir()
    s.splash_dir = assets / "splash"; s.splash_dir.mkdir()
    s.state_dir = state
    s.persist_dir = persist
    s.manifest_path = state / "assets.json"
    s.auth_token_path = persist / "auth_token"
    s.cms_status_path = state / "cms_status.json"
    s.cms_config_path = persist / "cms_config.json"
    s.device_name = "test-device"
    s.cms_url = "wss://cms.example.com/ws/device"
    s.cms_api_url = ""  # force derivation from active ws url
    s.cms_transport = "wps"
    s.log_level = "INFO"
    s.asset_budget_mb = 500
    s.storage_budget_mb = 500
    return s


def _make_client(tmp_path: Path) -> CMSClient:
    with patch("cms_client.service._get_device_id", return_value="dev-123"):
        client = CMSClient(_make_settings(tmp_path))
    client._active_cms_url = "wss://cms.example.com/ws/device"
    # Pretend the persist/api_key file exists with a real key.
    (client.settings.persist_dir / "api_key").write_text("secret-key")
    return client


def _extract_tar_gz(blob: bytes) -> dict[str, str]:
    buf = io.BytesIO(blob)
    with tarfile.open(fileobj=buf, mode="r:gz") as tf:
        out: dict[str, str] = {}
        for member in tf.getmembers():
            f = tf.extractfile(member)
            if f is None:
                continue
            out[member.name] = f.read().decode("utf-8")
        return out


class _FakeResponse:
    def __init__(self, status: int, body: dict | str = "ok"):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        if isinstance(self._body, dict):
            return self._body
        raise ValueError("not json")

    async def text(self):
        if isinstance(self._body, dict):
            return json.dumps(self._body)
        return self._body


class _FakeSession:
    """Records POSTs and returns canned responses."""

    def __init__(self, response: _FakeResponse):
        self._response = response
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def post(self, url, *, headers=None, data=None):
        self.calls.append({"url": url, "headers": dict(headers or {}), "data": data})
        return self._response


class _FakeAiohttp:
    def __init__(self, response: _FakeResponse):
        self.session = _FakeSession(response)
        self.ClientTimeout = lambda total=None: total  # accept kwarg

    def ClientSession(self, *a, **kw):
        return self.session


@pytest.mark.asyncio
async def test_small_payload_uses_json_ws_path(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = "tiny log line"
    fake_result.stderr = ""
    with patch("cms_client.service.subprocess.run", return_value=fake_result):
        await client._handle_request_logs(
            {"request_id": "r1", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    ws.send.assert_awaited_once()
    payload = json.loads(ws.send.call_args.args[0])
    assert payload["type"] == "logs_response"
    assert payload["request_id"] == "r1"
    assert payload["protocol_version"] == PROTOCOL_VERSION
    assert payload["logs"] == {"agora-api": "tiny log line"}
    assert payload["error"] is None


@pytest.mark.asyncio
async def test_journalctl_missing_reports_error_on_ws(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    with patch(
        "cms_client.service.subprocess.run",
        side_effect=FileNotFoundError("no journalctl"),
    ):
        await client._handle_request_logs(
            {"request_id": "rx", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    ws.send.assert_awaited_once()
    payload = json.loads(ws.send.call_args.args[0])
    assert payload["error"] == "journalctl not available on this device"
    assert payload["request_id"] == "rx"


@pytest.mark.asyncio
async def test_large_payload_http_uploaded_with_api_key(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    # Big enough output so the JSON response exceeds the cap.
    big = "x" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(200, {"status": "ready", "size_bytes": 1}))

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "big1", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    # No WS send should have occurred (upload path is pure HTTP).
    ws.send.assert_not_awaited()

    assert len(fake_aiohttp.session.calls) == 1
    call = fake_aiohttp.session.calls[0]
    assert call["url"] == (
        "https://cms.example.com/api/devices/dev-123/logs/big1/upload"
    )
    assert call["headers"]["X-Device-API-Key"] == "secret-key"
    assert call["headers"]["Content-Type"] == "application/gzip"
    # Body must be a valid gzipped tarball whose entry matches the service name.
    extracted = _extract_tar_gz(call["data"])
    assert "agora-api.log" in extracted
    assert extracted["agora-api.log"] == big


@pytest.mark.asyncio
async def test_cms_api_url_setting_takes_precedence(tmp_path):
    client = _make_client(tmp_path)
    client.settings.cms_api_url = "https://override.example.org"
    ws = MagicMock()
    ws.send = AsyncMock()

    big = "y" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(200, {"status": "ready"}))

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "big2", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    assert fake_aiohttp.session.calls[0]["url"].startswith(
        "https://override.example.org/api/devices/dev-123/logs/big2/upload"
    )


@pytest.mark.asyncio
async def test_409_already_ready_is_terminal_success(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    big = "z" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(409, {"status": "ready"}))

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "big3", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    # Treated as success — no error frame pushed back on the WS.
    ws.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_409_non_ready_surfaces_upload_failed(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    big = "q" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(409, {"status": "failed"}))

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "big4", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    ws.send.assert_awaited_once()
    payload = json.loads(ws.send.call_args.args[0])
    assert payload["type"] == "logs_response"
    assert payload["request_id"] == "big4"
    assert payload["error"].startswith("upload_failed:")


@pytest.mark.asyncio
async def test_5xx_response_surfaces_upload_failed(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    big = "p" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(500, "internal server error"))

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "big5", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    ws.send.assert_awaited_once()
    payload = json.loads(ws.send.call_args.args[0])
    assert payload["error"].startswith("upload_failed:")
    assert "500" in payload["error"]


@pytest.mark.asyncio
async def test_oversize_payload_short_circuits_without_http_call(tmp_path):
    client = _make_client(tmp_path)
    ws = MagicMock()
    ws.send = AsyncMock()

    big = "x" * (LOGS_JSON_MAX_BYTES + 100_000)
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = big
    fake_result.stderr = ""

    fake_aiohttp = _FakeAiohttp(_FakeResponse(200, {"status": "ready"}))
    # Pretend the gzipped tarball exploded past the CMS cap.
    oversized = b"\0" * (LOGS_UPLOAD_MAX_BYTES + 1)

    with patch("cms_client.service.subprocess.run", return_value=fake_result), \
         patch(
             "cms_client.service._build_logs_tar_gz", return_value=oversized,
         ), \
         patch("cms_client.service.aiohttp", fake_aiohttp, create=True):
        await client._handle_request_logs(
            {"request_id": "huge", "services": ["agora-api"], "since": "1h"},
            ws,
        )

    assert fake_aiohttp.session.calls == []
    ws.send.assert_awaited_once()
    payload = json.loads(ws.send.call_args.args[0])
    assert payload["error"].startswith("logs_too_large:")


def test_build_logs_tar_gz_roundtrip():
    blob = _build_logs_tar_gz({"agora-api": "hello", "agora-player": "world"})
    # gzip magic
    assert blob[:2] == b"\x1f\x8b"
    extracted = _extract_tar_gz(blob)
    assert extracted == {"agora-api.log": "hello", "agora-player.log": "world"}


def test_logs_api_base_falls_back_to_active_ws_url(tmp_path):
    client = _make_client(tmp_path)
    client.settings.cms_api_url = ""
    client._active_cms_url = "wss://cms.example.com/ws/device"
    assert client._logs_api_base() == "https://cms.example.com"


def test_logs_api_base_respects_setting(tmp_path):
    client = _make_client(tmp_path)
    client.settings.cms_api_url = "https://override.example.org"
    assert client._logs_api_base() == "https://override.example.org"
