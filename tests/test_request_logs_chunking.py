"""Tests for the chunked ``request_logs`` handler (Stage 3c / #345).

Covers:

- Small payloads (< JSON cap) → single ``logs_response`` JSON frame
  (legacy path, untouched).
- journalctl-unavailable error → single ``logs_response`` with error.
- Large payloads (> JSON cap) → gzipped tarball split into LGCK
  binary frames in the correct sequence.
- Oversize payloads (> assembled cap) → ``logs_response`` with a
  ``logs_too_large`` error instead of chunks.
- Wire-format helpers (``_encode_logs_chunk_frame``).
"""

from __future__ import annotations

import io
import json
import struct
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
    LOGS_CHUNK_ASSEMBLED_CAP,
    LOGS_CHUNK_FLAG_FINAL,
    LOGS_CHUNK_MAGIC,
    LOGS_CHUNK_MAX_COUNT,
    LOGS_CHUNK_PAYLOAD_BYTES,
    LOGS_JSON_MAX_BYTES,
    PROTOCOL_VERSION,
    _build_logs_tar_gz,
    _encode_logs_chunk_frame,
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
    s.schedule_path = state / "schedule.json"
    s.current_state_path = state / "current.json"
    s.desired_state_path = state / "desired.json"
    s.splash_config_path = state / "splash.txt"
    s.cms_config_path = persist / "cms_config.json"
    s.auth_token_path = persist / "cms_auth_token"
    s.storage_budget_mb = 500
    s.asset_budget_mb = 500
    return s


def _build_client(settings) -> CMSClient:
    with patch("cms_client.service._get_device_id", return_value="test-pi"):
        return CMSClient(settings)


# ── Wire-format tests ────────────────────────────────────────────────


def test_protocol_version_is_two():
    assert PROTOCOL_VERSION == 2


def test_encode_frame_layout():
    payload = b"hello"
    frame = _encode_logs_chunk_frame(
        request_id="abc", seq=1, total=3, payload=payload, is_final=False,
    )
    assert frame[:4] == LOGS_CHUNK_MAGIC
    assert frame[4] == 1  # version
    rid_len = struct.unpack("<H", frame[5:7])[0]
    assert rid_len == 3
    assert frame[7:10] == b"abc"
    seq, total, flags = struct.unpack("<HHB", frame[10:15])
    assert (seq, total, flags) == (1, 3, 0)
    assert frame[15:] == payload


def test_encode_frame_final_flag():
    frame = _encode_logs_chunk_frame(
        request_id="r", seq=2, total=3, payload=b"", is_final=True,
    )
    # header layout: magic(4) + ver(1) + rid_len(2) + rid(1) + seq(2) + total(2) + flags(1)
    flags = frame[12]
    assert flags & LOGS_CHUNK_FLAG_FINAL


def test_encode_frame_validates():
    with pytest.raises(ValueError):
        _encode_logs_chunk_frame(
            request_id="r", seq=3, total=3, payload=b"", is_final=True,
        )
    with pytest.raises(ValueError):
        _encode_logs_chunk_frame(
            request_id="r", seq=0, total=0, payload=b"", is_final=True,
        )


def test_build_logs_tar_gz_roundtrip():
    data = _build_logs_tar_gz({
        "agora-player": "line1\nline2",
        "path/with/slashes": "sanitised",
    })
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        names = sorted(m.name for m in tf.getmembers())
        assert names == sorted(["agora-player.log", "path_with_slashes.log"])
        body = tf.extractfile("agora-player.log").read()
        assert body == b"line1\nline2"


# ── Handler path selection ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_small_payload_uses_json_path(tmp_path):
    client = _build_client(_make_settings(tmp_path))
    ws = MagicMock()
    sent: list = []

    async def capture_send(data):
        sent.append(data)
    ws.send = capture_send

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "short"
    with patch("cms_client.service.subprocess.run", return_value=mock_result):
        await client._handle_request_logs(
            {"request_id": "req-1", "services": ["agora-player"], "since": "1h"},
            ws,
        )

    assert len(sent) == 1
    msg = json.loads(sent[0])
    assert msg["type"] == "logs_response"
    assert msg["request_id"] == "req-1"
    assert msg["error"] is None
    assert msg["logs"]["agora-player"] == "short"


@pytest.mark.asyncio
async def test_journalctl_missing_uses_json_error_path(tmp_path):
    client = _build_client(_make_settings(tmp_path))
    ws = MagicMock()
    sent: list = []

    async def capture_send(data):
        sent.append(data)
    ws.send = capture_send

    with patch("cms_client.service.subprocess.run", side_effect=FileNotFoundError()):
        await client._handle_request_logs(
            {"request_id": "req-2", "services": ["agora-player"]},
            ws,
        )

    assert len(sent) == 1
    msg = json.loads(sent[0])
    assert msg["error"] == "journalctl not available on this device"


@pytest.mark.asyncio
async def test_large_payload_streams_as_chunks(tmp_path):
    client = _build_client(_make_settings(tmp_path))
    ws = MagicMock()
    sent: list = []

    async def capture_send(data):
        sent.append(data)
    ws.send = capture_send

    # Use text that compresses poorly so the tarball is guaranteed to
    # be > LOGS_JSON_MAX_BYTES but < the assembled cap.
    import os
    big = os.urandom(1_500_000).hex()  # ~3 MB of hex ASCII
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = big
    with patch("cms_client.service.subprocess.run", return_value=mock_result):
        await client._handle_request_logs(
            {"request_id": "big-req", "services": ["agora-player"]},
            ws,
        )

    # Expect only binary frames — no JSON.
    assert len(sent) >= 2
    for frame in sent:
        assert isinstance(frame, (bytes, bytearray))
        assert frame[:4] == LOGS_CHUNK_MAGIC

    # Re-assemble and verify it's a valid tar.gz containing our data.
    reassembled = b""
    for i, frame in enumerate(sent):
        # Parse header to extract payload slice.
        rid_len = struct.unpack("<H", frame[5:7])[0]
        assert frame[7:7 + rid_len] == b"big-req"
        seq, total, flags = struct.unpack(
            "<HHB", frame[7 + rid_len:7 + rid_len + 5],
        )
        assert seq == i
        assert total == len(sent)
        if i == len(sent) - 1:
            assert flags & LOGS_CHUNK_FLAG_FINAL
        else:
            assert not (flags & LOGS_CHUNK_FLAG_FINAL)
        reassembled += frame[7 + rid_len + 5:]

    with tarfile.open(fileobj=io.BytesIO(reassembled), mode="r:gz") as tf:
        body = tf.extractfile("agora-player.log").read().decode("utf-8")
    assert body == big


@pytest.mark.asyncio
async def test_oversize_payload_sends_error_response(tmp_path, monkeypatch):
    client = _build_client(_make_settings(tmp_path))
    ws = MagicMock()
    sent: list = []

    async def capture_send(data):
        sent.append(data)
    ws.send = capture_send

    # Force the assembled-cap check to fire by patching the size limits
    # to something small, then feeding a payload that exceeds it.
    monkeypatch.setattr("cms_client.service.LOGS_JSON_MAX_BYTES", 100)
    monkeypatch.setattr("cms_client.service.LOGS_CHUNK_ASSEMBLED_CAP", 1000)

    mock_result = MagicMock()
    mock_result.returncode = 0
    # Random data doesn't compress, so the tarball exceeds the patched
    # assembled cap reliably.
    import os as _os
    mock_result.stdout = _os.urandom(3000).hex()  # 6000 ASCII chars
    with patch("cms_client.service.subprocess.run", return_value=mock_result):
        await client._handle_request_logs(
            {"request_id": "huge", "services": ["agora-player"]},
            ws,
        )

    assert len(sent) == 1
    msg = json.loads(sent[0])
    assert msg["type"] == "logs_response"
    assert msg["error"].startswith("logs_too_large")


@pytest.mark.asyncio
async def test_too_many_chunks_sends_error_response(tmp_path, monkeypatch):
    client = _build_client(_make_settings(tmp_path))
    ws = MagicMock()
    sent: list = []

    async def capture_send(data):
        sent.append(data)
    ws.send = capture_send

    # Tiny chunk payload size so the count cap is exercised before the
    # assembled-byte cap.
    monkeypatch.setattr("cms_client.service.LOGS_JSON_MAX_BYTES", 10)
    monkeypatch.setattr("cms_client.service.LOGS_CHUNK_PAYLOAD_BYTES", 64)
    monkeypatch.setattr("cms_client.service.LOGS_CHUNK_MAX_COUNT", 3)
    monkeypatch.setattr("cms_client.service.LOGS_CHUNK_ASSEMBLED_CAP", 100_000)

    mock_result = MagicMock()
    mock_result.returncode = 0
    import os as _os
    mock_result.stdout = _os.urandom(1500).hex()  # 3000 ASCII incompressible
    with patch("cms_client.service.subprocess.run", return_value=mock_result):
        await client._handle_request_logs(
            {"request_id": "manychunks", "services": ["agora-player"]},
            ws,
        )

    assert len(sent) == 1
    msg = json.loads(sent[0])
    assert msg["type"] == "logs_response"
    assert msg["error"].startswith("logs_too_large")


# ── Register capability advertisement ────────────────────────────────


def test_capabilities_advertised_in_module():
    """Sanity-check the module-level constants used in the register
    message so the CMS can trust the chunking path is supported."""
    assert LOGS_JSON_MAX_BYTES > 0
    assert LOGS_CHUNK_PAYLOAD_BYTES > 0
    assert LOGS_CHUNK_MAX_COUNT > 0
    assert LOGS_CHUNK_ASSEMBLED_CAP >= LOGS_CHUNK_PAYLOAD_BYTES
