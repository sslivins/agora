"""Tests for ``bind_display`` / ``unbind_display`` WS handlers + heartbeat slots.

PR 2a wires two new CMS-driven messages into ``cms_client.service``:

- ``bind_display``: persist slot-B credentials so the player can
  activate slot B; reply with ``bind_ack``.
- ``unbind_display``: remove slot-B credentials so the player tears
  slot B down; reply with ``unbind_ack``.

The heartbeat also advertises ``available_slots`` (Pi5 → ``["A","B"]``,
others → ``["A"]``).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient  # noqa: E402
from shared.devices_store import (  # noqa: E402
    SLOT_A,
    SLOT_B,
    devices_path,
    read_slot,
    write_slot,
)


# ── Fixture ──────────────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path: Path):
    """Bypass-init CMSClient stub with just enough surface for the handlers."""
    settings = MagicMock()
    settings.persist_dir = tmp_path / "persist"
    settings.persist_dir.mkdir()
    settings.state_dir = tmp_path / "state"
    settings.state_dir.mkdir()
    settings.assets_dir = tmp_path / "assets"
    settings.assets_dir.mkdir()
    settings.current_state_path = settings.state_dir / "current.json"
    settings.device_name = "agora-node-test"

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        c = CMSClient(settings)
    c.settings = settings
    c.device_id = "slot-a-device-id"
    return c


@pytest.fixture
def ws_mock():
    ws = MagicMock()
    ws.send = AsyncMock()
    return ws


def _sent_payloads(ws_mock) -> list[dict]:
    return [json.loads(call.args[0]) for call in ws_mock.send.await_args_list]


# ── bind_display ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bind_display_persists_creds_and_acks(client, ws_mock, tmp_path: Path):
    msg = {
        "type": "bind_display",
        "slot": "B",
        "device_id": "slot-b-id",
        "api_key": "slot-b-key",
    }
    await client._handle_bind_display(msg, ws_mock)

    creds = read_slot(client.settings.persist_dir, SLOT_B)
    assert creds == {"device_id": "slot-b-id", "api_key": "slot-b-key"}

    sent = _sent_payloads(ws_mock)
    assert sent == [{"type": "bind_ack", "slot": "B", "status": "ok"}]


@pytest.mark.asyncio
async def test_bind_display_does_not_touch_slot_a(client, ws_mock):
    write_slot(
        client.settings.persist_dir, SLOT_A,
        {"device_id": "slot-a-id", "api_key": "slot-a-key"},
    )
    msg = {
        "type": "bind_display", "slot": "B",
        "device_id": "slot-b-id", "api_key": "slot-b-key",
    }
    await client._handle_bind_display(msg, ws_mock)

    a = read_slot(client.settings.persist_dir, SLOT_A)
    assert a == {"device_id": "slot-a-id", "api_key": "slot-a-key"}


@pytest.mark.asyncio
async def test_bind_display_rejects_slot_a(client, ws_mock):
    msg = {
        "type": "bind_display", "slot": "A",
        "device_id": "x", "api_key": "y",
    }
    await client._handle_bind_display(msg, ws_mock)

    sent = _sent_payloads(ws_mock)
    assert len(sent) == 1
    assert sent[0]["type"] == "bind_ack"
    assert sent[0]["status"] == "error"
    assert read_slot(client.settings.persist_dir, SLOT_A) is None


@pytest.mark.asyncio
async def test_bind_display_rejects_unknown_slot(client, ws_mock):
    msg = {
        "type": "bind_display", "slot": "C",
        "device_id": "x", "api_key": "y",
    }
    await client._handle_bind_display(msg, ws_mock)

    sent = _sent_payloads(ws_mock)
    assert sent[0]["status"] == "error"


@pytest.mark.asyncio
async def test_bind_display_rejects_missing_device_id(client, ws_mock):
    msg = {"type": "bind_display", "slot": "B", "api_key": "k"}
    await client._handle_bind_display(msg, ws_mock)
    sent = _sent_payloads(ws_mock)
    assert sent[0]["status"] == "error"
    assert "device_id" in sent[0]["error"]


@pytest.mark.asyncio
async def test_bind_display_rejects_missing_api_key(client, ws_mock):
    msg = {"type": "bind_display", "slot": "B", "device_id": "d"}
    await client._handle_bind_display(msg, ws_mock)
    sent = _sent_payloads(ws_mock)
    assert sent[0]["status"] == "error"


@pytest.mark.asyncio
async def test_bind_display_idempotent_overwrites_existing(client, ws_mock):
    write_slot(
        client.settings.persist_dir, SLOT_B,
        {"device_id": "old", "api_key": "old-key"},
    )
    msg = {
        "type": "bind_display", "slot": "B",
        "device_id": "new", "api_key": "new-key",
    }
    await client._handle_bind_display(msg, ws_mock)
    creds = read_slot(client.settings.persist_dir, SLOT_B)
    assert creds == {"device_id": "new", "api_key": "new-key"}


# ── unbind_display ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unbind_display_clears_creds_and_acks(client, ws_mock):
    write_slot(
        client.settings.persist_dir, SLOT_B,
        {"device_id": "b-id", "api_key": "b-key"},
    )
    msg = {"type": "unbind_display", "slot": "B"}
    await client._handle_unbind_display(msg, ws_mock)
    assert read_slot(client.settings.persist_dir, SLOT_B) is None
    sent = _sent_payloads(ws_mock)
    assert sent == [{"type": "unbind_ack", "slot": "B", "status": "ok"}]


@pytest.mark.asyncio
async def test_unbind_display_preserves_slot_a(client, ws_mock):
    write_slot(
        client.settings.persist_dir, SLOT_A,
        {"device_id": "a-id", "api_key": "a-key"},
    )
    write_slot(
        client.settings.persist_dir, SLOT_B,
        {"device_id": "b-id", "api_key": "b-key"},
    )
    msg = {"type": "unbind_display", "slot": "B"}
    await client._handle_unbind_display(msg, ws_mock)
    a = read_slot(client.settings.persist_dir, SLOT_A)
    assert a == {"device_id": "a-id", "api_key": "a-key"}


@pytest.mark.asyncio
async def test_unbind_display_when_already_absent_still_acks_ok(client, ws_mock):
    msg = {"type": "unbind_display", "slot": "B"}
    await client._handle_unbind_display(msg, ws_mock)
    sent = _sent_payloads(ws_mock)
    assert sent[0]["status"] == "ok"


@pytest.mark.asyncio
async def test_unbind_display_rejects_slot_a(client, ws_mock):
    write_slot(
        client.settings.persist_dir, SLOT_A,
        {"device_id": "a", "api_key": "k"},
    )
    msg = {"type": "unbind_display", "slot": "A"}
    await client._handle_unbind_display(msg, ws_mock)
    sent = _sent_payloads(ws_mock)
    assert sent[0]["status"] == "error"
    assert read_slot(client.settings.persist_dir, SLOT_A) is not None


# ── Heartbeat available_slots ────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_status_advertises_two_slots_on_pi5(client):
    client.settings.current_state_path.write_text(
        '{"mode": "splash", "asset": null}'
    )
    client._ws = MagicMock()
    client._ws.send = AsyncMock()
    with patch("shared.board.hdmi_port_count", return_value=2), \
         patch("cms_client.service._get_storage_mb", return_value=(100, 50)), \
         patch("cms_client.service._get_cpu_temp", return_value=42.0), \
         patch("cms_client.service._is_ssh_enabled", return_value=False), \
         patch("cms_client.service._is_local_api_enabled", return_value=False):
        await client._send_status()
    sent = json.loads(client._ws.send.await_args.args[0])
    assert sent["available_slots"] == ["A", "B"]


@pytest.mark.asyncio
async def test_send_status_single_slot_on_pi_zero(client):
    client.settings.current_state_path.write_text(
        '{"mode": "splash", "asset": null}'
    )
    client._ws = MagicMock()
    client._ws.send = AsyncMock()
    with patch("shared.board.hdmi_port_count", return_value=1), \
         patch("cms_client.service._get_storage_mb", return_value=(100, 50)), \
         patch("cms_client.service._get_cpu_temp", return_value=42.0), \
         patch("cms_client.service._is_ssh_enabled", return_value=False), \
         patch("cms_client.service._is_local_api_enabled", return_value=False):
        await client._send_status()
    sent = json.loads(client._ws.send.await_args.args[0])
    assert sent["available_slots"] == ["A"]


@pytest.mark.asyncio
async def test_send_status_falls_back_to_slot_a_on_board_error(client):
    client.settings.current_state_path.write_text(
        '{"mode": "splash", "asset": null}'
    )
    client._ws = MagicMock()
    client._ws.send = AsyncMock()
    with patch("shared.board.hdmi_port_count", side_effect=RuntimeError("boom")), \
         patch("cms_client.service._get_storage_mb", return_value=(100, 50)), \
         patch("cms_client.service._get_cpu_temp", return_value=42.0), \
         patch("cms_client.service._is_ssh_enabled", return_value=False), \
         patch("cms_client.service._is_local_api_enabled", return_value=False):
        await client._send_status()
    sent = json.loads(client._ws.send.await_args.args[0])
    assert sent["available_slots"] == ["A"]
