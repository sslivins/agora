"""Tests for the CMS client transport abstraction.

Covers:
- DirectTransport pass-through (send + iter behave like raw ws).
- WPSTransport wrap/unwrap of the Azure Web PubSub envelope.
- _derive_api_base URL conversion (wss://host/ws → https://host).
- open_wps() calls POST /api/devices/{id}/connect-token with the
  X-Device-API-Key header and threads the returned url/token into
  websockets.connect() with the json.webpubsub.azure.v1 subprotocol.
- open_transport() dispatches to the correct implementation.
"""

from __future__ import annotations

import json
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Stub heavy deps before importing the module under test.
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client import transport as transport_mod
from cms_client.transport import (
    DirectTransport,
    TransportError,
    WPSTransport,
    _derive_api_base,
    open_transport,
)


class _FakeWS:
    """Minimal async-iterable websocket stand-in."""

    def __init__(self, incoming: list[str] | None = None) -> None:
        self.sent: list[str] = []
        self._incoming = list(incoming or [])
        self.closed = False

    async def send(self, data) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        self._iter = iter(self._incoming)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class TestDirectTransport:
    @pytest.mark.asyncio
    async def test_send_is_passthrough(self):
        ws = _FakeWS()
        t = DirectTransport(ws)
        await t.send(json.dumps({"type": "register", "id": "abc"}))
        assert ws.sent == ['{"type": "register", "id": "abc"}']

    @pytest.mark.asyncio
    async def test_iter_is_passthrough(self):
        ws = _FakeWS(['{"type":"sync"}', '{"type":"play"}'])
        t = DirectTransport(ws)
        got = [raw async for raw in t]
        assert got == ['{"type":"sync"}', '{"type":"play"}']

    @pytest.mark.asyncio
    async def test_close_closes_ws(self):
        ws = _FakeWS()
        t = DirectTransport(ws)
        await t.close()
        assert ws.closed
        # Second close is a no-op
        await t.close()


class TestWPSTransport:
    @pytest.mark.asyncio
    async def test_send_wraps_in_wps_envelope(self):
        ws = _FakeWS()
        t = WPSTransport(ws)
        await t.send(json.dumps({"type": "register", "device_id": "pi-01"}))
        assert len(ws.sent) == 1
        env = json.loads(ws.sent[0])
        assert env["type"] == "event"
        assert env["event"] == "message"
        assert env["dataType"] == "json"
        assert env["data"] == {"type": "register", "device_id": "pi-01"}

    @pytest.mark.asyncio
    async def test_send_rejects_binary(self):
        t = WPSTransport(_FakeWS())
        with pytest.raises(TransportError):
            await t.send(b"\x00\x01")

    @pytest.mark.asyncio
    async def test_send_rejects_non_json(self):
        t = WPSTransport(_FakeWS())
        with pytest.raises(TransportError):
            await t.send("not json")

    @pytest.mark.asyncio
    async def test_iter_unwraps_message_frames(self):
        frames = [
            json.dumps({
                "type": "message",
                "from": "server",
                "dataType": "json",
                "data": {"type": "sync", "schedules": []},
            }),
            json.dumps({
                "type": "message",
                "dataType": "json",
                "data": {"type": "play", "asset_id": "x"},
            }),
        ]
        t = WPSTransport(_FakeWS(frames))
        payloads = [json.loads(raw) async for raw in t]
        assert payloads == [
            {"type": "sync", "schedules": []},
            {"type": "play", "asset_id": "x"},
        ]

    @pytest.mark.asyncio
    async def test_iter_skips_system_and_ack_frames(self):
        frames = [
            json.dumps({"type": "system", "event": "connected"}),
            json.dumps({"type": "ack", "ackId": 1, "success": True}),
            json.dumps({
                "type": "message",
                "dataType": "json",
                "data": {"type": "sync"},
            }),
        ]
        t = WPSTransport(_FakeWS(frames))
        payloads = [json.loads(raw) async for raw in t]
        assert payloads == [{"type": "sync"}]

    @pytest.mark.asyncio
    async def test_iter_handles_stringified_data(self):
        """Some senders double-encode the data field."""
        inner = {"type": "sync"}
        frames = [
            json.dumps({
                "type": "message",
                "dataType": "text",
                "data": json.dumps(inner),
            }),
        ]
        t = WPSTransport(_FakeWS(frames))
        payloads = [json.loads(raw) async for raw in t]
        assert payloads == [inner]

    @pytest.mark.asyncio
    async def test_iter_drops_malformed_json(self):
        frames = ["not-json", json.dumps({
            "type": "message", "dataType": "json", "data": {"ok": True},
        })]
        t = WPSTransport(_FakeWS(frames))
        payloads = [json.loads(raw) async for raw in t]
        assert payloads == [{"ok": True}]


class TestDeriveApiBase:
    @pytest.mark.parametrize("ws_url,expected", [
        ("wss://cms.example.com/ws/device", "https://cms.example.com"),
        ("ws://192.168.1.100:8080/ws/device", "http://192.168.1.100:8080"),
        ("wss://cms.example.com:8443/ws/device/pi-01", "https://cms.example.com:8443"),
        ("https://cms.example.com/", "https://cms.example.com"),
    ])
    def test_conversion(self, ws_url, expected):
        assert _derive_api_base(ws_url) == expected


class TestOpenTransport:
    @pytest.mark.asyncio
    async def test_dispatches_to_direct(self):
        fake_ws = _FakeWS()

        async def fake_connect(url, **kwargs):
            assert url == "wss://cms.example.com/ws/device"
            # direct mode must NOT pass the WPS subprotocol
            assert "subprotocols" not in kwargs or kwargs["subprotocols"] is None
            return fake_ws

        with patch.object(transport_mod.websockets, "connect", side_effect=fake_connect):
            t = await open_transport(
                mode="direct",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
            )
        assert isinstance(t, DirectTransport)

    @pytest.mark.asyncio
    async def test_dispatches_to_wps(self):
        fake_ws = _FakeWS()
        captured = {}

        async def fake_connect(url, **kwargs):
            captured["url"] = url
            captured["subprotocols"] = kwargs.get("subprotocols")
            return fake_ws

        async def fake_request_token(api_base, device_id, api_key, **kw):
            captured["api_base"] = api_base
            captured["device_id"] = device_id
            captured["api_key"] = api_key
            return ("wss://wps.example.com/client/hubs/devices?access_token=xyz", "xyz")

        with patch.object(transport_mod.websockets, "connect", side_effect=fake_connect), \
             patch.object(transport_mod, "_request_connect_token", side_effect=fake_request_token):
            t = await open_transport(
                mode="wps",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
                api_key="k_abc123",
            )
        assert isinstance(t, WPSTransport)
        assert captured["api_base"] == "https://cms.example.com"
        assert captured["device_id"] == "pi-01"
        assert captured["api_key"] == "k_abc123"
        assert captured["subprotocols"] == ["json.webpubsub.azure.v1"]
        assert "access_token=xyz" in captured["url"]

    @pytest.mark.asyncio
    async def test_wps_appends_token_if_missing(self):
        fake_ws = _FakeWS()
        captured = {}

        async def fake_connect(url, **kwargs):
            captured["url"] = url
            return fake_ws

        async def fake_request_token(api_base, device_id, api_key, **kw):
            # URL lacks access_token; returned separately
            return ("wss://wps.example.com/client/hubs/devices", "t_sep")

        with patch.object(transport_mod.websockets, "connect", side_effect=fake_connect), \
             patch.object(transport_mod, "_request_connect_token", side_effect=fake_request_token):
            await open_transport(
                mode="wps",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
                api_key="k",
            )
        assert captured["url"].endswith("?access_token=t_sep")

    @pytest.mark.asyncio
    async def test_wps_requires_api_key(self):
        with pytest.raises(TransportError, match="requires a device_api_key"):
            await open_transport(
                mode="wps",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
                api_key="",
            )

    @pytest.mark.asyncio
    async def test_rejects_unknown_mode(self):
        with pytest.raises(TransportError, match="Unknown transport mode"):
            await open_transport(
                mode="quantum",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
            )

    @pytest.mark.asyncio
    async def test_defaults_to_direct(self):
        fake_ws = _FakeWS()

        async def fake_connect(url, **kwargs):
            return fake_ws

        with patch.object(transport_mod.websockets, "connect", side_effect=fake_connect):
            t = await open_transport(
                mode="",
                cms_url="wss://cms.example.com/ws/device",
                device_id="pi-01",
            )
        assert isinstance(t, DirectTransport)
