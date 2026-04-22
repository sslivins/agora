"""Transport abstraction for CMS communication.

Two transports are supported:

* **DirectTransport** — a plain WebSocket connection to the CMS
  (``wss://host/ws/device``).  Payloads go over the wire unchanged
  as JSON text frames.  This is the original and default behaviour.

* **WPSTransport** — an Azure Web PubSub client.  The device first
  asks the CMS for a short-lived client access URL + access token
  via ``POST /api/devices/{device_id}/connect-token`` (authenticated
  with ``X-Device-API-Key``), then opens a WebSocket to the Azure
  endpoint using the ``json.webpubsub.azure.v1`` subprotocol.
  Outgoing application messages are wrapped in the WPS event
  envelope; incoming envelopes are unwrapped so the caller only
  sees the inner JSON payload.

Both transports expose a websocket-like interface — ``await t.send(str)``
accepts an already-JSON-serialized message string, and iterating
``async for raw in t`` yields JSON strings.  This lets the existing
handlers in ``service.py`` stay unchanged.

Select the transport with the ``AGORA_CMS_TRANSPORT`` environment
variable (``direct`` or ``wps``).  In WPS mode, the device API key
used to call the connect-token endpoint is sourced from the
``AGORA_DEVICE_API_KEY`` env var or, if unset, read from
``<persist_dir>/api_key`` — the same file CMS rotates into via the
config message and that direct-mode transport uses for asset
downloads.  This key is distinct from the per-device auth_token
minted over the register handshake (API key for bootstrap,
auth_token for subsequent WS register messages).
"""

from __future__ import annotations

import json
import logging
from typing import AsyncIterator, Optional
from urllib.parse import urlparse

import websockets

logger = logging.getLogger("agora.cms_client.transport")

WPS_SUBPROTOCOL = "json.webpubsub.azure.v1"


class TransportError(Exception):
    """Raised when transport setup or operation fails."""


class _Transport:
    """Common async-context-manager base for transports."""

    def __init__(self, ws) -> None:
        self._ws = ws
        self._closed = False

    async def send(self, data) -> None:  # pragma: no cover - interface only
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[str]:  # pragma: no cover
        raise NotImplementedError

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._ws.close()
        except Exception:
            logger.debug("Error closing underlying websocket", exc_info=True)

    async def __aenter__(self) -> "_Transport":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class DirectTransport(_Transport):
    """Plain WebSocket transport (unchanged behaviour from v1)."""

    async def send(self, data) -> None:
        await self._ws.send(data)

    async def __aiter__(self) -> AsyncIterator[str]:
        async for raw in self._ws:
            yield raw


class WPSTransport(_Transport):
    """Azure Web PubSub transport.

    Outgoing strings are parsed and re-wrapped as::

        {"type": "event", "event": "message",
         "dataType": "json", "data": <payload>}

    Incoming frames of type ``"message"`` have their ``data`` field
    re-serialized to a JSON string and yielded.  All other frame
    types (``system``, ``ack``, etc.) are logged and skipped so
    callers only see application messages.
    """

    async def send(self, data) -> None:
        if isinstance(data, (bytes, bytearray)):
            raise TransportError("WPS transport does not yet support binary frames")
        try:
            payload = json.loads(data)
        except (TypeError, json.JSONDecodeError) as e:
            raise TransportError(f"WPS transport.send expects a JSON string: {e}") from e
        envelope = {
            "type": "event",
            "event": "message",
            "dataType": "json",
            "data": payload,
        }
        await self._ws.send(json.dumps(envelope))

    async def __aiter__(self) -> AsyncIterator[str]:
        async for raw in self._ws:
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Received non-JSON WPS frame; dropping")
                continue
            ftype = frame.get("type")
            if ftype == "message":
                data = frame.get("data")
                if isinstance(data, dict):
                    yield json.dumps(data)
                elif isinstance(data, str):
                    try:
                        decoded = json.loads(data)
                    except json.JSONDecodeError:
                        logger.warning("WPS message.data was a non-JSON string; dropping")
                        continue
                    if isinstance(decoded, dict):
                        yield json.dumps(decoded)
                    else:
                        logger.warning("WPS message.data decoded to non-object; dropping")
                else:
                    logger.warning("WPS message.data was not an object; dropping")
            elif ftype in ("system", "ack"):
                logger.debug("WPS %s frame: %s", ftype, frame)
            else:
                logger.debug("WPS unknown frame type %r; dropping", ftype)


def _derive_api_base(cms_url: str) -> str:
    """Turn a ws(s):// URL into the http(s):// API base used for connect-token."""
    p = urlparse(cms_url)
    scheme = "https" if p.scheme in ("wss", "https") else "http"
    return f"{scheme}://{p.netloc}"


async def _request_connect_token(
    api_base: str,
    device_id: str,
    api_key: str,
    *,
    http_timeout: float = 10.0,
) -> tuple[str, str]:
    """Call POST /api/devices/{id}/connect-token and return (url, token)."""
    import aiohttp  # type: ignore

    url = f"{api_base.rstrip('/')}/api/devices/{device_id}/connect-token"
    headers = {"X-Device-API-Key": api_key, "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=http_timeout)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, headers=headers) as resp:
            if resp.status in (401, 403):
                raise TransportError(
                    f"connect-token rejected ({resp.status}) — check device_api_key"
                )
            if resp.status >= 400:
                body = await resp.text()
                raise TransportError(
                    f"connect-token failed: HTTP {resp.status} {body[:200]!r}"
                )
            data = await resp.json()
    wss_url = data.get("url") or ""
    access_token = data.get("access_token") or data.get("token") or ""
    if not wss_url:
        raise TransportError(f"connect-token response missing 'url': {data!r}")
    return wss_url, access_token


async def open_direct(cms_url: str) -> DirectTransport:
    """Open a plain websocket to ``cms_url`` and wrap it as a DirectTransport."""
    ws = await websockets.connect(
        cms_url,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    )
    return DirectTransport(ws)


async def open_wps(
    *,
    cms_url: str,
    device_id: str,
    api_key: str,
    api_base: Optional[str] = None,
) -> WPSTransport:
    """Bootstrap a WPS transport.

    Steps:
      1. Derive HTTP(S) API base from ``cms_url`` (or use override).
      2. POST /api/devices/{device_id}/connect-token with X-Device-API-Key.
      3. Open a websocket to the returned URL using the WPS subprotocol.
      4. Return a WPSTransport wrapping the socket.
    """
    if not api_key:
        raise TransportError("WPS transport requires a device_api_key")
    base = api_base or _derive_api_base(cms_url)
    wss_url, access_token = await _request_connect_token(base, device_id, api_key)
    if access_token and "access_token=" not in wss_url:
        joiner = "&" if "?" in wss_url else "?"
        wss_url = f"{wss_url}{joiner}access_token={access_token}"
    ws = await websockets.connect(
        wss_url,
        subprotocols=[WPS_SUBPROTOCOL],
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    )
    return WPSTransport(ws)


async def open_transport(
    *,
    mode: str,
    cms_url: str,
    device_id: str,
    api_key: str = "",
    api_base: Optional[str] = None,
) -> _Transport:
    """Factory: open the transport matching ``mode`` ("direct" or "wps")."""
    m = (mode or "direct").lower().strip()
    if m == "direct":
        return await open_direct(cms_url)
    if m == "wps":
        return await open_wps(
            cms_url=cms_url,
            device_id=device_id,
            api_key=api_key,
            api_base=api_base,
        )
    raise TransportError(f"Unknown transport mode {mode!r} (expected 'direct' or 'wps')")
