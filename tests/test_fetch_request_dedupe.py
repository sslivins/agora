"""Tests for de-duplication of outbound ``fetch_request`` messages.

A single CMS sync cycle used to emit the same ``fetch_request`` ~3x for one
still-missing asset: ``_handle_sync`` evaluates the schedule directly, then
wakes the eval loop to evaluate it again, and the proactive ``_fetch_loop``
runs on its own timer — three uncoordinated emitters, none tracking whether a
request was already outstanding. The CMS faithfully answered each with a
``fetch_asset`` (deduped device-side, but a wasted round-trip).

The device now suppresses a duplicate ``fetch_request`` while either a prior
request is awaiting its reply (``_inflight_fetches`` marker, bounded by
``FETCH_REQUEST_DEDUPE_TTL``) or a download for the asset is already active
(``_fetch_tasks``). The marker is cleared when the reply arrives (the download
task then owns suppression) and on send failure / reconnect so a genuinely lost
request still recovers.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cms_client.service import FETCH_REQUEST_DEDUPE_TTL, CMSClient


def _schedule_json(asset: str = "a.mp4", checksum: str = "c1") -> str:
    """A schedule with one entry that is active at any wall-clock time."""
    return json.dumps({
        "schedules": [{
            "asset": asset,
            "asset_checksum": checksum,
            "start_time": "00:00:00",
            "end_time": "23:59:59",
        }],
        "timezone": "UTC",
    })


@pytest.fixture
def client(tmp_path):
    settings = MagicMock()
    settings.schedule_path = tmp_path / "schedule.json"
    with patch.object(CMSClient, "__init__", lambda self, s: None):
        c = CMSClient(settings)
    c.settings = settings
    c.device_id = "d1"
    c.asset_manager = MagicMock()
    c.asset_manager.has_asset.return_value = False
    c._fetch_tasks = {}
    c._inflight_fetches = {}
    c._ws = AsyncMock()
    return c


async def _drain():
    # Let any fire-and-forget tasks scheduled via create_task run.
    for _ in range(5):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_two_fetch_cycles_send_single_request(client):
    """Two proactive fetch cycles for the same missing asset send one request."""
    client.settings.schedule_path.write_text(_schedule_json())

    await client._check_and_fetch_missing()
    await client._check_and_fetch_missing()

    assert client._ws.send.await_count == 1
    sent = json.loads(client._ws.send.await_args.args[0])
    assert sent["type"] == "fetch_request"
    assert sent["asset"] == "a.mp4"


@pytest.mark.asyncio
async def test_eval_request_suppresses_fetch_loop(client):
    """A request from schedule eval suppresses the proactive loop's duplicate."""
    client.settings.schedule_path.write_text(_schedule_json())

    client._request_asset_fetch("a.mp4")
    await _drain()
    assert client._ws.send.await_count == 1

    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 1


@pytest.mark.asyncio
async def test_active_download_suppresses_request(client):
    """No new request while a download task for the asset is in flight."""
    client.settings.schedule_path.write_text(_schedule_json())

    async def _never():
        await asyncio.Event().wait()

    task = asyncio.create_task(_never())
    client._fetch_tasks["a.mp4"] = task
    try:
        await client._check_and_fetch_missing()
        assert client._ws.send.await_count == 0
    finally:
        task.cancel()


@pytest.mark.asyncio
async def test_should_send_transitions(client):
    """First request allowed; marker then active task each suppress; clears recover."""
    assert client._should_send_fetch_request("a.mp4") is True
    # Marker now set → suppressed.
    assert client._should_send_fetch_request("a.mp4") is False

    # Reply received clears the marker (mirrors _spawn_fetch_asset).
    client._inflight_fetches.pop("a.mp4", None)

    task = asyncio.create_task(asyncio.sleep(10))
    client._fetch_tasks["a.mp4"] = task
    try:
        # Active download suppresses regardless of TTL.
        assert client._should_send_fetch_request("a.mp4") is False
    finally:
        task.cancel()


@pytest.mark.asyncio
async def test_retry_after_task_completes_and_still_missing(client):
    """Once the download task ends and the asset is still missing, re-request."""
    client.settings.schedule_path.write_text(_schedule_json())

    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 1

    # Reply arrived (marker cleared) and the download task has finished.
    client._inflight_fetches.pop("a.mp4", None)
    done = asyncio.create_task(asyncio.sleep(0))
    await done
    client._fetch_tasks["a.mp4"] = done

    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 2


@pytest.mark.asyncio
async def test_ttl_expiry_allows_resend(client):
    """A request with no reply is retried after the dedupe TTL elapses."""
    client.settings.schedule_path.write_text(_schedule_json())

    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 1

    # Age the in-flight marker past the safety-net TTL.
    client._inflight_fetches["a.mp4"] -= FETCH_REQUEST_DEDUPE_TTL + 1

    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 2


@pytest.mark.asyncio
async def test_send_failure_clears_marker(client):
    """A failed send drops the marker so the next cycle retries."""
    client.settings.schedule_path.write_text(_schedule_json())
    client._ws.send.side_effect = RuntimeError("boom")

    await client._check_and_fetch_missing()
    assert "a.mp4" not in client._inflight_fetches

    client._ws.send.side_effect = None
    await client._check_and_fetch_missing()
    assert client._ws.send.await_count == 2


@pytest.mark.asyncio
async def test_receipt_pops_inflight_marker(client):
    """Receiving a fetch_asset reply clears the outstanding-request marker."""
    client._inflight_fetches["a.mp4"] = 123.0
    # Heavy download machinery is irrelevant here; stub it out.
    with patch.object(client, "_fetch_asset_locked", new=AsyncMock()):
        client._spawn_fetch_asset({"asset_name": "a.mp4"}, ws=AsyncMock())
    assert "a.mp4" not in client._inflight_fetches
