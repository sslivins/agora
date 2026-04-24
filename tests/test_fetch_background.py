"""Tests for agora#136: fetch_asset runs in background, not on the WS read path.

The CMS websocket read loop must stay responsive while large asset downloads
are in flight. Fetches are:

* dispatched as background tasks (``_spawn_fetch_asset``),
* serialized by ``_fetch_lock`` so AssetManager eviction math is correct,
* superseded when a new fetch for the same asset arrives,
* cancelled when ``delete_asset``/``wipe_assets`` races them, and
* cancelled on WS disconnect.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from cms_client.service import CMSClient  # noqa: E402


@pytest.fixture
def client(tmp_path):
    settings = MagicMock()
    settings.assets_dir = tmp_path
    settings.manifest_path = tmp_path / "assets.json"
    settings.asset_budget_mb = 100
    with patch.object(CMSClient, "__init__", lambda self, s: None):
        c = CMSClient(settings)
    c.settings = settings
    c.device_id = "d1"
    c.asset_manager = MagicMock()
    c._fetch_tasks = {}
    c._fetch_lock = asyncio.Lock()
    return c


@pytest.mark.asyncio
async def test_spawn_fetch_runs_in_background(client):
    """Spawning does not block the caller even on a slow fetch."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_fetch(msg, ws):
        started.set()
        await release.wait()

    with patch.object(client, "_handle_fetch_asset", side_effect=slow_fetch):
        client._spawn_fetch_asset({"asset_name": "a.mp4"}, ws=AsyncMock())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        # Caller returned immediately; task is running.
        assert "a.mp4" in client._fetch_tasks
        assert not client._fetch_tasks["a.mp4"].done()
        release.set()
        await client._fetch_tasks["a.mp4"] if "a.mp4" in client._fetch_tasks else None
    # Cleanup callback pops when the task finishes.
    await asyncio.sleep(0)
    assert "a.mp4" not in client._fetch_tasks


@pytest.mark.asyncio
async def test_spawn_with_missing_asset_name_is_noop(client):
    client._spawn_fetch_asset({}, ws=AsyncMock())
    assert client._fetch_tasks == {}


@pytest.mark.asyncio
async def test_duplicate_fetch_supersedes(client):
    """A second fetch for the same asset cancels the first."""
    first_cancelled = asyncio.Event()
    first_started = asyncio.Event()
    calls = []

    async def fake_fetch(msg, ws):
        calls.append(msg.get("tag"))
        if msg.get("tag") == "first":
            first_started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                first_cancelled.set()
                raise

    with patch.object(client, "_handle_fetch_asset", side_effect=fake_fetch):
        client._spawn_fetch_asset({"asset_name": "a.mp4", "tag": "first"}, ws=AsyncMock())
        await asyncio.wait_for(first_started.wait(), timeout=1.0)
        client._spawn_fetch_asset({"asset_name": "a.mp4", "tag": "second"}, ws=AsyncMock())
        await asyncio.wait_for(first_cancelled.wait(), timeout=1.0)
        # Second task should be the one we track now.
        t = client._fetch_tasks.get("a.mp4")
        assert t is not None
        # Let the second run.
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_fetches_are_serialized_by_lock(client):
    """Two different-asset fetches execute one at a time under ``_fetch_lock``."""
    active = 0
    peak = 0
    gate = asyncio.Event()

    async def fake_fetch(msg, ws):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await gate.wait()
        active -= 1

    with patch.object(client, "_handle_fetch_asset", side_effect=fake_fetch):
        client._spawn_fetch_asset({"asset_name": "a.mp4"}, ws=AsyncMock())
        client._spawn_fetch_asset({"asset_name": "b.mp4"}, ws=AsyncMock())
        await asyncio.sleep(0.05)
        gate.set()
        for t in list(client._fetch_tasks.values()):
            await asyncio.wait_for(t, timeout=1.0)
    assert peak == 1


@pytest.mark.asyncio
async def test_cancel_fetch_by_name_cancels_in_flight(client):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_fetch(msg, ws):
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with patch.object(client, "_handle_fetch_asset", side_effect=slow_fetch):
        client._spawn_fetch_asset({"asset_name": "a.mp4"}, ws=AsyncMock())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await client._cancel_fetch("a.mp4")
        assert cancelled.is_set()
        assert "a.mp4" not in client._fetch_tasks


@pytest.mark.asyncio
async def test_cancel_fetch_unknown_name_is_noop(client):
    await client._cancel_fetch("")
    await client._cancel_fetch("never-started")  # no crash


@pytest.mark.asyncio
async def test_cancel_all_fetches(client):
    cancelled = {"a.mp4": False, "b.mp4": False}
    started = {"a.mp4": asyncio.Event(), "b.mp4": asyncio.Event()}

    async def slow_fetch(msg, ws):
        name = msg["asset_name"]
        started[name].set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled[name] = True
            raise

    with patch.object(client, "_handle_fetch_asset", side_effect=slow_fetch):
        client._spawn_fetch_asset({"asset_name": "a.mp4"}, ws=AsyncMock())
        client._spawn_fetch_asset({"asset_name": "b.mp4"}, ws=AsyncMock())
        await asyncio.wait_for(started["a.mp4"].wait(), timeout=1.0)
        # b is queued behind the lock, but _cancel_all still cancels it.
        await client._cancel_all_fetches()
    assert cancelled["a.mp4"]
    assert client._fetch_tasks == {}
