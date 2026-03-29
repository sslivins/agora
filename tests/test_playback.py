"""Tests for playback control API endpoints."""

import io

import pytest

from shared.models import DesiredState, PlaybackMode
from shared.state import read_state


@pytest.mark.asyncio
class TestPlayback:
    async def _upload_asset(self, client):
        """Helper: upload a test video asset."""
        await client.post(
            "/api/v1/assets/upload",
            files={"file": ("test.mp4", io.BytesIO(b"fakevideo"), "video/mp4")},
        )

    async def test_play(self, client, settings):
        await self._upload_asset(client)

        resp = await client.post("/api/v1/play", json={"asset": "test.mp4"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["desired"]["mode"] == "play"
        assert data["desired"]["asset"] == "test.mp4"

        # Verify state file written
        state = read_state(settings.desired_state_path, DesiredState)
        assert state.mode == PlaybackMode.PLAY
        assert state.asset == "test.mp4"

    async def test_play_with_loop(self, client, settings):
        await self._upload_asset(client)

        resp = await client.post("/api/v1/play", json={"asset": "test.mp4", "loop": True})
        assert resp.status_code == 200

        state = read_state(settings.desired_state_path, DesiredState)
        assert state.loop is True

    async def test_play_nonexistent_asset(self, client):
        resp = await client.post("/api/v1/play", json={"asset": "nonexistent.mp4"})
        assert resp.status_code == 404

    async def test_stop(self, client, settings):
        resp = await client.post("/api/v1/stop")
        assert resp.status_code == 200
        assert resp.json()["desired"]["mode"] == "stop"

        state = read_state(settings.desired_state_path, DesiredState)
        assert state.mode == PlaybackMode.STOP

    async def test_splash(self, client, settings):
        resp = await client.post("/api/v1/splash")
        assert resp.status_code == 200
        assert resp.json()["desired"]["mode"] == "splash"

        state = read_state(settings.desired_state_path, DesiredState)
        assert state.mode == PlaybackMode.SPLASH

    async def test_play_requires_auth(self, unauthed_client):
        resp = await unauthed_client.post("/api/v1/play", json={"asset": "x.mp4"})
        assert resp.status_code == 401

    async def test_stop_requires_auth(self, unauthed_client):
        resp = await unauthed_client.post("/api/v1/stop")
        assert resp.status_code == 401


@pytest.mark.asyncio
class TestStatus:
    async def test_health(self, client):
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["device_name"] == "test-node"
        assert "uptime_seconds" in data

    async def test_status(self, client):
        resp = await client.get("/api/v1/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["device_name"] == "test-node"
        assert "current_state" in data
        assert "desired_state" in data
        assert "asset_count" in data

    async def test_status_requires_auth(self, unauthed_client):
        resp = await unauthed_client.get("/api/v1/status")
        assert resp.status_code == 401
