"""Tests for API authentication."""

import pytest


@pytest.mark.asyncio
class TestAPIKeyAuth:
    async def test_valid_api_key(self, client):
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200

    async def test_invalid_api_key(self, configured_app):
        from httpx import ASGITransport, AsyncClient

        transport = ASGITransport(app=configured_app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"X-API-Key": "wrong-key"},
        ) as ac:
            resp = await ac.get("/api/v1/status")
            assert resp.status_code == 401

    async def test_no_auth(self, unauthed_client):
        resp = await unauthed_client.get("/api/v1/status")
        assert resp.status_code == 401

    async def test_health_no_auth_required(self, unauthed_client):
        """Health endpoint is public."""
        resp = await unauthed_client.get("/api/v1/health")
        assert resp.status_code == 200


@pytest.mark.asyncio
class TestWebAuth:
    async def test_login_success(self, unauthed_client):
        resp = await unauthed_client.post(
            "/login",
            data={"username": "admin", "password": "testpass"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "agora_session" in resp.cookies

    async def test_login_wrong_password(self, unauthed_client):
        resp = await unauthed_client.post(
            "/login",
            data={"username": "admin", "password": "wrong"},
            follow_redirects=False,
        )
        assert "agora_session" not in resp.cookies

    async def test_login_page_accessible(self, unauthed_client):
        resp = await unauthed_client.get("/login")
        assert resp.status_code == 200

    async def test_protected_web_page_redirects(self, unauthed_client):
        resp = await unauthed_client.get("/", follow_redirects=False)
        assert resp.status_code == 303
        assert "/login" in resp.headers["location"]

    async def test_logout(self, web_client):
        resp = await web_client.get("/logout", follow_redirects=False)
        assert resp.status_code == 303
