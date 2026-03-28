"""Shared test fixtures for Agora device tests."""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.config import Settings


@pytest.fixture
def settings(tmp_path):
    """Create Settings with temp directories."""
    return Settings(
        agora_base=tmp_path,
        api_key="test-api-key",
        web_username="admin",
        web_password="testpass",
        secret_key="test-secret",
        device_name="test-node",
    )


@pytest.fixture
def configured_app(settings):
    """Create a FastAPI app with test settings."""
    from api.main import app

    settings.ensure_dirs()
    app.state.settings = settings
    return app


@pytest_asyncio.fixture
async def client(configured_app):
    """Authenticated async HTTP client (API key auth)."""
    transport = ASGITransport(app=configured_app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-API-Key": "test-api-key"},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def web_client(configured_app):
    """Web client with session cookie auth."""
    transport = ASGITransport(app=configured_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post("/login", data={"username": "admin", "password": "testpass"}, follow_redirects=False)
        yield ac


@pytest_asyncio.fixture
async def unauthed_client(configured_app):
    """Unauthenticated async HTTP client."""
    transport = ASGITransport(app=configured_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
