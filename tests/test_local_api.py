"""Tests for local API disable middleware and CMS client helpers."""

import pytest
import pytest_asyncio
from pathlib import Path

from cms_client.service import _is_local_api_enabled, _safe_unlink


class TestIsLocalApiEnabled:
    def test_returns_true_when_no_flag_file(self, tmp_path):
        assert _is_local_api_enabled(tmp_path) is True

    def test_returns_true_when_flag_is_true(self, tmp_path):
        (tmp_path / "local_api_enabled").write_text("true")
        assert _is_local_api_enabled(tmp_path) is True

    def test_returns_false_when_flag_is_false(self, tmp_path):
        (tmp_path / "local_api_enabled").write_text("false")
        assert _is_local_api_enabled(tmp_path) is False

    def test_case_insensitive(self, tmp_path):
        (tmp_path / "local_api_enabled").write_text("False")
        assert _is_local_api_enabled(tmp_path) is False

    def test_whitespace_handling(self, tmp_path):
        (tmp_path / "local_api_enabled").write_text("  false\n")
        assert _is_local_api_enabled(tmp_path) is False


class TestSafeUnlink:
    def test_deletes_existing_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("data")
        _safe_unlink(f)
        assert not f.exists()

    def test_missing_file_no_error(self, tmp_path):
        _safe_unlink(tmp_path / "nonexistent.txt")


@pytest.mark.asyncio
class TestLocalApiMiddleware:
    async def test_api_blocked_when_disabled(self, settings, tmp_path):
        """API requests should return 403 when local_api_enabled flag is false."""
        from httpx import ASGITransport, AsyncClient
        from api.main import app

        settings.ensure_dirs()
        app.state.settings = settings

        # Write the flag file
        flag_path = settings.persist_dir / "local_api_enabled"
        flag_path.write_text("false")

        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"X-API-Key": "test-api-key"},
        ) as ac:
            resp = await ac.get("/api/v1/status")
            assert resp.status_code == 403
            assert "disabled" in resp.json()["detail"].lower()

    async def test_api_allowed_when_enabled(self, client):
        """API requests should work normally when no flag file or flag is true."""
        resp = await client.get("/api/v1/status")
        assert resp.status_code == 200

    async def test_api_allowed_when_no_flag(self, client):
        """Default state (no flag file) should allow API requests."""
        resp = await client.get("/api/v1/status")
        assert resp.status_code == 200

    async def test_non_api_routes_not_blocked(self, settings, tmp_path):
        """Non-API routes (web UI) should not be blocked."""
        from httpx import ASGITransport, AsyncClient
        from api.main import app

        settings.ensure_dirs()
        app.state.settings = settings

        flag_path = settings.persist_dir / "local_api_enabled"
        flag_path.write_text("false")

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            # Login page should still work
            resp = await ac.get("/login")
            # Should not be 403 — either 200 or redirect is fine
            assert resp.status_code != 403
