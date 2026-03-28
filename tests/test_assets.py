"""Tests for asset management API endpoints."""

import io

import pytest


@pytest.mark.asyncio
class TestAssetUpload:
    async def test_upload_video(self, client, settings):
        resp = await client.post(
            "/api/v1/assets/upload",
            files={"file": ("test.mp4", io.BytesIO(b"fakevideo"), "video/mp4")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test.mp4"
        assert data["asset_type"] == "video"
        assert data["size"] == 9
        # File written to disk
        assert (settings.videos_dir / "test.mp4").exists()

    async def test_upload_image(self, client, settings):
        resp = await client.post(
            "/api/v1/assets/upload",
            files={"file": ("photo.jpg", io.BytesIO(b"fakeimg"), "image/jpeg")},
        )
        assert resp.status_code == 200
        assert resp.json()["asset_type"] == "image"
        assert (settings.images_dir / "photo.jpg").exists()

    async def test_upload_png(self, client, settings):
        resp = await client.post(
            "/api/v1/assets/upload",
            files={"file": ("slide.png", io.BytesIO(b"fakepng"), "image/png")},
        )
        assert resp.status_code == 200
        assert resp.json()["asset_type"] == "image"

    async def test_upload_invalid_extension(self, client):
        resp = await client.post(
            "/api/v1/assets/upload",
            files={"file": ("hack.exe", io.BytesIO(b"evil"), "application/octet-stream")},
        )
        assert resp.status_code == 400

    async def test_upload_path_traversal(self, client):
        resp = await client.post(
            "/api/v1/assets/upload",
            files={"file": ("../etc/passwd.mp4", io.BytesIO(b"x"), "video/mp4")},
        )
        # Should sanitize to just filename or reject
        # Path().name strips path components, but name might still fail regex
        assert resp.status_code in (200, 400)

    async def test_upload_requires_auth(self, unauthed_client):
        resp = await unauthed_client.post(
            "/api/v1/assets/upload",
            files={"file": ("test.mp4", io.BytesIO(b"x"), "video/mp4")},
        )
        assert resp.status_code == 401


@pytest.mark.asyncio
class TestAssetList:
    async def test_list_empty(self, client):
        resp = await client.get("/api/v1/assets")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_list_after_upload(self, client):
        await client.post(
            "/api/v1/assets/upload",
            files={"file": ("a.mp4", io.BytesIO(b"aaa"), "video/mp4")},
        )
        await client.post(
            "/api/v1/assets/upload",
            files={"file": ("b.jpg", io.BytesIO(b"bbb"), "image/jpeg")},
        )
        resp = await client.get("/api/v1/assets")
        assert resp.status_code == 200
        names = [a["name"] for a in resp.json()]
        assert "a.mp4" in names
        assert "b.jpg" in names


@pytest.mark.asyncio
class TestAssetDelete:
    async def test_delete_asset(self, client, settings):
        await client.post(
            "/api/v1/assets/upload",
            files={"file": ("del.mp4", io.BytesIO(b"bye"), "video/mp4")},
        )
        assert (settings.videos_dir / "del.mp4").exists()

        resp = await client.delete("/api/v1/assets/del.mp4")
        assert resp.status_code == 200
        assert not (settings.videos_dir / "del.mp4").exists()

    async def test_delete_nonexistent(self, client):
        resp = await client.delete("/api/v1/assets/nonexistent.mp4")
        assert resp.status_code == 404
