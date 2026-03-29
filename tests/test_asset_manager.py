"""Tests for the device-side AssetManager."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from cms_client.asset_manager import AssetManager


@pytest.fixture
def manager(tmp_path):
    """Create an AssetManager with a temp directory and fixed budget."""
    manifest = tmp_path / "state" / "assets.json"
    manifest.parent.mkdir(parents=True)
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    (assets_dir / "videos").mkdir()
    (assets_dir / "images").mkdir()
    (assets_dir / "splash").mkdir()
    # Use a fixed budget so tests don't depend on host disk size
    return AssetManager(manifest_path=manifest, assets_dir=assets_dir, budget_mb=100)


class TestAssetManagerBasics:
    def test_empty_manifest(self, manager):
        assert manager.get_all() == {}
        assert manager.total_size_bytes == 0

    def test_register_and_has_asset(self, manager):
        manager.register("video.mp4", "videos/video.mp4", 1000, "abc123")
        assert manager.has_asset("video.mp4") is True
        assert manager.has_asset("video.mp4", "abc123") is True
        assert manager.has_asset("video.mp4", "wrong") is False
        assert manager.has_asset("missing.mp4") is False

    def test_register_updates_total(self, manager):
        manager.register("a.mp4", "videos/a.mp4", 5000, "aaa")
        assert manager.total_size_bytes == 5000
        manager.register("b.mp4", "videos/b.mp4", 3000, "bbb")
        assert manager.total_size_bytes == 8000

    def test_remove(self, manager):
        # Create a real file to remove
        file_path = manager.assets_dir / "videos" / "remove-me.mp4"
        file_path.write_bytes(b"x" * 100)
        manager.register("remove-me.mp4", "videos/remove-me.mp4", 100, "rm")
        assert manager.has_asset("remove-me.mp4") is True

        manager.remove("remove-me.mp4")
        assert manager.has_asset("remove-me.mp4") is False
        assert not file_path.exists()

    def test_remove_nonexistent_is_safe(self, manager):
        manager.remove("no-such-file.mp4")  # should not raise

    def test_touch_updates_last_used(self, manager):
        manager.register("video.mp4", "videos/video.mp4", 1000, "abc")
        ts1 = manager.get_all()["video.mp4"]["last_used"]

        import time
        time.sleep(0.01)
        manager.touch("video.mp4")
        ts2 = manager.get_all()["video.mp4"]["last_used"]
        assert ts2 >= ts1

    def test_touch_nonexistent_is_safe(self, manager):
        manager.touch("no-such-file.mp4")  # should not raise


class TestBudget:
    def test_fixed_budget(self, manager):
        assert manager.budget_mb == 100
        assert manager.budget_bytes == 100 * 1024 * 1024

    def test_available_bytes(self, manager):
        assert manager.available_bytes == 100 * 1024 * 1024
        manager.register("a.mp4", "videos/a.mp4", 10 * 1024 * 1024, "a")
        assert manager.available_bytes == 90 * 1024 * 1024

    def test_zero_budget_uses_partition(self, tmp_path):
        """budget_mb=0 means use 80% of partition."""
        manifest = tmp_path / "state" / "assets.json"
        manifest.parent.mkdir(parents=True)
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        mgr = AssetManager(manifest_path=manifest, assets_dir=assets_dir, budget_mb=0)
        # Should return something > 0 based on actual disk
        assert mgr.budget_mb > 0

    def test_budget_capped_at_partition(self, tmp_path):
        """If configured budget exceeds 80% of partition, use partition limit."""
        manifest = tmp_path / "state" / "assets.json"
        manifest.parent.mkdir(parents=True)
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        # Set absurdly high budget
        mgr = AssetManager(manifest_path=manifest, assets_dir=assets_dir, budget_mb=999999999)
        # Should be capped at 80% of partition
        assert mgr.budget_mb < 999999999


class TestEviction:
    def _fill_manager(self, manager, items):
        """Register multiple assets with sequential timestamps."""
        import time
        for name, size_mb in items:
            path = f"videos/{name}"
            (manager.assets_dir / "videos" / name).write_bytes(b"x" * 100)
            manager.register(name, path, size_mb * 1024 * 1024, f"hash-{name}")
            time.sleep(0.01)  # ensure different last_used timestamps

    def test_evict_not_needed(self, manager):
        """No eviction needed if space is available."""
        manager.register("small.mp4", "videos/small.mp4", 1024, "sm")
        result = manager.evict_for(1024, set(), None)
        assert result is True

    def test_evict_unprotected_first(self, manager):
        """Tier 1: evicts unscheduled assets before scheduled ones."""
        self._fill_manager(manager, [
            ("old.mp4", 30),
            ("scheduled.mp4", 30),
            ("newer.mp4", 30),
        ])

        # Need 20 MB, only 10 MB free (100 - 90 = 10)
        scheduled = {"scheduled.mp4"}
        result = manager.evict_for(20 * 1024 * 1024, scheduled, None)
        assert result is True
        # old.mp4 (unprotected, oldest) should be evicted first
        assert not manager.has_asset("old.mp4")
        # scheduled.mp4 should still be there
        assert manager.has_asset("scheduled.mp4")

    def test_evict_lru_order(self, manager):
        """Unprotected assets evicted in LRU order (oldest first)."""
        self._fill_manager(manager, [
            ("oldest.mp4", 30),
            ("middle.mp4", 30),
            ("newest.mp4", 30),
        ])

        # Need 50 MB, only 10 free — need to evict 40 MB
        result = manager.evict_for(50 * 1024 * 1024, set(), None)
        assert result is True
        # oldest and middle should be gone (60 MB freed)
        assert not manager.has_asset("oldest.mp4")
        assert not manager.has_asset("middle.mp4")
        assert manager.has_asset("newest.mp4")

    def test_evict_tier2_protected(self, manager):
        """Tier 2: evicts scheduled assets if unprotected aren't enough."""
        self._fill_manager(manager, [
            ("sched1.mp4", 40),
            ("sched2.mp4", 40),
        ])

        # Need 30 MB, 20 free. All are scheduled, so tier 2 kicks in.
        result = manager.evict_for(30 * 1024 * 1024, {"sched1.mp4", "sched2.mp4"}, None)
        assert result is True
        # At least one should be evicted
        evicted = not manager.has_asset("sched1.mp4") or not manager.has_asset("sched2.mp4")
        assert evicted

    def test_evict_default_asset_protected(self, manager):
        """Default asset is treated as protected."""
        self._fill_manager(manager, [
            ("default.png", 30),
            ("regular.mp4", 30),
        ])

        # Need 50 MB, 40 free. Need to evict 10 MB.
        result = manager.evict_for(50 * 1024 * 1024, set(), "default.png")
        assert result is True
        # regular.mp4 (unprotected) should be evicted first
        assert not manager.has_asset("regular.mp4")
        assert manager.has_asset("default.png")

    def test_cannot_fit_exceeds_total_budget(self, manager):
        """Asset larger than total budget returns False."""
        result = manager.evict_for(200 * 1024 * 1024, set(), None)
        assert result is False

    def test_evict_makes_exact_room(self, manager):
        """Eviction stops once enough room is freed."""
        self._fill_manager(manager, [
            ("a.mp4", 25),
            ("b.mp4", 25),
            ("c.mp4", 25),
        ])

        # Need 30 MB, 25 free. Evicting a.mp4 (25 MB, oldest) gives 50 free.
        result = manager.evict_for(30 * 1024 * 1024, set(), None)
        assert result is True
        assert not manager.has_asset("a.mp4")
        assert manager.has_asset("b.mp4")
        assert manager.has_asset("c.mp4")

    def test_evict_everything_still_not_enough(self, manager):
        """If all evicted but still not enough (asset fits in budget but was edge case)."""
        self._fill_manager(manager, [
            ("a.mp4", 45),
            ("b.mp4", 45),
        ])

        # Need 95 MB. Budget is 100. Currently using 90 = 10 free.
        # Evict both (90 MB freed) → 100 free, 95 needed. Should succeed.
        result = manager.evict_for(95 * 1024 * 1024, set(), None)
        assert result is True


class TestPersistence:
    def test_manifest_persists_to_disk(self, manager):
        manager.register("video.mp4", "videos/video.mp4", 1000, "abc")

        # Create new manager from same manifest
        mgr2 = AssetManager(
            manifest_path=manager.manifest_path,
            assets_dir=manager.assets_dir,
            budget_mb=100,
        )
        assert mgr2.has_asset("video.mp4") is True
        assert mgr2.total_size_bytes == 1000

    def test_manifest_survives_remove(self, manager):
        file_path = manager.assets_dir / "videos" / "del.mp4"
        file_path.write_bytes(b"x" * 50)
        manager.register("del.mp4", "videos/del.mp4", 50, "del")
        manager.remove("del.mp4")

        mgr2 = AssetManager(
            manifest_path=manager.manifest_path,
            assets_dir=manager.assets_dir,
            budget_mb=100,
        )
        assert mgr2.has_asset("del.mp4") is False


class TestRebuildFromDisk:
    def test_rebuild_discovers_untracked_files(self, manager):
        """Files on disk not in manifest get added during rebuild."""
        video = manager.assets_dir / "videos" / "found.mp4"
        video.write_bytes(b"hello video")

        image = manager.assets_dir / "images" / "found.jpg"
        image.write_bytes(b"hello image")

        manager.rebuild_from_disk(
            manager.assets_dir / "videos",
            manager.assets_dir / "images",
            manager.assets_dir / "splash",
        )

        assert manager.has_asset("found.mp4") is True
        assert manager.has_asset("found.jpg") is True
        assert manager.total_size_bytes == len(b"hello video") + len(b"hello image")

    def test_rebuild_preserves_existing(self, manager):
        """Already-tracked assets are not overwritten during rebuild."""
        manager.register("existing.mp4", "videos/existing.mp4", 999, "original-hash")

        video = manager.assets_dir / "videos" / "existing.mp4"
        video.write_bytes(b"different content")

        manager.rebuild_from_disk(
            manager.assets_dir / "videos",
            manager.assets_dir / "images",
            manager.assets_dir / "splash",
        )

        # Should keep the original registration, not overwrite
        assert manager.get_all()["existing.mp4"]["checksum"] == "original-hash"

    def test_rebuild_skips_tmp_files(self, manager):
        """Temp files (.tmp suffix) should be ignored."""
        tmp_file = manager.assets_dir / "videos" / "partial.mp4.tmp"
        tmp_file.write_bytes(b"incomplete")

        manager.rebuild_from_disk(
            manager.assets_dir / "videos",
            manager.assets_dir / "images",
            manager.assets_dir / "splash",
        )

        assert manager.has_asset("partial.mp4.tmp") is False
