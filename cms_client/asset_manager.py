"""Asset manager — tracks assets on disk, handles budget and LRU eviction."""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from shared.state import atomic_write

logger = logging.getLogger("agora.asset_manager")


class AssetManager:
    """Manages a local asset manifest with budget-aware LRU eviction."""

    def __init__(self, manifest_path: Path, assets_dir: Path, budget_mb: int = 0):
        self.manifest_path = manifest_path
        self.assets_dir = assets_dir
        self._budget_mb = budget_mb
        self._manifest: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load manifest from disk."""
        try:
            data = json.loads(self.manifest_path.read_text())
            self._manifest = data.get("assets", {})
        except (FileNotFoundError, json.JSONDecodeError):
            self._manifest = {}

    def _save(self) -> None:
        """Persist manifest to disk atomically."""
        data = {
            "budget_mb": self.budget_mb,
            "assets": self._manifest,
        }
        atomic_write(self.manifest_path, json.dumps(data, indent=2))

    @property
    def budget_mb(self) -> int:
        """Effective budget: configured value or 80% of partition."""
        if self._budget_mb > 0:
            cap = self._partition_80pct_mb()
            return min(self._budget_mb, cap)
        return self._partition_80pct_mb()

    @property
    def budget_bytes(self) -> int:
        return self.budget_mb * 1024 * 1024

    @property
    def total_size_bytes(self) -> int:
        return sum(e.get("size_bytes", 0) for e in self._manifest.values())

    @property
    def available_bytes(self) -> int:
        return max(0, self.budget_bytes - self.total_size_bytes)

    def _partition_80pct_mb(self) -> int:
        try:
            stat = shutil.disk_usage(self.assets_dir)
            return int(stat.total * 0.8 / (1024 * 1024))
        except OSError:
            return 2000  # fallback 2 GB

    def has_asset(self, name: str, checksum: str | None = None) -> bool:
        """Check if an asset exists in the manifest (and optionally matches checksum)."""
        entry = self._manifest.get(name)
        if not entry:
            return False
        if checksum and entry.get("checksum") != checksum:
            return False
        return True

    def get(self, name: str) -> dict | None:
        """Return a copy of the manifest entry for `name`, or None."""
        entry = self._manifest.get(name)
        return dict(entry) if entry else None

    def touch(self, name: str) -> None:
        """Update last_used timestamp for an asset."""
        if name in self._manifest:
            self._manifest[name]["last_used"] = datetime.now(timezone.utc).isoformat()
            self._save()

    def register(self, name: str, path: str, size_bytes: int, checksum: str) -> None:
        """Add or update an asset in the manifest."""
        self._manifest[name] = {
            "path": path,
            "size_bytes": size_bytes,
            "checksum": checksum,
            "last_used": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def remove(self, name: str) -> None:
        """Remove an asset from the manifest and delete the file."""
        entry = self._manifest.pop(name, None)
        if entry:
            file_path = self.assets_dir / entry["path"]
            file_path.unlink(missing_ok=True)
            self._save()
            logger.info("Evicted asset: %s (%d bytes)", name, entry.get("size_bytes", 0))

    def get_all(self) -> dict[str, dict]:
        """Return all tracked assets."""
        return dict(self._manifest)

    def evict_for(
        self,
        needed_bytes: int,
        scheduled_assets: set[str],
        default_asset: str | None,
    ) -> bool:
        """Evict assets to make room for `needed_bytes`.

        Two-tier LRU:
          1. Evict unscheduled assets first (LRU order)
          2. If still not enough, evict scheduled assets (LRU order)

        Returns True if enough space was freed, False if the asset simply cannot fit.
        """
        if self.available_bytes >= needed_bytes:
            return True

        # Cannot ever fit even with empty budget
        if needed_bytes > self.budget_bytes:
            return False

        protected = scheduled_assets | ({default_asset} if default_asset else set())

        # Tier 1: evict unprotected assets (LRU)
        unprotected = [
            (name, entry) for name, entry in self._manifest.items()
            if name not in protected
        ]
        unprotected.sort(key=lambda x: x[1].get("last_used", ""))

        for name, entry in unprotected:
            if self.available_bytes >= needed_bytes:
                return True
            self.remove(name)

        # Tier 2: evict protected assets (LRU) — must make room
        remaining = [
            (name, entry) for name, entry in self._manifest.items()
        ]
        remaining.sort(key=lambda x: x[1].get("last_used", ""))

        for name, entry in remaining:
            if self.available_bytes >= needed_bytes:
                return True
            self.remove(name)

        return self.available_bytes >= needed_bytes

    def rebuild_from_disk(self, videos_dir: Path, images_dir: Path, splash_dir: Path) -> None:
        """Scan asset directories and rebuild manifest for files not already tracked."""
        import hashlib

        # Prune entries for files that no longer exist on disk
        stale = [name for name, info in self._manifest.items()
                 if not (self.assets_dir / info["path"]).exists()]
        for name in stale:
            del self._manifest[name]
            logger.info("Pruned stale manifest entry: %s", name)

        for dir_path in [videos_dir, images_dir, splash_dir]:
            if not dir_path.exists():
                continue
            for file_path in dir_path.iterdir():
                if file_path.is_file() and not file_path.name.endswith(".tmp"):
                    name = file_path.name
                    if name not in self._manifest:
                        sha256 = hashlib.sha256()
                        with open(file_path, "rb") as f:
                            while chunk := f.read(65536):
                                sha256.update(chunk)
                        rel_path = str(file_path.relative_to(self.assets_dir))
                        self._manifest[name] = {
                            "path": rel_path,
                            "size_bytes": file_path.stat().st_size,
                            "checksum": sha256.hexdigest(),
                            "last_used": datetime.now(timezone.utc).isoformat(),
                        }

        self._save()
        logger.info("Manifest rebuilt: %d assets tracked (%d MB used / %d MB budget)",
                     len(self._manifest), self.total_size_bytes // (1024 * 1024), self.budget_mb)
