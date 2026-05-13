"""Slot staging + tryboot trigger — the ``Stager`` collaborator.

Implements steps 6–10 of ``docs/bundle-format.md`` §"On-device apply
flow":

1. Decompress + extract ``bundle.tar.zst`` into a working subdir of
   the staging directory.
2. Sanity-check the top-level entries (``boot/``, ``root/``,
   ``meta.json``).
3. Parse ``meta.json`` via :func:`bundle.parse_bundle_meta`.
4. Defense-in-depth: compare ``meta.version`` to
   ``payload.target_version``.
5. Verify the sha256 manifest via :func:`bundle.verify_bundle_manifest`.
6. Determine which slot is currently running via
   :func:`slot_mgr.slot_state` and flip the other one.
7. Resolve the target mountpoints for that slot's boot + root
   partitions.
8. ``rsync -aHAX --delete --numeric-ids`` the extracted ``boot/`` and
   ``root/`` trees onto the inactive slot's partitions.
9. Trigger tryboot via :func:`slot_mgr.trigger_tryboot` (which
   rewrites ``[tryboot] boot_partition`` in autoboot.txt, records
   ``last_tryboot_target``, and reboots).

Exposed as :class:`SlotStager`, implementing the ``Stager`` Protocol
from :mod:`os_updater.service`. The service awaits :meth:`stage`
between transitioning the FSM to ``TRYBOOT_PENDING`` and to
``TRYBOOT_RUNNING``.

Decompression is split into two subprocess calls (``zstd -d`` then
``tar -xf``) rather than a piped one-step so that a failure of either
half is unambiguously attributable to its source. The intermediate
``bundle.tar`` is unlinked on success. The plan budgets 2 GB of free
space on ``/data`` for staging; a v1.x bundle is ~1 GB compressed +
~3 GB extracted + ~1 GB intermediate tar, well within budget.

Error taxonomy (the service maps each to a distinct wire code):

* :class:`StagingError` — generic staging failure. Maps to
  ``failed:stage_failed``.
* :class:`RsyncError` — an ``rsync`` subprocess returned non-zero.
  Maps to ``failed:stage_rsync_failed``.
* :class:`TrybootError` — :func:`slot_mgr.trigger_tryboot` raised
  (e.g. pinned device, autoboot rewrite failed, reboot subprocess
  failed). Maps to ``failed:tryboot_failed``.
* :class:`BundleIntegrityError` (from :mod:`bundle`) — re-raised
  unchanged when decompression / extraction / version-mismatch /
  manifest verification fails. Maps to ``failed:bundle_invalid``.

The staging directory is **not** cleaned up on failure — forensics
trump disk reclaim, especially for ``bundle_invalid`` cases where the
contents are diagnostic. The service-level cleanup that runs on
``agora-os-updater.service`` start (24h TTL sweep, per Phase 2
deliverable §"Partial-download cleanup") catches everything
eventually.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from os_updater.bundle import (
    BundleIntegrityError,
    Runner,
    _default_runner,
    parse_bundle_meta,
    verify_bundle_manifest,
)
from os_updater.dispatch import DispatchPayload


logger = logging.getLogger(__name__)


# ── Defaults ───────────────────────────────────────────────────────────────


#: Filename of the signed artifact in the staging dir, matching
#: :data:`os_updater.verifier.DEFAULT_BUNDLE_FILENAME`.
DEFAULT_BUNDLE_FILENAME = "bundle.tar.zst"

#: Subdirectory under ``staging_dir`` for the extracted tree.
DEFAULT_UNPACKED_SUBDIR = "unpacked"

#: Intermediate tarball produced by ``zstd -d`` before ``tar -xf``
#: extracts it. Deleted on success.
DEFAULT_INTERMEDIATE_TAR_NAME = "bundle.tar"

#: Slot A's boot partition mountpoint. Phase 0 fstab.
DEFAULT_BOOT_MOUNT_SLOT_A = Path("/boot/firmware")

#: Slot B's boot partition mountpoint. Phase 0 fstab keeps slot B's
#: FAT32 partition mounted at the mirror path so slot_mgr can keep
#: ``autoboot.txt`` in sync across both copies.
DEFAULT_BOOT_MOUNT_SLOT_B = Path("/boot/firmware-b")

#: Where the inactive slot's root partition is mounted while the
#: running slot writes into it. The systemd unit that launches the
#: agora-os-updater daemon mounts this path before daemon start;
#: :class:`SlotStager` does not mount or unmount it.
DEFAULT_INACTIVE_ROOT_MOUNT = Path("/mnt/inactive-root")

#: zstd long-range window matching the builder side
#: (docs/bundle-format.md §"Compression": ``zstd -19 --long=27``).
#: ``--long=27`` requires the same flag on decompress to allocate the
#: 128 MB window.
DEFAULT_ZSTD_LONG = 27

#: Per-step subprocess timeouts. Generous defaults for the slowest
#: 32 GB SD cards in the field — actual times on a v1 bundle are
#: ~30 s decompress, ~60 s extract, ~3–5 min rsync per side.
DEFAULT_DECOMPRESS_TIMEOUT_S = 600.0
DEFAULT_EXTRACT_TIMEOUT_S = 900.0
DEFAULT_RSYNC_TIMEOUT_S = 1800.0


# ── Exceptions ─────────────────────────────────────────────────────────────


class StagingError(Exception):
    """Base class for slot-staging failures.

    Service maps to ``failed:stage_failed`` (see
    :meth:`os_updater.service.OSUpdaterService._classify_failure`).
    """


class RsyncError(StagingError):
    """``rsync`` returned non-zero while writing the inactive slot.

    Service maps to ``failed:stage_rsync_failed``.
    """


class TrybootError(StagingError):
    """:func:`slot_mgr.trigger_tryboot` raised while arming the reboot.

    Service maps to ``failed:tryboot_failed``.
    """


# ── Pure helpers (no I/O) ──────────────────────────────────────────────────


def other_slot(slot: int) -> int:
    """Flip a slot number (1↔2). Raises :class:`StagingError` on bad input."""
    if slot == 1:
        return 2
    if slot == 2:
        return 1
    raise StagingError(f"invalid slot number: {slot!r} (expected 1 or 2)")


def boot_mount_for_slot(
    slot: int,
    *,
    slot_a_mount: Path = DEFAULT_BOOT_MOUNT_SLOT_A,
    slot_b_mount: Path = DEFAULT_BOOT_MOUNT_SLOT_B,
) -> Path:
    """Return the mountpoint of ``slot``'s boot (FAT32) partition.

    Raises :class:`StagingError` on bad input.
    """
    if slot == 1:
        return slot_a_mount
    if slot == 2:
        return slot_b_mount
    raise StagingError(f"invalid slot number: {slot!r} (expected 1 or 2)")


# ── Subprocess wrappers ────────────────────────────────────────────────────


def _tail(text: Optional[str], limit: int = 2000) -> str:
    """Truncate ``text`` for inclusion in log lines / exception detail."""
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return "…" + text[-limit:]


def decompress_and_extract(
    bundle_path: Path,
    unpacked_dir: Path,
    *,
    runner: Runner = _default_runner,
    intermediate_tar_name: str = DEFAULT_INTERMEDIATE_TAR_NAME,
    zstd_long: int = DEFAULT_ZSTD_LONG,
    decompress_timeout_s: float = DEFAULT_DECOMPRESS_TIMEOUT_S,
    extract_timeout_s: float = DEFAULT_EXTRACT_TIMEOUT_S,
) -> None:
    """Decompress ``bundle.tar.zst`` then extract the tarball into ``unpacked_dir``.

    Two-step pipeline (matches docs/bundle-format.md §"Decompression":
    ``zstd -d --long=N -f`` then ``tar -xf``). Cleaner failure
    attribution than a piped one-step at the cost of ~1 GB of
    temporary disk usage. ``unpacked_dir`` is created if missing.

    Subprocess failure at either step raises
    :class:`BundleIntegrityError` — both kinds of failure mean the
    bytes the device received don't decompose into a usable tree,
    which is the right wire code (``failed:bundle_invalid``).
    """
    bundle_path = Path(bundle_path)
    unpacked_dir = Path(unpacked_dir)
    unpacked_dir.mkdir(parents=True, exist_ok=True)
    intermediate = unpacked_dir.parent / intermediate_tar_name

    logger.info(
        "decompressing bundle: src=%s intermediate=%s long=%d",
        bundle_path,
        intermediate,
        zstd_long,
    )
    try:
        result = runner(
            [
                "zstd",
                "-d",
                f"--long={zstd_long}",
                "-f",
                str(bundle_path),
                "-o",
                str(intermediate),
            ],
            timeout=decompress_timeout_s,
        )
    except FileNotFoundError as exc:
        raise BundleIntegrityError(
            f"zstd binary not found on PATH: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BundleIntegrityError(
            f"zstd decompress timed out after {decompress_timeout_s}s: {bundle_path}"
        ) from exc
    if result.returncode != 0:
        raise BundleIntegrityError(
            f"zstd decompress failed (rc={result.returncode}): "
            f"stderr={_tail(result.stderr)!r}"
        )

    logger.info(
        "extracting bundle tar: src=%s dest=%s",
        intermediate,
        unpacked_dir,
    )
    try:
        result = runner(
            [
                "tar",
                "-xf",
                str(intermediate),
                "-C",
                str(unpacked_dir),
                "--no-same-owner",
                "--no-same-permissions",
            ],
            timeout=extract_timeout_s,
        )
    except FileNotFoundError as exc:
        raise BundleIntegrityError(
            f"tar binary not found on PATH: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BundleIntegrityError(
            f"tar extract timed out after {extract_timeout_s}s: {intermediate}"
        ) from exc
    if result.returncode != 0:
        raise BundleIntegrityError(
            f"tar extract failed (rc={result.returncode}): "
            f"stderr={_tail(result.stderr)!r}"
        )

    try:
        intermediate.unlink()
    except FileNotFoundError:
        pass  # tar's job is done, missing intermediate is fine
    except OSError as exc:
        logger.warning(
            "could not remove intermediate tar at %s: %s "
            "(staging continues; 24h sweeper will clean it up)",
            intermediate,
            exc,
        )


def rsync_tree(
    src_dir: Path,
    dst_dir: Path,
    *,
    runner: Runner = _default_runner,
    rsync_timeout_s: float = DEFAULT_RSYNC_TIMEOUT_S,
) -> None:
    """Mirror ``src_dir`` onto ``dst_dir`` with ``rsync -aHAX --delete``.

    Flags:

    * ``-a`` — archive mode (preserves perms, ownership, symlinks,
      times, etc.).
    * ``-H`` — preserve hard links.
    * ``-A`` — preserve ACLs.
    * ``-X`` — preserve extended attributes.
    * ``--delete`` — remove destination files not present in source.
      Critical: we want the inactive slot to exactly match the
      bundle's content, not be a union of bundle + whatever the
      previous version left there.
    * ``--numeric-ids`` — preserve uid/gid as numbers (the bundle's
      uid table may not exist on the device).

    Both ``src_dir`` and ``dst_dir`` are passed with trailing slashes
    so rsync mirrors content rather than nesting one inside the other.

    Raises :class:`RsyncError` on subprocess timeout, missing rsync
    binary, or non-zero exit code.
    """
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    src_arg = str(src_dir) + "/"
    dst_arg = str(dst_dir) + "/"

    logger.info("rsync: %s -> %s", src_arg, dst_arg)
    try:
        result = runner(
            [
                "rsync",
                "-aHAX",
                "--delete",
                "--numeric-ids",
                src_arg,
                dst_arg,
            ],
            timeout=rsync_timeout_s,
        )
    except FileNotFoundError as exc:
        raise RsyncError(f"rsync binary not found on PATH: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RsyncError(
            f"rsync timed out after {rsync_timeout_s}s: {src_arg} -> {dst_arg}"
        ) from exc
    if result.returncode != 0:
        raise RsyncError(
            f"rsync failed (rc={result.returncode}) "
            f"writing {dst_arg}: stderr={_tail(result.stderr)!r}"
        )


# ── slot_mgr injection seams ───────────────────────────────────────────────


def _default_slot_state() -> Any:
    """Lazy-import :func:`slot_mgr.slot_state` and return its result.

    Mirrors :func:`precheck.core._default_slot_state` — keeps the
    dependency edge visible in code review and lets tests stub
    ``slot_state_fn=`` cleanly without monkey-patching the slot_mgr
    namespace.

    The real return value is a :class:`slot_mgr.SlotStatus` dataclass
    with ``running_slot: Optional[int]`` (None if /proc/cmdline doesn't
    contain a ``root=PARTLABEL=root-{A,B}`` token).
    """
    from slot_mgr import slot_state

    return slot_state()


def _default_trigger_tryboot(target_slot: int) -> Any:
    """Lazy-import :func:`slot_mgr.trigger_tryboot` and call it.

    Lazy import mirrors :func:`_default_slot_state`. The real call
    rewrites ``[tryboot] boot_partition`` in ``autoboot.txt``, records
    ``last_tryboot_target`` in ``slot-state.json``, then executes
    ``sudo reboot '0 tryboot'``. Returns the updated
    :class:`slot_mgr.SlotState` for logging.
    """
    from slot_mgr import trigger_tryboot

    return trigger_tryboot(target_slot)


# ── Stager ─────────────────────────────────────────────────────────────────


@dataclass
class SlotStager:
    """Concrete :class:`os_updater.service.Stager` implementation.

    All collaborators are injectable for tests. Defaults wire up the
    production paths (slot_mgr.slot_state, ``/boot/firmware{,-b}``,
    ``/mnt/inactive-root``, real :func:`slot_mgr.trigger_tryboot`).

    The orchestration is synchronous; :meth:`stage` wraps it in
    :func:`asyncio.to_thread` to keep the daemon's event loop
    responsive during the multi-minute decompress + extract + rsync
    phase, matching the pattern in :class:`SignatureVerifier`.
    """

    runner: Runner = field(default=_default_runner)
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME
    unpacked_subdir: str = DEFAULT_UNPACKED_SUBDIR
    intermediate_tar_name: str = DEFAULT_INTERMEDIATE_TAR_NAME
    boot_subdir: str = "boot"
    root_subdir: str = "root"
    meta_filename: str = "meta.json"
    boot_mount_slot_a: Path = field(default_factory=lambda: DEFAULT_BOOT_MOUNT_SLOT_A)
    boot_mount_slot_b: Path = field(default_factory=lambda: DEFAULT_BOOT_MOUNT_SLOT_B)
    inactive_root_mount: Path = field(default_factory=lambda: DEFAULT_INACTIVE_ROOT_MOUNT)
    zstd_long: int = DEFAULT_ZSTD_LONG
    decompress_timeout_s: float = DEFAULT_DECOMPRESS_TIMEOUT_S
    extract_timeout_s: float = DEFAULT_EXTRACT_TIMEOUT_S
    rsync_timeout_s: float = DEFAULT_RSYNC_TIMEOUT_S
    slot_state_fn: Optional[Callable[[], Any]] = None
    trigger_tryboot_fn: Optional[Callable[[int], Any]] = None

    async def stage(self, payload: DispatchPayload, staging_dir: Path) -> None:
        """Run the full stage-and-tryboot pipeline (steps 6–10 of the doc).

        Async entrypoint matching the :class:`Stager` protocol. The
        synchronous body lives in :meth:`_stage_sync` for ease of
        testing without an event loop.
        """
        await asyncio.to_thread(self._stage_sync, payload, staging_dir)

    def _stage_sync(self, payload: DispatchPayload, staging_dir: Path) -> None:
        staging_dir = Path(staging_dir)
        bundle_path = staging_dir / self.bundle_filename
        unpacked_dir = staging_dir / self.unpacked_subdir

        logger.info(
            "staging bundle: release=%s target_version=%s "
            "bundle=%s unpacked=%s",
            payload.release_id,
            payload.target_version,
            bundle_path,
            unpacked_dir,
        )

        # 1–2. Decompress + extract. Failures raise BundleIntegrityError.
        decompress_and_extract(
            bundle_path,
            unpacked_dir,
            runner=self.runner,
            intermediate_tar_name=self.intermediate_tar_name,
            zstd_long=self.zstd_long,
            decompress_timeout_s=self.decompress_timeout_s,
            extract_timeout_s=self.extract_timeout_s,
        )

        # 2b. Top-level entries must be exactly {boot/, root/, meta.json}.
        self._check_top_level_entries(unpacked_dir)

        # 3. Parse meta.json (raises BundleIntegrityError on any issue).
        meta_path = unpacked_dir / self.meta_filename
        meta = parse_bundle_meta(meta_path)
        logger.info(
            "bundle meta parsed: release=%s meta.version=%s "
            "meta.schema_version=%d manifest_entries=%d",
            payload.release_id,
            meta.version,
            meta.schema_version,
            len(meta.sha256_manifest),
        )

        # 4. Defense-in-depth target_version match.
        if meta.version != payload.target_version:
            raise BundleIntegrityError(
                f"bundle meta.version={meta.version!r} does not match "
                f"dispatch payload target_version={payload.target_version!r}"
            )

        # 5. Manifest sha256 verification.
        verify_bundle_manifest(unpacked_dir, meta)
        logger.info(
            "bundle manifest verified: release=%s files=%d",
            payload.release_id,
            len(meta.sha256_manifest),
        )

        # 6. Determine running + inactive slots via slot_mgr.
        running = self._read_running_slot()
        inactive = other_slot(running)
        logger.info(
            "slot detection: running=%d inactive=%d (release=%s)",
            running,
            inactive,
            payload.release_id,
        )

        # 7. Resolve target mountpoints for the inactive slot.
        boot_target = boot_mount_for_slot(
            inactive,
            slot_a_mount=self.boot_mount_slot_a,
            slot_b_mount=self.boot_mount_slot_b,
        )
        root_target = Path(self.inactive_root_mount)
        if not boot_target.is_dir():
            raise StagingError(
                f"inactive slot's boot mountpoint missing: {boot_target} "
                f"(expected mounted by systemd unit before daemon start)"
            )
        if not root_target.is_dir():
            raise StagingError(
                f"inactive slot's root mountpoint missing: {root_target} "
                f"(expected mounted by systemd unit before daemon start)"
            )

        # 8. rsync boot/ then root/.
        rsync_tree(
            unpacked_dir / self.boot_subdir,
            boot_target,
            runner=self.runner,
            rsync_timeout_s=self.rsync_timeout_s,
        )
        rsync_tree(
            unpacked_dir / self.root_subdir,
            root_target,
            runner=self.runner,
            rsync_timeout_s=self.rsync_timeout_s,
        )

        # 9. Trigger tryboot. slot_mgr handles autoboot.txt rewrite +
        # state persistence + sudo reboot '0 tryboot'.
        trigger_fn = self.trigger_tryboot_fn or _default_trigger_tryboot
        logger.info(
            "triggering tryboot to slot %d (release=%s)",
            inactive,
            payload.release_id,
        )
        try:
            trigger_fn(inactive)
        except Exception as exc:  # noqa: BLE001 — slot_mgr's PinnedError etc.
            raise TrybootError(
                f"trigger_tryboot({inactive}) failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    def _read_running_slot(self) -> int:
        slot_state_fn = self.slot_state_fn or _default_slot_state
        try:
            status = slot_state_fn()
        except Exception as exc:  # noqa: BLE001 — slot_mgr exposes a few
            raise StagingError(
                f"could not read slot state: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        running = getattr(status, "running_slot", None)
        if running not in (1, 2):
            raise StagingError(
                f"slot_mgr could not determine running slot "
                f"(running_slot={running!r}); refusing to stage"
            )
        return running

    def _check_top_level_entries(self, unpacked_dir: Path) -> None:
        expected = {self.boot_subdir, self.root_subdir, self.meta_filename}
        try:
            found = {entry.name for entry in unpacked_dir.iterdir()}
        except FileNotFoundError as exc:
            raise BundleIntegrityError(
                f"unpacked dir disappeared after extract: {unpacked_dir}"
            ) from exc
        extra = found - expected
        if extra:
            raise BundleIntegrityError(
                f"extracted bundle has unexpected top-level entries: "
                f"{sorted(extra)!r} (expected exactly {sorted(expected)!r})"
            )
        missing = expected - found
        if missing:
            raise BundleIntegrityError(
                f"extracted bundle missing required top-level entries: "
                f"{sorted(missing)!r}"
            )
