"""Tests for :mod:`os_updater.apply` — slot staging + tryboot trigger.

Acceptance hooks (plan.md §"Phase 2 — Acceptance"):

* Decompress + extract + manifest verify → fan-in via
  :class:`TestSlotStagerHappyPath`.
* Bundle integrity tampers (extra entries, version mismatch, manifest
  failure) → :class:`TestSlotStagerErrors`.
* rsync failure surfaces as ``RsyncError`` → :class:`TestRsyncTree`,
  :class:`TestSlotStagerErrors`.
* Pinned-device path: ``trigger_tryboot`` raising →
  :class:`TestSlotStagerErrors.test_pinned_device_raises_tryboot_error`.
* Concurrency interlock / pre-flight isn't this file's scope; the
  service tests own that.

These tests never invoke real ``zstd``, ``tar``, ``rsync``, or
``slot_mgr.trigger_tryboot`` — they pass fake :data:`Runner` and
``trigger_tryboot_fn`` callables to keep the suite fast and
CI-portable.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import pytest

from os_updater.apply import (
    DEFAULT_BOOT_MOUNT_SLOT_A,
    DEFAULT_BOOT_MOUNT_SLOT_B,
    RsyncError,
    SlotStager,
    StagingError,
    TrybootError,
    boot_mount_for_slot,
    decompress_and_extract,
    other_slot,
    rsync_tree,
)
from os_updater.bundle import BundleIntegrityError
from os_updater.dispatch import DispatchPayload


# ── Fake runner ────────────────────────────────────────────────────────────


@dataclass
class _FakeRunner:
    """Records every invocation; returns canned results per *command-head*.

    ``results_by_head`` maps the first arg of the command (e.g.
    ``"zstd"``, ``"tar"``, ``"rsync"``) to a return code. Unmapped
    heads default to 0.

    For decompress + extract tests we want to assert calls happened
    in a specific order, so :attr:`calls` is preserved in arrival
    order with the full argv list.
    """

    results_by_head: dict[str, int] = field(default_factory=dict)
    stderr_by_head: dict[str, str] = field(default_factory=dict)
    raise_file_not_found_for: set[str] = field(default_factory=set)
    raise_timeout_for: set[str] = field(default_factory=set)
    calls: list[Sequence[str]] = field(default_factory=list)

    def __call__(self, args, **kwargs):
        argv = list(args)
        self.calls.append(argv)
        head = argv[0] if argv else ""
        if head in self.raise_file_not_found_for:
            raise FileNotFoundError(head)
        if head in self.raise_timeout_for:
            raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get("timeout"))
        rc = self.results_by_head.get(head, 0)
        stderr = self.stderr_by_head.get(head, "")
        return subprocess.CompletedProcess(
            args=argv, returncode=rc, stdout="", stderr=stderr
        )


def _make_payload(target_version: str = "1.1.0") -> DispatchPayload:
    return DispatchPayload(
        release_id="rel_test",
        target_version=target_version,
        min_from_version="1.0.0",
        bundle_url="https://example.com/bundle.tar.zst",
        signature_url="https://example.com/bundle.tar.zst.minisig",
    )


# ── Pure helpers ───────────────────────────────────────────────────────────


class TestOtherSlot:
    def test_1_flips_to_2(self):
        assert other_slot(1) == 2

    def test_2_flips_to_1(self):
        assert other_slot(2) == 1

    @pytest.mark.parametrize("bad", [0, 3, -1, 99])
    def test_invalid_slot_raises_staging_error(self, bad):
        with pytest.raises(StagingError, match="invalid slot number"):
            other_slot(bad)


class TestBootMountForSlot:
    def test_slot_1_returns_a_mount(self, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        assert boot_mount_for_slot(1, slot_a_mount=a, slot_b_mount=b) == a

    def test_slot_2_returns_b_mount(self, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        assert boot_mount_for_slot(2, slot_a_mount=a, slot_b_mount=b) == b

    def test_defaults_match_phase_0_fstab(self):
        """Sanity-pin the production paths — touching these is a fleet-wide
        breaking change."""
        assert boot_mount_for_slot(1) == DEFAULT_BOOT_MOUNT_SLOT_A
        assert boot_mount_for_slot(2) == DEFAULT_BOOT_MOUNT_SLOT_B

    @pytest.mark.parametrize("bad", [0, 3, -1])
    def test_invalid_slot_raises_staging_error(self, bad):
        with pytest.raises(StagingError, match="invalid slot number"):
            boot_mount_for_slot(bad)


# ── decompress_and_extract ─────────────────────────────────────────────────


class TestDecompressAndExtract:
    def test_happy_path_calls_zstd_then_tar(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"fake compressed bytes")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner()

        # Pre-place the intermediate tar so cleanup at end finds something
        # to delete (matches what real zstd would have written).
        intermediate = tmp_path / "bundle.tar"
        intermediate.write_bytes(b"fake tar bytes")

        decompress_and_extract(bundle, unpacked, runner=runner)
        assert unpacked.is_dir()
        assert len(runner.calls) == 2
        assert runner.calls[0][0] == "zstd"
        assert runner.calls[0][1] == "-d"
        assert runner.calls[1][0] == "tar"
        assert runner.calls[1][1] == "-xf"

    def test_intermediate_tar_is_unlinked_on_success(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        intermediate = tmp_path / "bundle.tar"
        intermediate.write_bytes(b"y")

        decompress_and_extract(bundle, unpacked, runner=_FakeRunner())
        assert not intermediate.exists(), "intermediate tar should be removed on success"

    def test_missing_intermediate_at_cleanup_is_silent(self, tmp_path):
        """If something else already removed the intermediate (e.g.
        a previous run's leftovers got swept), cleanup must not raise."""
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        # Don't pre-create the intermediate; runner is a no-op anyway.
        decompress_and_extract(bundle, unpacked, runner=_FakeRunner())

    def test_long_flag_is_passed_to_zstd(self, tmp_path):
        """``zstd -d --long=27`` is required to match the builder side
        per docs/bundle-format.md §"Compression"."""
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner()
        decompress_and_extract(bundle, unpacked, runner=runner, zstd_long=27)
        zstd_argv = runner.calls[0]
        assert "--long=27" in zstd_argv

    def test_zstd_failure_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"corrupted")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner(
            results_by_head={"zstd": 1},
            stderr_by_head={"zstd": "Decoding error"},
        )
        with pytest.raises(BundleIntegrityError, match="zstd decompress failed"):
            decompress_and_extract(bundle, unpacked, runner=runner)
        # Tar shouldn't even be invoked.
        assert len(runner.calls) == 1

    def test_zstd_binary_missing_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner(raise_file_not_found_for={"zstd"})
        with pytest.raises(BundleIntegrityError, match="zstd binary not found"):
            decompress_and_extract(bundle, unpacked, runner=runner)

    def test_zstd_timeout_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner(raise_timeout_for={"zstd"})
        with pytest.raises(BundleIntegrityError, match="zstd decompress timed out"):
            decompress_and_extract(bundle, unpacked, runner=runner)

    def test_tar_failure_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        intermediate = tmp_path / "bundle.tar"
        intermediate.write_bytes(b"y")
        runner = _FakeRunner(
            results_by_head={"tar": 2},
            stderr_by_head={"tar": "Cannot read"},
        )
        with pytest.raises(BundleIntegrityError, match="tar extract failed"):
            decompress_and_extract(bundle, unpacked, runner=runner)

    def test_tar_binary_missing_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner(raise_file_not_found_for={"tar"})
        with pytest.raises(BundleIntegrityError, match="tar binary not found"):
            decompress_and_extract(bundle, unpacked, runner=runner)

    def test_tar_timeout_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        unpacked = tmp_path / "unpacked"
        runner = _FakeRunner(raise_timeout_for={"tar"})
        with pytest.raises(BundleIntegrityError, match="tar extract timed out"):
            decompress_and_extract(bundle, unpacked, runner=runner)


# ── rsync_tree ─────────────────────────────────────────────────────────────


class TestRsyncTree:
    def test_happy_path_calls_rsync_with_canonical_flags(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        runner = _FakeRunner()
        rsync_tree(src, dst, runner=runner)
        assert len(runner.calls) == 1
        argv = runner.calls[0]
        assert argv[0] == "rsync"
        assert "-aHAX" in argv
        assert "--delete" in argv
        assert "--numeric-ids" in argv

    def test_trailing_slashes_added_to_src_and_dst(self, tmp_path):
        """rsync semantics: ``src/`` mirrors contents into ``dst/``
        rather than nesting under ``dst/src/``. Critical: bare paths
        would mis-target the slot."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        runner = _FakeRunner()
        rsync_tree(src, dst, runner=runner)
        argv = runner.calls[0]
        # Last two args are the paths.
        assert argv[-2].endswith("/")
        assert argv[-1].endswith("/")

    def test_non_zero_rc_raises_rsync_error(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        runner = _FakeRunner(
            results_by_head={"rsync": 23},
            stderr_by_head={"rsync": "some files vanished"},
        )
        with pytest.raises(RsyncError, match="rsync failed"):
            rsync_tree(src, dst, runner=runner)

    def test_rsync_binary_missing_raises_rsync_error(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        runner = _FakeRunner(raise_file_not_found_for={"rsync"})
        with pytest.raises(RsyncError, match="rsync binary not found"):
            rsync_tree(src, dst, runner=runner)

    def test_rsync_timeout_raises_rsync_error(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        runner = _FakeRunner(raise_timeout_for={"rsync"})
        with pytest.raises(RsyncError, match="rsync timed out"):
            rsync_tree(src, dst, runner=runner)


# ── SlotStager: pipeline helpers ───────────────────────────────────────────


def _build_unpacked_tree(
    staging_dir: Path,
    target_version: str = "1.1.0",
    files: dict[str, bytes] | None = None,
    extra_top_level: list[str] | None = None,
    missing_top_level: list[str] | None = None,
) -> tuple[Path, dict]:
    """Build a faux ``unpacked/`` tree under ``staging_dir`` and return
    the path + the meta dict that names every regular file under it.

    Used to simulate what zstd + tar would have produced. The
    SlotStager pipeline picks up from there.
    """
    unpacked = staging_dir / "unpacked"
    boot = unpacked / "boot"
    root = unpacked / "root"
    boot.mkdir(parents=True)
    root.mkdir(parents=True)
    if files is None:
        files = {
            "boot/cmdline.txt": b"console=tty1 root=PARTLABEL=root-A",
            "root/etc/os-release": b'NAME="Agora OS"\n',
        }
    for relpath, content in files.items():
        target = unpacked / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    manifest = {
        relpath: hashlib.sha256(content).hexdigest()
        for relpath, content in files.items()
    }
    meta = {
        "version": target_version,
        "min_from_version": "1.0.0",
        "schema_version": 2,
        "sha256_manifest": manifest,
        "created_at": "2026-04-22T00:00:00Z",
        "builder": "test",
    }
    (unpacked / "meta.json").write_text(json.dumps(meta))

    for extra in extra_top_level or []:
        (unpacked / extra).touch()
    for missing in missing_top_level or []:
        target = unpacked / missing
        if target.is_dir():
            for child in target.rglob("*"):
                if child.is_file():
                    child.unlink()
            target.rmdir()
        elif target.exists():
            target.unlink()

    return unpacked, meta


class _FakeSlotStatus:
    """Mimics :class:`slot_mgr.SlotStatus` — only attr accessed is
    ``running_slot``."""

    def __init__(self, running_slot):
        self.running_slot = running_slot


def _make_stager(
    tmp_path: Path,
    *,
    runner: _FakeRunner | None = None,
    running_slot=1,
    trigger_tryboot_exc: Exception | None = None,
    trigger_tryboot_calls: list[int] | None = None,
) -> SlotStager:
    """Build a SlotStager pointed at ``tmp_path`` with injected seams."""
    if runner is None:
        runner = _FakeRunner()

    def fake_slot_state():
        return _FakeSlotStatus(running_slot)

    def fake_trigger_tryboot(target_slot):
        if trigger_tryboot_calls is not None:
            trigger_tryboot_calls.append(target_slot)
        if trigger_tryboot_exc is not None:
            raise trigger_tryboot_exc
        return _FakeSlotStatus(target_slot)

    # All mountpoints live under tmp_path so the .is_dir() checks pass.
    slot_a_boot = tmp_path / "boot-slot-a"
    slot_b_boot = tmp_path / "boot-slot-b"
    inactive_root = tmp_path / "inactive-root"
    slot_a_boot.mkdir()
    slot_b_boot.mkdir()
    inactive_root.mkdir()

    return SlotStager(
        runner=runner,
        boot_mount_slot_a=slot_a_boot,
        boot_mount_slot_b=slot_b_boot,
        inactive_root_mount=inactive_root,
        slot_state_fn=fake_slot_state,
        trigger_tryboot_fn=fake_trigger_tryboot,
    )


def _seed_staging(tmp_path: Path) -> Path:
    """Create ``staging_dir/bundle.tar.zst`` + matching ``unpacked/``
    tree so the SlotStager pipeline can run without real zstd/tar."""
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "bundle.tar.zst").write_bytes(b"fake compressed")
    _build_unpacked_tree(staging_dir)
    return staging_dir


# ── SlotStager: happy path ─────────────────────────────────────────────────


class TestSlotStagerHappyPath:
    def test_full_pipeline_running_slot_1(self, tmp_path):
        """Running slot 1 ⇒ tryboot must target slot 2 ⇒ boot rsync hits
        slot-B mount."""
        staging_dir = _seed_staging(tmp_path)
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        stager = _make_stager(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )

        stager._stage_sync(_make_payload(), staging_dir)

        # Tryboot fired with the inactive slot.
        assert tryboot_calls == [2]
        # zstd, tar, rsync, rsync — 4 subprocess calls.
        heads = [c[0] for c in runner.calls]
        assert heads == ["zstd", "tar", "rsync", "rsync"]
        # First rsync writes the slot-B boot mount.
        rsync_calls = [c for c in runner.calls if c[0] == "rsync"]
        assert str(stager.boot_mount_slot_b) in rsync_calls[0][-1]
        # Second rsync writes inactive-root.
        assert str(stager.inactive_root_mount) in rsync_calls[1][-1]

    def test_full_pipeline_running_slot_2(self, tmp_path):
        """Running slot 2 ⇒ tryboot must target slot 1 ⇒ rsync hits
        slot-A mount."""
        staging_dir = _seed_staging(tmp_path)
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        stager = _make_stager(
            tmp_path,
            runner=runner,
            running_slot=2,
            trigger_tryboot_calls=tryboot_calls,
        )
        stager._stage_sync(_make_payload(), staging_dir)
        assert tryboot_calls == [1]
        rsync_calls = [c for c in runner.calls if c[0] == "rsync"]
        assert str(stager.boot_mount_slot_a) in rsync_calls[0][-1]

    def test_stage_async_entrypoint_dispatches_to_sync(self, tmp_path):
        """The async ``stage()`` is a thin :func:`asyncio.to_thread` wrapper.
        Hitting it once proves the wiring."""
        import asyncio

        staging_dir = _seed_staging(tmp_path)
        tryboot_calls: list[int] = []
        stager = _make_stager(
            tmp_path,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        asyncio.run(stager.stage(_make_payload(), staging_dir))
        assert tryboot_calls == [2]


# ── SlotStager: error paths ────────────────────────────────────────────────


class TestSlotStagerErrors:
    def test_unexpected_top_level_entries_raises_integrity_error(self, tmp_path):
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"x")
        _build_unpacked_tree(staging_dir, extra_top_level=["unexpected.txt"])
        stager = _make_stager(tmp_path, running_slot=1)
        with pytest.raises(BundleIntegrityError, match="unexpected top-level entries"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_missing_top_level_entry_raises_integrity_error(self, tmp_path):
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"x")
        # Build the tree, then yank one of the required dirs.
        _build_unpacked_tree(staging_dir, missing_top_level=["boot"])
        stager = _make_stager(tmp_path, running_slot=1)
        with pytest.raises(BundleIntegrityError, match="missing required top-level"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_meta_version_mismatch_raises_integrity_error(self, tmp_path):
        """Defense-in-depth: bundle's ``meta.version`` must match the
        dispatch payload's ``target_version`` even if the signature is
        valid. Catches a swapped-bundle scenario."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"x")
        # Bundle says 1.2.0; payload below says 1.1.0.
        _build_unpacked_tree(staging_dir, target_version="1.2.0")
        stager = _make_stager(tmp_path, running_slot=1)
        with pytest.raises(BundleIntegrityError, match="does not match.*target_version"):
            stager._stage_sync(_make_payload("1.1.0"), staging_dir)

    def test_manifest_mismatch_raises_integrity_error(self, tmp_path):
        """File on disk hashes differently than the manifest claims."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"x")
        unpacked, _meta = _build_unpacked_tree(staging_dir)
        # Tamper with one of the files after the manifest was sealed.
        (unpacked / "boot/cmdline.txt").write_bytes(b"tampered")
        stager = _make_stager(tmp_path, running_slot=1)
        with pytest.raises(BundleIntegrityError, match="sha256 mismatch"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_running_slot_none_raises_staging_error(self, tmp_path):
        """``slot_state()`` couldn't determine which slot is running —
        refuse to stage (would otherwise risk writing to the live slot)."""
        staging_dir = _seed_staging(tmp_path)
        stager = _make_stager(tmp_path, running_slot=None)
        with pytest.raises(StagingError, match="could not determine running slot"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_slot_state_fn_raising_is_wrapped_in_staging_error(self, tmp_path):
        """slot_mgr may raise — wrap as :class:`StagingError` so the
        service classifies it as ``stage_failed``, not ``error_<TypeName>``."""
        staging_dir = _seed_staging(tmp_path)

        def boom():
            raise RuntimeError("dbus is down")

        runner = _FakeRunner()
        slot_a = tmp_path / "boot-a"
        slot_b = tmp_path / "boot-b"
        inactive = tmp_path / "inactive"
        slot_a.mkdir(); slot_b.mkdir(); inactive.mkdir()
        stager = SlotStager(
            runner=runner,
            boot_mount_slot_a=slot_a,
            boot_mount_slot_b=slot_b,
            inactive_root_mount=inactive,
            slot_state_fn=boom,
            trigger_tryboot_fn=lambda s: None,
        )
        with pytest.raises(StagingError, match="could not read slot state"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_missing_boot_mountpoint_raises_staging_error(self, tmp_path):
        """If the systemd unit didn't mount slot-B's boot partition, the
        stager must refuse — writing into a non-mount would silently
        target the *running* slot's filesystem."""
        staging_dir = _seed_staging(tmp_path)
        stager = _make_stager(tmp_path, running_slot=1)
        # Yank the slot-B boot dir we built in the helper.
        import shutil
        shutil.rmtree(stager.boot_mount_slot_b)
        with pytest.raises(StagingError, match="boot mountpoint missing"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_missing_inactive_root_mountpoint_raises_staging_error(self, tmp_path):
        staging_dir = _seed_staging(tmp_path)
        stager = _make_stager(tmp_path, running_slot=1)
        import shutil
        shutil.rmtree(stager.inactive_root_mount)
        with pytest.raises(StagingError, match="root mountpoint missing"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_rsync_failure_raises_rsync_error(self, tmp_path):
        staging_dir = _seed_staging(tmp_path)
        runner = _FakeRunner(
            results_by_head={"rsync": 23},
            stderr_by_head={"rsync": "vanished"},
        )
        stager = _make_stager(tmp_path, runner=runner, running_slot=1)
        with pytest.raises(RsyncError, match="rsync failed"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_pinned_device_raises_tryboot_error(self, tmp_path):
        """``slot_mgr.trigger_tryboot`` can raise ``PinnedError`` (or any
        exception) when 3-strikes pinning is active. The stager must wrap
        that as :class:`TrybootError` for the wire code."""
        staging_dir = _seed_staging(tmp_path)
        tryboot_calls: list[int] = []
        stager = _make_stager(
            tmp_path,
            running_slot=1,
            trigger_tryboot_exc=RuntimeError("device pinned"),
            trigger_tryboot_calls=tryboot_calls,
        )
        with pytest.raises(TrybootError, match="trigger_tryboot"):
            stager._stage_sync(_make_payload(), staging_dir)
        # The trigger was actually called — wrapping happens after the raise.
        assert tryboot_calls == [2]

    def test_meta_json_missing_raises_integrity_error(self, tmp_path):
        """Edge: ``meta.json`` deleted after extract but before parse.
        Shouldn't normally happen but the parse error must still surface."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"x")
        unpacked, _ = _build_unpacked_tree(staging_dir)
        (unpacked / "meta.json").unlink()
        # Top-level entries check fires first — it sees boot/, root/ but
        # no meta.json — that's a "missing required top-level" error.
        stager = _make_stager(tmp_path, running_slot=1)
        with pytest.raises(BundleIntegrityError, match="missing required top-level"):
            stager._stage_sync(_make_payload(), staging_dir)
