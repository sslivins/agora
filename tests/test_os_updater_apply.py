"""Tests for :mod:`os_updater.apply` — slot staging + tryboot trigger.

Acceptance hooks (plan.md §"Phase 2 — Acceptance"):

* Streaming decompress + extract + manifest verify → fan-in via
  :class:`TestSlotStagerHappyPath`.
* Bundle integrity tampers (version mismatch, missing-on-disk, manifest
  failure) → :class:`TestSlotStagerErrors`.
* Pinned-device path: ``trigger_tryboot`` raising →
  :class:`TestSlotStagerErrors.test_pinned_device_raises_tryboot_error`.
* fstab template substitution + cmdline rewrite (the two v0.0.6-test
  bricked-the-Pi bugs) → :class:`TestSubstituteFstab`,
  :class:`TestRewriteCmdline`.
* Concurrency interlock / pre-flight isn't this file's scope; the
  service tests own that.

These tests never invoke real ``zstd``, ``tar``, ``rsync``, or
``slot_mgr.trigger_tryboot`` — they pass a fake ``pipeline_runner``
(for the streaming zstd|tar pipeline), a fake ``runner`` (for ``cp -a``
fleet-state copies), and a ``trigger_tryboot_fn`` callable to keep the
suite fast and CI-portable.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import pytest

from os_updater.apply import (
    DEFAULT_BOOT_MOUNT_OPTS,
    DEFAULT_BOOT_MOUNT_SLOT_A,
    DEFAULT_BOOT_MOUNT_SLOT_B,
    DEFAULT_MOUNT_TIMEOUT_S,
    DEFAULT_ROOT_MOUNT_OPTS,
    FLEET_STATE_COPY_IF_PRESENT,
    FLEET_STATE_REQUIRED,
    STAGE_PROGRESS_PHASES,
    CmdlineError,
    FleetStateMissingError,
    FleetStateWriteError,
    FstabError,
    RsyncError,
    SlotStager,
    StagingError,
    TrybootError,
    _is_mountpoint,
    boot_mount_for_slot,
    boot_partlabel_for_slot,
    copy_fleet_state,
    ensure_partition_mounted,
    extract_meta_only,
    other_slot,
    rewrite_cmdline,
    root_partlabel_for_slot,
    rsync_tree,
    stream_extract_subtree,
    substitute_fstab,
)
from os_updater.bundle import BundleIntegrityError
from os_updater.dispatch import DispatchPayload


# ── Fake runner (cp / rsync) ───────────────────────────────────────────────


@dataclass
class _FakeRunner:
    """Records every invocation; returns canned results per *command-head*.

    ``results_by_head`` maps the first arg of the command (e.g.
    ``"rsync"``, ``"cp"``) to a return code. Unmapped heads default
    to 0.

    For ordering-sensitive assertions, :attr:`calls` is preserved in
    arrival order with the full argv list.
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


# ── Fake pipeline runner (zstd | tar) ──────────────────────────────────────


@dataclass
class _FakePipelineRunner:
    """Stand-in for :func:`os_updater.apply._default_pipeline_runner`.

    Models the streaming ``zstd -dc <bundle> | tar -x ... -C <dst> <subpath>``
    pipeline. Doesn't invoke any real subprocesses — instead writes a
    set of pre-canned files into the ``-C`` target directory, mimicking
    what a real ``tar -x --strip-components=1 -C <dst> <subpath>`` would
    have produced after stripping the leading ``<subpath>/``.

    Two call shapes are supported, matching the production code:

    * **Meta-only** (``extract_meta_only``): ``tar_stdout_path`` is set,
      and the canned :attr:`meta_bytes` is written to that path.
    * **Subtree extract** (``stream_extract_subtree``): the last arg of
      ``tar_argv`` is the subpath (e.g. ``"boot"`` / ``"root"``); the
      ``-C`` flag's value is the destination dir. Files from
      :attr:`files_by_subpath[subpath]` are written into ``<dst>/<rel>``.

    Failure injection:

    * :attr:`fail_rc_by_subpath` maps a subpath (or ``"__meta__"``) to a
      4-tuple ``(zstd_rc, zstd_stderr, tar_rc, tar_stderr)`` returned
      verbatim. Apply-side error-classification uses ``zstd_rc != 0``
      vs ``tar_rc != 0`` to distinguish "zstd decompress failed" from
      "tar extract failed".
    * :attr:`raise_file_not_found` — first call raises
      ``FileNotFoundError`` (binary missing).
    * :attr:`raise_timeout` — first call raises
      ``subprocess.TimeoutExpired`` (pipeline hung).
    """

    files_by_subpath: dict[str, dict[str, bytes]] = field(default_factory=dict)
    meta_bytes: bytes = b""
    fail_rc_by_subpath: dict[str, tuple[int, str, int, str]] = field(
        default_factory=dict
    )
    raise_file_not_found: bool = False
    raise_timeout: bool = False
    # list of (subpath_or_meta_marker, tar_stdout_path_or_None, dst_dir_or_None)
    calls: list[tuple[str, Optional[str], Optional[str]]] = field(default_factory=list)

    def __call__(
        self,
        zstd_argv,
        tar_argv,
        *,
        tar_stdout_path=None,
        timeout_s,
    ):
        if self.raise_file_not_found:
            raise FileNotFoundError("zstd")
        if self.raise_timeout:
            raise subprocess.TimeoutExpired(
                cmd=list(zstd_argv), timeout=timeout_s
            )

        tar_list = list(tar_argv)

        # Meta-only path: tar -xOf - meta.json with stdout redirected.
        if tar_stdout_path is not None:
            self.calls.append(("__meta__", str(tar_stdout_path), None))
            if "__meta__" in self.fail_rc_by_subpath:
                return self.fail_rc_by_subpath["__meta__"]
            Path(tar_stdout_path).parent.mkdir(parents=True, exist_ok=True)
            Path(tar_stdout_path).write_bytes(self.meta_bytes)
            return (0, "", 0, "")

        # Subtree extract: last positional arg is the subpath; -C flag
        # points at the dst dir.
        subpath = tar_list[-1]
        dst_idx = tar_list.index("-C")
        dst = Path(tar_list[dst_idx + 1])

        self.calls.append((subpath, None, str(dst)))
        if subpath in self.fail_rc_by_subpath:
            return self.fail_rc_by_subpath[subpath]

        # Mimic ``tar --strip-components=1``: keys are already
        # rooted-after-strip (no leading ``boot/`` / ``root/``).
        for rel, content in self.files_by_subpath.get(subpath, {}).items():
            target = dst / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)

        return (0, "", 0, "")


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


# ── boot_partlabel_for_slot / root_partlabel_for_slot ──────────────────────


class TestPartlabelHelpers:
    """Slot→partlabel mapping — these must NEVER drift from
    ``agora-os/image-build/assemble.sh``'s ``sgdisk -c`` invocations,
    or the wrong partition gets mounted in step 5."""

    def test_boot_slot_1_returns_boot_a(self):
        assert boot_partlabel_for_slot(1) == "boot-A"

    def test_boot_slot_2_returns_boot_b(self):
        assert boot_partlabel_for_slot(2) == "boot-B"

    def test_root_slot_1_returns_root_a(self):
        assert root_partlabel_for_slot(1) == "root-A"

    def test_root_slot_2_returns_root_b(self):
        assert root_partlabel_for_slot(2) == "root-B"

    @pytest.mark.parametrize("bad", [0, 3, -1, 99])
    def test_invalid_slot_raises_staging_error_for_boot(self, bad):
        with pytest.raises(StagingError, match="invalid slot number"):
            boot_partlabel_for_slot(bad)

    @pytest.mark.parametrize("bad", [0, 3, -1, 99])
    def test_invalid_slot_raises_staging_error_for_root(self, bad):
        with pytest.raises(StagingError, match="invalid slot number"):
            root_partlabel_for_slot(bad)


# ── _is_mountpoint ─────────────────────────────────────────────────────────


class TestIsMountpoint:
    """Pure-function tests for the ``/proc/self/mounts`` lookup."""

    def test_returns_true_when_target_present(self, tmp_path):
        target = tmp_path / "mnt-target"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text(f"/dev/foo {target} ext4 rw 0 0\n")
        assert _is_mountpoint(target, mounts_path=mounts) is True

    def test_returns_false_when_target_absent(self, tmp_path):
        target = tmp_path / "mnt-target"
        target.mkdir()
        other = tmp_path / "other"
        other.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text(f"/dev/bar {other} ext4 rw 0 0\n")
        assert _is_mountpoint(target, mounts_path=mounts) is False

    def test_returns_false_when_mounts_file_missing(self, tmp_path):
        target = tmp_path / "mnt-target"
        target.mkdir()
        missing = tmp_path / "does-not-exist"
        assert _is_mountpoint(target, mounts_path=missing) is False

    def test_decodes_octal_escapes_in_mountpoint(self, tmp_path):
        """``/proc/self/mounts`` encodes literal spaces in mountpoints
        as ``\\040``. The helper must decode those before comparing."""
        target = tmp_path / "with space"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        # Field 2 must be encoded with \040 for the space.
        encoded = str(target).replace(" ", r"\040")
        mounts.write_text(f"/dev/baz {encoded} ext4 rw 0 0\n")
        assert _is_mountpoint(target, mounts_path=mounts) is True

    def test_ignores_short_lines(self, tmp_path):
        """A blank or 1-field line in mounts must not crash the parser."""
        target = tmp_path / "mnt-target"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text(
            "\n"
            "single-field-line\n"
            f"/dev/foo {target} ext4 rw 0 0\n"
        )
        assert _is_mountpoint(target, mounts_path=mounts) is True


# ── ensure_partition_mounted ───────────────────────────────────────────────


class TestEnsurePartitionMounted:
    """Step-5 helper: idempotent mount of a labeled partition."""

    def test_noop_when_already_mounted(self, tmp_path):
        target = tmp_path / "mnt"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text(f"/dev/foo {target} ext4 rw 0 0\n")
        runner = _FakeRunner()
        ensure_partition_mounted(
            "root-B",
            target,
            fstype="ext4",
            opts="rw",
            runner=runner,
            mounts_path=mounts,
            partlabel_base=tmp_path / "partlabel-base",
        )
        # No mount(8) call should have been recorded.
        assert runner.calls == []

    def test_invokes_mount_when_not_mounted(self, tmp_path):
        target = tmp_path / "mnt"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text("")
        partlabel_base = tmp_path / "partlabel-base"
        partlabel_base.mkdir()
        runner = _FakeRunner()
        ensure_partition_mounted(
            "boot-B",
            target,
            fstype="vfat",
            opts="umask=0077",
            runner=runner,
            mounts_path=mounts,
            partlabel_base=partlabel_base,
        )
        assert len(runner.calls) == 1
        argv = runner.calls[0]
        # Verify argv shape: mount -t vfat -o umask=0077 /<base>/boot-B <target>
        assert argv[0] == "mount"
        assert argv[1:3] == ["-t", "vfat"]
        assert argv[3:5] == ["-o", "umask=0077"]
        assert argv[5] == str(partlabel_base / "boot-B")
        assert argv[6] == str(target)

    def test_creates_target_directory_if_absent(self, tmp_path):
        target = tmp_path / "deep" / "nested" / "mnt"
        assert not target.exists()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text("")
        partlabel_base = tmp_path / "partlabel-base"
        partlabel_base.mkdir()
        runner = _FakeRunner()
        ensure_partition_mounted(
            "boot-B",
            target,
            fstype="vfat",
            opts="rw",
            runner=runner,
            mounts_path=mounts,
            partlabel_base=partlabel_base,
        )
        assert target.is_dir()

    def test_raises_staging_error_when_mount_returns_nonzero(self, tmp_path):
        target = tmp_path / "mnt"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text("")
        partlabel_base = tmp_path / "partlabel-base"
        partlabel_base.mkdir()
        runner = _FakeRunner(
            results_by_head={"mount": 32},
            stderr_by_head={"mount": "wrong fs type, bad option, bad superblock"},
        )
        with pytest.raises(StagingError) as exc:
            ensure_partition_mounted(
                "root-B",
                target,
                fstype="ext4",
                opts="rw",
                runner=runner,
                mounts_path=mounts,
                partlabel_base=partlabel_base,
            )
        msg = str(exc.value)
        assert "failed to mount root-B" in msg
        assert "rc=32" in msg
        # Stderr is surfaced so the wire telemetry actually says what went wrong.
        assert "bad superblock" in msg

    def test_raises_staging_error_when_mount_binary_missing(self, tmp_path):
        target = tmp_path / "mnt"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text("")
        runner = _FakeRunner(raise_file_not_found_for={"mount"})
        with pytest.raises(StagingError, match="mount\\(8\\) binary not found"):
            ensure_partition_mounted(
                "boot-B",
                target,
                fstype="vfat",
                opts="rw",
                runner=runner,
                mounts_path=mounts,
                partlabel_base=tmp_path / "partlabel-base",
            )

    def test_raises_staging_error_when_mount_times_out(self, tmp_path):
        target = tmp_path / "mnt"
        target.mkdir()
        mounts = tmp_path / "proc-mounts"
        mounts.write_text("")
        partlabel_base = tmp_path / "partlabel-base"
        partlabel_base.mkdir()
        runner = _FakeRunner(raise_timeout_for={"mount"})
        with pytest.raises(StagingError, match="mount\\(8\\) timed out"):
            ensure_partition_mounted(
                "boot-B",
                target,
                fstype="vfat",
                opts="rw",
                runner=runner,
                mounts_path=mounts,
                partlabel_base=partlabel_base,
                timeout_s=0.5,
            )


# ── stream_extract_subtree ─────────────────────────────────────────────────


class TestStreamExtractSubtree:
    def test_happy_path_writes_files_into_dst(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"fake compressed bytes")
        dst = tmp_path / "dst"
        pipeline = _FakePipelineRunner(
            files_by_subpath={
                "boot": {
                    "cmdline.txt": b"console=tty1 root=PARTLABEL=root-A rw\n",
                    "config.txt": b"# pi config\n",
                },
            },
        )

        stream_extract_subtree(bundle, "boot", dst, pipeline_runner=pipeline)

        assert (dst / "cmdline.txt").read_bytes() == (
            b"console=tty1 root=PARTLABEL=root-A rw\n"
        )
        assert (dst / "config.txt").read_bytes() == b"# pi config\n"
        # Pipeline was invoked exactly once for the boot subtree.
        assert len(pipeline.calls) == 1
        subpath, stdout_path, _ = pipeline.calls[0]
        assert subpath == "boot"
        assert stdout_path is None

    def test_passes_long_flag_and_bundle_path_to_pipeline(self, tmp_path):
        """``--long=NN`` must match the builder side to decompress
        long-range bundles. Captured by inspecting the argv via a
        thin pipeline-runner wrapper."""
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        dst = tmp_path / "dst"
        seen: list[tuple[Sequence[str], Sequence[str]]] = []

        def capturing_pipeline(zstd_argv, tar_argv, *, tar_stdout_path=None, timeout_s):
            seen.append((list(zstd_argv), list(tar_argv)))
            return (0, "", 0, "")

        stream_extract_subtree(
            bundle, "root", dst, pipeline_runner=capturing_pipeline, zstd_long=27
        )

        zstd_argv, tar_argv = seen[0]
        assert "zstd" in zstd_argv[0]
        assert "--long=27" in zstd_argv
        assert str(bundle) in zstd_argv
        assert "tar" in tar_argv[0]
        assert "--strip-components=1" in tar_argv
        assert "-C" in tar_argv
        assert str(dst) in tar_argv
        assert tar_argv[-1] == "root"

    def test_tar_argv_preserves_perms_and_ownership(self, tmp_path):
        """Regression guard for sslivins/agora#187.

        The streaming extract runs as root and writes directly into
        the inactive slot's rootfs. It MUST NOT pass
        ``--no-same-owner`` or ``--no-same-permissions``: those flags
        strip setuid/setgid bits and reset every extracted file's
        uid/gid to the running user, which on the device meant slot
        B came up with non-functional ``sudo``, ``su``, ``passwd``,
        etc. and ``/home/agora`` owned by root.

        GNU tar's defaults already do the right thing when invoked as
        root (the stager always is), so the correct fix was simply
        to remove the flags. This test pins that decision."""
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        dst = tmp_path / "dst"
        captured: list[Sequence[str]] = []

        def capturing_pipeline(zstd_argv, tar_argv, *, tar_stdout_path=None, timeout_s):
            captured.append(list(tar_argv))
            return (0, "", 0, "")

        stream_extract_subtree(
            bundle, "root", dst, pipeline_runner=capturing_pipeline
        )

        tar_argv = captured[0]
        assert "--no-same-owner" not in tar_argv, (
            "tar must NOT be invoked with --no-same-owner: it would "
            "reset every extracted file's uid/gid to the running user, "
            "breaking the inactive-slot rootfs (see agora#187)"
        )
        assert "--no-same-permissions" not in tar_argv, (
            "tar must NOT be invoked with --no-same-permissions: it "
            "applies umask and strips setuid/setgid/sticky bits, "
            "leaving sudo/su/passwd/etc. non-functional on the "
            "inactive slot (see agora#187)"
        )


    def test_zstd_failure_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"corrupted")
        dst = tmp_path / "dst"
        pipeline = _FakePipelineRunner(
            fail_rc_by_subpath={"boot": (1, "Decoding error", 0, "")},
        )
        with pytest.raises(BundleIntegrityError, match="zstd decompress failed"):
            stream_extract_subtree(bundle, "boot", dst, pipeline_runner=pipeline)

    def test_tar_failure_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        dst = tmp_path / "dst"
        pipeline = _FakePipelineRunner(
            fail_rc_by_subpath={"root": (0, "", 2, "Cannot read")},
        )
        with pytest.raises(BundleIntegrityError, match="tar extract failed"):
            stream_extract_subtree(bundle, "root", dst, pipeline_runner=pipeline)

    def test_binary_missing_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        dst = tmp_path / "dst"
        pipeline = _FakePipelineRunner(raise_file_not_found=True)
        with pytest.raises(BundleIntegrityError, match="not found"):
            stream_extract_subtree(bundle, "boot", dst, pipeline_runner=pipeline)

    def test_timeout_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        dst = tmp_path / "dst"
        pipeline = _FakePipelineRunner(raise_timeout=True)
        with pytest.raises(BundleIntegrityError, match="timed out"):
            stream_extract_subtree(bundle, "boot", dst, pipeline_runner=pipeline)


# ── extract_meta_only ──────────────────────────────────────────────────────


class TestExtractMetaOnly:
    def test_happy_path_writes_meta_to_dst(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        meta_path = tmp_path / "staging" / "meta.json"
        canned = json.dumps({"version": "1.1.0"}).encode()
        pipeline = _FakePipelineRunner(meta_bytes=canned)

        extract_meta_only(bundle, meta_path, pipeline_runner=pipeline)

        assert meta_path.read_bytes() == canned
        # Pipeline was invoked once with tar_stdout_path set.
        assert len(pipeline.calls) == 1
        subpath, stdout_path, _ = pipeline.calls[0]
        assert subpath == "__meta__"
        assert stdout_path == str(meta_path)

    def test_tar_failure_raises_bundle_integrity_error(self, tmp_path):
        """``tar -xOf - meta.json`` exits non-zero if ``meta.json`` is
        absent from the archive. Apply must surface this as a bundle
        integrity error."""
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        meta_path = tmp_path / "staging" / "meta.json"
        pipeline = _FakePipelineRunner(
            fail_rc_by_subpath={
                "__meta__": (0, "", 2, "meta.json: Not found in archive"),
            },
        )
        with pytest.raises(BundleIntegrityError, match="tar"):
            extract_meta_only(bundle, meta_path, pipeline_runner=pipeline)

    def test_timeout_raises_bundle_integrity_error(self, tmp_path):
        bundle = tmp_path / "bundle.tar.zst"
        bundle.write_bytes(b"x")
        meta_path = tmp_path / "staging" / "meta.json"
        pipeline = _FakePipelineRunner(raise_timeout=True)
        with pytest.raises(BundleIntegrityError, match="timed out"):
            extract_meta_only(bundle, meta_path, pipeline_runner=pipeline)


# ── substitute_fstab ───────────────────────────────────────────────────────


_FSTAB_TEMPLATE = (
    b"PARTLABEL={{BOOT_PARTLABEL}}  /boot/firmware  vfat  defaults  0  2\n"
    b"PARTLABEL=data  /data  ext4  defaults  0  2\n"
)


class TestSubstituteFstab:
    def test_slot_1_substitutes_boot_a(self, tmp_path):
        root = tmp_path / "root"
        (root / "etc").mkdir(parents=True)
        (root / "etc" / "fstab.template").write_bytes(_FSTAB_TEMPLATE)

        substitute_fstab(root, 1)

        out = (root / "etc" / "fstab").read_bytes()
        assert b"PARTLABEL=boot-A" in out
        assert b"{{BOOT_PARTLABEL}}" not in out

    def test_slot_2_substitutes_boot_b(self, tmp_path):
        root = tmp_path / "root"
        (root / "etc").mkdir(parents=True)
        (root / "etc" / "fstab.template").write_bytes(_FSTAB_TEMPLATE)

        substitute_fstab(root, 2)

        out = (root / "etc" / "fstab").read_bytes()
        assert b"PARTLABEL=boot-B" in out
        assert b"{{BOOT_PARTLABEL}}" not in out

    def test_missing_placeholder_raises_fstab_error(self, tmp_path):
        """A template with no ``{{BOOT_PARTLABEL}}`` placeholder is a
        producer bug — refuse to write a "rendered" fstab that would
        silently mount the wrong boot partition."""
        root = tmp_path / "root"
        (root / "etc").mkdir(parents=True)
        (root / "etc" / "fstab.template").write_bytes(
            b"# no placeholder\nPARTLABEL=boot-A  /boot/firmware  vfat  defaults  0  2\n"
        )
        with pytest.raises(FstabError, match="placeholder"):
            substitute_fstab(root, 1)

    def test_missing_template_raises_fstab_error(self, tmp_path):
        """No template file at all is also a producer bug; bricks the
        device on tryboot — the v0.0.6-test class of failure."""
        root = tmp_path / "root"
        (root / "etc").mkdir(parents=True)
        with pytest.raises(FstabError, match="template"):
            substitute_fstab(root, 1)


# ── rewrite_cmdline ────────────────────────────────────────────────────────


class TestRewriteCmdline:
    def test_slot_1_writes_root_partlabel_a(self, tmp_path):
        boot = tmp_path / "boot"
        boot.mkdir()
        (boot / "cmdline.txt").write_text(
            "console=tty1 root=PARTLABEL=root-B rw rootfstype=ext4\n"
        )

        rewrite_cmdline(boot, 1)

        out = (boot / "cmdline.txt").read_text()
        assert "root=PARTLABEL=root-A" in out
        # The old "root=PARTLABEL=root-B" must be gone.
        assert "root=PARTLABEL=root-B" not in out
        # Other tokens preserved.
        assert "console=tty1" in out
        assert "rootfstype=ext4" in out

    def test_slot_2_writes_root_partlabel_b(self, tmp_path):
        boot = tmp_path / "boot"
        boot.mkdir()
        (boot / "cmdline.txt").write_text(
            "console=tty1 root=PARTLABEL=root-A rw\n"
        )

        rewrite_cmdline(boot, 2)

        out = (boot / "cmdline.txt").read_text()
        assert "root=PARTLABEL=root-B" in out
        assert "root=PARTLABEL=root-A" not in out

    def test_appends_rw_if_missing(self, tmp_path):
        """rootfs must be mounted r/w so agora-firstboot can resize and
        seed /data. A cmdline lacking ``rw`` is a producer bug we
        defend against."""
        boot = tmp_path / "boot"
        boot.mkdir()
        (boot / "cmdline.txt").write_text(
            "console=tty1 root=PARTLABEL=root-A ro rootfstype=ext4\n"
        )

        rewrite_cmdline(boot, 2)

        out = (boot / "cmdline.txt").read_text()
        # ``ro`` token preserved verbatim (we don't strip it), but ``rw``
        # is appended if not present.
        assert " rw" in out or out.startswith("rw") or "rw " in out

    def test_strips_stray_root_token(self, tmp_path):
        """Multiple ``root=`` tokens in the source would be ambiguous;
        rewrite collapses to exactly one."""
        boot = tmp_path / "boot"
        boot.mkdir()
        (boot / "cmdline.txt").write_text(
            "root=/dev/mmcblk0p3 console=tty1 root=PARTLABEL=root-A rw\n"
        )

        rewrite_cmdline(boot, 1)

        out = (boot / "cmdline.txt").read_text()
        assert out.count("root=") == 1
        assert "root=PARTLABEL=root-A" in out
        assert "/dev/mmcblk0p3" not in out

    def test_missing_cmdline_raises_cmdline_error(self, tmp_path):
        boot = tmp_path / "boot"
        boot.mkdir()
        with pytest.raises(CmdlineError, match="cmdline"):
            rewrite_cmdline(boot, 1)


# ── rsync_tree (still exported for fleet-state callers / tests) ────────────


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


_DEFAULT_BOOT_FILES = {
    "cmdline.txt": b"console=tty1 root=PARTLABEL=root-A rw rootfstype=ext4\n",
}
_DEFAULT_ROOT_FILES = {
    "etc/os-release": b'NAME="Agora OS"\n',
    "etc/fstab.template": _FSTAB_TEMPLATE,
}


def _build_partitioned_bundle(
    *,
    target_version: str = "1.1.0",
    boot_files: dict[str, bytes] | None = None,
    root_files: dict[str, bytes] | None = None,
    extra_manifest_entries: dict[str, bytes] | None = None,
    drop_manifest_entries: list[str] | None = None,
) -> tuple[bytes, dict[str, dict[str, bytes]]]:
    """Build ``(meta_bytes, files_by_subpath)`` matching what a real
    ``zstd | tar --strip-components=1`` pipeline would have produced.

    The manifest in ``meta_bytes`` uses bundle-relative paths (e.g.
    ``"boot/cmdline.txt"`` / ``"root/etc/fstab.template"``); the
    ``files_by_subpath`` dict has the partition-relative paths the
    fake pipeline runner writes after ``--strip-components=1``.

    ``extra_manifest_entries`` adds rows to the manifest using their
    hashes — the matching files are NOT written by the runner, which
    is how we simulate the "missing on disk" error path.

    ``drop_manifest_entries`` removes rows from the manifest by their
    bundle-relative path — the matching files ARE still written by
    the runner, simulating "extra file on disk".
    """
    boot_files = (
        dict(_DEFAULT_BOOT_FILES) if boot_files is None else dict(boot_files)
    )
    root_files = (
        dict(_DEFAULT_ROOT_FILES) if root_files is None else dict(root_files)
    )

    files_by_subpath = {"boot": boot_files, "root": root_files}

    manifest: dict[str, str] = {}
    for rel, content in boot_files.items():
        manifest[f"boot/{rel}"] = hashlib.sha256(content).hexdigest()
    for rel, content in root_files.items():
        manifest[f"root/{rel}"] = hashlib.sha256(content).hexdigest()

    for key, content in (extra_manifest_entries or {}).items():
        manifest[key] = hashlib.sha256(content).hexdigest()

    for key in drop_manifest_entries or []:
        manifest.pop(key, None)

    meta = {
        "version": target_version,
        "min_from_version": "1.0.0",
        "schema_version": 2,
        "sha256_manifest": manifest,
        "created_at": "2026-04-22T00:00:00Z",
        "builder": "test",
    }
    return json.dumps(meta).encode(), files_by_subpath


class _FakeSlotStatus:
    """Mimics :class:`slot_mgr.SlotStatus` — only attr accessed is
    ``running_slot``."""

    def __init__(self, running_slot):
        self.running_slot = running_slot


def _seed_fake_slot_a(slot_a_root: Path) -> None:
    """Populate ``slot_a_root`` with every :data:`FLEET_STATE_REQUIRED`
    fixture plus one ``copy_if_present`` optional file.

    Mirrors a real provisioned device's slot A so fleet-state copy
    sees a satisfied required set. Tests that want to exercise
    "missing required" / "missing optional" paths call ``unlink``
    on individual files after this seeds.
    """
    slot_a_root.mkdir(parents=True, exist_ok=True)

    files = {
        "etc/agora/environment": b"AGORA_FLEET_ID=test-fleet\n",
        "opt/agora/persist/cms_config.json": b'{"cms_url":"https://cms.example"}',
        "opt/agora/persist/provisioned": b"1\n",
        "etc/machine-id": b"00112233445566778899aabbccddeeff\n",
        # Two ssh host keys so the glob matches >= 1 (and exercises multi-match).
        # Content is opaque test bytes (apply is a byte-for-byte cp; the contents
        # are never inspected). Avoid real SSH-key headers so secret-scanners
        # don't flag the fixture.
        "etc/ssh/ssh_host_rsa_key": b"fake-rsa-key-bytes-for-test\n",
        "etc/ssh/ssh_host_ed25519_key": b"fake-ed25519-key-bytes-for-test\n",
        # One optional file, matching the wifi-*.nmconnection glob.
        "etc/NetworkManager/system-connections/wifi-home.nmconnection": (
            b"[connection]\nid=home\n"
        ),
        # Optional operator SSH state — D60 gap fix (agora#198). The
        # ``home/agora/.ssh`` entry in FLEET_STATE_COPY_IF_PRESENT is a
        # directory; seeding ``authorized_keys`` here causes the parent
        # ``.ssh`` dir to exist on slot A, which is what
        # ``_resolve_fleet_state_sources`` returns for that literal.
        "home/agora/.ssh/authorized_keys": (
            b"ssh-ed25519 AAAAfake-key-for-test operator@workstation\n"
        ),
    }
    for rel, content in files.items():
        path = slot_a_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def _make_stager(
    tmp_path: Path,
    *,
    runner: _FakeRunner | None = None,
    pipeline_runner: _FakePipelineRunner | None = None,
    running_slot=1,
    trigger_tryboot_exc: Exception | None = None,
    trigger_tryboot_calls: list[int] | None = None,
    seed_fleet_state: bool = True,
) -> SlotStager:
    """Build a SlotStager pointed at ``tmp_path`` with injected seams.

    By default seeds a fake slot-A rootfs under ``tmp_path/slot-a-root``
    populated with every :data:`FLEET_STATE_REQUIRED` fixture so the
    full happy-path pipeline runs end-to-end. Pass
    ``seed_fleet_state=False`` to leave slot A empty for "missing
    required" tests.

    ``pipeline_runner`` defaults to a no-canned-files fake which is
    useful for tests that don't drive the full pipeline (e.g. the
    slot-state error tests that bail before ``stream_extract_subtree``).
    """
    if runner is None:
        runner = _FakeRunner()
    if pipeline_runner is None:
        pipeline_runner = _FakePipelineRunner()

    def fake_slot_state():
        return _FakeSlotStatus(running_slot)

    def fake_trigger_tryboot(target_slot):
        if trigger_tryboot_calls is not None:
            trigger_tryboot_calls.append(target_slot)
        if trigger_tryboot_exc is not None:
            raise trigger_tryboot_exc
        return _FakeSlotStatus(target_slot)

    # All mountpoints live under tmp_path. They start as empty dirs;
    # SlotStager.step5 calls ensure_partition_mounted on slot-B's boot
    # + the inactive-root, which reads ``mounts_path`` (a fake
    # /proc/self/mounts) and no-ops if the target is already mounted.
    # We seed that fake mounts file with the slot-B + inactive-root
    # entries so happy-path tests skip mount(8) entirely — there's no
    # real device node to mount and the runner would record a phantom
    # ``mount`` call. Tests that exercise the mount-fails-with-stderr
    # path can swap in a runner whose `results_by_head["mount"]=N` and
    # leave the entries out of the mounts file (rewriting mounts_file
    # after _make_stager returns, or constructing the stager
    # explicitly).
    slot_a_boot = tmp_path / "boot-slot-a"
    slot_b_boot = tmp_path / "boot-slot-b"
    inactive_root = tmp_path / "inactive-root"
    slot_a_boot.mkdir()
    slot_b_boot.mkdir()
    inactive_root.mkdir()

    mounts_file = tmp_path / "proc-mounts"
    mounts_file.write_text(
        f"/dev/mmcblk0p1 {slot_a_boot} vfat rw 0 0\n"
        f"/dev/mmcblk0p2 {slot_b_boot} vfat rw 0 0\n"
        f"/dev/mmcblk0p4 {inactive_root} ext4 rw 0 0\n"
    )
    partlabel_base = tmp_path / "partlabel-base"
    partlabel_base.mkdir()

    slot_a_root = tmp_path / "slot-a-root"
    if seed_fleet_state:
        _seed_fake_slot_a(slot_a_root)
    else:
        slot_a_root.mkdir()

    return SlotStager(
        runner=runner,
        pipeline_runner=pipeline_runner,
        boot_mount_slot_a=slot_a_boot,
        boot_mount_slot_b=slot_b_boot,
        inactive_root_mount=inactive_root,
        slot_state_fn=fake_slot_state,
        trigger_tryboot_fn=fake_trigger_tryboot,
        slot_a_root=slot_a_root,
        mounts_path=mounts_file,
        partlabel_base=partlabel_base,
    )


def _setup_stager_with_bundle(
    tmp_path: Path,
    *,
    target_version: str = "1.1.0",
    boot_files: dict[str, bytes] | None = None,
    root_files: dict[str, bytes] | None = None,
    extra_manifest_entries: dict[str, bytes] | None = None,
    drop_manifest_entries: list[str] | None = None,
    runner: _FakeRunner | None = None,
    running_slot=1,
    trigger_tryboot_exc: Exception | None = None,
    trigger_tryboot_calls: list[int] | None = None,
    seed_fleet_state: bool = True,
) -> tuple[Path, SlotStager, _FakePipelineRunner]:
    """One-call setup: build staging dir + canned bundle + stager.

    Returns ``(staging_dir, stager, pipeline_runner)``. The pipeline
    runner is exposed so tests can introspect or mutate it (e.g. add a
    second invocation that fails)."""
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "bundle.tar.zst").write_bytes(b"fake compressed")

    meta_bytes, files_by_subpath = _build_partitioned_bundle(
        target_version=target_version,
        boot_files=boot_files,
        root_files=root_files,
        extra_manifest_entries=extra_manifest_entries,
        drop_manifest_entries=drop_manifest_entries,
    )
    pipeline_runner = _FakePipelineRunner(
        files_by_subpath=files_by_subpath, meta_bytes=meta_bytes
    )

    stager = _make_stager(
        tmp_path,
        runner=runner,
        pipeline_runner=pipeline_runner,
        running_slot=running_slot,
        trigger_tryboot_exc=trigger_tryboot_exc,
        trigger_tryboot_calls=trigger_tryboot_calls,
        seed_fleet_state=seed_fleet_state,
    )
    return staging_dir, stager, pipeline_runner


# ── SlotStager: happy path ─────────────────────────────────────────────────


class TestSlotStagerHappyPath:
    def test_full_pipeline_running_slot_1(self, tmp_path):
        """Running slot 1 ⇒ tryboot must target slot 2 ⇒ streaming
        writes hit the slot-B boot mount + the inactive-root mount.

        Asserts (a) trigger_tryboot fired with slot 2, (b) only cp -a
        calls land in the synchronous runner (zstd/tar/rsync go through
        the pipeline_runner now), (c) substitute_fstab wrote
        ``etc/fstab`` referencing boot-B, (d) rewrite_cmdline rewrote
        cmdline.txt to ``root=PARTLABEL=root-B``.
        """
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )

        stager._stage_sync(_make_payload(), staging_dir)

        # Tryboot fired with the inactive slot.
        assert tryboot_calls == [2]

        # Pipeline calls: meta-only, then boot, then root.
        pipeline_subpaths = [c[0] for c in pipeline.calls]
        assert pipeline_subpaths == ["__meta__", "boot", "root"]

        # Runner only saw cp calls — no zstd/tar/rsync at all.
        heads = [c[0] for c in runner.calls]
        assert heads, "expected cp invocations for fleet-state copy"
        assert all(h == "cp" for h in heads), (
            f"expected only cp calls in runner, got {heads}"
        )
        # 4 required literals + 2 ssh host keys + 1 optional wifi nmconnection
        # + 1 home/agora/.ssh dir from _seed_fake_slot_a == 8 cp invocations.
        assert len(heads) == 8

        # Boot was streamed into slot-B boot mount.
        boot_call = next(c for c in pipeline.calls if c[0] == "boot")
        assert boot_call[2] == str(stager.boot_mount_slot_b)

        # Root was streamed into inactive-root mount.
        root_call = next(c for c in pipeline.calls if c[0] == "root")
        assert root_call[2] == str(stager.inactive_root_mount)

        # Every cp's destination must be inside the inactive_root_mount —
        # otherwise we're writing identity files into the running slot.
        cp_calls = [c for c in runner.calls if c[0] == "cp"]
        for cp in cp_calls:
            assert str(stager.inactive_root_mount) in cp[-1]

        # substitute_fstab side-effect: etc/fstab now exists on slot B
        # and references boot-B (since we tryboot to slot 2).
        fstab = (stager.inactive_root_mount / "etc" / "fstab").read_bytes()
        assert b"PARTLABEL=boot-B" in fstab
        assert b"{{BOOT_PARTLABEL}}" not in fstab

        # rewrite_cmdline side-effect: cmdline.txt references root-B.
        cmdline = (stager.boot_mount_slot_b / "cmdline.txt").read_text()
        assert "root=PARTLABEL=root-B" in cmdline
        assert "root=PARTLABEL=root-A" not in cmdline

    def test_full_pipeline_running_slot_2(self, tmp_path):
        """Running slot 2 ⇒ tryboot must target slot 1 ⇒ streaming
        writes hit the slot-A boot mount + the inactive-root mount."""
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=2,
            trigger_tryboot_calls=tryboot_calls,
        )
        stager._stage_sync(_make_payload(), staging_dir)
        assert tryboot_calls == [1]

        boot_call = next(c for c in pipeline.calls if c[0] == "boot")
        assert boot_call[2] == str(stager.boot_mount_slot_a)

        # cmdline rewritten to root-A.
        cmdline = (stager.boot_mount_slot_a / "cmdline.txt").read_text()
        assert "root=PARTLABEL=root-A" in cmdline

        # fstab references boot-A.
        fstab = (stager.inactive_root_mount / "etc" / "fstab").read_bytes()
        assert b"PARTLABEL=boot-A" in fstab

    def test_stage_async_entrypoint_dispatches_to_sync(self, tmp_path):
        """The async ``stage()`` is a thin :func:`asyncio.to_thread` wrapper.
        Hitting it once proves the wiring."""
        import asyncio

        tryboot_calls: list[int] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        asyncio.run(stager.stage(_make_payload(), staging_dir))
        assert tryboot_calls == [2]


# ── SlotStager: error paths ────────────────────────────────────────────────


class TestSlotStagerErrors:
    def test_missing_on_disk_raises_integrity_error(self, tmp_path):
        """Manifest references a file the pipeline runner didn't write
        (simulates a tar bug, an unexpected ``--strip-components`` mismatch,
        or a builder regression). verify_bundle_manifest catches it."""
        # The runner only writes etc/os-release; the manifest also
        # references etc/fstab.template (via _DEFAULT_ROOT_FILES) but
        # we'll strip it from the runner's payload below.
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        # Drop a file from the runner so manifest verify sees it missing.
        pipeline.files_by_subpath["root"].pop("etc/fstab.template")

        with pytest.raises(BundleIntegrityError, match="missing on disk"):
            stager._stage_sync(_make_payload(), staging_dir)

        # tryboot must NOT fire — verify failed first.
        assert tryboot_calls == []

    def test_extra_file_on_disk_raises_integrity_error(self, tmp_path):
        """Pipeline runner writes a file the manifest doesn't list —
        could be a tar bug or a tampered bundle that added rootkit
        binaries the producer never signed. verify_bundle_manifest
        catches it."""
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        # Add an extra file to the runner that's not in the manifest.
        pipeline.files_by_subpath["root"]["etc/sneaky.bin"] = b"surprise!\n"

        with pytest.raises(BundleIntegrityError, match="extra file"):
            stager._stage_sync(_make_payload(), staging_dir)

        assert tryboot_calls == []

    def test_meta_version_mismatch_raises_integrity_error(self, tmp_path):
        """Defense-in-depth: bundle's ``meta.version`` must match the
        dispatch payload's ``target_version`` even if the signature is
        valid. Catches a swapped-bundle scenario."""
        tryboot_calls: list[int] = []
        # Bundle says 1.2.0; payload below says 1.1.0.
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            target_version="1.2.0",
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        with pytest.raises(BundleIntegrityError, match="does not match.*target_version"):
            stager._stage_sync(_make_payload("1.1.0"), staging_dir)
        # tryboot must NOT fire — version mismatch is detected BEFORE
        # we write a single byte to slot B.
        assert tryboot_calls == []

    def test_manifest_sha256_mismatch_raises_integrity_error(self, tmp_path):
        """File written by the pipeline runner hashes differently than
        the manifest claims (simulates a torn write or a tampered bundle
        whose manifest wasn't re-signed)."""
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        # Tamper with the file the runner will write.
        pipeline.files_by_subpath["boot"]["cmdline.txt"] = b"tampered\n"

        with pytest.raises(BundleIntegrityError, match="sha256 mismatch"):
            stager._stage_sync(_make_payload(), staging_dir)

        assert tryboot_calls == []

    def test_meta_extract_failure_raises_integrity_error(self, tmp_path):
        """``tar -xOf - meta.json`` returns non-zero (meta member absent
        from archive). Should surface BEFORE any partition writes."""
        runner = _FakeRunner()
        tryboot_calls: list[int] = []
        staging_dir, stager, pipeline = _setup_stager_with_bundle(
            tmp_path,
            runner=runner,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
        )
        pipeline.fail_rc_by_subpath["__meta__"] = (
            0,
            "",
            2,
            "meta.json: Not found in archive",
        )
        with pytest.raises(BundleIntegrityError):
            stager._stage_sync(_make_payload(), staging_dir)
        assert tryboot_calls == []

    def test_running_slot_none_raises_staging_error(self, tmp_path):
        """``slot_state()`` couldn't determine which slot is running —
        refuse to stage (would otherwise risk writing to the live slot)."""
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=None
        )
        with pytest.raises(StagingError, match="could not determine running slot"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_slot_state_fn_raising_is_wrapped_in_staging_error(self, tmp_path):
        """slot_mgr may raise — wrap as :class:`StagingError` so the
        service classifies it as ``stage_failed``, not ``error_<TypeName>``."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "bundle.tar.zst").write_bytes(b"fake")
        meta_bytes, files_by_subpath = _build_partitioned_bundle()
        pipeline = _FakePipelineRunner(
            files_by_subpath=files_by_subpath, meta_bytes=meta_bytes
        )

        def boom():
            raise RuntimeError("dbus is down")

        runner = _FakeRunner()
        slot_a = tmp_path / "boot-a"
        slot_b = tmp_path / "boot-b"
        inactive = tmp_path / "inactive"
        slot_a.mkdir(); slot_b.mkdir(); inactive.mkdir()
        stager = SlotStager(
            runner=runner,
            pipeline_runner=pipeline,
            boot_mount_slot_a=slot_a,
            boot_mount_slot_b=slot_b,
            inactive_root_mount=inactive,
            slot_state_fn=boom,
            trigger_tryboot_fn=lambda s: None,
        )
        with pytest.raises(StagingError, match="could not read slot state"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_mount_failure_for_boot_raises_staging_error(self, tmp_path):
        """If ``mount(8)`` fails on slot-B's boot partition (e.g. the
        device node doesn't exist), the stager must refuse — writing
        into a non-mount would silently target the running slot."""
        runner = _FakeRunner(
            results_by_head={"mount": 32},
            stderr_by_head={"mount": "no such device"},
        )
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1, runner=runner
        )
        # Empty mounts file → ensure_partition_mounted will try to
        # mount → runner returns rc=32.
        stager.mounts_path.write_text("")
        with pytest.raises(StagingError, match="failed to mount boot-B"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_mount_failure_for_root_raises_staging_error(self, tmp_path):
        """Same as the boot variant but the boot partition mount
        succeeds first; root mount fails second."""
        # Fake runner: mount succeeds for vfat, fails for ext4.
        class _PerArgRunner:
            calls: list[Sequence[str]] = []

            def __call__(self, args, **kwargs):
                argv = list(args)
                self.calls.append(argv)
                if argv[:3] == ["mount", "-t", "vfat"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0, stdout="", stderr=""
                    )
                if argv[:3] == ["mount", "-t", "ext4"]:
                    return subprocess.CompletedProcess(
                        args=argv,
                        returncode=32,
                        stdout="",
                        stderr="superblock corrupt",
                    )
                return subprocess.CompletedProcess(
                    args=argv, returncode=0, stdout="", stderr=""
                )

        runner = _PerArgRunner()
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1, runner=runner
        )
        stager.mounts_path.write_text("")
        with pytest.raises(StagingError, match="failed to mount root-B"):
            stager._stage_sync(_make_payload(), staging_dir)

    def test_pinned_device_raises_tryboot_error(self, tmp_path):
        """``slot_mgr.trigger_tryboot`` can raise ``PinnedError`` (or any
        exception) when 3-strikes pinning is active. The stager must wrap
        that as :class:`TrybootError` for the wire code."""
        tryboot_calls: list[int] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=1,
            trigger_tryboot_exc=RuntimeError("device pinned"),
            trigger_tryboot_calls=tryboot_calls,
        )
        with pytest.raises(TrybootError, match="trigger_tryboot"):
            stager._stage_sync(_make_payload(), staging_dir)
        # The trigger was actually called — wrapping happens after the raise.
        assert tryboot_calls == [2]


# ── copy_fleet_state (D60) ─────────────────────────────────────────────────


class TestCopyFleetState:
    """Apply-time fleet-state allowlist copy from running rootfs (slot A)
    into the freshly-unpacked inactive slot.

    These tests exercise the function in isolation; the SlotStager
    integration is covered by :class:`TestSlotStagerFleetStateIntegration`
    below.
    """

    def test_happy_path_copies_required_and_optional(self, tmp_path):
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        runner = _FakeRunner()

        copy_fleet_state(slot_a, slot_b, runner=runner)

        cps = [c for c in runner.calls if c[0] == "cp"]
        # 4 required literals + 2 ssh host keys + 1 optional wifi conn
        # + 1 home/agora/.ssh dir = 8
        assert len(cps) == 8, f"expected 8 cp invocations, got {len(cps)}: {cps}"
        # Every cp call uses cp -a (preserve mode/owner/timestamps).
        for cp in cps:
            assert cp[1] == "-a", f"expected cp -a, got {cp}"
        # Destinations must all live under slot_b.
        for cp in cps:
            assert cp[-1].startswith(str(slot_b))

    def test_missing_required_raises_fleet_state_missing_error(self, tmp_path):
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        # Yank one of the required literal files.
        (slot_a / "etc/agora/environment").unlink()
        runner = _FakeRunner()

        with pytest.raises(FleetStateMissingError) as exc_info:
            copy_fleet_state(slot_a, slot_b, runner=runner)

        assert exc_info.value.path == "etc/agora/environment"
        # On abort, no cp must have been executed for the missing entry's
        # subsequent siblings — fail-fast semantics.
        cps = [c for c in runner.calls if c[0] == "cp"]
        # `etc/agora/environment` is the first item in FLEET_STATE_REQUIRED,
        # so zero cp calls should fire before the abort.
        assert cps == []

    def test_missing_required_glob_raises_fleet_state_missing_error(self, tmp_path):
        """Glob entries (``etc/ssh/ssh_host_*``) with zero matches are
        also a hard failure — a real device must have ssh host keys."""
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        # Strip every ssh host key so the glob returns empty.
        import shutil
        shutil.rmtree(slot_a / "etc/ssh")
        runner = _FakeRunner()

        with pytest.raises(FleetStateMissingError) as exc_info:
            copy_fleet_state(slot_a, slot_b, runner=runner)

        assert "ssh_host" in exc_info.value.path

    def test_missing_optional_is_silently_skipped(self, tmp_path, caplog):
        """Optional entries (``api_key``, ``wifi-*.nmconnection``) with
        zero matches must NOT abort. Device boots slot B and re-mints
        the missing state on next CMS handshake."""
        import logging

        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        # Drop the only optional file we seeded.
        (slot_a / "etc/NetworkManager/system-connections/wifi-home.nmconnection").unlink()
        runner = _FakeRunner()

        with caplog.at_level(logging.INFO, logger="os_updater.apply"):
            copy_fleet_state(slot_a, slot_b, runner=runner)

        cps = [c for c in runner.calls if c[0] == "cp"]
        # Required set still copied: 4 literals + 2 ssh keys = 6, plus
        # the still-present home/agora/.ssh optional dir = 7.
        assert len(cps) == 7
        # A skip event must be logged for diagnostic purposes.
        assert any("fleet_state_skipped" in r.message for r in caplog.records)

    def test_cp_nonzero_exit_raises_fleet_state_write_error(self, tmp_path):
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        runner = _FakeRunner(
            results_by_head={"cp": 1},
            stderr_by_head={"cp": "permission denied"},
        )

        with pytest.raises(FleetStateWriteError) as exc_info:
            copy_fleet_state(slot_a, slot_b, runner=runner)

        # First entry in FLEET_STATE_REQUIRED is etc/agora/environment.
        assert exc_info.value.path == "etc/agora/environment"

    def test_cp_timeout_raises_fleet_state_write_error(self, tmp_path):
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        runner = _FakeRunner(raise_timeout_for={"cp"})

        with pytest.raises(FleetStateWriteError) as exc_info:
            copy_fleet_state(slot_a, slot_b, runner=runner)

        assert exc_info.value.path == "etc/agora/environment"

    # ── home/agora/.ssh — D60 gap fix (agora#198) ──────────────────────────

    def test_ssh_dir_copied_when_present(self, tmp_path):
        """``home/agora/.ssh`` is a directory entry in COPY_IF_PRESENT.
        When the directory exists on slot A, ``copy_fleet_state`` issues
        a single ``cp -a`` for the directory itself (not one per child),
        so ``cp -a`` preserves the 0700 perms + ``agora:agora`` ownership
        that ``sshd`` enforces on ``authorized_keys`` (see agora#198).
        """
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        runner = _FakeRunner()

        copy_fleet_state(slot_a, slot_b, runner=runner)

        cps = [c for c in runner.calls if c[0] == "cp"]
        ssh_cps = [
            c for c in cps
            if c[-2].endswith(str(Path("home/agora/.ssh")))
            or c[-2].endswith(str(Path("home") / "agora" / ".ssh"))
        ]
        assert len(ssh_cps) == 1, (
            f"expected exactly one cp -a for home/agora/.ssh as a directory, "
            f"got: {ssh_cps}"
        )
        # cp -a was used (preserves perms/owner).
        assert ssh_cps[0][1] == "-a"
        # Source is the slot-A .ssh dir; destination is under slot-B.
        assert str(slot_a / "home" / "agora" / ".ssh") == ssh_cps[0][-2]
        assert str(slot_b / "home" / "agora" / ".ssh") == ssh_cps[0][-1]

    def test_ssh_dir_absent_skips_silently(self, tmp_path, caplog):
        """A production-deployed Pi may have no operator SSH access.
        Absence of ``home/agora/.ssh`` must NOT abort the apply — it's
        ``COPY_IF_PRESENT``, not ``REQUIRED``.
        """
        import logging
        import shutil as _shutil

        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        # Drop the seeded .ssh dir.
        _shutil.rmtree(slot_a / "home" / "agora" / ".ssh")
        runner = _FakeRunner()

        with caplog.at_level(logging.INFO, logger="os_updater.apply"):
            copy_fleet_state(slot_a, slot_b, runner=runner)

        cps = [c for c in runner.calls if c[0] == "cp"]
        # 4 required literals + 2 ssh host keys + 1 wifi optional = 7.
        assert len(cps) == 7
        # The skip event must mention the .ssh path so operator-facing
        # telemetry can distinguish "missing wifi" from "missing .ssh".
        skip_records = [
            r for r in caplog.records
            if "fleet_state_skipped" in r.message and "home/agora/.ssh" in r.message
        ]
        assert skip_records, (
            f"expected a fleet_state_skipped log for home/agora/.ssh, "
            f"got: {[r.message for r in caplog.records]}"
        )

    def test_ssh_dir_idempotent_replaces_stale_dst(self, tmp_path):
        """If slot B already has a ``home/agora/.ssh`` from a prior
        partial-apply recovery, ``_cp_one`` must ``rmtree`` it before
        ``cp -a`` so the source ends up AT dst, not nested as
        ``slot_b/home/agora/.ssh/.ssh``.
        """
        slot_a = tmp_path / "slot-a"
        slot_b = tmp_path / "slot-b"
        slot_b.mkdir()
        _seed_fake_slot_a(slot_a)
        # Simulate a stale .ssh directory left over from a prior
        # partial apply (e.g. apply aborted after rsync but before
        # tryboot triggered).
        stale_ssh = slot_b / "home" / "agora" / ".ssh"
        stale_ssh.mkdir(parents=True)
        stale_marker = stale_ssh / "stale_authorized_keys"
        stale_marker.write_bytes(b"this should be removed before cp -a runs\n")
        assert stale_marker.exists()

        runner = _FakeRunner()
        copy_fleet_state(slot_a, slot_b, runner=runner)

        # The stale destination must have been removed by ``_cp_one``'s
        # rmtree guard. Because ``_FakeRunner`` does NOT execute ``cp``,
        # the dst directory should now not exist at all (rmtree removed
        # it; the mocked cp left nothing).
        assert not stale_marker.exists(), (
            "stale slot-B .ssh content survived: rmtree guard in _cp_one did not fire"
        )
        assert not stale_ssh.exists(), (
            "expected slot_b/home/agora/.ssh to be removed by _cp_one's rmtree guard "
            "(mocked cp does not recreate it)"
        )
        # And the parent home/agora dir must still exist (mkdir was called
        # before the rmtree, on the parent — that's the structural invariant).
        assert (slot_b / "home" / "agora").exists()


# ── SlotStager × fleet-state integration (D60) ─────────────────────────────


class TestSlotStagerFleetStateIntegration:
    """The SlotStager pipeline is the production caller — these tests
    pin the wire-up: missing required fleet-state aborts the apply
    BEFORE trigger_tryboot is invoked, so a half-applied slot B never
    becomes the active slot."""

    def test_missing_required_aborts_before_trigger_tryboot(self, tmp_path):
        tryboot_calls: list[int] = []
        # seed_fleet_state=False ⇒ slot A is empty ⇒ FLEET_STATE_REQUIRED
        # check fails on the first entry.
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=1,
            trigger_tryboot_calls=tryboot_calls,
            seed_fleet_state=False,
        )

        with pytest.raises(FleetStateMissingError) as exc_info:
            stager._stage_sync(_make_payload(), staging_dir)

        # Critical invariant: tryboot must NOT have fired.
        assert tryboot_calls == [], (
            "trigger_tryboot must not run when fleet-state copy aborts; "
            "promoting a slot with no machine-id/cms_config would brick the device"
        )
        # Surface the missing path for the operator-facing telemetry.
        assert exc_info.value.path in FLEET_STATE_REQUIRED


# ── SlotStager mount integration (step 5) ──────────────────────────────────


class TestSlotStagerMountIntegration:
    """End-to-end style: drive ``_stage_sync`` and assert that step 5
    calls mount(8) with the correct argv for the inactive slot's boot
    *and* root partitions, in that order.

    These tests exist because v0.0.7-test bricked the Pi by trusting
    non-existent systemd ``.mount`` units. Step 5 now self-mounts.
    """

    def _capture_runner(self):
        """Return a runner that records mount(8) calls but no-ops them
        (rc=0). Non-mount commands fall through to plain rc=0 so the
        rest of ``_stage_sync`` (e.g. ``sync``) keeps working."""
        recorder = _FakeRunner()
        return recorder

    def test_running_slot_1_mounts_slot_b_partitions(self, tmp_path):
        runner = self._capture_runner()
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=1,
            runner=runner,
        )
        # Empty the mounts file so _is_mountpoint reports False for
        # everything; ensure_partition_mounted must invoke mount(8).
        stager.mounts_path.write_text("")

        stager._stage_sync(_make_payload(), staging_dir)

        mount_calls = [c for c in runner.calls if c[0] == "mount"]
        assert len(mount_calls) >= 2, (
            f"expected at least 2 mount(8) calls (boot + root); got {mount_calls!r}"
        )

        # The first two mounts produced by step 5 are the boot and root
        # of the *inactive* slot (slot B when running_slot=1).
        boot_call, root_call = mount_calls[0], mount_calls[1]
        assert boot_call[1:3] == ["-t", "vfat"], boot_call
        assert "boot-B" in boot_call[5], boot_call
        assert root_call[1:3] == ["-t", "ext4"], root_call
        assert "root-B" in root_call[5], root_call

    def test_running_slot_2_mounts_slot_a_partitions(self, tmp_path):
        runner = self._capture_runner()
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=2,
            runner=runner,
        )
        stager.mounts_path.write_text("")

        stager._stage_sync(_make_payload(), staging_dir)

        mount_calls = [c for c in runner.calls if c[0] == "mount"]
        assert len(mount_calls) >= 2, (
            f"expected at least 2 mount(8) calls (boot + root); got {mount_calls!r}"
        )
        boot_call, root_call = mount_calls[0], mount_calls[1]
        assert "boot-A" in boot_call[5], boot_call
        assert "root-A" in root_call[5], root_call

    def test_already_mounted_short_circuits(self, tmp_path):
        """Defense in depth: if a future agora-os release ever ships
        systemd ``.mount`` units that pre-mount the inactive slot, step
        5 must no-op rather than fight."""
        runner = self._capture_runner()
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path,
            running_slot=1,
            runner=runner,
        )
        # The fixture from _make_stager already populated mounts_path
        # with entries for both slot B's boot + root mountpoints. So
        # ensure_partition_mounted should short-circuit on both.

        stager._stage_sync(_make_payload(), staging_dir)

        mount_calls = [c for c in runner.calls if c[0] == "mount"]
        assert mount_calls == [], (
            f"step 5 must not invoke mount(8) when targets are already "
            f"mountpoints; got {mount_calls!r}"
        )



# -- SlotStager: progress_callback (agora#202) ----------------------------


class TestSlotStagerProgressCallback:
    """The optional ``progress_callback`` kwarg fires once per phase boundary.

    Wired by the service to ``emit_event(STAGE_PROGRESS, ...)``; tests
    here pin the contract that (a) all 7 phases fire in declared order,
    (b) a missing kwarg keeps the legacy code path silent, (c) a
    raising callback does NOT brick the stage. The exhaustive phase
    list lives in :data:`STAGE_PROGRESS_PHASES`.
    """

    def test_phases_emitted_in_declared_order(self, tmp_path):
        """Happy-path stage with a list-collector callback gets all
        7 phase names in :data:`STAGE_PROGRESS_PHASES` order."""
        phases: list[str] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1,
        )
        stager._stage_sync(
            _make_payload(), staging_dir, progress_callback=phases.append,
        )
        assert tuple(phases) == STAGE_PROGRESS_PHASES

    def test_default_no_callback_keeps_legacy_behavior(self, tmp_path):
        """Omitting the kwarg matches pre-#202 behavior — regression guard."""
        tryboot_calls: list[int] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1, trigger_tryboot_calls=tryboot_calls,
        )
        stager._stage_sync(_make_payload(), staging_dir)
        assert tryboot_calls == [2]

    def test_raising_callback_does_not_break_stage(self, tmp_path):
        """A buggy callback that throws on every phase must NOT brick
        an in-flight OTA — STAGE_PROGRESS is advisory. Tryboot still
        fires; cmdline + fstab still rewritten on slot B."""
        tryboot_calls: list[int] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1, trigger_tryboot_calls=tryboot_calls,
        )

        def boom(phase: str) -> None:
            raise RuntimeError(f"crash on phase {phase!r}")

        stager._stage_sync(
            _make_payload(), staging_dir, progress_callback=boom,
        )

        assert tryboot_calls == [2]
        cmdline = (stager.boot_mount_slot_b / "cmdline.txt").read_text()
        assert "root=PARTLABEL=root-B" in cmdline

    def test_async_entrypoint_forwards_callback(self, tmp_path):
        """``SlotStager.stage`` is the async wrapper around ``_stage_sync``;
        the new kwarg must pass through both layers."""
        import asyncio

        phases: list[str] = []
        staging_dir, stager, _pipeline = _setup_stager_with_bundle(
            tmp_path, running_slot=1,
        )
        asyncio.run(
            stager.stage(
                _make_payload(), staging_dir, progress_callback=phases.append,
            )
        )
        assert tuple(phases) == STAGE_PROGRESS_PHASES
