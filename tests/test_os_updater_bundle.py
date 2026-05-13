"""Tests for :mod:`os_updater.bundle` + :mod:`os_updater.verifier`.

Acceptance hooks (plan.md §"Phase 2 — Acceptance"):

* ``Tamper test: hand-modify a single bundle byte; verify the device emits
  failed:signature_invalid`` — exercised by
  :class:`TestVerifySignature.test_all_pubkeys_fail_raises` plus
  :class:`TestServiceFailureClassification.test_signature_invalid_maps_to_short_code`.
* D54 two-pubkey design (primary + recovery) — exercised by
  :class:`TestVerifySignature.test_falls_back_to_recovery_pubkey`.
* Signing-key-rotation foundation (``/etc/agora/update-pubkeys.d/*.pem``)
  — exercised by :class:`TestDiscoverPubkeys.test_search_dir_is_alphabetical_and_first`.

These tests never invoke the real ``minisign`` binary — they pass a
fake :data:`os_updater.bundle.Runner` that returns canned
:class:`subprocess.CompletedProcess` objects. That keeps the suite
fast and CI-portable; a future integration smoke test on the lab Pi
can exercise the real binary end-to-end.
"""

from __future__ import annotations

import asyncio
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import pytest

from os_updater.bundle import (
    BundleError,
    BundleIntegrityError,
    BundleSignatureError,
    discover_pubkeys,
    verify_signature,
)
from os_updater.dispatch import DispatchPayload
from os_updater.verifier import SignatureVerifier


# ── Fake runner ────────────────────────────────────────────────────────────


@dataclass
class _FakeRunner:
    """Records every invocation and returns a canned result.

    ``results_by_pubkey`` maps a pubkey-file basename (e.g.
    ``"update-pubkey.pem"``) to the return code that should come back
    when that pubkey is on the command line. Unmapped pubkeys default
    to ``failing_returncode``.
    """

    results_by_pubkey: dict[str, int] = field(default_factory=dict)
    failing_returncode: int = 1
    raise_file_not_found: bool = False
    raise_timeout: bool = False
    calls: list[Sequence[str]] = field(default_factory=list)

    def __call__(self, args, **kwargs):
        self.calls.append(list(args))
        if self.raise_file_not_found:
            raise FileNotFoundError("minisign")
        if self.raise_timeout:
            raise subprocess.TimeoutExpired(cmd=list(args), timeout=kwargs.get("timeout"))
        # Find the ``-p <pubkey>`` flag.
        pubkey_arg = ""
        for i, a in enumerate(args):
            if a == "-p" and i + 1 < len(args):
                pubkey_arg = Path(args[i + 1]).name
                break
        rc = self.results_by_pubkey.get(pubkey_arg, self.failing_returncode)
        stderr = "" if rc == 0 else "Signature verification failed\n"
        return subprocess.CompletedProcess(
            args=list(args), returncode=rc, stdout="", stderr=stderr
        )


def _write(path: Path, content: bytes = b"x") -> Path:
    path.write_bytes(content)
    return path


# ── discover_pubkeys ───────────────────────────────────────────────────────


class TestDiscoverPubkeys:
    def test_returns_empty_when_nothing_present(self, tmp_path):
        keys = discover_pubkeys(
            search_dir=tmp_path / "missing",
            primary=tmp_path / "missing-primary.pem",
            recovery=tmp_path / "missing-recovery.pem",
        )
        assert keys == []

    def test_returns_only_primary_when_recovery_missing(self, tmp_path):
        primary = _write(tmp_path / "update-pubkey.pem")
        keys = discover_pubkeys(
            search_dir=tmp_path / "search-dir-does-not-exist",
            primary=primary,
            recovery=tmp_path / "recovery-missing.pem",
        )
        assert keys == [primary]

    def test_returns_only_recovery_when_primary_missing(self, tmp_path):
        recovery = _write(tmp_path / "update-pubkey-recovery.pem")
        keys = discover_pubkeys(
            search_dir=tmp_path / "search-dir-does-not-exist",
            primary=tmp_path / "primary-missing.pem",
            recovery=recovery,
        )
        assert keys == [recovery]

    def test_search_dir_is_alphabetical_and_first(self, tmp_path):
        """Rotation-foundation lookup beats both fallbacks, in sorted order."""
        search_dir = tmp_path / "update-pubkeys.d"
        search_dir.mkdir()
        # Intentionally out of file-creation order to prove the sort.
        b_pem = _write(search_dir / "b.pem")
        a_pem = _write(search_dir / "a.pem")
        c_pem = _write(search_dir / "c.pem")
        primary = _write(tmp_path / "update-pubkey.pem")
        recovery = _write(tmp_path / "update-pubkey-recovery.pem")

        keys = discover_pubkeys(
            search_dir=search_dir,
            primary=primary,
            recovery=recovery,
        )
        assert keys == [a_pem, b_pem, c_pem, primary, recovery]

    def test_non_pem_files_in_search_dir_are_ignored(self, tmp_path):
        search_dir = tmp_path / "update-pubkeys.d"
        search_dir.mkdir()
        good = _write(search_dir / "rotated.pem")
        _write(search_dir / "README")
        _write(search_dir / "old-key.pub")  # different extension
        primary = _write(tmp_path / "update-pubkey.pem")

        keys = discover_pubkeys(
            search_dir=search_dir,
            primary=primary,
            recovery=tmp_path / "no-recovery.pem",
        )
        assert keys == [good, primary]


# ── verify_signature ───────────────────────────────────────────────────────


class TestVerifySignature:
    def _setup_bundle(self, tmp_path: Path) -> tuple[Path, Path]:
        bundle = _write(tmp_path / "bundle.tar.zst", b"bundle bytes")
        sig = _write(tmp_path / "bundle.tar.zst.minisig", b"signature bytes")
        return bundle, sig

    def test_primary_pubkey_succeeds_first_try(self, tmp_path):
        bundle, sig = self._setup_bundle(tmp_path)
        primary = _write(tmp_path / "update-pubkey.pem")
        runner = _FakeRunner(results_by_pubkey={"update-pubkey.pem": 0})

        winner = verify_signature(
            bundle_path=bundle,
            signature_path=sig,
            pubkeys=[primary],
            runner=runner,
        )
        assert winner == primary
        assert len(runner.calls) == 1
        # The CLI invocation matches the documented contract.
        assert runner.calls[0][:2] == ["minisign", "-V"]
        assert str(primary) in runner.calls[0]
        assert str(bundle) in runner.calls[0]
        assert str(sig) in runner.calls[0]

    def test_falls_back_to_recovery_pubkey(self, tmp_path):
        """D54: primary fails, recovery wins, primary is tried first."""
        bundle, sig = self._setup_bundle(tmp_path)
        primary = _write(tmp_path / "update-pubkey.pem")
        recovery = _write(tmp_path / "update-pubkey-recovery.pem")
        runner = _FakeRunner(
            results_by_pubkey={
                "update-pubkey.pem": 1,
                "update-pubkey-recovery.pem": 0,
            }
        )

        winner = verify_signature(
            bundle_path=bundle,
            signature_path=sig,
            pubkeys=[primary, recovery],
            runner=runner,
        )
        assert winner == recovery
        assert len(runner.calls) == 2

    def test_stops_after_first_success(self, tmp_path):
        """A third key in the list is never tried if key #1 succeeds."""
        bundle, sig = self._setup_bundle(tmp_path)
        keys = [
            _write(tmp_path / f"key-{i}.pem") for i in range(3)
        ]
        runner = _FakeRunner(results_by_pubkey={"key-0.pem": 0})
        winner = verify_signature(
            bundle_path=bundle, signature_path=sig, pubkeys=keys, runner=runner
        )
        assert winner == keys[0]
        assert len(runner.calls) == 1

    def test_all_pubkeys_fail_raises(self, tmp_path):
        bundle, sig = self._setup_bundle(tmp_path)
        keys = [_write(tmp_path / f"key-{i}.pem") for i in range(3)]
        runner = _FakeRunner(failing_returncode=1)

        with pytest.raises(BundleSignatureError) as excinfo:
            verify_signature(
                bundle_path=bundle, signature_path=sig, pubkeys=keys, runner=runner
            )
        # The last stderr line should be surfaced for ops debugging.
        assert "Signature verification failed" in str(excinfo.value)
        assert len(runner.calls) == 3

    def test_pubkey_listed_but_file_missing_is_skipped(self, tmp_path):
        """A pubkey that vanished between discover and verify doesn't crash.

        We treat it as just another failed attempt and move to the next.
        """
        bundle, sig = self._setup_bundle(tmp_path)
        missing = tmp_path / "missing-key.pem"  # not written
        present = _write(tmp_path / "real-key.pem")
        runner = _FakeRunner(results_by_pubkey={"real-key.pem": 0})

        winner = verify_signature(
            bundle_path=bundle,
            signature_path=sig,
            pubkeys=[missing, present],
            runner=runner,
        )
        assert winner == present
        # Runner is never called for the missing key.
        assert len(runner.calls) == 1

    def test_empty_pubkey_list_raises(self, tmp_path):
        bundle, sig = self._setup_bundle(tmp_path)
        with pytest.raises(BundleSignatureError, match="misconfigured"):
            verify_signature(
                bundle_path=bundle,
                signature_path=sig,
                pubkeys=[],
                runner=_FakeRunner(),
            )

    def test_missing_bundle_raises(self, tmp_path):
        sig = _write(tmp_path / "bundle.tar.zst.minisig")
        primary = _write(tmp_path / "update-pubkey.pem")
        with pytest.raises(BundleSignatureError, match="bundle not found"):
            verify_signature(
                bundle_path=tmp_path / "nope.tar.zst",
                signature_path=sig,
                pubkeys=[primary],
                runner=_FakeRunner(),
            )

    def test_missing_signature_raises(self, tmp_path):
        bundle = _write(tmp_path / "bundle.tar.zst")
        primary = _write(tmp_path / "update-pubkey.pem")
        with pytest.raises(BundleSignatureError, match="signature not found"):
            verify_signature(
                bundle_path=bundle,
                signature_path=tmp_path / "nope.minisig",
                pubkeys=[primary],
                runner=_FakeRunner(),
            )

    def test_missing_minisign_binary_raises_clearly(self, tmp_path):
        bundle, sig = self._setup_bundle(tmp_path)
        primary = _write(tmp_path / "update-pubkey.pem")
        runner = _FakeRunner(raise_file_not_found=True)
        with pytest.raises(BundleSignatureError, match="minisign binary not found"):
            verify_signature(
                bundle_path=bundle,
                signature_path=sig,
                pubkeys=[primary],
                runner=runner,
            )

    def test_timeout_moves_to_next_pubkey(self, tmp_path):
        bundle, sig = self._setup_bundle(tmp_path)
        first = _write(tmp_path / "first.pem")
        second = _write(tmp_path / "second.pem")

        # Timeout twice in a row → no key verifies → BundleSignatureError.
        with pytest.raises(BundleSignatureError, match="timed out"):
            verify_signature(
                bundle_path=bundle,
                signature_path=sig,
                pubkeys=[first, second],
                runner=_FakeRunner(raise_timeout=True),
            )


# ── SignatureVerifier (the async adapter) ──────────────────────────────────


def _ok_payload(**overrides) -> DispatchPayload:
    base = dict(
        release_id="rel_test_1",
        target_version="1.1.0",
        min_from_version="1.0.0",
        bundle_url="https://x/y/bundle.tar.zst",
        signature_url="https://x/y/bundle.tar.zst.minisig",
    )
    base.update(overrides)
    return DispatchPayload(**base)


def _stage(tmp_path: Path) -> Path:
    """Create a staging dir with bundle + signature files written."""
    staging = tmp_path / "staging" / "rel_test_1"
    staging.mkdir(parents=True)
    _write(staging / "bundle.tar.zst", b"bundle bytes")
    _write(staging / "bundle.tar.zst.minisig", b"signature bytes")
    return staging


class TestSignatureVerifier:
    def test_happy_path_delegates_to_verify_signature(self, tmp_path):
        primary = _write(tmp_path / "primary.pem")
        staging = _stage(tmp_path)
        runner = _FakeRunner(results_by_pubkey={"primary.pem": 0})

        v = SignatureVerifier(
            pubkey_search_dir=tmp_path / "no-such-dir",
            primary_pubkey=primary,
            recovery_pubkey=tmp_path / "no-recovery.pem",
            runner=runner,
        )

        # No exception = success. SignatureVerifier.run is async.
        asyncio.run(v.run(_ok_payload(), staging))

        assert len(runner.calls) == 1
        # Bundle + sig paths from the staging dir made it onto the CLI.
        assert str(staging / "bundle.tar.zst") in runner.calls[0]
        assert str(staging / "bundle.tar.zst.minisig") in runner.calls[0]

    def test_all_keys_fail_raises_bundle_signature_error(self, tmp_path):
        primary = _write(tmp_path / "primary.pem")
        recovery = _write(tmp_path / "recovery.pem")
        staging = _stage(tmp_path)
        runner = _FakeRunner(failing_returncode=1)

        v = SignatureVerifier(
            pubkey_search_dir=tmp_path / "no-such-dir",
            primary_pubkey=primary,
            recovery_pubkey=recovery,
            runner=runner,
        )

        with pytest.raises(BundleSignatureError):
            asyncio.run(v.run(_ok_payload(), staging))

        # Both keys were tried before giving up.
        assert len(runner.calls) == 2

    def test_no_pubkeys_anywhere_raises(self, tmp_path):
        """Misconfigured rootfs (no primary, no recovery, empty search dir)."""
        staging = _stage(tmp_path)
        v = SignatureVerifier(
            pubkey_search_dir=tmp_path / "empty-dir",
            primary_pubkey=tmp_path / "no-primary.pem",
            recovery_pubkey=tmp_path / "no-recovery.pem",
            runner=_FakeRunner(),
        )
        with pytest.raises(BundleSignatureError, match="misconfigured"):
            asyncio.run(v.run(_ok_payload(), staging))

    def test_rotation_dir_keys_are_tried_first(self, tmp_path):
        """A key in update-pubkeys.d/ wins ahead of primary."""
        rotation = tmp_path / "update-pubkeys.d"
        rotation.mkdir()
        rotated = _write(rotation / "new-key.pem")
        primary = _write(tmp_path / "primary.pem")
        staging = _stage(tmp_path)

        runner = _FakeRunner(results_by_pubkey={"new-key.pem": 0})
        v = SignatureVerifier(
            pubkey_search_dir=rotation,
            primary_pubkey=primary,
            recovery_pubkey=tmp_path / "no-recovery.pem",
            runner=runner,
        )

        asyncio.run(v.run(_ok_payload(), staging))
        # Verified on the first call — primary was never even tried.
        assert len(runner.calls) == 1
        assert str(rotated) in runner.calls[0]


# ── _classify_failure short-codes ──────────────────────────────────────────


class TestServiceFailureClassification:
    """The service maps typed bundle exceptions onto stable wire codes.

    Pulled into this file because the assertions read most naturally
    next to the exception types they're testing.
    """

    def test_signature_invalid_maps_to_short_code(self):
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(BundleSignatureError("bad sig"))
            == "signature_invalid"
        )

    def test_integrity_invalid_maps_to_short_code(self):
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(BundleIntegrityError("sha mismatch"))
            == "bundle_invalid"
        )

    def test_bundle_error_base_falls_back_to_typename(self):
        """Base BundleError is unmapped — only the concrete subclasses
        get pinned codes. A bare BundleError shouldn't normally be
        raised in production code, so the generic fallback is fine.
        """
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(BundleError("unknown"))
            == "error_BundleError"
        )

    def test_unrelated_exception_falls_back_to_typename(self):
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(RuntimeError("boom"))
            == "error_RuntimeError"
        )

    def test_rsync_error_maps_to_stage_rsync_failed(self):
        from os_updater.apply import RsyncError
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(RsyncError("rsync rc=23"))
            == "stage_rsync_failed"
        )

    def test_tryboot_error_maps_to_tryboot_failed(self):
        from os_updater.apply import TrybootError
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(TrybootError("pinned"))
            == "tryboot_failed"
        )

    def test_staging_error_base_maps_to_stage_failed(self):
        """Bare ``StagingError`` (not a subclass) is the catch-all for
        stage-time failures that aren't rsync or tryboot — e.g.
        missing mountpoint, slot detection failed.
        """
        from os_updater.apply import StagingError
        from os_updater.service import OSUpdaterService

        assert (
            OSUpdaterService._classify_failure(StagingError("mount missing"))
            == "stage_failed"
        )

    def test_subclass_arms_take_precedence_over_base(self):
        """``RsyncError`` and ``TrybootError`` both subclass
        ``StagingError``. If the arm order ever drifted so the base
        arm fired first, all three would collapse to ``stage_failed``
        and the CMS would lose the wire-code distinction. Pin it.
        """
        from os_updater.apply import RsyncError, StagingError, TrybootError
        from os_updater.service import OSUpdaterService

        assert issubclass(RsyncError, StagingError)
        assert issubclass(TrybootError, StagingError)
        assert OSUpdaterService._classify_failure(RsyncError("a")) == "stage_rsync_failed"
        assert OSUpdaterService._classify_failure(TrybootError("b")) == "tryboot_failed"


# ── parse_bundle_meta ──────────────────────────────────────────────────────


def _valid_meta_dict() -> dict:
    """Return a freshly-constructed valid meta.json dict for happy-path tests."""
    return {
        "version": "1.1.0",
        "min_from_version": "1.0.0",
        "schema_version": 2,
        "sha256_manifest": {
            "boot/cmdline.txt": "a" * 64,
            "root/etc/os-release": "b" * 64,
        },
        "created_at": "2026-04-22T00:00:00Z",
        "builder": "github-actions",
    }


class TestParseBundleMeta:
    def test_happy_path_returns_bundlemeta(self, tmp_path):
        import json

        from os_updater.bundle import BundleMeta, parse_bundle_meta

        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(_valid_meta_dict()))
        meta = parse_bundle_meta(meta_path)
        assert isinstance(meta, BundleMeta)
        assert meta.version == "1.1.0"
        assert meta.min_from_version == "1.0.0"
        assert meta.schema_version == 2
        assert meta.created_at == "2026-04-22T00:00:00Z"
        assert meta.builder == "github-actions"
        assert dict(meta.sha256_manifest) == {
            "boot/cmdline.txt": "a" * 64,
            "root/etc/os-release": "b" * 64,
        }

    def test_missing_file_raises(self, tmp_path):
        from os_updater.bundle import parse_bundle_meta

        with pytest.raises(BundleIntegrityError, match="meta.json missing"):
            parse_bundle_meta(tmp_path / "meta.json")

    def test_invalid_json_raises(self, tmp_path):
        from os_updater.bundle import parse_bundle_meta

        meta_path = tmp_path / "meta.json"
        meta_path.write_text("{not valid json")
        with pytest.raises(BundleIntegrityError, match="not valid JSON"):
            parse_bundle_meta(meta_path)

    def test_non_object_root_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(["not", "an", "object"]))
        with pytest.raises(BundleIntegrityError, match="must be a JSON object"):
            parse_bundle_meta(meta_path)

    @pytest.mark.parametrize(
        "key",
        ["version", "min_from_version", "schema_version", "sha256_manifest",
         "created_at", "builder"],
    )
    def test_missing_required_key_raises(self, tmp_path, key):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        del d[key]
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="missing required keys"):
            parse_bundle_meta(meta_path)

    def test_wrong_type_for_version_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["version"] = 110  # int, must be str
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="version.*must be string"):
            parse_bundle_meta(meta_path)

    def test_wrong_type_for_schema_version_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["schema_version"] = "2"  # str, must be int
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="schema_version.*must be int"):
            parse_bundle_meta(meta_path)

    def test_bool_schema_version_rejected(self, tmp_path):
        """``True`` is technically an ``int`` in Python. Make sure the
        guard catches that — a JSON ``true`` in ``schema_version`` is
        almost certainly a builder bug, not a real version.
        """
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["schema_version"] = True
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="schema_version.*must be int"):
            parse_bundle_meta(meta_path)

    def test_manifest_not_object_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["sha256_manifest"] = ["not", "an", "object"]
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="sha256_manifest.*must be object"):
            parse_bundle_meta(meta_path)

    def test_manifest_non_string_digest_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["sha256_manifest"] = {"boot/cmdline.txt": 12345}
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="must be a hex string"):
            parse_bundle_meta(meta_path)

    def test_manifest_non_hex_digest_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["sha256_manifest"] = {"boot/cmdline.txt": "Z" * 64}
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="64-char lowercase hex"):
            parse_bundle_meta(meta_path)

    def test_manifest_wrong_length_digest_raises(self, tmp_path):
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["sha256_manifest"] = {"boot/cmdline.txt": "a" * 32}  # too short
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        with pytest.raises(BundleIntegrityError, match="64-char lowercase hex"):
            parse_bundle_meta(meta_path)

    def test_unknown_keys_ignored(self, tmp_path):
        """Forward-compat: newer builders may add fields."""
        import json

        from os_updater.bundle import parse_bundle_meta

        d = _valid_meta_dict()
        d["future_field"] = "ignored"
        d["another_one"] = [1, 2, 3]
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(d))
        meta = parse_bundle_meta(meta_path)
        assert meta.version == "1.1.0"  # parsed successfully


# ── verify_bundle_manifest ─────────────────────────────────────────────────


def _build_extracted_bundle(
    tmp_path: Path,
    files: dict[str, bytes] | None = None,
) -> Path:
    """Build a minimal extracted bundle tree with boot/ and root/ subdirs.

    ``files`` maps bundle-relative paths (e.g. ``"boot/cmdline.txt"``)
    to raw bytes. Defaults to one tiny file per subdir.
    """
    root = tmp_path / "unpacked"
    (root / "boot").mkdir(parents=True)
    (root / "root").mkdir(parents=True)
    if files is None:
        files = {"boot/cmdline.txt": b"console=tty1", "root/etc/os-release": b"name=Agora"}
    for relpath, content in files.items():
        target = root / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return root


def _meta_for_files(files: dict[str, bytes]) -> "object":
    """Build a real :class:`BundleMeta` with sha256 of each ``files`` entry."""
    import hashlib

    from os_updater.bundle import BundleMeta

    manifest = {
        relpath: hashlib.sha256(content).hexdigest()
        for relpath, content in files.items()
    }
    return BundleMeta(
        version="1.1.0",
        min_from_version="1.0.0",
        schema_version=2,
        sha256_manifest=manifest,
        created_at="2026-04-22T00:00:00Z",
        builder="test",
    )


class TestVerifyBundleManifest:
    def test_happy_path_real_hashes(self, tmp_path):
        """End-to-end with the real ``_sha256_hex`` — proves the streaming
        hash function and the walk both work."""
        from os_updater.bundle import verify_bundle_manifest

        files = {
            "boot/cmdline.txt": b"console=tty1 root=PARTLABEL=root-A",
            "root/etc/os-release": b'NAME="Agora OS"\nVERSION="1.1.0"\n',
        }
        root = _build_extracted_bundle(tmp_path, files)
        meta = _meta_for_files(files)
        verify_bundle_manifest(root, meta)  # raises on failure

    def test_missing_extracted_root_raises(self, tmp_path):
        from os_updater.bundle import verify_bundle_manifest

        files = {"boot/cmdline.txt": b"x"}
        meta = _meta_for_files(files)
        with pytest.raises(BundleIntegrityError, match="extracted-bundle root"):
            verify_bundle_manifest(tmp_path / "does-not-exist", meta)

    def test_extracted_root_is_file_not_dir_raises(self, tmp_path):
        from os_updater.bundle import verify_bundle_manifest

        target = tmp_path / "not-a-dir"
        target.write_bytes(b"file content")
        meta = _meta_for_files({"boot/cmdline.txt": b"x"})
        with pytest.raises(BundleIntegrityError, match="not a directory"):
            verify_bundle_manifest(target, meta)

    def test_missing_required_subdir_raises(self, tmp_path):
        from os_updater.bundle import verify_bundle_manifest

        root = tmp_path / "unpacked"
        root.mkdir()
        (root / "boot").mkdir()
        # No root/ subdir.
        meta = _meta_for_files({"boot/cmdline.txt": b"x"})
        # boot/cmdline.txt isn't on disk either, so we expect either the
        # missing-subdir message or the missing-file message — but the
        # subdir guard runs first.
        with pytest.raises(BundleIntegrityError, match="missing required top-level dir"):
            verify_bundle_manifest(root, meta)

    def test_missing_file_on_disk_raises(self, tmp_path):
        """Manifest names a file the extracted tree doesn't contain."""
        from os_updater.bundle import verify_bundle_manifest

        # Build the tree with boot/cmdline.txt but the manifest lists two.
        files_on_disk = {"boot/cmdline.txt": b"x", "root/foo": b"y"}
        root = _build_extracted_bundle(tmp_path, files_on_disk)
        meta = _meta_for_files(
            {**files_on_disk, "boot/ssh-keys.tar": b"z"}  # extra in manifest
        )
        with pytest.raises(BundleIntegrityError, match="missing on disk"):
            verify_bundle_manifest(root, meta)

    def test_extra_file_on_disk_raises(self, tmp_path):
        """File exists on disk but isn't named in the manifest."""
        from os_updater.bundle import verify_bundle_manifest

        files_on_disk = {
            "boot/cmdline.txt": b"x",
            "root/foo": b"y",
            "root/sneaky": b"surprise",
        }
        root = _build_extracted_bundle(tmp_path, files_on_disk)
        meta = _meta_for_files(
            {"boot/cmdline.txt": b"x", "root/foo": b"y"}  # manifest doesn't list sneaky
        )
        with pytest.raises(BundleIntegrityError, match="not listed in sha256_manifest"):
            verify_bundle_manifest(root, meta)

    def test_hash_mismatch_raises(self, tmp_path):
        """File exists but its bytes were tampered after manifest was built."""
        from os_updater.bundle import verify_bundle_manifest

        files = {"boot/cmdline.txt": b"original content"}
        root = _build_extracted_bundle(tmp_path, files)
        meta = _meta_for_files(files)
        # Now corrupt the file on disk without updating meta.
        (root / "boot/cmdline.txt").write_bytes(b"tampered content")
        with pytest.raises(BundleIntegrityError, match="sha256 mismatch"):
            verify_bundle_manifest(root, meta)

    def test_symlinks_are_skipped(self, tmp_path):
        """Symlinks aren't expected in bundle manifest; walker skips them.

        Per bundle-format.md §"Manifest scope: regular files only".
        On Windows symlinks need admin/dev-mode; gate on capability.
        """
        from os_updater.bundle import verify_bundle_manifest

        files = {"boot/cmdline.txt": b"x", "root/etc/os-release": b"y"}
        root = _build_extracted_bundle(tmp_path, files)
        meta = _meta_for_files(files)
        # Try to create a symlink; skip the test if the platform refuses.
        link_path = root / "root/etc/os-release-link"
        try:
            link_path.symlink_to(root / "root/etc/os-release")
        except (OSError, NotImplementedError):
            pytest.skip("platform doesn't allow symlink creation in this context")
        # Should not raise — symlink is skipped, not flagged as extra.
        verify_bundle_manifest(root, meta)

    def test_directories_are_skipped(self, tmp_path):
        """Empty subdirs in the tree aren't files; walker shouldn't flag them."""
        from os_updater.bundle import verify_bundle_manifest

        files = {"boot/cmdline.txt": b"x", "root/etc/os-release": b"y"}
        root = _build_extracted_bundle(tmp_path, files)
        # Add empty dirs.
        (root / "root/empty-dir").mkdir()
        (root / "boot/another-empty").mkdir()
        meta = _meta_for_files(files)
        verify_bundle_manifest(root, meta)  # raises if empty dirs leaked

    def test_injected_sha256_fn_is_used(self, tmp_path):
        """Verify the ``sha256_fn`` seam: tests stub it so multi-GB bundle
        walks don't actually hash files. Proves the seam is wired."""
        from os_updater.bundle import verify_bundle_manifest

        files = {"boot/cmdline.txt": b"any", "root/etc/os-release": b"thing"}
        root = _build_extracted_bundle(tmp_path, files)
        meta = _meta_for_files(files)
        # Replace the meta with a synthetic manifest using a fake hash, then
        # stub sha256_fn to return that same fake hash for every file.
        # We reuse _meta_for_files but override its manifest values.
        from os_updater.bundle import BundleMeta

        fake_meta = BundleMeta(
            version=meta.version,
            min_from_version=meta.min_from_version,
            schema_version=meta.schema_version,
            sha256_manifest={k: "f" * 64 for k in meta.sha256_manifest},
            created_at=meta.created_at,
            builder=meta.builder,
        )
        calls = []

        def fake_sha256(path):
            calls.append(path)
            return "f" * 64

        verify_bundle_manifest(root, fake_meta, sha256_fn=fake_sha256)
        # Exactly 2 regular files → 2 calls.
        assert len(calls) == 2
