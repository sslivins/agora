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
