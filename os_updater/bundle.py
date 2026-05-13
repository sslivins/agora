"""Bundle format + integrity primitives.

This module owns the format-level concerns documented in
``docs/bundle-format.md``:

* The typed exception hierarchy the service maps onto its
  ``failed:<reason>`` wire codes.
* :func:`discover_pubkeys` — the directory-lookup-plus-fallback resolver
  for the rootfs-baked minisign pubkeys (D54: primary + recovery, plus
  the ``/etc/agora/update-pubkeys.d/*.pem`` rotation foundation called
  out in plan.md §"Phase 2 — Deliverables").
* :func:`verify_signature` — shells out to the ``minisign`` CLI through
  an injectable :data:`Runner` seam, trying each candidate pubkey in
  order until one verifies (or all fail).

The sha256-manifest verification half lives in
:func:`verify_bundle_manifest` and remains a stub until the
``p2-stage-and-tryboot`` sibling lands — that's the PR where the
tarball gets extracted into the staging directory, which is the
prerequisite for walking the unpacked tree.

The :class:`SignatureVerifier` adapter in :mod:`os_updater.verifier`
implements the ``Verifier`` protocol from :mod:`os_updater.service` and
wires :func:`verify_signature` into the FSM between ``DOWNLOADING`` and
``STAGED``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Callable, Iterable, Sequence


# ── Exception hierarchy ────────────────────────────────────────────────────


class BundleError(Exception):
    """Base class for bundle-related failures."""


class BundleSignatureError(BundleError):
    """No baked-in pubkey verified the bundle's minisign signature.

    Service maps this to ``failed:signature_invalid`` (see
    :meth:`os_updater.service.OSUpdaterService._classify_failure`).
    """


class BundleIntegrityError(BundleError):
    """sha256 manifest mismatch or missing file in the unpacked bundle.

    Service maps this to ``failed:bundle_invalid``.
    """


# ── Subprocess seam ────────────────────────────────────────────────────────

#: Callable type matching :func:`subprocess.run`. Tests pass a fake that
#: returns a canned :class:`subprocess.CompletedProcess`. Mirrors the
#: convention from :mod:`precheck.core`.
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def _default_runner(
    args: Sequence[str],
    *,
    check: bool = False,
    capture_output: bool = True,
    text: bool = True,
    timeout: float | None = None,
) -> "subprocess.CompletedProcess[str]":
    """Thin wrapper around :func:`subprocess.run` with our defaults applied."""
    return subprocess.run(
        list(args),
        check=check,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )


#: Default per-call timeout for ``minisign -V``. The binary is fast — a
#: failure here means the OS scheduler is stuck or the binary is
#: missing, not that signature math is slow.
DEFAULT_MINISIGN_TIMEOUT_S = 30.0


# ── Pubkey discovery ───────────────────────────────────────────────────────


#: Default location for the rotation-foundation pubkey directory
#: (plan.md §"Signing-key rotation foundation"). A future release can
#: drop additional ``*.pem`` files here without rebuilding the verify
#: code path.
DEFAULT_PUBKEY_SEARCH_DIR = Path("/etc/agora/update-pubkeys.d")

#: D54 primary pubkey, hot key custody (GH Actions secret + 1Password +
#: paper backup).
DEFAULT_PRIMARY_PUBKEY = Path("/etc/agora/update-pubkey.pem")

#: D54 recovery pubkey, deep-cold-storage custody (paper backup in safe
#: + encrypted alternate-location copy). Used iff the primary is ever
#: compromised; the verify path accepts it transparently.
DEFAULT_RECOVERY_PUBKEY = Path("/etc/agora/update-pubkey-recovery.pem")


def discover_pubkeys(
    *,
    search_dir: Path = DEFAULT_PUBKEY_SEARCH_DIR,
    primary: Path = DEFAULT_PRIMARY_PUBKEY,
    recovery: Path = DEFAULT_RECOVERY_PUBKEY,
) -> list[Path]:
    """Resolve the ordered list of pubkeys to attempt during verification.

    Order:

    1. Every ``*.pem`` under ``search_dir``, sorted alphabetically. This
       is the rotation-foundation slot — a future release adds a new
       key by dropping a file here, then the next release ships a
       bundle signed with that key. The alphabetical sort makes the
       order deterministic + auditable.
    2. ``primary`` (D54 hot key) if it exists on disk.
    3. ``recovery`` (D54 cold key) if it exists on disk.

    Missing search-dir and missing fallback files are both silently
    skipped — a freshly-built rootfs will not have populated
    ``search_dir`` (it ships empty) but will always have ``primary``
    and ``recovery``, so the list is non-empty in practice. A
    completely empty result is the caller's signal that the rootfs is
    misconfigured; callers should treat that as a
    :class:`BundleSignatureError` rather than crashing.

    The same ``Path`` is not deduplicated across the three sources —
    if for some reason an operator copies ``update-pubkey.pem`` into
    ``update-pubkeys.d/``, the verifier will simply try it twice. That
    costs one extra ``minisign -V`` invocation; it doesn't break
    anything.
    """
    keys: list[Path] = []
    if search_dir.is_dir():
        keys.extend(sorted(search_dir.glob("*.pem")))
    for fallback in (primary, recovery):
        if fallback.is_file():
            keys.append(fallback)
    return keys


# ── Signature verification ─────────────────────────────────────────────────


def verify_signature(
    *,
    bundle_path: Path,
    signature_path: Path,
    pubkeys: Iterable[Path],
    runner: Runner = _default_runner,
    timeout_s: float = DEFAULT_MINISIGN_TIMEOUT_S,
) -> Path:
    """Verify a bundle's minisign signature using each pubkey in order.

    Invokes ``minisign -V -p <pubkey> -x <sig> -m <bundle>`` once per
    candidate pubkey. The first invocation with a zero return code
    wins, and that pubkey's :class:`Path` is returned so the caller
    (and lifecycle event payload) can record which key verified.

    Raises :class:`BundleSignatureError` if every pubkey fails. The
    last stderr is preserved in the exception message to aid
    debugging — minisign's stderr is typically a single line like
    ``Signature verification failed``.

    Why subprocess instead of a Python library: ``minisign`` already
    ships on the rootfs (it's the same binary used by the CI signing
    workflow), and the codebase's convention for testable external
    binaries is the injectable :data:`Runner` seam already established
    in :mod:`precheck.core`. Avoiding a new pip dep also keeps the
    rootfs build small.

    Parameters
    ----------
    bundle_path
        The signed artifact (typically the zstd-compressed tarball at
        ``staging_dir / "bundle.tar.zst"``).
    signature_path
        The detached ``.minisig`` next to it.
    pubkeys
        Ordered iterable of candidate pubkey paths. Typically the
        output of :func:`discover_pubkeys`.
    runner
        Injection seam matching :func:`subprocess.run`. Tests pass a
        fake that returns canned :class:`subprocess.CompletedProcess`
        objects without invoking the real binary.
    timeout_s
        Per-invocation timeout for the ``minisign`` subprocess. A
        timeout produces the same failure path as a non-zero return
        code (move on to the next pubkey).
    """
    bundle_path = Path(bundle_path)
    signature_path = Path(signature_path)
    if not bundle_path.is_file():
        raise BundleSignatureError(f"bundle not found at {bundle_path}")
    if not signature_path.is_file():
        raise BundleSignatureError(f"signature not found at {signature_path}")

    pubkey_list = list(pubkeys)
    if not pubkey_list:
        raise BundleSignatureError(
            "no pubkeys to attempt; rootfs is misconfigured "
            "(see /etc/agora/update-pubkey*.pem)"
        )

    last_detail = ""
    for pubkey in pubkey_list:
        if not pubkey.is_file():
            last_detail = f"pubkey {pubkey} not present on disk"
            continue
        args = [
            "minisign",
            "-V",
            "-p",
            str(pubkey),
            "-x",
            str(signature_path),
            "-m",
            str(bundle_path),
        ]
        try:
            result = runner(
                args,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        except FileNotFoundError as exc:  # minisign binary missing
            raise BundleSignatureError(
                f"minisign binary not found on PATH: {exc}"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            last_detail = f"timed out after {timeout_s}s verifying against {pubkey}"
            continue
        if result.returncode == 0:
            return pubkey
        stderr = (result.stderr or "").strip()
        last_detail = (
            f"pubkey {pubkey} rejected the signature "
            f"(rc={result.returncode}, stderr={stderr!r})"
        )

    raise BundleSignatureError(
        f"no pubkey verified the signature; last attempt: {last_detail}"
    )


# ── Manifest verification (stub — filled in by p2-stage-and-tryboot) ───────


def parse_bundle_meta(*args, **kwargs):  # noqa: D401 — stub
    """Parse ``meta.json`` from an extracted bundle.

    Implemented by sibling todo ``p2-stage-and-tryboot`` — that's the
    PR where the tarball gets extracted into the staging directory
    (the verifier in this PR only handles the *signed-blob* half).
    """
    raise NotImplementedError("see sibling todo p2-stage-and-tryboot")


def verify_bundle_manifest(*args, **kwargs):  # noqa: D401 — stub
    """Verify each extracted file's sha256 against ``meta.json``.

    Implemented by sibling todo ``p2-stage-and-tryboot`` for the same
    reason as :func:`parse_bundle_meta`.
    """
    raise NotImplementedError("see sibling todo p2-stage-and-tryboot")
