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
* :func:`parse_bundle_meta` — parses the ``meta.json`` shipped at the
  top of the extracted tarball into a typed :class:`BundleMeta`.
* :func:`verify_bundle_manifest` — walks the extracted tarball and
  asserts that every regular file under ``boot/`` and ``root/`` matches
  the sha256 recorded in ``meta.sha256_manifest``.

The :class:`SignatureVerifier` adapter in :mod:`os_updater.verifier`
implements the ``Verifier`` protocol from :mod:`os_updater.service` and
wires :func:`verify_signature` into the FSM between ``DOWNLOADING`` and
``STAGED``. The :class:`SlotStager` adapter in :mod:`os_updater.apply`
extends the chain to ``TRYBOOT_PENDING`` and consumes both
:func:`parse_bundle_meta` and :func:`verify_bundle_manifest`.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence


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


# ── Manifest parsing + verification ─────────────────────────────────────────


#: Required keys in ``meta.json``. Unknown keys are silently ignored
#: (forward-compat per docs/bundle-format.md §"Forward compatibility").
_REQUIRED_META_KEYS: tuple[str, ...] = (
    "version",
    "min_from_version",
    "schema_version",
    "sha256_manifest",
    "created_at",
    "builder",
)

#: Buffer size for streaming sha256 of bundle files. 1 MiB strikes a
#: balance between memory pressure and per-call hashlib overhead.
_HASH_CHUNK_BYTES: int = 1 << 20


@dataclass(frozen=True)
class BundleMeta:
    """Parsed ``meta.json`` from an extracted bundle.

    ``sha256_manifest`` is a mapping of *bundle-relative* posix paths
    (e.g. ``"boot/cmdline.txt"``, ``"root/etc/os-release"``) to the
    lowercase hex sha256 of each file's bytes.  ``meta.json`` is NOT in
    its own manifest (self-referential); its integrity is covered by
    the detached minisign signature on the outer ``bundle.tar.zst``.
    """

    version: str
    min_from_version: str
    schema_version: int
    sha256_manifest: Mapping[str, str]
    created_at: str
    builder: str


def parse_bundle_meta(meta_path: Path) -> BundleMeta:
    """Parse ``meta.json`` into a typed :class:`BundleMeta`.

    Raises :class:`BundleIntegrityError` (which the service maps to
    ``failed:bundle_invalid``) on any of:

    * ``meta_path`` missing.
    * JSON parse failure.
    * Top-level value not a JSON object.
    * Any of :data:`_REQUIRED_META_KEYS` missing.
    * Field type mismatch (``version``/``min_from_version``/``created_at``/
      ``builder`` must be ``str``; ``schema_version`` must be ``int``;
      ``sha256_manifest`` must be ``dict[str, str]``).
    * ``sha256_manifest`` contains a value that isn't a 64-char lowercase
      hex string (basic shape check; full content comparison happens in
      :func:`verify_bundle_manifest`).

    Unknown keys are accepted and silently ignored — bundles produced
    by a newer builder can ship extra metadata without bricking older
    devices.
    """
    meta_path = Path(meta_path)
    try:
        raw = meta_path.read_bytes()
    except FileNotFoundError as exc:
        raise BundleIntegrityError(
            f"meta.json missing from extracted bundle at {meta_path}"
        ) from exc
    except OSError as exc:
        raise BundleIntegrityError(
            f"failed reading meta.json at {meta_path}: {exc}"
        ) from exc

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BundleIntegrityError(
            f"meta.json at {meta_path} is not valid JSON: {exc}"
        ) from exc

    if not isinstance(decoded, dict):
        raise BundleIntegrityError(
            f"meta.json at {meta_path} must be a JSON object, got {type(decoded).__name__}"
        )

    missing = [k for k in _REQUIRED_META_KEYS if k not in decoded]
    if missing:
        raise BundleIntegrityError(
            f"meta.json at {meta_path} missing required keys: {missing!r}"
        )

    def _require(key: str, expected_type: type, type_label: str):
        value = decoded[key]
        if not isinstance(value, expected_type) or (
            expected_type is int and isinstance(value, bool)
        ):
            raise BundleIntegrityError(
                f"meta.json field {key!r} must be {type_label}, got {type(value).__name__}"
            )
        return value

    version = _require("version", str, "string")
    min_from_version = _require("min_from_version", str, "string")
    schema_version = _require("schema_version", int, "int")
    created_at = _require("created_at", str, "string")
    builder = _require("builder", str, "string")
    manifest_raw = _require("sha256_manifest", dict, "object")

    sha256_manifest: dict[str, str] = {}
    for relpath, digest in manifest_raw.items():
        if not isinstance(relpath, str):
            raise BundleIntegrityError(
                f"meta.json sha256_manifest contains non-string key: {relpath!r}"
            )
        if not isinstance(digest, str):
            raise BundleIntegrityError(
                f"meta.json sha256_manifest entry {relpath!r} must be a hex string, "
                f"got {type(digest).__name__}"
            )
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise BundleIntegrityError(
                f"meta.json sha256_manifest entry {relpath!r} is not a 64-char "
                f"lowercase hex sha256: {digest!r}"
            )
        sha256_manifest[relpath] = digest

    return BundleMeta(
        version=version,
        min_from_version=min_from_version,
        schema_version=schema_version,
        sha256_manifest=sha256_manifest,
        created_at=created_at,
        builder=builder,
    )


def _sha256_hex(path: Path, chunk_bytes: int = _HASH_CHUNK_BYTES) -> str:
    """Return the lowercase-hex sha256 of ``path``'s bytes, streamed.

    Reads in :data:`_HASH_CHUNK_BYTES` chunks so a 200 MB kernel image
    doesn't load entirely into RAM on the Pi 5.
    """
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            buf = fh.read(chunk_bytes)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def verify_bundle_manifest(
    extracted_root: Path,
    meta: BundleMeta,
    *,
    sha256_fn: Callable[[Path], str] | None = None,
) -> None:
    """Hash every regular file under ``extracted_root`` and compare to ``meta``.

    Walks ``extracted_root/boot`` and ``extracted_root/root`` (the two
    directories the manifest covers per docs/bundle-format.md §"Bundle
    layout") and, for each regular file, computes its sha256 and
    asserts the entry in :attr:`BundleMeta.sha256_manifest` matches.

    Symbolic links, directories, FIFOs, sockets, and device nodes are
    skipped per docs/bundle-format.md §"Manifest scope: regular files
    only".  ``meta.json`` itself is excluded from the manifest (covered
    by the outer minisign signature) and is therefore not expected to
    appear in the walk's results.

    Raises :class:`BundleIntegrityError` (mapped to ``failed:bundle_invalid``)
    on any of:

    * ``extracted_root`` doesn't exist or isn't a directory.
    * A path listed in the manifest is missing on disk.
    * A regular file exists on disk but is absent from the manifest
      (extra file — could indicate tampering).
    * A file's computed sha256 doesn't match the manifest entry.

    Parameters
    ----------
    sha256_fn:
        Injection seam for tests — defaults to :func:`_sha256_hex`.
        Receives a :class:`Path` and returns lowercase hex.
    """
    sha256_fn = sha256_fn or _sha256_hex
    root = Path(extracted_root)
    if not root.is_dir():
        raise BundleIntegrityError(
            f"extracted-bundle root missing or not a directory: {root}"
        )

    expected: dict[str, str] = dict(meta.sha256_manifest)
    seen: set[str] = set()

    for subdir in ("boot", "root"):
        subdir_path = root / subdir
        if not subdir_path.is_dir():
            raise BundleIntegrityError(
                f"extracted bundle missing required top-level dir: {subdir!r}"
            )

        for entry in sorted(subdir_path.rglob("*")):
            if entry.is_symlink() or not entry.is_file():
                continue

            relpath = entry.relative_to(root).as_posix()
            seen.add(relpath)

            digest_expected = expected.get(relpath)
            if digest_expected is None:
                raise BundleIntegrityError(
                    f"extra file on disk not listed in sha256_manifest: {relpath!r}"
                )

            digest_actual = sha256_fn(entry)
            if digest_actual != digest_expected:
                raise BundleIntegrityError(
                    f"sha256 mismatch for {relpath!r}: "
                    f"expected={digest_expected} actual={digest_actual}"
                )

    missing_on_disk = sorted(set(expected) - seen)
    if missing_on_disk:
        raise BundleIntegrityError(
            f"files listed in sha256_manifest are missing on disk: {missing_on_disk!r}"
        )
