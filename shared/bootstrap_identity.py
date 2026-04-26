"""Device-side crypto + secret-file primitives for the HTTPS bootstrap flow.

This is the firmware complement to ``cms.services.device_identity`` in the
CMS repo (umbrella issue #420).  All functions here are pure or touch only
the two on-disk secret files (``device_key`` and ``pairing_secret``); HTTP
wiring lives in ``cms_client.bootstrap_client`` (stage B.2) and service
orchestration lives in ``cms_client.service`` (stage B.3).

Wire-format invariants that MUST stay in lockstep with CMS:

- Device identity is a raw 32-byte Ed25519 **seed** (not PKCS8, not the
  expanded 64-byte form).  CMS ``_ed25519_priv_to_x25519`` and the firmware
  here both take the seed directly.
- Public key on the wire is standard base64 (``+/`` alphabet, ``=`` padding)
  of the 32-byte Ed25519 public key.
- ``/connect-token`` signing input is ``f"{device_id}|{timestamp}|{nonce}"``
  UTF-8; ``timestamp`` stringified via ``str(int(timestamp))`` so device
  and CMS produce bit-identical bytes regardless of JSON int vs str shape.
- Fleet-HMAC input is ``"register|{device_id}|{pubkey_b64}|{pairing_secret_hash}|{fleet_id}|{timestamp}|{nonce}"``
  UTF-8, HMAC-SHA256 under the fleet secret, hex-encoded.
- ECIES wire format is base64 of
  ``eph_x25519_pubkey(32) || aesgcm_nonce(12) || ciphertext || tag(16)``,
  HKDF-SHA256 with salt=None and info=``agora-bootstrap-ecies-v1``.
- Pairing secret is 8 chars of Crockford base32 (uppercase, no I/L/O/U),
  hashed as ``sha256(secret.encode("utf-8")).hexdigest()`` — the admin in
  CMS types/pastes that exact string into the adopt modal (formatted
  ``XXXX-XXXX`` on-screen for readability; the adopt UI normalizes
  hyphens, whitespace, case, and the I/L→1, O→0 substitutions).

File-system contract (both files):

- Parent dir must be a directory owned by the current uid, not group- or
  world-writable.  Perms are repaired to ``0o700`` if same owner + too
  permissive; mismatched owner is a hard error.
- Secret file is a regular file (no symlinks, FIFOs, devices) opened with
  ``O_NOFOLLOW``; must be owned by the current uid; perms repaired to
  ``0o400`` if same owner + too permissive; mismatched owner is a hard error.
- **Malformed contents never trigger silent regeneration.**  That would
  strand an already-adopted device whose seed briefly looked corrupt.  The
  caller sees ``BootstrapKeyFileError`` / ``BootstrapSecretFileError`` and
  has to intervene (delete the file explicitly, or factory-reset).
- Writes are fd-based with ``O_CREAT | O_EXCL | O_NOFOLLOW``, fsynced, then
  the parent directory is fsynced — power-loss-safe on first boot.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

PAIRING_SECRET_LEN_BYTES = 5  # 40 bits of entropy → 8 chars Crockford
PAIRING_SECRET_TEXT_LEN = 8  # 8 Crockford-base32 chars

_SEED_LEN = 32
_FILE_MODE = 0o400
_DIR_MODE = 0o700
_ECIES_HKDF_INFO = b"agora-bootstrap-ecies-v1"

# Crockford base32 alphabet (no I, L, O, U).  Manually-typed pairing
# codes are visually unambiguous; the CMS normalizer maps I/L→1 and
# O→0 on input.  See cms/services/device_bootstrap.py:normalize_pairing_secret.
_B32_ALPHA = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
_CROCKFORD_ALPHA = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


# ---------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------


class BootstrapKeyFileError(Exception):
    """Raised when the device-key file fails invariants."""


class BootstrapSecretFileError(Exception):
    """Raised when the pairing-secret file fails invariants."""


# ---------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class DeviceIdentity:
    """Output of :func:`load_or_create_device_identity`.

    ``seed`` is the raw 32-byte Ed25519 seed (keep secret).
    ``pubkey_b64`` is standard-base64 of the 32-byte public key.
    """

    seed: bytes
    pubkey_b64: str


# ---------------------------------------------------------------------
# File-system helpers (fd-based, TOCTOU-safe)
# ---------------------------------------------------------------------


def _check_parent_dir(path: Path, exc_cls: type[Exception]) -> None:
    """Validate and, where safe, repair the *immediate* parent of ``path``.

    Rules enforced on the immediate parent only:
    - must exist and be a directory
    - must be owned by the current uid
    - must not be group- or world-writable
    - same-owner over-permissive mode is repaired to ``0o700``

    Note: if the parent is missing, we create it with ``mkdir(parents=True,
    mode=0o700)``; intermediate ancestors inherit that mode on creation
    but pre-existing ancestors are **not** audited.  Callers that care
    about the full chain should place the secret under a stable
    application directory (e.g. ``/opt/agora/persist``) that is itself
    managed by packaging.
    """
    parent = path.parent
    if not parent.exists():
        try:
            parent.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)
        except OSError as e:
            raise exc_cls(f"mkdir({parent}) failed: {e}") from e
        return
    st = os.stat(parent, follow_symlinks=False)
    if not stat.S_ISDIR(st.st_mode):
        raise exc_cls(f"{parent} is not a directory")
    uid = os.getuid() if hasattr(os, "getuid") else st.st_uid
    if st.st_uid != uid:
        raise exc_cls(
            f"{parent} is owned by uid={st.st_uid}, expected {uid}; "
            f"fix ownership or choose a different path"
        )
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        try:
            os.chmod(parent, _DIR_MODE)
        except OSError as e:
            raise exc_cls(
                f"{parent} is group/world-writable and chmod failed: {e}"
            ) from e


def _fsync_dir(parent: Path) -> None:
    """Best-effort fsync of ``parent``.  Silently ignored if unsupported."""
    try:
        dir_fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


def _open_existing_regular_file(path: Path, exc_cls: type[Exception]) -> int:
    """Open ``path`` with ``O_NOFOLLOW`` and validate via ``fstat``.

    Returns an open file descriptor in read-only mode.  Caller must close it.
    Raises ``exc_cls`` if the file is a symlink, not a regular file, owned
    by a different uid, or (repairably) has loose permissions.

    Same-owner over-permissive mode is repaired to ``0o400`` via ``fchmod``.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as e:
        # ELOOP means a symlink was followed; distinguish for the caller.
        if getattr(e, "errno", None) in (40,):  # ELOOP on Linux
            raise exc_cls(f"{path} is a symlink; refusing to follow") from e
        raise exc_cls(f"open({path}) failed: {e}") from e
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise exc_cls(f"{path} is not a regular file")
        uid = os.getuid() if hasattr(os, "getuid") else st.st_uid
        if st.st_uid != uid:
            raise exc_cls(
                f"{path} is owned by uid={st.st_uid}, expected {uid}; "
                f"refusing to load a secret from a file owned by another user"
            )
        perm = st.st_mode & 0o777
        if perm != _FILE_MODE:
            # Same owner and loose perms → repair via fd.
            try:
                os.fchmod(fd, _FILE_MODE)
            except OSError as e:
                raise exc_cls(
                    f"{path} has mode {perm:o} and fchmod failed: {e}"
                ) from e
    except Exception:
        os.close(fd)
        raise
    return fd


def _create_new_secret_file(
    path: Path, data: bytes, exc_cls: type[Exception],
) -> bool:
    """Atomically create ``path`` with ``data`` and mode ``0o400``.

    Uses ``O_CREAT | O_EXCL | O_NOFOLLOW`` on the *final* path so the
    operation is create-once: concurrent callers cannot overwrite each
    other.  Returns ``True`` on success, ``False`` if the file already
    exists (``EEXIST``) — caller should fall through to the existing-file
    validation path.  Any other failure (``ENOSPC``, ``EROFS``, ``EPERM``,
    ...) is wrapped in ``exc_cls``.

    On success, ``fsync``s the file fd and the parent directory so the
    newly-created file is durable across power-loss.
    """
    parent = path.parent
    try:
        fd = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            _FILE_MODE,
        )
    except FileExistsError:
        return False
    except OSError as e:
        errno_val = getattr(e, "errno", None)
        hint = ""
        if errno_val == 30:  # EROFS
            hint = " (read-only filesystem?)"
        elif errno_val == 28:  # ENOSPC
            hint = " (no space left on device)"
        elif errno_val == 13:  # EACCES
            hint = " (permission denied; check parent ownership/mode)"
        raise exc_cls(f"create({path}) failed{hint}: {e}") from e
    try:
        try:
            os.write(fd, data)
            os.fsync(fd)
        except OSError as e:
            raise exc_cls(f"write({path}) failed: {e}") from e
    except Exception:
        # Don't leave a partial file behind; O_EXCL on the next attempt
        # would otherwise mis-diagnose an already-created file.
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)
    _fsync_dir(parent)
    return True


# ---------------------------------------------------------------------
# Device identity (Ed25519 seed on disk)
# ---------------------------------------------------------------------


def _derive_pubkey_b64(seed: bytes) -> str:
    if len(seed) != _SEED_LEN:
        raise BootstrapKeyFileError(
            f"ed25519 seed must be {_SEED_LEN} bytes, got {len(seed)}"
        )
    priv = Ed25519PrivateKey.from_private_bytes(seed)
    pub_bytes = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(pub_bytes).decode("ascii")


def load_or_create_device_identity(key_path: Path) -> DeviceIdentity:
    """Load the device's Ed25519 seed from ``key_path`` or create it.

    Create-once semantics: two concurrent callers observing an absent
    file will not both generate and overwrite — the loser's ``O_EXCL``
    create fails with ``EEXIST`` and that caller reads the winner's
    contents instead.

    Never regenerates a malformed existing file — raises instead so an
    already-adopted device isn't silently re-issued a new identity.
    """
    _check_parent_dir(key_path, BootstrapKeyFileError)
    seed = os.urandom(_SEED_LEN)
    if _create_new_secret_file(key_path, seed, BootstrapKeyFileError):
        return DeviceIdentity(seed=seed, pubkey_b64=_derive_pubkey_b64(seed))
    # File already existed (or was concurrently created): validate + load.
    fd = _open_existing_regular_file(key_path, BootstrapKeyFileError)
    try:
        blob = os.read(fd, _SEED_LEN + 1)
    finally:
        os.close(fd)
    if len(blob) != _SEED_LEN:
        raise BootstrapKeyFileError(
            f"{key_path} is {len(blob)} bytes; expected {_SEED_LEN}. "
            f"Refusing to silently regenerate (would strand the adopted "
            f"device); delete the file manually to factory-reset identity."
        )
    return DeviceIdentity(seed=blob, pubkey_b64=_derive_pubkey_b64(blob))


# ---------------------------------------------------------------------
# Pairing secret
# ---------------------------------------------------------------------


def _generate_pairing_secret() -> str:
    raw = os.urandom(PAIRING_SECRET_LEN_BYTES)
    # Manual Crockford base32 (stdlib has none).  5 bytes = 40 bits = 8 chars.
    n = int.from_bytes(raw, "big")
    chars = []
    for _ in range(PAIRING_SECRET_TEXT_LEN):
        chars.append(_CROCKFORD_ALPHA[n & 0x1F])
        n >>= 5
    text = "".join(reversed(chars))
    assert len(text) == PAIRING_SECRET_TEXT_LEN, (
        f"crockford length drift: got {len(text)}"
    )
    return text


def load_or_create_pairing_secret(secret_path: Path) -> str:
    """Load the pairing secret from ``secret_path`` or create it.

    Shares the same file-system contract and create-once semantics as
    :func:`load_or_create_device_identity`.  Returns the 8-char
    Crockford-base32 text form.
    """
    _check_parent_dir(secret_path, BootstrapSecretFileError)
    new_text = _generate_pairing_secret()
    if _create_new_secret_file(
        secret_path, new_text.encode("ascii"), BootstrapSecretFileError,
    ):
        return new_text
    # File already existed (or was concurrently created): validate + load.
    fd = _open_existing_regular_file(secret_path, BootstrapSecretFileError)
    try:
        blob = os.read(fd, PAIRING_SECRET_TEXT_LEN + 2)
    finally:
        os.close(fd)
    text = blob.decode("ascii", errors="replace").strip()
    if len(text) != PAIRING_SECRET_TEXT_LEN or not set(text).issubset(_B32_ALPHA):
        raise BootstrapSecretFileError(
            f"{secret_path} contents are not a {PAIRING_SECRET_TEXT_LEN}-char "
            f"Crockford-base32 string; refusing to silently regenerate. "
            f"Delete the file manually to generate a fresh secret."
        )
    return text


def pairing_secret_hash_hex(secret: str) -> str:
    """sha256 hex of UTF-8 bytes.  Matches the CMS adopt-lookup convention."""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------


def connect_token_canonical_bytes(device_id: str, timestamp: int, nonce: str) -> bytes:
    """Bit-identical canonicalisation of a ``/connect-token`` request.

    CMS uses ``str(req.timestamp)`` before canonicalising; we match by
    calling ``str(int(timestamp))`` so a JSON int and a JSON str round-trip
    to the same bytes here.
    """
    return f"{device_id}|{int(timestamp)}|{nonce}".encode("utf-8")


def sign_connect_token_request(
    seed: bytes, device_id: str, timestamp: int, nonce: str,
) -> str:
    """Ed25519-sign the canonical bytes and return a standard-base64 signature."""
    if len(seed) != _SEED_LEN:
        raise ValueError(f"ed25519 seed must be {_SEED_LEN} bytes")
    priv = Ed25519PrivateKey.from_private_bytes(seed)
    sig = priv.sign(connect_token_canonical_bytes(device_id, timestamp, nonce))
    return base64.b64encode(sig).decode("ascii")


# ---------------------------------------------------------------------
# ECIES decrypt (mirror of CMS encrypt_for_device)
# ---------------------------------------------------------------------


def _ed25519_priv_to_x25519(seed: bytes) -> X25519PrivateKey:
    """Derive the X25519 private key from a raw Ed25519 seed.

    Implementation matches the CMS side byte-for-byte (RFC 8032 §5.1.5):
    SHA-512 the seed, take the low 32 bytes, apply RFC 7748 clamping.
    """
    if len(seed) != _SEED_LEN:
        raise ValueError("ed25519 seed must be 32 bytes")
    h = hashlib.sha512(seed).digest()[:32]
    scalar = bytearray(h)
    scalar[0] &= 248
    scalar[31] &= 127
    scalar[31] |= 64
    return X25519PrivateKey.from_private_bytes(bytes(scalar))


def decrypt_adopt_payload(seed: bytes, ciphertext_b64: str) -> bytes:
    """Inverse of CMS ``encrypt_for_device``.

    Raises ``ValueError`` on malformed base64, a too-short blob, a derived
    nonce/prefix mismatch, or AES-GCM authentication failure (propagates
    ``InvalidTag`` as ``ValueError``).
    """
    try:
        blob = base64.b64decode(ciphertext_b64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError("invalid base64 ciphertext") from e
    if len(blob) < 32 + 12 + 16:
        raise ValueError("ciphertext too short")
    eph_pub, nonce, ct = blob[:32], blob[32:44], blob[44:]
    x_priv = _ed25519_priv_to_x25519(seed)
    shared = x_priv.exchange(X25519PublicKey.from_public_bytes(eph_pub))
    key_material = HKDF(
        algorithm=hashes.SHA256(),
        length=32 + 12,
        salt=None,
        info=_ECIES_HKDF_INFO,
    ).derive(shared)
    key, derived_nonce = key_material[:32], key_material[32:]
    if derived_nonce != nonce:
        # CMS's encrypt_for_device uses the HKDF-derived nonce as the
        # on-wire nonce, so a mismatch means the blob was tampered with
        # before the AEAD check could catch it.
        raise ValueError("nonce mismatch")
    try:
        return AESGCM(key).decrypt(nonce, ct, associated_data=None)
    except Exception as e:
        raise ValueError("AES-GCM authentication failed") from e


# ---------------------------------------------------------------------
# Fleet HMAC (register gate)
# ---------------------------------------------------------------------


def fleet_hmac_input(
    *,
    device_id: str,
    pubkey_b64: str,
    pairing_secret_hash: str,
    fleet_id: str,
    timestamp: int,
    nonce: str,
) -> bytes:
    """Canonical MAC input for ``POST /api/devices/register``.  Mirrors CMS."""
    parts = [
        "register",
        device_id,
        pubkey_b64,
        pairing_secret_hash,
        fleet_id,
        str(int(timestamp)),
        nonce,
    ]
    return "|".join(parts).encode("utf-8")


def compute_fleet_hmac_hex(
    secret: bytes,
    *,
    device_id: str,
    pubkey_b64: str,
    pairing_secret_hash: str,
    fleet_id: str,
    timestamp: int,
    nonce: str,
) -> str:
    """HMAC-SHA256, hex-encoded, over :func:`fleet_hmac_input`."""
    message = fleet_hmac_input(
        device_id=device_id,
        pubkey_b64=pubkey_b64,
        pairing_secret_hash=pairing_secret_hash,
        fleet_id=fleet_id,
        timestamp=timestamp,
        nonce=nonce,
    )
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


__all__ = [
    "BootstrapKeyFileError",
    "BootstrapSecretFileError",
    "DeviceIdentity",
    "PAIRING_SECRET_LEN_BYTES",
    "PAIRING_SECRET_TEXT_LEN",
    "compute_fleet_hmac_hex",
    "connect_token_canonical_bytes",
    "decrypt_adopt_payload",
    "fleet_hmac_input",
    "load_or_create_device_identity",
    "load_or_create_pairing_secret",
    "pairing_secret_hash_hex",
    "sign_connect_token_request",
]
