"""Unit tests for ``shared.bootstrap_identity``.

Stage B.1 of the bootstrap redesign (issue #420).  The module is pure
library code (crypto + secret-file primitives); these tests avoid any
cross-repo imports and instead mirror the CMS ``encrypt_for_device``
wire format inline when needed, so the suite pins the wire format on
the device side independently.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import stat
import sys
from pathlib import Path

import pytest
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

from shared.bootstrap_identity import (
    BootstrapKeyFileError,
    BootstrapSecretFileError,
    DeviceIdentity,
    PAIRING_SECRET_TEXT_LEN,
    compute_fleet_hmac_hex,
    connect_token_canonical_bytes,
    decrypt_adopt_payload,
    fleet_hmac_input,
    load_or_create_device_identity,
    load_or_create_pairing_secret,
    pairing_secret_hash_hex,
    sign_connect_token_request,
)

_POSIX = sys.platform != "win32"
posix_only = pytest.mark.skipif(
    not _POSIX, reason="fd-based fs invariants are POSIX-only"
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _ed25519_pub_to_x25519(pub_bytes: bytes) -> X25519PublicKey:
    """Mirror of CMS ``_ed25519_pub_to_x25519`` — derive X25519 pub from Ed25519 pub.

    Uses the standard Montgomery-from-Edwards u = (1+y)/(1-y) mapping.  We
    only need this for the self-contained ECIES fixture below; the library
    under test does *not* exercise this branch.
    """
    # We can avoid re-implementing y→u by doing the encrypt against a
    # recipient whose identity we already own: derive X25519 priv from
    # the seed and use its public key as the recipient.  That's what
    # ``_encrypt_for_device_seed`` does instead.
    raise NotImplementedError  # pragma: no cover


def _x25519_priv_from_ed25519_seed(seed: bytes) -> X25519PrivateKey:
    h = hashlib.sha512(seed).digest()[:32]
    scalar = bytearray(h)
    scalar[0] &= 248
    scalar[31] &= 127
    scalar[31] |= 64
    return X25519PrivateKey.from_private_bytes(bytes(scalar))


def _encrypt_for_device_seed(seed: bytes, plaintext: bytes) -> str:
    """Produce a bootstrap-ECIES ciphertext addressed to ``seed``'s identity.

    Byte-for-byte match with CMS ``encrypt_for_device``.  We encrypt
    against the X25519 pub derived from the same seed the decrypt path
    will use, sidestepping the Ed25519-pub → X25519-pub conversion
    (not under test here).
    """
    recip_x_pub = _x25519_priv_from_ed25519_seed(seed).public_key()

    eph_priv = X25519PrivateKey.generate()
    eph_pub_bytes = eph_priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    shared = eph_priv.exchange(recip_x_pub)
    key_material = HKDF(
        algorithm=hashes.SHA256(),
        length=32 + 12,
        salt=None,
        info=b"agora-bootstrap-ecies-v1",
    ).derive(shared)
    key, nonce = key_material[:32], key_material[32:]
    ct = AESGCM(key).encrypt(nonce, plaintext, associated_data=None)
    return base64.b64encode(eph_pub_bytes + nonce + ct).decode("ascii")


# ---------------------------------------------------------------------
# Device identity
# ---------------------------------------------------------------------


@posix_only
def test_load_or_create_device_identity_roundtrip(tmp_path: Path) -> None:
    # Parent must be 0o700 + owned by us — tmp_path satisfies this.
    os.chmod(tmp_path, 0o700)
    key_path = tmp_path / "device_key"

    ident1 = load_or_create_device_identity(key_path)
    assert isinstance(ident1, DeviceIdentity)
    assert len(ident1.seed) == 32
    # Pubkey must match what Ed25519PrivateKey derives from the seed.
    priv = Ed25519PrivateKey.from_private_bytes(ident1.seed)
    pub = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    assert base64.b64decode(ident1.pubkey_b64) == pub

    # On-disk file should be the raw 32-byte seed.
    assert key_path.read_bytes() == ident1.seed
    assert stat.S_IMODE(os.stat(key_path).st_mode) == 0o400

    # Reload returns the same identity.
    ident2 = load_or_create_device_identity(key_path)
    assert ident2.seed == ident1.seed
    assert ident2.pubkey_b64 == ident1.pubkey_b64


@posix_only
def test_loose_perms_repaired_when_same_owner(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    key_path = tmp_path / "device_key"
    load_or_create_device_identity(key_path)

    # Admin fat-fingered mode 0o644 after-the-fact; same owner, we repair.
    os.chmod(key_path, 0o644)
    ident = load_or_create_device_identity(key_path)
    assert stat.S_IMODE(os.stat(key_path).st_mode) == 0o400
    assert len(ident.seed) == 32


@posix_only
def test_different_owner_is_hard_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    os.chmod(tmp_path, 0o700)
    key_path = tmp_path / "device_key"
    load_or_create_device_identity(key_path)

    # Simulate "file owned by someone else" by faking the effective uid.
    real_uid = os.getuid()
    monkeypatch.setattr(os, "getuid", lambda: real_uid + 1)
    with pytest.raises(BootstrapKeyFileError, match="owned by uid"):
        load_or_create_device_identity(key_path)


@posix_only
def test_malformed_seed_never_regenerates(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    key_path = tmp_path / "device_key"
    # Write a 31-byte file (one short) with permissive perms then tight.
    key_path.write_bytes(b"\x00" * 31)
    os.chmod(key_path, 0o400)

    with pytest.raises(BootstrapKeyFileError, match="bytes; expected 32"):
        load_or_create_device_identity(key_path)

    # Crucially the file is untouched — admin must intervene explicitly.
    assert key_path.read_bytes() == b"\x00" * 31


@posix_only
def test_symlink_target_refused(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    real = tmp_path / "real_key"
    real.write_bytes(os.urandom(32))
    os.chmod(real, 0o400)

    key_path = tmp_path / "device_key"
    os.symlink(real, key_path)
    with pytest.raises(BootstrapKeyFileError, match="symlink"):
        load_or_create_device_identity(key_path)


@posix_only
def test_parent_dir_group_writable_repaired(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o770)  # group-writable; same owner, should repair
    key_path = tmp_path / "device_key"
    load_or_create_device_identity(key_path)
    # _DIR_MODE == 0o700 after repair
    assert stat.S_IMODE(os.stat(tmp_path).st_mode) == 0o700


@posix_only
def test_parent_not_a_directory(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    # Plant a regular file where the parent directory should be.
    bogus_parent = tmp_path / "bogus"
    bogus_parent.write_bytes(b"not a dir")
    key_path = bogus_parent / "device_key"
    with pytest.raises(BootstrapKeyFileError, match="not a directory"):
        load_or_create_device_identity(key_path)


@posix_only
def test_existing_path_is_directory_not_file(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    key_path = tmp_path / "device_key"
    key_path.mkdir(mode=0o700)
    with pytest.raises(BootstrapKeyFileError, match="not a regular file"):
        load_or_create_device_identity(key_path)


# ---------------------------------------------------------------------
# Pairing secret
# ---------------------------------------------------------------------


@posix_only
def test_pairing_secret_roundtrip(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    secret_path = tmp_path / "pairing_secret"
    text1 = load_or_create_pairing_secret(secret_path)
    assert len(text1) == PAIRING_SECRET_TEXT_LEN
    assert text1 == text1.upper()  # uppercase base32
    assert set(text1).issubset(set("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"))
    assert stat.S_IMODE(os.stat(secret_path).st_mode) == 0o400

    text2 = load_or_create_pairing_secret(secret_path)
    assert text2 == text1


@posix_only
def test_pairing_secret_malformed_never_regenerates(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    secret_path = tmp_path / "pairing_secret"
    # Wrong length + non-base32 chars.
    secret_path.write_text("shortsecret")
    os.chmod(secret_path, 0o400)
    with pytest.raises(BootstrapSecretFileError, match="refusing to silently"):
        load_or_create_pairing_secret(secret_path)
    assert secret_path.read_text() == "shortsecret"


def test_pairing_secret_hash_hex_frozen() -> None:
    # Pin the hash contract with a frozen input → expected SHA-256 hex.
    digest = pairing_secret_hash_hex("MFRGG2DFMZTWQ2LKNNWG23TV")
    expected = hashlib.sha256(
        "MFRGG2DFMZTWQ2LKNNWG23TV".encode("utf-8")
    ).hexdigest()
    assert digest == expected


# ---------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------


def test_connect_token_canonical_bytes_stable() -> None:
    # Timestamps may arrive as int or str off the wire — both must produce
    # the exact same canonical bytes once coerced.
    a = connect_token_canonical_bytes("dev-1", 1700000000, "nonce-xyz")
    b = connect_token_canonical_bytes("dev-1", int("1700000000"), "nonce-xyz")
    assert a == b == b"dev-1|1700000000|nonce-xyz"


def test_sign_connect_token_roundtrip() -> None:
    seed = os.urandom(32)
    priv = Ed25519PrivateKey.from_private_bytes(seed)
    pub = priv.public_key()
    sig_b64 = sign_connect_token_request(
        seed, device_id="dev-1", timestamp=1700000000, nonce="n"
    )
    sig = base64.b64decode(sig_b64)
    pub.verify(sig, b"dev-1|1700000000|n")  # raises on failure


def test_sign_connect_token_rejects_wrong_seed_length() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        sign_connect_token_request(b"\x00" * 31, "d", 1, "n")


# ---------------------------------------------------------------------
# ECIES decrypt
# ---------------------------------------------------------------------


def test_decrypt_adopt_payload_roundtrip() -> None:
    seed = os.urandom(32)
    plaintext = b'{"device_id":"abc","api_key":"k"}'
    ct_b64 = _encrypt_for_device_seed(seed, plaintext)
    assert decrypt_adopt_payload(seed, ct_b64) == plaintext


def test_decrypt_adopt_payload_tamper_triggers_auth_failure() -> None:
    seed = os.urandom(32)
    ct_b64 = _encrypt_for_device_seed(seed, b"hello world")
    raw = bytearray(base64.b64decode(ct_b64))
    # Flip one bit deep in the ciphertext region (past the 32+12 prefix).
    raw[50] ^= 0x01
    tampered = base64.b64encode(bytes(raw)).decode("ascii")
    with pytest.raises(ValueError, match="AES-GCM authentication failed"):
        decrypt_adopt_payload(seed, tampered)


def test_decrypt_adopt_payload_nonce_mismatch() -> None:
    seed = os.urandom(32)
    ct_b64 = _encrypt_for_device_seed(seed, b"hello world")
    raw = bytearray(base64.b64decode(ct_b64))
    # Flip the first byte of the on-wire nonce; HKDF-derived nonce won't
    # match and we short-circuit before AEAD.
    raw[32] ^= 0x01
    tampered = base64.b64encode(bytes(raw)).decode("ascii")
    with pytest.raises(ValueError, match="nonce mismatch"):
        decrypt_adopt_payload(seed, tampered)


def test_decrypt_adopt_payload_too_short() -> None:
    with pytest.raises(ValueError, match="too short"):
        decrypt_adopt_payload(os.urandom(32), base64.b64encode(b"x" * 10).decode())


def test_decrypt_adopt_payload_bad_base64() -> None:
    with pytest.raises(ValueError, match="invalid base64"):
        decrypt_adopt_payload(os.urandom(32), "!!!not base64!!!")


# ---------------------------------------------------------------------
# Fleet HMAC
# ---------------------------------------------------------------------


def test_fleet_hmac_input_canonical() -> None:
    buf = fleet_hmac_input(
        device_id="dev-1",
        pubkey_b64="PUB",
        pairing_secret_hash="HASH",
        fleet_id="fleet-A",
        timestamp=1700000000,
        nonce="n",
    )
    assert buf == b"register|dev-1|PUB|HASH|fleet-A|1700000000|n"


def test_compute_fleet_hmac_hex_matches_stdlib() -> None:
    secret = b"fleet-secret-bytes"
    got = compute_fleet_hmac_hex(
        secret,
        device_id="dev-1",
        pubkey_b64="PUB",
        pairing_secret_hash="HASH",
        fleet_id="fleet-A",
        timestamp=1700000000,
        nonce="n",
    )
    expected = hmac.new(
        secret,
        b"register|dev-1|PUB|HASH|fleet-A|1700000000|n",
        hashlib.sha256,
    ).hexdigest()
    assert got == expected
