"""Tests for :mod:`provision.pairing` — the pairing-secret reader.

These tests are pure-Python and run in CI without cairo /
gobject-introspection.  Behavioral tests for the OOBE display flow that
consume this helper live in :mod:`tests.test_provision_qr_display`,
which is gated on ``cairo`` being importable.
"""
from __future__ import annotations

from provision.pairing import read_pairing_secret

# 26-char RFC-4648 base32 — every char is in the alphabet ([A-Z2-7]).
SAMPLE_SECRET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_missing_file_returns_none(tmp_path):
    assert read_pairing_secret(tmp_path / "nope") is None


def test_valid_secret(tmp_path):
    p = tmp_path / "pairing_secret"
    p.write_text(SAMPLE_SECRET)
    assert read_pairing_secret(p) == SAMPLE_SECRET


def test_strips_trailing_newline(tmp_path):
    p = tmp_path / "pairing_secret"
    p.write_text(SAMPLE_SECRET + "\n")
    assert read_pairing_secret(p) == SAMPLE_SECRET


def test_wrong_length_returns_none(tmp_path):
    p = tmp_path / "pairing_secret"
    p.write_text("ABCDEFGHIJ")
    assert read_pairing_secret(p) is None


def test_invalid_chars_returns_none(tmp_path):
    p = tmp_path / "pairing_secret"
    # "1" and "8" are NOT in the RFC-4648 base32 alphabet (A-Z + 2-7).
    p.write_text("1BCDEFGHIJKLMNOPQRSTUVWXY8")
    assert read_pairing_secret(p) is None


def test_lowercase_rejected(tmp_path):
    p = tmp_path / "pairing_secret"
    p.write_text(SAMPLE_SECRET.lower())
    assert read_pairing_secret(p) is None


def test_internal_whitespace_rejected(tmp_path):
    p = tmp_path / "pairing_secret"
    p.write_text("ABCDEFGHIJKL MNOPQRSTUVWXY")  # space in middle
    assert read_pairing_secret(p) is None


def test_does_not_create_file(tmp_path):
    """Provision must never write the secret — cms-client owns that file."""
    p = tmp_path / "pairing_secret"
    read_pairing_secret(p)
    assert not p.exists()


def test_unreadable_directory_returns_none(tmp_path):
    """A path whose parent doesn't exist is treated as 'no secret'."""
    p = tmp_path / "no_such_dir" / "pairing_secret"
    assert read_pairing_secret(p) is None
