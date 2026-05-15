"""Tests for the shared ``/etc/agora/version`` parser in
:mod:`shared.version_file`.

The strict variant (:func:`read_os_version_strict`) is exercised
indirectly by :mod:`tests.test_os_updater_main` (which still imports
``os_updater.main._read_current_version`` — that's a thin wrapper over
the strict parser). These tests focus on the lenient
:func:`parse_version_file` path that ``cms_client`` will use to populate
the ``os_version`` field in its register message (per the M4 phase of
the CMS-migration sub-plan), where missing keys must not raise.
"""

from __future__ import annotations

import pytest

from shared.version_file import (
    parse_version_file,
    read_os_version_strict,
)


def _write(tmp_path, content: str):
    p = tmp_path / "version"
    p.write_text(content, encoding="utf-8")
    return p


class TestParseVersionFile:
    """Lenient parser: missing keys return None, malformed lines raise."""

    def test_returns_both_well_known_keys(self, tmp_path):
        p = _write(
            tmp_path,
            "# header\nagora_os_version=0.0.4-test\nagora_app_floor=1.11.0\n",
        )
        assert parse_version_file(p) == {
            "agora_os_version": "0.0.4-test",
            "agora_app_floor": "1.11.0",
        }

    def test_missing_keys_return_none(self, tmp_path):
        # cms_client needs to be able to register from a workstation
        # checkout where /etc/agora/version may have only one of the
        # two keys (or unrelated keys) — None must not raise.
        p = _write(tmp_path, "agora_os_version=1.2.3\n")
        assert parse_version_file(p) == {
            "agora_os_version": "1.2.3",
            "agora_app_floor": None,
        }

    def test_empty_value_returns_none(self, tmp_path):
        # An empty value is semantically equivalent to the key being
        # absent; both surface as None so the caller has a single
        # falsy check.
        p = _write(tmp_path, "agora_os_version=\nagora_app_floor=1.11.0\n")
        assert parse_version_file(p) == {
            "agora_os_version": None,
            "agora_app_floor": "1.11.0",
        }

    def test_skips_comments_and_blank_lines(self, tmp_path):
        p = _write(
            tmp_path,
            "\n# comment\n\nagora_os_version=1.2.3\n\n# trailing\n",
        )
        assert parse_version_file(p)["agora_os_version"] == "1.2.3"

    def test_tolerates_unknown_keys(self, tmp_path):
        # agora-os may grow new keys (e.g. agora_kernel_pin) without
        # coordinating with this parser; unknown keys are silently
        # dropped, not stored.
        p = _write(
            tmp_path,
            "agora_os_version=2.0.0\nagora_app_floor=1.11.0\nfuture_field=hello\n",
        )
        result = parse_version_file(p)
        assert result["agora_os_version"] == "2.0.0"
        assert result["agora_app_floor"] == "1.11.0"
        assert "future_field" not in result

    def test_strips_surrounding_whitespace(self, tmp_path):
        p = _write(tmp_path, "  agora_os_version =   1.2.3   \n")
        assert parse_version_file(p)["agora_os_version"] == "1.2.3"

    def test_malformed_line_raises(self, tmp_path):
        # Malformed lines are a hard fail in both modes — better to
        # surface a corrupted version file than to silently drop data.
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nthis-line-has-no-equals\n",
        )
        with pytest.raises(RuntimeError, match="malformed line"):
            parse_version_file(p)

    def test_missing_file_raises_filenotfounderror(self, tmp_path):
        # Caller (cms_client) can catch this and fall back to None
        # when running off-device.
        p = tmp_path / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            parse_version_file(p)

    def test_empty_file_returns_all_none(self, tmp_path):
        p = _write(tmp_path, "")
        assert parse_version_file(p) == {
            "agora_os_version": None,
            "agora_app_floor": None,
        }


class TestReadOsVersionStrict:
    """Strict wrapper: missing/empty agora_os_version raises."""

    def test_happy_path(self, tmp_path):
        p = _write(
            tmp_path,
            "agora_os_version=0.0.16-test\nagora_app_floor=1.11.0\n",
        )
        assert read_os_version_strict(p) == "0.0.16-test"

    def test_missing_key_raises(self, tmp_path):
        p = _write(tmp_path, "agora_app_floor=1.11.0\n")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            read_os_version_strict(p)

    def test_empty_value_raises_with_distinctive_message(self, tmp_path):
        # The error message disambiguates "empty value" from "key
        # missing" so on-device journal logs are actionable.
        p = _write(tmp_path, "agora_os_version=\n")
        with pytest.raises(RuntimeError, match="empty value"):
            read_os_version_strict(p)

    def test_missing_file_raises_filenotfounderror(self, tmp_path):
        p = tmp_path / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            read_os_version_strict(p)

    def test_malformed_line_raises(self, tmp_path):
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nthis-line-has-no-equals\n",
        )
        with pytest.raises(RuntimeError, match="malformed line"):
            read_os_version_strict(p)
