"""Tests for the ``/etc/agora/version`` parser in :mod:`os_updater.main`.

agora-os ships ``/etc/agora/version`` as a multi-line key=value file
(``agora_os_version=...``, ``agora_app_floor=...``). The original
implementation did a naive ``.strip()`` of the entire file and treated
the result as the version string, which broke every floor check. These
tests pin the new parser's contract.
"""

from __future__ import annotations

import pytest

from os_updater.main import _read_current_version


def _write(tmp_path, content: str):
    p = tmp_path / "version"
    p.write_text(content, encoding="utf-8")
    return p


class TestReadCurrentVersion:
    def test_returns_agora_os_version_value(self, tmp_path):
        p = _write(
            tmp_path,
            "# header\nagora_os_version=0.0.4-test\nagora_app_floor=1.11.0\n",
        )
        assert _read_current_version(p) == "0.0.4-test"

    def test_skips_comments_and_blank_lines(self, tmp_path):
        p = _write(
            tmp_path,
            "\n# comment line\n\nagora_os_version=1.2.3\n\n# trailing comment\n",
        )
        assert _read_current_version(p) == "1.2.3"

    def test_tolerates_unknown_keys(self, tmp_path):
        # agora-os may grow new keys without coordinating with this
        # parser; only the missing agora_os_version line is fatal.
        p = _write(
            tmp_path,
            "agora_os_version=2.0.0\nagora_app_floor=1.11.0\nfuture_field=hello\n",
        )
        assert _read_current_version(p) == "2.0.0"

    def test_strips_surrounding_whitespace_in_value(self, tmp_path):
        p = _write(tmp_path, "agora_os_version =   1.2.3   \n")
        assert _read_current_version(p) == "1.2.3"

    def test_missing_agora_os_version_raises(self, tmp_path):
        p = _write(tmp_path, "agora_app_floor=1.11.0\nfuture_field=hello\n")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_empty_value_raises(self, tmp_path):
        p = _write(tmp_path, "agora_os_version=\n")
        with pytest.raises(RuntimeError, match="empty value"):
            _read_current_version(p)

    def test_empty_file_raises(self, tmp_path):
        p = _write(tmp_path, "")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_comment_only_file_raises(self, tmp_path):
        p = _write(tmp_path, "# only comments\n# nothing useful\n")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_malformed_line_without_equals_raises(self, tmp_path):
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nthis-line-has-no-equals\n",
        )
        with pytest.raises(RuntimeError, match="malformed line"):
            _read_current_version(p)

    def test_literal_todo_placeholder_passes_through(self, tmp_path):
        # Regression: pre-fix v0.0.4 images shipped with literal "TODO"
        # as the value. The parser shouldn't blow up on it — the floor
        # check downstream is responsible for noticing "TODO" isn't
        # semver-shaped. This test pins that we return the literal
        # string rather than swallowing it as "missing".
        p = _write(tmp_path, "agora_os_version=TODO\nagora_app_floor=TODO\n")
        assert _read_current_version(p) == "TODO"

    def test_multiple_agora_os_version_lines_last_wins(self, tmp_path):
        # Not a documented format feature, but the parser walks the
        # whole file; pin "last write wins" so a future bug that adds
        # duplicate lines is at least deterministic.
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nagora_os_version=2.0.0\n",
        )
        assert _read_current_version(p) == "2.0.0"
