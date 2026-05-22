"""Tests for :mod:`os_updater.dispatch` — payload parsing + validation.

Acceptance hooks (plan §"Phase 2 — Acceptance"):
* Required fields enforced.
* version regex (``major.minor.patch[-pre]``).
* release_id regex (``[A-Za-z0-9._-]{1,128}``).
* ``force_now`` / ``force_downgrade`` default to false.
* ``extra="ignore"`` for forward compatibility (Phase 3 fields).
* https / http urls accepted, other schemes rejected.
"""

from __future__ import annotations

import pytest

from os_updater.dispatch import (
    DispatchPayload,
    DispatchPayloadError,
    parse_dispatch_payload,
)


def _ok_msg(**overrides):
    base = {
        "type": "os_update_dispatch",
        "release_id": "rel_2026_05_07_v1.1.0",
        "target_version": "1.1.0",
        "min_from_version": "1.0.0",
        "bundle_url": "https://github.com/x/y/releases/download/v1.1.0/bundle.zst",
        "signature_url": "https://github.com/x/y/releases/download/v1.1.0/bundle.zst.minisig",
    }
    base.update(overrides)
    return base


class TestHappyPath:
    def test_minimum_required_fields_parse(self):
        p = parse_dispatch_payload(_ok_msg())
        assert isinstance(p, DispatchPayload)
        assert p.release_id == "rel_2026_05_07_v1.1.0"
        assert p.target_version == "1.1.0"
        assert p.min_from_version == "1.0.0"

    def test_force_flags_default_false(self):
        p = parse_dispatch_payload(_ok_msg())
        assert p.force_now is False
        assert p.force_downgrade is False

    def test_force_flags_round_trip(self):
        p = parse_dispatch_payload(_ok_msg(force_now=True, force_downgrade=True))
        assert p.force_now is True
        assert p.force_downgrade is True

    def test_prerelease_version_accepted(self):
        p = parse_dispatch_payload(
            _ok_msg(target_version="1.1.0-rc1", min_from_version="1.0.0-beta.2")
        )
        assert p.target_version == "1.1.0-rc1"
        assert p.min_from_version == "1.0.0-beta.2"

    def test_multi_token_prerelease_accepted(self):
        """Tags like ``v0.0.40-test-k612`` produce a multi-hyphen prerelease
        when stripped of the leading ``v``. The dispatch regex must accept
        that or every multi-token test build looks invalid to the device."""
        p = parse_dispatch_payload(
            _ok_msg(target_version="0.0.40-test-k612", min_from_version="0.0.0")
        )
        assert p.target_version == "0.0.40-test-k612"

    def test_extra_field_ignored(self):
        """Phase 3 adds ``not_before`` etc. — older daemons must ignore
        anything they don't recognize so a CMS-side schema bump doesn't
        brick the fleet."""
        msg = _ok_msg(scheduled_at="2026-05-07T03:00:00Z", some_phase3_field="abc")
        p = parse_dispatch_payload(msg)
        assert p.release_id == "rel_2026_05_07_v1.1.0"

    def test_release_id_max_length(self):
        rid = "a" * 128
        p = parse_dispatch_payload(_ok_msg(release_id=rid))
        assert p.release_id == rid


class TestRejection:
    @pytest.mark.parametrize(
        "missing",
        [
            "release_id",
            "target_version",
            "min_from_version",
            "bundle_url",
            "signature_url",
        ],
    )
    def test_missing_required_field_rejected(self, missing):
        msg = _ok_msg()
        del msg[missing]
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(msg)

    @pytest.mark.parametrize(
        "bad",
        ["", "1", "1.2", "v1.2.3", "1.2.3.4", "1.2.x", "abc", " 1.2.3"],
    )
    def test_bad_version_rejected(self, bad):
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(_ok_msg(target_version=bad))

    @pytest.mark.parametrize(
        "bad",
        ["", "rel id with spaces", "a" * 129, "rel/1", "rel:1", "rel#1"],
    )
    def test_bad_release_id_rejected(self, bad):
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(_ok_msg(release_id=bad))

    @pytest.mark.parametrize(
        "bad",
        [
            "ftp://example.com/bundle.zst",
            "file:///tmp/bundle.zst",
            "bundle.zst",
            "",
        ],
    )
    def test_bad_url_rejected(self, bad):
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(_ok_msg(bundle_url=bad))

    def test_non_object_payload_rejected(self):
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(["not", "an", "object"])
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload(None)
        with pytest.raises(DispatchPayloadError):
            parse_dispatch_payload("string")
