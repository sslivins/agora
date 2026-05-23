"""Tests for shared.devices_store -- the multi-display credential store.

PR 1 of the multi-display work introduces persist/devices.json as the
slot-keyed source of truth for device credentials.  These tests cover
the helper module in isolation: read/write/remove/wipe semantics, plus
the dual-read fallback to the legacy persist/api_key file.

Integration tests for the wired-in call sites (api/auth, cms_client's
_resolve_device_api_key / _read_api_key / api_key rotation handler /
factory_reset) live in test_devices_store_integration.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.devices_store import (
    DEVICES_FILENAME,
    KNOWN_SLOTS,
    SLOT_A,
    SLOT_B,
    devices_path,
    list_slots,
    read_api_key_with_fallback,
    read_slot,
    remove_slot,
    wipe,
    write_slot,
)


@pytest.fixture
def persist(tmp_path):
    persist = tmp_path / "persist"
    persist.mkdir()
    return persist


class TestKnownSlots:
    def test_constants(self):
        assert SLOT_A == "A"
        assert SLOT_B == "B"
        assert SLOT_A in KNOWN_SLOTS and SLOT_B in KNOWN_SLOTS

    def test_filename(self):
        assert DEVICES_FILENAME == "devices.json"


class TestDevicesPath:
    def test_joins_persist_dir(self, persist):
        assert devices_path(persist) == persist / "devices.json"

    def test_accepts_string_path(self, tmp_path):
        p = devices_path(str(tmp_path))
        assert isinstance(p, Path)
        assert p == tmp_path / "devices.json"


class TestReadSlot:
    def test_missing_file_returns_none(self, persist):
        assert read_slot(persist, SLOT_A) is None

    def test_missing_slot_returns_none(self, persist):
        devices_path(persist).write_text(json.dumps({"A": {"device_id": "d1"}}))
        assert read_slot(persist, SLOT_B) is None

    def test_returns_copy(self, persist):
        devices_path(persist).write_text(
            json.dumps({"A": {"device_id": "d1", "api_key": "k1"}})
        )
        entry = read_slot(persist, SLOT_A)
        assert entry == {"device_id": "d1", "api_key": "k1"}
        entry["api_key"] = "mutated"
        # On-disk file unchanged
        assert read_slot(persist, SLOT_A)["api_key"] == "k1"

    def test_invalid_json_treated_as_empty(self, persist):
        devices_path(persist).write_text("{ not json }")
        assert read_slot(persist, SLOT_A) is None

    def test_non_dict_root_treated_as_empty(self, persist):
        devices_path(persist).write_text(json.dumps(["A", "B"]))
        assert read_slot(persist, SLOT_A) is None

    def test_non_dict_slot_value_ignored(self, persist):
        devices_path(persist).write_text(
            json.dumps({"A": "not-a-dict", "B": {"device_id": "d2", "api_key": "k2"}})
        )
        assert read_slot(persist, SLOT_A) is None
        assert read_slot(persist, SLOT_B) == {"device_id": "d2", "api_key": "k2"}


class TestWriteSlot:
    def test_creates_file_if_absent(self, persist):
        assert not devices_path(persist).exists()
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        assert devices_path(persist).exists()
        assert read_slot(persist, SLOT_A) == {"device_id": "d1", "api_key": "k1"}

    def test_upserts_without_clobbering_other_slots(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        write_slot(persist, SLOT_B, {"device_id": "d2", "api_key": "k2"})
        assert read_slot(persist, SLOT_A) == {"device_id": "d1", "api_key": "k1"}
        assert read_slot(persist, SLOT_B) == {"device_id": "d2", "api_key": "k2"}

    def test_full_replace_at_slot_level(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1", "extra": "x"})
        # Re-write with a different shape -- old keys are dropped.
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k2"})
        assert read_slot(persist, SLOT_A) == {"device_id": "d1", "api_key": "k2"}

    def test_payload_copy_is_independent(self, persist):
        payload = {"device_id": "d1", "api_key": "k1"}
        write_slot(persist, SLOT_A, payload)
        payload["api_key"] = "mutated"
        assert read_slot(persist, SLOT_A)["api_key"] == "k1"

    def test_rejects_non_dict_payload(self, persist):
        with pytest.raises(TypeError):
            write_slot(persist, SLOT_A, "not-a-dict")  # type: ignore[arg-type]

    def test_atomic_no_temp_left_behind(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        # Only devices.json should be present after a write -- the atomic
        # write helper rewrites + renames its temp file.
        entries = sorted(p.name for p in persist.iterdir())
        assert entries == ["devices.json"]


class TestRemoveSlot:
    def test_returns_false_when_missing(self, persist):
        assert remove_slot(persist, SLOT_A) is False
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        assert remove_slot(persist, SLOT_B) is False

    def test_removes_present_slot(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        write_slot(persist, SLOT_B, {"device_id": "d2", "api_key": "k2"})
        assert remove_slot(persist, SLOT_B) is True
        assert read_slot(persist, SLOT_A) == {"device_id": "d1", "api_key": "k1"}
        assert read_slot(persist, SLOT_B) is None

    def test_removes_file_when_last_slot_gone(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        assert devices_path(persist).exists()
        assert remove_slot(persist, SLOT_A) is True
        assert not devices_path(persist).exists()


class TestListSlots:
    def test_empty_when_missing(self, persist):
        assert tuple(list_slots(persist)) == ()

    def test_returns_populated_slots(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        write_slot(persist, SLOT_B, {"device_id": "d2", "api_key": "k2"})
        assert set(list_slots(persist)) == {SLOT_A, SLOT_B}


class TestWipe:
    def test_removes_file(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "k1"})
        assert devices_path(persist).exists()
        wipe(persist)
        assert not devices_path(persist).exists()

    def test_idempotent_when_missing(self, persist):
        # No exception when file does not exist.
        wipe(persist)
        wipe(persist)


class TestReadApiKeyWithFallback:
    def test_devices_json_wins(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "new-key"})
        (persist / "api_key").write_text("legacy-key")
        assert read_api_key_with_fallback(persist) == "new-key"

    def test_legacy_when_devices_json_absent(self, persist):
        (persist / "api_key").write_text("legacy-key\n")
        assert read_api_key_with_fallback(persist) == "legacy-key"

    def test_legacy_when_devices_json_has_other_slot(self, persist):
        write_slot(persist, SLOT_B, {"device_id": "d2", "api_key": "slot-b-key"})
        (persist / "api_key").write_text("legacy-key")
        # Default slot is A -- B not consulted.
        assert read_api_key_with_fallback(persist) == "legacy-key"

    def test_legacy_when_devices_json_slot_missing_api_key(self, persist):
        # Bound but no api_key yet -- fall through to legacy file.
        write_slot(persist, SLOT_A, {"device_id": "d1"})
        (persist / "api_key").write_text("legacy-key")
        assert read_api_key_with_fallback(persist) == "legacy-key"

    def test_legacy_when_devices_json_api_key_is_blank(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "   "})
        (persist / "api_key").write_text("legacy-key")
        assert read_api_key_with_fallback(persist) == "legacy-key"

    def test_returns_empty_when_no_source(self, persist):
        assert read_api_key_with_fallback(persist) == ""

    def test_slot_b_lookup(self, persist):
        write_slot(persist, SLOT_B, {"device_id": "d2", "api_key": "slot-b-key"})
        assert read_api_key_with_fallback(persist, slot=SLOT_B) == "slot-b-key"

    def test_strips_whitespace(self, persist):
        write_slot(persist, SLOT_A, {"device_id": "d1", "api_key": "  k1  "})
        assert read_api_key_with_fallback(persist) == "k1"

    def test_custom_legacy_path(self, tmp_path):
        persist = tmp_path / "persist"
        persist.mkdir()
        legacy = tmp_path / "custom" / "legacy_api_key"
        legacy.parent.mkdir()
        legacy.write_text("custom-legacy")
        assert read_api_key_with_fallback(persist, legacy_key_path=legacy) == "custom-legacy"
