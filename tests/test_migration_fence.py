"""Unit tests for :mod:`migration_fence`.

The fence reader is built around three injectable seams
(``sentinel_path``, ``sentinel_reader``, ``slot_state_fn``) so every test
runs on a developer laptop without ``/data``, ``slot_mgr``, or
``/proc/cmdline``. We exercise the happy path, every documented deny
path, and the convenience wrapper + CLI.
"""

from __future__ import annotations

import io
import json
import sys
from collections import namedtuple
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

from migration_fence import (
    DEFAULT_SENTINEL_PATH,
    FenceStatus,
    MigrationFenceError,
    check_migration_fence,
    is_migration_allowed,
    parse_sentinel,
)
from migration_fence import core as fence_core


# ── Helpers ─────────────────────────────────────────────────────────────────


# Mirrors the ``running_slot`` attribute of ``slot_mgr.SlotStatus`` without
# importing slot_mgr - the fence only reads that one field.
FakeSlotStatus = namedtuple("FakeSlotStatus", ["running_slot"])


def _reader_returning(payload: Optional[str]):
    """Build a fake ``sentinel_reader`` that returns ``payload``."""
    calls: list[Path] = []

    def fn(path: Path) -> Optional[str]:
        calls.append(path)
        return payload

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _reader_raising(exc: Exception):
    """Build a fake ``sentinel_reader`` that always raises ``exc``."""
    calls: list[Path] = []

    def fn(path: Path) -> Optional[str]:
        calls.append(path)
        raise exc

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _slot(running: Optional[int]):
    """Build a fake ``slot_state_fn`` that yields the given running slot."""
    calls: list[int] = []

    def fn() -> FakeSlotStatus:
        calls.append(len(calls))
        return FakeSlotStatus(running_slot=running)

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _slot_raising(exc: Exception):
    """Build a fake ``slot_state_fn`` that always raises ``exc``."""
    def fn() -> FakeSlotStatus:
        raise exc

    return fn


# ── parse_sentinel ──────────────────────────────────────────────────────────


class TestParseSentinel:
    def test_empty_string_returns_empty_dict(self):
        assert parse_sentinel("") == {}

    def test_whitespace_only_returns_empty_dict(self):
        assert parse_sentinel("   \n\n  \t\n") == {}

    def test_single_key_value(self):
        assert parse_sentinel("slot=2\n") == {"slot": "2"}

    def test_multiple_keys(self):
        text = "slot=1\npromoted_at=2026-04-22T18:30:00+00:00\n"
        result = parse_sentinel(text)
        assert result == {
            "slot": "1",
            "promoted_at": "2026-04-22T18:30:00+00:00",
        }

    def test_strips_surrounding_whitespace_on_key_and_value(self):
        assert parse_sentinel("  slot  =  2  \n") == {"slot": "2"}

    def test_lines_without_equals_are_skipped(self):
        text = "slot=1\nthis is a comment\npromoted_at=now\n"
        assert parse_sentinel(text) == {"slot": "1", "promoted_at": "now"}

    def test_blank_lines_are_skipped(self):
        text = "\nslot=1\n\n\npromoted_at=now\n\n"
        assert parse_sentinel(text) == {"slot": "1", "promoted_at": "now"}

    def test_empty_key_is_skipped(self):
        # "=value" has an empty key after strip; we tolerate it silently.
        assert parse_sentinel("=value\nslot=2\n") == {"slot": "2"}

    def test_value_can_be_empty_string(self):
        # Empty value is preserved - the caller decides if it's meaningful.
        assert parse_sentinel("slot=\n") == {"slot": ""}

    def test_duplicate_keys_last_wins(self):
        assert parse_sentinel("slot=1\nslot=2\n") == {"slot": "2"}

    def test_value_containing_equals_sign_is_kept_intact(self):
        # ``partition`` splits on the first ``=`` only; later ``='s land
        # in the value verbatim. Lets us forward future structured fields
        # without re-engineering the parser.
        assert parse_sentinel("note=a=b=c\n") == {"note": "a=b=c"}

    def test_no_trailing_newline(self):
        # Last line missing \n must still parse.
        assert parse_sentinel("slot=2") == {"slot": "2"}

    def test_crlf_line_endings(self):
        # ``str.splitlines`` handles \r\n natively.
        assert parse_sentinel("slot=1\r\npromoted_at=x\r\n") == {
            "slot": "1",
            "promoted_at": "x",
        }


# ── check_migration_fence: sentinel absent / unreadable ─────────────────────


class TestCheckMigrationFenceSentinelAbsent:
    def test_missing_file_denies(self):
        reader = _reader_returning(None)
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert result.sentinel_present is False
        assert "absent" in result.reason
        assert result.allowed_slot is None
        # running_slot is still populated as a courtesy
        assert result.running_slot == 1
        assert result.measurement == {}

    def test_missing_file_still_reports_running_slot_when_state_raises(self):
        # Both the sentinel is missing and slot_state explodes - reason
        # is still the absent-sentinel path; running_slot is None.
        reader = _reader_returning(None)
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot_raising(RuntimeError("no cmdline")),
        )
        assert result.allowed is False
        assert "absent" in result.reason
        assert result.running_slot is None

    def test_reader_raises_permission_error_denies(self):
        reader = _reader_raising(PermissionError("denied"))
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert result.sentinel_present is False
        assert "unreadable" in result.reason
        assert "PermissionError" in result.reason
        assert result.running_slot == 1

    def test_reader_raises_os_error_denies(self):
        reader = _reader_raising(OSError(5, "I/O error"))
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(2),
        )
        assert result.allowed is False
        assert "unreadable" in result.reason
        assert "OSError" in result.reason

    def test_reader_called_with_default_path(self):
        reader = _reader_returning(None)
        check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert reader.calls == [Path(DEFAULT_SENTINEL_PATH)]

    def test_reader_called_with_custom_path(self, tmp_path):
        custom = tmp_path / "elsewhere"
        reader = _reader_returning(None)
        check_migration_fence(
            sentinel_path=custom,
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert reader.calls == [custom]


# ── check_migration_fence: malformed sentinel ───────────────────────────────


class TestCheckMigrationFenceMalformedSentinel:
    def test_missing_slot_key_denies(self):
        reader = _reader_returning("promoted_at=2026-04-22\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert result.sentinel_present is True
        assert "missing 'slot' key" in result.reason
        assert result.allowed_slot is None
        # measurement still surfaces what we did parse
        assert result.measurement == {"promoted_at": "2026-04-22"}

    def test_empty_slot_value_denies(self):
        reader = _reader_returning("slot=\npromoted_at=now\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert "missing 'slot' key" in result.reason
        assert result.allowed_slot is None

    def test_non_integer_slot_value_denies(self):
        reader = _reader_returning("slot=abc\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert result.sentinel_present is True
        assert "not an integer" in result.reason
        assert result.allowed_slot is None
        assert result.measurement == {"slot": "abc"}

    def test_slot_zero_denies(self):
        reader = _reader_returning("slot=0\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert "not a valid A/B slot" in result.reason
        assert result.allowed_slot == 0  # int parsed, but rejected by range check

    def test_slot_three_denies(self):
        reader = _reader_returning("slot=3\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert "not a valid A/B slot" in result.reason
        assert result.allowed_slot == 3

    def test_negative_slot_denies(self):
        reader = _reader_returning("slot=-1\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert "not a valid A/B slot" in result.reason


# ── check_migration_fence: running-slot indeterminate ───────────────────────


class TestCheckMigrationFenceRunningSlotIndeterminate:
    def test_running_slot_none_denies(self):
        reader = _reader_returning("slot=1\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(None),
        )
        assert result.allowed is False
        assert result.sentinel_present is True
        assert "running slot unknown" in result.reason
        assert result.allowed_slot == 1
        assert result.running_slot is None

    def test_slot_state_raises_denies(self):
        reader = _reader_returning("slot=1\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot_raising(RuntimeError("cmdline parse failed")),
        )
        assert result.allowed is False
        assert "slot_state unavailable" in result.reason
        assert "RuntimeError" in result.reason
        assert result.allowed_slot == 1
        assert result.running_slot is None

    def test_slot_state_returns_object_without_running_slot_attr_denies(self):
        # If someone wires us a stand-in that doesn't expose
        # ``running_slot``, ``getattr`` returns ``None`` and we treat it
        # exactly like a real None.
        reader = _reader_returning("slot=1\n")

        class WeirdStatus:
            pass

        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=lambda: WeirdStatus(),
        )
        assert result.allowed is False
        assert "running slot unknown" in result.reason


# ── check_migration_fence: slot mismatch ────────────────────────────────────


class TestCheckMigrationFenceSlotMismatch:
    def test_sentinel_slot_2_running_slot_1_denies(self):
        reader = _reader_returning("slot=2\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is False
        assert result.sentinel_present is True
        assert "slot 2" in result.reason
        assert "slot 1" in result.reason
        assert "tryboot-revert" in result.reason
        assert result.allowed_slot == 2
        assert result.running_slot == 1

    def test_sentinel_slot_1_running_slot_2_denies(self):
        reader = _reader_returning("slot=1\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(2),
        )
        assert result.allowed is False
        assert result.allowed_slot == 1
        assert result.running_slot == 2


# ── check_migration_fence: happy path ───────────────────────────────────────


class TestCheckMigrationFenceSlotMatch:
    def test_slot_1_match_allows(self):
        reader = _reader_returning("slot=1\npromoted_at=2026-04-22T18:00:00+00:00\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is True
        assert result.sentinel_present is True
        assert result.allowed_slot == 1
        assert result.running_slot == 1
        assert "matches running slot 1" in result.reason
        assert result.measurement == {
            "slot": "1",
            "promoted_at": "2026-04-22T18:00:00+00:00",
        }

    def test_slot_2_match_allows(self):
        reader = _reader_returning("slot=2\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(2),
        )
        assert result.allowed is True
        assert result.allowed_slot == 2
        assert result.running_slot == 2

    def test_extra_keys_tolerated(self):
        # Future slot_mgr versions can add fields without breaking the
        # consumer. ``measurement`` carries them through for telemetry.
        reader = _reader_returning(
            "slot=1\npromoted_at=now\nfuture_field=hello\n"
        )
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is True
        assert result.measurement["future_field"] == "hello"

    def test_whitespace_around_slot_value_tolerated(self):
        reader = _reader_returning("slot =  2  \n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(2),
        )
        assert result.allowed is True
        assert result.allowed_slot == 2

    def test_measurement_is_a_plain_dict(self):
        # measurement is materialized into a dict so callers can json.dumps it
        # without surprises.
        reader = _reader_returning("slot=1\n")
        result = check_migration_fence(
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert isinstance(result.measurement, dict)
        # And the dataclass itself is JSON-friendly via dataclasses.asdict
        # (used by the CLI).
        import dataclasses
        json.dumps(dataclasses.asdict(result))


# ── is_migration_allowed convenience wrapper ────────────────────────────────


class TestIsMigrationAllowed:
    def test_returns_true_on_match(self):
        reader = _reader_returning("slot=1\n")
        assert (
            is_migration_allowed(
                sentinel_reader=reader,
                slot_state_fn=_slot(1),
            )
            is True
        )

    def test_returns_false_on_mismatch(self):
        reader = _reader_returning("slot=1\n")
        assert (
            is_migration_allowed(
                sentinel_reader=reader,
                slot_state_fn=_slot(2),
            )
            is False
        )

    def test_returns_false_on_missing_sentinel(self):
        assert (
            is_migration_allowed(
                sentinel_reader=_reader_returning(None),
                slot_state_fn=_slot(1),
            )
            is False
        )

    def test_returns_false_on_reader_exception(self):
        assert (
            is_migration_allowed(
                sentinel_reader=_reader_raising(OSError("io")),
                slot_state_fn=_slot(1),
            )
            is False
        )

    def test_passes_through_custom_path(self, tmp_path):
        custom = tmp_path / "x"
        reader = _reader_returning(None)
        is_migration_allowed(
            sentinel_path=custom,
            sentinel_reader=reader,
            slot_state_fn=_slot(1),
        )
        assert reader.calls == [custom]


# ── Default reader (touches the filesystem) ─────────────────────────────────


class TestDefaultSentinelReader:
    def test_returns_text_when_file_exists(self, tmp_path):
        p = tmp_path / "sentinel"
        p.write_text("slot=2\npromoted_at=now\n", encoding="utf-8")
        assert fence_core._default_sentinel_reader(p) == "slot=2\npromoted_at=now\n"

    def test_returns_none_when_file_missing(self, tmp_path):
        p = tmp_path / "nope"
        assert fence_core._default_sentinel_reader(p) is None

    def test_end_to_end_with_real_file(self, tmp_path):
        # Drive the public API with the real reader + a fake slot state.
        sentinel = tmp_path / "migration-allowed"
        sentinel.write_text("slot=1\npromoted_at=now\n", encoding="utf-8")
        result = check_migration_fence(
            sentinel_path=sentinel,
            slot_state_fn=_slot(1),
        )
        assert result.allowed is True
        assert result.allowed_slot == 1


# ── Default slot_state: lazy import contract ────────────────────────────────


class TestDefaultSlotStateLazyImport:
    def test_default_slot_state_imports_and_calls_slot_mgr(self, monkeypatch):
        # The body of _default_slot_state does ``from slot_mgr import
        # slot_state`` lazily. If a future refactor turns it back into a
        # module-level import, the test environment without slot_mgr
        # installed would import-fail before this test even runs - so the
        # presence of this test together with a passing CI is the proof
        # that the import really is lazy.
        called = {"hit": False}

        def fake_slot_state():
            called["hit"] = True
            return FakeSlotStatus(running_slot=1)

        # Patch the symbol on the slot_mgr module that the lazy import
        # would resolve. Provide a fake module if slot_mgr isn't
        # installed - safer than touching the real one.
        import types

        fake_mod = types.ModuleType("slot_mgr")
        fake_mod.slot_state = fake_slot_state  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "slot_mgr", fake_mod)

        status = fence_core._default_slot_state()
        assert called["hit"] is True
        assert status.running_slot == 1

    def test_check_migration_fence_uses_default_slot_state_when_unset(
        self, monkeypatch
    ):
        # End-to-end: drive the public function with no slot_state_fn
        # override and verify it routes through _default_slot_state ->
        # slot_mgr.slot_state.
        import types

        fake_mod = types.ModuleType("slot_mgr")
        fake_mod.slot_state = lambda: FakeSlotStatus(running_slot=1)  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "slot_mgr", fake_mod)

        reader = _reader_returning("slot=1\n")
        result = check_migration_fence(sentinel_reader=reader)
        assert result.allowed is True
        assert result.running_slot == 1


# ── Module surface ──────────────────────────────────────────────────────────


class TestModuleSurface:
    def test_fence_status_is_frozen(self):
        s = FenceStatus(
            allowed=True,
            reason="x",
            allowed_slot=1,
            running_slot=1,
            sentinel_present=True,
        )
        with pytest.raises(Exception):
            s.allowed = False  # type: ignore[misc]

    def test_fence_status_measurement_defaults_to_empty(self):
        s = FenceStatus(
            allowed=False,
            reason="r",
            allowed_slot=None,
            running_slot=None,
            sentinel_present=False,
        )
        assert s.measurement == {}

    def test_migration_fence_error_is_runtimeerror(self):
        assert issubclass(MigrationFenceError, RuntimeError)

    def test_default_sentinel_path_is_under_data_agora(self):
        # Must agree with slot_mgr.paths.migration_allowed_sentinel_path()
        # on the device. Hard-coding here means a refactor on either side
        # will fail this test instead of silently disagreeing.
        assert DEFAULT_SENTINEL_PATH == "/data/agora/migration-allowed"

    def test_public_api_exported_at_package_level(self):
        import migration_fence

        # Everything in __all__ must actually resolve as an attribute.
        for name in migration_fence.__all__:
            assert hasattr(migration_fence, name), name

    def test_version_present(self):
        import migration_fence

        assert isinstance(migration_fence.__version__, str)
        # Loose sanity check - not enforcing semver shape, just non-empty.
        assert migration_fence.__version__

    def test_all_list_shape(self):
        import migration_fence

        # Bundles the obviously-public names. Tightens the contract so a
        # refactor can't silently drop or rename a symbol.
        assert set(migration_fence.__all__) == {
            "DEFAULT_SENTINEL_PATH",
            "FenceStatus",
            "MigrationFenceError",
            "__version__",
            "check_migration_fence",
            "is_migration_allowed",
            "parse_sentinel",
        }


# ── CLI (python -m migration_fence) ─────────────────────────────────────────


class TestCLI:
    def _run(self, argv, *, fence_result):
        """Invoke ``__main__.main`` with the given argv + patched fence.

        Returns ``(exit_code, parsed_json_stdout)``.
        """
        from migration_fence import __main__ as cli

        out = io.StringIO()
        with patch.object(cli, "check_migration_fence", return_value=fence_result):
            with patch.object(sys, "stdout", out):
                rc = cli.main(argv)
        return rc, json.loads(out.getvalue())

    def _allowed(self):
        return FenceStatus(
            allowed=True,
            reason="sentinel matches running slot 1",
            allowed_slot=1,
            running_slot=1,
            sentinel_present=True,
            measurement={"slot": "1"},
        )

    def _denied(self, reason="sentinel absent"):
        return FenceStatus(
            allowed=False,
            reason=reason,
            allowed_slot=None,
            running_slot=None,
            sentinel_present=False,
            measurement={},
        )

    def test_no_check_flag_returns_zero_even_when_denied(self):
        # Plain ``python -m migration_fence`` is meant for human
        # inspection - it should always exit 0 and just print status.
        rc, payload = self._run([], fence_result=self._denied())
        assert rc == 0
        assert payload["allowed"] is False
        assert payload["reason"] == "sentinel absent"

    def test_check_flag_zero_when_allowed(self):
        rc, payload = self._run(["--check"], fence_result=self._allowed())
        assert rc == 0
        assert payload["allowed"] is True
        assert payload["allowed_slot"] == 1
        assert payload["running_slot"] == 1
        assert payload["measurement"] == {"slot": "1"}

    def test_check_flag_one_when_denied(self):
        rc, payload = self._run(
            ["--check"], fence_result=self._denied("sentinel absent at /x")
        )
        assert rc == 1
        assert payload["allowed"] is False
        assert payload["reason"] == "sentinel absent at /x"

    def test_output_is_valid_json(self):
        # Both --check and no-args paths must emit parseable JSON. We've
        # already proven it parses in _run; here we just check structure.
        rc, payload = self._run([], fence_result=self._allowed())
        assert rc == 0
        for key in (
            "allowed",
            "reason",
            "allowed_slot",
            "running_slot",
            "sentinel_present",
            "measurement",
        ):
            assert key in payload

    def test_measurement_is_a_dict_even_when_empty(self):
        rc, payload = self._run([], fence_result=self._denied())
        assert isinstance(payload["measurement"], dict)
