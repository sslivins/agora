"""Tests for :mod:`os_updater.migrate` — the forward-migration runner.

Acceptance hooks (plan §"Phase 2 — Acceptance"):

* Migration runs **only after promote** — the fence-deny path raises
  :class:`MigrationFenceDenied` without touching ``SCHEMA_VERSION``.
* ``SCHEMA_VERSION`` advances 1→N in lockstep with successful script
  execution; a mid-sequence failure leaves it at the highest success.
* Discovery surfaces malformed filenames and duplicate NNNs eagerly so
  ordering is never ambiguous.

Tests inject a fake :data:`Runner` (mirrors :mod:`os_updater.bundle`'s
test pattern) so we never exec a real shell, and a fake
:data:`FenceCheckFn` so we can drive both allowed and denied branches
without a ``/data`` mount.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Callable

import pytest

from migration_fence import FenceStatus
from os_updater.migrate import (
    DEFAULT_BASH,
    DEFAULT_MIGRATION_TIMEOUT_S,
    ForwardMigrator,
    MigrationDiscoveryError,
    MigrationError,
    MigrationFenceDenied,
    MigrationResult,
    MigrationScriptError,
    MigrationStep,
    SchemaVersionError,
    apply_migration,
    discover_migrations,
    read_schema_version,
    run_pending_migrations,
    write_schema_version,
)


# ── Helpers ────────────────────────────────────────────────────────────────


def _allowed_fence(slot: int = 1) -> Callable[[], FenceStatus]:
    """Fence stub that always permits migration."""

    def _check() -> FenceStatus:
        return FenceStatus(
            allowed=True,
            reason="ok",
            allowed_slot=slot,
            running_slot=slot,
            sentinel_present=True,
            measurement={"slot": str(slot)},
        )

    return _check


def _denied_fence(
    reason: str = "sentinel missing",
    allowed_slot=None,
    running_slot=2,
    sentinel_present: bool = False,
) -> Callable[[], FenceStatus]:
    """Fence stub that always denies migration with the given reason."""

    def _check() -> FenceStatus:
        return FenceStatus(
            allowed=False,
            reason=reason,
            allowed_slot=allowed_slot,
            running_slot=running_slot,
            sentinel_present=sentinel_present,
            measurement={},
        )

    return _check


class _RecordingRunner:
    """Runner stub that records calls and returns canned results."""

    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[tuple] = []

    def __call__(self, args, *, check=False, capture_output=True, text=True, timeout=None):
        self.calls.append((tuple(args), timeout))
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


class _SequencedRunner:
    """Runner stub that yields a different return value per call.

    Used to exercise the "first migration succeeds, second fails"
    invariant: ``SCHEMA_VERSION`` must reflect the highest success, not
    the failed step's NNN.
    """

    def __init__(self, outcomes: list[tuple[int, str, str]]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple] = []

    def __call__(self, args, *, check=False, capture_output=True, text=True, timeout=None):
        self.calls.append((tuple(args), timeout))
        rc, stdout, stderr = self.outcomes.pop(0)
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=rc,
            stdout=stdout,
            stderr=stderr,
        )


def _write_migration(root: Path, name: str, body: str = "#!/bin/bash\nexit 0\n") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(body, encoding="utf-8")
    return path


# ── read_schema_version ────────────────────────────────────────────────────


class TestReadSchemaVersion:
    def test_missing_file_returns_zero(self, tmp_path):
        assert read_schema_version(tmp_path / "nope") == 0

    def test_empty_file_returns_zero(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("", encoding="utf-8")
        assert read_schema_version(p) == 0

    def test_whitespace_only_returns_zero(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("   \n  \t\n", encoding="utf-8")
        assert read_schema_version(p) == 0

    def test_valid_integer_with_trailing_newline(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("7\n", encoding="utf-8")
        assert read_schema_version(p) == 7

    def test_non_integer_raises(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("abc\n", encoding="utf-8")
        with pytest.raises(SchemaVersionError):
            read_schema_version(p)

    def test_negative_integer_raises(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("-1\n", encoding="utf-8")
        with pytest.raises(SchemaVersionError):
            read_schema_version(p)

    def test_os_error_raises_schema_version_error(self, tmp_path, monkeypatch):
        p = tmp_path / "v"
        p.write_text("3\n", encoding="utf-8")

        def boom(*args, **kwargs):
            raise OSError("disk on fire")

        monkeypatch.setattr(Path, "read_text", boom)
        with pytest.raises(SchemaVersionError):
            read_schema_version(p)


# ── write_schema_version ───────────────────────────────────────────────────


class TestWriteSchemaVersion:
    def test_writes_value_with_trailing_newline(self, tmp_path):
        p = tmp_path / "v"
        write_schema_version(p, 4)
        assert p.read_text(encoding="utf-8") == "4\n"

    def test_overwrites_existing_value_atomically(self, tmp_path):
        p = tmp_path / "v"
        p.write_text("1\n", encoding="utf-8")
        write_schema_version(p, 9)
        assert p.read_text(encoding="utf-8") == "9\n"

    def test_negative_refuses(self, tmp_path):
        with pytest.raises(SchemaVersionError):
            write_schema_version(tmp_path / "v", -2)

    def test_round_trip(self, tmp_path):
        p = tmp_path / "v"
        write_schema_version(p, 12)
        assert read_schema_version(p) == 12


# ── discover_migrations ────────────────────────────────────────────────────


class TestDiscoverMigrations:
    def test_missing_root_returns_empty(self, tmp_path):
        assert discover_migrations(tmp_path / "missing", after_version=0) == []

    def test_empty_dir_returns_empty(self, tmp_path):
        (tmp_path / "m").mkdir()
        assert discover_migrations(tmp_path / "m", after_version=0) == []

    def test_non_directory_raises(self, tmp_path):
        p = tmp_path / "m"
        p.write_text("not a dir", encoding="utf-8")
        with pytest.raises(MigrationDiscoveryError):
            discover_migrations(p, after_version=0)

    def test_returns_sorted_by_version(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "003_third.sh")
        _write_migration(root, "001_first.sh")
        _write_migration(root, "002_second.sh")
        steps = discover_migrations(root, after_version=0)
        assert [s.version for s in steps] == [1, 2, 3]
        assert [s.name for s in steps] == ["001_first", "002_second", "003_third"]

    def test_filters_by_after_version(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "001_a.sh")
        _write_migration(root, "002_b.sh")
        _write_migration(root, "003_c.sh")
        steps = discover_migrations(root, after_version=1)
        assert [s.version for s in steps] == [2, 3]

    def test_after_version_equal_excludes(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "001_a.sh")
        assert discover_migrations(root, after_version=1) == []

    def test_malformed_filename_raises(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "001_ok.sh")
        _write_migration(root, "bad_name.sh")
        with pytest.raises(MigrationDiscoveryError):
            discover_migrations(root, after_version=0)

    def test_duplicate_nnn_raises(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "001_a.sh")
        _write_migration(root, "001_b.sh")
        with pytest.raises(MigrationDiscoveryError):
            discover_migrations(root, after_version=0)

    def test_ignores_non_sh_files(self, tmp_path):
        root = tmp_path / "m"
        _write_migration(root, "001_ok.sh")
        (root / "README.md").write_text("nothing", encoding="utf-8")
        (root / "002_notes.txt").write_text("nothing", encoding="utf-8")
        steps = discover_migrations(root, after_version=0)
        assert [s.version for s in steps] == [1]


# ── apply_migration ────────────────────────────────────────────────────────


class TestApplyMigration:
    def _step(self, tmp_path: Path) -> MigrationStep:
        p = _write_migration(tmp_path / "m", "001_demo.sh")
        return MigrationStep(version=1, name="001_demo", path=p)

    def test_success_invokes_bash_with_safety_flags(self, tmp_path):
        step = self._step(tmp_path)
        runner = _RecordingRunner(returncode=0, stdout="ok\n")
        apply_migration(step, runner=runner)
        assert len(runner.calls) == 1
        args, timeout = runner.calls[0]
        assert args == (DEFAULT_BASH, "-eo", "pipefail", "-u", str(step.path))
        assert timeout == DEFAULT_MIGRATION_TIMEOUT_S

    def test_non_zero_raises_with_captured_output(self, tmp_path):
        step = self._step(tmp_path)
        runner = _RecordingRunner(returncode=2, stdout="boom\n", stderr="kapow\n")
        with pytest.raises(MigrationScriptError) as excinfo:
            apply_migration(step, runner=runner)
        err = excinfo.value
        assert err.returncode == 2
        assert err.stdout == "boom\n"
        assert err.stderr == "kapow\n"
        assert err.step is step

    def test_timeout_mapped_to_rc_124(self, tmp_path):
        step = self._step(tmp_path)

        def runner(args, **kwargs):
            raise subprocess.TimeoutExpired(cmd=args, timeout=1.0, output="partial")

        with pytest.raises(MigrationScriptError) as excinfo:
            apply_migration(step, runner=runner, timeout_s=1.0)
        assert excinfo.value.returncode == 124
        assert "timed out" in excinfo.value.stderr

    def test_file_not_found_mapped_to_rc_127(self, tmp_path):
        step = self._step(tmp_path)

        def runner(args, **kwargs):
            raise FileNotFoundError("bash gone")

        with pytest.raises(MigrationScriptError) as excinfo:
            apply_migration(step, runner=runner)
        assert excinfo.value.returncode == 127

    def test_generic_os_error_mapped_to_rc_neg1(self, tmp_path):
        step = self._step(tmp_path)

        def runner(args, **kwargs):
            raise OSError("EIO")

        with pytest.raises(MigrationScriptError) as excinfo:
            apply_migration(step, runner=runner)
        assert excinfo.value.returncode == -1


# ── run_pending_migrations ─────────────────────────────────────────────────


class TestRunPendingMigrations:
    def test_fence_denied_raises_without_touching_schema(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("1\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "002_change.sh")

        with pytest.raises(MigrationFenceDenied) as excinfo:
            run_pending_migrations(
                schema_version_path=schema,
                migrations_root=root,
                runner=_RecordingRunner(),
                fence_check_fn=_denied_fence(reason="sentinel missing"),
            )

        # Schema untouched on fence deny — that's the whole point.
        assert schema.read_text(encoding="utf-8") == "1\n"
        # FenceStatus rides on the exception for telemetry.
        assert excinfo.value.status.reason == "sentinel missing"

    def test_no_pending_returns_noop_result(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("5\n", encoding="utf-8")
        root = tmp_path / "m"
        # Migrations all <= current; nothing pending.
        _write_migration(root, "001_old.sh")
        _write_migration(root, "005_also_old.sh")

        runner = _RecordingRunner()
        result = run_pending_migrations(
            schema_version_path=schema,
            migrations_root=root,
            runner=runner,
            fence_check_fn=_allowed_fence(),
        )

        assert isinstance(result, MigrationResult)
        assert result.starting_version == 5
        assert result.ending_version == 5
        assert result.applied == ()
        assert runner.calls == []
        assert schema.read_text(encoding="utf-8") == "5\n"

    def test_applies_in_ascending_order_and_bumps_schema(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("0\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "003_c.sh")
        _write_migration(root, "001_a.sh")
        _write_migration(root, "002_b.sh")

        runner = _RecordingRunner(returncode=0)
        result = run_pending_migrations(
            schema_version_path=schema,
            migrations_root=root,
            runner=runner,
            fence_check_fn=_allowed_fence(),
        )

        assert [args[-1] for (args, _to) in runner.calls] == [
            str(root / "001_a.sh"),
            str(root / "002_b.sh"),
            str(root / "003_c.sh"),
        ]
        assert result.starting_version == 0
        assert result.ending_version == 3
        assert [s.version for s in result.applied] == [1, 2, 3]
        assert schema.read_text(encoding="utf-8") == "3\n"

    def test_mid_sequence_failure_leaves_schema_at_last_success(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("1\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "002_ok.sh")
        _write_migration(root, "003_bad.sh")
        _write_migration(root, "004_never.sh")

        # 002 succeeds, 003 fails, 004 must not run.
        runner = _SequencedRunner(
            outcomes=[
                (0, "two ok\n", ""),
                (7, "", "three exploded\n"),
            ]
        )

        with pytest.raises(MigrationScriptError) as excinfo:
            run_pending_migrations(
                schema_version_path=schema,
                migrations_root=root,
                runner=runner,
                fence_check_fn=_allowed_fence(),
            )

        # 004 was queued but never executed.
        assert len(runner.calls) == 2
        assert excinfo.value.step.version == 3
        # Schema reflects 002 (the highest success), not 003.
        assert schema.read_text(encoding="utf-8") == "2\n"

    def test_filters_only_pending(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("2\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "001_done.sh")
        _write_migration(root, "002_done.sh")
        _write_migration(root, "003_new.sh")

        runner = _RecordingRunner(returncode=0)
        result = run_pending_migrations(
            schema_version_path=schema,
            migrations_root=root,
            runner=runner,
            fence_check_fn=_allowed_fence(),
        )

        # Only the new one ran.
        assert len(runner.calls) == 1
        assert [s.version for s in result.applied] == [3]
        assert result.starting_version == 2
        assert result.ending_version == 3
        assert schema.read_text(encoding="utf-8") == "3\n"


# ── ForwardMigrator (async adapter) ────────────────────────────────────────


class TestForwardMigrator:
    def test_run_awaits_pending_and_stashes_result(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("0\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "001_first.sh")

        runner = _RecordingRunner(returncode=0)
        migrator = ForwardMigrator(
            schema_version_path=schema,
            migrations_root=root,
            runner=runner,
            fence_check_fn=_allowed_fence(),
        )

        asyncio.run(migrator.run())

        assert migrator.last_result is not None
        assert migrator.last_result.ending_version == 1
        assert [s.version for s in migrator.last_result.applied] == [1]
        assert schema.read_text(encoding="utf-8") == "1\n"

    def test_run_propagates_fence_denied(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("1\n", encoding="utf-8")
        migrator = ForwardMigrator(
            schema_version_path=schema,
            migrations_root=tmp_path / "m",
            runner=_RecordingRunner(),
            fence_check_fn=_denied_fence(),
        )

        with pytest.raises(MigrationFenceDenied):
            asyncio.run(migrator.run())

        assert migrator.last_result is None
        # Schema preserved.
        assert schema.read_text(encoding="utf-8") == "1\n"

    def test_run_propagates_script_error(self, tmp_path):
        schema = tmp_path / "SCHEMA_VERSION"
        schema.write_text("0\n", encoding="utf-8")
        root = tmp_path / "m"
        _write_migration(root, "001_boom.sh")

        runner = _RecordingRunner(returncode=2, stderr="nope\n")
        migrator = ForwardMigrator(
            schema_version_path=schema,
            migrations_root=root,
            runner=runner,
            fence_check_fn=_allowed_fence(),
        )

        with pytest.raises(MigrationScriptError):
            asyncio.run(migrator.run())

        assert migrator.last_result is None
        # First migration never recorded a success.
        assert schema.read_text(encoding="utf-8") == "0\n"


# ── Exception hierarchy sanity ─────────────────────────────────────────────


class TestExceptionHierarchy:
    """Lock the subclass relationships the service classifier depends on."""

    def test_all_subclass_migration_error(self):
        assert issubclass(MigrationFenceDenied, MigrationError)
        assert issubclass(MigrationScriptError, MigrationError)
        assert issubclass(SchemaVersionError, MigrationError)
        assert issubclass(MigrationDiscoveryError, MigrationError)
