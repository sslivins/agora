"""Tests for :mod:`os_updater.cli`.

Covers:

* Dispatcher routing — empty argv, leading flags, and unknown
  subcommands fall through to :func:`os_updater.main.main` so the
  systemd unit (``python -m os_updater`` with no args) keeps working.
* ``stage`` subcommand happy path with an injected stager factory.
* ``--confirm`` gate (dry-run by default).
* ``--target-version`` is required.
* Exit codes for the documented failure classes.
* Top-level ``--help`` / ``--version`` work without dragging in
  unrelated subsystems.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable, List, Optional

import pytest

from os_updater import cli
from os_updater.apply import (
    BundleIntegrityError,
    RsyncError,
    SlotStager,
    StagingError,
    TrybootError,
)
from os_updater.bundle import BundleError


# ── helpers ───────────────────────────────────────────────────────────────


class _FakeStager:
    """Stand-in for :class:`SlotStager` used in stage_command tests.

    Records the ``stage(payload, staging_dir)`` call and either
    returns or raises the configured outcome.
    """

    def __init__(
        self,
        *,
        raises: Optional[BaseException] = None,
        on_call: Optional[Callable[..., None]] = None,
    ) -> None:
        self.calls: List[Any] = []
        self._raises = raises
        self._on_call = on_call

    async def stage(self, payload, staging_dir):  # mirrors SlotStager.stage
        self.calls.append((payload, Path(staging_dir)))
        if self._on_call is not None:
            self._on_call(payload, staging_dir)
        if self._raises is not None:
            raise self._raises


def _make_factory(stager: _FakeStager) -> Callable[..., _FakeStager]:
    captured: dict = {}

    def factory(**kwargs):
        captured.update(kwargs)
        return stager

    factory.captured = captured  # type: ignore[attr-defined]
    return factory


def _fake_bundle(tmp_path: Path, name: str = "test-bundle.tar.zst") -> Path:
    """Create a placeholder bundle file — content doesn't matter; the
    real :class:`SlotStager` isn't invoked in these tests."""

    bundle = tmp_path / name
    bundle.write_bytes(b"not-a-real-zstd-stream")
    return bundle


# ── dispatcher routing ────────────────────────────────────────────────────


def test_main_empty_argv_falls_through_to_daemon(monkeypatch):
    """Bare ``python -m os_updater`` (systemd's invocation) must hit
    the daemon entry point with no args."""

    called: dict = {}

    def fake_daemon(argv):
        called["argv"] = list(argv)
        return 42

    monkeypatch.setattr("os_updater.main.main", fake_daemon)
    rc = cli.main([])
    assert rc == 42
    assert called["argv"] == []


def test_main_leading_flag_falls_through_to_daemon(monkeypatch):
    """``python -m os_updater --log-level DEBUG`` — leading flags
    aren't subcommands; route everything to the daemon."""

    called: dict = {}

    def fake_daemon(argv):
        called["argv"] = list(argv)
        return 0

    monkeypatch.setattr("os_updater.main.main", fake_daemon)
    rc = cli.main(["--log-level", "DEBUG"])
    assert rc == 0
    assert called["argv"] == ["--log-level", "DEBUG"]


def test_main_unknown_first_arg_falls_through_to_daemon(monkeypatch):
    """Anything not in ``_SUBCOMMANDS`` is forwarded as-is to the
    daemon, which will reject it via its own argparse if invalid."""

    called: dict = {}

    def fake_daemon(argv):
        called["argv"] = list(argv)
        return 7

    monkeypatch.setattr("os_updater.main.main", fake_daemon)
    rc = cli.main(["pwiggle"])
    assert rc == 7
    assert called["argv"] == ["pwiggle"]


def test_main_explicit_daemon_subcommand_strips_keyword(monkeypatch):
    """``python -m os_updater daemon --log-level DEBUG`` — strip the
    ``daemon`` token, forward the rest to the daemon parser."""

    called: dict = {}

    def fake_daemon(argv):
        called["argv"] = list(argv)
        return 0

    monkeypatch.setattr("os_updater.main.main", fake_daemon)
    rc = cli.main(["daemon", "--log-level", "DEBUG"])
    assert rc == 0
    assert called["argv"] == ["--log-level", "DEBUG"]


def test_main_stage_routes_to_stage_command(monkeypatch):
    """The ``stage`` token hands the remaining argv to
    :func:`stage_command`, not the daemon."""

    daemon_called = {"hit": False}

    def fake_daemon(argv):
        daemon_called["hit"] = True
        return 0

    monkeypatch.setattr("os_updater.main.main", fake_daemon)

    received: dict = {}

    def fake_stage(argv, **kwargs):
        received["argv"] = list(argv)
        return 99

    monkeypatch.setattr(cli, "stage_command", fake_stage)
    rc = cli.main(["stage", "/tmp/foo", "--target-version", "1.2.3"])
    assert rc == 99
    assert received["argv"] == ["/tmp/foo", "--target-version", "1.2.3"]
    assert daemon_called["hit"] is False


def test_main_version_flag(capsys):
    """Top-level ``--version`` prints the package version without
    touching the daemon (avoids importing slot_mgr on Windows)."""

    from os_updater import __version__

    rc = cli.main(["--version"])
    out = capsys.readouterr().out
    assert rc == 0
    assert __version__ in out


def test_main_help_flag(capsys):
    """Top-level ``-h`` describes both subcommands."""

    rc = cli.main(["-h"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "stage" in out and "daemon" in out


# ── stage_command — argument validation ───────────────────────────────────


def test_stage_command_requires_target_version(tmp_path):
    """Without ``--target-version`` argparse should exit 2."""

    bundle = _fake_bundle(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        cli.stage_command([str(bundle)])
    assert excinfo.value.code == 2


def test_stage_command_dry_run_without_confirm(tmp_path, capsys):
    """Without ``--confirm``, the command prints the plan and exits 0
    without touching the staging dir or constructing a stager."""

    bundle = _fake_bundle(tmp_path)
    factory_calls = {"count": 0}

    def factory(**_):
        factory_calls["count"] += 1
        raise AssertionError("factory must not be called on dry-run")

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=factory,
    )
    assert rc == 0
    assert factory_calls["count"] == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "1.2.3" in out
    # Staging root must not have been created on a dry run.
    assert not (tmp_path / "staging").exists()


def test_stage_command_rejects_invalid_target_version(tmp_path, capsys):
    """The DispatchPayload validator rejects non-semver target_version
    → exit code 2."""

    bundle = _fake_bundle(tmp_path)
    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "not-a-version",
            "--confirm",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
    )
    err = capsys.readouterr().err
    assert rc == 2
    assert "invalid dispatch arguments" in err


def test_stage_command_missing_bundle_returns_3(tmp_path, capsys):
    """Bundle file doesn't exist → exit code 3 from
    :func:`_prepare_staging_dir`."""

    rc = cli.stage_command(
        [
            str(tmp_path / "does-not-exist.tar.zst"),
            "--target-version",
            "1.2.3",
            "--confirm",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=lambda **_: _FakeStager(),
    )
    err = capsys.readouterr().err
    assert rc == 3
    assert "failed to prepare staging dir" in err


# ── stage_command — happy path ────────────────────────────────────────────


def test_stage_command_confirm_invokes_stager(tmp_path, capsys):
    """``--confirm`` runs the full flow: prepare staging dir, build
    stager, run ``stage()``. Bundle is hard-linked into
    ``<staging-root>/<release-id>/bundle.tar.zst``."""

    bundle = _fake_bundle(tmp_path)
    staging_root = tmp_path / "staging"
    fake = _FakeStager()
    factory = _make_factory(fake)

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--release-id",
            "manual-test-001",
            "--confirm",
            "--staging-root",
            str(staging_root),
        ],
        stager_factory=factory,
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert len(fake.calls) == 1
    payload, staging_dir = fake.calls[0]
    assert staging_dir == staging_root / "manual-test-001"
    assert (staging_dir / "bundle.tar.zst").is_file()
    # Hard link → both paths see the same bytes.
    assert (staging_dir / "bundle.tar.zst").read_bytes() == bundle.read_bytes()
    # Payload satisfies the DispatchPayload contract.
    assert payload.release_id == "manual-test-001"
    assert payload.target_version == "1.2.3"
    assert payload.force_now is True
    assert payload.force_downgrade is True
    # Stager factory was called with the no_reboot flag.
    assert factory.captured == {"no_reboot": False}
    assert "EXECUTING" in out
    assert "tryboot" in out.lower()


def test_stage_command_no_reboot_passes_flag_to_factory(tmp_path):
    """``--no-reboot`` propagates through to the stager factory."""

    bundle = _fake_bundle(tmp_path)
    fake = _FakeStager()
    factory = _make_factory(fake)

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--confirm",
            "--no-reboot",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=factory,
    )
    assert rc == 0
    assert factory.captured == {"no_reboot": True}


def test_stage_command_default_release_id_when_omitted(tmp_path):
    """Omitting ``--release-id`` produces ``manual-<UTC-isotime>``."""

    bundle = _fake_bundle(tmp_path)
    fake = _FakeStager()
    factory = _make_factory(fake)

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--confirm",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=factory,
    )
    assert rc == 0
    payload, _ = fake.calls[0]
    # release_id matches manual-YYYYMMDDTHHMMSSZ
    assert re.fullmatch(r"manual-\d{8}T\d{6}Z", payload.release_id), payload.release_id


def test_stage_command_reuses_release_dir(tmp_path):
    """Re-running with the same release_id wipes any prior staging
    contents (safe to retry after a failure)."""

    bundle = _fake_bundle(tmp_path)
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    stale_dir = staging_root / "manual-replay"
    stale_dir.mkdir()
    (stale_dir / "stale.txt").write_text("debris from prior run")

    fake = _FakeStager()
    factory = _make_factory(fake)

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--release-id",
            "manual-replay",
            "--confirm",
            "--staging-root",
            str(staging_root),
        ],
        stager_factory=factory,
    )
    assert rc == 0
    # Stale file gone; fresh bundle present.
    assert not (stale_dir / "stale.txt").exists()
    assert (stale_dir / "bundle.tar.zst").exists()


# ── stage_command — failure exit codes ────────────────────────────────────


@pytest.mark.parametrize(
    "exc, expected_rc, fragment",
    [
        (BundleIntegrityError("bad meta"), 10, "bundle integrity"),
        (BundleError("zstd died"), 11, "bundle error"),
        (RsyncError("rsync exited 23"), 12, "rsync"),
        (TrybootError("autoboot rewrite failed"), 13, "tryboot"),
        (StagingError("misc staging fail"), 14, "staging failed"),
        (RuntimeError("boom"), 1, "unexpected failure"),
    ],
)
def test_stage_command_exit_codes(tmp_path, capsys, exc, expected_rc, fragment):
    """Each documented failure class maps to its own exit code with a
    distinct error message on stderr."""

    bundle = _fake_bundle(tmp_path)
    fake = _FakeStager(raises=exc)
    factory = _make_factory(fake)

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--confirm",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=factory,
    )
    err = capsys.readouterr().err
    assert rc == expected_rc
    assert fragment in err.lower()


def test_stage_command_factory_failure_returns_4(tmp_path, capsys):
    """If the stager factory itself raises (e.g. slot_mgr not
    importable), the CLI surfaces that as exit code 4 with a clear
    message — not a traceback to the terminal."""

    bundle = _fake_bundle(tmp_path)

    def factory(**_):
        raise ImportError("slot_mgr unavailable on this host")

    rc = cli.stage_command(
        [
            str(bundle),
            "--target-version",
            "1.2.3",
            "--confirm",
            "--staging-root",
            str(tmp_path / "staging"),
        ],
        stager_factory=factory,
    )
    err = capsys.readouterr().err
    assert rc == 4
    assert "SlotStager" in err
