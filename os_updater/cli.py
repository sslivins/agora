"""Top-level command dispatcher for ``python -m os_updater``.

``os_updater`` ships two operator-facing entrypoints:

``daemon`` (default)
    The long-running WPS-driven update service. Invoked by
    ``systemd/agora-os-updater.service`` as ``python3 -m os_updater``.
    Implementation lives in :func:`os_updater.main.main`.

``stage <bundle>``
    A break-glass / QA tool that installs a locally-resident bundle
    into the inactive slot and (optionally) triggers tryboot. This
    bypasses the CMS dispatch + download + signature-verify phases —
    use it for:

    - Field recovery when a Pi can't reach CMS
    - Pre-release QA validation of a bundle on lab hardware
    - Phase-by-phase acceptance testing while the rest of the update
      pipeline is still being built

    The bundle is not downloaded, not signature-checked, and not
    floor-checked against ``min_from_version``. The operator is
    asserting trust in the bundle file they're passing in.

Backward compatibility
----------------------
``python -m os_updater`` with no subcommand (or any leading flag like
``--log-level=DEBUG``) falls through to the daemon, so the existing
systemd unit keeps working unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import logging
import os
import shutil
import sys
from functools import partial
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from os_updater import __version__
from os_updater.apply import (
    DEFAULT_BUNDLE_FILENAME,
    BundleIntegrityError,
    RsyncError,
    SlotStager,
    StagingError,
    TrybootError,
)
from os_updater.bundle import BundleError
from os_updater.dispatch import DispatchPayload
from os_updater.service import DEFAULT_STAGING_ROOT


log = logging.getLogger("agora.os_updater.cli")


#: Subcommands the dispatcher knows about. Anything else (or empty
#: argv) falls through to the daemon.
_SUBCOMMANDS = ("stage", "daemon")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stdout,
    )


def _build_stage_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="os_updater stage",
        description=(
            "Manually install an OS bundle into the inactive slot and "
            "trigger tryboot. Bypasses CMS dispatch, download, and "
            "signature verification — the operator is asserting trust "
            "in the bundle file."
        ),
    )
    p.add_argument(
        "bundle",
        type=Path,
        help="Path to the .tar.zst bundle file on local disk.",
    )
    p.add_argument(
        "--target-version",
        required=True,
        help=(
            "Semver of the OS version this bundle delivers. Must match "
            "the bundle's meta.json version (defense-in-depth check)."
        ),
    )
    p.add_argument(
        "--release-id",
        default=None,
        help=(
            "Opaque release identifier (logged + persisted in updater "
            "state). Defaults to manual-<UTC-isotime>."
        ),
    )
    p.add_argument(
        "--min-from-version",
        default="0.0.0",
        help=(
            "Minimum running version required by this bundle. Not "
            "enforced in manual mode — included for parity with the "
            "DispatchPayload schema. Default: 0.0.0 (permissive)."
        ),
    )
    p.add_argument(
        "--staging-root",
        type=Path,
        default=DEFAULT_STAGING_ROOT,
        help=(
            "Parent directory under which per-install staging dirs are "
            f"created (default: {DEFAULT_STAGING_ROOT})."
        ),
    )
    p.add_argument(
        "--no-reboot",
        action="store_true",
        help=(
            "Stage the bundle and arm tryboot, but do not actually "
            "reboot. Useful for inspecting the inactive slot before "
            "handing control to the bootloader."
        ),
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help=(
            "Required to actually run. Without it, the command prints "
            "the install plan and exits with code 0 (dry-run)."
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level (default: INFO).",
    )
    return p


def _default_release_id() -> str:
    """``manual-YYYYMMDDTHHMMSSZ`` — passes DispatchPayload's regex."""

    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"manual-{now}"


def _prepare_staging_dir(
    bundle: Path,
    staging_root: Path,
    release_id: str,
    *,
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME,
) -> Path:
    """Create ``<staging_root>/<release_id>/`` and link the bundle into it.

    Returns the staging directory path. Uses a hard link when the
    bundle and staging root are on the same filesystem (cheap, no
    extra disk), falling back to ``shutil.copy2`` for cross-device
    cases. We avoid symlinks because some zstd / tar builds resolve
    them in surprising ways under sandboxed mounts.
    """

    if not bundle.is_file():
        raise FileNotFoundError(f"bundle file not found: {bundle}")

    staging_root.mkdir(parents=True, exist_ok=True)
    staging_dir = staging_root / release_id
    if staging_dir.exists():
        # Clear any prior debris from a re-run with the same release_id.
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    target = staging_dir / bundle_filename
    try:
        os.link(str(bundle), str(target))
    except OSError:
        # Cross-device or platform that refuses hard links — copy.
        shutil.copy2(str(bundle), str(target))
    return staging_dir


def _make_stager(*, no_reboot: bool) -> SlotStager:
    """Construct a production :class:`SlotStager`.

    When ``no_reboot`` is True, we wrap :func:`slot_mgr.trigger_tryboot`
    with ``reboot=False`` so the bootloader is armed but the device
    stays up. Lazy-imports slot_mgr so unit tests can exercise the
    rest of the module without the slot_mgr deps installed.
    """

    import slot_mgr  # local import per docstring

    trigger_fn: Optional[Callable[[int], Any]] = None
    if no_reboot:
        trigger_fn = partial(slot_mgr.trigger_tryboot, reboot=False)

    return SlotStager(
        slot_state_fn=slot_mgr.slot_state,
        trigger_tryboot_fn=trigger_fn,
    )


def stage_command(
    argv: Sequence[str],
    *,
    stager_factory: Callable[..., SlotStager] = _make_stager,
    asyncio_run: Callable[[Any], Any] = asyncio.run,
    out: Any = None,
    err: Any = None,
) -> int:
    """Implementation of ``python -m os_updater stage <bundle> ...``.

    Returns a process exit code. Injection seams (``stager_factory``,
    ``asyncio_run``, ``out``, ``err``) keep this testable without
    real subprocesses or a real Pi. ``out``/``err`` resolve to the
    *current* ``sys.stdout`` / ``sys.stderr`` when None, so pytest's
    ``capsys`` (which swaps the module-level streams at test time)
    can capture our output.
    """

    if out is None:
        out = sys.stdout
    if err is None:
        err = sys.stderr

    parser = _build_stage_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)

    release_id = args.release_id or _default_release_id()

    # Synthesize a DispatchPayload that satisfies the validators.
    # bundle_url / signature_url aren't used by SlotStager — the
    # download phase ran before stage() in the daemon flow — so we
    # drop in https placeholders that satisfy the http(s)-scheme
    # check but are never fetched.
    try:
        payload = DispatchPayload(
            release_id=release_id,
            target_version=args.target_version,
            min_from_version=args.min_from_version,
            bundle_url=f"https://manual.invalid/{release_id}.tar.zst",
            signature_url=f"https://manual.invalid/{release_id}.tar.zst.minisig",
            force_now=True,
            force_downgrade=True,
        )
    except Exception as exc:  # pydantic ValidationError
        print(f"error: invalid dispatch arguments: {exc}", file=err)
        return 2

    # Build the install plan summary before doing anything destructive.
    reboot_note = (
        "Device will NOT auto-reboot (--no-reboot); inspect inactive "
        "slot then reboot manually to attempt tryboot."
        if args.no_reboot
        else "Device WILL REBOOT to attempt the new slot via tryboot."
    )
    plan = (
        f"Bundle:         {args.bundle}\n"
        f"Release ID:     {release_id}\n"
        f"Target version: {args.target_version}\n"
        f"Staging root:   {args.staging_root}\n"
        f"\n{reboot_note}\n"
    )

    if not args.confirm:
        print("Manual OS install — DRY RUN (pass --confirm to execute):", file=out)
        print(plan, file=out)
        return 0

    print("Manual OS install — EXECUTING:", file=out)
    print(plan, file=out)

    try:
        staging_dir = _prepare_staging_dir(
            args.bundle, args.staging_root, release_id
        )
    except (FileNotFoundError, OSError) as exc:
        print(f"error: failed to prepare staging dir: {exc}", file=err)
        return 3

    print(f"Staging dir:    {staging_dir}", file=out)

    try:
        stager = stager_factory(no_reboot=args.no_reboot)
    except Exception as exc:
        print(f"error: failed to build SlotStager: {exc}", file=err)
        return 4

    try:
        asyncio_run(stager.stage(payload, staging_dir))
    except BundleIntegrityError as exc:
        print(f"error: bundle integrity check failed: {exc}", file=err)
        return 10
    except BundleError as exc:
        print(f"error: bundle error: {exc}", file=err)
        return 11
    except RsyncError as exc:
        print(f"error: rsync to inactive slot failed: {exc}", file=err)
        return 12
    except TrybootError as exc:
        print(f"error: tryboot arming failed: {exc}", file=err)
        return 13
    except StagingError as exc:
        print(f"error: staging failed: {exc}", file=err)
        return 14
    except Exception as exc:
        print(f"error: unexpected failure: {exc}", file=err)
        log.exception("manual stage failed")
        return 1

    if args.no_reboot:
        print(
            "\nStaging complete. Tryboot is armed but reboot was "
            "suppressed. Run `reboot` when ready, or revert the boot "
            "config via slot_mgr if you change your mind.",
            file=out,
        )
    else:
        # We shouldn't actually reach here when slot_mgr.trigger_tryboot
        # successfully reboots, but a test/fake might return normally.
        print(
            "\nStaging complete and tryboot triggered. Device should "
            "now be rebooting.",
            file=out,
        )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Top-level dispatcher invoked by ``python -m os_updater``.

    Routes the ``stage`` and ``daemon`` subcommands. Anything else
    (empty argv, leading ``--flag``, unknown first arg) falls through
    to the daemon entry point so the existing systemd unit keeps
    working without modification.
    """

    if argv is None:
        argv = sys.argv[1:]

    # Top-level --help / --version: handle here so they describe the
    # dispatcher (which knows about subcommands), not just the daemon.
    if argv and argv[0] in ("-h", "--help"):
        _print_top_level_help()
        return 0
    if argv and argv[0] == "--version":
        print(f"os_updater {__version__}")
        return 0

    if argv and argv[0] in _SUBCOMMANDS:
        sub = argv[0]
        rest = list(argv[1:])
        if sub == "stage":
            return stage_command(rest)
        if sub == "daemon":
            from os_updater.main import main as daemon_main

            return daemon_main(rest)

    # Back-compat: fall through to the daemon. Covers
    # ``python -m os_updater`` (systemd) and any leading-flag form.
    from os_updater.main import main as daemon_main

    return daemon_main(list(argv))


def _print_top_level_help() -> None:
    print(
        "usage: python -m os_updater [-h] [--version] <subcommand> [args...]\n"
        "\n"
        "agora-os-updater — A/B atomic OS update orchestrator.\n"
        "\n"
        "subcommands:\n"
        "  daemon              run the WPS-driven update service "
        "(default if no subcommand)\n"
        "  stage <bundle>      manually install a local bundle into "
        "the inactive slot\n"
        "\n"
        "Pass ``-h`` after a subcommand for its specific options, e.g.:\n"
        "  python -m os_updater stage --help\n"
        "  python -m os_updater daemon --help\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
