"""Production entry point for ``python -m os_updater``.

The Phase 2 PR (``p2-os-updater-service``) ships this entry point so the
daemon can be wired into systemd and exercised on a real device. The bulk
of the daemon's behavior lives in :class:`os_updater.service.OSUpdaterService`
— this module is the thin glue that:

1. Parses CLI args (``--state-path``, ``--staging-root``, ``--log-level``).
2. Builds a :class:`os_updater.service.WPSTransport` adapter on top of the
   real :mod:`cms_client.transport` (open_wps).  The adapter exposes the
   minimal ``connect``/``recv``/``close`` shape the service expects, even
   though :mod:`cms_client.transport` itself uses an ``async for`` iterator
   internally.
3. Reads the device's current OS version from ``/etc/agora/version`` (or a
   ``--current-version-file`` override) for the ``min_from_version`` floor
   check (plan #21).
4. Installs a SIGTERM/SIGINT handler that cancels the run loop cleanly so
   systemd can restart us without a forced kill.

The actual download / verify / stage / migrate hooks are injected by
sibling Phase 2 todos via the protocols on :class:`OSUpdaterService`.
Until those land, ``main()`` wires in the default ``NotImplementedError``
stubs — the daemon will still start, accept a WPS connection, and reject
any dispatch with ``failed:error_NotImplementedError`` so the CMS sees
the daemon is alive but the rest of Phase 2 isn't done.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from os_updater import __version__
from os_updater.service import (
    DEFAULT_STAGING_ROOT,
    OSUpdaterService,
    WPSTransport,
)
from os_updater.state import DEFAULT_STATE_PATH


log = logging.getLogger("agora.os_updater")


#: Path to the baked-in current-version file. Phase 0 writes the OS image
#: version into ``/etc/agora/version`` (plan §"Phase 0 — Deliverables"
#: re: ``/etc/agora/version``). One-line plain text, semver-shaped.
DEFAULT_CURRENT_VERSION_FILE = Path("/etc/agora/version")


def _read_current_version(path: Path) -> str:
    """Read ``/etc/agora/version`` and strip whitespace.

    Raises if the file is missing or empty — better to fail loud at daemon
    startup than to ship an empty string into the floor check and either
    accept every dispatch (string comparison) or reject every dispatch
    (semver parse).
    """

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError(f"{path} is empty; cannot determine current version")
    return raw


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="os_updater",
        description=(
            "agora-os-updater — on-device daemon that orchestrates A/B "
            "atomic OS updates. Subscribes to the CMS over WPS for "
            "os_update_dispatch messages, downloads + verifies the bundle, "
            "stages it to the inactive slot, triggers tryboot, and emits "
            "lifecycle events."
        ),
    )
    p.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help=f"updater-state.json location (default: {DEFAULT_STATE_PATH})",
    )
    p.add_argument(
        "--staging-root",
        type=Path,
        default=DEFAULT_STAGING_ROOT,
        help=(
            "per-dispatch staging directory root "
            f"(default: {DEFAULT_STAGING_ROOT})"
        ),
    )
    p.add_argument(
        "--current-version-file",
        type=Path,
        default=DEFAULT_CURRENT_VERSION_FILE,
        help=(
            "path to the baked-in OS version file "
            f"(default: {DEFAULT_CURRENT_VERSION_FILE})"
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="logging level (default: INFO)",
    )
    p.add_argument(
        "--version",
        action="version",
        version=f"os_updater {__version__}",
    )
    return p


def _configure_logging(level: str) -> None:
    """Plain stdout logger; systemd captures it into journald."""

    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stdout,
    )


class _CMSWPSTransportAdapter:
    """Adapt :mod:`cms_client.transport`'s async-iterator WPS transport
    to the :class:`WPSTransport` protocol that :class:`OSUpdaterService`
    expects.

    The cms_client transport is async-iterable — ``async for raw in t``
    yields JSON strings.  The service wants ``await t.recv()`` returning
    a dict.  We hold the iterator, await one item per recv, and parse
    JSON before returning.

    A fresh instance is produced per :class:`OSUpdaterService` reconnect
    attempt (via ``transport_factory``), so we don't have to worry about
    re-using an exhausted iterator.
    """

    def __init__(
        self,
        opener,  # callable() -> awaitable returning a cms_client transport
    ) -> None:
        self._opener = opener
        self._t = None
        self._iter = None

    async def connect(self) -> None:
        self._t = await self._opener()
        self._iter = self._t.__aiter__()

    async def recv(self) -> dict[str, Any]:
        if self._iter is None:
            raise RuntimeError("transport.recv() called before connect()")
        raw = await self._iter.__anext__()
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"non-JSON WPS frame: {exc}") from exc
        if not isinstance(obj, dict):
            raise RuntimeError(
                f"WPS frame decoded to {type(obj).__name__}, expected object"
            )
        return obj

    async def close(self) -> None:
        if self._t is not None:
            try:
                await self._t.close()
            except Exception:
                log.debug("error closing WPS transport", exc_info=True)
            finally:
                self._t = None
                self._iter = None


def _build_transport_factory(settings):
    """Build a factory that the service can call to get a fresh transport
    on each reconnect.

    Imports :mod:`api.config` and :mod:`cms_client.transport` lazily so
    unit tests can exercise this module without those deps installed.
    Phase 3 will likely refactor this to share the cms_client daemon's
    existing WPS connection (plan §"Phase 2 — Deliverables", line about
    "the existing WPS connection") — for now, agora-os-updater opens its
    own connection with its own connect-token round-trip.
    """

    from cms_client.transport import open_wps  # local import per docstring

    cms_url = settings.cms_url
    device_id = settings.device_name
    api_key = settings.device_api_key
    api_base = settings.cms_api_url or None

    if not cms_url:
        raise RuntimeError("AGORA_CMS_URL is unset; cannot open WPS")
    if not device_id:
        raise RuntimeError("AGORA_DEVICE_NAME is unset; cannot open WPS")
    if not api_key:
        raise RuntimeError("AGORA_DEVICE_API_KEY is unset; cannot open WPS")

    async def _open():
        return await open_wps(
            cms_url=cms_url,
            device_id=device_id,
            api_key=api_key,
            api_base=api_base,
        )

    def factory() -> WPSTransport:
        return _CMSWPSTransportAdapter(_open)

    return factory


async def _run(args: argparse.Namespace) -> int:
    """Build the service and drive its run loop until cancelled."""

    # Lazy import so ``--help`` doesn't pull in pydantic-settings.
    from api.config import load_settings

    settings = load_settings()
    current_version = _read_current_version(args.current_version_file)
    log.info("agora-os-updater starting; current_version=%s", current_version)

    service = OSUpdaterService(
        transport_factory=_build_transport_factory(settings),
        current_version_provider=lambda: current_version,
        state_path=args.state_path,
        staging_root=args.staging_root,
    )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _handle_signal(signum: int, _frame: Any = None) -> None:
        log.info("signal %d received; shutting down", signum)
        loop.call_soon_threadsafe(stop.set)

    # SIGTERM is what systemd sends on stop/restart.
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            # Windows test runs hit this — fall back to signal.signal.
            signal.signal(sig, _handle_signal)

    runner = asyncio.create_task(service.run(), name="os-updater-run")
    stopper = asyncio.create_task(stop.wait(), name="os-updater-stop")
    done, pending = await asyncio.wait(
        {runner, stopper},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if stopper in done and not runner.done():
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("error during shutdown")
    for p in pending:
        p.cancel()
    log.info("agora-os-updater stopped")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
