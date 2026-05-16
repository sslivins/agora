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

``main()`` injects the production collaborators for all three of the
service's hot-path protocols:

* :class:`os_updater.downloader.BundleDownloader` (Downloader)
* :class:`os_updater.verifier.SignatureVerifier` (Verifier)
* :class:`os_updater.apply.SlotStager` (Stager)

The Migrator hook is still the service default (no-op fence + script
runner is exercised by :class:`OSUpdaterService` itself); future PRs may
inject an override if the migration story grows configurable bits.
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
from os_updater.apply import SlotStager
from os_updater.downloader import BundleDownloader
from os_updater.events import WpsEventSink
from os_updater.service import (
    DEFAULT_STAGING_ROOT,
    OSUpdaterService,
    WPSTransport,
)
from os_updater.state import DEFAULT_STATE_PATH
from os_updater.verifier import SignatureVerifier


log = logging.getLogger("agora.os_updater")


#: Path to the baked-in current-version file. agora-os writes this at
#: image-build time (assemble.sh) and at OTA-bundle-build time
#: (build-bundle.sh). It's a multi-line key=value file:
#:
#:     # comment
#:     agora_os_version=0.0.4-test
#:     agora_app_floor=1.11.0
#:
#: We consume only ``agora_os_version`` here (per Decision #2 the
#: ``agora_app_floor`` is the agora-app channel's contract, not the OS
#: updater's). The shared :mod:`shared.version_file` parser handles the
#: actual line-by-line parsing — both this daemon and ``cms_client``
#: (which reads the file to populate ``os_version`` in its register
#: message, per the M4 phase of the CMS-migration plan) go through that
#: single parser so any future regex change stays in lockstep.
DEFAULT_CURRENT_VERSION_FILE = Path("/etc/agora/version")


def _read_current_version(path: Path) -> str:
    """Thin back-compat wrapper around the shared parser.

    The actual parser lives in :func:`shared.version_file.read_os_version_strict`
    so ``cms_client`` can use the same logic for its register-message
    construction. This wrapper exists so the daemon's call sites and
    its long-standing test suite (which imports this symbol directly)
    don't have to know about the shared module.
    """

    from shared.version_file import read_os_version_strict

    return read_os_version_strict(path)


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

    async def send(self, data) -> None:
        """Forward outbound payloads to the underlying cms_client transport.

        Used by :class:`os_updater.events.WpsEventSink` to emit lifecycle
        events back to the CMS over the same websocket the service uses
        to receive dispatch messages.  Delegates to ``self._t.send`` which
        is the live :class:`cms_client.transport._Transport` (Direct or
        WPS-enveloped). agora#216.
        """
        if self._t is None:
            raise RuntimeError("transport.send() called before connect()")
        await self._t.send(data)

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

    Two paths, selected at runtime:

    * **Bootstrap-v2** (``settings.bootstrap_v2`` is truthy and
      ``settings.bootstrap_state_path`` exists with usable
      ``wps_url`` + ``wps_jwt``): each ``_open()`` re-reads the
      state file and uses the pre-minted WPS URL + JWT directly,
      bypassing the legacy api_key connect-token round-trip. The
      cms_client daemon running alongside os_updater proactively
      refreshes the JWT and rewrites this file, so re-reading on
      every reconnect picks up new tokens without os_updater
      having to do its own refresh dance.
    * **Legacy api_key**: original behavior. Requires
      ``AGORA_CMS_URL``, ``AGORA_DEVICE_NAME``, and
      ``AGORA_DEVICE_API_KEY`` env vars. Will be deleted once the
      bootstrap-v2 migration is complete (see plan M8).

    Imports :mod:`cms_client.transport` lazily so unit tests can
    exercise this module without that dep installed.
    """

    from cms_client.transport import open_wps  # local import per docstring

    if _bootstrap_v2_available(settings):
        return _build_bootstrap_v2_factory(settings, open_wps)
    return _build_legacy_api_key_factory(settings, open_wps)


def _bootstrap_v2_available(settings) -> bool:
    """Return True iff bootstrap-v2 is enabled AND its state file is usable.

    A truthy ``settings.bootstrap_v2`` alone isn't enough: on a freshly
    flashed device that hasn't yet completed bootstrap-v2 enrollment the
    state file won't exist, and we'd rather fail open to the legacy path
    than crash on first boot.
    """
    if not getattr(settings, "bootstrap_v2", False):
        return False
    path = getattr(settings, "bootstrap_state_path", None)
    if not path:
        return False
    try:
        import json as _json
        from pathlib import Path as _Path

        state = _json.loads(_Path(path).read_text())
    except (OSError, ValueError):
        return False
    return bool(state.get("wps_url")) and bool(state.get("wps_jwt"))


def _build_bootstrap_v2_factory(settings, open_wps):
    """Factory for the bootstrap-v2 (pre-minted JWT) path.

    cms_url and device_id are sourced from ``bootstrap_state.json``
    too — the legacy ``settings.cms_url`` / ``settings.device_name``
    fields are *not* required on bootstrap-v2 devices, since the
    pre-minted URL contains everything ``open_wps`` actually needs
    to dial. The two are passed defensively for forward-compat with
    older ``open_wps`` signatures.
    """
    import json as _json
    from pathlib import Path as _Path

    bootstrap_path = _Path(settings.bootstrap_state_path)

    async def _open():
        try:
            state = _json.loads(bootstrap_path.read_text())
        except FileNotFoundError as e:
            raise RuntimeError(
                f"bootstrap_state.json missing at {bootstrap_path}; "
                "device not paired via bootstrap-v2"
            ) from e
        wps_url = state.get("wps_url") or ""
        wps_jwt = state.get("wps_jwt") or ""
        if not wps_url or not wps_jwt:
            raise RuntimeError(
                f"bootstrap_state.json at {bootstrap_path} missing wps_url/wps_jwt"
            )
        cms_url = state.get("cms_api_base") or getattr(settings, "cms_url", "") or ""
        device_id = state.get("device_id") or getattr(settings, "device_name", "") or ""
        return await open_wps(
            cms_url=cms_url,
            device_id=device_id,
            pre_minted_url=wps_url,
            pre_minted_token=wps_jwt,
        )

    def factory() -> WPSTransport:
        return _CMSWPSTransportAdapter(_open)

    return factory


def _build_legacy_api_key_factory(settings, open_wps):
    """Factory for the legacy api_key WPS connect path.

    Slated for deletion in plan M8 once every fleet device is on
    bootstrap-v2.
    """
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

    # Forward-reference dance: the WpsEventSink needs a callable that
    # returns the *currently-live* transport, but the transport itself
    # only exists per-reconnect inside the service's run loop. The
    # service exposes ``_active_transport`` (Optional[WPSTransport])
    # which is set on connect and cleared on disconnect; the
    # transport_provider lambda below closes over the freshly-built
    # service instance and reads the attribute lazily on each event
    # send. agora#215.
    service: Optional[OSUpdaterService] = None

    def _transport_provider() -> Optional[WPSTransport]:
        if service is None:  # pragma: no cover — defensive, only window is <100us
            return None
        return service._active_transport

    event_sink = WpsEventSink(transport_provider=_transport_provider)

    service = OSUpdaterService(
        transport_factory=_build_transport_factory(settings),
        event_sink=event_sink,
        current_version_provider=lambda: current_version,
        downloader=BundleDownloader(),
        verifier=SignatureVerifier(),
        stager=SlotStager(),
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
