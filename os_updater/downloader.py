"""``BundleDownloader`` -- the default :class:`Downloader` implementation.

Adapts ``aiohttp`` streaming downloads to the ``Downloader`` protocol from
:mod:`os_updater.service`. Sits at the head of the FSM (``IDLE`` ->
``DOWNLOADING``): given a :class:`DispatchPayload`, fetches the
``bundle.tar.zst`` + ``bundle.tar.zst.minisig`` artifacts from the URLs the
CMS dispatched and writes them into the staging directory using the
canonical filenames the :class:`SignatureVerifier` (and downstream
:class:`SlotStager`) expect.

Implementation notes (Phase 2):

* Uses ``aiohttp`` (already a runtime dependency via ``cms_client``) for
  consistency with the rest of the on-device HTTP path. The pattern mirrors
  ``cms_client.service._download_one_asset``: stream-to-disk via
  ``resp.content.iter_chunked`` into a ``.tmp`` sibling, ``fsync``, then
  atomic rename onto the final path.
* No HTTP Range / resume in v1. A partial download from a previous crash
  is reaped by the service's 24h on-boot staging sweeper (see
  :mod:`os_updater.service`). The Range-resume contract in the
  ``Downloader`` protocol docstring is aspirational; revisit if real
  bandwidth-constrained installs start failing repeatedly.
* HTTP errors (non-200), aiohttp ``ClientError``, and local ``OSError``
  are all wrapped in :class:`BundleDownloadError` (a :class:`BundleError`
  subclass) so the service's :meth:`OSUpdaterService._classify_failure`
  reports a stable ``failed:download_failed`` wire code rather than the
  generic ``error_<TypeName>`` bucket.
* The bundle URLs are plain ``str`` (no api-key header). The CMS hands out
  the GitHub-release ``browser_download_url`` directly, and GitHub's
  release-asset endpoint is unauthenticated for public repos. If the
  release ever moves to a private bucket, this is the seam to extend.
* Bytes-progress is reported via an optional ``progress_callback``
  signature ``(bytes_done: int, bytes_total: int) -> None``.  The
  downloader calls back at most every ``PROGRESS_MIN_INTERVAL_S``
  seconds during the streaming GET, plus a final forced call at
  completion so a stuck callback never lands at 99% in the UI.  The
  service wraps this into a ``download_progress`` lifecycle event so
  the CMS progress badge can animate live (agora#215).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from os_updater.bundle import BundleError
from os_updater.dispatch import DispatchPayload
from os_updater.events import PROGRESS_MIN_INTERVAL_S, RateLimitedProgress
from os_updater.verifier import (
    DEFAULT_BUNDLE_FILENAME,
    DEFAULT_SIGNATURE_FILENAME,
)


logger = logging.getLogger(__name__)


#: Default chunk size for the streaming download. 64 KiB balances syscall
#: overhead against memory pressure on the Pi 5 (1 GiB bundle / 64 KiB =
#: ~16k iterations -- nothing).
DEFAULT_CHUNK_SIZE = 65536


#: Type alias for the bytes-progress callback shape.
ProgressCallback = Callable[[int, int], None]


class BundleDownloadError(BundleError):
    """Raised on HTTP / network / write failures during bundle download.

    Service maps this to ``failed:download_failed`` (see
    :meth:`os_updater.service.OSUpdaterService._classify_failure`).
    """


@dataclass
class BundleDownloader:
    """Streams the bundle + signature artifacts into ``staging_dir``.

    Construction parameters are all overridable for tests:

    * ``chunk_size`` -- bytes per ``iter_chunked`` step; tests may shrink
      it to exercise multi-chunk paths.
    * ``bundle_filename`` / ``signature_filename`` -- on-disk filenames
      that the verifier + stager expect.  Default to the canonical values
      from :mod:`os_updater.verifier`.
    * ``progress_callback`` -- if set, invoked with ``(bytes_done,
      bytes_total)`` during the bundle download (NOT the much-smaller
      signature download).  Rate-limited internally via
      :class:`RateLimitedProgress`; final call is always forced.
    """

    chunk_size: int = DEFAULT_CHUNK_SIZE
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME
    signature_filename: str = DEFAULT_SIGNATURE_FILENAME
    progress_callback: Optional[ProgressCallback] = None

    async def run(
        self, payload: DispatchPayload, staging_dir: Path
    ) -> None:
        """Download the bundle + signature into ``staging_dir``.

        Idempotent on a fresh staging dir: caller (the service) is
        responsible for cleaning up partial state from a prior aborted
        attempt before invoking us. We just create the dir if missing and
        write the two artifacts.
        """

        staging_dir.mkdir(parents=True, exist_ok=True)

        bundle_target = staging_dir / self.bundle_filename
        sig_target = staging_dir / self.signature_filename

        logger.info(
            "downloading bundle: release_id=%s target_version=%s url=%s -> %s",
            payload.release_id,
            payload.target_version,
            payload.bundle_url,
            bundle_target,
        )
        await self._fetch(payload.bundle_url, bundle_target, with_progress=True)

        logger.info(
            "downloading signature: release_id=%s url=%s -> %s",
            payload.release_id,
            payload.signature_url,
            sig_target,
        )
        await self._fetch(payload.signature_url, sig_target, with_progress=False)

        logger.info(
            "bundle download complete: release_id=%s target_version=%s",
            payload.release_id,
            payload.target_version,
        )

    async def _fetch(
        self, url: str, target: Path, *, with_progress: bool = False,
    ) -> None:
        """Stream ``url`` into ``target`` via a ``.tmp`` sibling + rename.

        ``aiohttp`` is imported lazily so test runs that patch
        ``sys.modules["aiohttp"]`` see the mock when the production code
        first reaches for it. Same trick the cms_client downloader uses.

        When ``with_progress`` is true and a ``progress_callback`` is
        configured, fires ``(bytes_done, bytes_total)`` callbacks
        throughout the streaming GET.
        """

        import aiohttp

        rate_limited: Optional[RateLimitedProgress] = None
        if with_progress and self.progress_callback is not None:
            rate_limited = RateLimitedProgress(self.progress_callback)

        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise BundleDownloadError(
                            f"HTTP {resp.status} fetching {url}"
                        )
                    # ``Content-Length`` is the only available total here
                    # because the bundle is served as a single GitHub
                    # release asset (no chunked-transfer-encoding).
                    # Missing/garbled header -> emit progress with
                    # total=0 so the UI falls back to the "Downloading
                    # bundle" label-only badge.
                    try:
                        total = int(resp.headers.get("Content-Length") or 0)
                    except (TypeError, ValueError):
                        total = 0
                    bytes_done = 0
                    with open(tmp, "wb") as f:
                        async for chunk in resp.content.iter_chunked(
                            self.chunk_size
                        ):
                            f.write(chunk)
                            bytes_done += len(chunk)
                            if rate_limited is not None:
                                rate_limited(bytes_done, total)
                        f.flush()
                        os.fsync(f.fileno())
                    # Final forced emit so the badge always reaches
                    # 100% before the next FSM event clears it.
                    if rate_limited is not None:
                        rate_limited(bytes_done, total or bytes_done, force=True)
            os.replace(tmp, target)
        except BundleDownloadError:
            # Already a typed failure -- still want to drop the tmp so a
            # subsequent retry on the same staging dir starts clean.
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            raise
        except aiohttp.ClientError as exc:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            raise BundleDownloadError(
                f"network error fetching {url}: {exc}"
            ) from exc
        except OSError as exc:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            raise BundleDownloadError(
                f"local write error for {target}: {exc}"
            ) from exc
