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
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from os_updater.bundle import BundleError
from os_updater.dispatch import DispatchPayload
from os_updater.verifier import (
    DEFAULT_BUNDLE_FILENAME,
    DEFAULT_SIGNATURE_FILENAME,
)


logger = logging.getLogger(__name__)


#: Default chunk size for the streaming download. 64 KiB balances syscall
#: overhead against memory pressure on the Pi 5 (1 GiB bundle / 64 KiB =
#: ~16k iterations -- nothing).
DEFAULT_CHUNK_SIZE = 65536


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
    """

    chunk_size: int = DEFAULT_CHUNK_SIZE
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME
    signature_filename: str = DEFAULT_SIGNATURE_FILENAME

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
        await self._fetch(payload.bundle_url, bundle_target)

        logger.info(
            "downloading signature: release_id=%s url=%s -> %s",
            payload.release_id,
            payload.signature_url,
            sig_target,
        )
        await self._fetch(payload.signature_url, sig_target)

        logger.info(
            "bundle download complete: release_id=%s target_version=%s",
            payload.release_id,
            payload.target_version,
        )

    async def _fetch(self, url: str, target: Path) -> None:
        """Stream ``url`` into ``target`` via a ``.tmp`` sibling + rename.

        ``aiohttp`` is imported lazily so test runs that patch
        ``sys.modules["aiohttp"]`` see the mock when the production code
        first reaches for it. Same trick the cms_client downloader uses.
        """

        import aiohttp

        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise BundleDownloadError(
                            f"HTTP {resp.status} fetching {url}"
                        )
                    with open(tmp, "wb") as f:
                        async for chunk in resp.content.iter_chunked(
                            self.chunk_size
                        ):
                            f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
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
