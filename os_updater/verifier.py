"""``SignatureVerifier`` — the default :class:`Verifier` implementation.

Adapts :func:`os_updater.bundle.verify_signature` to the ``Verifier``
protocol from :mod:`os_updater.service`. Sits in the FSM between
``DOWNLOADING`` and ``STAGED``: the downloader writes
``bundle.tar.zst`` + ``bundle.tar.zst.minisig`` into the staging dir,
the verifier checks the signature against the rootfs-baked pubkeys
(:data:`bundle.DEFAULT_PRIMARY_PUBKEY` + :data:`bundle.DEFAULT_RECOVERY_PUBKEY`,
with the :data:`bundle.DEFAULT_PUBKEY_SEARCH_DIR` rotation slot tried
first), and on failure raises :class:`bundle.BundleSignatureError`,
which the service maps to ``failed:signature_invalid``.

Scope of this PR (p2-signature-verify): signature only. The
sha256-manifest half lives behind :func:`bundle.verify_bundle_manifest`
and stays a stub until the ``p2-stage-and-tryboot`` PR adds tarball
extraction. That sibling PR will likely call the manifest helper from
inside the stager, since the stager owns the extracted tree anyway.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from os_updater.bundle import (
    DEFAULT_MINISIGN_TIMEOUT_S,
    DEFAULT_PRIMARY_PUBKEY,
    DEFAULT_PUBKEY_SEARCH_DIR,
    DEFAULT_RECOVERY_PUBKEY,
    BundleSignatureError,
    Runner,
    _default_runner,
    discover_pubkeys,
    verify_signature,
)
from os_updater.dispatch import DispatchPayload


logger = logging.getLogger(__name__)


#: Default filename of the signed artifact inside the staging dir. The
#: downloader writes it here; the stager (next sibling PR) extracts
#: from it.
DEFAULT_BUNDLE_FILENAME = "bundle.tar.zst"

#: Default filename of the detached minisign signature inside the
#: staging dir.
DEFAULT_SIGNATURE_FILENAME = "bundle.tar.zst.minisig"


@dataclass
class SignatureVerifier:
    """Verifier that delegates to :func:`bundle.verify_signature`.

    Construction parameters are all overridable for tests:

    * ``pubkey_search_dir`` / ``primary_pubkey`` / ``recovery_pubkey``
      are passed through to :func:`bundle.discover_pubkeys`.
    * ``bundle_filename`` / ``signature_filename`` determine where in
      ``staging_dir`` to look for the artifacts produced by the
      downloader.
    * ``runner`` is the subprocess seam; defaults to a real
      ``subprocess.run`` invocation via :func:`bundle._default_runner`.
    * ``minisign_timeout_s`` caps each individual ``minisign -V`` call.

    The ``run`` coroutine wraps the synchronous :func:`verify_signature`
    in :func:`asyncio.to_thread` so the daemon's event loop stays
    responsive while minisign is busy. Verification itself is fast
    (~10ms for a 1 GB bundle on a Pi 5) but the threading boundary is
    cheap insurance and matches the codebase's standard practice for
    blocking syscalls inside async handlers.
    """

    pubkey_search_dir: Path = field(default_factory=lambda: DEFAULT_PUBKEY_SEARCH_DIR)
    primary_pubkey: Path = field(default_factory=lambda: DEFAULT_PRIMARY_PUBKEY)
    recovery_pubkey: Path = field(default_factory=lambda: DEFAULT_RECOVERY_PUBKEY)
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME
    signature_filename: str = DEFAULT_SIGNATURE_FILENAME
    runner: Runner = field(default=_default_runner)
    minisign_timeout_s: float = DEFAULT_MINISIGN_TIMEOUT_S

    async def run(
        self, payload: DispatchPayload, staging_dir: Path
    ) -> None:
        """Verify the downloaded bundle in ``staging_dir``.

        Raises :class:`BundleSignatureError` on any failure. The
        service catches this, transitions the FSM to ``FAILED``, and
        emits ``failed:signature_invalid``.
        """
        bundle_path = Path(staging_dir) / self.bundle_filename
        signature_path = Path(staging_dir) / self.signature_filename

        pubkeys = discover_pubkeys(
            search_dir=self.pubkey_search_dir,
            primary=self.primary_pubkey,
            recovery=self.recovery_pubkey,
        )
        logger.info(
            "verifying bundle signature: release=%s bundle=%s sig=%s "
            "candidate_pubkeys=%d",
            payload.release_id,
            bundle_path,
            signature_path,
            len(pubkeys),
        )

        winning_pubkey = await asyncio.to_thread(
            verify_signature,
            bundle_path=bundle_path,
            signature_path=signature_path,
            pubkeys=pubkeys,
            runner=self.runner,
            timeout_s=self.minisign_timeout_s,
        )
        logger.info(
            "bundle signature verified: release=%s pubkey=%s",
            payload.release_id,
            winning_pubkey,
        )
