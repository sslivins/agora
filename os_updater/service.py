"""``OSUpdaterService`` — the orchestrator.

Owns the agora-os-updater FSM. Listens on the WPS connection for
``os_update_dispatch`` messages, validates them against the concurrency
interlock (plan #23) and the ``min_from_version`` floor (plan #21 / #24),
and then drives the in-flight dispatch through the four collaborator
hooks:

* ``downloader`` — downloads the bundle to ``staging_dir`` (p2-bundle-format)
* ``verifier`` — verifies minisign + sha256 manifest (p2-signature-verify)
* ``stager`` — rsync staging content to the inactive slot + trigger tryboot
  (p2-stage-and-tryboot)
* ``migrator`` — runs forward migrations post-promote (p2-forward-migration)

Each hook is a callable injected at construction; defaults raise
``NotImplementedError`` so an unmaintained Phase 2 deployment fails loud
rather than silently no-op'ing. The sibling todos fill these in with real
implementations and unit tests.

Sequence diagram (happy path):

    [WPS msg arrives]
    parse_dispatch_payload
    check is_busy(state)
    check version floor (skipped iff force_downgrade)
    state: idle/failed -> downloading
    emit DOWNLOAD_STARTED
    downloader.run(payload, staging_dir)
    state: downloading -> staged
    emit SIGNATURE_VERIFIED + STAGED
    state: staged -> tryboot_pending
    stager.stage(staging_dir, payload)
    state: tryboot_pending -> tryboot_running
    emit TRYBOOT_INITIATED
    [reboot happens]
    [next boot: state.fsm == tryboot_running. slot-confirm runs.]
    [if confirmed:] state: tryboot_running -> promoted_pending_migration
    emit SLOT_CONFIRMED + PROMOTED
    state: promoted_pending_migration -> migrating
    migrator.run()
    state: migrating -> idle
    emit MIGRATION_COMPLETE

Any failure -> state: <current> -> FAILED with reason; emit
FAILED:<reason>. The dispatch then sits in FAILED until the next dispatch
arrives, which moves it back to DOWNLOADING per LEGAL_TRANSITIONS.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

from migration_fence import (
    DEFAULT_SENTINEL_PATH,
    FenceStatus,
    check_migration_fence,
)
from os_updater.dispatch import (
    DispatchPayload,
    DispatchPayloadError,
    parse_dispatch_payload,
)
from os_updater.events import (
    EventSink,
    LifecycleEventType,
    OutboxEventSink,
    emit_event,
)
from os_updater.migrate import ForwardMigrator
from os_updater.state import (
    DEFAULT_STATE_PATH,
    UpdaterFSMState,
    UpdaterState,
    is_busy,
    load_state,
    save_state,
    transition,
)

#: Root of per-dispatch staging directories. Each dispatch gets a
#: subdirectory named after ``release_id``. Lives on ``/data`` so it
#: survives slot switches.
DEFAULT_STAGING_ROOT = Path("/data/.update/staging")

#: TTL for stale per-dispatch staging dirs reaped on daemon start.
#: The happy-path cleanup in :meth:`OSUpdaterService.continue_after_promote`
#: removes the staging dir as soon as forward migrations succeed; this
#: sweep is the safety net that mops up everything else (failed
#: updates, crashes mid-download, abandoned tryboot dirs) so ``/data``
#: doesn't accumulate orphaned bundles forever.
STAGING_SWEEP_TTL_SECONDS: float = 24 * 60 * 60

#: Allowed prefix for the resolved staging root. The sweep refuses to
#: operate if :attr:`staging_root` resolves outside this prefix, which
#: blocks a symlink-based attack that would otherwise let a botched
#: migration redirect the root-owned sweeper at the wrong tree. Tests
#: override via the ``staging_sweep_allowed_prefix`` kwarg.
DEFAULT_STAGING_SWEEP_ALLOWED_PREFIX = "/data/"

#: Initial / max reconnect backoff in seconds for the WPS connection.
#: Mirrors the existing cms_client transport behavior.
_RECONNECT_INITIAL_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0

#: Cadence (seconds) for the promote-handshake watcher tick. The watcher
#: polls ``migration_fence`` while the FSM is in ``tryboot_running`` and
#: drives the SLOT_CONFIRMED -> PROMOTED -> MIGRATION_COMPLETE chain once
#: slot-mgr writes the migration-allowed sentinel. See issue
#: ``sslivins/agora#209`` Bug B and ``files/v0026-ota-postmortem.md``.
_PROMOTE_HANDSHAKE_TICK_SEC = 30.0

#: Same regex as dispatch._VERSION_RE — kept local to avoid importing a
#: private symbol. Used by the version-floor check.
_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([A-Za-z0-9.-]+))?$")


log = logging.getLogger(__name__)


class UpdaterError(Exception):
    """Base class for the daemon's typed errors."""


class UpdaterBusyError(UpdaterError):
    """Raised by :meth:`OSUpdaterService.handle_dispatch` when busy.

    Mapped to a ``declined:busy`` lifecycle event by the caller. The
    typed exception (vs a return-code) lets tests assert on the rejection
    path without inspecting events.
    """


class VersionFloorError(UpdaterError):
    """Raised when current version < ``min_from_version`` and no override."""


def _parse_version(value: str) -> tuple[int, int, int, str]:
    """Return a sortable tuple ``(major, minor, patch, prerelease)``.

    A version without a prerelease suffix sorts above one with (semver
    rule). Encoding: empty string for "no prerelease" comes after any
    non-empty prerelease string when we invert it — implemented below as
    ``"~"`` (high-ASCII sentinel) so plain ``X.Y.Z`` > ``X.Y.Z-rc1``.
    """

    m = _VERSION_RE.match(value)
    if not m:
        raise ValueError(f"unparseable version: {value!r}")
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    pre = m.group(4) or "~"  # ~ is high-ASCII, sorts after any prerelease char
    return major, minor, patch, pre


def version_at_least(current: str, floor: str) -> bool:
    """Return True iff ``current >= floor`` under the semver-lite order."""

    return _parse_version(current) >= _parse_version(floor)


# ---------------------------------------------------------------------------
# Collaborator protocols (filled in by sibling todos in Phase 2)
# ---------------------------------------------------------------------------


class Downloader(Protocol):
    """Owns the download into ``staging_dir`` (p2-bundle-format).

    Implementation must support HTTP Range resume for partial-download
    cleanup (plan §"Phase 2 — Deliverables"). Phase 2 default raises
    ``NotImplementedError`` so we fail loud if the sibling todo hasn't
    landed yet.

    ``progress_callback`` (added in agora#219) is the per-dispatch
    bytes-progress hook the service wires so each chunk fired by the
    downloader becomes a ``download_progress`` lifecycle event.  Kwarg-
    only and optional so non-progress-aware implementations (the test
    stubs / Phase 2 ``_DefaultDownloader``) can ignore it.
    """

    async def run(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:  # pragma: no cover - protocol
        ...


class Verifier(Protocol):
    """Owns minisign signature + sha256 manifest verification (p2-signature-verify)."""

    async def run(
        self, payload: DispatchPayload, staging_dir: Path
    ) -> None:  # pragma: no cover - protocol
        ...


class Stager(Protocol):
    """Owns rsync-to-inactive-slot + agora-slot-mgr trigger-tryboot (p2-stage-and-tryboot)."""

    async def stage(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[str], None]] = None,
        extract_progress_callback: Optional[
            Callable[[str, int, int], None]
        ] = None,
    ) -> None:  # pragma: no cover - protocol
        ...


class Migrator(Protocol):
    """Owns post-promote ``/etc/agora/migrations/`` runner (p2-forward-migration)."""

    async def run(self) -> None:  # pragma: no cover - protocol
        ...


class CurrentVersionProvider(Protocol):
    """Returns the current device version for the floor check."""

    def __call__(self) -> str:  # pragma: no cover - protocol
        ...


class WPSTransport(Protocol):
    """Minimal contract for the WPS connection the service consumes.

    The full transport lives in ``cms_client.transport``. The service
    only needs ``connect``, ``recv``, and ``close`` — the message-sender
    half is hidden behind :class:`EventSink`.
    """

    async def connect(self) -> None:  # pragma: no cover
        ...

    async def recv(self) -> dict[str, Any]:  # pragma: no cover
        ...

    async def close(self) -> None:  # pragma: no cover
        ...


def _default_unimplemented(name: str) -> Callable[..., Awaitable[None]]:
    """Build an async hook that fails with a pointer to its sibling todo."""

    async def _stub(*args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            f"{name!r} collaborator not wired; see sibling todo for Phase 2"
        )

    return _stub


@dataclass
class _DefaultDownloader:
    async def run(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        raise NotImplementedError("downloader not wired; see p2-bundle-format")


@dataclass
class _DefaultVerifier:
    async def run(self, payload: DispatchPayload, staging_dir: Path) -> None:
        raise NotImplementedError("verifier not wired; see p2-signature-verify")


@dataclass
class _DefaultStager:
    async def stage(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        raise NotImplementedError("stager not wired; see p2-stage-and-tryboot")


def _stub_current_version() -> str:
    raise NotImplementedError(
        "current_version_provider not wired; service.py requires injection"
    )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class OSUpdaterService:
    """The orchestrator.

    All collaborators are injected. Defaults raise
    :class:`NotImplementedError`, which is intentional — the Phase 2 PR
    ships the FSM + dispatch + event scaffolding; sibling PRs fill in
    the actual download / verify / stage / migrate steps.

    Three useful entry points for tests:

    * :meth:`handle_dispatch` — synchronous-ish helper that runs the
      dispatch-validation steps and (when accepted) drives the
      collaborator chain through the success path. Returns ``None``;
      failures raise :class:`UpdaterError` subclasses.
    * :meth:`recover_on_start` — runs once at daemon startup.  If the
      persisted FSM state isn't ``idle``/``failed``, reset to ``failed``
      with ``resumed_from_<state>`` so we don't accept new dispatches
      while in an indeterminate state.
    * :meth:`run` — the async run loop. Connects to WPS, recv-dispatches
      forever with 2s..60s backoff. The blocking loop tests don't need.
    """

    def __init__(
        self,
        *,
        transport_factory: Callable[[], WPSTransport],
        event_sink: Optional[EventSink] = None,
        current_version_provider: CurrentVersionProvider = _stub_current_version,
        downloader: Optional[Downloader] = None,
        verifier: Optional[Verifier] = None,
        stager: Optional[Stager] = None,
        migrator: Optional[Migrator] = None,
        state_path: Path = DEFAULT_STATE_PATH,
        staging_root: Path = DEFAULT_STAGING_ROOT,
        migration_sentinel_path: Optional[Path] = None,
        promote_handshake_tick_sec: float = _PROMOTE_HANDSHAKE_TICK_SEC,
        staging_sweep_ttl_sec: float = STAGING_SWEEP_TTL_SECONDS,
        staging_sweep_allowed_prefix: str = DEFAULT_STAGING_SWEEP_ALLOWED_PREFIX,
    ) -> None:
        self.transport_factory = transport_factory
        self.event_sink: EventSink = event_sink or OutboxEventSink()
        self.current_version_provider = current_version_provider
        self.downloader: Downloader = downloader or _DefaultDownloader()
        self.verifier: Verifier = verifier or _DefaultVerifier()
        self.stager: Stager = stager or _DefaultStager()
        self.migrator: Migrator = migrator or ForwardMigrator()
        self.state_path = state_path
        self.staging_root = staging_root
        self.migration_sentinel_path = migration_sentinel_path
        self._promote_handshake_tick_sec = promote_handshake_tick_sec
        self.staging_sweep_ttl_sec = staging_sweep_ttl_sec
        self.staging_sweep_allowed_prefix = staging_sweep_allowed_prefix
        self.state: UpdaterState = load_state(self.state_path)
        # Phase 2 / agora#215: the per-reconnect WPS transport, set by
        # :meth:`run` after every successful ``transport_factory()`` +
        # ``connect()``, cleared on any disconnect/exception in the loop.
        # ``WpsEventSink`` reads this lazily via the
        # ``transport_provider`` it was constructed with so that
        # lifecycle events (including the high-rate
        # ``download_progress`` / ``extract_progress`` events) can be
        # fire-and-forgot over the live transport without the sink
        # needing to know about reconnect bookkeeping.
        self._active_transport: Optional[WPSTransport] = None

    # -- public API used by tests and the run loop --------------------------

    def recover_on_start(self) -> None:
        """Reset the FSM on daemon startup AND sweep stale staging dirs.

        FSM reset (three cases):

        * ``idle`` / ``failed`` — nothing to do.
        * ``tryboot_running`` — this is the normal post-reboot resumption
          point.  We DO NOT reset here; slot-confirm will drive the next
          transition.  (Tracked by plan #23's state-machine notes.)
        * Anything else — we crashed in the middle of a download / stage
          / migrate. Transition to ``FAILED`` with a synthetic reason so
          the next dispatch arriving moves us back to ``DOWNLOADING``,
          per :data:`LEGAL_TRANSITIONS`. Emit a ``failed`` lifecycle
          event so the CMS sees what happened.

        Then runs :meth:`sweep_stale_staging` so the 24h-TTL filesystem
        reaper executes once per daemon start — failures intentionally
        leave the staging dir on disk for forensics, and this sweep is
        the safety net that keeps ``/data`` from accumulating orphaned
        bundles forever. Sweep errors are logged and swallowed; the FSM
        reset is the primary responsibility of this method.
        """

        s = self.state
        if s.fsm in (UpdaterFSMState.IDLE, UpdaterFSMState.FAILED):
            self._safe_sweep_stale_staging()
            return
        if s.fsm is UpdaterFSMState.TRYBOOT_RUNNING:
            # Normal post-tryboot wake — leave alone for slot-confirm.
            self._safe_sweep_stale_staging()
            return

        prior = s.fsm.value
        reason = f"resumed_from_{prior}"
        log.warning("recovering from interrupted state %s -> failed", prior)
        transition(s, UpdaterFSMState.FAILED, reason=reason)
        emit_event(
            s,
            LifecycleEventType.FAILED,
            self.event_sink,
            reason=reason,
            state_path=self.state_path,
        )
        self._safe_sweep_stale_staging()

    def _safe_sweep_stale_staging(self) -> None:
        """Run :meth:`sweep_stale_staging` with errors logged-and-swallowed.

        The FSM-reset half of :meth:`recover_on_start` is the contract
        the rest of the daemon depends on; a sweep failure must not
        prevent daemon startup.
        """

        try:
            self.sweep_stale_staging()
        except Exception:
            log.exception("staging sweep failed; continuing daemon startup")

    def sweep_stale_staging(self) -> None:
        """Reap stale per-dispatch staging dirs left behind by failures.

        Iterates :attr:`staging_root` and removes any entry whose mtime
        is older than :attr:`staging_sweep_ttl_sec` (24h default). The
        active dispatch's staging dir (``self.state.staging_dir``) is
        skipped defensively. This is load-bearing for tryboot wakes
        (FSM == ``TRYBOOT_RUNNING``) and tolerates one extra dispatch
        cycle of accumulation for stale-after-crash cases (where the
        FSM is ``FAILED`` but ``staging_dir`` still references a dir
        older than the TTL).

        Defends against a symlink-based attack: if :attr:`staging_root`
        is itself a symlink, or resolves outside the configured allowed
        prefix (default ``/data/``), the sweep refuses to operate. The
        daemon runs as root, so the cost of one extra check is trivial
        compared to the blast radius of recursively rmtree'ing the
        wrong tree.

        Designed to be called once at daemon start (from
        :meth:`recover_on_start`). Idempotent and safe to call again.
        """

        if self.staging_root.is_symlink():
            log.error(
                "staging_root %s is a symlink; refusing to sweep",
                self.staging_root,
            )
            return

        try:
            resolved = self.staging_root.resolve()
        except Exception:
            log.exception(
                "could not resolve staging root %s", self.staging_root
            )
            return

        if not str(resolved).startswith(self.staging_sweep_allowed_prefix):
            log.error(
                "staging_root %s resolves to %s outside allowed prefix %r; "
                "refusing to sweep",
                self.staging_root,
                resolved,
                self.staging_sweep_allowed_prefix,
            )
            return

        try:
            entries = list(self.staging_root.iterdir())
        except FileNotFoundError:
            return  # Staging root is created lazily on first dispatch.
        except Exception:
            log.exception(
                "could not list staging root %s", self.staging_root
            )
            return

        now = time.time()
        cutoff = now - self.staging_sweep_ttl_sec
        active = self.state.staging_dir  # str or None
        try:
            active_resolved = Path(active).resolve() if active else None
        except Exception:
            active_resolved = None

        for entry in entries:
            try:
                entry_resolved = entry.resolve()
            except Exception:
                entry_resolved = None

            if active_resolved is not None and entry_resolved == active_resolved:
                continue  # Don't reap the in-flight dispatch.

            try:
                mtime = entry.stat().st_mtime
            except FileNotFoundError:
                continue  # Raced with another sweeper / OS — fine.
            except Exception:
                log.exception("could not stat staging entry %s", entry)
                continue

            if mtime >= cutoff:
                continue  # Fresh enough; keep for forensics.

            age = now - mtime
            try:
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    entry.unlink(missing_ok=True)
                log.info(
                    "swept stale staging entry %s (age=%.0fs)", entry, age
                )
            except Exception:
                log.exception(
                    "failed to remove stale staging entry %s", entry
                )

    async def handle_dispatch(self, raw_msg: Any) -> None:
        """Drive a single dispatch end-to-end on the happy path.

        Validates the payload, the busy interlock, and the version floor.
        On any failure, transitions the FSM to ``FAILED`` (or stays in
        the current state for ``declined:busy``) and emits the
        corresponding lifecycle event.

        Tests can call this directly without standing up a transport.
        """

        try:
            payload = parse_dispatch_payload(raw_msg)
        except DispatchPayloadError as exc:
            self._emit_failed("invalid_payload", reason_detail=str(exc))
            return

        # Concurrency interlock (#23).
        if is_busy(self.state):
            log.info(
                "rejecting dispatch %s while in state %s",
                payload.release_id,
                self.state.fsm.value,
            )
            self._emit_declined(
                "busy",
                payload=payload,
                detail=f"in state {self.state.fsm.value}",
            )
            raise UpdaterBusyError(
                f"updater busy in state {self.state.fsm.value}"
            )

        # Version floor (#21 / #24).
        try:
            current_version = self.current_version_provider()
        except NotImplementedError:
            raise
        if not payload.force_downgrade and not version_at_least(
            current_version, payload.min_from_version
        ):
            log.warning(
                "refusing dispatch %s: current=%s < min_from_version=%s",
                payload.release_id,
                current_version,
                payload.min_from_version,
            )
            # Pre-admission rejection — the dispatch never entered the
            # pipeline so we don't touch the FSM. Stamp metadata on the
            # state transiently so the lifecycle event references the
            # right release_id, then restore. Plan acceptance: "verify the
            # device emits failed:version_floor and the inactive slot is
            # untouched" — leaving the FSM in IDLE makes that trivially
            # true.
            self._emit_pre_admission_failed(
                payload,
                reason="version_floor",
                detail={
                    "current_version": current_version,
                    "min_from_version": payload.min_from_version,
                },
            )
            raise VersionFloorError(
                f"current={current_version} < min_from_version={payload.min_from_version}"
            )

        # Accepted — drive the success chain.
        self._record_dispatch_metadata(payload)
        transition(self.state, UpdaterFSMState.DOWNLOADING)
        save_state(self.state, path=self.state_path)
        emit_event(
            self.state,
            LifecycleEventType.DOWNLOAD_STARTED,
            self.event_sink,
            state_path=self.state_path,
        )

        staging_dir = self.staging_root / payload.release_id
        try:
            def _on_download_progress(bytes_done: int, bytes_total: int) -> None:
                """Forward each rate-limited chunk callback from the
                downloader into a DOWNLOAD_PROGRESS lifecycle event so
                the CMS badge can animate as the bundle streams down
                (agora#219).

                Closure: captures ``self.state`` / ``self.event_sink`` /
                ``self.state_path`` at the moment this dispatch is in
                flight, not at downloader construction time.  Bound here
                rather than passed into ``BundleDownloader(...)`` in
                ``main.py`` because the downloader instance is reused
                across dispatches but ``self.state`` is dispatch-scoped.
                ``emit_event`` swallows sink failures and the downloader
                wraps this callback in its own safe-invoke, so any
                failure here is logged-and-ignored — DOWNLOAD_PROGRESS
                is advisory.
                """
                emit_event(
                    self.state,
                    LifecycleEventType.DOWNLOAD_PROGRESS,
                    self.event_sink,
                    payload={
                        "bytes_done": bytes_done,
                        "bytes_total": bytes_total,
                    },
                    state_path=self.state_path,
                )

            await self.downloader.run(
                payload,
                staging_dir,
                progress_callback=_on_download_progress,
            )
            await self.verifier.run(payload, staging_dir)
            transition(self.state, UpdaterFSMState.STAGED)
            save_state(self.state, path=self.state_path)
            emit_event(
                self.state,
                LifecycleEventType.SIGNATURE_VERIFIED,
                self.event_sink,
                state_path=self.state_path,
            )
            emit_event(
                self.state,
                LifecycleEventType.STAGED,
                self.event_sink,
                state_path=self.state_path,
            )

            transition(self.state, UpdaterFSMState.TRYBOOT_PENDING)
            save_state(self.state, path=self.state_path)

            def _on_stage_progress(phase: str) -> None:
                """Wired into :meth:`Stager.stage` so each phase boundary
                emits a STAGE_PROGRESS lifecycle event (agora#202).

                Bound here so the closure captures ``self.state`` /
                ``self.event_sink`` / ``self.state_path`` at the moment
                this dispatch is in flight, not at stager-construction
                time. ``emit_event`` already swallows sink failures, and
                the stager wraps this callback in its own safe-invoke,
                so any failure here is logged-and-ignored — STAGE_PROGRESS
                is advisory.
                """
                emit_event(
                    self.state,
                    LifecycleEventType.STAGE_PROGRESS,
                    self.event_sink,
                    payload={"phase": phase},
                    state_path=self.state_path,
                )

            def _on_extract_progress(
                phase: str, bytes_done: int, bytes_total: int
            ) -> None:
                """Wired into :meth:`Stager.stage` so each fdinfo poll
                of the boot/rootfs extract emits an EXTRACT_PROGRESS
                lifecycle event with compressed-bytes-consumed
                (agora#215).

                ``phase`` is one of ``"extracting_boot"`` or
                ``"extracting_rootfs"`` so the CMS progress bar can
                weight the two passes correctly. The CMS-side parser
                clamps the percentage at 100% even when zstd's pos:
                lands slightly past the file size (zstd reads in
                window-sized chunks).
                """
                emit_event(
                    self.state,
                    LifecycleEventType.EXTRACT_PROGRESS,
                    self.event_sink,
                    payload={
                        "phase": phase,
                        "bytes_done": bytes_done,
                        "bytes_total": bytes_total,
                    },
                    state_path=self.state_path,
                )

            await self.stager.stage(
                payload,
                staging_dir,
                progress_callback=_on_stage_progress,
                extract_progress_callback=_on_extract_progress,
            )

            transition(self.state, UpdaterFSMState.TRYBOOT_RUNNING)
            save_state(self.state, path=self.state_path)
            emit_event(
                self.state,
                LifecycleEventType.TRYBOOT_INITIATED,
                self.event_sink,
                state_path=self.state_path,
            )
        except Exception as exc:
            log.exception(
                "dispatch %s failed in state %s",
                payload.release_id,
                self.state.fsm.value,
            )
            reason = self._classify_failure(exc)
            transition(self.state, UpdaterFSMState.FAILED, reason=reason)
            emit_event(
                self.state,
                LifecycleEventType.FAILED,
                self.event_sink,
                reason=reason,
                payload={"detail": str(exc)},
                state_path=self.state_path,
            )
            raise UpdaterError(reason) from exc

    async def continue_after_promote(self) -> None:
        """Run forward migrations after a successful tryboot promote.

        Invoked by ``agora-slot-mgr`` (or its agent equivalent) after the
        FSM has been advanced to ``PROMOTED_PENDING_MIGRATION``. Drives
        ``MIGRATING -> IDLE`` on success, ``MIGRATING -> FAILED`` on any
        migrator exception.  The fence check inside the migrator is what
        gates "promote actually completed"; on fence-denied we land in
        ``FAILED`` with reason ``migration_fence_denied`` and the caller
        is expected to retry on the next dispatch (per plan #22 / Phase 1
        forward-migration fence).

        Raises :class:`UpdaterError` if called outside
        ``PROMOTED_PENDING_MIGRATION``; the FSM is left untouched in
        that case so the caller can decide how to recover.
        """

        if self.state.fsm is not UpdaterFSMState.PROMOTED_PENDING_MIGRATION:
            raise UpdaterError(
                f"continue_after_promote called in state {self.state.fsm.value!r}; "
                "expected promoted_pending_migration"
            )

        transition(self.state, UpdaterFSMState.MIGRATING)
        save_state(self.state, path=self.state_path)

        try:
            await self.migrator.run()
        except Exception as exc:
            log.exception(
                "forward migration failed in state %s", self.state.fsm.value
            )
            reason = self._classify_failure(exc)
            transition(self.state, UpdaterFSMState.FAILED, reason=reason)
            emit_event(
                self.state,
                LifecycleEventType.FAILED,
                self.event_sink,
                reason=reason,
                payload={"detail": str(exc)},
                state_path=self.state_path,
            )
            raise UpdaterError(reason) from exc

        staging_dir = self.state.staging_dir
        if staging_dir:
            try:
                shutil.rmtree(staging_dir, ignore_errors=True)
            except Exception:  # pragma: no cover - defensive
                log.exception("failed to clean staging dir %s", staging_dir)

        sentinel_path = Path(self.migration_sentinel_path or DEFAULT_SENTINEL_PATH)
        try:
            sentinel_path.unlink(missing_ok=True)
        except Exception:  # pragma: no cover - defensive
            log.exception("failed to remove migration sentinel %s", sentinel_path)

        transition(self.state, UpdaterFSMState.IDLE)
        save_state(self.state, path=self.state_path)
        emit_event(
            self.state,
            LifecycleEventType.MIGRATION_COMPLETE,
            self.event_sink,
            state_path=self.state_path,
        )

    async def _tick_promote_handshake(self) -> None:
        """Check the migration fence and advance the FSM if slot-mgr has promoted.

        Idempotent: a no-op unless ``state.fsm`` is ``TRYBOOT_RUNNING``,
        ``state.target_version`` is set, the fence reports ``allowed``,
        and the currently-running version matches the dispatch target.

        Closes the gap between ``slot_mgr.promote_slot()`` (which writes
        the migration-allowed sentinel) and ``continue_after_promote()``
        (which drives MIGRATING -> IDLE). See issue ``sslivins/agora#209``
        Bug B and ``files/v0026-ota-postmortem.md`` option (a).
        """

        state = self.state
        if state.fsm is not UpdaterFSMState.TRYBOOT_RUNNING:
            return
        if state.target_version is None:
            log.debug("promote-handshake tick skipped: no target_version on state")
            return

        try:
            fence: FenceStatus = check_migration_fence(
                sentinel_path=self.migration_sentinel_path
            )
        except Exception:
            log.exception("promote-handshake tick: check_migration_fence raised")
            return

        if not fence.allowed:
            log.debug(
                "promote-handshake tick: fence not allowed (%s)", fence.reason
            )
            return

        try:
            current = self.current_version_provider()
        except Exception:
            log.exception("promote-handshake tick: current_version_provider raised")
            return

        if current != state.target_version:
            log.info(
                "promote-handshake tick: version mismatch current=%r target=%r; "
                "treating as stale sentinel and waiting",
                current,
                state.target_version,
            )
            return

        log.info(
            "promote-handshake tick: fence allowed and version matches; "
            "advancing FSM (slot=%s target=%s)",
            fence.allowed_slot,
            state.target_version,
        )

        emit_event(
            state,
            LifecycleEventType.SLOT_CONFIRMED,
            self.event_sink,
            payload={"slot": fence.allowed_slot},
            state_path=self.state_path,
        )

        transition(state, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)
        save_state(state, path=self.state_path)

        emit_event(
            state,
            LifecycleEventType.PROMOTED,
            self.event_sink,
            payload={"slot": fence.allowed_slot},
            state_path=self.state_path,
        )

        try:
            await self.continue_after_promote()
        except UpdaterError:
            # continue_after_promote already emitted FAILED + logged; the
            # FSM is now in FAILED so subsequent ticks will no-op.
            pass

    async def _promote_handshake_loop(self) -> None:
        """Periodically drive ``_tick_promote_handshake`` until cancelled."""

        while True:
            try:
                await self._tick_promote_handshake()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("promote-handshake tick failed; will retry")
            await asyncio.sleep(self._promote_handshake_tick_sec)

    async def run(self) -> None:
        """Main async run loop.

        Connects to WPS with exponential backoff (2s -> 60s), receives
        messages, dispatches them. Also spawns a background watcher task
        that periodically polls the migration fence to advance the FSM
        out of ``tryboot_running`` once slot-mgr has promoted the slot.
        Returns only on cancellation.
        """

        self.recover_on_start()

        # Bind the main loop on the sink so worker-thread emit sites
        # (stage_progress, extract_progress -- both fire from
        # ``asyncio.to_thread`` workers and the fdinfo poll thread)
        # can schedule sends via ``run_coroutine_threadsafe``.  Pre-
        # agora#219, the sink called ``asyncio.get_running_loop`` in
        # its own thread which raised in worker threads, silently
        # dropping every progress event.  Duck-typed because
        # OutboxEventSink (Phase 2 disk-only sink) doesn't need it.
        bind_loop = getattr(self.event_sink, "bind_loop", None)
        if callable(bind_loop):
            bind_loop(asyncio.get_running_loop())

        try:
            await self._tick_promote_handshake()
        except Exception:
            log.exception("startup promote-handshake tick failed; loop will retry")

        handshake_task = asyncio.create_task(
            self._promote_handshake_loop(),
            name="os-updater-promote-handshake",
        )

        try:
            delay = _RECONNECT_INITIAL_DELAY
            while True:
                transport = self.transport_factory()
                try:
                    await transport.connect()
                    self._active_transport = transport
                    log.info("WPS connected; awaiting dispatch messages")
                    delay = _RECONNECT_INITIAL_DELAY
                    while True:
                        msg = await transport.recv()
                        msg_type = msg.get("type") if isinstance(msg, dict) else None
                        if msg_type != "os_update_dispatch":
                            log.debug("ignoring non-dispatch message type=%r", msg_type)
                            continue
                        try:
                            await self.handle_dispatch(msg)
                        except UpdaterError:
                            # Already emitted; loop continues so we can accept
                            # the next dispatch once we're back to FAILED.
                            pass
                except asyncio.CancelledError:
                    self._active_transport = None
                    await transport.close()
                    raise
                except Exception:
                    self._active_transport = None
                    log.exception("WPS loop crashed; reconnecting in %.1fs", delay)
                    try:
                        await transport.close()
                    except Exception:  # pragma: no cover - defensive
                        pass
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_MAX_DELAY)
        finally:
            self._active_transport = None
            handshake_task.cancel()
            try:
                await handshake_task
            except (asyncio.CancelledError, Exception):
                pass

    # -- internals ----------------------------------------------------------

    def _record_dispatch_metadata(self, payload: DispatchPayload) -> None:
        """Stamp ``state`` with the in-flight dispatch identifiers.

        Done before transitioning out of ``IDLE`` so that lifecycle events
        emitted on the happy AND failure paths both reference the correct
        ``release_id`` / ``target_version``.
        """

        self.state.release_id = payload.release_id
        self.state.target_version = payload.target_version
        self.state.staging_dir = str(self.staging_root / payload.release_id)

    def _emit_failed(
        self, reason: str, *, reason_detail: Optional[str] = None
    ) -> None:
        """Record a synthetic failure that didn't come from a real transition.

        Used for malformed-payload rejections — we don't have a payload to
        stamp metadata from, so we just emit a ``failed:<reason>`` event
        with the detail in the payload. State FSM is unaffected.
        """

        emit_event(
            self.state,
            LifecycleEventType.FAILED,
            self.event_sink,
            reason=reason,
            payload={"detail": reason_detail} if reason_detail else None,
            state_path=self.state_path,
        )

    def _emit_pre_admission_failed(
        self,
        payload: DispatchPayload,
        *,
        reason: str,
        detail: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Emit ``failed:<reason>`` for a dispatch that never entered the pipeline.

        Stamps the lifecycle event with the rejected dispatch's
        ``release_id`` / ``target_version`` so the CMS can correlate, but
        leaves the persistent state's metadata fields untouched (the
        dispatch was rejected, not started). The FSM stays in its current
        state — typically IDLE.
        """

        saved = (
            self.state.release_id,
            self.state.target_version,
            self.state.staging_dir,
        )
        try:
            self.state.release_id = payload.release_id
            self.state.target_version = payload.target_version
            self.state.staging_dir = None
            emit_event(
                self.state,
                LifecycleEventType.FAILED,
                self.event_sink,
                reason=reason,
                payload=dict(detail or {}),
                state_path=self.state_path,
            )
        finally:
            (
                self.state.release_id,
                self.state.target_version,
                self.state.staging_dir,
            ) = saved
            save_state(self.state, path=self.state_path)

    def _emit_declined(
        self,
        reason: str,
        *,
        payload: DispatchPayload,
        detail: Optional[str] = None,
    ) -> None:
        """Emit ``declined:<reason>`` for a rejected dispatch.

        ``declined`` events MUST carry the rejected dispatch's
        ``release_id`` so the CMS can mark the right
        ``scheduled_dispatches`` row as ``declined_busy`` per plan #23.
        We stamp the metadata transiently on a local copy of the state
        so the persisted state on disk is unchanged.
        """

        saved = (
            self.state.release_id,
            self.state.target_version,
            self.state.staging_dir,
        )
        try:
            self.state.release_id = payload.release_id
            self.state.target_version = payload.target_version
            self.state.staging_dir = None
            emit_event(
                self.state,
                LifecycleEventType.DECLINED,
                self.event_sink,
                reason=reason,
                payload={"detail": detail} if detail else None,
                state_path=self.state_path,
            )
        finally:
            (
                self.state.release_id,
                self.state.target_version,
                self.state.staging_dir,
            ) = saved
            save_state(self.state, path=self.state_path)

    @staticmethod
    def _classify_failure(exc: BaseException) -> str:
        """Map an exception type to a ``failed:<reason>`` short code.

        Typed bundle exceptions get pinned short codes so the CMS sees
        the stable wire strings documented in ``docs/bundle-format.md``
        and plan.md (``signature_invalid``, ``bundle_invalid``).
        Unknown exception types still get a generic ``error_<TypeName>``
        bucket so nothing slips through silently.
        """

        # Local import avoids a top-of-file cycle with os_updater.bundle
        # (which imports nothing from this module today, but keep the
        # boundary one-way to stay tolerant of future drift).
        from os_updater.apply import (
            FleetStateMissingError,
            FleetStateWriteError,
            RsyncError,
            StagingError,
            TrybootError,
        )
        from os_updater.bundle import BundleIntegrityError, BundleSignatureError
        from os_updater.downloader import BundleDownloadError
        from os_updater.migrate import (
            MigrationDiscoveryError,
            MigrationError,
            MigrationFenceDenied,
            MigrationScriptError,
            SchemaVersionError,
        )

        # Order: BundleDownloadError, BundleSignatureError, BundleIntegrityError
        # all subclass BundleError but each has a distinct wire code. They
        # don't share a hierarchy beyond BundleError, so order within this
        # block is documentation-only.
        if isinstance(exc, BundleDownloadError):
            return "download_failed"
        if isinstance(exc, BundleSignatureError):
            return "signature_invalid"
        if isinstance(exc, BundleIntegrityError):
            return "bundle_invalid"
        # Order matters: RsyncError, TrybootError, FleetStateMissingError,
        # and FleetStateWriteError all subclass StagingError, so the
        # subclass arms must come first.
        if isinstance(exc, RsyncError):
            return "stage_rsync_failed"
        if isinstance(exc, TrybootError):
            return "tryboot_failed"
        if isinstance(exc, FleetStateMissingError):
            return "fleet_state_missing"
        if isinstance(exc, FleetStateWriteError):
            return "slot_b_write_failed"
        if isinstance(exc, StagingError):
            return "stage_failed"
        # Order matters: MigrationFenceDenied / MigrationScriptError /
        # SchemaVersionError / MigrationDiscoveryError all subclass
        # MigrationError, so the specific arms must come first. The
        # generic MigrationError arm catches anything new without
        # falling through to the unknown-error bucket.
        if isinstance(exc, MigrationFenceDenied):
            return "migration_fence_denied"
        if isinstance(exc, MigrationScriptError):
            return "migration_script_failed"
        if isinstance(exc, SchemaVersionError):
            return "migration_schema_version_invalid"
        if isinstance(exc, MigrationDiscoveryError):
            return "migration_discovery_failed"
        if isinstance(exc, MigrationError):
            return "migration_failed"

        name = type(exc).__name__
        return f"error_{name}"
