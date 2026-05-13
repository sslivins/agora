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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

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

#: Initial / max reconnect backoff in seconds for the WPS connection.
#: Mirrors the existing cms_client transport behavior.
_RECONNECT_INITIAL_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0

#: Same regex as dispatch._VERSION_RE — kept local to avoid importing a
#: private symbol. Used by the version-floor check.
_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([A-Za-z0-9.]+))?$")


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
    """

    async def run(
        self, payload: DispatchPayload, staging_dir: Path
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
        self, payload: DispatchPayload, staging_dir: Path
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
    async def run(self, payload: DispatchPayload, staging_dir: Path) -> None:
        raise NotImplementedError("downloader not wired; see p2-bundle-format")


@dataclass
class _DefaultVerifier:
    async def run(self, payload: DispatchPayload, staging_dir: Path) -> None:
        raise NotImplementedError("verifier not wired; see p2-signature-verify")


@dataclass
class _DefaultStager:
    async def stage(self, payload: DispatchPayload, staging_dir: Path) -> None:
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
        self.state: UpdaterState = load_state(self.state_path)

    # -- public API used by tests and the run loop --------------------------

    def recover_on_start(self) -> None:
        """Reset the FSM on daemon startup if we crashed mid-dispatch.

        Three cases:

        * ``idle`` / ``failed`` — nothing to do.
        * ``tryboot_running`` — this is the normal post-reboot resumption
          point.  We DO NOT reset here; slot-confirm will drive the next
          transition.  (Tracked by plan #23's state-machine notes.)
        * Anything else — we crashed in the middle of a download / stage
          / migrate. Transition to ``FAILED`` with a synthetic reason so
          the next dispatch arriving moves us back to ``DOWNLOADING``,
          per :data:`LEGAL_TRANSITIONS`. Emit a ``failed`` lifecycle
          event so the CMS sees what happened.
        """

        s = self.state
        if s.fsm in (UpdaterFSMState.IDLE, UpdaterFSMState.FAILED):
            return
        if s.fsm is UpdaterFSMState.TRYBOOT_RUNNING:
            # Normal post-tryboot wake — leave alone for slot-confirm.
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
            await self.downloader.run(payload, staging_dir)
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
            await self.stager.stage(payload, staging_dir)

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

        transition(self.state, UpdaterFSMState.IDLE)
        save_state(self.state, path=self.state_path)
        emit_event(
            self.state,
            LifecycleEventType.MIGRATION_COMPLETE,
            self.event_sink,
            state_path=self.state_path,
        )

    async def run(self) -> None:
        """Main async run loop.

        Connects to WPS with exponential backoff (2s -> 60s), receives
        messages, dispatches them. Returns only on cancellation.
        """

        self.recover_on_start()
        delay = _RECONNECT_INITIAL_DELAY
        while True:
            transport = self.transport_factory()
            try:
                await transport.connect()
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
                await transport.close()
                raise
            except Exception:
                log.exception("WPS loop crashed; reconnecting in %.1fs", delay)
                try:
                    await transport.close()
                except Exception:  # pragma: no cover - defensive
                    pass
                await asyncio.sleep(delay)
                delay = min(delay * 2, _RECONNECT_MAX_DELAY)

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
        from os_updater.apply import RsyncError, StagingError, TrybootError
        from os_updater.bundle import BundleIntegrityError, BundleSignatureError
        from os_updater.migrate import (
            MigrationDiscoveryError,
            MigrationError,
            MigrationFenceDenied,
            MigrationScriptError,
            SchemaVersionError,
        )

        if isinstance(exc, BundleSignatureError):
            return "signature_invalid"
        if isinstance(exc, BundleIntegrityError):
            return "bundle_invalid"
        # Order matters: RsyncError and TrybootError subclass
        # StagingError, so the subclass arms must come first.
        if isinstance(exc, RsyncError):
            return "stage_rsync_failed"
        if isinstance(exc, TrybootError):
            return "tryboot_failed"
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
