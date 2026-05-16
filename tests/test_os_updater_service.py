"""Tests for :mod:`os_updater.service` — the orchestrator.

Acceptance hooks (plan §"Phase 2 — Acceptance"):

* Busy interlock → ``declined:busy`` (plan #23).
* Version floor → ``failed:version_floor``, FSM stays in ``IDLE``
  (plan #21 / #24, "the inactive slot is untouched").
* ``force_downgrade=true`` skips the floor check.
* Invalid payload → ``failed:invalid_payload``.
* ``recover_on_start`` resets stuck mid-pipeline states to ``FAILED``;
  leaves ``IDLE`` / ``FAILED`` / ``TRYBOOT_RUNNING`` alone.
* Happy path drives the FSM ``IDLE → DOWNLOADING → STAGED →
  TRYBOOT_PENDING → TRYBOOT_RUNNING`` and emits the lifecycle events in
  the right order.

These tests inject the collaborator stubs (Downloader, Verifier, Stager,
Migrator) so we exercise the service in isolation. The real
implementations land in sibling Phase 2 PRs.
"""

from __future__ import annotations

import asyncio

import pytest

from os_updater.events import (
    LifecycleEvent,
    LifecycleEventType,
)
from os_updater.service import (
    OSUpdaterService,
    UpdaterBusyError,
    UpdaterError,
    VersionFloorError,
    version_at_least,
)
from os_updater.state import (
    UpdaterFSMState,
    UpdaterState,
    load_state,
    save_state,
)


# ── Helpers ────────────────────────────────────────────────────────────────


class _ListSink:
    """In-memory EventSink that captures every emitted event."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def put(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _OkStub:
    """Async no-op for Downloader / Verifier / Stager / Migrator."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def run(self, *args, **kwargs) -> None:
        self.calls.append(("run", args, kwargs))

    async def stage(self, *args, **kwargs) -> None:
        self.calls.append(("stage", args, kwargs))


class _BoomDownloader:
    async def run(self, payload, staging_dir, *, progress_callback=None):
        raise RuntimeError("network is on fire")


class _BoomSignatureVerifier:
    """Raises :class:`BundleSignatureError` like a real verifier on a bad sig.

    Exercises the typed-exception → ``signature_invalid`` short-code
    mapping in ``OSUpdaterService._classify_failure``. Mirrors the
    pattern used by :class:`_BoomDownloader` for unrelated failures.
    """

    async def run(self, payload, staging_dir):
        # Local import keeps the test file's top-of-module imports
        # focused on the service surface.
        from os_updater.bundle import BundleSignatureError

        raise BundleSignatureError("primary and recovery both rejected the sig")


class _BoomIntegrityVerifier:
    """Raises :class:`BundleIntegrityError` to exercise ``bundle_invalid``.

    The integrity check actually runs from the stager (it needs an
    extracted tree, which the verifier doesn't have), but the
    classifier mapping lives on the service. Injecting this stub at
    the verifier seam is the cheapest way to verify the mapping
    without standing up the whole stager pipeline.
    """

    async def run(self, payload, staging_dir):
        from os_updater.bundle import BundleIntegrityError

        raise BundleIntegrityError("sha256 manifest mismatch")


def _ok_dispatch(**overrides):
    base = {
        "type": "os_update_dispatch",
        "release_id": "rel_test_1",
        "target_version": "1.1.0",
        "min_from_version": "1.0.0",
        "bundle_url": "https://x/y/bundle.zst",
        "signature_url": "https://x/y/bundle.zst.minisig",
    }
    base.update(overrides)
    return base


def _build_service(
    tmp_path,
    *,
    current_version: str = "1.0.0",
    downloader=None,
    verifier=None,
    stager=None,
    migrator=None,
    sink=None,
    migration_sentinel_path=None,
    promote_handshake_tick_sec: float = 30.0,
):
    """Construct an :class:`OSUpdaterService` for tests with safe defaults."""

    state_path = tmp_path / "state.json"
    staging_root = tmp_path / "staging"
    return OSUpdaterService(
        transport_factory=lambda: object(),  # never called by these tests
        event_sink=sink or _ListSink(),
        current_version_provider=lambda: current_version,
        downloader=downloader or _OkStub(),
        verifier=verifier or _OkStub(),
        stager=stager or _OkStub(),
        migrator=migrator or _OkStub(),
        state_path=state_path,
        staging_root=staging_root,
        migration_sentinel_path=migration_sentinel_path,
        promote_handshake_tick_sec=promote_handshake_tick_sec,
    )


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


# ── version_at_least ───────────────────────────────────────────────────────


class TestVersionAtLeast:
    @pytest.mark.parametrize(
        "current,floor,expected",
        [
            ("1.0.0", "1.0.0", True),
            ("1.0.1", "1.0.0", True),
            ("1.1.0", "1.0.99", True),
            ("2.0.0", "1.999.999", True),
            ("1.0.0", "1.0.1", False),
            ("0.9.9", "1.0.0", False),
            # Prerelease ordering: 1.0.0 (release) > 1.0.0-rc1 (prerelease).
            ("1.0.0", "1.0.0-rc1", True),
            ("1.0.0-rc1", "1.0.0", False),
            ("1.0.0-rc2", "1.0.0-rc1", True),
        ],
    )
    def test_compare(self, current, floor, expected):
        assert version_at_least(current, floor) is expected


# ── recover_on_start ───────────────────────────────────────────────────────


class TestRecoverOnStart:
    def test_idle_unchanged(self, tmp_path):
        s = _build_service(tmp_path)
        s.state = UpdaterState(fsm=UpdaterFSMState.IDLE)
        save_state(s.state, s.state_path)
        s.recover_on_start()
        assert s.state.fsm is UpdaterFSMState.IDLE

    def test_failed_unchanged(self, tmp_path):
        s = _build_service(tmp_path)
        s.state = UpdaterState(
            fsm=UpdaterFSMState.FAILED, last_failure_reason="prior"
        )
        save_state(s.state, s.state_path)
        s.recover_on_start()
        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "prior"

    def test_tryboot_running_unchanged(self, tmp_path):
        """Normal post-reboot resumption — slot-confirm drives the next move."""
        s = _build_service(tmp_path)
        s.state = UpdaterState(
            fsm=UpdaterFSMState.TRYBOOT_RUNNING,
            release_id="rel-1",
            target_version="1.1.0",
        )
        save_state(s.state, s.state_path)
        s.recover_on_start()
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING

    @pytest.mark.parametrize(
        "stuck",
        [
            UpdaterFSMState.DOWNLOADING,
            UpdaterFSMState.STAGED,
            UpdaterFSMState.TRYBOOT_PENDING,
            UpdaterFSMState.PROMOTED_PENDING_MIGRATION,
            UpdaterFSMState.MIGRATING,
        ],
    )
    def test_stuck_state_resets_to_failed(self, tmp_path, stuck):
        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        s.state = UpdaterState(
            fsm=stuck,
            release_id="rel-stuck",
            target_version="1.2.0",
        )
        save_state(s.state, s.state_path)
        s.recover_on_start()
        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == f"resumed_from_{stuck.value}"
        # Emitted a lifecycle event so CMS sees the recovery.
        assert len(sink.events) == 1
        ev = sink.events[0]
        assert ev.event_type is LifecycleEventType.FAILED
        assert ev.reason == f"resumed_from_{stuck.value}"


# ── handle_dispatch: rejection paths ──────────────────────────────────────


class TestHandleDispatchRejection:
    def test_invalid_payload_emits_failed_invalid_payload(self, tmp_path):
        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        asyncio.run(s.handle_dispatch({"type": "os_update_dispatch"}))  # missing fields
        assert s.state.fsm is UpdaterFSMState.IDLE
        assert any(
            e.event_type is LifecycleEventType.FAILED and e.reason == "invalid_payload"
            for e in sink.events
        )

    def test_busy_state_emits_declined_busy_and_raises(self, tmp_path):
        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        # Force a busy state.
        s.state = UpdaterState(
            fsm=UpdaterFSMState.DOWNLOADING,
            release_id="rel-prior",
            target_version="1.0.5",
        )
        save_state(s.state, s.state_path)

        with pytest.raises(UpdaterBusyError):
            asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-new")))

        # State unchanged on rejection.
        assert s.state.fsm is UpdaterFSMState.DOWNLOADING
        assert s.state.release_id == "rel-prior"
        # Declined event carries the NEW dispatch's release_id (so CMS can
        # mark the right scheduled_dispatches row), not the in-flight one.
        declined = [
            e
            for e in sink.events
            if e.event_type is LifecycleEventType.DECLINED and e.reason == "busy"
        ]
        assert len(declined) == 1
        assert declined[0].release_id == "rel-new"

    def test_version_floor_emits_failed_and_fsm_stays_idle(self, tmp_path):
        """The plan acceptance bar: ``the inactive slot is untouched``. We
        enforce that by leaving the FSM in IDLE so the daemon never starts
        the staging chain. release_id on persistent state must also be
        restored to its pre-dispatch value (None on a fresh daemon)."""

        sink = _ListSink()
        s = _build_service(tmp_path, current_version="1.0.0", sink=sink)
        msg = _ok_dispatch(min_from_version="2.0.0", release_id="rel-floor")

        with pytest.raises(VersionFloorError):
            asyncio.run(s.handle_dispatch(msg))

        # FSM untouched — pre-admission rejection (the test the plan calls out).
        assert s.state.fsm is UpdaterFSMState.IDLE
        # Persistent state's release_id is NOT mutated for a rejected
        # dispatch (save+restore pattern in _emit_pre_admission_failed).
        assert s.state.release_id is None
        assert s.state.target_version is None
        assert s.state.staging_dir is None

        failed = [
            e
            for e in sink.events
            if e.event_type is LifecycleEventType.FAILED
            and e.reason == "version_floor"
        ]
        assert len(failed) == 1
        # Lifecycle event DOES carry the rejected dispatch's release_id
        # so CMS can correlate.
        assert failed[0].release_id == "rel-floor"
        assert failed[0].target_version == "1.1.0"
        # Detail payload carries the comparison data.
        assert failed[0].payload["current_version"] == "1.0.0"
        assert failed[0].payload["min_from_version"] == "2.0.0"

    def test_version_floor_persistent_state_restored_on_disk(self, tmp_path):
        """After a version-floor rejection, on-disk state must not carry
        the rejected dispatch's release_id (which would later confuse a
        ``recover_on_start`` reload)."""

        s = _build_service(tmp_path, current_version="1.0.0")
        msg = _ok_dispatch(min_from_version="2.0.0", release_id="rel-floor")
        with pytest.raises(VersionFloorError):
            asyncio.run(s.handle_dispatch(msg))

        reloaded = load_state(s.state_path)
        assert reloaded.fsm is UpdaterFSMState.IDLE
        assert reloaded.release_id is None
        assert reloaded.target_version is None
        # event_id WAS incremented and DID survive the restore (the only
        # persistent side-effect of the rejection).
        assert reloaded.last_event_id == 1

    def test_force_downgrade_skips_floor_check(self, tmp_path):
        sink = _ListSink()
        s = _build_service(tmp_path, current_version="1.5.0", sink=sink)
        msg = _ok_dispatch(
            min_from_version="2.0.0",
            target_version="1.4.0",
            force_downgrade=True,
            release_id="rel-downgrade",
        )
        asyncio.run(s.handle_dispatch(msg))
        # Reached TRYBOOT_RUNNING via the happy path with default OkStubs.
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING


# ── handle_dispatch: happy path ───────────────────────────────────────────


class TestHandleDispatchHappyPath:
    def test_drives_fsm_through_tryboot_running(self, tmp_path):
        sink = _ListSink()
        downloader = _OkStub()
        verifier = _OkStub()
        stager = _OkStub()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            downloader=downloader,
            verifier=verifier,
            stager=stager,
        )
        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-1")))

        # FSM lands in TRYBOOT_RUNNING (the post-tryboot reboot is what
        # would take it further; that's not in Phase 2 scope).
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING
        assert s.state.release_id == "rel-1"
        assert s.state.target_version == "1.1.0"
        assert s.state.staging_dir is not None
        assert s.state.staging_dir.endswith("rel-1")

        # Lifecycle events were emitted in the right order with the right
        # release_id stamped.
        kinds = [e.event_type for e in sink.events]
        assert kinds == [
            LifecycleEventType.DOWNLOAD_STARTED,
            LifecycleEventType.SIGNATURE_VERIFIED,
            LifecycleEventType.STAGED,
            LifecycleEventType.TRYBOOT_INITIATED,
        ]
        assert all(e.release_id == "rel-1" for e in sink.events)

        # Collaborators were invoked once each.
        assert len(downloader.calls) == 1
        assert len(verifier.calls) == 1
        assert len(stager.calls) == 1

    def test_downloader_failure_transitions_to_failed_with_classifier(self, tmp_path):
        sink = _ListSink()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            downloader=_BoomDownloader(),
        )

        with pytest.raises(UpdaterError):
            asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-x")))

        assert s.state.fsm is UpdaterFSMState.FAILED
        # _classify_failure maps the exception type name.
        assert s.state.last_failure_reason == "error_RuntimeError"

        failed_events = [
            e for e in sink.events if e.event_type is LifecycleEventType.FAILED
        ]
        assert len(failed_events) == 1
        assert failed_events[0].reason == "error_RuntimeError"
        # Detail payload carries the exception message.
        assert "network is on fire" in failed_events[0].payload["detail"]

    def test_signature_failure_maps_to_signature_invalid(self, tmp_path):
        """BundleSignatureError → ``failed:signature_invalid`` (D54, p2-signature-verify).

        Acceptance hook (plan §"Phase 2 — Acceptance"): "Tamper test:
        hand-modify a single bundle byte; verify the device emits
        failed:signature_invalid". The classifier is what produces the
        stable wire string.
        """
        sink = _ListSink()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            verifier=_BoomSignatureVerifier(),
        )

        with pytest.raises(UpdaterError):
            asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-sig")))

        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "signature_invalid"

        failed_events = [
            e for e in sink.events if e.event_type is LifecycleEventType.FAILED
        ]
        assert len(failed_events) == 1
        assert failed_events[0].reason == "signature_invalid"
        assert "primary and recovery" in failed_events[0].payload["detail"]

    def test_integrity_failure_maps_to_bundle_invalid(self, tmp_path):
        """BundleIntegrityError → ``failed:bundle_invalid``.

        Even though integrity verification will actually run inside
        the stager once ``p2-stage-and-tryboot`` lands, the
        classifier mapping is owned by the service. This test pins
        the wire string now so a future stager that raises this type
        gets the right short code with zero additional service-side
        changes.
        """
        sink = _ListSink()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            verifier=_BoomIntegrityVerifier(),
        )

        with pytest.raises(UpdaterError):
            asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-int")))

        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "bundle_invalid"

        failed_events = [
            e for e in sink.events if e.event_type is LifecycleEventType.FAILED
        ]
        assert len(failed_events) == 1
        assert failed_events[0].reason == "bundle_invalid"
        assert "sha256 manifest mismatch" in failed_events[0].payload["detail"]


# ── continue_after_promote ─────────────────────────────────────────────────


class _BoomMigrator:
    """Migrator stub whose :meth:`run` raises a configurable exception.

    Used to drive the failure arms of
    :meth:`OSUpdaterService.continue_after_promote` — each migration-
    specific subclass exercises a distinct ``_classify_failure`` arm
    (plan #22 wire-code coverage).
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls = 0

    async def run(self) -> None:
        self.calls += 1
        raise self._exc


class _RecordingMigrator:
    """Migrator stub that records call count without raising."""

    def __init__(self) -> None:
        self.calls = 0

    async def run(self) -> None:
        self.calls += 1


def _force_state(service, fsm: UpdaterFSMState) -> None:
    """Place the service's FSM into ``fsm`` and persist it.

    ``LEGAL_TRANSITIONS`` rejects direct jumps from ``IDLE`` into
    ``PROMOTED_PENDING_MIGRATION`` (the legal route is through the
    tryboot pipeline), but for unit-testing the post-promote handoff
    we need to start there. Direct mutation + save matches the
    summary's documented approach.
    """

    service.state.fsm = fsm
    save_state(service.state, path=service.state_path)


class TestContinueAfterPromote:
    """Acceptance for the post-promote forward-migration entry point.

    Plan §"Phase 2": ``continue_after_promote`` is invoked by
    ``agora-slot-mgr`` after a successful promote. Drives ``MIGRATING
    -> IDLE`` on success and ``MIGRATING -> FAILED`` with a
    migration-specific wire code on failure (plan #22 + Phase 1
    forward-migration fence).
    """

    def test_happy_path_migrates_and_returns_to_idle(self, tmp_path):
        migrator = _RecordingMigrator()
        sink = _ListSink()
        s = _build_service(tmp_path, migrator=migrator, sink=sink)
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)

        _run(s.continue_after_promote())

        assert migrator.calls == 1
        assert s.state.fsm is UpdaterFSMState.IDLE

        reloaded = load_state(path=s.state_path)
        assert reloaded.fsm is UpdaterFSMState.IDLE

        complete = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.MIGRATION_COMPLETE
        ]
        assert len(complete) == 1
        failed = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.FAILED
        ]
        assert failed == []

    def test_fence_denied_maps_to_migration_fence_denied(self, tmp_path):
        from migration_fence import FenceStatus
        from os_updater.migrate import MigrationFenceDenied

        denied = FenceStatus(
            allowed=False,
            reason="sentinel absent",
            allowed_slot=None,
            running_slot=1,
            sentinel_present=False,
        )
        migrator = _BoomMigrator(MigrationFenceDenied(denied))
        sink = _ListSink()
        s = _build_service(tmp_path, migrator=migrator, sink=sink)
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)

        with pytest.raises(UpdaterError, match="migration_fence_denied"):
            _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "migration_fence_denied"

        failed = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.FAILED
        ]
        assert len(failed) == 1
        assert failed[0].reason == "migration_fence_denied"
        assert "sentinel absent" in failed[0].payload["detail"]

    def test_script_failure_maps_to_migration_script_failed(self, tmp_path):
        from os_updater.migrate import MigrationScriptError, MigrationStep

        step = MigrationStep(
            version=2, name="002_demo", path=tmp_path / "002_demo.sh"
        )
        migrator = _BoomMigrator(
            MigrationScriptError(
                step, returncode=7, stdout="boom\n", stderr="bad thing\n"
            )
        )
        sink = _ListSink()
        s = _build_service(tmp_path, migrator=migrator, sink=sink)
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)

        with pytest.raises(UpdaterError, match="migration_script_failed"):
            _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "migration_script_failed"

        failed = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.FAILED
        ]
        assert len(failed) == 1
        assert failed[0].reason == "migration_script_failed"

    def test_generic_migration_error_maps_to_migration_failed(self, tmp_path):
        from os_updater.migrate import MigrationError

        migrator = _BoomMigrator(MigrationError("something else broke"))
        sink = _ListSink()
        s = _build_service(tmp_path, migrator=migrator, sink=sink)
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)

        with pytest.raises(UpdaterError, match="migration_failed"):
            _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.FAILED
        assert s.state.last_failure_reason == "migration_failed"

    def test_wrong_state_raises_without_touching_fsm(self, tmp_path):
        migrator = _RecordingMigrator()
        sink = _ListSink()
        s = _build_service(tmp_path, migrator=migrator, sink=sink)
        assert s.state.fsm is UpdaterFSMState.IDLE

        with pytest.raises(
            UpdaterError, match="expected promoted_pending_migration"
        ):
            _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.IDLE
        assert migrator.calls == 0
        assert sink.events == []



# -- handle_dispatch wires STAGE_PROGRESS (agora#202) ----------------------


class _ProgressEmittingStager:
    """Fake stager that captures the ``progress_callback`` kwarg and
    invokes it once during ``stage()``. Used to verify the service
    closure emits a STAGE_PROGRESS lifecycle event with the right
    phase payload."""

    def __init__(self, phase: str = "extracting_rootfs") -> None:
        self.phase = phase
        self.calls: list[tuple] = []
        self.captured_callback = None
        self.captured_extract_callback = None

    async def stage(
        self,
        payload,
        staging_dir,
        *,
        progress_callback=None,
        extract_progress_callback=None,
    ):
        self.calls.append(("stage", payload, staging_dir))
        self.captured_callback = progress_callback
        self.captured_extract_callback = extract_progress_callback
        if progress_callback is not None:
            progress_callback(self.phase)
        if extract_progress_callback is not None:
            # Simulate one mid-pass byte-progress emit so the service-
            # side closure has something to forward to the outbox.
            extract_progress_callback("extracting_rootfs", 500_000, 1_000_000)


class TestHandleDispatchStageProgress:
    """The service-side ``_on_stage_progress`` closure forwards each
    phase the stager announces into a STAGE_PROGRESS lifecycle event
    on the outbox (agora#202)."""

    def test_stager_callback_emits_stage_progress_event(self, tmp_path):
        sink = _ListSink()
        stager = _ProgressEmittingStager(phase="extracting_rootfs")
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            stager=stager,
        )

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-progress")))

        # Service handed the stager a non-None callback.
        assert stager.captured_callback is not None

        # Exactly one STAGE_PROGRESS event with the right payload.
        progress_events = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.STAGE_PROGRESS
        ]
        assert len(progress_events) == 1
        assert progress_events[0].payload == {"phase": "extracting_rootfs"}
        assert progress_events[0].release_id == "rel-progress"
        assert progress_events[0].target_version == "1.1.0"

    def test_stage_progress_lands_between_staged_and_tryboot_initiated(self, tmp_path):
        """Progress is mid-stage, so its event_id must sit after STAGED
        and before TRYBOOT_INITIATED in the outbox stream. Pinning the
        ordering keeps CMS-side rollups sane."""
        sink = _ListSink()
        stager = _ProgressEmittingStager(phase="finalizing")
        s = _build_service(tmp_path, current_version="1.0.0", sink=sink, stager=stager)

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-order")))

        kinds = [e.event_type for e in sink.events]
        assert LifecycleEventType.STAGE_PROGRESS in kinds

        progress_idx = kinds.index(LifecycleEventType.STAGE_PROGRESS)
        # The service emits SIGNATURE_VERIFIED + STAGED *before* it
        # calls stager.stage(progress_callback=...), then emits
        # TRYBOOT_INITIATED once stage() resolves. The progress
        # callback fires *inside* stage(), so the progress event
        # lands between STAGED and TRYBOOT_INITIATED in event_id
        # order. Pinning this ordering keeps CMS-side rollups sane.
        staged_idx = kinds.index(LifecycleEventType.STAGED)
        tryboot_idx = kinds.index(LifecycleEventType.TRYBOOT_INITIATED)
        assert staged_idx < progress_idx < tryboot_idx

        # Monotonic event_id matches ordering.
        ids = [e.event_id for e in sink.events]
        assert ids == sorted(ids)

    def test_no_progress_event_when_stager_never_invokes_callback(self, tmp_path):
        """If a stager doesn't call its progress_callback (e.g. legacy
        stager from before #202, or one that fails before any phase
        boundary), the outbox simply has no STAGE_PROGRESS events.
        Regression guard against accidentally emitting an empty/None
        event from the closure."""
        sink = _ListSink()
        # _OkStub.stage just appends to .calls and returns — never calls back.
        s = _build_service(
            tmp_path, current_version="1.0.0", sink=sink, stager=_OkStub(),
        )

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-silent")))

        progress_events = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.STAGE_PROGRESS
        ]
        assert progress_events == []


# ── promote handshake tick (Bug B fix, issue #209) ─────────────────────────


class TestPromoteHandshakeTick:
    """Acceptance for the periodic promote-handshake watcher.

    Closes the gap between ``slot_mgr.promote_slot()`` (which writes the
    migration-allowed sentinel) and ``continue_after_promote()`` (which
    drives MIGRATING -> IDLE). See ``sslivins/agora#209`` Bug B and
    ``files/v0026-ota-postmortem.md`` option (a).
    """

    @staticmethod
    def _patch_fence(monkeypatch, fence_or_factory):
        """Patch ``check_migration_fence`` on the service module.

        Accepts either a ``FenceStatus`` (returned verbatim on every call)
        or a zero-arg callable producing one.
        """

        from migration_fence import FenceStatus

        def _stub(**_kwargs):
            if callable(fence_or_factory) and not isinstance(
                fence_or_factory, FenceStatus
            ):
                return fence_or_factory()
            return fence_or_factory

        monkeypatch.setattr(
            "os_updater.service.check_migration_fence", _stub
        )

    def test_tick_noop_when_fsm_not_tryboot_running(self, tmp_path, monkeypatch):
        from migration_fence import FenceStatus

        calls = {"n": 0}

        def _should_not_be_called(**_kwargs):
            calls["n"] += 1
            return FenceStatus(
                allowed=True,
                reason="ok",
                allowed_slot=2,
                running_slot=2,
                sentinel_present=True,
            )

        monkeypatch.setattr(
            "os_updater.service.check_migration_fence", _should_not_be_called
        )

        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        # state.fsm defaults to IDLE; no need to force.
        assert s.state.fsm is UpdaterFSMState.IDLE

        _run(s._tick_promote_handshake())

        assert calls["n"] == 0
        assert sink.events == []
        assert s.state.fsm is UpdaterFSMState.IDLE

    def test_tick_noop_when_no_target_version(self, tmp_path, monkeypatch):
        calls = {"n": 0}

        def _should_not_be_called(**_kwargs):
            calls["n"] += 1
            raise AssertionError("fence must not be checked without target_version")

        monkeypatch.setattr(
            "os_updater.service.check_migration_fence", _should_not_be_called
        )

        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        # target_version intentionally left None.

        _run(s._tick_promote_handshake())

        assert calls["n"] == 0
        assert sink.events == []
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING

    def test_tick_noop_when_fence_not_allowed(self, tmp_path, monkeypatch):
        from migration_fence import FenceStatus

        denied = FenceStatus(
            allowed=False,
            reason="sentinel absent",
            allowed_slot=None,
            running_slot=1,
            sentinel_present=False,
        )
        self._patch_fence(monkeypatch, denied)

        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        save_state(s.state, path=s.state_path)

        _run(s._tick_promote_handshake())

        assert sink.events == []
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING

    def test_tick_noop_when_version_mismatch(self, tmp_path, monkeypatch):
        from migration_fence import FenceStatus

        allowed = FenceStatus(
            allowed=True,
            reason="ok",
            allowed_slot=2,
            running_slot=2,
            sentinel_present=True,
        )
        self._patch_fence(monkeypatch, allowed)

        sink = _ListSink()
        # current_version_provider returns 0.0.20 but target is 0.0.21
        s = _build_service(tmp_path, sink=sink, current_version="0.0.20")
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        save_state(s.state, path=s.state_path)

        _run(s._tick_promote_handshake())

        assert sink.events == []
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING

    def test_tick_swallows_fence_exception(self, tmp_path, monkeypatch):
        def _boom(**_kwargs):
            raise RuntimeError("fence boom")

        monkeypatch.setattr(
            "os_updater.service.check_migration_fence", _boom
        )

        sink = _ListSink()
        s = _build_service(tmp_path, sink=sink)
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        save_state(s.state, path=s.state_path)

        # Must not raise — the tick is a periodic background watcher and
        # operational issues should not crash the loop.
        _run(s._tick_promote_handshake())

        assert sink.events == []
        assert s.state.fsm is UpdaterFSMState.TRYBOOT_RUNNING

    def test_tick_happy_path_drives_full_transition(self, tmp_path, monkeypatch):
        from migration_fence import FenceStatus

        sentinel_path = tmp_path / "migration-allowed"
        sentinel_path.write_text("slot=2\n")

        staging_dir = tmp_path / "staging" / "rel_happy_1"
        staging_dir.mkdir(parents=True)
        (staging_dir / "scratch.txt").write_text("payload")

        allowed = FenceStatus(
            allowed=True,
            reason="ok",
            allowed_slot=2,
            running_slot=2,
            sentinel_present=True,
        )
        self._patch_fence(monkeypatch, allowed)

        sink = _ListSink()
        migrator = _RecordingMigrator()
        s = _build_service(
            tmp_path,
            sink=sink,
            migrator=migrator,
            current_version="0.0.21",
            migration_sentinel_path=sentinel_path,
        )
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        s.state.staging_dir = str(staging_dir)
        s.state.release_id = "rel_happy_1"
        save_state(s.state, path=s.state_path)

        _run(s._tick_promote_handshake())

        # FSM walked TRYBOOT_RUNNING -> PROMOTED_PENDING_MIGRATION ->
        # MIGRATING -> IDLE.
        assert s.state.fsm is UpdaterFSMState.IDLE
        assert migrator.calls == 1

        kinds = [e.event_type for e in sink.events]
        assert LifecycleEventType.SLOT_CONFIRMED in kinds
        assert LifecycleEventType.PROMOTED in kinds
        assert LifecycleEventType.MIGRATION_COMPLETE in kinds
        # Ordering: slot_confirmed -> promoted -> migration_complete.
        sc_idx = kinds.index(LifecycleEventType.SLOT_CONFIRMED)
        pr_idx = kinds.index(LifecycleEventType.PROMOTED)
        mc_idx = kinds.index(LifecycleEventType.MIGRATION_COMPLETE)
        assert sc_idx < pr_idx < mc_idx

        # Slot payload from fence.allowed_slot.
        sc_event = sink.events[sc_idx]
        pr_event = sink.events[pr_idx]
        assert sc_event.payload.get("slot") == 2
        assert pr_event.payload.get("slot") == 2

        # Cleanup ran: staging dir removed, sentinel unlinked.
        assert not staging_dir.exists()
        assert not sentinel_path.exists()

        # IDLE transition cleared dispatch-specific fields.
        assert s.state.target_version is None
        assert s.state.staging_dir is None
        assert s.state.release_id is None

    def test_tick_cleanup_handles_missing_staging_dir(
        self, tmp_path, monkeypatch
    ):
        """Happy path but ``state.staging_dir`` points at a path that
        doesn't exist. ``shutil.rmtree(..., ignore_errors=True)`` must
        swallow the FileNotFoundError so the FSM still completes."""
        from migration_fence import FenceStatus

        sentinel_path = tmp_path / "migration-allowed"
        sentinel_path.write_text("slot=2\n")

        allowed = FenceStatus(
            allowed=True,
            reason="ok",
            allowed_slot=2,
            running_slot=2,
            sentinel_present=True,
        )
        self._patch_fence(monkeypatch, allowed)

        sink = _ListSink()
        s = _build_service(
            tmp_path,
            sink=sink,
            migrator=_RecordingMigrator(),
            current_version="0.0.21",
            migration_sentinel_path=sentinel_path,
        )
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        # Point at a path that was never created.
        s.state.staging_dir = str(tmp_path / "staging" / "does-not-exist")
        save_state(s.state, path=s.state_path)

        _run(s._tick_promote_handshake())

        assert s.state.fsm is UpdaterFSMState.IDLE
        assert not sentinel_path.exists()

    def test_continue_after_promote_cleans_up_staging_dir(self, tmp_path):
        """Direct invocation: ``continue_after_promote`` from
        PROMOTED_PENDING_MIGRATION must rmtree ``state.staging_dir`` before
        the IDLE transition clears the field."""
        staging_dir = tmp_path / "staging" / "rel_cleanup_1"
        staging_dir.mkdir(parents=True)
        (staging_dir / "blob.bin").write_bytes(b"x" * 32)

        sink = _ListSink()
        s = _build_service(
            tmp_path,
            sink=sink,
            migrator=_RecordingMigrator(),
            migration_sentinel_path=tmp_path / "sentinel-absent",
        )
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)
        s.state.staging_dir = str(staging_dir)
        save_state(s.state, path=s.state_path)

        _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.IDLE
        assert not staging_dir.exists()

    def test_continue_after_promote_unlinks_sentinel(self, tmp_path):
        """Direct invocation: ``continue_after_promote`` must remove the
        migration-allowed sentinel before the IDLE transition. Otherwise
        a stale sentinel would let the next tryboot fence-pass without
        slot-mgr ever re-confirming."""
        sentinel_path = tmp_path / "migration-allowed"
        sentinel_path.write_text("slot=2\n")

        sink = _ListSink()
        s = _build_service(
            tmp_path,
            sink=sink,
            migrator=_RecordingMigrator(),
            migration_sentinel_path=sentinel_path,
        )
        _force_state(s, UpdaterFSMState.PROMOTED_PENDING_MIGRATION)

        _run(s.continue_after_promote())

        assert s.state.fsm is UpdaterFSMState.IDLE
        assert not sentinel_path.exists()

    def test_promote_handshake_loop_cancellable(self, tmp_path, monkeypatch):
        """Spawning the loop, awaiting one tick, then cancelling must
        exit cleanly without raising past the cancellation."""
        from migration_fence import FenceStatus

        # Fence denied → tick is a fast no-op. Lets us spin the loop a
        # couple of times without driving any FSM state.
        denied = FenceStatus(
            allowed=False,
            reason="sentinel absent",
            allowed_slot=None,
            running_slot=1,
            sentinel_present=False,
        )
        self._patch_fence(monkeypatch, denied)

        sink = _ListSink()
        s = _build_service(
            tmp_path,
            sink=sink,
            promote_handshake_tick_sec=0.01,
        )
        _force_state(s, UpdaterFSMState.TRYBOOT_RUNNING)
        s.state.target_version = "0.0.21"
        save_state(s.state, path=s.state_path)

        async def _spin_and_cancel():
            task = asyncio.create_task(s._promote_handshake_loop())
            # Give the loop a couple of ticks.
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                return "cancelled"
            return "exited"

        result = asyncio.run(_spin_and_cancel())
        assert result == "cancelled"
        # No spurious lifecycle events from the no-op ticks.
        assert sink.events == []


# -- handle_dispatch wires EXTRACT_PROGRESS (agora#215) -------------------------


class TestHandleDispatchExtractProgress:
    """The service-side ``_on_extract_progress`` closure forwards each
    byte-progress emit from the stager into an EXTRACT_PROGRESS
    lifecycle event on the outbox (agora#215)."""

    def test_stager_extract_callback_emits_extract_progress_event(self, tmp_path):
        sink = _ListSink()
        stager = _ProgressEmittingStager(phase="extracting_rootfs")
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            stager=stager,
        )

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-extract")))

        # Service handed the stager a non-None extract callback.
        assert stager.captured_extract_callback is not None

        extract_events = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.EXTRACT_PROGRESS
        ]
        assert len(extract_events) == 1
        assert extract_events[0].payload == {
            "phase": "extracting_rootfs",
            "bytes_done": 500_000,
            "bytes_total": 1_000_000,
        }
        assert extract_events[0].release_id == "rel-extract"
        assert extract_events[0].target_version == "1.1.0"

    def test_extract_progress_lands_between_staged_and_tryboot_initiated(self, tmp_path):
        """Like STAGE_PROGRESS, EXTRACT_PROGRESS is mid-stage and must
        sit between STAGED and TRYBOOT_INITIATED so CMS rollups are
        sane."""
        sink = _ListSink()
        stager = _ProgressEmittingStager(phase="extracting_rootfs")
        s = _build_service(tmp_path, current_version="1.0.0", sink=sink, stager=stager)

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-extract-ord")))

        kinds = [e.event_type for e in sink.events]
        assert LifecycleEventType.EXTRACT_PROGRESS in kinds
        ext_idx = kinds.index(LifecycleEventType.EXTRACT_PROGRESS)
        staged_idx = kinds.index(LifecycleEventType.STAGED)
        tryboot_idx = kinds.index(LifecycleEventType.TRYBOOT_INITIATED)
        assert staged_idx < ext_idx < tryboot_idx

        ids = [e.event_id for e in sink.events]
        assert ids == sorted(ids)


# -- handle_dispatch wires DOWNLOAD_PROGRESS (agora#219) -----------------------


class _ProgressEmittingDownloader:
    """Downloader stub that fires the ``progress_callback`` kwarg so
    the test can assert the service's ``_on_download_progress`` closure
    forwards the bytes_done/bytes_total into a DOWNLOAD_PROGRESS event.

    Mirrors ``_ProgressEmittingStager`` -- separate class so the
    captured-callback assertions stay local to each test.
    """

    def __init__(self) -> None:
        self.captured_progress_callback = None
        self.calls: list[tuple] = []

    async def run(self, payload, staging_dir, *, progress_callback=None):
        self.calls.append(("run", payload, staging_dir))
        self.captured_progress_callback = progress_callback
        if progress_callback is not None:
            # Simulate two rate-limited chunks plus the final 100% emit
            # so the test asserts on the last (terminal) event the same
            # way the real downloader does.
            progress_callback(0, 1_000_000)
            progress_callback(500_000, 1_000_000)
            progress_callback(1_000_000, 1_000_000)


class TestHandleDispatchDownloadProgress:
    """The service-side ``_on_download_progress`` closure forwards each
    rate-limited chunk emit from the downloader into a DOWNLOAD_PROGRESS
    lifecycle event (agora#219).  Pre-#219 the service never bound a
    download callback, so the BundleDownloader's ``progress_callback``
    field stayed ``None`` and zero download_progress events ever fired
    on production -- the CMS badge stayed empty during the whole
    download phase.
    """

    def test_downloader_progress_callback_emits_lifecycle_events(self, tmp_path):
        sink = _ListSink()
        downloader = _ProgressEmittingDownloader()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            downloader=downloader,
        )

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-dl")))

        # Service handed the downloader a non-None progress callback.
        assert downloader.captured_progress_callback is not None

        progress = [
            e for e in sink.events
            if e.event_type is LifecycleEventType.DOWNLOAD_PROGRESS
        ]
        assert len(progress) == 3
        assert progress[0].payload == {"bytes_done": 0, "bytes_total": 1_000_000}
        assert progress[-1].payload == {
            "bytes_done": 1_000_000, "bytes_total": 1_000_000,
        }
        # Every event must carry the dispatch's release / target metadata.
        for evt in progress:
            assert evt.release_id == "rel-dl"
            assert evt.target_version == "1.1.0"

    def test_download_progress_lands_between_download_started_and_staged(self, tmp_path):
        """DOWNLOAD_PROGRESS is mid-download and must sit between
        DOWNLOAD_STARTED and SIGNATURE_VERIFIED / STAGED so CMS
        timeline rollups stay ordered.  Mirrors the same constraint
        the existing EXTRACT_PROGRESS test enforces.
        """
        sink = _ListSink()
        downloader = _ProgressEmittingDownloader()
        s = _build_service(
            tmp_path,
            current_version="1.0.0",
            sink=sink,
            downloader=downloader,
        )

        asyncio.run(s.handle_dispatch(_ok_dispatch(release_id="rel-dl-ord")))

        kinds = [e.event_type for e in sink.events]
        assert LifecycleEventType.DOWNLOAD_PROGRESS in kinds
        started_idx = kinds.index(LifecycleEventType.DOWNLOAD_STARTED)
        first_progress_idx = kinds.index(LifecycleEventType.DOWNLOAD_PROGRESS)
        staged_idx = kinds.index(LifecycleEventType.STAGED)
        assert started_idx < first_progress_idx < staged_idx

        ids = [e.event_id for e in sink.events]
        assert ids == sorted(ids)


# -- service.run binds the asyncio loop on the sink (agora#219) ---------------


class TestServiceRunBindsLoop:
    """``OSUpdaterService.run`` MUST bind the running asyncio loop on
    the sink at startup so worker-thread emit sites (stage/extract
    progress) can schedule sends via ``run_coroutine_threadsafe``.
    Pre-#219 the sink's ``put`` called ``asyncio.get_running_loop`` in
    the calling thread itself, which raised in worker threads, so
    every off-loop progress event was silently dropped.

    This is a regression guard against someone removing the
    ``bind_loop`` call in a future refactor.  Duck-typed: sinks that
    don't expose ``bind_loop`` (OutboxEventSink) MUST still work.
    """

    def test_run_calls_bind_loop_when_sink_supports_it(self, tmp_path):
        bound: list = []

        class _BindCapturingSink:
            def __init__(self):
                self.events = []

            def bind_loop(self, loop):
                bound.append(loop)

            def put(self, event):
                self.events.append(event)

        sink = _BindCapturingSink()
        s = _build_service(tmp_path, current_version="1.0.0", sink=sink)

        async def runner():
            task = asyncio.create_task(s.run())
            # Yield enough for run() to reach the bind_loop call before
            # the first long await (transport.connect()).
            for _ in range(5):
                await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return asyncio.get_running_loop()

        loop_used = asyncio.run(runner())
        assert len(bound) == 1
        assert bound[0] is loop_used

    def test_run_works_when_sink_has_no_bind_loop_method(self, tmp_path):
        """OutboxEventSink doesn't have ``bind_loop`` -- the service
        must duck-type the call and skip it without raising."""

        class _NoBindSink:
            def __init__(self):
                self.events = []

            def put(self, event):
                self.events.append(event)

        sink = _NoBindSink()
        s = _build_service(tmp_path, current_version="1.0.0", sink=sink)

        async def runner():
            task = asyncio.create_task(s.run())
            for _ in range(5):
                await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # Must not raise.
        asyncio.run(runner())
