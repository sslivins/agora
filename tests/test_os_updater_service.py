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
    async def run(self, payload, staging_dir):
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
