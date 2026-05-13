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
