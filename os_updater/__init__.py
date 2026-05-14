"""agora-os-updater — on-device daemon that orchestrates OS updates.

Phase 2 of the A/B atomic update plan (plan.md §"Phase 2 — agora-os-updater
service"). Subscribes to the CMS over the existing WPS connection for
``os_update_dispatch`` control messages, downloads the bundle, verifies the
minisign signature + sha256 manifest, stages it to ``/data/.update/staging/``,
calls :mod:`slot_mgr` to write the inactive slot, triggers tryboot, and emits
lifecycle events back to CMS over WPS.

This package ships the SHELL of the daemon: the package layout, the explicit
8-state finite state machine, the persistent state file
(``/data/agora/updater-state.json``), the dispatch payload model + validation,
the lifecycle-event emitter scaffold, and the async run loop.  The actual
bundle download, signature verification, slot staging, and forward-migration
runner are filled in by sibling todos (``p2-bundle-format``,
``p2-signature-verify``, ``p2-stage-and-tryboot``, ``p2-forward-migration``)
via the injection seams documented on :class:`OSUpdaterService`.

Phase 2 ratified sub-decisions #17–24 govern the wire protocol, on-disk
layout, and error semantics — see plan.md.
"""

from os_updater.state import (
    DEFAULT_STATE_PATH,
    SCHEMA_VERSION,
    UpdaterFSMState,
    UpdaterState,
    is_busy,
    load_state,
    save_state,
    transition,
)
from os_updater.dispatch import (
    DispatchPayload,
    DispatchPayloadError,
    parse_dispatch_payload,
)
from os_updater.events import (
    DEFAULT_OUTBOX_DIR,
    LifecycleEvent,
    LifecycleEventType,
    OutboxEventSink,
    emit_event,
    next_event_id,
)
from os_updater.service import (
    DEFAULT_STAGING_ROOT,
    OSUpdaterService,
    UpdaterBusyError,
    UpdaterError,
    VersionFloorError,
)
from os_updater.bundle import (
    DEFAULT_PRIMARY_PUBKEY,
    DEFAULT_PUBKEY_SEARCH_DIR,
    DEFAULT_RECOVERY_PUBKEY,
    BundleError,
    BundleIntegrityError,
    BundleMeta,
    BundleSignatureError,
    Runner,
    discover_pubkeys,
    parse_bundle_meta,
    verify_bundle_manifest,
    verify_signature,
)
from os_updater.migrate import (
    DEFAULT_MIGRATIONS_ROOT,
    DEFAULT_SCHEMA_VERSION_PATH,
    ForwardMigrator,
    MigrationDiscoveryError,
    MigrationError,
    MigrationFenceDenied,
    MigrationResult,
    MigrationScriptError,
    MigrationStep,
    SchemaVersionError,
    discover_migrations,
    read_schema_version,
    run_pending_migrations,
    write_schema_version,
)
from os_updater.verifier import SignatureVerifier
from os_updater.apply import (
    CmdlineError,
    FleetStateMissingError,
    FleetStateWriteError,
    FstabError,
    RsyncError,
    SlotStager,
    StagingError,
    TrybootError,
    extract_meta_only,
    rewrite_cmdline,
    stream_extract_subtree,
    substitute_fstab,
)

__version__ = "0.1.0"

__all__ = [
    "BundleError",
    "BundleIntegrityError",
    "BundleMeta",
    "BundleSignatureError",
    "CmdlineError",
    "DEFAULT_MIGRATIONS_ROOT",
    "DEFAULT_OUTBOX_DIR",
    "DEFAULT_PRIMARY_PUBKEY",
    "DEFAULT_PUBKEY_SEARCH_DIR",
    "DEFAULT_RECOVERY_PUBKEY",
    "DEFAULT_SCHEMA_VERSION_PATH",
    "DEFAULT_STAGING_ROOT",
    "DEFAULT_STATE_PATH",
    "DispatchPayload",
    "DispatchPayloadError",
    "FleetStateMissingError",
    "FleetStateWriteError",
    "ForwardMigrator",
    "FstabError",
    "LifecycleEvent",
    "LifecycleEventType",
    "MigrationDiscoveryError",
    "MigrationError",
    "MigrationFenceDenied",
    "MigrationResult",
    "MigrationScriptError",
    "MigrationStep",
    "OSUpdaterService",
    "OutboxEventSink",
    "RsyncError",
    "Runner",
    "SCHEMA_VERSION",
    "SchemaVersionError",
    "SignatureVerifier",
    "SlotStager",
    "StagingError",
    "TrybootError",
    "UpdaterBusyError",
    "UpdaterError",
    "UpdaterFSMState",
    "UpdaterState",
    "VersionFloorError",
    "__version__",
    "discover_migrations",
    "discover_pubkeys",
    "emit_event",
    "extract_meta_only",
    "is_busy",
    "load_state",
    "next_event_id",
    "parse_bundle_meta",
    "parse_dispatch_payload",
    "read_schema_version",
    "rewrite_cmdline",
    "run_pending_migrations",
    "save_state",
    "stream_extract_subtree",
    "substitute_fstab",
    "transition",
    "verify_bundle_manifest",
    "verify_signature",
    "write_schema_version",
]
