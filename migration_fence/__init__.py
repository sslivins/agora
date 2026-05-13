"""Forward-migration fence reader.

The companion to :func:`slot_mgr.promote_slot`. When ``promote_slot()`` makes
a tentative slot the permanent default it writes a ``slot=N`` sentinel at
``/data/agora/migration-allowed``. This package is the consumer side: every
piece of agora code that wants to forward-migrate persistent state on
``/data`` (schema bumps, on-disk format changes, anything that's hard to
undo) must call :func:`is_migration_allowed` first and refuse if the
sentinel is absent or names a different slot than the one we're running on.

Why a fence at all
------------------

Phase 1 boots are always two-stage: tryboot -> slot-confirm -> promote.
Between tryboot and promote, the new agora.service is running but the
device could revert to the previous slot on the next normal reboot (the
bootloader's [all] section still points at the old slot). If the new
service migrated /data forward during that window, a tryboot-revert would
land the *old* kernel on a *newer* /data and either crash on schema
mismatch or - worse - silently corrupt data.

The fence makes "migrate forward on /data" idempotent with promotion:

* Sentinel missing                  -> tentative, do not migrate
* Sentinel present, slot mismatch   -> sentinel is from a previous slot,
                                       do not migrate
* Sentinel present, slot matches    -> we own this slot now, migrating
                                       is safe

CLI
---

``python -m migration_fence`` prints the current fence status as JSON and
exits 0 when migration is allowed, 1 when it is not. Use this from
``agora.service``'s pre-migration step::

    ExecStartPre=/usr/bin/python3 -m migration_fence --check

Surface
-------

Public API is intentionally small:

* :func:`is_migration_allowed` - one-liner boolean for service code
* :func:`check_migration_fence` - full structured status for diagnostics
* :class:`FenceStatus` - dataclass returned by check_migration_fence
* :exc:`MigrationFenceError` - reserved for *programmer* errors (e.g.
  passing a negative slot to a helper). Operational failures are reported
  as ``FenceStatus(allowed=False, reason=...)``, not exceptions.
"""

from migration_fence.core import (
    DEFAULT_SENTINEL_PATH,
    FenceStatus,
    MigrationFenceError,
    check_migration_fence,
    is_migration_allowed,
    parse_sentinel,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_SENTINEL_PATH",
    "FenceStatus",
    "MigrationFenceError",
    "__version__",
    "check_migration_fence",
    "is_migration_allowed",
    "parse_sentinel",
]
