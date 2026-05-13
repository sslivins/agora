"""Forward-migration runner — implemented by sibling todo p2-forward-migration.

After a successful slot promote (Phase 1 :mod:`slot_mgr`), the runner
discovers shell scripts at ``/etc/agora/migrations/NNN_*.sh`` where ``NNN``
> the current ``/data/SCHEMA_VERSION``, executes them in ascending order
under ``set -euo pipefail``, and bumps the version on success. See plan.md
§"Phase 2 — Deliverables" + #22.

Gated by the Phase 1 ``migration-allowed`` sentinel (see
:mod:`migration_fence`) — the runner refuses to start unless the sentinel
matches the current running slot.
"""

from __future__ import annotations


class MigrationError(Exception):
    """Base class for migration-related failures."""


def run_pending_migrations(*args, **kwargs):  # noqa: D401
    raise NotImplementedError("see sibling todo p2-forward-migration")
