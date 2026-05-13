"""Slot staging + tryboot trigger — implemented by sibling todo p2-stage-and-tryboot.

Owns the steps after signature verification:

* rsync the staged ``boot/`` content into the inactive boot partition
* rsync the staged ``root/`` content into the inactive root partition
* shell out to ``agora-slot-mgr trigger-tryboot`` (Phase 1 CLI)

Exposed as the ``Stager`` collaborator on :class:`os_updater.service.OSUpdaterService`.
"""

from __future__ import annotations


class StagingError(Exception):
    """Base class for staging-related failures."""


def stage_to_inactive_slot(*args, **kwargs):  # noqa: D401
    raise NotImplementedError("see sibling todo p2-stage-and-tryboot")


def trigger_tryboot(*args, **kwargs):  # noqa: D401
    raise NotImplementedError("see sibling todo p2-stage-and-tryboot")
