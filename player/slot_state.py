"""Per-slot state for the multi-display chromium-backend player.

A ``SlotState`` represents the runtime state of one virtual device --
i.e. one HDMI output's playback pipeline. The Pi5 (and CM5) can host
two of these (slots "A" and "B"); other boards host only "A".

What lives in a SlotState:
  * A :class:`ChromiumPlayer` instance (one per slot, on its own port,
    pinned to its target HDMI output by ``app_id``).
  * A :class:`SlotPaths` describing where this slot's volatile state
    (desired / current / cms_status / schedule) and durable per-slot
    config (splash) live on disk.

What does NOT live in a SlotState:
  * Sway -- shared across slots, owned by the ``Coordinator``.
  * The CMS WebSocket -- one per *device*, owned by ``cms_client``.
    Each CMS message names its target slot in its envelope (added in
    a later PR).
  * The asset cache (``assets_dir``) -- fleet-wide; slots share it.

Path layouts:
  * ``SlotPaths.legacy(base)`` -- the pre-multi-display layout, where
    a single device's state lived directly under ``state/`` and
    ``persist/``. Used for slot A during the transition so existing
    code paths keep working.
  * ``SlotPaths.for_slot(base, slot)`` -- the new per-slot layout
    under ``state/displays/<slot>/`` and ``persist/displays/<slot>/``.
    Used for slot B always; slot A will move here once dual-write is
    in place.

This module is pure scaffolding: nothing instantiates ``SlotState`` yet.
The ``Coordinator`` (see ``player/coordinator.py``) will be the only
caller, wired in by a subsequent commit.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from player.chromium_backend import ChromiumPlayer

logger = logging.getLogger("agora.player.slot_state")


@dataclass(frozen=True)
class SlotPaths:
    """Per-slot file path constants for the chromium-backend player.

    Five paths only; everything else (asset cache, auth tokens, board
    config, etc.) is fleet-wide / device-wide and lives outside the
    per-slot tree.
    """

    desired: Path        # current playback intent (state/)
    current: Path        # what is actually playing (state/)
    cms_status: Path     # last heartbeat-shape status snapshot (state/)
    schedule: Path       # schedule overlay (state/)
    splash: Path         # configured splash image path/value (persist/)

    @classmethod
    def legacy(cls, base_path: Path) -> "SlotPaths":
        """Pre-multi-display layout used for slot A during the transition.

        Slot A keeps writing to these legacy locations until the
        ``state/displays/A/`` re-rooting commit lands (with dual-write
        so a future rollback can still find the legacy files).
        """
        base = Path(base_path)
        return cls(
            desired=base / "state" / "desired.json",
            current=base / "state" / "current.json",
            cms_status=base / "state" / "cms_status.json",
            schedule=base / "state" / "schedule.json",
            splash=base / "persist" / "splash",
        )

    @classmethod
    def for_slot(cls, base_path: Path, slot: str) -> "SlotPaths":
        """Per-slot layout: state/displays/<slot>/ + persist/displays/<slot>/.

        Used for slot B always, and for slot A after re-rooting lands.
        """
        base = Path(base_path)
        state = base / "state" / "displays" / slot
        persist = base / "persist" / "displays" / slot
        return cls(
            desired=state / "desired.json",
            current=state / "current.json",
            cms_status=state / "cms_status.json",
            schedule=state / "schedule.json",
            splash=persist / "splash",
        )


class SlotState:
    """Per-slot ChromiumPlayer + state-paths bundle.

    Constructed by the ``Coordinator`` -- callers should not build one
    directly. See :meth:`Coordinator.activate_slot`.

    Lifecycle:
        state = SlotState(slot="A", paths=..., chromium_player=...)
        state.start()
        # ... daemon drives playback by writing to state.paths.desired,
        # and by calling state.chromium_player.show_image(...)
        state.stop()
    """

    def __init__(
        self,
        slot: str,
        paths: SlotPaths,
        chromium_player: "ChromiumPlayer",
    ) -> None:
        self.slot = slot
        self.paths = paths
        self.chromium_player = chromium_player

    # ── Lifecycle ──

    def ensure_dirs(self) -> None:
        """Create the state/persist parent dirs for this slot.

        Idempotent. Safe to call at every start (slot bind/unbind may
        recreate the SlotState in place).
        """
        for p in (
            self.paths.desired.parent,
            self.paths.current.parent,
            self.paths.cms_status.parent,
            self.paths.schedule.parent,
            self.paths.splash.parent,
        ):
            try:
                p.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logger.warning(
                    "SlotState[%s]: could not create %s: %s", self.slot, p, e,
                )

    def start(self) -> None:
        """Bring this slot online: ensure dirs + start its ChromiumPlayer."""
        self.ensure_dirs()
        self.chromium_player.start()

    def stop(self) -> None:
        """Tear this slot down: stop its ChromiumPlayer.

        Does NOT delete state files -- those are the daemon's
        rollback / restart safety net.
        """
        self.chromium_player.stop()

    def is_alive(self) -> bool:
        return self.chromium_player.is_alive()

    def __repr__(self) -> str:
        return (
            f"SlotState(slot={self.slot!r}, port={self.chromium_player.port}, "
            f"alive={self.is_alive()})"
        )
