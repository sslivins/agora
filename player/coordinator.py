"""Top-level orchestrator for the multi-display chromium-backend player.

The Coordinator owns:
  * The shared :class:`SwayManager` (one sway instance, declares both
    HDMI-A-1 and HDMI-A-2 at boot).
  * A ``dict[slot, SlotState]`` mapping slot id ("A" / "B") to its
    live :class:`SlotState`.

Slot lifecycle:
  * Slot A is always activated at ``start()`` -- the primary display.
  * Slot B is activated at ``start()`` if ``persist/devices.json`` has
    slot B credentials (set by a prior :func:`bind_display` WebSocket
    message), and at runtime by :meth:`activate_slot` from the
    ``bind_display`` handler.
  * :meth:`deactivate_slot` tears a slot down. ``unbind_display`` calls
    this then wipes slot B credentials.

This module is the integration seam between the WebSocket message
handlers in ``cms_client`` and the per-slot playback machinery in
``SlotState``. It deliberately does NOT own:
  * The GLib main loop (still :class:`AgoraPlayer`'s).
  * inotify watchers / file-change callbacks (still :class:`AgoraPlayer`'s).
  * The CMS WebSocket client (still :class:`cms_client.CMSClient`'s).

Those stay where they are for PR 2a; ``AgoraPlayer`` plumbs slot
dispatch into its existing callbacks. A future refactor may move them
here, but this commit keeps the blast radius small.

Pure scaffolding: nothing instantiates ``Coordinator`` yet. The wiring
into :class:`AgoraPlayer` lands in the next commit.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Optional

from player.slot_state import SlotPaths, SlotState
from player.sway_manager import SwayManager, app_id_for_slot

logger = logging.getLogger("agora.player.coordinator")


# Per-slot shell server ports. Slot A keeps the historical port (8780)
# so the legacy code paths that hard-coded it stay correct. Slot B
# gets the next port.
PORT_FOR_SLOT = {
    "A": 8780,
    "B": 8781,
}


def _port_for_slot(slot: str) -> int:
    try:
        return PORT_FOR_SLOT[slot]
    except KeyError as exc:
        raise ValueError(f"Unknown slot {slot!r}; expected one of {list(PORT_FOR_SLOT)}") from exc


# Type alias for the chromium event callback: invoked as
#   on_chromium_event(slot: str, payload: dict)
# whenever a slot's shell WebSocket emits an event (ended, error, etc.).
OnChromiumEvent = Callable[[str, dict], None]


class Coordinator:
    """Singleton orchestrator for sway + per-slot SlotStates.

    Constructed once per :class:`AgoraPlayer` instance when the
    chromium backend is enabled (the default; opt out via
    ``AGORA_PLAYER_BACKEND=mpv``).
    """

    def __init__(
        self,
        base_path: Path,
        assets_dir: Path,
        available_slots: tuple[str, ...] = ("A",),
        slot_a_paths_mode: str = "legacy",
        on_chromium_event: Optional[OnChromiumEvent] = None,
    ) -> None:
        """Initialise the Coordinator.

        :param base_path: ``/opt/agora`` (or test fixture root).
        :param assets_dir: shared asset cache; ALL slots read from here.
        :param available_slots: which slots this hardware supports
            (typically ``("A",)`` on single-output boards, ``("A", "B")``
            on Pi5 / CM5). Determines sway config + which slot ids can
            be activated.
        :param slot_a_paths_mode: ``"legacy"`` (slot A uses pre-multi-
            display path layout for back-compat) or ``"per_slot"`` (slot
            A also writes under ``state/displays/A/``). The transition
            from legacy → per_slot happens in a separate commit; until
            then, default is ``"legacy"``.
        :param on_chromium_event: callback invoked as
            ``on_chromium_event(slot, payload)`` for each shell event.
        """
        if slot_a_paths_mode not in ("legacy", "per_slot"):
            raise ValueError(
                f"slot_a_paths_mode must be 'legacy' or 'per_slot', got {slot_a_paths_mode!r}"
            )
        self.base_path = Path(base_path)
        self.assets_dir = Path(assets_dir)
        self.available_slots = tuple(available_slots)
        self.slot_a_paths_mode = slot_a_paths_mode
        self._on_chromium_event = on_chromium_event

        # SwayManager is constructed for the *available* slot set so
        # its config only emits for_window pin lines for slots that
        # might actually have a kiosk window. Outputs (HDMI-A-1 and
        # HDMI-A-2) are always declared regardless -- see SwayManager.
        self.sway_manager = SwayManager(slots=self.available_slots)

        self.slots: dict[str, SlotState] = {}

    # ── Lifecycle ──

    def start(self) -> None:
        """Start sway and activate slot A. Conditionally activate slot B.

        Slot B is activated only if (a) it's in ``available_slots`` and
        (b) ``persist/devices.json`` has slot B credentials. The
        ``bind_display`` WS handler activates slot B at runtime if it
        was not yet present.
        """
        self.sway_manager.start()
        self.activate_slot("A")
        if "B" in self.available_slots and self._slot_b_creds_present():
            self.activate_slot("B")

    def stop(self) -> None:
        """Tear down all active slots, then stop sway."""
        for slot in list(self.slots):
            self.deactivate_slot(slot)
        self.sway_manager.stop()

    # ── Slot management ──

    def activate_slot(self, slot: str) -> Optional[SlotState]:
        """Bring a slot online. Idempotent: returns existing state if already up.

        Returns ``None`` if ``slot`` is not in ``available_slots`` (e.g.
        a bind_display targets slot B on a single-output board) -- the
        caller is responsible for NACKing the bind in that case.
        """
        if slot in self.slots:
            return self.slots[slot]
        if slot not in self.available_slots:
            logger.warning(
                "Coordinator: cannot activate slot %s; not in available_slots %s",
                slot, self.available_slots,
            )
            return None
        paths = self._paths_for_slot(slot)
        chromium_player = self._build_chromium_player(slot)
        state = SlotState(
            slot=slot,
            paths=paths,
            chromium_player=chromium_player,
        )
        logger.info(
            "Coordinator: activating slot %s (port=%d, paths_mode=%s)",
            slot, chromium_player.port,
            "legacy" if (slot == "A" and self.slot_a_paths_mode == "legacy") else "per_slot",
        )
        state.start()
        self.slots[slot] = state
        return state

    def deactivate_slot(self, slot: str) -> None:
        """Tear a slot down. No-op if the slot was never active."""
        state = self.slots.pop(slot, None)
        if state is None:
            return
        logger.info("Coordinator: deactivating slot %s", slot)
        state.stop()

    def has_slot(self, slot: str) -> bool:
        return slot in self.slots

    # ── Internals ──

    def _paths_for_slot(self, slot: str) -> SlotPaths:
        """Pick the right SlotPaths factory for this slot."""
        if slot == "A" and self.slot_a_paths_mode == "legacy":
            return SlotPaths.legacy(self.base_path)
        return SlotPaths.for_slot(self.base_path, slot)

    def _build_chromium_player(self, slot: str) -> "ChromiumPlayer":  # noqa: F821  (lazy import)
        """Construct the per-slot ChromiumPlayer wired to the shared sway."""
        # Lazy import keeps importing this module cheap in tests that
        # mock out the chromium backend entirely.
        from player.chromium_backend import ChromiumPlayer
        return ChromiumPlayer(
            assets_dir=self.assets_dir,
            port=_port_for_slot(slot),
            sway_manager=self.sway_manager,
            app_id=app_id_for_slot(slot),
            on_event=self._make_event_callback(slot),
        )

    def _make_event_callback(self, slot: str) -> Optional[Callable[[dict], None]]:
        """Adapt the slot-aware Coordinator callback to ChromiumPlayer's contract.

        ``ChromiumPlayer`` expects ``Callable[[dict], None]``; the
        Coordinator's caller wants ``Callable[[slot, dict], None]`` so it
        can route events to the right SlotState. We close over the slot
        id here.
        """
        cb = self._on_chromium_event
        if cb is None:
            return None
        slot_id = slot
        def _cb(payload: dict) -> None:
            try:
                cb(slot_id, payload)
            except Exception:
                logger.exception(
                    "Coordinator: chromium event callback for slot %s raised",
                    slot_id,
                )
        return _cb

    def _slot_b_creds_present(self) -> bool:
        """True iff persist/devices.json has slot B credentials.

        Late import so the Coordinator stays decoupled from
        ``shared.devices_store`` at module load time (and so tests can
        construct a Coordinator without setting up a fake devices.json).
        """
        try:
            from shared.devices_store import SLOT_B, read_slot
            creds = read_slot(self.base_path / "persist", SLOT_B)
        except Exception:
            logger.exception("Coordinator: error reading devices.json slot B")
            return False
        return creds is not None and bool(creds.get("api_key"))
