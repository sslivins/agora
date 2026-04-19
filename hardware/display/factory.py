"""Board-aware factory for :class:`DisplayProbe` instances."""
from __future__ import annotations

import logging
from typing import Optional

from shared.board import Board, HdmiPort, get_board, get_hdmi_ports

from .base import DisplayProbe
from .drm_sysfs import DrmSysfsDisplayProbe
from .i2c_edid import I2cEdidDisplayProbe
from .null import NullDisplayProbe

logger = logging.getLogger("agora.hardware.display")

_cached_probe: Optional[DisplayProbe] = None


def build_probe(board: Board, ports: list[HdmiPort]) -> DisplayProbe:
    """Return the best :class:`DisplayProbe` for *board* and *ports*.

    * Pi 5 / CM 5 → :class:`DrmSysfsDisplayProbe`
    * Pi Zero 2 W / Pi 3 / Pi 4 → :class:`I2cEdidDisplayProbe`
    * Unknown → :class:`NullDisplayProbe`
    """
    if board is Board.PI_5:
        return DrmSysfsDisplayProbe(ports)
    if board in (Board.PI_4, Board.ZERO_2W):
        return I2cEdidDisplayProbe(ports)
    return NullDisplayProbe(ports)


def get_display_probe() -> DisplayProbe:
    """Return a cached :class:`DisplayProbe` for the current board."""
    global _cached_probe
    if _cached_probe is None:
        board = get_board()
        ports = get_hdmi_ports()
        _cached_probe = build_probe(board, ports)
        logger.info(
            "Display probe: %s (board=%s, ports=%s)",
            type(_cached_probe).__name__,
            board.value,
            [p.name for p in ports],
        )
    return _cached_probe


def reset_cached_probe() -> None:
    """Forget the cached probe instance (test-only helper)."""
    global _cached_probe
    _cached_probe = None
