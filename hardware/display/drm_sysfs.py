"""DRM sysfs based HDMI probe (primary for Pi 5 / CM 5).

Reads ``/sys/class/drm/card*-{drm_connector}/status`` for each HDMI port.
The file contains ``connected``, ``disconnected`` or ``unknown``.  On Pi 5
/ CM 5 this is HPD-IRQ driven and reliable, even when
``hdmi_force_hotplug=1`` is set in ``/boot/firmware/config.txt`` (the RP1
chip handles hot-plug natively and ignores the flag).

On older Pis (VC4 HDMI driver) the same file will frequently report
``connected`` regardless of physical state — use :mod:`i2c_edid` instead on
those boards.
"""
from __future__ import annotations

import glob
import logging
from pathlib import Path
from typing import Optional

from shared.board import HdmiPort

from .base import DisplayProbe, PortStatus

logger = logging.getLogger("agora.hardware.display.drm_sysfs")

_SYSFS_ROOT = Path("/sys/class/drm")


def _resolve_status_path(connector: str) -> Optional[Path]:
    """Find the sysfs ``status`` file for a DRM connector name.

    Matches ``/sys/class/drm/card*-{connector}/status``.  Returns ``None`` if
    no such file exists on this kernel (e.g. a Pi without DRM KMS).
    """
    pattern = str(_SYSFS_ROOT / f"card*-{connector}" / "status")
    matches = sorted(glob.glob(pattern))
    if not matches:
        return None
    return Path(matches[0])


def _read_status(path: Path) -> Optional[bool]:
    """Read a DRM connector ``status`` file, mapping text to tri-state bool."""
    try:
        value = path.read_text().strip().lower()
    except (FileNotFoundError, PermissionError, OSError) as e:
        logger.debug("DRM sysfs read failed for %s: %s", path, e)
        return None
    if value == "connected":
        return True
    if value == "disconnected":
        return False
    return None


class DrmSysfsDisplayProbe(DisplayProbe):
    """HDMI presence via ``/sys/class/drm/card*-{connector}/status``.

    Each :class:`HdmiPort` passed in must carry a ``drm_connector`` value
    (e.g. ``"HDMI-A-1"``).  Ports with no ``drm_connector`` or a missing
    sysfs file report ``connected=None``.
    """

    def __init__(self, ports: list[HdmiPort]):
        self._ports = list(ports)

    def probe_all(self) -> list[PortStatus]:
        out: list[PortStatus] = []
        for port in self._ports:
            connector = port.drm_connector
            if not connector:
                out.append(PortStatus(name=port.name, connected=None))
                continue
            path = _resolve_status_path(connector)
            if path is None:
                out.append(PortStatus(name=port.name, connected=None))
                continue
            out.append(PortStatus(name=port.name, connected=_read_status(path)))
        return out
