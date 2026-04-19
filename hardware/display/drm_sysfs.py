"""DRM sysfs based HDMI probe (primary for Pi 5 / CM 5).

Reads ``/sys/class/drm/card*-{drm_connector}/status`` for each HDMI port and
cross-checks the sibling ``edid`` file.

The ``status`` file contains ``connected``, ``disconnected`` or ``unknown``.
On current Pi 5 firmware (and on VC4 with ``hdmi_force_hotplug=1``) the file
can read ``connected`` even when no display is physically attached.  A real
display always returns EDID data over DDC, so we require a non-empty EDID
before trusting the ``connected`` verdict.

Empirical behaviour on Pi 5 / RP1 (verified 2026-04):
  No display attached:  status=connected, edid=0 bytes   → connected=False
  Display attached:     status=connected, edid=256 bytes → connected=True
  status=disconnected:  always trusted → connected=False

Note: the DRM ``edid`` sysfs attribute is a dynamic binary attribute declared
with ``size = 0`` in the kernel, so ``stat(2)`` on it **always** reports
``st_size = 0`` regardless of whether a monitor is attached.  We therefore
read a small prefix of the file to determine whether EDID data is actually
present, rather than trusting the inode size.
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


def _resolve_connector_dir(connector: str) -> Optional[Path]:
    """Find the sysfs directory for a DRM connector name.

    Matches ``/sys/class/drm/card*-{connector}/``.  Returns ``None`` if no
    such directory exists on this kernel (e.g. a Pi without DRM KMS).
    """
    pattern = str(_SYSFS_ROOT / f"card*-{connector}")
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


def _edid_has_data(connector_dir: Path) -> bool:
    """Return True if the connector's EDID blob contains real data.

    We read a small prefix of the ``edid`` file (rather than calling
    ``stat``) because the sysfs binary attribute reports ``st_size=0``
    unconditionally.  Only an actual ``read`` yields the 128+ bytes
    published by a real monitor.  Any non-empty read is treated as
    "EDID present"; we don't require a valid header because partial
    reads from flaky HDMI links still indicate a responsive sink.
    """
    try:
        with (connector_dir / "edid").open("rb") as f:
            return bool(f.read(8))
    except (FileNotFoundError, PermissionError, OSError) as e:
        logger.debug("DRM sysfs EDID read failed for %s: %s", connector_dir, e)
        return False


class DrmSysfsDisplayProbe(DisplayProbe):
    """HDMI presence via ``/sys/class/drm/card*-{connector}/{status,edid}``.

    Each :class:`HdmiPort` passed in must carry a ``drm_connector`` value
    (e.g. ``"HDMI-A-1"``).  Ports with no ``drm_connector`` or a missing
    sysfs directory report ``connected=None``.

    The probe cross-checks the sysfs ``status`` file against the ``edid``
    file's size: if ``status=connected`` but EDID is empty (0 bytes), the
    port is reported as ``connected=False`` — the sysfs ``status`` file is
    known to lie under various firmware/config conditions, whereas EDID
    only populates when a real monitor answers on DDC.
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
            connector_dir = _resolve_connector_dir(connector)
            if connector_dir is None:
                out.append(PortStatus(name=port.name, connected=None))
                continue
            status = _read_status(connector_dir / "status")
            if status is True and not _edid_has_data(connector_dir):
                logger.debug(
                    "%s: sysfs status=connected but EDID empty — treating as disconnected",
                    connector,
                )
                status = False
            out.append(PortStatus(name=port.name, connected=status))
        return out

