"""Per-board HDMI display presence detection.

Selects the most reliable probe strategy for the current board:

* **Pi 5 / CM5** — ``/sys/class/drm/card*-HDMI-A-N/status`` (HPD IRQ driven,
  reliable even with ``hdmi_force_hotplug=1`` set).
* **Pi Zero 2 W / Pi 3 / Pi 4** — I²C EDID read at ``0x50`` on the HDMI DDC
  bus.  Sysfs ``status`` is unreliable on the older VC4 HDMI driver when
  ``hdmi_force_hotplug=1`` is set (it stays permanently ``connected``).
* **Unknown boards** — ``NullProbe`` returns ``None`` for every port.

Probes report **all** HDMI ports on the board so callers can expose richer
UI later (e.g. "nothing on HDMI-0 but HDMI-1 is plugged in").  The primary
port is always index 0 in the returned list.
"""
from __future__ import annotations

from .base import DisplayProbe, PortStatus
from .factory import build_probe, get_display_probe, reset_cached_probe

__all__ = [
    "DisplayProbe",
    "PortStatus",
    "build_probe",
    "get_display_probe",
    "reset_cached_probe",
]
