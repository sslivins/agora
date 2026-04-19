"""Null display probe for unknown/unsupported boards.

Returns ``connected=None`` for every port.  Used as the final fallback when
neither the DRM sysfs nor the I²C EDID strategies apply.
"""
from __future__ import annotations

from shared.board import HdmiPort

from .base import DisplayProbe, PortStatus


class NullDisplayProbe(DisplayProbe):
    """Always reports :class:`PortStatus` with ``connected=None``."""

    def __init__(self, ports: list[HdmiPort]):
        self._ports = list(ports)

    def probe_all(self) -> list[PortStatus]:
        return [PortStatus(name=p.name, connected=None) for p in self._ports]
