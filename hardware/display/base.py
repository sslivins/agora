"""Abstract base class for HDMI display presence probes."""
from __future__ import annotations

from abc import ABC, abstractmethod

# Re-export the canonical wire-level PortStatus model so hardware probes and
# CurrentState share a single type (avoids pydantic/dataclass coercion
# errors and keeps the wire schema in one place).
from shared.models import PortStatus

__all__ = ["DisplayProbe", "PortStatus"]


class DisplayProbe(ABC):
    """Strategy for detecting whether HDMI displays are connected.

    ``PortStatus.connected`` is tri-state:

    * ``True``  — display detected
    * ``False`` — confirmed nothing attached
    * ``None``  — could not determine (probe unavailable, transient error)
    """

    @abstractmethod
    def probe_all(self) -> list[PortStatus]:
        """Probe every HDMI port on the board.

        Returns a list of :class:`PortStatus` in board port order (port 0 first).
        The list length equals the board's HDMI port count; it is never empty
        on a board with ports declared.  Individual ports may carry
        ``connected=None`` if that port can't be determined.
        """
