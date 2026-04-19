"""I²C EDID probe (primary for Pi Zero 2 W / Pi 3 / Pi 4).

Opens the per-port HDMI DDC bus and attempts to talk to the EDID EEPROM at
address ``0x50``.  A successful 1-byte read means a display is attached
(the EEPROM ACKed).  ``EIO`` (errno 5) means no device is answering
— interpreted as disconnected.

Requires the ``i2c-dev`` kernel module to be loaded so ``/dev/i2c-N``
exists.  :class:`I2cEdidDisplayProbe` attempts ``modprobe i2c-dev`` once at
construction time; failure (e.g. module missing, not running as root) is
logged at debug level and the probe will return ``None`` for any ports
whose bus device is absent.

This matches the behaviour of the legacy ``PlayerService._is_display_
connected`` it replaces, generalised to multiple ports.
"""
from __future__ import annotations

import logging
import os
import subprocess
from typing import Optional

from shared.board import HdmiPort

from .base import DisplayProbe, PortStatus

try:  # pragma: no cover - Windows dev environments have no fcntl
    import fcntl as _fcntl
except ImportError:
    _fcntl = None  # type: ignore[assignment]

logger = logging.getLogger("agora.hardware.display.i2c_edid")

_I2C_SLAVE = 0x0703
_EDID_ADDR = 0x50


def _try_load_i2c_dev() -> None:
    """Best-effort ``modprobe i2c-dev``.

    On provisioned Agora images this module is loaded via
    ``/etc/modules-load.d``; this call is a safety net for fresh images /
    dev environments.  Non-root or module-missing failures are silently
    ignored.
    """
    try:
        subprocess.run(
            ["modprobe", "i2c-dev"],
            check=False,
            timeout=3,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.SubprocessError, OSError) as e:
        logger.debug("modprobe i2c-dev failed: %s", e)


def _probe_bus(i2c_bus: str) -> Optional[bool]:
    """Probe a single HDMI DDC bus for an EDID EEPROM at 0x50.

    Returns ``True`` on ACK+read, ``False`` on EIO NAK, ``None`` if the bus
    device cannot even be opened (i.e. i2c-dev not loaded or bus absent),
    or if this platform has no ``fcntl`` (e.g. running on Windows CI for
    unit tests).
    """
    if _fcntl is None:
        return None
    try:
        fd = os.open(i2c_bus, os.O_RDWR)
    except OSError:
        return None
    try:
        try:
            _fcntl.ioctl(fd, _I2C_SLAVE, _EDID_ADDR)
            os.read(fd, 1)
            return True
        except OSError:
            return False
    finally:
        os.close(fd)


class I2cEdidDisplayProbe(DisplayProbe):
    """HDMI presence via EDID EEPROM probe over I²C DDC."""

    def __init__(self, ports: list[HdmiPort], *, auto_load_module: bool = True):
        self._ports = list(ports)
        if auto_load_module:
            _try_load_i2c_dev()

    def probe_all(self) -> list[PortStatus]:
        return [
            PortStatus(name=p.name, connected=_probe_bus(p.i2c_bus))
            for p in self._ports
        ]
