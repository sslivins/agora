"""Device identity helpers shared across services."""


def get_device_serial() -> str:
    """Read the Pi CPU serial number."""
    try:
        with open("/sys/firmware/devicetree/base/serial-number") as f:
            return f.read().strip().strip("\x00")
    except OSError:
        pass
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("Serial"):
                return line.split(":")[1].strip()
    except OSError:
        pass
    return "unknown"


def get_device_serial_suffix(length: int = 4) -> str:
    """Return last N hex chars of the Pi serial number (uppercase)."""
    serial = get_device_serial()
    if serial == "unknown":
        return "0000"
    return serial[-length:].upper()
