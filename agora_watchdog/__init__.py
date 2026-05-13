"""Hardware-watchdog pinger for the Pi 5 RP1 watchdog (/dev/watchdog0).

A tiny single-purpose daemon that opens the watchdog device, sets its
timeout, and writes a keepalive byte at a fixed cadence. Lives in its
own systemd unit (``agora-watchdog.service``) so that its lifecycle is
independent of the other agora-* services — if any of those crash,
systemd will restart them; if the kernel itself hangs, this daemon
stops petting the watchdog and the RP1 fires a hard reset.

Per Decision #16 of the multi-replica OTA plan:
- device:   /dev/watchdog0  (RP1-backed on the Pi 5)
- timeout:  12s             (RP1 hardware max is ~15s)
- cadence:  5s

All three values are overridable via environment variables so the unit
tests can redirect the daemon at a tempfile and inject a fake clock.

Public surface:

    AGORA_WATCHDOG_DEVICE          path to the watchdog character device
    AGORA_WATCHDOG_TIMEOUT_S       integer seconds, passed to WDIOC_SETTIMEOUT
    AGORA_WATCHDOG_PING_INTERVAL_S float seconds between writes

The CLI entrypoint is ``python3 -m agora_watchdog``; the systemd unit
execs that with the standard ``PYTHONPATH=/opt/agora/src`` environment.
"""

from agora_watchdog.pinger import (
    DEFAULT_DEVICE,
    DEFAULT_PING_INTERVAL_S,
    DEFAULT_TIMEOUT_S,
    MAGIC_CLOSE,
    Pinger,
    WatchdogError,
    main,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_DEVICE",
    "DEFAULT_PING_INTERVAL_S",
    "DEFAULT_TIMEOUT_S",
    "MAGIC_CLOSE",
    "Pinger",
    "WatchdogError",
    "main",
    "__version__",
]
