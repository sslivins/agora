"""Run the hardware-watchdog pinger.

Usage: ``python3 -m agora_watchdog``

Exits 0 on clean shutdown (SIGTERM / SIGINT), non-zero on errors. See
``agora_watchdog.pinger.main`` for the behavior.
"""
from agora_watchdog.pinger import main

if __name__ == "__main__":
    raise SystemExit(main())
