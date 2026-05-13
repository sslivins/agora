"""Allow ``python -m os_updater`` to run the daemon."""

from __future__ import annotations

from os_updater.main import main


if __name__ == "__main__":
    raise SystemExit(main())
