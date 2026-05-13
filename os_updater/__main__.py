"""Allow ``python -m os_updater`` to run the CLI dispatcher.

The dispatcher (see :mod:`os_updater.cli`) routes the ``stage`` and
``daemon`` subcommands. With no subcommand, it falls through to the
daemon entry point so the existing systemd unit (which invokes
``python3 -m os_updater`` with no args) keeps working unchanged.
"""

from __future__ import annotations

from os_updater.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
