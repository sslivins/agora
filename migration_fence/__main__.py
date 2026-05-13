"""``python -m migration_fence`` - status / gate for service ExecStartPre.

Prints the current :class:`FenceStatus` as JSON on stdout and exits:

* 0 if migration is allowed (sentinel present, slot matches running)
* 1 if migration is denied for any operational reason

Wire from a systemd unit's pre-migration step::

    ExecStartPre=/usr/bin/python3 -m migration_fence --check

For human inspection on the device::

    python3 -m migration_fence

(no ``--check`` always exits 0 - useful when you just want the JSON).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from typing import Sequence

from migration_fence.core import check_migration_fence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m migration_fence",
        description=(
            "Print the forward-migration fence status as JSON. "
            "With --check, exit non-zero when migration is not allowed."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 when migration is not allowed (for use in ExecStartPre).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    status = check_migration_fence()
    payload = dataclasses.asdict(status)
    payload["measurement"] = dict(payload.get("measurement") or {})
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    if args.check and not status.allowed:
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by integration paths
    raise SystemExit(main())
