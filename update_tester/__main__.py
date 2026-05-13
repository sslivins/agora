"""CLI entry point for the update-tester package.

Usage::

    python -m update_tester [--run-id ID] [--output-dir DIR] [--no-output]
                            [--deadline-seconds N] [--version]

Runs the 4-test synthetic-load battery, prints the result as indented
JSON to stdout, and (unless ``--no-output``) writes the same JSON to
``<output-dir>/<run-id>.json`` atomically.

Exit codes:

* ``0`` — every test passed and the deadline was not hit.
* ``1`` — at least one test failed, or the deadline was hit, or the
  artifact write failed.
* ``2`` — a check raised :class:`UpdateTesterError` (programmer
  error: invalid argument, unsupported combination).
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from update_tester import __version__
from update_tester.core import (
    DEFAULT_DEADLINE_SECONDS,
    DEFAULT_OUTPUT_DIR,
    UpdateTesterError,
    battery_to_dict,
    run_test_battery,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="update_tester",
        description=(
            "Run the agora update-tester synthetic-load test battery "
            "(render canary, WPS end-to-end, memory/CPU stress, "
            "/data integrity) on the currently running slot."
        ),
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="override the generated run id (default: fresh uuid4 hex)",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "directory to write <run-id>.json into "
            f"(default: {DEFAULT_OUTPUT_DIR})"
        ),
    )
    p.add_argument(
        "--no-output",
        action="store_true",
        help="skip writing the JSON artifact (still prints to stdout)",
    )
    p.add_argument(
        "--deadline-seconds",
        type=int,
        default=DEFAULT_DEADLINE_SECONDS,
        help=(
            "overall battery deadline in seconds "
            f"(default: {DEFAULT_DEADLINE_SECONDS})"
        ),
    )
    p.add_argument(
        "--version",
        action="version",
        version=f"update_tester {__version__}",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        battery = run_test_battery(
            run_id=args.run_id,
            output_dir=None if args.no_output else args.output_dir,
            write_output=not args.no_output,
            deadline_seconds=args.deadline_seconds,
        )
    except UpdateTesterError as exc:
        print(f"update_tester: invalid arguments: {exc}", file=sys.stderr)
        return 2

    payload = battery_to_dict(battery)
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0 if battery.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
