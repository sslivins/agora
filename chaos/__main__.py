"""Command-line entry point for the chaos harness.

Usage
-----

::

    python -m chaos list
    python -m chaos run <scenario-name>
    python -m chaos run-all [--json]

Exit codes
----------

* ``0`` — all scenarios passed.
* ``1`` — at least one scenario failed.
* ``2`` — programmer / usage error (bad arguments, unknown scenario name,
  harness raised :class:`chaos.ChaosError`).
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Sequence

from chaos import (
    ChaosError,
    SCENARIOS,
    get_scenario,
    list_scenarios,
    make_env,
    run_scenario,
    run_scenarios,
    summarize_results,
)


def _build_env_factory():
    """Return a callable that mints a fresh :class:`ChaosEnv` in a new tempdir."""

    def factory():
        root = Path(tempfile.mkdtemp(prefix="chaos-"))
        return make_env(root)

    return factory


def _cmd_list(args: argparse.Namespace) -> int:
    scenarios = list_scenarios()
    if args.json:
        print(json.dumps({"scenarios": scenarios}, indent=2))
        return 0
    for s in scenarios:
        print(f"{s['name']}")
        print(f"    {s['description']}")
    print(f"\n{len(scenarios)} scenario(s)")
    return 0


def _format_result(result, *, verbose: bool) -> str:
    icon = "PASS" if result.ok else "FAIL"
    line = f"[{icon}] {result.name}"
    if result.detail:
        line += f"  ({result.detail})"
    if verbose and result.traceback:
        line += "\n" + result.traceback
    return line


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        scenario = get_scenario(args.name)
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    env = _build_env_factory()()
    result = run_scenario(scenario, env)
    if args.json:
        print(json.dumps(summarize_results([result]), indent=2))
    else:
        print(_format_result(result, verbose=True))
    return 0 if result.ok else 1


def _cmd_run_all(args: argparse.Namespace) -> int:
    results = run_scenarios(SCENARIOS, _build_env_factory())
    if args.json:
        print(json.dumps(summarize_results(results), indent=2))
    else:
        for r in results:
            print(_format_result(r, verbose=args.verbose))
        passed = sum(1 for r in results if r.ok)
        total = len(results)
        print(f"\n{passed}/{total} scenarios passed")
    return 0 if all(r.ok for r in results) else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m chaos",
        description="Phase 1 software-reboot chaos harness for slot_mgr.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    list_p = sub.add_parser("list", help="list every registered scenario")
    list_p.add_argument("--json", action="store_true", help="emit JSON output")
    list_p.set_defaults(func=_cmd_list)

    run_p = sub.add_parser("run", help="run one scenario by name")
    run_p.add_argument("name", help="scenario name (see `list`)")
    run_p.add_argument("--json", action="store_true", help="emit JSON output")
    run_p.set_defaults(func=_cmd_run)

    all_p = sub.add_parser("run-all", help="run every registered scenario")
    all_p.add_argument("--json", action="store_true", help="emit JSON output")
    all_p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="include tracebacks for failures",
    )
    all_p.set_defaults(func=_cmd_run_all)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ChaosError as exc:
        print(f"chaos-harness error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
