"""argparse CLI for ``agora-slot-mgr``.

Verbs implemented in this PR:

* ``status``           - report current slot + tentative + pin + strikes
* ``trigger-tryboot N`` - rewrite [tryboot] in autoboot.txt and reboot
* ``promote N``         - rewrite [all] in autoboot.txt, reset strikes
* ``unpin``             - clear the 3-strikes pin
* ``record-strike N``   - bump the strike counter (used by slot-confirm
  and watchdog services to mark a failed tentative boot)

Slot operands accept both numeric form (``1`` / ``2``) and letter form
(``A`` / ``B``) for friendliness on the command line.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from typing import Sequence

from slot_mgr import __version__
from slot_mgr import core


def _parse_slot(raw: str) -> int:
    raw = raw.strip()
    if raw in ("1", "A", "a"):
        return core.SLOT_A
    if raw in ("2", "B", "b"):
        return core.SLOT_B
    raise argparse.ArgumentTypeError(
        f"slot must be one of 1, 2, A, B (got {raw!r})"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agora-slot-mgr",
        description="Manage A/B boot slots on a Pi 5 agora device.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"agora-slot-mgr {__version__}",
    )
    sub = parser.add_subparsers(dest="verb", required=True)

    p_status = sub.add_parser("status", help="print current slot state")
    p_status.add_argument(
        "--json",
        action="store_true",
        help="emit machine-parsable JSON instead of human text",
    )

    p_tryboot = sub.add_parser(
        "trigger-tryboot",
        help="stage a one-shot tryboot of the target slot and reboot",
    )
    p_tryboot.add_argument("slot", type=_parse_slot, help="target slot (1, 2, A, B)")
    p_tryboot.add_argument(
        "--no-reboot",
        action="store_true",
        help="write autoboot.txt + persist state but do not actually reboot",
    )

    p_promote = sub.add_parser(
        "promote",
        help="make the target slot the permanent default (rewrites [all])",
    )
    p_promote.add_argument("slot", type=_parse_slot, help="target slot (1, 2, A, B)")

    sub.add_parser("unpin", help="clear the three-strikes pin")

    p_strike = sub.add_parser(
        "record-strike",
        help="record a failed tryboot for a slot (used by slot-confirm / watchdog)",
    )
    p_strike.add_argument("slot", type=_parse_slot, help="slot whose tryboot failed")
    p_strike.add_argument(
        "--reason",
        default="tryboot failed",
        help="free-form annotation written to slot-state.json on pin",
    )

    return parser


def _status_to_dict(status: core.SlotStatus) -> dict:
    d = asdict(status)
    # Make the strikes dict JSON-friendly (str keys).
    d["strikes"] = {str(k): int(v) for k, v in status.strikes.items()}
    return d


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.verb == "status":
            status = core.slot_state()
            if args.json:
                print(json.dumps(_status_to_dict(status), indent=2, sort_keys=True))
            else:
                print(core.format_status_human(status))
            return 0

        if args.verb == "trigger-tryboot":
            state = core.trigger_tryboot(args.slot, reboot=not args.no_reboot)
            print(
                f"staged tryboot of slot {args.slot}; "
                f"last_tryboot_at={state.last_tryboot_at}"
            )
            if args.no_reboot:
                print("--no-reboot set; not invoking `reboot '0 tryboot'`")
            return 0

        if args.verb == "promote":
            state = core.promote_slot(args.slot)
            print(
                f"promoted slot {args.slot} to default; "
                f"strikes={state.strikes}; "
                f"last_success_at={state.last_success_at}"
            )
            return 0

        if args.verb == "unpin":
            state = core.unpin()
            print(
                f"unpinned; pinned={state.pinned}; strikes={state.strikes}"
            )
            return 0

        if args.verb == "record-strike":
            state = core.record_tryboot_strike(args.slot, reason=args.reason)
            print(
                f"recorded strike for slot {args.slot}; "
                f"strikes={state.strikes}; pinned={state.pinned}"
            )
            return 0

        # argparse with required=True keeps us from reaching here, but keep the
        # branch so mypy is happy.
        parser.error(f"unknown verb {args.verb!r}")
        return 2

    except core.PinnedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except core.InvalidSlotError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except core.SlotMgrError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
