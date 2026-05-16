"""CLI entrypoint: ``python -m slot_confirm``.

Modes (these may be combined):

* (no flags)       — run the 4-check gate, print JSON to stdout, exit 0.
* ``--check``       — exit 1 if any check failed; useful for
                       systemd ``ExecStartPost=`` style invocations
                       that want to react to gate failure without
                       performing an action.
* ``--auto``        — after running the gate, *act on* the recommended
                       ``next_action`` by invoking the corresponding
                       :mod:`slot_mgr` verb:

                         next_action="promote"  → slot_mgr.promote_slot(running_slot)
                         next_action="strike"   → slot_mgr.record_tryboot_strike(running_slot)
                         next_action="deferred" → no-op (services not yet aged)
                         next_action="skipped"  → no-op
                         next_action="error"    → no action, exit 2

                       The emitted JSON adds ``action_taken`` and any
                       ``action_error``.

JSON shape (without ``--auto``)::

    {
      "ok": true,
      "next_action": "promote",
      "running_slot": 2,
      "tentative": true,
      "error": "",
      "checks": [
        {"name": "agora_services_active", "ok": true, "detail": "…", "measurement": {…}},
        ...
      ]
    }

Exit codes:

* 0  — checks ran (default), or checks ran + acted (--auto), or
       ``--check`` and ``ok=True``.
* 1  — ``--check`` and ``ok=False``.
* 2  — couldn't even run the gate (slot_mgr import failed, or
       ``next_action=="error"`` under ``--auto``).
* 75 — ``--auto`` and ``next_action=="deferred"``. The agora-*
       services aren't yet aged (≥5 min Active) but everything
       else is healthy; the strike counter MUST NOT advance for
       this transient condition. Exit 75 (``EX_TEMPFAIL``) is a
       signal to systemd's ``Restart=on-failure`` to retry the
       unit after ``RestartSec``. See bug #209.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from typing import Any, Sequence

from slot_confirm import __version__
from slot_confirm.core import ConfirmStatus, slot_confirm


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m slot_confirm",
        description=(
            "Run the slot-confirm 4-check gate against the current boot."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"slot_confirm {__version__}",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if any check failed",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help=(
            "act on the recommended next_action: promote / strike / skip. "
            "Uses slot_mgr.promote_slot or slot_mgr.record_tryboot_strike."
        ),
    )
    return parser


def _act(status: ConfirmStatus) -> tuple[str, str]:
    """Carry out ``status.next_action`` via slot_mgr.

    Returns a ``(action_taken, action_error)`` tuple. ``action_taken``
    is one of ``"promote"``, ``"strike"``, ``"deferred"``,
    ``"skipped"``, or ``"none"`` (when ``next_action == "error"``).
    ``action_error`` is a free-form message on failure, ``""``
    otherwise.
    """
    if status.next_action == "skipped":
        return "skipped", ""

    if status.next_action == "deferred":
        # agora-* services are up but haven't met the ≥5min Active
        # bar yet (bug #209). No on-device side effect — the CLI
        # caller (systemd unit) is expected to retry via
        # Restart=on-failure on the EX_TEMPFAIL exit code emitted by
        # main().
        return "deferred", ""

    if status.next_action == "error":
        return "none", status.error or "slot_state() failed"

    if status.running_slot is None:
        return "none", "running_slot is unknown; cannot act"

    try:
        from slot_mgr import promote_slot, record_tryboot_strike
    except ImportError as exc:
        return "none", f"slot_mgr import failed: {exc}"

    try:
        if status.next_action == "promote":
            promote_slot(status.running_slot)
            return "promote", ""
        if status.next_action == "strike":
            record_tryboot_strike(
                status.running_slot,
                reason="slot-confirm checks failed",
            )
            return "strike", ""
    except Exception as exc:  # noqa: BLE001
        return "none", f"{status.next_action} action raised: {exc}"

    return "none", f"unrecognised next_action {status.next_action!r}"


def _to_payload(status: ConfirmStatus) -> dict[str, Any]:
    """Convert :class:`ConfirmStatus` to a JSON-friendly dict."""
    payload = asdict(status)
    # asdict turns the checks tuple into a list of dicts; re-wrap
    # each measurement so JSON serialises cleanly even if it was a
    # custom Mapping.
    checks = []
    for c in payload.get("checks", []):
        measurement = c.get("measurement") or {}
        checks.append(
            {
                "name": c["name"],
                "ok": c["ok"],
                "detail": c["detail"],
                "measurement": dict(measurement),
            }
        )
    payload["checks"] = checks
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    status = slot_confirm()
    payload = _to_payload(status)

    if args.auto:
        action_taken, action_error = _act(status)
        payload["action_taken"] = action_taken
        payload["action_error"] = action_error

    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")

    if args.auto and status.next_action == "error":
        return 2
    if args.auto and status.next_action == "deferred":
        # EX_TEMPFAIL — signals systemd's Restart=on-failure to retry
        # this unit after RestartSec. See bug #209 and the deferred
        # branch in slot_confirm.core.slot_confirm().
        return 75
    if args.check and not status.ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
