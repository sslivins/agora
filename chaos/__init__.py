"""Chaos harness for ``slot_mgr`` — Phase 1 software-reboot subset.

The harness simulates power-loss / crashes at each disk-write seam in
``slot_mgr.core`` (trigger_tryboot, promote_slot, record_tryboot_strike,
unpin) and verifies that the on-disk state stays recoverable. See
:mod:`chaos.core` for the framework primitives and :mod:`chaos.scenarios`
for the concrete injection points.

Public surface:

* :class:`ChaosEnv`, :func:`make_env`, :func:`active_env`,
  :func:`set_cmdline_slot` — build & activate an isolated filesystem env.
* :class:`ChaosError`, :class:`ChaosPowerCut` — harness exception types.
* :func:`inject_after_nth_call`, :func:`inject_first_call` — patch a
  module-level callable to fault after N successful invocations.
* :class:`Scenario`, :class:`ScenarioResult` — value types.
* :func:`run_scenario`, :func:`run_scenarios`, :func:`summarize_results`
  — execution + reporting.
* :data:`SCENARIOS`, :func:`list_scenarios`, :func:`get_scenario` — the
  registered scenario catalogue.
"""

from chaos.core import (
    ChaosEnv,
    ChaosError,
    ChaosPowerCut,
    Scenario,
    ScenarioResult,
    active_env,
    inject_after_nth_call,
    inject_first_call,
    make_env,
    run_scenario,
    run_scenarios,
    set_cmdline_slot,
    summarize_results,
)
from chaos.scenarios import SCENARIOS, get_scenario, list_scenarios

__version__ = "0.1.0"

__all__ = [
    "ChaosEnv",
    "ChaosError",
    "ChaosPowerCut",
    "Scenario",
    "ScenarioResult",
    "SCENARIOS",
    "active_env",
    "get_scenario",
    "inject_after_nth_call",
    "inject_first_call",
    "list_scenarios",
    "make_env",
    "run_scenario",
    "run_scenarios",
    "set_cmdline_slot",
    "summarize_results",
]
