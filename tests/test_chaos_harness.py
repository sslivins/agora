"""Tests for the Phase 1 chaos harness.

Three layers:

* **Framework unit tests** — exercise :mod:`chaos.core` primitives in
  isolation (env builder, injection helpers, scenario runner).
* **Parametrized scenario sweep** — runs every entry in
  :data:`chaos.scenarios.SCENARIOS` against a fresh env and asserts each
  one passes. This is the meat of the suite: if any of the 17 chaos
  scenarios regresses, this lights up red.
* **CLI smoke tests** — invoke ``python -m chaos`` as a subprocess to
  cover the argparse + exit-code wiring.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from chaos import (
    ChaosEnv,
    ChaosError,
    ChaosPowerCut,
    SCENARIOS,
    Scenario,
    active_env,
    get_scenario,
    inject_after_nth_call,
    inject_first_call,
    list_scenarios,
    make_env,
    run_scenario,
    run_scenarios,
    summarize_results,
)
from chaos import core as chaos_core


# ---------------------------------------------------------------------------
# Framework: make_env / active_env
# ---------------------------------------------------------------------------


def test_make_env_creates_expected_layout(tmp_path):
    env = make_env(tmp_path)

    assert env.autoboot_path.exists()
    assert env.autoboot_mirror_path.exists()
    assert env.data_dir.exists()
    assert env.cmdline_path.exists()
    # slot-state.json and sentinel are NOT pre-created.
    assert not env.slot_state_path.exists()
    assert not env.sentinel_path.exists()

    # Both autoboot copies start byte-identical.
    assert env.autoboot_path.read_text() == env.autoboot_mirror_path.read_text()


def test_make_env_rejects_bad_slot(tmp_path):
    with pytest.raises(ChaosError):
        make_env(tmp_path, running_slot=99)
    with pytest.raises(ChaosError):
        make_env(tmp_path, default_slot=0)


def test_active_env_sets_and_restores_envvars(tmp_path, monkeypatch):
    monkeypatch.setenv("AGORA_AUTOBOOT_PATH", "/sentinel")
    monkeypatch.delenv("AGORA_SLOT_DATA_DIR", raising=False)

    env = make_env(tmp_path)
    with active_env(env):
        import os

        assert os.environ["AGORA_AUTOBOOT_PATH"] == str(env.autoboot_path)
        assert os.environ["AGORA_SLOT_DATA_DIR"] == str(env.data_dir)

    import os

    assert os.environ["AGORA_AUTOBOOT_PATH"] == "/sentinel"
    assert "AGORA_SLOT_DATA_DIR" not in os.environ


def test_set_cmdline_slot_updates_cmdline(tmp_path):
    env = make_env(tmp_path, running_slot=1)
    assert "root-A" in env.cmdline_path.read_text()

    chaos_core.set_cmdline_slot(env, 2)
    assert "root-B" in env.cmdline_path.read_text()


# ---------------------------------------------------------------------------
# Framework: injection helpers
# ---------------------------------------------------------------------------


class _FakeModule:
    """Tiny module-like stand-in for the injection tests."""

    counter = 0

    @classmethod
    def reset(cls):
        cls.counter = 0

    @classmethod
    def bump(cls):
        cls.counter += 1
        return cls.counter


def test_inject_after_nth_call_lets_first_n_through():
    _FakeModule.reset()
    with inject_after_nth_call(_FakeModule, "bump", 2, message="x"):
        assert _FakeModule.bump() == 1
        assert _FakeModule.bump() == 2
        with pytest.raises(ChaosPowerCut):
            _FakeModule.bump()


def test_inject_first_call_faults_immediately():
    _FakeModule.reset()
    with inject_first_call(_FakeModule, "bump", message="y"):
        with pytest.raises(ChaosPowerCut):
            _FakeModule.bump()
    # Patch was restored: real function works again.
    _FakeModule.reset()
    assert _FakeModule.bump() == 1


def test_inject_negative_n_is_programmer_error():
    with pytest.raises(ChaosError):
        inject_after_nth_call(_FakeModule, "bump", -1)


def test_inject_restores_attribute_on_exit():
    _FakeModule.reset()
    with inject_first_call(_FakeModule, "bump"):
        pass
    # Attribute is back to the real implementation: calls succeed again.
    assert _FakeModule.bump() == 1
    assert _FakeModule.bump() == 2


# ---------------------------------------------------------------------------
# Framework: run_scenario error handling
# ---------------------------------------------------------------------------


def _ok_scenario(env: ChaosEnv) -> None:
    return None


def _failing_scenario(env: ChaosEnv) -> None:
    assert False, "deliberate failure"


def _exploding_scenario(env: ChaosEnv) -> None:
    raise RuntimeError("kaboom")


def _leaks_chaos_powercut(env: ChaosEnv) -> None:
    raise ChaosPowerCut("leaked")


def test_run_scenario_passes_on_success(tmp_path):
    env = make_env(tmp_path)
    result = run_scenario(
        Scenario(name="ok", description="d", run=_ok_scenario), env
    )
    assert result.ok
    assert result.detail == ""
    assert result.error_type is None
    assert result.traceback == ""


def test_run_scenario_captures_assertion_failure(tmp_path):
    env = make_env(tmp_path)
    result = run_scenario(
        Scenario(name="fail", description="d", run=_failing_scenario), env
    )
    assert not result.ok
    assert "deliberate failure" in result.detail
    assert result.error_type == "AssertionError"
    assert "deliberate failure" in result.traceback


def test_run_scenario_captures_unexpected_exception(tmp_path):
    env = make_env(tmp_path)
    result = run_scenario(
        Scenario(name="boom", description="d", run=_exploding_scenario), env
    )
    assert not result.ok
    assert result.error_type == "RuntimeError"
    assert "kaboom" in result.detail


def test_run_scenario_captures_leaked_chaos_powercut(tmp_path):
    env = make_env(tmp_path)
    result = run_scenario(
        Scenario(name="leak", description="d", run=_leaks_chaos_powercut), env
    )
    assert not result.ok
    assert result.error_type == "ChaosPowerCut"
    assert "leaked" in result.detail


def test_summarize_results_reports_counts(tmp_path):
    env = make_env(tmp_path)
    results = [
        run_scenario(Scenario(name="a", description="", run=_ok_scenario), env),
        run_scenario(
            Scenario(name="b", description="", run=_failing_scenario),
            make_env(tmp_path / "b_root"),
        ),
    ]
    summary = summarize_results(results)
    assert summary["total"] == 2
    assert summary["passed"] == 1
    assert summary["failed"] == 1
    assert summary["ok"] is False
    assert {s["name"] for s in summary["scenarios"]} == {"a", "b"}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_meets_phase_1_minimum_scenario_count():
    """Phase 1 spec calls for ≥15 software-reboot injection points."""
    assert len(SCENARIOS) >= 15, (
        f"Phase 1 requires ≥15 chaos scenarios; got {len(SCENARIOS)}"
    )


def test_registry_names_are_unique():
    names = [s.name for s in SCENARIOS]
    assert len(set(names)) == len(names), "duplicate scenario names in SCENARIOS"


def test_list_scenarios_matches_registry():
    listed = list_scenarios()
    assert len(listed) == len(SCENARIOS)
    assert {s["name"] for s in listed} == {s.name for s in SCENARIOS}


def test_get_scenario_round_trips():
    s = SCENARIOS[0]
    assert get_scenario(s.name) is s


def test_get_scenario_unknown_name_raises():
    with pytest.raises(KeyError):
        get_scenario("no-such-scenario")


# ---------------------------------------------------------------------------
# The main event: every scenario passes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario",
    SCENARIOS,
    ids=lambda s: s.name,
)
def test_chaos_scenario_passes(scenario, tmp_path):
    env = make_env(tmp_path)
    result = run_scenario(scenario, env)
    if not result.ok:
        pytest.fail(
            f"{scenario.name} failed: {result.detail}\n\n{result.traceback}"
        )


def test_run_scenarios_walks_every_scenario(tmp_path):
    counter = {"n": 0}

    def factory():
        counter["n"] += 1
        return make_env(tmp_path / f"run_{counter['n']}")

    results = run_scenarios(SCENARIOS, factory)
    assert len(results) == len(SCENARIOS)
    assert all(r.ok for r in results), [
        (r.name, r.detail) for r in results if not r.ok
    ]


# ---------------------------------------------------------------------------
# CLI smoke
# ---------------------------------------------------------------------------


def _run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "chaos", *args],
        capture_output=True,
        text=True,
    )


def test_cli_list_prints_every_scenario():
    p = _run_cli("list")
    assert p.returncode == 0, p.stderr
    for s in SCENARIOS:
        assert s.name in p.stdout


def test_cli_list_json_is_machine_readable():
    p = _run_cli("list", "--json")
    assert p.returncode == 0, p.stderr
    payload = json.loads(p.stdout)
    assert len(payload["scenarios"]) == len(SCENARIOS)


def test_cli_run_unknown_scenario_exits_2():
    p = _run_cli("run", "does-not-exist")
    assert p.returncode == 2
    assert "unknown scenario" in p.stderr.lower()


def test_cli_run_single_scenario_succeeds():
    target = SCENARIOS[0].name
    p = _run_cli("run", target)
    assert p.returncode == 0, p.stderr + p.stdout
    assert "PASS" in p.stdout
    assert target in p.stdout


def test_cli_run_all_json_exits_0_when_all_pass():
    p = _run_cli("run-all", "--json")
    assert p.returncode == 0, p.stderr + p.stdout
    payload = json.loads(p.stdout)
    assert payload["ok"] is True
    assert payload["passed"] == len(SCENARIOS)
