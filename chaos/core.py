"""Core chaos-harness framework: env builder, injection helpers, scenario runner.

The chaos harness simulates power-loss / crashes during ``slot_mgr`` state
transitions and verifies that recovery (a notional "next boot") leaves the
device in a sensible, recoverable state.

Implementation strategy
-----------------------

Atomic-write *is* atomic in Python: ``tempfile.mkstemp`` + ``os.fdopen`` +
``os.chmod`` + ``os.replace`` either fully commits or rolls back with no
partial state on disk. The software-reboot subset of the chaos harness
therefore models "power loss" as faulting *between* successive disk
operations (not mid-flight inside a single atomic write); each scenario
chooses a seam — e.g. "after primary autoboot.txt written, before mirror
written" — and raises :class:`ChaosPowerCut` there.

Faulting "mid-flight" inside an atomic write is observably indistinguishable
from faulting before the write started (the tempfile cleanup is part of the
atomic-write contract). The harness exploits this and only models the
between-step seams. True mid-flight power cuts (where the kernel itself dies
mid-write, leaving a half-written FAT32 sector on a boot partition, say)
require a hardware power-cut rig and are covered by the smart-plug subset
deferred to ``p1-smart-plug-pick``.

:class:`ChaosPowerCut` subclasses :class:`BaseException` rather than
:class:`Exception` so production code's ``except Exception`` clauses don't
accidentally swallow the simulated fault and prevent the scenario from
exercising recovery.
"""

from __future__ import annotations

import contextlib
import os
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional, Sequence
from unittest import mock

from slot_mgr.autoboot import SLOT_TO_BOOT_PARTITION

#: Re-export the slot→boot-partition mapping from :mod:`slot_mgr.autoboot` so
#: scenarios that build autoboot fixtures stay consistent with the production
#: layout (slot 1 → partition 1 (boot-A), slot 2 → partition 3 (boot-B)).
SLOT_TO_BOOT_PART = SLOT_TO_BOOT_PARTITION


class ChaosError(RuntimeError):
    """Programmer error in the harness itself (bad scenario, missing dep)."""


class ChaosPowerCut(BaseException):
    """Simulated power-loss exception.

    Subclasses :class:`BaseException` so ``except Exception:`` clauses in
    production code don't swallow the fault before the scenario can test
    recovery. Scenarios that *do* want to catch the fault (to model a
    successful reboot after the crash) catch this class explicitly.
    """


# ---------------------------------------------------------------------------
# Environment builder
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChaosEnv:
    """Isolated filesystem environment for one scenario.

    Mirrors the on-device layout under a temporary root so scenarios can
    drive ``slot_mgr`` without touching ``/boot/firmware`` or ``/data``.

    Attributes
    ----------
    root:
        Absolute path of the temporary root directory.
    autoboot_path:
        ``<root>/boot/firmware/autoboot.txt`` — primary autoboot file.
    autoboot_mirror_path:
        ``<root>/boot/firmware-b/autoboot.txt`` — mirror.
    data_dir:
        ``<root>/data/agora`` — equivalent of ``/data/agora``.
    slot_state_path:
        ``<root>/data/agora/slot-state.json``.
    sentinel_path:
        ``<root>/data/agora/migration-allowed`` (forward-migration fence).
    cmdline_path:
        ``<root>/proc/cmdline`` — writable fake; scenarios mutate it to
        simulate a different boot.
    """

    root: Path
    autoboot_path: Path
    autoboot_mirror_path: Path
    data_dir: Path
    slot_state_path: Path
    sentinel_path: Path
    cmdline_path: Path


def _autoboot_contents(default_slot: int) -> str:
    """Return a valid autoboot.txt body with the given ``[all]`` slot."""
    default_bp = SLOT_TO_BOOT_PART[default_slot]
    tryboot_bp = SLOT_TO_BOOT_PART[2 if default_slot == 1 else 1]
    return (
        "[all]\n"
        f"boot_partition={default_bp}\n"
        "\n"
        "[tryboot]\n"
        f"boot_partition={tryboot_bp}\n"
    )


def _cmdline_contents(running_slot: int) -> str:
    """Return a valid /proc/cmdline body for the given running slot."""
    label = "A" if running_slot == 1 else "B"
    return f"root=PARTLABEL=root-{label} rootfstype=ext4 rootwait\n"


def make_env(
    root: Path,
    *,
    running_slot: int = 1,
    default_slot: int = 1,
) -> ChaosEnv:
    """Build a fresh isolated filesystem under ``root``.

    Parameters
    ----------
    root:
        Pre-existing empty directory the harness will populate.
    running_slot:
        Which slot the simulated kernel cmdline reports (default 1).
    default_slot:
        Which slot the simulated autoboot.txt ``[all]`` block points at
        (default 1; same as ``running_slot`` ⇒ ``tentative=False``).
    """
    if running_slot not in SLOT_TO_BOOT_PART:
        raise ChaosError(f"running_slot must be 1 or 2, got {running_slot!r}")
    if default_slot not in SLOT_TO_BOOT_PART:
        raise ChaosError(f"default_slot must be 1 or 2, got {default_slot!r}")

    firmware = root / "boot" / "firmware"
    firmware_b = root / "boot" / "firmware-b"
    data_agora = root / "data" / "agora"
    proc = root / "proc"

    for d in (firmware, firmware_b, data_agora, proc):
        d.mkdir(parents=True, exist_ok=True)

    autoboot_path = firmware / "autoboot.txt"
    mirror_path = firmware_b / "autoboot.txt"
    cmdline_path = proc / "cmdline"

    autoboot_text = _autoboot_contents(default_slot)
    autoboot_path.write_text(autoboot_text)
    mirror_path.write_text(autoboot_text)
    cmdline_path.write_text(_cmdline_contents(running_slot))

    return ChaosEnv(
        root=root,
        autoboot_path=autoboot_path,
        autoboot_mirror_path=mirror_path,
        data_dir=data_agora,
        slot_state_path=data_agora / "slot-state.json",
        sentinel_path=data_agora / "migration-allowed",
        cmdline_path=cmdline_path,
    )


@contextmanager
def active_env(env: ChaosEnv) -> Iterator[ChaosEnv]:
    """Context manager: set ``AGORA_*`` env vars to point at ``env`` paths.

    Restores prior env-var values on exit. Scenarios always wrap their
    production-code invocations in this so :mod:`slot_mgr.paths` resolves to
    the isolated tree.
    """
    overrides = {
        "AGORA_AUTOBOOT_PATH": str(env.autoboot_path),
        "AGORA_AUTOBOOT_MIRROR_PATH": str(env.autoboot_mirror_path),
        "AGORA_SLOT_DATA_DIR": str(env.data_dir),
        "AGORA_PROC_CMDLINE": str(env.cmdline_path),
    }
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    try:
        yield env
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def set_cmdline_slot(env: ChaosEnv, slot: int) -> None:
    """Rewrite ``env.cmdline_path`` to claim we just booted into ``slot``.

    Used by scenarios that simulate a reboot after a chaos injection — the
    bootloader's pick of slot is reflected in the new ``/proc/cmdline``.
    """
    if slot not in SLOT_TO_BOOT_PART:
        raise ChaosError(f"slot must be 1 or 2, got {slot!r}")
    env.cmdline_path.write_text(_cmdline_contents(slot))


# ---------------------------------------------------------------------------
# Injection helpers
# ---------------------------------------------------------------------------


def inject_after_nth_call(
    module: object,
    attr: str,
    n: int,
    *,
    message: str = "chaos",
) -> contextlib.AbstractContextManager:
    """Context manager: let ``module.attr`` run normally for ``n`` calls, then fault.

    On call ``n+1`` (1-indexed), raises :class:`ChaosPowerCut` *before*
    invoking the original function — so the n+1th call's side effect is
    not committed.

    Parameters
    ----------
    module:
        The module (or class) holding the attribute to patch.
    attr:
        The attribute name to replace.
    n:
        Number of calls that should run normally before the fault. ``n=0``
        means the very first call faults.
    message:
        Free-form text included in the ``ChaosPowerCut`` message — names the
        scenario for easier debugging when a test fails.
    """
    if n < 0:
        raise ChaosError(f"n must be >= 0, got {n!r}")
    orig = getattr(module, attr)
    state = {"calls": 0}

    def wrapper(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] > n:
            raise ChaosPowerCut(f"{message} (call {state['calls']})")
        return orig(*args, **kwargs)

    return mock.patch.object(module, attr, new=wrapper)


def inject_first_call(
    module: object,
    attr: str,
    *,
    message: str = "chaos",
) -> contextlib.AbstractContextManager:
    """Shortcut for ``inject_after_nth_call(module, attr, 0, message=…)``."""
    return inject_after_nth_call(module, attr, 0, message=message)


# ---------------------------------------------------------------------------
# Scenario primitives
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """One chaos-injection scenario.

    The ``run`` callable receives a freshly-built :class:`ChaosEnv` already
    wrapped in :func:`active_env` (so ``slot_mgr`` will route I/O into the
    isolated tree). It should:

    1. Drive the production-code path that's being chaos-tested.
    2. Catch :class:`ChaosPowerCut` from the injected fault.
    3. Optionally simulate "next boot" (e.g. mutate cmdline, restart state).
    4. Assert the invariants (use plain ``assert`` — :func:`run_scenario`
       captures :class:`AssertionError` into the result).

    The callable returns ``None`` on success; any uncaught exception is
    captured as a failure.
    """

    name: str
    description: str
    run: Callable[[ChaosEnv], None]


@dataclass(frozen=True)
class ScenarioResult:
    """Outcome of running one scenario.

    Attributes
    ----------
    name:
        Scenario identifier (matches ``Scenario.name``).
    ok:
        Whether all invariants held.
    detail:
        Short human-readable summary (empty on success; exception message
        or assertion text on failure).
    error_type:
        Exception class name when ``ok=False`` and the failure came from an
        unexpected exception (not an :class:`AssertionError`); ``None``
        otherwise.
    traceback:
        Full traceback string when ``ok=False``; empty otherwise.
    """

    name: str
    ok: bool
    detail: str = ""
    error_type: Optional[str] = None
    traceback: str = ""


def run_scenario(scenario: Scenario, env: ChaosEnv) -> ScenarioResult:
    """Execute one scenario under ``env``. Catches failures into a result."""
    try:
        with active_env(env):
            scenario.run(env)
        return ScenarioResult(name=scenario.name, ok=True)
    except AssertionError as exc:
        return ScenarioResult(
            name=scenario.name,
            ok=False,
            detail=str(exc) or "assertion failed",
            error_type="AssertionError",
            traceback=traceback.format_exc(),
        )
    except ChaosPowerCut as exc:
        # ChaosPowerCut leaking out is a bug in the scenario itself - it
        # should have been caught after the injected fault fired.
        return ScenarioResult(
            name=scenario.name,
            ok=False,
            detail=f"uncaught ChaosPowerCut: {exc}",
            error_type="ChaosPowerCut",
            traceback=traceback.format_exc(),
        )
    except Exception as exc:  # noqa: BLE001 - this is the catch-all sink
        return ScenarioResult(
            name=scenario.name,
            ok=False,
            detail=f"{type(exc).__name__}: {exc}",
            error_type=type(exc).__name__,
            traceback=traceback.format_exc(),
        )


def run_scenarios(
    scenarios: Iterable[Scenario],
    env_factory: Callable[[], ChaosEnv],
) -> list[ScenarioResult]:
    """Run each scenario in a freshly-built env. Returns all results."""
    results: list[ScenarioResult] = []
    for scenario in scenarios:
        env = env_factory()
        results.append(run_scenario(scenario, env))
    return results


def summarize_results(results: Sequence[ScenarioResult]) -> dict[str, object]:
    """Return a compact summary dict suitable for ``json.dumps``."""
    total = len(results)
    passed = sum(1 for r in results if r.ok)
    failed = total - passed
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "ok": failed == 0,
        "scenarios": [
            {
                "name": r.name,
                "ok": r.ok,
                "detail": r.detail,
                "error_type": r.error_type,
            }
            for r in results
        ],
    }
