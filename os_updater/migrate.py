"""Forward-migration runner — /data schema upgrade post-promote.

p2-forward-migration. Applies shell-script migrations from
``/etc/agora/migrations/NNN_description.sh`` after the slot is promoted,
bumping ``/data/SCHEMA_VERSION`` in lockstep. Gated by the
migration-allowed sentinel from Phase 1 (PR #169) via :mod:`migration_fence`
— refuses to run on a tentative tryboot slot, which is the whole point of
the fence.

Plan §"Phase 2 — Deliverables":

    Forward-migration runner: reads /data/SCHEMA_VERSION, discovers
    /etc/agora/migrations/NNN_*.sh with NNN > current, applies in
    order. Runs post-promote only, gated by migration-allowed sentinel
    from Phase 1.

Plan §"Phase 2 — Acceptance":

    Ship a v2 release with a 001_add_demo_field.sh migration; dispatch
    to a v1 device; verify SCHEMA_VERSION goes 1→2 only after promote.

Module shape mirrors :mod:`os_updater.bundle`:

* Typed exception hierarchy mapped onto ``failed:<reason>`` wire codes by
  :meth:`os_updater.service.OSUpdaterService._classify_failure`.
* :data:`Runner` Protocol seam so tests inject a fake ``subprocess.run``.
* :func:`run_pending_migrations` is the synchronous orchestrator; the
  :class:`ForwardMigrator` dataclass wraps it in :func:`asyncio.to_thread`
  to implement the ``Migrator`` Protocol from :mod:`os_updater.service`.

Wire codes (paired with :meth:`OSUpdaterService._classify_failure`):

* :class:`MigrationFenceDenied` → ``migration_fence_denied`` — sentinel
  said no (slot mismatch, missing/unreadable/malformed sentinel). The
  fence package returns ``FenceStatus(allowed=False)`` for all
  operational denials; we raise this so the daemon emits a precise wire
  code instead of the generic bucket.
* :class:`MigrationScriptError` → ``migration_script_failed`` — a
  migration shell script exited non-zero under ``set -euo pipefail``.
  Halts the chain; ``SCHEMA_VERSION`` stays at the highest script that
  did succeed. Recovery is ship-a-fix-forward (plan §"Risks").
* :class:`SchemaVersionError` → ``migration_schema_version_invalid`` —
  ``/data/SCHEMA_VERSION`` is malformed, unwritable, or unreadable.
* :class:`MigrationDiscoveryError` → ``migration_discovery_failed`` —
  the migrations directory has a malformed filename, a duplicate NNN,
  or is otherwise unreadable.

Empty migrations directory is NOT an error — every patch release where
the on-disk schema didn't change ships zero migrations, and the runner
no-ops cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence, TYPE_CHECKING

from shared.state import atomic_write

if TYPE_CHECKING:  # pragma: no cover - import-cycle avoidance
    from migration_fence import FenceStatus


logger = logging.getLogger(__name__)


#: Default location of the on-device schema version file. Lives on
#: ``/data`` so it survives slot switches — that's the whole point.
DEFAULT_SCHEMA_VERSION_PATH = Path("/data/SCHEMA_VERSION")

#: Default location of the migration scripts. Lives in the rootfs so
#: a slot promote takes the new migration set with it; the fence
#: ensures the new scripts only run after the slot is promoted.
DEFAULT_MIGRATIONS_ROOT = Path("/etc/agora/migrations")

#: Bash binary used to execute migration scripts. Overridable for tests.
DEFAULT_BASH = "/bin/bash"

#: Per-script wall-clock timeout. Sized for a generous fsync-heavy
#: migration on a slow SD card; individual scripts that need longer
#: should split into multiple migrations.
DEFAULT_MIGRATION_TIMEOUT_S = 600.0

#: Migration filename grammar: three-digit NNN, underscore, snake-case
#: identifier, ``.sh`` extension. Anything else is rejected to keep
#: ordering deterministic and to surface typos at discovery time.
_MIGRATION_FILENAME_RE = re.compile(r"^(\d{3})_[A-Za-z0-9][A-Za-z0-9_-]*\.sh$")


# ── Exception hierarchy ────────────────────────────────────────────────────


class MigrationError(Exception):
    """Base class for forward-migration failures.

    The os-updater service maps subclasses onto specific
    ``failed:<reason>`` wire codes via
    :meth:`OSUpdaterService._classify_failure`. Generic
    :class:`MigrationError` instances fall through to ``migration_failed``.
    """


class MigrationFenceDenied(MigrationError):
    """The migration-allowed sentinel did not authorize a migration run.

    Mapped to ``failed:migration_fence_denied``. Operationally three
    flavors, all surfaced verbatim in the attached :class:`FenceStatus`:

    * Sentinel absent — the slot promote step never wrote it (we are
      still in tryboot, not promoted). The fence is doing its job.
    * Sentinel slot ≠ running slot — someone tryboot-reverted between
      promote and our run. Don't migrate /data on a slot we're not on.
    * Sentinel unreadable / malformed — operational fault, retry next
      boot.

    Carries the full :class:`FenceStatus` so callers can log the
    structured reason (``status.reason``) alongside the wire code.
    """

    def __init__(self, status: "FenceStatus") -> None:
        super().__init__(status.reason)
        self.status = status


class MigrationScriptError(MigrationError):
    """A migration shell script exited with non-zero status.

    Mapped to ``failed:migration_script_failed``. Halts the chain;
    ``SCHEMA_VERSION`` is left at the highest script that did succeed
    (we update the version *after* the script returns 0, so a partial
    sequence is well-defined). Recovery is shipping a fix-forward —
    see plan.md §"Risks & mitigations".
    """

    def __init__(
        self,
        step: "MigrationStep",
        *,
        returncode: int,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(
            f"migration {step.name} (NNN={step.version:03d}) "
            f"exited with rc={returncode}"
        )
        self.step = step
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class SchemaVersionError(MigrationError):
    """/data/SCHEMA_VERSION is malformed, unreadable, or unwritable.

    Mapped to ``failed:migration_schema_version_invalid``. Either the
    file has non-integer content, a negative integer, or an OS-level
    read/write error.
    """


class MigrationDiscoveryError(MigrationError):
    """Migrations directory has a malformed filename or a duplicate NNN.

    Mapped to ``failed:migration_discovery_failed``. Caught at the
    discovery step so we never half-apply a sequence whose ordering is
    ambiguous.
    """


# ── Subprocess seam (mirrors :mod:`os_updater.bundle`) ────────────────────


#: Callable type matching :func:`subprocess.run`. Tests pass a fake
#: that returns a canned :class:`subprocess.CompletedProcess`.
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def _default_runner(
    args: Sequence[str],
    *,
    check: bool = False,
    capture_output: bool = True,
    text: bool = True,
    timeout: Optional[float] = None,
) -> "subprocess.CompletedProcess[str]":
    """Thin wrapper around :func:`subprocess.run` with our defaults applied."""
    return subprocess.run(
        list(args),
        check=check,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )


# ── Fence seam (mirrors :mod:`migration_fence`) ───────────────────────────


#: Callable type returning a :class:`FenceStatus`. Tests inject a fake
#: so we can drive both allowed and denied branches without ``/data``.
FenceCheckFn = Callable[[], "FenceStatus"]


def _default_fence_check() -> "FenceStatus":
    """Lazy import of :func:`migration_fence.check_migration_fence`.

    Kept as a function (not a module-level import) so :mod:`os_updater`
    stays importable in environments without ``slot_mgr`` — the fence
    package imports ``slot_mgr`` lazily too. Mirrors the pattern from
    :func:`migration_fence.core._default_slot_state`.
    """
    from migration_fence import check_migration_fence

    return check_migration_fence()


# ── Migration model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MigrationStep:
    """A single migration to apply, parsed from ``NNN_<name>.sh``.

    Immutable so it can be safely shared by reference; equality is by
    version + name so a discovery that yields two different ``Path``
    objects pointing at the same logical step still compares equal.
    """

    version: int
    name: str
    path: Path = field(compare=False)


@dataclass(frozen=True)
class MigrationResult:
    """Summary of a forward-migration run.

    Returned to the caller so it can attach the version-bump and the
    applied-script list to the ``migration_complete`` lifecycle event
    payload for CMS-side rollups.
    """

    starting_version: int
    ending_version: int
    applied: tuple[MigrationStep, ...]


# ── Schema-version IO ──────────────────────────────────────────────────────


def read_schema_version(path: Path = DEFAULT_SCHEMA_VERSION_PATH) -> int:
    """Read ``/data/SCHEMA_VERSION`` and return it as a non-negative int.

    A missing file returns ``0``. This is the "fresh device" case: the
    Phase 0 firstboot service seeds ``SCHEMA_VERSION=1``, so a missing
    file in practice only happens in unit tests, but treating it as 0
    is the safe operational fallback (a release with a ``001_init.sh``
    migration will correctly bump the version on the next migrate).

    An empty file is also treated as ``0`` for the same reason.

    A non-integer or negative value raises :class:`SchemaVersionError`,
    which the service maps to ``failed:migration_schema_version_invalid``.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise SchemaVersionError(f"reading {path}: {exc}") from exc

    stripped = text.strip()
    if not stripped:
        return 0
    try:
        version = int(stripped)
    except ValueError as exc:
        raise SchemaVersionError(
            f"{path} content is not an integer: {text!r}"
        ) from exc
    if version < 0:
        raise SchemaVersionError(f"{path} has negative value: {version}")
    return version


def write_schema_version(path: Path, version: int) -> None:
    """Atomically write the new ``SCHEMA_VERSION`` value.

    Uses :func:`shared.state.atomic_write` (tempfile + rename) so a
    crash mid-write leaves the previous version intact — critical for
    the "schema version reflects last successful step" invariant.

    Trailing newline included so the file matches the existing Phase 0
    seed (``echo 1 > /data/SCHEMA_VERSION``).
    """
    if version < 0:
        raise SchemaVersionError(
            f"refusing to write negative version: {version}"
        )
    try:
        atomic_write(path, f"{version}\n")
    except OSError as exc:
        raise SchemaVersionError(f"writing {path}: {exc}") from exc


# ── Discovery ──────────────────────────────────────────────────────────────


def _parse_migration_filename(name: str) -> tuple[int, str]:
    """Return ``(version, name_without_extension)`` for an ``NNN_*.sh``.

    Raises :class:`MigrationDiscoveryError` on a malformed filename so
    the bad file gets surfaced at discovery rather than at execution
    (and so a typo doesn't get silently skipped).
    """
    m = _MIGRATION_FILENAME_RE.match(name)
    if not m:
        raise MigrationDiscoveryError(
            f"migration filename {name!r} does not match NNN_<name>.sh"
        )
    return int(m.group(1)), name[: -len(".sh")]


def discover_migrations(
    root: Path = DEFAULT_MIGRATIONS_ROOT,
    *,
    after_version: int,
) -> list[MigrationStep]:
    """Return migration steps with ``NNN > after_version``, sorted ascending.

    A missing ``root`` is NOT an error — it returns the empty list.
    That's the normal case for a release with zero schema changes.

    Raises :class:`MigrationDiscoveryError` on:

    * a filename that doesn't match ``NNN_<name>.sh``
    * two scripts sharing the same NNN
    * the path existing but not being a directory
    * an OS-level read error listing the directory
    """
    if not root.exists():
        return []
    if not root.is_dir():
        raise MigrationDiscoveryError(
            f"{root} exists but is not a directory"
        )
    try:
        entries = sorted(
            p
            for p in root.iterdir()
            if p.is_file() and p.suffix == ".sh"
        )
    except OSError as exc:
        raise MigrationDiscoveryError(
            f"listing {root}: {exc}"
        ) from exc

    by_version: dict[int, MigrationStep] = {}
    for path in entries:
        version, stem = _parse_migration_filename(path.name)
        if version in by_version:
            raise MigrationDiscoveryError(
                f"duplicate migration NNN={version:03d}: "
                f"{by_version[version].path.name} vs {path.name}"
            )
        by_version[version] = MigrationStep(
            version=version, name=stem, path=path
        )

    return sorted(
        (s for s in by_version.values() if s.version > after_version),
        key=lambda s: s.version,
    )


# ── Apply ──────────────────────────────────────────────────────────────────


def apply_migration(
    step: MigrationStep,
    *,
    runner: Runner = _default_runner,
    bash: str = DEFAULT_BASH,
    timeout_s: float = DEFAULT_MIGRATION_TIMEOUT_S,
) -> None:
    """Execute one migration script under ``set -euo pipefail``.

    The script is invoked as ``bash -eo pipefail -u <path>`` so the
    three safety flags are enforced even if the script itself forgets
    to set them. The plan calls for exactly this combination.

    Captures stdout and stderr and attaches them to the
    :class:`MigrationScriptError` on failure so the daemon can include a
    snippet in the ``failed:migration_script_failed`` event payload
    (the journal still has the full output via the daemon's logger).
    """
    logger.info(
        "applying migration: NNN=%03d name=%s path=%s",
        step.version,
        step.name,
        step.path,
    )
    cmd = [bash, "-eo", "pipefail", "-u", str(step.path)]
    try:
        result = runner(cmd, timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        captured_stdout = (
            exc.stdout
            if isinstance(exc.stdout, str)
            else (exc.stdout.decode("utf-8", "replace") if exc.stdout else "")
        )
        raise MigrationScriptError(
            step,
            returncode=124,
            stdout=captured_stdout,
            stderr=f"timed out after {timeout_s}s",
        ) from exc
    except FileNotFoundError as exc:
        raise MigrationScriptError(
            step,
            returncode=127,
            stderr=str(exc),
        ) from exc
    except OSError as exc:
        raise MigrationScriptError(
            step,
            returncode=-1,
            stderr=str(exc),
        ) from exc

    if result.returncode != 0:
        raise MigrationScriptError(
            step,
            returncode=result.returncode,
            stdout=result.stdout or "",
            stderr=result.stderr or "",
        )

    logger.info(
        "migration applied: NNN=%03d name=%s rc=%d",
        step.version,
        step.name,
        result.returncode,
    )


# ── Orchestrator ───────────────────────────────────────────────────────────


def run_pending_migrations(
    *,
    schema_version_path: Path = DEFAULT_SCHEMA_VERSION_PATH,
    migrations_root: Path = DEFAULT_MIGRATIONS_ROOT,
    runner: Runner = _default_runner,
    bash: str = DEFAULT_BASH,
    timeout_s: float = DEFAULT_MIGRATION_TIMEOUT_S,
    fence_check_fn: FenceCheckFn = _default_fence_check,
) -> MigrationResult:
    """Run every pending forward-migration in ``NNN`` order.

    Algorithm:

    1. Call ``fence_check_fn()``. If ``not status.allowed``, raise
       :class:`MigrationFenceDenied` (carrying the structured status).
       The fence is what enforces the "post-promote only" rule from the
       plan — without it, a tryboot-revert could land on a /data that's
       been forward-migrated.
    2. Read ``schema_version_path`` to get the current version.
    3. :func:`discover_migrations` ↑ that root, filtered to
       ``NNN > current``, sorted ascending.
    4. For each step in order:

       a. :func:`apply_migration` (raises :class:`MigrationScriptError`
          on non-zero exit; the exception propagates without bumping
          ``SCHEMA_VERSION``).
       b. :func:`write_schema_version` to ``step.version``.

    5. Return a :class:`MigrationResult` summarizing the run.

    An empty pending list returns a no-op result with
    ``starting_version == ending_version`` and an empty ``applied``
    tuple. Most releases will look like this.
    """
    status = fence_check_fn()
    if not status.allowed:
        logger.warning(
            "forward-migration denied by fence: reason=%r "
            "allowed_slot=%s running_slot=%s",
            status.reason,
            status.allowed_slot,
            status.running_slot,
        )
        raise MigrationFenceDenied(status)

    current = read_schema_version(schema_version_path)
    steps = discover_migrations(migrations_root, after_version=current)
    logger.info(
        "forward-migration starting: current=%d pending=%d NNN=%s",
        current,
        len(steps),
        [s.version for s in steps],
    )

    applied: list[MigrationStep] = []
    for step in steps:
        apply_migration(
            step,
            runner=runner,
            bash=bash,
            timeout_s=timeout_s,
        )
        write_schema_version(schema_version_path, step.version)
        applied.append(step)

    ending = applied[-1].version if applied else current
    logger.info(
        "forward-migration complete: %d -> %d (%d applied)",
        current,
        ending,
        len(applied),
    )
    return MigrationResult(
        starting_version=current,
        ending_version=ending,
        applied=tuple(applied),
    )


# ── Migrator adapter ──────────────────────────────────────────────────────


@dataclass
class ForwardMigrator:
    """``Migrator`` Protocol implementation backed by :func:`run_pending_migrations`.

    Construction parameters are all overridable for tests, mirroring
    :class:`os_updater.verifier.SignatureVerifier`. The :meth:`run`
    coroutine wraps the synchronous orchestrator in
    :func:`asyncio.to_thread` so the daemon's event loop stays
    responsive while shell scripts execute.

    The :class:`MigrationResult` returned by the orchestrator is
    stashed on ``self.last_result`` for callers that want to attach it
    to the ``migration_complete`` event payload — Phase 4's event-buffer
    enrichment can pick this up without a wider Protocol change.
    """

    schema_version_path: Path = field(
        default_factory=lambda: DEFAULT_SCHEMA_VERSION_PATH
    )
    migrations_root: Path = field(
        default_factory=lambda: DEFAULT_MIGRATIONS_ROOT
    )
    runner: Runner = field(default=_default_runner)
    bash: str = DEFAULT_BASH
    timeout_s: float = DEFAULT_MIGRATION_TIMEOUT_S
    fence_check_fn: FenceCheckFn = field(default=_default_fence_check)
    last_result: Optional[MigrationResult] = field(default=None, init=False)

    async def run(self) -> None:
        """Run pending migrations and record the result on ``self``."""
        result = await asyncio.to_thread(
            run_pending_migrations,
            schema_version_path=self.schema_version_path,
            migrations_root=self.migrations_root,
            runner=self.runner,
            bash=self.bash,
            timeout_s=self.timeout_s,
            fence_check_fn=self.fence_check_fn,
        )
        self.last_result = result
