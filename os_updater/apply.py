"""Slot staging + tryboot trigger — the ``Stager`` collaborator.

Implements steps 6–10 of ``docs/bundle-format.md`` §"On-device apply
flow", streaming-decompress edition (PR #3, plan.md §"OTA v0.0.6-test"
follow-up):

1. ``zstd -dc | tar -xOf - meta.json`` → stream meta.json to disk
   without touching the bundle's other contents. Cheapest possible
   ``target_version`` reject.
2. Parse ``meta.json`` via :func:`bundle.parse_bundle_meta`.
3. Defense-in-depth: compare ``meta.version`` to
   ``payload.target_version`` BEFORE we write a single byte to slot B.
4. Determine running + inactive slots via :func:`slot_mgr.slot_state`.
5. Ensure target mountpoints for the inactive slot's boot + root
   partitions are mounted; if not (the agora-os v0.0.7-test failure
   mode — no ``.mount`` units shipped), mount them ourselves via
   :func:`ensure_partition_mounted` against the GPT partition labels
   baked by ``agora-os/image-build/assemble.sh``. Idempotent — a
   future agora-os release that ships systemd ``.mount`` units will
   trip the already-mounted short-circuit and no-op here.
6. ``zstd -dc | tar -x --strip-components=1 -C <boot_target> boot``
   — stream the boot subtree straight onto the inactive boot
   partition. ``--strip-components=1`` rewrites ``boot/foo`` to
   ``<boot_target>/foo`` AND naturally filters anything that isn't
   under ``boot/``, so a top-level allowlist check is no longer
   needed.
7. Same as 6 for ``root`` onto the inactive root partition.
8. Verify the sha256 manifest via :func:`bundle.verify_bundle_manifest`
   against the bytes that actually landed on the partitions.
9. Copy per-device fleet-state files from the running rootfs (slot
   A) into slot B (machine-id, SSH host keys, CMS creds, etc. — see
   D60/D63 in plan.md). Done AFTER verify so the manifest check
   doesn't see files we added.
10a. Substitute ``etc/fstab`` for the inactive slot from the bundled
     ``etc/fstab.template`` (``{{BOOT_PARTLABEL}}`` placeholder).
     Critical: the device bricks at boot if its fstab points at the
     wrong boot partition.
10b. Rewrite ``cmdline.txt`` defensively (force ``root=PARTLABEL=root-{A,B}``
     and ``rw`` flag), insulating us from per-slot cmdline drift in
     the bundle.
10c. Trigger tryboot via :func:`slot_mgr.trigger_tryboot` (rewrites
     ``[tryboot] boot_partition``, records ``last_tryboot_target``,
     and reboots).

Exposed as :class:`SlotStager`, implementing the ``Stager`` Protocol
from :mod:`os_updater.service`. The service awaits :meth:`stage`
between transitioning the FSM to ``TRYBOOT_PENDING`` and to
``TRYBOOT_RUNNING``.

Streaming-decompress saves ~8 GB peak on ``/data`` versus the old
two-step (decompress to ``bundle.tar`` on /data, then extract). Each
pass re-reads the ~1 GB compressed bundle from SD; Pi 5 zstd is
~50-100 MB/s, so all three passes total ~3 min — same ballpark as
the old flow. Trade-off: lower disk-headroom requirements at the
cost of three reads instead of one.

Error taxonomy (the service maps each to a distinct wire code):

* :class:`StagingError` — generic staging failure. Maps to
  ``failed:stage_failed``.
* :class:`RsyncError` — legacy; no longer raised by the streaming
  path but kept exported for callers / tests that still use
  :func:`rsync_tree` directly. Maps to ``failed:stage_rsync_failed``.
* :class:`TrybootError` — :func:`slot_mgr.trigger_tryboot` raised
  (e.g. pinned device, autoboot rewrite failed, reboot subprocess
  failed). Maps to ``failed:tryboot_failed``.
* :class:`FleetStateMissingError` — a required fleet-state file was
  not present on slot A. Maps to ``failed:fleet_state_missing``.
* :class:`FleetStateWriteError` — ``cp -a`` to slot B failed. Maps
  to ``failed:slot_b_write_failed``.
* :class:`FstabError` — bundled fstab template missing /
  unsubstitutable / unwritable. Maps to ``failed:fstab_failed``.
* :class:`CmdlineError` — cmdline.txt missing or unwritable on the
  inactive boot partition. Maps to ``failed:cmdline_failed``.
* :class:`BundleIntegrityError` (from :mod:`bundle`) — re-raised
  unchanged when decompression / extraction / version-mismatch /
  manifest verification fails. Maps to ``failed:bundle_invalid``.

Staging contents (meta.json) are **not** cleaned up on failure —
forensics trump disk reclaim, especially for ``bundle_invalid`` cases.
The service-level cleanup that runs on ``agora-os-updater.service``
start (24h TTL sweep, per Phase 2 deliverable §"Partial-download
cleanup") catches everything eventually.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from os_updater.bundle import (
    BundleIntegrityError,
    Runner,
    _default_runner,
    parse_bundle_meta,
    verify_bundle_manifest,
)
from os_updater.dispatch import DispatchPayload
from os_updater.events import PROGRESS_MIN_INTERVAL_S


logger = logging.getLogger(__name__)


# ── Defaults ───────────────────────────────────────────────────────────────


#: Filename of the signed artifact in the staging dir, matching
#: :data:`os_updater.verifier.DEFAULT_BUNDLE_FILENAME`.
DEFAULT_BUNDLE_FILENAME = "bundle.tar.zst"

#: Mountpoint of the **active** slot's boot (FAT32) partition. agora-os
#: assemble.sh convention: regardless of which slot is active, the
#: bootloader-selected boot partition is always exposed at
#: ``/boot/firmware``. Read-only from the OTA stager's perspective —
#: this is where we **read** per-device boot fleet-state from (e.g.
#: ``autoboot.txt``) before tryboot.
#:
#: **Naming history:** previously called ``DEFAULT_BOOT_MOUNT_SLOT_A``,
#: but the value never depended on slot identity — it always pointed at
#: whatever slot the bootloader landed on. The old name caused a 5-bug
#: chain (sslivins/agora-os v0.0.18-test brick) where the stager
#: dispatched the inactive boot extract to ``/boot/firmware`` (the
#: ACTIVE slot's boot, corrupting it) because :func:`boot_mount_for_slot`
#: returned ``slot_a_mount`` when ``slot == 1`` even though the running
#: slot was 2. Renamed to ``DEFAULT_ACTIVE_BOOT_MOUNT`` to make the
#: invariant unambiguous.
DEFAULT_ACTIVE_BOOT_MOUNT = Path("/boot/firmware")

#: Mountpoint of the **inactive** slot's boot (FAT32) partition. Phase 0
#: fstab keeps the inactive slot's FAT32 partition mounted at the mirror
#: path so slot_mgr can keep ``autoboot.txt`` in sync across both copies
#: and the OTA stager has a stable target for the boot-subtree extract.
#:
#: **Naming history:** previously ``DEFAULT_BOOT_MOUNT_SLOT_B``. See
#: :data:`DEFAULT_ACTIVE_BOOT_MOUNT` for the rename rationale.
DEFAULT_INACTIVE_BOOT_MOUNT = Path("/boot/firmware-b")

#: Where the inactive slot's root partition is mounted while the
#: running slot writes into it. :class:`SlotStager` ensures this is
#: mounted itself at the start of step 5 via
#: :func:`ensure_partition_mounted` — earlier versions trusted the
#: daemon-launcher systemd unit to mount it before daemon start, but
#: no such unit shipped in agora-os v0.0.7-test or earlier, leading
#: to silent writes onto the active rootfs and a brick on tryboot.
DEFAULT_INACTIVE_ROOT_MOUNT = Path("/mnt/inactive-root")

#: Default location of the kernel's mount table. Tests inject a path
#: to a fixture file with mountinfo-style content (one mount per
#: line, mountpoint in column 2).
DEFAULT_MOUNTS_PATH = Path("/proc/self/mounts")

#: Base path under which the kernel materializes GPT-partition-label
#: symlinks. Tests inject a tmp path. Parallels
#: :data:`precheck.core.DEFAULT_PARTLABEL_BASE` — duplicated here so
#: ``apply`` does not depend on ``precheck``.
DEFAULT_PARTLABEL_BASE = Path("/dev/disk/by-partlabel")

#: Mount options for the inactive slot's boot (FAT32) partition.
#: ``flush`` makes writes more durable and matches the fstab template
#: shipped by agora-os.
DEFAULT_BOOT_MOUNT_OPTS = "defaults,flush"

#: Mount options for the inactive slot's root (ext4) partition.
#: ``noatime`` because the partition is write-once-then-tryboot —
#: avoiding noisy atime writes during the multi-GB streaming extract.
DEFAULT_ROOT_MOUNT_OPTS = "defaults,noatime"

#: Per-call timeout for the ``mount(8)`` invocations issued by
#: :func:`ensure_partition_mounted`. The kernel and udev are fast;
#: anything taking longer than this on a healthy partition is
#: structurally wrong.
DEFAULT_MOUNT_TIMEOUT_S = 30.0

#: Compiled regex matching the C-style octal escapes used by
#: ``/proc/self/mounts`` to encode special characters in mountpoint
#: paths (``\040`` space, ``\011`` tab, ``\012`` newline, ``\134``
#: backslash). Only octal escapes are valid; the kernel never emits
#: ``\u``/``\U``/``\N`` style escapes that Python's ``unicode_escape``
#: codec would otherwise mishandle for Windows-style test paths.
_MOUNTS_OCTAL_RE = re.compile(r"\\([0-7]{3})")

#: zstd long-range window matching the builder side
#: (docs/bundle-format.md §"Compression": ``zstd -19 --long=27``).
#: ``--long=27`` requires the same flag on decompress to allocate the
#: 128 MB window.
DEFAULT_ZSTD_LONG = 27

#: Per-step subprocess timeout. Generous default for the slowest 32 GB
#: SD cards in the field — actual times on a v1 bundle are ~3 min per
#: ``zstd | tar`` streaming pass on a Pi 5. The streaming pipeline does
#: three passes (meta-only, boot/, root/) that re-read the .tar.zst from
#: disk each time; this timeout applies to each pass individually.
DEFAULT_STREAM_TIMEOUT_S = 900.0
DEFAULT_RSYNC_TIMEOUT_S = 1800.0

#: Per-``cp -a`` invocation timeout for the fleet-state copy step.
#: Each file is small (machine-id is 32 bytes, ssh host keys a few KB,
#: cms_config.json single-digit KB, wifi-*.nmconnection a few KB);
#: 30 s is comfortably 1000× the worst real-world case.
DEFAULT_FLEET_STATE_CP_TIMEOUT_S = 30.0

#: Closed enumeration of phase names emitted by :meth:`SlotStager._stage_sync`
#: via the optional ``progress_callback``. The CMS-side parser switches
#: on these strings (carried in ``LifecycleEvent.payload["phase"]``) to
#: render an upgrade-progress UI, so any addition is a coordinated
#: change. Order matches the actual emission order during a happy-path
#: stage. Tracked as ``sslivins/agora#202``.
STAGE_PROGRESS_PHASES: tuple[str, ...] = (
    "extracting_meta",
    "mounting_inactive",
    "wiping_inactive",
    "extracting_boot",
    "extracting_rootfs",
    "verifying_manifest",
    "copying_fleet_state",
    "finalizing",
)


def _safe_emit_progress(
    progress_callback: Optional[Callable[[str], None]], phase: str
) -> None:
    """Best-effort invocation of a stage progress callback.

    The callback is wired by the service to ``emit_event(STAGE_PROGRESS, ...)``
    which can fail (full disk on ``/data``, sink down, etc.). A buggy
    callback or a sink failure must NOT brick an OTA mid-stage — the
    progress signal is purely advisory. Swallow + log on failure;
    :meth:`_stage_sync` keeps running.
    """

    if progress_callback is None:
        return
    try:
        progress_callback(phase)
    except Exception:  # noqa: BLE001 — intentional firewall around advisory callback
        logger.exception(
            "progress_callback raised on phase=%s; continuing stage", phase
        )


#: Soft deadline for the zstd PID to materialize after the pipeline runner
#: starts. The default subprocess.Popen child shows up in /proc within
#: microseconds; 5 s is a comfortably large ceiling even on a thrashing Pi.
#: If the poll never sees a PID by this deadline it just exits quietly —
#: progress is advisory and we'd rather lose progress than introduce a
#: spurious timeout.
_PID_OBSERVER_WAIT_S = 5.0


def _read_fdinfo_pos(pid: int) -> Optional[int]:
    """Return the byte position of ``/proc/<pid>/fdinfo/0`` (zstd's stdin).

    Returns ``None`` if the file is unreadable for any reason (PID gone,
    not on Linux, malformed contents). The poller treats every ``None``
    as "stop polling" — there's nothing to recover from at this layer.
    """

    try:
        with open(f"/proc/{pid}/fdinfo/0", "rt") as f:
            for line in f:
                if line.startswith("pos:"):
                    try:
                        return int(line.split(":", 1)[1].strip())
                    except (ValueError, IndexError):
                        return None
        return None
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None


def _poll_extract_progress(
    pid_holder: dict[str, Optional[int]],
    stop_event: threading.Event,
    compressed_size: int,
    callback: Callable[[int, int], None],
    *,
    interval_s: float = PROGRESS_MIN_INTERVAL_S,
    pid_wait_deadline_s: float = _PID_OBSERVER_WAIT_S,
    fdinfo_reader: Callable[[int], Optional[int]] = _read_fdinfo_pos,
) -> None:
    """Poll ``/proc/<zstd_pid>/fdinfo/0`` and emit bytes-progress.

    Runs in a worker thread spawned by :func:`stream_extract_subtree`.
    Reads the ``pos:`` field of zstd's stdin fdinfo (== compressed
    bytes consumed from the .tar.zst on disk) at most every
    ``interval_s`` seconds, then calls ``callback(bytes_read,
    compressed_size)``.

    Exits cleanly when any of these happens, in order of likelihood:

    * ``stop_event`` is set by the caller (pipeline completed).
    * fdinfo read returns ``None`` (zstd exited; ``/proc/<pid>/...`` gone).
    * ``pid_holder["pid"]`` is still ``None`` after ``pid_wait_deadline_s``.

    Callback exceptions are caught — progress is advisory, must not
    take down the extract. Test-only knobs (``interval_s``,
    ``pid_wait_deadline_s``, ``fdinfo_reader``) are explicit kwargs so
    unit tests can drive the poller without touching ``/proc``.
    """

    # Wait for the pipeline runner to capture and publish zstd's PID.
    wait_deadline = time.monotonic() + pid_wait_deadline_s
    while pid_holder["pid"] is None:
        if stop_event.wait(0.05):
            return
        if time.monotonic() > wait_deadline:
            logger.debug(
                "extract progress poller gave up waiting for zstd PID after %.2fs",
                pid_wait_deadline_s,
            )
            return

    pid = pid_holder["pid"]
    assert pid is not None  # for type narrowing
    while not stop_event.is_set():
        pos = fdinfo_reader(pid)
        if pos is None:
            return
        try:
            callback(pos, compressed_size)
        except Exception:  # noqa: BLE001 — advisory; never brick the extract
            logger.exception(
                "extract progress callback raised; continuing poll"
            )
        if stop_event.wait(interval_s):
            return

#: Root of the currently-running rootfs from which
#: :func:`copy_fleet_state` reads per-device identity files. Path is
#: ``/`` in production; tests inject ``tmp_path`` to bypass.
#:
#: **Naming history:** previously ``DEFAULT_SLOT_A_ROOT``. The value
#: was always ``/`` (the running slot's rootfs) regardless of which
#: slot the device booted into, so "slot_a" was misleading. Renamed
#: to ``DEFAULT_RUNNING_ROOT`` alongside the broader
#: active-vs-inactive rename in the v0.0.19-test fix.
DEFAULT_RUNNING_ROOT = Path("/")

#: Files that **must** be present on slot A and copied into slot B
#: before tryboot. Each entry is a path **relative** to the rootfs
#: root (no leading slash) so it composes with both ``slot_a_root``
#: and ``slot_b_root``. Patterns containing ``*`` are globs; for a
#: required glob entry, ``>=1`` match is required. A required entry
#: missing on slot A is a hard abort (``failed:fleet_state_missing``).
#:
#: Refusing to apply on absence is the safe default: a device missing
#: any of these is either unprovisioned or in a broken state and the
#: OTA shouldn't paper over that. See D60 in plan.md §"Phase 2".
FLEET_STATE_REQUIRED: tuple[str, ...] = (
    "etc/agora/environment",
    "opt/agora/persist/cms_config.json",
    "opt/agora/persist/provisioned",
    "etc/machine-id",
    "etc/ssh/ssh_host_*",
)

#: Files that **may** be present on slot A and are copied into slot B
#: if found, but whose absence is not an error. Globs with zero
#: matches are fine. Same path-relative semantics as
#: :data:`FLEET_STATE_REQUIRED`.
#:
#: ``api_key`` is absent before the device has completed its first
#: CMS handshake; ``agora-api`` re-mints it on first contact.
#: ``wifi-*.nmconnection`` is empty for ethernet-only deployments.
#: ``home/agora/.ssh`` is absent on production-deployed Pis that have
#: no operator SSH access — copied as a directory entry so ``cp -a``
#: carries the 0700 perms + ``agora:agora`` ownership that ``sshd``
#: enforces on ``authorized_keys`` (see agora#198).
FLEET_STATE_COPY_IF_PRESENT: tuple[str, ...] = (
    "opt/agora/persist/api_key",
    "etc/NetworkManager/system-connections/wifi-*.nmconnection",
    "home/agora/.ssh",
)

#: Files that **must** be present on the active boot partition
#: (``/boot/firmware``) and copied into the inactive boot partition
#: (``/boot/firmware-b``) before tryboot.
#:
#: ``autoboot.txt`` is the **canonical** target: the bundle producer
#: deliberately omits it so we can copy the device's own copy here.
#: It carries the per-device tryboot/active slot selection and MUST
#: survive the OTA — without it the bootloader has no slot-selection
#: information and the device hangs at the rainbow screen.
#:
#: This is the boot-partition analog of :data:`FLEET_STATE_REQUIRED`.
#: Refusing to apply on absence is the safe default: a device missing
#: ``autoboot.txt`` on its active boot is either pre-A/B (vintage
#: image, manual reflash required) or in a broken state, and the OTA
#: shouldn't paper over that. See plan.md §"OTA v0.0.18-test brick".
BOOT_FLEET_STATE_REQUIRED: tuple[str, ...] = ("autoboot.txt",)

#: Boot-partition analog of :data:`FLEET_STATE_COPY_IF_PRESENT`.
#: Currently empty — no boot files are merely-optional. Reserved for
#: future use (e.g. a per-device ``config.txt`` overlay).
BOOT_FLEET_STATE_COPY_IF_PRESENT: tuple[str, ...] = ()

#: Relative path inside the inactive slot's root where the bundle ships
#: an fstab template with ``{{BOOT_PARTLABEL}}`` placeholder. Replaced
#: at apply-time with the inactive slot's boot PARTLABEL (``boot-A`` /
#: ``boot-B``) so the freshly-written slot mounts its OWN boot partition,
#: not the slot that built the bundle. See PR #14 on agora-os and the
#: §"OTA v0.0.7" note in plan.md.
DEFAULT_FSTAB_TEMPLATE_REL = "etc/fstab.template"

#: Relative path inside the inactive slot's root where the rendered
#: fstab is written. ``substitute_fstab`` writes here; nothing else
#: ships at this path (the bundle producer's ``rm -f bundle/root/etc/fstab``
#: keeps the manifest from listing it).
DEFAULT_FSTAB_REL = "etc/fstab"

#: Relative path inside the inactive slot's boot partition for the
#: kernel command line. ``rewrite_cmdline`` strips any ``root=`` token
#: and writes back ``root=PARTLABEL=root-A|B rw <rest>`` so the kernel
#: lands on the target slot's rootfs. Defends against the agora-os
#: cmdline-B.txt template missing ``rw`` (#cmdline-B-rw-typo).
DEFAULT_CMDLINE_REL = "cmdline.txt"


# ── Exceptions ─────────────────────────────────────────────────────────────


class StagingError(Exception):
    """Base class for slot-staging failures.

    Service maps to ``failed:stage_failed`` (see
    :meth:`os_updater.service.OSUpdaterService._classify_failure`).
    """


class RsyncError(StagingError):
    """``rsync`` returned non-zero while writing the inactive slot.

    Service maps to ``failed:stage_rsync_failed``.
    """


class TrybootError(StagingError):
    """:func:`slot_mgr.trigger_tryboot` raised while arming the reboot.

    Service maps to ``failed:tryboot_failed``.
    """


class FleetStateMissingError(StagingError):
    """A required fleet-state file/glob was not present on slot A.

    Carries ``path`` — the relative path (or glob pattern) that
    couldn't be satisfied — so callers and tests can pin the
    diagnostic without grepping the message string.

    Service maps to ``failed:fleet_state_missing``. See D60 in
    plan.md §"Phase 2".
    """

    def __init__(self, message: str, *, path: str) -> None:
        super().__init__(message)
        self.path = path


class FleetStateWriteError(StagingError):
    """``cp -a`` from slot A into slot B failed mid-fleet-state-copy.

    Carries ``path`` — the relative path of the source file whose
    copy returned non-zero (or timed out / hit a missing-binary
    error).

    Service maps to ``failed:slot_b_write_failed``. See D60 in
    plan.md §"Phase 2".
    """

    def __init__(self, message: str, *, path: str) -> None:
        super().__init__(message)
        self.path = path


class FstabError(StagingError):
    """``substitute_fstab`` could not render ``etc/fstab`` on the inactive slot.

    Common causes: bundle didn't ship ``etc/fstab.template``, the
    template was missing the ``{{BOOT_PARTLABEL}}`` placeholder, or
    write to slot B failed.

    Service maps to ``failed:fstab_substitute_failed``.
    """


class CmdlineError(StagingError):
    """``rewrite_cmdline`` could not rewrite ``cmdline.txt`` on the inactive slot.

    Common causes: bundle didn't ship a ``cmdline.txt`` on its boot/
    partition, or write to the FAT32 boot partition failed.

    Service maps to ``failed:cmdline_rewrite_failed``.
    """


# ── Pure helpers (no I/O) ──────────────────────────────────────────────────


def other_slot(slot: int) -> int:
    """Flip a slot number (1↔2). Raises :class:`StagingError` on bad input."""
    if slot == 1:
        return 2
    if slot == 2:
        return 1
    raise StagingError(f"invalid slot number: {slot!r} (expected 1 or 2)")


#: GPT partition labels baked into the image by
#: ``agora-os/image-build/assemble.sh`` (see plan.md D51).
_BOOT_PARTLABEL_FOR_SLOT = {1: "boot-A", 2: "boot-B"}
_ROOT_PARTLABEL_FOR_SLOT = {1: "root-A", 2: "root-B"}


def boot_partlabel_for_slot(slot: int) -> str:
    """Return the GPT partition label for ``slot``'s boot (FAT32) partition.

    Raises :class:`StagingError` on bad input.
    """
    if slot in _BOOT_PARTLABEL_FOR_SLOT:
        return _BOOT_PARTLABEL_FOR_SLOT[slot]
    raise StagingError(f"invalid slot number: {slot!r} (expected 1 or 2)")


def root_partlabel_for_slot(slot: int) -> str:
    """Return the GPT partition label for ``slot``'s root (ext4) partition.

    Raises :class:`StagingError` on bad input.
    """
    if slot in _ROOT_PARTLABEL_FOR_SLOT:
        return _ROOT_PARTLABEL_FOR_SLOT[slot]
    raise StagingError(f"invalid slot number: {slot!r} (expected 1 or 2)")


# ── Subprocess wrappers ────────────────────────────────────────────────────


def _tail(text: Optional[str], limit: int = 2000) -> str:
    """Truncate ``text`` for inclusion in log lines / exception detail."""
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return "…" + text[-limit:]


def _mounted_device(
    target: Path,
    *,
    mounts_path: Path = DEFAULT_MOUNTS_PATH,
) -> Optional[Path]:
    """Return the device backing ``target`` per ``mounts_path``, or None.

    Reads ``mounts_path`` (``/proc/self/mounts`` in production) and looks
    for a line whose second field (mountpoint) resolves to the same path
    as ``target`` after :py:meth:`Path.resolve`. The first field
    (device) of the matching line is returned as a :class:`Path`.
    Returns ``None`` if ``target`` is not a mountpoint or if
    ``mounts_path`` is missing — pure read with no side effects.

    ``/proc/self/mounts`` encodes special characters in both the device
    and mountpoint fields with C-style octal escapes (e.g. a literal
    space becomes ``\\040``); this helper decodes them before
    comparison/return so that paths containing spaces still match and
    round-trip correctly.

    This is the partlabel-verification primitive that fixes Bug 2 of
    the v0.0.18-test brick: :func:`ensure_partition_mounted` uses it
    to confirm the **right** device is mounted at ``target`` before
    treating the mountpoint as ready, rather than admitting any
    occupied mountpoint as "good enough."
    """
    target = Path(target).resolve()
    try:
        content = Path(mounts_path).read_text()
    except FileNotFoundError:
        return None
    for line in content.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        # ``/proc/self/mounts`` encodes only a fixed set of characters as
        # C-style **octal** escapes (\040 space, \011 tab, \012 newline,
        # \134 backslash). We deliberately do not run this through
        # Python's full ``unicode_escape`` codec because that would also
        # try to interpret ``\U``/``\u``/``\N`` etc., which blows up on
        # Windows paths like ``C:\Users\...`` even though we never see
        # such paths on real Pi hardware. Tests still want to run on
        # the maintainer's workstation.
        decoded_target = _MOUNTS_OCTAL_RE.sub(
            lambda m: chr(int(m.group(1), 8)), parts[1]
        )
        try:
            if Path(decoded_target).resolve() == target:
                decoded_device = _MOUNTS_OCTAL_RE.sub(
                    lambda m: chr(int(m.group(1), 8)), parts[0]
                )
                return Path(decoded_device)
        except OSError:
            continue
    return None


def _is_mountpoint(
    target: Path,
    *,
    mounts_path: Path = DEFAULT_MOUNTS_PATH,
) -> bool:
    """Return True if ``target`` appears as a mountpoint in ``mounts_path``.

    Thin wrapper around :func:`_mounted_device` that discards the
    device identity. Kept as a separate helper so existing callers
    (and tests) that only care about "is anything mounted here?" stay
    boolean-shaped.
    """
    return _mounted_device(target, mounts_path=mounts_path) is not None


def ensure_partition_mounted(
    partlabel: str,
    target: Path,
    *,
    fstype: str,
    opts: str,
    runner: Runner = _default_runner,
    mounts_path: Path = DEFAULT_MOUNTS_PATH,
    partlabel_base: Path = DEFAULT_PARTLABEL_BASE,
    timeout_s: float = DEFAULT_MOUNT_TIMEOUT_S,
) -> None:
    """Ensure the partition labeled ``partlabel`` is mounted at ``target``.

    Idempotent. If ``target`` is already a mountpoint per ``mounts_path``,
    the device backing it is read via :func:`_mounted_device` and
    compared byte-for-byte against the expected
    ``<partlabel_base>/<partlabel>`` path. On match: no-op. On
    mismatch: raise :class:`StagingError` immediately with both device
    names — refusing to extract a bundle onto whatever happens to be
    mounted there.

    Otherwise (no mountpoint):

    1. Create ``target`` (and any missing parents) as a directory.
    2. Invoke ``mount(8)`` via ``runner`` to mount
       ``<partlabel_base>/<partlabel>`` at ``target`` with the supplied
       fstype and options.

    Raises :class:`StagingError` on any of:
    - mismatched device at an already-mounted ``target``,
    - ``mount(8)`` returning non-zero (with stderr in the message),
    - ``mount(8)`` binary missing from PATH,
    - ``mount(8)`` exceeding ``timeout_s``.

    The partlabel-mismatch check fixes Bug 2 of the v0.0.18-test
    brick: in that incident the active boot partition (``boot-B``,
    GPT label) was mounted at ``/boot/firmware`` (the canonical
    ACTIVE boot mountpoint), but the OTA stager treated
    ``/boot/firmware`` as the **slot A** boot mount because of Bug
    1's slot-keyed lookup, and dispatched the inactive-boot extract
    onto the active slot. With partlabel verification, the apply step
    would have aborted at ``ensure_partition_mounted("boot-A",
    /boot/firmware)`` because the actual device at that mountpoint
    was ``/dev/disk/by-partlabel/boot-B``.

    Defense-in-depth from the v0.0.7-test brick is preserved: if
    agora-os didn't ship systemd .mount units for ``boot-B`` and
    ``root-B``, this function still re-runs ``mount(8)`` after
    finding no mountpoint at ``target``.
    """
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    expected_device = (Path(partlabel_base) / partlabel).resolve()
    actual_device = _mounted_device(target, mounts_path=mounts_path)
    if actual_device is not None:
        try:
            actual_resolved = actual_device.resolve()
        except OSError:
            actual_resolved = actual_device
        if actual_resolved != expected_device:
            raise StagingError(
                f"partition mismatch at {target}: expected "
                f"{expected_device} (partlabel {partlabel}) but "
                f"{actual_device} is mounted there. Refusing to "
                "extract bundle onto the wrong partition; check the "
                "device's fstab and slot identification logic."
            )
        logger.info(
            "partition %s already mounted at %s, skipping mount",
            partlabel,
            target,
        )
        return
    device = Path(partlabel_base) / partlabel
    cmd = ["mount", "-t", fstype, "-o", opts, str(device), str(target)]
    logger.info(
        "mounting partition: partlabel=%s device=%s target=%s fstype=%s opts=%s",
        partlabel,
        device,
        target,
        fstype,
        opts,
    )
    try:
        result = runner(cmd, timeout=timeout_s)
    except FileNotFoundError as exc:
        raise StagingError(
            f"mount(8) binary not found on PATH while mounting {partlabel}: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise StagingError(
            f"mount(8) timed out after {timeout_s}s while mounting "
            f"{partlabel} at {target}"
        ) from exc
    if result.returncode != 0:
        raise StagingError(
            f"failed to mount {partlabel} at {target}: "
            f"rc={result.returncode} stderr={_tail(result.stderr)!r}"
        )
    logger.info("mounted partition %s at %s", partlabel, target)


#: Type alias for the pipeline-runner seam — exists so tests can inject
#: a fake without touching ``subprocess.Popen``. Signature mirrors
#: :func:`_default_pipeline_runner`. Returns
#: ``(zstd_rc, zstd_stderr, tar_rc, tar_stderr)``.
PipelineRunner = Callable[
    [Sequence[str], Sequence[str]],
    tuple[int, str, int, str],
]


def _default_pipeline_runner(
    zstd_argv: Sequence[str],
    tar_argv: Sequence[str],
    *,
    tar_stdout_path: Optional[Path] = None,
    timeout_s: float,
    pid_observer: Optional[Callable[[int], None]] = None,
) -> tuple[int, str, int, str]:
    """Run ``zstd_argv | tar_argv`` as a streaming pipeline.

    The streaming-decompress story (see plan.md §"OTA v0.0.7"): we need
    to pipe ``zstd -dc <bundle> | tar -xf - -C <dst> <subpath>`` directly
    onto the target partition without ever landing the 3.5 GB intermediate
    tar on ``/data``. Three passes per apply (meta-only, boot/, root/)
    each re-read the .tar.zst from disk; this helper runs one pass.

    If ``tar_stdout_path`` is set, ``tar``'s stdout is redirected to that
    file (used by ``tar -O`` for meta-only extract — the manifest member
    is written to stdout instead of disk).

    If ``pid_observer`` is set, it's invoked exactly once with zstd's
    PID immediately after :func:`subprocess.Popen` returns. The bytes-
    progress poller (:func:`_poll_extract_progress`) uses this to look
    up ``/proc/<pid>/fdinfo/0`` for the compressed-bytes-consumed
    counter. Observer exceptions are caught so a buggy observer cannot
    bring down an OTA.

    Always returns ``(zstd_rc, zstd_stderr, tar_rc, tar_stderr)`` so the
    caller decides how to classify failure (zstd-side ⇒ corrupted bytes
    ⇒ ``BundleIntegrityError``; tar-side likewise).

    Raises :class:`FileNotFoundError` if either binary is missing on
    PATH (callers map to ``BundleIntegrityError``). Raises
    :class:`subprocess.TimeoutExpired` on per-pass timeout (callers
    likewise).
    """
    stdout_fp = None
    zstd_proc: Optional[subprocess.Popen] = None
    tar_proc: Optional[subprocess.Popen] = None
    try:
        zstd_proc = subprocess.Popen(
            list(zstd_argv),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if pid_observer is not None:
            try:
                pid_observer(zstd_proc.pid)
            except Exception:  # noqa: BLE001 — observer is advisory
                logger.exception(
                    "pid_observer raised; continuing pipeline"
                )
        try:
            if tar_stdout_path is not None:
                stdout_fp = open(tar_stdout_path, "wb")
                tar_stdout: Any = stdout_fp
            else:
                tar_stdout = subprocess.DEVNULL
            tar_proc = subprocess.Popen(
                list(tar_argv),
                stdin=zstd_proc.stdout,
                stdout=tar_stdout,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError:
            # tar binary missing — tear down the zstd we already started.
            zstd_proc.kill()
            try:
                zstd_proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            raise

        # Close our handle to zstd's stdout so tar sees EOF when zstd exits.
        assert zstd_proc.stdout is not None
        zstd_proc.stdout.close()

        try:
            _tar_stdout, tar_stderr = tar_proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            tar_proc.kill()
            zstd_proc.kill()
            try:
                tar_proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            try:
                zstd_proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            raise

        # tar exited (success or fail) — drain zstd's stderr & wait.
        try:
            _zstd_stdout, zstd_stderr = zstd_proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            zstd_proc.kill()
            _zstd_stdout, zstd_stderr = zstd_proc.communicate()

        return (
            zstd_proc.returncode if zstd_proc.returncode is not None else -1,
            zstd_stderr or "",
            tar_proc.returncode if tar_proc.returncode is not None else -1,
            tar_stderr or "",
        )
    finally:
        if stdout_fp is not None:
            try:
                stdout_fp.close()
            except OSError:
                pass


def stream_extract_subtree(
    bundle_path: Path,
    subpath: str,
    dst_dir: Path,
    *,
    pipeline_runner: Callable[..., tuple[int, str, int, str]] = _default_pipeline_runner,
    zstd_long: int = DEFAULT_ZSTD_LONG,
    timeout_s: float = DEFAULT_STREAM_TIMEOUT_S,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    compressed_size: Optional[int] = None,
    poller: Callable[..., None] = _poll_extract_progress,
) -> None:
    """Stream-extract ``<subpath>/`` from ``bundle.tar.zst`` directly into ``dst_dir``.

    Eliminates the old two-step land-on-/data flow (decompress to
    ``bundle.tar`` then ``tar -xf``) that consumed ~8 GB peak. Uses
    ``--strip-components=1`` so ``boot/foo`` lands at ``<dst_dir>/foo``.
    Anything outside ``<subpath>/`` in the archive is naturally filtered
    by the trailing argument to ``tar``.

    ``dst_dir`` is created if missing. Raises
    :class:`BundleIntegrityError` on any zstd or tar failure (corrupted
    bytes, missing binary, timeout). Bundle bytes → unusable, same wire-code
    rationale as for the meta-only extractor.

    When both ``progress_callback`` and ``compressed_size`` are
    provided, a sidecar thread polls ``/proc/<zstd_pid>/fdinfo/0`` at
    most every ``PROGRESS_MIN_INTERVAL_S`` seconds and emits
    ``(bytes_done, compressed_size)`` callbacks throughout the pass.
    The poller exits as soon as the pipeline returns (success OR
    failure) so a failed extract never leaks a thread. After a
    successful return the caller's callback is fired one last time with
    ``(compressed_size, compressed_size)`` so the badge always reaches
    100% before the next FSM event clears it. ``poller`` is overridable
    for tests; default points at :func:`_poll_extract_progress`.
    """
    bundle_path = Path(bundle_path)
    dst_dir = Path(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    zstd_argv = ["zstd", "-dc", f"--long={zstd_long}", "-f", str(bundle_path)]
    # GNU tar's defaults are correct here when this code runs as root
    # (which the stager always does, via sudo): --same-owner restores
    # numeric uid/gid from the archive, and --same-permissions restores
    # full mode bits including setuid/setgid/sticky. The previous
    # two-step staging flow rsync'd from staging into the slot and
    # could afford --no-same-owner / --no-same-permissions; the
    # streaming flow extracts directly into the inactive slot's
    # rootfs, so those flags would silently strip setuid bits
    # (sudo, su, mount, passwd, ping, ...) and reset every file's
    # ownership to the running uid. See sslivins/agora#187.
    tar_argv = [
        "tar",
        "-x",
        "--strip-components=1",
        "-C",
        str(dst_dir),
        subpath,
    ]

    logger.info(
        "stream extract: bundle=%s subpath=%s dst=%s long=%d",
        bundle_path,
        subpath,
        dst_dir,
        zstd_long,
    )

    # Optional bytes-progress polling: only active when the caller wants
    # progress AND has handed us a non-zero compressed size to anchor
    # the percentage against. The sidecar thread reads
    # /proc/<zstd_pid>/fdinfo/0 'pos:' field, so it's a no-op outside
    # Linux (test fakes that swap pipeline_runner won't see polling).
    poll_thread: Optional[threading.Thread] = None
    stop_event: Optional[threading.Event] = None
    pid_holder: Optional[dict[str, Optional[int]]] = None
    pipeline_kwargs: dict[str, Any] = {"timeout_s": timeout_s}
    want_progress = (
        progress_callback is not None
        and compressed_size is not None
        and compressed_size > 0
    )
    if want_progress:
        assert progress_callback is not None  # for type narrowing
        assert compressed_size is not None
        stop_event = threading.Event()
        pid_holder = {"pid": None}

        def _pid_observer(p: int) -> None:
            pid_holder["pid"] = p  # type: ignore[index]

        pipeline_kwargs["pid_observer"] = _pid_observer
        poll_thread = threading.Thread(
            target=poller,
            args=(pid_holder, stop_event, compressed_size, progress_callback),
            name=f"extract-progress-{subpath}",
            daemon=True,
        )
        poll_thread.start()

    try:
        zstd_rc, zstd_stderr, tar_rc, tar_stderr = pipeline_runner(
            zstd_argv, tar_argv, **pipeline_kwargs
        )
    except FileNotFoundError as exc:
        # We don't know which binary was missing from the OSError alone;
        # the message usually names it. Surface it raw.
        raise BundleIntegrityError(
            f"zstd or tar binary not found on PATH: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BundleIntegrityError(
            f"stream extract timed out after {timeout_s}s: bundle={bundle_path} "
            f"subpath={subpath}"
        ) from exc
    finally:
        # Always tear down the polling thread before returning — a stuck
        # poller (e.g. PID never showed up, fdinfo unreadable on a
        # non-Linux test box) would otherwise leak across passes.
        if stop_event is not None:
            stop_event.set()
        if poll_thread is not None:
            poll_thread.join(timeout=5.0)

    if zstd_rc != 0:
        raise BundleIntegrityError(
            f"zstd decompress failed (rc={zstd_rc}): stderr={_tail(zstd_stderr)!r}"
        )
    if tar_rc != 0:
        raise BundleIntegrityError(
            f"tar extract failed (rc={tar_rc}): subpath={subpath} "
            f"stderr={_tail(tar_stderr)!r}"
        )

    # Final 100% emit so the badge always lands at the end of the pass
    # rather than wherever the last fdinfo poll happened to catch zstd.
    if want_progress and progress_callback is not None and compressed_size is not None:
        try:
            progress_callback(compressed_size, compressed_size)
        except Exception:  # noqa: BLE001 — advisory
            logger.exception(
                "extract progress final-emit raised; ignoring"
            )


def extract_meta_only(
    bundle_path: Path,
    dst_meta_path: Path,
    *,
    pipeline_runner: Callable[..., tuple[int, str, int, str]] = _default_pipeline_runner,
    zstd_long: int = DEFAULT_ZSTD_LONG,
    timeout_s: float = DEFAULT_STREAM_TIMEOUT_S,
    meta_member: str = "meta.json",
) -> None:
    """Stream-extract just ``meta.json`` from ``bundle.tar.zst`` to ``dst_meta_path``.

    Cheapest-possible early-rejection pass: we re-read the .tar.zst from
    disk and stop the tar stream as soon as the named member is emitted.
    ``tar -O <member>`` writes the member to stdout; we redirect stdout
    to a file via the pipeline runner.

    ``dst_meta_path``'s parent is created if missing. Raises
    :class:`BundleIntegrityError` on any zstd or tar failure.
    """
    bundle_path = Path(bundle_path)
    dst_meta_path = Path(dst_meta_path)
    dst_meta_path.parent.mkdir(parents=True, exist_ok=True)

    zstd_argv = ["zstd", "-dc", f"--long={zstd_long}", "-f", str(bundle_path)]
    tar_argv = ["tar", "-xOf", "-", meta_member]

    logger.info(
        "stream extract meta: bundle=%s member=%s dst=%s",
        bundle_path,
        meta_member,
        dst_meta_path,
    )
    try:
        zstd_rc, zstd_stderr, tar_rc, tar_stderr = pipeline_runner(
            zstd_argv,
            tar_argv,
            tar_stdout_path=dst_meta_path,
            timeout_s=timeout_s,
        )
    except FileNotFoundError as exc:
        raise BundleIntegrityError(
            f"zstd or tar binary not found on PATH: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BundleIntegrityError(
            f"meta-only extract timed out after {timeout_s}s: bundle={bundle_path}"
        ) from exc

    if zstd_rc != 0:
        raise BundleIntegrityError(
            f"zstd decompress failed (rc={zstd_rc}): stderr={_tail(zstd_stderr)!r}"
        )
    if tar_rc != 0:
        raise BundleIntegrityError(
            f"tar meta extract failed (rc={tar_rc}): member={meta_member} "
            f"stderr={_tail(tar_stderr)!r}"
        )


def substitute_fstab(root_target: Path, inactive_slot: int) -> None:
    """Write ``<root_target>/etc/fstab`` from ``etc/fstab.template``.

    Bundles ship ``etc/fstab.template`` (rendered by
    agora-os/image-build/customize-rootfs.sh) with a ``{{BOOT_PARTLABEL}}``
    placeholder; build-bundle.sh deletes ``etc/fstab`` so the placeholder
    can't accidentally hit a device. Apply-time substitutes the
    inactive-slot's boot partlabel and writes the final fstab.

    Raises :class:`FstabError` if the template is missing, the
    placeholder is absent, or the write fails.
    """
    root_target = Path(root_target)
    template_path = root_target / DEFAULT_FSTAB_TEMPLATE_REL
    fstab_path = root_target / DEFAULT_FSTAB_REL

    if not template_path.is_file():
        raise FstabError(
            f"fstab template missing at {template_path} "
            f"(bundle did not ship etc/fstab.template; rebuild with agora-os "
            f"PR #14 or later)"
        )

    try:
        template = template_path.read_text()
    except OSError as exc:
        raise FstabError(f"could not read fstab template {template_path}: {exc}") from exc

    if "{{BOOT_PARTLABEL}}" not in template:
        raise FstabError(
            f"fstab template at {template_path} missing {{{{BOOT_PARTLABEL}}}} "
            f"placeholder; cannot determine slot's boot partlabel"
        )

    boot_partlabel = "boot-A" if inactive_slot == 1 else "boot-B"
    fstab = template.replace("{{BOOT_PARTLABEL}}", boot_partlabel)

    logger.info(
        "substitute_fstab: slot=%d boot_partlabel=%s template=%s -> %s",
        inactive_slot,
        boot_partlabel,
        template_path,
        fstab_path,
    )
    try:
        fstab_path.parent.mkdir(parents=True, exist_ok=True)
        fstab_path.write_text(fstab)
    except OSError as exc:
        raise FstabError(f"could not write fstab to {fstab_path}: {exc}") from exc


def rewrite_cmdline(
    boot_target: Path,
    target_slot: int,
    *,
    cmdline_rel: str = DEFAULT_CMDLINE_REL,
) -> None:
    """Rewrite ``<boot_target>/cmdline.txt`` for the target slot.

    Bundles ship per-slot ``cmdline.txt`` files (cmdline-A.txt / cmdline-B.txt
    in build-bundle.sh) that reference ``root=PARTLABEL=root-{A,B}``. Apply-time
    we canonicalize the cmdline to be safe even if the bundle's per-slot files
    have drift between A and B (e.g., the well-known cmdline-B.txt ``rw``-flag
    typo): strip any existing ``root=`` token, set our own, and ensure ``rw``
    is present. The result is written back atomically over the existing file.

    Raises :class:`CmdlineError` if the file is missing or the write fails.
    """
    boot_target = Path(boot_target)
    cmdline_path = boot_target / cmdline_rel

    if not cmdline_path.is_file():
        raise CmdlineError(
            f"cmdline file missing at {cmdline_path} (bundle did not ship "
            f"boot/{cmdline_rel}; rebuild with agora-os PR #14 or later)"
        )

    try:
        content = cmdline_path.read_text()
    except OSError as exc:
        raise CmdlineError(f"could not read {cmdline_path}: {exc}") from exc

    target_partlabel = "root-A" if target_slot == 1 else "root-B"

    canonical = re.sub(r"\s*\broot=\S+", "", content).strip()
    tokens = canonical.split()
    if "rw" not in tokens:
        tokens.append("rw")
    rewritten = f"root=PARTLABEL={target_partlabel} " + " ".join(tokens) + "\n"

    logger.info(
        "rewrite_cmdline: slot=%d partlabel=%s path=%s",
        target_slot,
        target_partlabel,
        cmdline_path,
    )
    try:
        cmdline_path.write_text(rewritten)
    except OSError as exc:
        raise CmdlineError(f"could not write {cmdline_path}: {exc}") from exc


def rsync_tree(
    src_dir: Path,
    dst_dir: Path,
    *,
    runner: Runner = _default_runner,
    rsync_timeout_s: float = DEFAULT_RSYNC_TIMEOUT_S,
) -> None:
    """Mirror ``src_dir`` onto ``dst_dir`` with ``rsync -aHAX --delete``.

    Flags:

    * ``-a`` — archive mode (preserves perms, ownership, symlinks,
      times, etc.).
    * ``-H`` — preserve hard links.
    * ``-A`` — preserve ACLs.
    * ``-X`` — preserve extended attributes.
    * ``--delete`` — remove destination files not present in source.
      Critical: we want the inactive slot to exactly match the
      bundle's content, not be a union of bundle + whatever the
      previous version left there.
    * ``--numeric-ids`` — preserve uid/gid as numbers (the bundle's
      uid table may not exist on the device).

    Both ``src_dir`` and ``dst_dir`` are passed with trailing slashes
    so rsync mirrors content rather than nesting one inside the other.

    Raises :class:`RsyncError` on subprocess timeout, missing rsync
    binary, or non-zero exit code.
    """
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    src_arg = str(src_dir) + "/"
    dst_arg = str(dst_dir) + "/"

    logger.info("rsync: %s -> %s", src_arg, dst_arg)
    try:
        result = runner(
            [
                "rsync",
                "-aHAX",
                "--delete",
                "--numeric-ids",
                src_arg,
                dst_arg,
            ],
            timeout=rsync_timeout_s,
        )
    except FileNotFoundError as exc:
        raise RsyncError(f"rsync binary not found on PATH: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RsyncError(
            f"rsync timed out after {rsync_timeout_s}s: {src_arg} -> {dst_arg}"
        ) from exc
    if result.returncode != 0:
        raise RsyncError(
            f"rsync failed (rc={result.returncode}) "
            f"writing {dst_arg}: stderr={_tail(result.stderr)!r}"
        )


def copy_fleet_state(
    source_root: Path,
    dest_root: Path,
    *,
    runner: Runner = _default_runner,
    required: tuple[str, ...] = FLEET_STATE_REQUIRED,
    copy_if_present: tuple[str, ...] = FLEET_STATE_COPY_IF_PRESENT,
    cp_timeout_s: float = DEFAULT_FLEET_STATE_CP_TIMEOUT_S,
) -> None:
    """Copy per-device identity files from one rootfs into another.

    Implements step 8b of the apply flow (D60 in plan.md §"Phase 2").
    Each entry in ``required`` and ``copy_if_present`` is a path
    **relative** to the rootfs root (no leading slash). Patterns
    containing ``*`` are treated as globs evaluated against
    ``source_root``; literal patterns are required to exist as
    regular files / symlinks.

    Historically this was hardcoded ``slot_a_root`` → ``slot_b_root``
    for the rootfs identity copy. With the v0.0.19 fix, the same
    primitive is also used for ``active_boot_mount`` →
    ``inactive_boot_mount`` (copying ``autoboot.txt`` so the
    bootloader can still find a slot to boot after the inactive
    boot partition is wiped and re-extracted). Naming is generic
    (source/dest) to reflect that.

    Semantics:

    * **Required literal** — file must exist on ``source_root``.
      Raises :class:`FleetStateMissingError` (with ``path`` set to
      the relative pattern) otherwise.
    * **Required glob** — at least one match required. Zero matches
      raises :class:`FleetStateMissingError`.
    * **Copy-if-present literal/glob** — zero matches is fine and
      logged at INFO as ``fleet_state_skipped``.

    Each copy is executed as ``cp -a`` per file: the ``-a`` flag
    preserves uid/gid/mode/atime/mtime/xattrs/symlinks (matching
    rsync's ``-a`` semantics for the per-file case but without
    rsync's source-list ergonomics that don't fit a glob+literal
    mix). Parent directories on ``dest_root`` are created with mode
    0755 if missing.

    Any non-zero ``cp`` exit, missing-binary error, or timeout
    raises :class:`FleetStateWriteError`. Per the module-level "no
    cleanup on failure" policy, neither ``dest_root`` nor the
    staging directory is touched on failure — the device boots from
    its active slot on next reboot and the operator can forensic
    the partial copy.
    """
    source_root = Path(source_root)
    dest_root = Path(dest_root)

    for pattern in required:
        sources = _resolve_fleet_state_sources(source_root, pattern)
        if not sources:
            raise FleetStateMissingError(
                f"required fleet-state entry not present at source: "
                f"{pattern!r} (resolved under {source_root})",
                path=pattern,
            )
        for src in sources:
            _cp_one(
                src,
                source_root,
                dest_root,
                pattern,
                runner=runner,
                cp_timeout_s=cp_timeout_s,
            )

    for pattern in copy_if_present:
        sources = _resolve_fleet_state_sources(source_root, pattern)
        if not sources:
            logger.info(
                "fleet_state_skipped: %r (no match under %s)",
                pattern,
                source_root,
            )
            continue
        for src in sources:
            _cp_one(
                src,
                source_root,
                dest_root,
                pattern,
                runner=runner,
                cp_timeout_s=cp_timeout_s,
            )


def _resolve_fleet_state_sources(source_root: Path, pattern: str) -> list[Path]:
    """Return the list of source paths matched by ``pattern``.

    Glob patterns (containing ``*``) are evaluated against
    ``source_root`` via :meth:`Path.glob`; literal patterns return
    ``[source_root / pattern]`` if the path exists, else ``[]``.
    Symlinks count as existing — ``cp -a`` will preserve them.
    """
    if "*" in pattern:
        return sorted(source_root.glob(pattern))
    candidate = source_root / pattern
    if candidate.exists() or candidate.is_symlink():
        return [candidate]
    return []


def _cp_one(
    src: Path,
    source_root: Path,
    dest_root: Path,
    pattern: str,
    *,
    runner: Runner,
    cp_timeout_s: float,
) -> None:
    """Run ``cp -a <src> <dst>`` for a single fleet-state file.

    Computes ``dst`` by re-rooting ``src`` from ``source_root`` onto
    ``dest_root`` (preserving the in-rootfs path). Creates the
    parent directory on ``dest_root`` if missing. Raises
    :class:`FleetStateWriteError` on any subprocess failure.
    """
    rel = src.relative_to(source_root)
    dst = dest_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)

    # If ``src`` is a directory (e.g. ``home/agora/.ssh``) and ``dst``
    # already exists on dest_root from a prior partial apply, remove it
    # first so ``cp -a`` writes AT ``dst`` rather than INTO ``dst`` —
    # i.e. avoids creating ``dest_root/home/agora/.ssh/.ssh``. Safe
    # because copy_fleet_state is the sole source of truth for the
    # fleet-state entries on dest_root.
    if src.is_dir() and dst.exists():
        shutil.rmtree(dst)

    # ``rel.as_posix()`` keeps the telemetry-bound path forward-slash
    # regardless of host platform — devices are Linux but tests run
    # on Windows.
    rel_posix = rel.as_posix()

    logger.info("fleet_state_copy: %s -> %s", src, dst)
    try:
        result = runner(["cp", "-a", str(src), str(dst)], timeout=cp_timeout_s)
    except FileNotFoundError as exc:
        raise FleetStateWriteError(
            f"cp binary not found on PATH while copying {src} -> {dst}: {exc}",
            path=rel_posix,
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise FleetStateWriteError(
            f"cp timed out after {cp_timeout_s}s: {src} -> {dst}",
            path=rel_posix,
        ) from exc
    if result.returncode != 0:
        raise FleetStateWriteError(
            f"cp failed (rc={result.returncode}) writing {dst}: "
            f"stderr={_tail(result.stderr)!r}",
            path=rel_posix,
        )


# ── slot_mgr injection seams ───────────────────────────────────────────────


def _default_slot_state() -> Any:
    """Lazy-import :func:`slot_mgr.slot_state` and return its result.

    Mirrors :func:`precheck.core._default_slot_state` — keeps the
    dependency edge visible in code review and lets tests stub
    ``slot_state_fn=`` cleanly without monkey-patching the slot_mgr
    namespace.

    The real return value is a :class:`slot_mgr.SlotStatus` dataclass
    with ``running_slot: Optional[int]`` (None if /proc/cmdline doesn't
    contain a ``root=PARTLABEL=root-{A,B}`` token).
    """
    from slot_mgr import slot_state

    return slot_state()


def _default_trigger_tryboot(target_slot: int) -> Any:
    """Lazy-import :func:`slot_mgr.trigger_tryboot` and call it.

    Lazy import mirrors :func:`_default_slot_state`. The real call
    rewrites ``[tryboot] boot_partition`` in ``autoboot.txt``, records
    ``last_tryboot_target`` in ``slot-state.json``, then executes
    ``sudo reboot '0 tryboot'``. Returns the updated
    :class:`slot_mgr.SlotState` for logging.
    """
    from slot_mgr import trigger_tryboot

    return trigger_tryboot(target_slot)


# ── Stager ─────────────────────────────────────────────────────────────────


@dataclass
class SlotStager:
    """Concrete :class:`os_updater.service.Stager` implementation.

    All collaborators are injectable for tests. Defaults wire up the
    production paths (slot_mgr.slot_state, ``/boot/firmware{,-b}``,
    ``/mnt/inactive-root``, real :func:`slot_mgr.trigger_tryboot`).

    The orchestration is synchronous; :meth:`stage` wraps it in
    :func:`asyncio.to_thread` to keep the daemon's event loop
    responsive during the multi-minute decompress + extract + rsync
    phase, matching the pattern in :class:`SignatureVerifier`.
    """

    runner: Runner = field(default=_default_runner)
    bundle_filename: str = DEFAULT_BUNDLE_FILENAME
    boot_subdir: str = "boot"
    root_subdir: str = "root"
    meta_filename: str = "meta.json"
    #: Mountpoint of the ACTIVE slot's boot partition — always
    #: ``/boot/firmware`` on a healthy device per agora-os fstab
    #: convention (regardless of whether the active slot is 1 or 2).
    #: Read-only source for boot-fleet-state copy (``autoboot.txt``);
    #: never written by the stager.
    active_boot_mount: Path = field(default_factory=lambda: DEFAULT_ACTIVE_BOOT_MOUNT)
    #: Mountpoint of the INACTIVE slot's boot partition — always
    #: ``/boot/firmware-b`` on a healthy device per agora-os fstab
    #: convention. Destination for the boot-subtree extract +
    #: boot-fleet-state copy.
    inactive_boot_mount: Path = field(default_factory=lambda: DEFAULT_INACTIVE_BOOT_MOUNT)
    inactive_root_mount: Path = field(default_factory=lambda: DEFAULT_INACTIVE_ROOT_MOUNT)
    zstd_long: int = DEFAULT_ZSTD_LONG
    stream_timeout_s: float = DEFAULT_STREAM_TIMEOUT_S
    rsync_timeout_s: float = DEFAULT_RSYNC_TIMEOUT_S
    pipeline_runner: Callable[..., tuple[int, str, int, str]] = field(
        default=_default_pipeline_runner
    )
    slot_state_fn: Optional[Callable[[], Any]] = None
    trigger_tryboot_fn: Optional[Callable[[int], Any]] = None
    #: Mountpoint of the running rootfs — always ``/`` on a real
    #: device, overridable in tests. Read-only source for the
    #: rootfs-fleet-state copy (machine-id, ssh host keys, etc.);
    #: never written by the stager.
    running_root: Path = field(default_factory=lambda: DEFAULT_RUNNING_ROOT)
    fleet_state_required: tuple[str, ...] = FLEET_STATE_REQUIRED
    fleet_state_copy_if_present: tuple[str, ...] = FLEET_STATE_COPY_IF_PRESENT
    #: Fleet-state files copied from the active boot partition into
    #: the inactive boot partition AFTER the boot subtree is wiped
    #: and re-extracted. Default = ``("autoboot.txt",)``; the bundle
    #: deliberately omits autoboot.txt because slot_mgr (not the
    #: stager) is the authority for which slot the bootloader
    #: should select. Without this copy, the bootloader has no way
    #: to find any slot — device bricks.
    boot_fleet_state_required: tuple[str, ...] = BOOT_FLEET_STATE_REQUIRED
    boot_fleet_state_copy_if_present: tuple[str, ...] = (
        BOOT_FLEET_STATE_COPY_IF_PRESENT
    )
    fleet_state_cp_timeout_s: float = DEFAULT_FLEET_STATE_CP_TIMEOUT_S
    fstab_template_rel: str = DEFAULT_FSTAB_TEMPLATE_REL
    fstab_rel: str = DEFAULT_FSTAB_REL
    cmdline_rel: str = DEFAULT_CMDLINE_REL
    #: Mount options for slot B's boot (FAT32) partition when this
    #: stager has to mount it itself in step 5. Tests do not exercise
    #: this knob; production matches the fstab template shipped by
    #: agora-os.
    boot_mount_opts: str = DEFAULT_BOOT_MOUNT_OPTS
    #: Mount options for slot B's root (ext4) partition when this
    #: stager has to mount it itself in step 5.
    root_mount_opts: str = DEFAULT_ROOT_MOUNT_OPTS
    #: Per-call timeout for the ``mount(8)`` invocations in step 5.
    mount_timeout_s: float = DEFAULT_MOUNT_TIMEOUT_S
    #: Path to the kernel mount table consulted by step 5 to detect
    #: an already-mounted target. Tests inject a fixture file.
    mounts_path: Path = field(default_factory=lambda: DEFAULT_MOUNTS_PATH)
    #: Base path to ``/dev/disk/by-partlabel/`` consulted by step 5
    #: to resolve ``boot-A``/``boot-B``/``root-A``/``root-B`` to a
    #: device node. Tests inject a tmp path.
    partlabel_base: Path = field(default_factory=lambda: DEFAULT_PARTLABEL_BASE)

    async def stage(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[str], None]] = None,
        extract_progress_callback: Optional[
            Callable[[str, int, int], None]
        ] = None,
    ) -> None:
        """Run the full stage-and-tryboot pipeline (10-step streaming orchestration).

        Async entrypoint matching the :class:`Stager` protocol. The
        synchronous body lives in :meth:`_stage_sync` for ease of
        testing without an event loop.

        ``progress_callback``, if provided, is invoked with each phase
        name from :data:`STAGE_PROGRESS_PHASES` at that phase's
        boundary (before the phase's work begins). Callback exceptions
        are caught and logged — they do not interrupt the stage.

        ``extract_progress_callback``, if provided, is invoked during
        the two long ``stream_extract_subtree`` passes with
        ``(phase, bytes_done, compressed_size)`` where ``phase`` is
        either ``"extracting_boot"`` or ``"extracting_rootfs"``. Rate-
        limited to ``PROGRESS_MIN_INTERVAL_S`` cadence by the polling
        thread. Final ``(compressed_size, compressed_size)`` is fired
        at the end of each pass so the badge always lands at 100%.
        Callback exceptions are caught and logged.
        """
        await asyncio.to_thread(
            self._stage_sync,
            payload,
            staging_dir,
            progress_callback=progress_callback,
            extract_progress_callback=extract_progress_callback,
        )

    def _stage_sync(
        self,
        payload: DispatchPayload,
        staging_dir: Path,
        *,
        progress_callback: Optional[Callable[[str], None]] = None,
        extract_progress_callback: Optional[
            Callable[[str, int, int], None]
        ] = None,
    ) -> None:
        staging_dir = Path(staging_dir)
        bundle_path = staging_dir / self.bundle_filename
        meta_path = staging_dir / self.meta_filename

        logger.info(
            "staging bundle (streaming): release=%s target_version=%s "
            "bundle=%s staging=%s",
            payload.release_id,
            payload.target_version,
            bundle_path,
            staging_dir,
        )

        # 1. Meta-only extract — cheapest target_version reject before
        # we touch the inactive partitions. ~30s zstd pass with tar -O.
        _safe_emit_progress(progress_callback, "extracting_meta")
        extract_meta_only(
            bundle_path,
            meta_path,
            pipeline_runner=self.pipeline_runner,
            zstd_long=self.zstd_long,
            timeout_s=self.stream_timeout_s,
            meta_member=self.meta_filename,
        )

        # 2. Parse meta.json (raises BundleIntegrityError on any issue).
        meta = parse_bundle_meta(meta_path)
        logger.info(
            "bundle meta parsed: release=%s meta.version=%s "
            "meta.schema_version=%d manifest_entries=%d",
            payload.release_id,
            meta.version,
            meta.schema_version,
            len(meta.sha256_manifest),
        )

        # 3. Defense-in-depth target_version match — abort BEFORE we've
        # written anything to slot B.
        if meta.version != payload.target_version:
            raise BundleIntegrityError(
                f"bundle meta.version={meta.version!r} does not match "
                f"dispatch payload target_version={payload.target_version!r}"
            )

        # 4. Determine running + inactive slots via slot_mgr.
        running = self._read_running_slot()
        inactive = other_slot(running)
        logger.info(
            "slot detection: running=%d inactive=%d (release=%s)",
            running,
            inactive,
            payload.release_id,
        )

        # 5. Ensure the inactive slot's boot + root partitions are
        # mounted at their canonical mountpoints. Per agora-os fstab
        # convention the INACTIVE slot's boot is always
        # ``/boot/firmware-b`` and its root always at
        # ``/mnt/inactive-root``, regardless of whether the inactive
        # slot is 1 or 2. (The pre-v0.0.19 ``boot_mount_for_slot``
        # lookup was slot-keyed; if Bug 1 had not been fixed, this
        # would have resolved to ``/boot/firmware`` when the inactive
        # slot was 1, which is the ACTIVE slot's mountpoint —
        # bricking the device by extracting the bundle onto the
        # running slot. See v0.0.18-test post-mortem.)
        #
        # Defense-in-depth (see plan.md §"OTA v0.0.7-test mount
        # gap"): early agora-os images did not ship systemd
        # ``.mount`` units for ``/boot/firmware-b`` and
        # ``/mnt/inactive-root``, so the mountpoints were empty
        # rootfs directories and step 6/7's stream-extract would
        # silently write the bundle onto the **active** rootfs (slot
        # A), leaving slot B stale. ``ensure_partition_mounted`` is
        # idempotent — if a systemd unit already mounted the
        # partition (the agora-os 0.0.8+ path), this is a no-op,
        # AND it now verifies the correct partlabel is mounted
        # there (Bug 2 fix).
        _safe_emit_progress(progress_callback, "mounting_inactive")
        boot_target = Path(self.inactive_boot_mount)
        root_target = Path(self.inactive_root_mount)
        ensure_partition_mounted(
            boot_partlabel_for_slot(inactive),
            boot_target,
            fstype="vfat",
            opts=self.boot_mount_opts,
            runner=self.runner,
            mounts_path=self.mounts_path,
            partlabel_base=self.partlabel_base,
            timeout_s=self.mount_timeout_s,
        )
        ensure_partition_mounted(
            root_partlabel_for_slot(inactive),
            root_target,
            fstype="ext4",
            opts=self.root_mount_opts,
            runner=self.runner,
            mounts_path=self.mounts_path,
            partlabel_base=self.partlabel_base,
            timeout_s=self.mount_timeout_s,
        )

        # 5b. Wipe the inactive slot's boot + root partitions before
        # extracting the bundle. Without this, an old release's files
        # that are NOT in the new bundle's manifest survive into the
        # newly-extracted slot and quietly contaminate it. The
        # streaming extract overwrites same-path files but leaves
        # orphans alone — this is how v0.0.18-test left a stale
        # ``bcm2712-rpi-5-b.dtb`` + companion overlays on the boot
        # partition that pointed the bootloader at a kernel the new
        # rootfs no longer matched. Wiping is a destructive, slot-B-
        # only operation; the staging runner aborts before this if
        # the wrong device is mounted at either target (see Bug 2
        # fix in ``ensure_partition_mounted``).
        _safe_emit_progress(progress_callback, "wiping_inactive")
        self._wipe_inactive_partitions(boot_target, root_target)

        # 6. Stream boot/ subtree straight onto the inactive boot partition.
        # --strip-components=1 turns "boot/foo" into "<boot_target>/foo" and
        # naturally filters anything not under boot/.
        _safe_emit_progress(progress_callback, "extracting_boot")
        try:
            compressed_size = bundle_path.stat().st_size
        except OSError:
            # Bundle disappeared between meta-extract and now — let the
            # real pipeline runner surface the failure; the size lookup
            # is only used to anchor progress and is purely advisory.
            compressed_size = 0
        boot_progress: Optional[Callable[[int, int], None]] = None
        root_progress: Optional[Callable[[int, int], None]] = None
        if extract_progress_callback is not None and compressed_size > 0:
            def _on_boot_bytes(done: int, total: int) -> None:
                try:
                    extract_progress_callback("extracting_boot", done, total)
                except Exception:  # noqa: BLE001 — advisory
                    logger.exception(
                        "extract_progress_callback raised; continuing extract"
                    )

            def _on_root_bytes(done: int, total: int) -> None:
                try:
                    extract_progress_callback("extracting_rootfs", done, total)
                except Exception:  # noqa: BLE001 — advisory
                    logger.exception(
                        "extract_progress_callback raised; continuing extract"
                    )

            boot_progress = _on_boot_bytes
            root_progress = _on_root_bytes

        stream_extract_subtree(
            bundle_path,
            self.boot_subdir,
            boot_target,
            pipeline_runner=self.pipeline_runner,
            zstd_long=self.zstd_long,
            timeout_s=self.stream_timeout_s,
            progress_callback=boot_progress,
            compressed_size=compressed_size if boot_progress else None,
        )

        # 7. Stream root/ subtree straight onto the inactive root partition.
        _safe_emit_progress(progress_callback, "extracting_rootfs")
        stream_extract_subtree(
            bundle_path,
            self.root_subdir,
            root_target,
            pipeline_runner=self.pipeline_runner,
            zstd_long=self.zstd_long,
            timeout_s=self.stream_timeout_s,
            progress_callback=root_progress,
            compressed_size=compressed_size if root_progress else None,
        )

        # 8. Manifest sha256 verification — hash the bytes that actually
        # landed on the partitions.
        _safe_emit_progress(progress_callback, "verifying_manifest")
        verify_bundle_manifest(
            {self.boot_subdir: boot_target, self.root_subdir: root_target},
            meta,
        )
        logger.info(
            "bundle manifest verified: release=%s files=%d",
            payload.release_id,
            len(meta.sha256_manifest),
        )

        # 9. Copy per-device fleet-state from the active slot into
        # the inactive slot. Done AFTER manifest verify so the
        # manifest check doesn't see files added by us. Two distinct
        # copies — the rootfs identity files (D60 of plan.md §"Phase
        # 2") and the boot-partition state (``autoboot.txt``,
        # required so the bootloader still has a slot to select
        # post-extract). See v0.0.18-test post-mortem.
        _safe_emit_progress(progress_callback, "copying_fleet_state")
        copy_fleet_state(
            self.running_root,
            root_target,
            runner=self.runner,
            required=self.fleet_state_required,
            copy_if_present=self.fleet_state_copy_if_present,
            cp_timeout_s=self.fleet_state_cp_timeout_s,
        )
        copy_fleet_state(
            self.active_boot_mount,
            boot_target,
            runner=self.runner,
            required=self.boot_fleet_state_required,
            copy_if_present=self.boot_fleet_state_copy_if_present,
            cp_timeout_s=self.fleet_state_cp_timeout_s,
        )

        # 10a. Substitute fstab template — bundles ship etc/fstab.template
        # with a {{BOOT_PARTLABEL}} placeholder; the device's etc/fstab
        # was deleted by build-bundle.sh. We render it for the inactive
        # slot here. Per plan.md §"v0.0.4-test bug list", this is critical
        # to avoid bricking the device on tryboot.
        _safe_emit_progress(progress_callback, "finalizing")
        substitute_fstab(root_target, inactive)

        # 10b. Rewrite cmdline.txt for the target slot — bundles ship per-
        # slot cmdline files but we canonicalize defensively (drop stray
        # root= tokens, force rw).
        rewrite_cmdline(boot_target, inactive, cmdline_rel=self.cmdline_rel)

        # 10c. Trigger tryboot. slot_mgr handles autoboot.txt rewrite +
        # state persistence + sudo reboot '0 tryboot'.
        trigger_fn = self.trigger_tryboot_fn or _default_trigger_tryboot
        logger.info(
            "triggering tryboot to slot %d (release=%s)",
            inactive,
            payload.release_id,
        )
        try:
            trigger_fn(inactive)
        except Exception as exc:  # noqa: BLE001 — slot_mgr's PinnedError etc.
            raise TrybootError(
                f"trigger_tryboot({inactive}) failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    def _wipe_inactive_partitions(self, boot_target: Path, root_target: Path) -> None:
        """Delete every entry under ``boot_target`` and ``root_target``.

        Run between mount (step 5) and extract (step 6) to clear out
        stale files from a prior release that are NOT in the new
        bundle's manifest. Without this, the streaming extract only
        overwrites same-path files and orphans survive — the exact
        contamination mode that bricked Pi100 on v0.0.18-test, where
        a stale ``bcm2712-rpi-5-b.dtb`` from the prior bundle pointed
        the bootloader at a kernel the new rootfs no longer matched.

        Uses ``find <target> -mindepth 1 -delete`` on the live mount
        so the mountpoint itself stays (preserving the device-node /
        FS — we are emphatically NOT formatting). ``-mindepth 1``
        keeps the mountpoint inode. The find binary handles
        FAT32-on-boot and ext4-on-root identically.

        Aborts with :class:`StagingError` on any subprocess failure;
        in particular this fails fast on EROFS, which would indicate
        the wrong partition is mounted (the active slot's root
        partition is mounted read-only on a healthy boot — wiping it
        would be catastrophic). The Bug 2 partlabel check in
        :func:`ensure_partition_mounted` should already have caught
        that case in step 5; this is defense-in-depth.
        """
        for label, target in (("boot", boot_target), ("root", root_target)):
            logger.info(
                "wiping inactive %s partition: %s (find -mindepth 1 -delete)",
                label,
                target,
            )
            try:
                result = self.runner(
                    ["find", str(target), "-mindepth", "1", "-delete"],
                    timeout=self.mount_timeout_s,
                )
            except FileNotFoundError as exc:
                raise StagingError(
                    f"find binary not found on PATH while wiping {target}: {exc}"
                ) from exc
            except subprocess.TimeoutExpired as exc:
                raise StagingError(
                    f"wipe of {target} timed out after {self.mount_timeout_s}s"
                ) from exc
            if result.returncode != 0:
                raise StagingError(
                    f"wipe of {target} failed (rc={result.returncode}): "
                    f"stderr={_tail(result.stderr)!r}"
                )

    def _read_running_slot(self) -> int:
        slot_state_fn = self.slot_state_fn or _default_slot_state
        try:
            status = slot_state_fn()
        except Exception as exc:  # noqa: BLE001 — slot_mgr exposes a few
            raise StagingError(
                f"could not read slot state: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        running = getattr(status, "running_slot", None)
        if running not in (1, 2):
            raise StagingError(
                f"slot_mgr could not determine running slot "
                f"(running_slot={running!r}); refusing to stage"
            )
        return running
