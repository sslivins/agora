"""Platform-agnostic helpers for the slideshow sequencer.

This module is intentionally pure-Python with no GLib / GStreamer /
mpv / Linux-path dependencies so it can be re-used unchanged by the
agora-softplayer (Windows host) and any future non-Pi platform that
runs the same chromium-shell slideshow logic.

Phase 4 of agora#226 lifts these in two steps:

* PR-1 — pure helpers (``parse_schema_version``, ``is_forward_schema``,
  ``parse_iso8601_utc``, ``locate_slide_at``) + constants.
* PR-2 — adds ``read_slideshow_manifest`` (file IO + parse + digest)
  and ``resolve_anchored_target`` (pure anchor → target-slide math
  with an ``AnchorStatus`` enum for callers to drive their own
  telemetry).

The full slideshow state machine itself stays in ``player/service.py``
for now — extracting it requires a Renderer/Timer/StateSink protocol
design with broader test churn and is tracked as a future epic.

The helpers are also re-exported from ``player/service.py`` as
module-level aliases (``_parse_schema_version``, ``_is_forward_schema``,
``_parse_iso8601_utc``, ``_locate_slide_at``, ``_CLOCK_SKEW_TOLERANCE_S``,
``_RESYNC_CAP_MS``) so existing imports and tests keep working without
modification.
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


# ── Manifest schema-version helpers (agora#226 Phase 0) ──

#: Highest manifest_schema_version this player understands.  Manifests
#: with a higher version are still played back (best-effort) but log a
#: one-shot "CMS is ahead of this player" INFO message.  Older versions
#: are accepted unconditionally.
PLAYER_MAX_MANIFEST_SCHEMA_VERSION = "1.5"


def parse_schema_version(version: str) -> tuple[int, int]:
    """Parse a ``major.minor`` (or ``major.minor.patch``) version string.

    Returns ``(major, minor)``.  Anything unparseable is treated as
    ``(0, 0)`` — i.e. older than every real version — so a corrupted
    or unexpected value never trips the "forward" comparison.  We only
    care about major+minor; patch-level differences are below the
    granularity of the manifest schema.
    """
    try:
        parts = version.split(".")
        return int(parts[0]), int(parts[1])
    except (AttributeError, IndexError, ValueError):
        return (0, 0)


def is_forward_schema(observed: str, player_max: str) -> bool:
    """True iff ``observed`` is strictly greater than ``player_max``.

    Used to decide whether to log the once-per-manifest "CMS is ahead of
    this player" INFO message.  Equal versions are NOT forward.
    """
    return parse_schema_version(observed) > parse_schema_version(player_max)


# ── Wall-clock anchor helpers (agora#226 Phase 2) ──

#: How far in the past we'll tolerate the wall clock being relative to
#: a manifest anchor before assuming NTP hasn't converged yet.  A Pi
#: without an RTC boots near the Unix epoch; once NTP syncs, the clock
#: jumps forward by several decades.  We don't want to drive playback
#: from a 1970 timestamp.
CLOCK_SKEW_TOLERANCE_S = 3600  # 1h

#: The biggest gap we ever sleep between resync evaluations.  The "next
#: advance" timer is always armed at min(remaining_ms, RESYNC_CAP_MS)
#: so we re-check the anchor at least this often — that's how a Pi
#: that fell behind catches up without needing a separate watchdog
#: tick.
RESYNC_CAP_MS = 5000


def parse_iso8601_utc(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 UTC timestamp (``...Z`` or ``+00:00`` suffix).

    Returns ``None`` if the value is missing, the wrong type, or
    unparseable — callers must treat ``None`` as "no anchor available"
    and fall back to legacy relative-timer playback.  We intentionally
    do not raise; bad manifest data should not crash the player.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        # ``fromisoformat`` accepts ``+00:00`` but not the ``Z`` suffix
        # until 3.11.  Normalize the suffix.
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def locate_slide_at(
    elapsed_ms: int, slides: list[dict],
) -> tuple[int, int]:
    """Locate the active slide given ``elapsed_ms`` into the cycle.

    Returns ``(target_idx, remaining_ms)`` where ``target_idx`` is the
    0-based slide index that should be on screen right now, and
    ``remaining_ms`` is the wall-clock time until the next slide
    boundary (always > 0 and <= that slide's duration_ms).

    ``elapsed_ms`` is normalized into ``[0, cycle_duration_ms)``, so
    negative values (clock skew) or values past the cycle wrap around
    correctly.  Empty slide lists raise ``ValueError`` — callers must
    not invoke the anchored path on a degenerate manifest.

    Slides with ``duration_ms <= 0`` are treated as 0-duration: the
    function still returns a valid ``(idx, remaining_ms)`` by walking
    past them.  If every slide is 0-duration the function returns
    ``(0, 1)`` so the caller schedules a 1ms re-evaluation rather than
    blocking forever.
    """
    if not slides:
        raise ValueError("locate_slide_at: slides must be non-empty")
    durations = [max(int(s.get("duration_ms") or 0), 0) for s in slides]
    cycle_ms = sum(durations)
    if cycle_ms <= 0:
        return (0, 1)
    pos = elapsed_ms % cycle_ms
    if pos < 0:
        pos += cycle_ms
    for idx, dur in enumerate(durations):
        if dur <= 0:
            continue
        if pos < dur:
            remaining = dur - pos
            if remaining <= 0:
                remaining = dur
            return (idx, remaining)
        pos -= dur
    # ``pos < cycle_ms`` guarantees this is unreachable, but a defensive
    # fallback keeps mypy happy and prevents UB on a bug elsewhere.
    return (len(slides) - 1, max(durations[-1], 1))


# ── Deck shuffle (agora#261, manifest schema 1.4) ──


def cycle_index_at(elapsed_ms: int, cycle_duration_ms: int) -> int:
    """Return the 0-based cycle ordinal for ``elapsed_ms`` into playback.

    ``elapsed_ms`` is ``now - anchor`` in milliseconds (may be negative
    under small clock skew within tolerance). Cycle 0 is the first pass
    through the deck, cycle 1 the second, etc. Floor division matches
    :func:`locate_slide_at`'s ``elapsed_ms % cycle_ms`` so the index and
    the in-cycle position are computed from the same wall clock and never
    disagree at a boundary.

    ``cycle_duration_ms <= 0`` (degenerate deck) returns ``0`` — the
    caller should not be on the anchored/shuffle path in that case, but a
    defensive constant keeps the seed stable rather than dividing by zero.
    """
    if cycle_duration_ms <= 0:
        return 0
    return elapsed_ms // cycle_duration_ms


def ordered_slides_for_cycle(
    base_slides: list[dict],
    shuffle: bool,
    shuffle_seed: int,
    cycle_index: int,
) -> list[dict]:
    """Return the slide order to play for cycle ``cycle_index``.

    Pure and deterministic: every device that shares ``shuffle_seed`` (the
    CMS emits a stable per-asset seed in the manifest) and computes the
    same ``cycle_index`` produces an identical permutation, so a fleet
    stays frame-for-frame in sync even though the order is "random".

    * ``shuffle`` False, or fewer than two slides → ``base_slides``
      order unchanged (returned as a new list; inputs are never mutated).
    * Otherwise a per-cycle seeded Fisher–Yates (``random.Random.shuffle``)
      permutes a fresh copy. The seed mixes ``shuffle_seed`` and
      ``cycle_index`` so consecutive cycles re-shuffle rather than
      repeating the same order, while staying reproducible.

    The total cycle duration is order-independent (the anchored engine
    sums per-slide durations), so permuting within a cycle never shifts
    cycle boundaries — :func:`cycle_index_at` stays valid across the
    permutation.
    """
    if not shuffle or len(base_slides) < 2:
        return list(base_slides)
    # Deterministic integer seed; bit-mix keeps adjacent cycles distinct.
    seed = (int(shuffle_seed) & 0x7FFFFFFF) * 0x9E3779B1 ^ int(cycle_index)
    order = list(range(len(base_slides)))
    random.Random(seed).shuffle(order)
    return [base_slides[i] for i in order]


# ── Per-slide visibility windows (agora#226 Phase 2, manifest 1.5) ──

#: Safety net for the window-boundary wake timer in ``player/service.py``.
#: ``next_window_boundary`` returns the exact civil instant a slide flips
#: open/closed, but a caller never sleeps longer than this between
#: re-evaluations so a missed/!misjudged boundary self-heals within ~15min
#: even on a drifting clock.
WINDOW_BOUNDARY_MAX_SLEEP_S = 900


def _parse_window_date(value: object) -> Optional[date]:
    """Parse an ISO ``YYYY-MM-DD`` string → :class:`date`; fail-open to None."""
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_window_time(value: object) -> Optional[time]:
    """Parse an ISO ``HH:MM:SS[.ffffff]`` string → :class:`time`; fail-open."""
    if not isinstance(value, str):
        return None
    try:
        return time.fromisoformat(value)
    except ValueError:
        return None


def _parse_window_days(value: object) -> Optional[list[int]]:
    """Normalize ``active_days`` → sorted unique ints in [0,6], or None.

    Empty list, None, wrong type, or all-invalid entries → None
    (always-open for the weekday dimension).
    """
    if not isinstance(value, (list, tuple)):
        return None
    days = sorted({
        int(d) for d in value
        if isinstance(d, int) and not isinstance(d, bool) and 0 <= d <= 6
    })
    return days or None


def parse_window(slide: dict) -> dict:
    """Extract + normalize the per-slide visibility window off a manifest slide.

    Returns a dict with keys ``valid_from``/``valid_to`` (:class:`date` | None),
    ``active_days`` (sorted ``list[int]`` | None), and
    ``active_start``/``active_end`` (:class:`time` | None).

    **Fail-open per dimension:** any missing, wrong-typed, or unparseable
    field becomes ``None`` (always-open for that dimension) rather than
    crashing playback or stranding a slide.  A slide with no window fields
    yields an all-``None`` dict, which :func:`slide_window_open` treats as
    always visible — byte-identical to pre-1.5 behaviour.
    """
    return {
        "valid_from": _parse_window_date(slide.get("valid_from")),
        "valid_to": _parse_window_date(slide.get("valid_to")),
        "active_days": _parse_window_days(slide.get("active_days")),
        "active_start": _parse_window_time(slide.get("active_start")),
        "active_end": _parse_window_time(slide.get("active_end")),
    }


def slide_has_window(slide: dict) -> bool:
    """True iff ``slide`` carries at least one (well-formed) window constraint."""
    return any(parse_window(slide).values())


def slide_window_open(slide: dict, local_now: datetime) -> bool:
    """Return True iff ``slide``'s visibility window is open at ``local_now``.

    ``local_now`` is device-local civil time — on the Pi a naive
    ``datetime.now()`` (the OS timezone is set by cms_client, so the naive
    system clock already reads device-local wall time).

    Ported verbatim from the CMS resolver predicate
    (``cms.services.slideshow_resolver._slide_window_open``) so the firmware
    flips a slide open/closed at the exact same instant the server would.

    A slide is VISIBLE iff ALL configured constraints pass; any unset
    (None) constraint is "always open" for that dimension:

      * Date range ``[valid_from, valid_to]`` — both ends INCLUSIVE.
      * Weekday set ``active_days`` (0=Mon..6=Sun) — empty/None = all days.
      * Time-of-day ``[active_start, active_end)`` — start INCLUSIVE, end
        EXCLUSIVE; ``active_start > active_end`` is an overnight wrap.

    Wrap + weekday interaction: in the post-midnight tail of a wrapped
    window the effective weekday is *yesterday's* (the window belongs to
    the day it opened).
    """
    w = parse_window(slide)
    d = local_now.date()
    t = local_now.time()

    valid_from = w["valid_from"]
    valid_to = w["valid_to"]
    if valid_from is not None and d < valid_from:
        return False
    if valid_to is not None and d > valid_to:
        return False

    start = w["active_start"]
    end = w["active_end"]
    days = w["active_days"]

    if start is not None and end is not None and start > end and t < end:
        effective_wd = (local_now - timedelta(days=1)).weekday()
    else:
        effective_wd = local_now.weekday()

    if days and effective_wd not in days:
        return False

    if start is not None and end is not None:
        if start < end:
            if not (start <= t < end):
                return False
        else:  # overnight wrap
            if not (t >= start or t < end):
                return False
    elif start is not None:
        if t < start:
            return False
    elif end is not None:
        if t >= end:
            return False

    return True


def visible_slides(slides: list[dict], local_now: datetime) -> list[dict]:
    """Filter ``slides`` to those whose visibility window is open at
    ``local_now``, preserving order.  Windowless slides always pass.
    """
    return [s for s in slides if slide_window_open(s, local_now)]


def next_window_boundary(
    slides: list[dict], local_now: datetime,
) -> Optional[datetime]:
    """Earliest strictly-future civil instant at which the visible set may
    change, or ``None`` if no slide carries a window.

    Returns a naive local-civil :class:`datetime` (same clock domain as
    ``local_now``).  We over-generate candidates — a redundant wake is
    harmless because the caller re-evaluates :func:`visible_slides` on every
    wake — but never under-generate, so a boundary is never missed.

    Candidates per windowed slide:
      * ``valid_from`` @ 00:00 (opens that calendar day)
      * ``valid_to + 1 day`` @ 00:00 (inclusive end → closes next midnight)
      * today's + tomorrow's ``active_start`` / ``active_end``
      * midnight tomorrow for weekday-restricted slides (weekday rolls over)

    Deliberately uses civil ``datetime.combine`` candidates (never
    ``local_now + timedelta``) so a DST transition shifts the wake to the
    correct wall-clock instant instead of an absolute offset.
    """
    today = local_now.date()
    midnight_tomorrow = datetime.combine(today + timedelta(days=1), time(0, 0))
    candidates: list[datetime] = []
    has_window = False

    for s in slides:
        w = parse_window(s)
        if not any(w.values()):
            continue
        has_window = True
        valid_from = w["valid_from"]
        valid_to = w["valid_to"]
        start = w["active_start"]
        end = w["active_end"]
        days = w["active_days"]

        if valid_from is not None:
            candidates.append(datetime.combine(valid_from, time(0, 0)))
        if valid_to is not None:
            candidates.append(
                datetime.combine(valid_to + timedelta(days=1), time(0, 0))
            )
        for base in (today, today + timedelta(days=1)):
            if start is not None:
                candidates.append(datetime.combine(base, start))
            if end is not None:
                candidates.append(datetime.combine(base, end))
        if days is not None:
            candidates.append(midnight_tomorrow)

    future = [c for c in candidates if c > local_now]
    if future:
        return min(future)
    # Windowed slides exist but every computed boundary is in the past
    # (e.g. a permanently-closed expired slide): re-evaluate at the next
    # midnight as a daily safety net rather than never waking.
    if has_window:
        return midnight_tomorrow
    return None



def read_slideshow_manifest(
    assets_dir: Path, name: str,
) -> Optional[tuple[dict, str]]:
    """Read, parse, and digest a slideshow manifest from disk.

    Returns ``(parsed_dict, sha256_hex)`` or ``None`` if the manifest is
    missing, unreadable, malformed JSON, or has no slides list.  Pure
    w.r.t. ``assets_dir``: callers pass the directory they want to read
    from, so the same function serves both the Pi (``/opt/agora/assets``)
    and the softplayer (Windows app-data dir).

    The SHA-256 digest is used to detect manifest edits between
    apply_desired calls — manifest contents are not part of
    DesiredState, so without the digest a CMS-side edit is invisible to
    the player.

    Exceptions (``OSError``, ``json.JSONDecodeError``,
    ``UnicodeDecodeError``) are swallowed and turned into ``None`` so
    callers can take a "manifest missing → show splash" path without
    sprinkling try/except everywhere.
    """
    path = assets_dir / "slideshows" / f"{name}.json"
    try:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    slides = data.get("slides")
    if not isinstance(slides, list) or not slides:
        return None
    return data, hashlib.sha256(raw).hexdigest()


# ── Anchored-playback resolution (agora#226 Phase 2 + Phase 4 PR-2) ──


class AnchorStatus(Enum):
    """Outcome of evaluating whether anchored playback should drive the
    next slide dispatch.  ``OK`` means the caller should use
    ``AnchorResolution.target``; everything else means fall back to the
    legacy relative-timer chain.
    """

    OK = "ok"
    NO_ANCHOR = "no_anchor"            # schema < 1.1 or missing started_at
    DEGENERATE_CYCLE = "degenerate"    # no slides / total duration <= 0
    CLOCK_SKEW_BEHIND = "skew_behind"  # now < anchor - tolerance (pre-NTP)


@dataclass(frozen=True)
class AnchorResolution:
    """Result of :func:`resolve_anchored_target`.

    * ``status`` — see :class:`AnchorStatus`.
    * ``target`` — ``(target_idx, remaining_ms)`` iff ``status == OK``.
    * ``skew_s`` — ``(anchor - now).total_seconds()``; informational,
      useful for the one-shot clock-skew telemetry message.  ``0.0``
      when no anchor was supplied.
    """

    status: AnchorStatus
    target: Optional[tuple[int, int]] = None
    skew_s: float = 0.0


def resolve_anchored_target(
    slides: list[dict],
    cycle_duration_ms: int,
    anchor: Optional[datetime],
    *,
    now: Optional[datetime] = None,
    clock_skew_tolerance_s: int = CLOCK_SKEW_TOLERANCE_S,
) -> AnchorResolution:
    """Compute the wall-clock-anchored target slide.

    Pure function — does not mutate inputs, does not log, does not read
    the wall clock unless ``now`` is left as ``None``.  Callers own all
    telemetry (the player logs the clock-skew-guard transitions; the
    softplayer might surface them differently).

    Inputs:

    * ``slides`` — the manifest's slide list (each dict has at least
      ``duration_ms``).  Empty/missing produces ``DEGENERATE_CYCLE``.
    * ``cycle_duration_ms`` — pre-computed sum of slide durations; passed
      in so callers can cache it on their slideshow-state dict instead
      of re-summing each tick.
    * ``anchor`` — UTC ``datetime`` parsed from ``manifest.started_at``,
      or ``None`` if the manifest is pre-1.1 or has no parseable
      ``started_at``.  ``None`` → ``NO_ANCHOR``.
    * ``now`` — override for testing; defaults to ``datetime.now(UTC)``.
    * ``clock_skew_tolerance_s`` — how far the wall clock may lag the
      anchor before we punt to legacy playback.  Default matches
      :data:`CLOCK_SKEW_TOLERANCE_S`.
    """
    if anchor is None:
        return AnchorResolution(AnchorStatus.NO_ANCHOR)
    if not slides or cycle_duration_ms <= 0:
        return AnchorResolution(AnchorStatus.DEGENERATE_CYCLE)
    if now is None:
        now = datetime.now(timezone.utc)
    skew_s = (anchor - now).total_seconds()
    if skew_s > clock_skew_tolerance_s:
        return AnchorResolution(AnchorStatus.CLOCK_SKEW_BEHIND, skew_s=skew_s)
    elapsed_ms = int((now - anchor).total_seconds() * 1000)
    target = locate_slide_at(elapsed_ms, slides)
    return AnchorResolution(AnchorStatus.OK, target=target, skew_s=skew_s)
