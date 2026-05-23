"""Platform-agnostic helpers for the slideshow sequencer.

This module is intentionally pure-Python with no GLib / GStreamer /
mpv / Linux-path dependencies so it can be re-used unchanged by the
agora-softplayer (Windows host) and any future non-Pi platform that
runs the same chromium-shell slideshow logic.

Phase 4 (incremental) of agora#226: PR-1 lifts the helpers that were
already pure; the slideshow state machine itself stays in
``player/service.py`` for now and will move in PR-2.

The helpers are also re-exported from ``player/service.py`` as
module-level aliases (``_parse_schema_version``, ``_is_forward_schema``,
``_parse_iso8601_utc``, ``_locate_slide_at``, ``_CLOCK_SKEW_TOLERANCE_S``,
``_RESYNC_CAP_MS``) so existing imports and tests keep working without
modification.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


# ── Manifest schema-version helpers (agora#226 Phase 0) ──

#: Highest manifest_schema_version this player understands.  Manifests
#: with a higher version are still played back (best-effort) but log a
#: one-shot "CMS is ahead of this player" INFO message.  Older versions
#: are accepted unconditionally.
PLAYER_MAX_MANIFEST_SCHEMA_VERSION = "1.1"


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
