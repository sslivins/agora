"""Unit tests for the pure helpers in ``player.slideshow_engine``.

These tests do not touch GLib / GStreamer / mpv / Linux paths, so they
also serve as a portability sanity-check for agora-softplayer's
upcoming consumption of the same module.
"""
from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

from player.slideshow_engine import (
    AnchorStatus,
    CLOCK_SKEW_TOLERANCE_S,
    PLAYER_MAX_MANIFEST_SCHEMA_VERSION,
    RESYNC_CAP_MS,
    WINDOW_BOUNDARY_MAX_SLEEP_S,
    cycle_index_at,
    is_forward_schema,
    locate_slide_at,
    next_window_boundary,
    ordered_slides_for_cycle,
    parse_iso8601_utc,
    parse_schema_version,
    parse_window,
    read_slideshow_manifest,
    resolve_anchored_target,
    slide_has_window,
    slide_window_open,
    visible_slides,
)


class TestParseSchemaVersion:
    def test_major_minor(self):
        assert parse_schema_version("1.0") == (1, 0)
        assert parse_schema_version("1.1") == (1, 1)
        assert parse_schema_version("2.5") == (2, 5)

    def test_patch_ignored(self):
        assert parse_schema_version("1.0.3") == (1, 0)

    def test_unparseable(self):
        assert parse_schema_version("abc") == (0, 0)
        assert parse_schema_version("") == (0, 0)
        assert parse_schema_version("1") == (0, 0)
        assert parse_schema_version(None) == (0, 0)  # type: ignore[arg-type]


class TestIsForwardSchema:
    def test_strictly_forward(self):
        assert is_forward_schema("1.1", "1.0") is True
        assert is_forward_schema("2.0", "1.5") is True

    def test_equal_not_forward(self):
        assert is_forward_schema("1.1", "1.1") is False

    def test_backward_not_forward(self):
        assert is_forward_schema("1.0", "1.1") is False


class TestParseIso8601Utc:
    def test_z_suffix(self):
        dt = parse_iso8601_utc("2026-05-23T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is timezone.utc
        assert dt.year == 2026 and dt.hour == 12

    def test_offset_suffix(self):
        dt = parse_iso8601_utc("2026-05-23T12:00:00+00:00")
        assert dt is not None
        assert dt.tzinfo is timezone.utc

    def test_returns_none_on_garbage(self):
        assert parse_iso8601_utc("not a date") is None
        assert parse_iso8601_utc("") is None
        assert parse_iso8601_utc(None) is None  # type: ignore[arg-type]
        assert parse_iso8601_utc(123) is None  # type: ignore[arg-type]


class TestLocateSlideAt:
    def test_first_slide(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        idx, remaining = locate_slide_at(500, slides)
        assert idx == 0
        assert remaining == 2500

    def test_second_slide(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        idx, remaining = locate_slide_at(5000, slides)
        assert idx == 1
        assert remaining == 2000

    def test_wraps_modulo_cycle(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        # 7000ms is one full cycle; 7500 -> 500ms into slide 0
        idx, remaining = locate_slide_at(7500, slides)
        assert idx == 0
        assert remaining == 2500

    def test_negative_elapsed_wraps(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        idx, remaining = locate_slide_at(-500, slides)
        # -500 % 7000 = 6500 -> slide 1, 500ms remaining
        assert idx == 1
        assert remaining == 500

    def test_zero_duration_skipped(self):
        slides = [
            {"duration_ms": 0},
            {"duration_ms": 1000},
            {"duration_ms": 0},
            {"duration_ms": 2000},
        ]
        idx, remaining = locate_slide_at(500, slides)
        assert idx == 1
        assert remaining == 500

    def test_all_zero_returns_degenerate(self):
        slides = [{"duration_ms": 0}, {"duration_ms": 0}]
        assert locate_slide_at(0, slides) == (0, 1)

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            locate_slide_at(0, [])


class TestReadSlideshowManifest:
    def test_happy_path(self, tmp_path: Path):
        manifest = {
            "manifest_schema_version": "1.1",
            "slides": [{"asset": "a.jpg", "duration_ms": 1000}],
        }
        (tmp_path / "slideshows").mkdir()
        (tmp_path / "slideshows" / "ss.json").write_text(json.dumps(manifest))
        result = read_slideshow_manifest(tmp_path, "ss")
        assert result is not None
        data, digest = result
        assert data["manifest_schema_version"] == "1.1"
        assert len(digest) == 64  # sha256 hex
        # Digest is deterministic for the same byte content
        result2 = read_slideshow_manifest(tmp_path, "ss")
        assert result2 is not None and result2[1] == digest

    def test_missing_returns_none(self, tmp_path: Path):
        assert read_slideshow_manifest(tmp_path, "nope") is None

    def test_malformed_json_returns_none(self, tmp_path: Path):
        (tmp_path / "slideshows").mkdir()
        (tmp_path / "slideshows" / "ss.json").write_text("{not json")
        assert read_slideshow_manifest(tmp_path, "ss") is None

    def test_non_dict_returns_none(self, tmp_path: Path):
        (tmp_path / "slideshows").mkdir()
        (tmp_path / "slideshows" / "ss.json").write_text("[1,2,3]")
        assert read_slideshow_manifest(tmp_path, "ss") is None

    def test_empty_slides_returns_none(self, tmp_path: Path):
        (tmp_path / "slideshows").mkdir()
        (tmp_path / "slideshows" / "ss.json").write_text('{"slides": []}')
        assert read_slideshow_manifest(tmp_path, "ss") is None

    def test_missing_slides_returns_none(self, tmp_path: Path):
        (tmp_path / "slideshows").mkdir()
        (tmp_path / "slideshows" / "ss.json").write_text('{"foo": "bar"}')
        assert read_slideshow_manifest(tmp_path, "ss") is None


def _anchor(offset_s: float = 0.0) -> datetime:
    """Return an anchor ``offset_s`` seconds before/after a fixed ``now``."""
    return _NOW - timedelta(seconds=offset_s)


_NOW = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)


class TestResolveAnchoredTarget:
    def test_no_anchor(self):
        slides = [{"duration_ms": 1000}]
        result = resolve_anchored_target(slides, 1000, None, now=_NOW)
        assert result.status is AnchorStatus.NO_ANCHOR
        assert result.target is None

    def test_degenerate_empty_slides(self):
        result = resolve_anchored_target([], 0, _anchor(0), now=_NOW)
        assert result.status is AnchorStatus.DEGENERATE_CYCLE

    def test_degenerate_zero_cycle(self):
        slides = [{"duration_ms": 0}]
        result = resolve_anchored_target(slides, 0, _anchor(0), now=_NOW)
        assert result.status is AnchorStatus.DEGENERATE_CYCLE

    def test_clock_skew_behind(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        # anchor is 2h in the future relative to now → wall clock too
        # far behind.
        future_anchor = _NOW + timedelta(hours=2)
        result = resolve_anchored_target(slides, 7000, future_anchor, now=_NOW)
        assert result.status is AnchorStatus.CLOCK_SKEW_BEHIND
        assert result.skew_s > CLOCK_SKEW_TOLERANCE_S

    def test_ok_first_slide(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        anchor = _NOW - timedelta(milliseconds=500)  # 500ms into slide 0
        result = resolve_anchored_target(slides, 7000, anchor, now=_NOW)
        assert result.status is AnchorStatus.OK
        assert result.target == (0, 2500)

    def test_ok_wraps_after_full_cycle(self):
        slides = [{"duration_ms": 3000}, {"duration_ms": 4000}]
        # 12 minutes into a 7s cycle wraps cleanly back to slide 0 +offset
        anchor = _NOW - timedelta(minutes=12)
        result = resolve_anchored_target(slides, 7000, anchor, now=_NOW)
        assert result.status is AnchorStatus.OK
        idx, _ = result.target  # type: ignore[misc]
        assert idx in (0, 1)


class TestConstants:
    def test_constants_match_documented_values(self):
        # Lock the documented behaviour: changing these requires updating
        # docs and the CMS-side scheduler.
        assert CLOCK_SKEW_TOLERANCE_S == 3600
        assert RESYNC_CAP_MS == 5000
        assert PLAYER_MAX_MANIFEST_SCHEMA_VERSION == "1.5"


class TestCycleIndexAt:
    def test_first_cycle(self):
        # 0ms in and partway into cycle 0 both return 0.
        assert cycle_index_at(0, 10_000) == 0
        assert cycle_index_at(9_999, 10_000) == 0

    def test_cycle_boundary(self):
        # Exactly one cycle in starts cycle 1.
        assert cycle_index_at(10_000, 10_000) == 1
        assert cycle_index_at(25_000, 10_000) == 2

    def test_floor_division_matches_locate(self):
        # The ordinal must use floor division so it agrees with
        # locate_slide_at's elapsed % cycle math at every boundary.
        assert cycle_index_at(19_999, 10_000) == 1
        assert cycle_index_at(20_000, 10_000) == 2

    def test_negative_elapsed_floors(self):
        # Small clock skew can make elapsed negative; floor division
        # yields -1 (Python floors toward negative infinity), which the
        # seed mixing tolerates without blowing up.
        assert cycle_index_at(-1, 10_000) == -1
        assert cycle_index_at(-10_000, 10_000) == -1
        assert cycle_index_at(-10_001, 10_000) == -2

    def test_zero_or_negative_duration_guard(self):
        # Degenerate deck: never divide by zero, return a stable 0.
        assert cycle_index_at(5_000, 0) == 0
        assert cycle_index_at(5_000, -1) == 0


class TestOrderedSlidesForCycle:
    def _slides(self, n: int) -> list[dict]:
        return [{"name": f"s{i}", "duration_ms": 1000} for i in range(n)]

    def test_shuffle_off_is_identity(self):
        base = self._slides(4)
        out = ordered_slides_for_cycle(base, False, 12345, 0)
        assert out == base

    def test_shuffle_off_returns_new_list(self):
        # Identity in content but a fresh list — callers must never get
        # the caller's own list back to mutate.
        base = self._slides(3)
        out = ordered_slides_for_cycle(base, False, 1, 0)
        assert out is not base

    def test_fewer_than_two_slides_identity(self):
        # A single-slide deck can't be permuted; shuffle is a no-op.
        base = self._slides(1)
        assert ordered_slides_for_cycle(base, True, 999, 5) == base
        assert ordered_slides_for_cycle([], True, 999, 5) == []

    def test_deterministic_same_seed_and_cycle(self):
        base = self._slides(8)
        a = ordered_slides_for_cycle(base, True, 4242, 3)
        b = ordered_slides_for_cycle(base, True, 4242, 3)
        assert a == b

    def test_actually_permutes(self):
        # With enough slides a shuffle should differ from base order for
        # at least one cycle (guards against a no-op seed bug).
        base = self._slides(10)
        orders = [
            [s["name"] for s in ordered_slides_for_cycle(base, True, 7, c)]
            for c in range(5)
        ]
        base_names = [s["name"] for s in base]
        assert any(o != base_names for o in orders)

    def test_different_cycles_reshuffle(self):
        # Consecutive cycles should generally produce different orders so
        # the deck doesn't repeat the same "random" sequence every cycle.
        base = self._slides(10)
        c0 = [s["name"] for s in ordered_slides_for_cycle(base, True, 88, 0)]
        c1 = [s["name"] for s in ordered_slides_for_cycle(base, True, 88, 1)]
        assert c0 != c1

    def test_different_seeds_diverge(self):
        # Two devices with different (buggy) seeds would desync; pin that
        # the seed actually drives the permutation.
        base = self._slides(10)
        a = [s["name"] for s in ordered_slides_for_cycle(base, True, 1, 0)]
        b = [s["name"] for s in ordered_slides_for_cycle(base, True, 2, 0)]
        assert a != b

    def test_permutation_preserves_membership(self):
        # Every slide appears exactly once — no drops or dupes.
        base = self._slides(6)
        out = ordered_slides_for_cycle(base, True, 555, 9)
        assert sorted(s["name"] for s in out) == sorted(s["name"] for s in base)
        assert len(out) == len(base)

    def test_does_not_mutate_input(self):
        base = self._slides(6)
        snapshot = list(base)
        ordered_slides_for_cycle(base, True, 555, 9)
        assert base == snapshot

    def test_negative_cycle_index_is_stable(self):
        # Clock-skew can yield a negative cycle index; it must not raise.
        base = self._slides(5)
        out = ordered_slides_for_cycle(base, True, 33, -1)
        assert sorted(s["name"] for s in out) == sorted(s["name"] for s in base)


# ---------------------------------------------------------------------------
# Per-slide visibility windows (manifest schema 1.5)
# ---------------------------------------------------------------------------


def _wslide(**kw) -> dict:
    """A minimal slide dict exposing only the five window fields.

    ``slide_window_open`` reads nothing else off the slide for the
    predicate, so this keeps the predicate tests focused.
    """
    d = {
        "valid_from": None,
        "valid_to": None,
        "active_days": None,
        "active_start": None,
        "active_end": None,
    }
    d.update(kw)
    return d


def _ln(y, m, d, hh=12, mm=0) -> datetime:
    """A naive local-civil datetime (the clock domain the predicate uses)."""
    return datetime(y, m, d, hh, mm)


class TestSlideWindowOpen:
    """Verbatim parity with the CMS resolver predicate
    (``cms.services.slideshow_resolver._slide_window_open``) — the firmware
    MUST flip a slide open/closed at the exact same civil instant the
    server would.  Cases ported 1:1 from agora-cms
    ``tests/test_slideshow_visibility.py::TestSlideWindowOpen``.
    """

    def test_no_window_is_always_open(self):
        assert slide_window_open(_wslide(), _ln(2026, 12, 25, 3, 0)) is True

    def test_date_range_inclusive_both_ends(self):
        row = _wslide(valid_from="2026-12-01", valid_to="2026-12-26")
        assert slide_window_open(row, _ln(2026, 11, 30)) is False
        assert slide_window_open(row, _ln(2026, 12, 1)) is True
        assert slide_window_open(row, _ln(2026, 12, 26)) is True
        assert slide_window_open(row, _ln(2026, 12, 27)) is False

    def test_only_valid_from(self):
        row = _wslide(valid_from="2026-06-01")
        assert slide_window_open(row, _ln(2026, 5, 31)) is False
        assert slide_window_open(row, _ln(2026, 6, 1)) is True
        assert slide_window_open(row, _ln(2030, 1, 1)) is True

    def test_only_valid_to(self):
        row = _wslide(valid_to="2026-06-01")
        assert slide_window_open(row, _ln(2026, 6, 1)) is True
        assert slide_window_open(row, _ln(2026, 6, 2)) is False

    def test_normal_time_window_start_incl_end_excl(self):
        row = _wslide(active_start="13:00:00", active_end="14:00:00")
        assert slide_window_open(row, _ln(2026, 6, 1, 12, 59)) is False
        assert slide_window_open(row, _ln(2026, 6, 1, 13, 0)) is True
        assert slide_window_open(row, _ln(2026, 6, 1, 13, 59)) is True
        assert slide_window_open(row, _ln(2026, 6, 1, 14, 0)) is False

    def test_only_active_start(self):
        row = _wslide(active_start="09:00:00")
        assert slide_window_open(row, _ln(2026, 6, 1, 8, 59)) is False
        assert slide_window_open(row, _ln(2026, 6, 1, 9, 0)) is True

    def test_only_active_end(self):
        row = _wslide(active_end="17:00:00")
        assert slide_window_open(row, _ln(2026, 6, 1, 16, 59)) is True
        assert slide_window_open(row, _ln(2026, 6, 1, 17, 0)) is False

    def test_overnight_wrap_window(self):
        row = _wslide(active_start="22:00:00", active_end="02:00:00")
        assert slide_window_open(row, _ln(2026, 6, 1, 21, 59)) is False
        assert slide_window_open(row, _ln(2026, 6, 1, 22, 0)) is True
        assert slide_window_open(row, _ln(2026, 6, 1, 23, 30)) is True
        assert slide_window_open(row, _ln(2026, 6, 2, 1, 59)) is True
        assert slide_window_open(row, _ln(2026, 6, 2, 2, 0)) is False

    def test_weekday_only(self):
        # 2026-06-01 is a Monday (weekday 0).
        row = _wslide(active_days=[0, 2, 4])  # Mon, Wed, Fri
        assert slide_window_open(row, _ln(2026, 6, 1)) is True   # Mon
        assert slide_window_open(row, _ln(2026, 6, 2)) is False  # Tue
        assert slide_window_open(row, _ln(2026, 6, 3)) is True   # Wed

    def test_empty_weekday_list_is_every_day(self):
        assert slide_window_open(_wslide(active_days=[]), _ln(2026, 6, 2)) is True

    def test_wrap_tail_belongs_to_opening_days_weekday(self):
        # Fri 22:00 .. Sat 02:00, allowed only on Friday (weekday 4).
        row = _wslide(
            active_start="22:00:00", active_end="02:00:00", active_days=[4],
        )
        assert slide_window_open(row, _ln(2026, 6, 5, 23, 0)) is True
        # Sat 00:30 wrap tail -> effective weekday Friday -> open.
        assert slide_window_open(row, _ln(2026, 6, 6, 0, 30)) is True
        # Sat 22:00 is a fresh Saturday opening -> not allowed.
        assert slide_window_open(row, _ln(2026, 6, 6, 22, 0)) is False

    def test_full_mixture_all_constraints(self):
        # Christmas-week flash sale: Dec 1-26, 1-2pm, weekdays only.
        row = _wslide(
            valid_from="2026-12-01",
            valid_to="2026-12-26",
            active_start="13:00:00",
            active_end="14:00:00",
            active_days=[0, 1, 2, 3, 4],
        )
        assert slide_window_open(row, _ln(2026, 12, 4, 13, 30)) is True
        assert slide_window_open(row, _ln(2026, 11, 27, 13, 30)) is False
        assert slide_window_open(row, _ln(2026, 12, 4, 9, 0)) is False
        assert slide_window_open(row, _ln(2026, 12, 5, 13, 30)) is False

    def test_fractional_second_time_parses(self):
        # CMS may serialise times with microseconds; both ends parse.
        row = _wslide(active_start="13:00:00.000000", active_end="14:00:00")
        assert slide_window_open(row, _ln(2026, 6, 1, 13, 30)) is True


class TestParseWindowFailOpen:
    """Each dimension fails OPEN (-> None) on missing/garbage input so a
    malformed manifest never strands a slide closed."""

    def test_all_none_when_no_fields(self):
        assert parse_window({}) == {
            "valid_from": None, "valid_to": None, "active_days": None,
            "active_start": None, "active_end": None,
        }

    def test_garbage_date_is_none(self):
        w = parse_window({"valid_from": "not-a-date", "valid_to": 12345})
        assert w["valid_from"] is None
        assert w["valid_to"] is None

    def test_garbage_time_is_none(self):
        w = parse_window({"active_start": "25:99:99", "active_end": None})
        assert w["active_start"] is None

    def test_active_days_deduped_sorted_clamped(self):
        # Out-of-range + duplicate + bool entries are dropped/normalised.
        w = parse_window({"active_days": [4, 0, 0, 9, -1, True]})
        assert w["active_days"] == [0, 4]

    def test_active_days_empty_becomes_none(self):
        assert parse_window({"active_days": []})["active_days"] is None

    def test_garbage_slide_window_open_is_visible(self):
        # A fully-garbage window dict -> all dimensions open -> visible.
        garbage = {
            "valid_from": "xx", "valid_to": object(), "active_days": "nope",
            "active_start": "99:99", "active_end": [],
        }
        assert slide_window_open(garbage, _ln(2026, 6, 1)) is True


class TestSlideHasWindow:
    def test_false_when_no_fields(self):
        assert slide_has_window({}) is False
        assert slide_has_window({"name": "x", "duration_ms": 5000}) is False

    def test_false_when_all_unparseable(self):
        assert slide_has_window({"valid_from": "garbage"}) is False

    def test_true_when_any_valid_constraint(self):
        assert slide_has_window({"valid_from": "2026-12-01"}) is True
        assert slide_has_window({"active_start": "09:00:00"}) is True
        assert slide_has_window({"active_days": [0, 1]}) is True


class TestVisibleSlides:
    def test_windowless_deck_all_pass_in_order(self):
        deck = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        assert visible_slides(deck, _ln(2026, 6, 1)) == deck

    def test_filters_closed_and_preserves_order(self):
        deck = [
            {"name": "always"},
            {"name": "morning", "active_start": "09:00:00",
             "active_end": "12:00:00"},
            {"name": "afternoon", "active_start": "13:00:00",
             "active_end": "17:00:00"},
        ]
        out = visible_slides(deck, _ln(2026, 6, 1, 13, 30))
        assert [s["name"] for s in out] == ["always", "afternoon"]

    def test_all_closed_yields_empty(self):
        deck = [
            {"name": "x", "active_start": "09:00:00", "active_end": "10:00:00"},
            {"name": "y", "active_start": "11:00:00", "active_end": "12:00:00"},
        ]
        assert visible_slides(deck, _ln(2026, 6, 1, 15, 0)) == []


class TestNextWindowBoundary:
    def test_none_for_windowless_deck(self):
        deck = [{"name": "a"}, {"name": "b"}]
        assert next_window_boundary(deck, _ln(2026, 6, 1, 10, 0)) is None

    def test_intraday_returns_next_time_edge(self):
        # Slide opens 13:00, closes 14:00.  At 10:00 the next edge is 13:00.
        deck = [{"name": "s", "active_start": "13:00:00", "active_end": "14:00:00"}]
        b = next_window_boundary(deck, _ln(2026, 6, 1, 10, 0))
        assert b == _ln(2026, 6, 1, 13, 0)

    def test_after_open_returns_close_edge(self):
        deck = [{"name": "s", "active_start": "13:00:00", "active_end": "14:00:00"}]
        b = next_window_boundary(deck, _ln(2026, 6, 1, 13, 30))
        assert b == _ln(2026, 6, 1, 14, 0)

    def test_valid_to_closes_next_midnight(self):
        # valid_to is inclusive, so the slide closes at midnight after it.
        deck = [{"name": "s", "valid_to": "2026-12-26"}]
        b = next_window_boundary(deck, _ln(2026, 12, 26, 10, 0))
        assert b == _ln(2026, 12, 27, 0, 0)

    def test_valid_from_opens_that_midnight(self):
        deck = [{"name": "s", "valid_from": "2026-12-25"}]
        b = next_window_boundary(deck, _ln(2026, 12, 20, 10, 0))
        assert b == _ln(2026, 12, 25, 0, 0)

    def test_strictly_future_only(self):
        # An edge exactly at now is not a *future* boundary; the next one
        # (the close edge) is returned instead.
        deck = [{"name": "s", "active_start": "13:00:00", "active_end": "14:00:00"}]
        b = next_window_boundary(deck, _ln(2026, 6, 1, 13, 0))
        assert b == _ln(2026, 6, 1, 14, 0)

    def test_expired_slide_falls_back_to_midnight(self):
        # Every computed edge is in the past -> daily safety-net midnight.
        deck = [{"name": "s", "valid_to": "2020-01-01"}]
        b = next_window_boundary(deck, _ln(2026, 6, 1, 10, 0))
        assert b == _ln(2026, 6, 2, 0, 0)

    def test_weekday_slide_wakes_at_midnight(self):
        deck = [{"name": "s", "active_days": [0]}]  # Mondays only
        b = next_window_boundary(deck, _ln(2026, 6, 1, 10, 0))
        # The weekday dimension can change at the next midnight.
        assert b == _ln(2026, 6, 2, 0, 0)


class TestWindowBoundaryConstant:
    def test_documented_value(self):
        assert WINDOW_BOUNDARY_MAX_SLEEP_S == 900
