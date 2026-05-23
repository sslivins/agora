"""Unit tests for the pure helpers in ``player.slideshow_engine``.

These tests do not touch GLib / GStreamer / mpv / Linux paths, so they
also serve as a portability sanity-check for agora-softplayer's
upcoming consumption of the same module.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from player.slideshow_engine import (
    AnchorStatus,
    CLOCK_SKEW_TOLERANCE_S,
    PLAYER_MAX_MANIFEST_SCHEMA_VERSION,
    RESYNC_CAP_MS,
    is_forward_schema,
    locate_slide_at,
    parse_iso8601_utc,
    parse_schema_version,
    read_slideshow_manifest,
    resolve_anchored_target,
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
        assert PLAYER_MAX_MANIFEST_SCHEMA_VERSION == "1.1"
