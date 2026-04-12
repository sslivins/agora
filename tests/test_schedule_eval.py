"""Tests for device-side schedule evaluation helpers."""

from datetime import date, datetime, timedelta, timezone

import pytest

# Import the module-level helpers from the CMS client service
# We need to import them carefully since service.py has heavy imports
import sys
from unittest.mock import patch, MagicMock

# Mock heavy dependencies so we can import the helpers
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())

from cms_client.service import _parse_time, _schedule_matches_now, _schedule_starts_within_hours


# ── _parse_time tests ──


class TestParseTime:
    def test_basic(self):
        assert _parse_time("09:30") == (9, 30, 0)

    def test_midnight(self):
        assert _parse_time("00:00") == (0, 0, 0)

    def test_end_of_day(self):
        assert _parse_time("23:59") == (23, 59, 0)

    def test_single_digit_hour(self):
        assert _parse_time("9:05") == (9, 5, 0)

    def test_with_seconds(self):
        assert _parse_time("09:30:45") == (9, 30, 45)

    def test_midnight_with_seconds(self):
        assert _parse_time("00:00:00") == (0, 0, 0)

    def test_end_of_day_with_seconds(self):
        assert _parse_time("23:59:59") == (23, 59, 59)


# ── _schedule_matches_now tests ──


class TestScheduleMatchesNow:
    """Test the device-side schedule matching (dict-based)."""

    def _entry(
        self,
        start_time="09:00",
        end_time="17:00",
        start_date=None,
        end_date=None,
        days_of_week=None,
    ):
        return {
            "id": "test-id",
            "name": "Test",
            "asset": "video.mp4",
            "start_time": start_time,
            "end_time": end_time,
            "start_date": start_date,
            "end_date": end_date,
            "days_of_week": days_of_week,
            "priority": 0,
        }

    def test_basic_match(self):
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(self._entry(), now) is True

    def test_before_window(self):
        now = datetime(2026, 3, 28, 8, 59)
        assert _schedule_matches_now(self._entry(), now) is False

    def test_after_window(self):
        now = datetime(2026, 3, 28, 17, 30)
        assert _schedule_matches_now(self._entry(), now) is False

    def test_start_time_inclusive(self):
        now = datetime(2026, 3, 28, 9, 0)
        assert _schedule_matches_now(self._entry(), now) is True

    def test_end_time_exclusive(self):
        """End time is exclusive — 17:00 should NOT match 9:00-17:00."""
        now = datetime(2026, 3, 28, 17, 0)
        assert _schedule_matches_now(self._entry(), now) is False

    def test_one_minute_before_end(self):
        now = datetime(2026, 3, 28, 16, 59)
        assert _schedule_matches_now(self._entry(), now) is True

    def test_overnight_before_midnight(self):
        entry = self._entry(start_time="22:00", end_time="06:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 23, 0)) is True

    def test_overnight_after_midnight(self):
        entry = self._entry(start_time="22:00", end_time="06:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 29, 2, 0)) is True

    def test_overnight_at_start(self):
        entry = self._entry(start_time="22:00", end_time="06:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 22, 0)) is True

    def test_overnight_end_exclusive(self):
        entry = self._entry(start_time="22:00", end_time="06:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 29, 6, 0)) is False

    def test_overnight_no_match_afternoon(self):
        entry = self._entry(start_time="22:00", end_time="06:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 14, 0)) is False

    def test_before_start_date(self):
        entry = self._entry(start_date="2026-04-01")
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is False

    def test_after_end_date(self):
        entry = self._entry(end_date="2026-03-27")
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is False

    def test_within_date_range(self):
        entry = self._entry(start_date="2026-03-01", end_date="2026-03-31")
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is True

    def test_day_of_week_match(self):
        """March 28, 2026 is Saturday (isoweekday=6)."""
        entry = self._entry(days_of_week=[6, 7])
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is True

    def test_day_of_week_no_match(self):
        entry = self._entry(days_of_week=[1, 2, 3, 4, 5])
        now = datetime(2026, 3, 28, 12, 0)  # Saturday
        assert _schedule_matches_now(entry, now) is False

    def test_no_days_means_every_day(self):
        entry = self._entry(days_of_week=None)
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is True

    def test_one_minute_window(self):
        entry = self._entry(start_time="13:20", end_time="13:21")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 13, 19)) is False
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 13, 20)) is True
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 13, 21)) is False

    def test_midnight_in_overnight(self):
        entry = self._entry(start_time="23:00", end_time="01:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 29, 0, 0)) is True

    def test_same_start_end(self):
        """Zero-length window should not match."""
        entry = self._entry(start_time="12:00", end_time="12:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 12, 0)) is False

    def test_second_resolution_start_inclusive(self):
        """Start time with seconds is inclusive."""
        entry = self._entry(start_time="09:00:30", end_time="17:00:00")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 29)) is False
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 30)) is True

    def test_second_resolution_end_exclusive(self):
        """End time with seconds is exclusive."""
        entry = self._entry(start_time="09:00:00", end_time="09:00:03")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 2)) is True
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 3)) is False

    def test_three_second_window(self):
        """A 3-second clip with loop_count=1 produces a 3-second window."""
        entry = self._entry(start_time="09:00:00", end_time="09:00:03")
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 8, 59, 59)) is False
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 0)) is True
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 1)) is True
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 2)) is True
        assert _schedule_matches_now(entry, datetime(2026, 3, 28, 9, 0, 3)) is False

    def test_hh_mm_still_works(self):
        """Legacy HH:MM format (no seconds) still works — seconds default to 0."""
        entry = self._entry(start_time="09:00", end_time="17:00")
        now = datetime(2026, 3, 28, 16, 59, 59)
        assert _schedule_matches_now(entry, now) is True

    def test_end_date_on_exact_date(self):
        """End date is inclusive: schedule ending today should match today."""
        entry = self._entry(end_date="2026-03-28")
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is True

    def test_start_date_on_exact_date(self):
        """Start date is inclusive: schedule starting today should match."""
        entry = self._entry(start_date="2026-03-28")
        now = datetime(2026, 3, 28, 12, 0)
        assert _schedule_matches_now(entry, now) is True


# ── _schedule_starts_within_hours tests ──


class TestScheduleStartsWithinHours:
    def _entry(
        self,
        start_time="09:00",
        end_time="17:00",
        start_date=None,
        end_date=None,
        days_of_week=None,
    ):
        return {
            "id": "test-id",
            "name": "Test",
            "asset": "video.mp4",
            "start_time": start_time,
            "end_time": end_time,
            "start_date": start_date,
            "end_date": end_date,
            "days_of_week": days_of_week,
            "priority": 0,
        }

    def test_schedule_today_within_lookahead(self):
        entry = self._entry(start_time="14:00", end_time="15:00")
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is True

    def test_expired_schedule(self):
        entry = self._entry(end_date="2026-03-27")
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is False

    def test_future_schedule_beyond_lookahead(self):
        entry = self._entry(start_date="2026-04-05")
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is False

    def test_schedule_tomorrow_within_lookahead(self):
        entry = self._entry(start_date="2026-03-29")
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is True

    def test_day_of_week_today_matches(self):
        """Saturday - should match if days include 6."""
        entry = self._entry(days_of_week=[6])
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is True

    def test_day_of_week_tomorrow_matches(self):
        """Saturday now, Sunday (7) schedule — tomorrow is within lookahead."""
        entry = self._entry(days_of_week=[7])
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is True

    def test_day_of_week_neither_today_nor_tomorrow(self):
        """Saturday — schedule for Monday only, neither today nor tomorrow."""
        entry = self._entry(days_of_week=[1])
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is False

    def test_no_days_of_week_always_eligible(self):
        entry = self._entry(days_of_week=None)
        now = datetime(2026, 3, 28, 10, 0)
        assert _schedule_starts_within_hours(entry, now, 24) is True
