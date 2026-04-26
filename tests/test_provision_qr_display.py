"""Behavioral tests for OOBE pairing-QR display flow.

CI does not install ``cairo`` / ``gobject-introspection`` / ``qrcode``,
so we mock them in ``sys.modules`` *before* importing provision modules
(matching the pattern in ``test_oobe_flow.py``).  Tests that exercise
the actual draw path patch out the cairo-dependent helpers, so the
underlying mocked cairo is never called.
"""
from __future__ import annotations

import asyncio
import json
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault("cairo", MagicMock())
sys.modules.setdefault("gi", MagicMock())
sys.modules.setdefault("gi.repository", MagicMock())
sys.modules.setdefault("qrcode", MagicMock())
sys.modules.setdefault("qrcode.constants", MagicMock())

from provision import service as provision_service  # noqa: E402
from provision import display as display_mod  # noqa: E402


SAMPLE_SECRET = "7K3Q4M2P"


def _make_display():
    display = MagicMock()
    display.available = True
    return display


def _run_adoption(display, *, status_sequence, secret_sequence, **kwargs):
    """Drive ``_wait_for_cms_adoption`` with scripted status + secret feeds."""
    status_iter = iter(status_sequence)
    secret_iter = iter(secret_sequence)
    shutdown = asyncio.Event()

    async def fake_sleep(_seconds):
        return None

    with patch.object(provision_service, "_read_cms_status",
                      side_effect=lambda: next(status_iter)), \
         patch.object(provision_service, "_read_pairing_secret",
                      side_effect=lambda *a, **kw: next(secret_iter)), \
         patch.object(provision_service, "_get_cms_host",
                      return_value="cms.example"), \
         patch.object(provision_service, "get_device_ip",
                      return_value="192.168.1.50"), \
         patch.object(provision_service.asyncio, "sleep", fake_sleep):
        result = asyncio.run(
            provision_service._wait_for_cms_adoption(
                display, shutdown, **kwargs,
            ),
        )
    return result


def test_pending_with_secret_immediately_shows_qr():
    display = _make_display()
    result = _run_adoption(
        display,
        status_sequence=[
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "registered"},
        ],
        secret_sequence=[SAMPLE_SECRET],
        ip="10.0.0.5", hostname="agora-abcd.local",
    )
    assert result == "adopted"
    display.show_pairing_qr.assert_called_once_with(
        secret=SAMPLE_SECRET,
        cms_host="cms.example",
        ip="10.0.0.5",
        hostname="agora-abcd.local",
    )
    display.show_cms_connected_pending.assert_not_called()


def test_pending_without_secret_falls_back_then_upgrades():
    """Race fix: ethernet path reaches 'pending' before cms-client has
    written the pairing secret.  We must show the legacy screen, then
    upgrade to the QR screen as soon as the secret appears."""
    display = _make_display()
    result = _run_adoption(
        display,
        status_sequence=[
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "registered"},
        ],
        # Secret appears on the second pending poll.
        secret_sequence=[None, SAMPLE_SECRET, "ignored-after-qr"],
    )
    assert result == "adopted"
    display.show_cms_connected_pending.assert_called_once_with("cms.example")
    display.show_pairing_qr.assert_called_once()
    _, kwargs = display.show_pairing_qr.call_args
    assert kwargs["secret"] == SAMPLE_SECRET


def test_pending_with_secret_renders_qr_only_once():
    """No flicker: the QR screen must be drawn once, not on every poll."""
    display = _make_display()
    _run_adoption(
        display,
        status_sequence=[
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "pending"},
            {"state": "connected", "registration": "registered"},
        ],
        secret_sequence=[SAMPLE_SECRET, SAMPLE_SECRET, SAMPLE_SECRET],
    )
    assert display.show_pairing_qr.call_count == 1


def test_show_pairing_qr_encodes_canonical_payload(monkeypatch):
    """The QR payload must match what the CMS scanner accepts:
    ``{"v":1,"secret":<8-char Crockford base32>}`` with no whitespace.
    See ``cms/static/app.js:_parsePairingQr``.
    """
    captured = {}

    def fake_draw_qr(ctx, cx, cy, data, module_size=6, quiet_zone=2):
        captured["data"] = data
        return True

    monkeypatch.setattr(display_mod, "_draw_qr_code", fake_draw_qr)
    monkeypatch.setattr(display_mod, "_draw_bg", lambda *a, **kw: None)
    monkeypatch.setattr(display_mod, "_draw_logo", lambda *a, **kw: 60)
    monkeypatch.setattr(display_mod, "_draw_text", lambda *a, **kw: 30)
    monkeypatch.setattr(
        display_mod, "_draw_badge", lambda *a, **kw: (200, 50),
    )
    monkeypatch.setattr(
        display_mod, "_draw_progress_dots", lambda *a, **kw: None,
    )

    pd = display_mod.ProvisionDisplay.__new__(display_mod.ProvisionDisplay)
    pd._width = 1920
    pd._height = 1080
    pd._surface = MagicMock()
    pd._fb_path = "/dev/null"
    pd._bpp = 32
    pd._frame = 0
    pd._rgb565_lib = None
    pd._ctx = lambda: MagicMock()
    pd._blit = lambda: None

    pd.show_pairing_qr(
        secret=SAMPLE_SECRET,
        cms_host="cms.example",
        ip="10.0.0.5",
        hostname="agora-abcd.local",
    )

    expected = json.dumps(
        {"v": 1, "secret": SAMPLE_SECRET}, separators=(",", ":"),
    )
    assert captured["data"] == expected
    # Sanity: round-trip parse — proves it's valid JSON the scanner can read.
    assert json.loads(captured["data"]) == {"v": 1, "secret": SAMPLE_SECRET}


def test_show_pairing_qr_noop_when_unavailable():
    pd = display_mod.ProvisionDisplay.__new__(display_mod.ProvisionDisplay)
    pd._surface = None  # `available` property returns False
    # Should silently return without raising or trying to draw anything.
    pd.show_pairing_qr(secret=SAMPLE_SECRET)


# ---------------------------------------------------------------------
# Stale-negative cms_status.json guard (PR #144)
# ---------------------------------------------------------------------
# cms-client doesn't update cms_status.json during the multi-minute
# bootstrap-v2 polling window.  Without the stale-negative guard, a
# leftover "error"/"disconnected+error" entry from a previous run
# dominates _wait_for_cms_adoption and bounces the device into
# reconfigure ~10s after boot.  Positive states (connected/pending,
# connected/registered) must NEVER be filtered, even if their
# timestamp predates the start of the wait by milliseconds (race).


from datetime import datetime, timedelta, timezone


def _ts(dt: datetime) -> str:
    return dt.isoformat()


def test_is_status_negative_stale_old_error_is_stale():
    old = _ts(datetime.now(timezone.utc) - timedelta(minutes=5))
    assert provision_service._is_status_negative_stale(
        {"state": "error", "error": "boom", "timestamp": old},
    ) is True


def test_is_status_negative_stale_fresh_error_is_fresh():
    fresh = _ts(datetime.now(timezone.utc) - timedelta(seconds=2))
    assert provision_service._is_status_negative_stale(
        {"state": "error", "error": "boom", "timestamp": fresh},
    ) is False


def test_is_status_negative_stale_disconnected_with_error_is_negative():
    old = _ts(datetime.now(timezone.utc) - timedelta(minutes=5))
    assert provision_service._is_status_negative_stale(
        {"state": "disconnected", "error": "x", "timestamp": old},
    ) is True


def test_is_status_negative_stale_disconnected_without_error_not_negative():
    """Plain disconnected with no error is transient, not negative."""
    old = _ts(datetime.now(timezone.utc) - timedelta(minutes=5))
    assert provision_service._is_status_negative_stale(
        {"state": "disconnected", "error": "", "timestamp": old},
    ) is False


def test_is_status_negative_stale_positive_states_never_stale():
    """connected/pending and connected/registered must never be filtered."""
    long_ago = _ts(datetime.now(timezone.utc) - timedelta(hours=1))
    assert provision_service._is_status_negative_stale(
        {"state": "connected", "registration": "pending",
         "error": "", "timestamp": long_ago},
    ) is False
    assert provision_service._is_status_negative_stale(
        {"state": "connected", "registration": "registered",
         "error": "", "timestamp": long_ago},
    ) is False


def test_is_status_negative_stale_missing_timestamp_treated_as_stale():
    """During OOBE we'd rather drop a timestamp-less negative than latch."""
    assert provision_service._is_status_negative_stale(
        {"state": "error", "error": "boom"},
    ) is True


def test_is_status_negative_stale_unparseable_timestamp_treated_as_stale():
    assert provision_service._is_status_negative_stale(
        {"state": "error", "error": "boom", "timestamp": "not-a-date"},
    ) is True


def test_stale_negative_does_not_increment_error_counter():
    """Loop should ignore a stale leftover error — no reconfigure trigger."""
    display = _make_display()
    old_ts = _ts(datetime.now(timezone.utc) - timedelta(minutes=5))
    # First several polls return a STALE error from a previous run.
    # If the stale-guard works, none of these should count toward
    # CMS_ERROR_THRESHOLD; instead the loop sees them as "connecting",
    # then a fresh adopted finishes the wait.
    statuses = [
        {"state": "error", "error": "old timeout", "timestamp": old_ts},
    ] * (provision_service.CMS_ERROR_THRESHOLD + 2)
    statuses.append({"state": "connected", "registration": "registered"})

    result = _run_adoption(
        display,
        status_sequence=statuses,
        secret_sequence=[None] * (len(statuses) + 1),
    )
    assert result == "adopted"
    display.show_cms_failed.assert_not_called()


def test_fresh_negative_still_counts():
    """Genuinely fresh negatives must still trigger reconfigure."""
    display = _make_display()
    fresh_ts = _ts(datetime.now(timezone.utc))
    statuses = [
        {"state": "error", "error": "fresh failure", "timestamp": fresh_ts},
    ] * (provision_service.CMS_ERROR_THRESHOLD + 1)

    result = _run_adoption(
        display,
        status_sequence=statuses,
        secret_sequence=[None] * (len(statuses) + 1),
    )
    assert result == "failed"


def test_fresh_pending_with_old_timestamp_still_shows_qr():
    """Race test: pending status timestamped slightly before the wait
    function starts must still trigger the QR screen.  Stale-filter
    must only target NEGATIVE states."""
    display = _make_display()
    # Timestamp older than STALE_NEG_SEC, but state is connected/pending
    # — never stale, must drive QR display.
    old_ts = _ts(datetime.now(timezone.utc) - timedelta(minutes=10))
    result = _run_adoption(
        display,
        status_sequence=[
            {"state": "connected", "registration": "pending",
             "error": "", "timestamp": old_ts},
            {"state": "connected", "registration": "registered"},
        ],
        secret_sequence=[SAMPLE_SECRET],
        ip="10.0.0.5", hostname="agora-abcd.local",
    )
    assert result == "adopted"
    display.show_pairing_qr.assert_called_once()


