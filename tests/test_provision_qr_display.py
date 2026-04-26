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


SAMPLE_SECRET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


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
    ``{"v":1,"secret":<26-char base32>}`` with no whitespace.
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
