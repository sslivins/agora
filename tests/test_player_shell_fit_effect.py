"""Contract test: player.js / player.css support per-slide fit + Ken Burns.

The CMS slideshow builder emits, per slide, a ``fit`` (cover|contain) and
an ``effect`` (none|ken_burns) field in manifest schema 1.3. The device
shell must honor both:

  * ``fit`` -> CSS ``object-fit`` on the image (and video) element.
  * ``effect == "ken_burns"`` -> a CSS animation scoped to the <img>.

This test pins the shell side of that contract by scraping player.js /
player.css for the required tokens. It deliberately does NOT import the
shell (it's browser JS) — substring assertions mirror the existing
``test_player_shell_transitions.py`` pattern.
"""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAYER_JS = REPO_ROOT / "player" / "shell" / "player.js"
PLAYER_CSS = REPO_ROOT / "player" / "shell" / "player.css"


def _js() -> str:
    return PLAYER_JS.read_text(encoding="utf-8")


def _css() -> str:
    return PLAYER_CSS.read_text(encoding="utf-8")


# ── fit (object-fit) ────────────────────────────────────────────────

def test_player_js_defines_known_fits_allowlist():
    js = _js()
    assert "KNOWN_FITS" in js
    # Both legal values must be allow-listed so the shell can override
    # its legacy ``object-fit: contain`` default with either.
    assert '"cover"' in js
    assert '"contain"' in js


def test_player_js_applies_object_fit_from_cmd():
    js = _js()
    assert "objectFit" in js
    assert "cmd.fit" in js


# ── Ken Burns effect ────────────────────────────────────────────────

def test_player_js_handles_ken_burns_effect():
    js = _js()
    assert "ken_burns" in js
    assert "fx-ken-burns" in js
    # Duration plumbed via a CSS custom property the keyframes consume.
    assert "--fx-duration-ms" in js
    assert "cmd.effect" in js


def test_player_css_defines_ken_burns_keyframes():
    css = _css()
    assert "@keyframes fx-ken-burns" in css
    assert "fx-ken-burns" in css
    # Scoped to the <img> so it doesn't fight the layer-level transform
    # used by push/zoom/dissolve transitions.
    assert "img.fx-ken-burns" in css


def test_player_css_ken_burns_uses_duration_variable():
    css = _css()
    assert "--fx-duration-ms" in css


# ── blur-fill backdrop (contain_blur) ───────────────────────────────

def test_player_js_handles_contain_blur_fit():
    js = _js()
    # The contain_blur branch builds a wrapper + backdrop instead of a
    # bare object-fit assignment.
    assert "contain_blur" in js
    assert "fit-blur-wrap" in js
    assert "fit-blur-backdrop" in js
    assert "fit-blur-fg" in js


def test_player_js_contain_blur_not_in_known_fits():
    js = _js()
    # contain_blur must NOT be allow-listed in KNOWN_FITS: it is styled
    # via CSS classes, not a direct object-fit value (which would be an
    # invalid CSS keyword).
    start = js.index("KNOWN_FITS")
    snippet = js[start:start + 120]
    assert "contain_blur" not in snippet


def test_player_css_defines_blur_fill_rules():
    css = _css()
    assert ".fit-blur-wrap" in css
    # Two-class selectors so they outrank the base ".layer img" object-fit.
    assert "img.fit-blur-backdrop" in css
    assert "img.fit-blur-fg" in css
    # Backdrop is a blurred cover copy.
    assert "blur(" in css
    assert "object-fit: cover" in css


# ── protocol doc ────────────────────────────────────────────────────

def test_player_js_header_documents_fit_and_effect():
    js = _js()
    head = js[:2000]
    assert "fit" in head
    assert "effect" in head
