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

import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


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
    # "in" / "out" are the two pure-zoom keyframes (no pan).
    assert "@keyframes fx-kb-in" in css
    assert "@keyframes fx-kb-out" in css
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


# ── Ken Burns direction (orthogonal zoom×pan, manifest schema 1.4) ──

_KB_PANS = ("left", "right", "up", "down",
            "up_left", "up_right", "down_left", "down_right")


def test_player_js_defines_kb_pan_allowlist():
    js = _js()
    # The pan tokens must be allow-listed so a malformed manifest can't
    # inject an arbitrary class name. Diagonals included.
    assert "KB_PANS" in js
    for p in _KB_PANS:
        assert f'"{p}"' in js


def test_player_js_defines_direction_parser():
    js = _js()
    # A single parser maps a wire token to exactly one fx-kb-* class.
    assert "kbDirectionClass" in js
    # Handles the in_/out_ zoom prefixes that encode zoom + pan.
    assert "in_" in js
    assert "out_" in js


def test_player_js_applies_direction_class_from_cmd():
    js = _js()
    assert "effect_direction" in js
    # The directional class is prefixed fx-kb- and built from the cmd.
    assert "fx-kb-" in js
    assert "cmd.effect_direction" in js


def test_player_css_defines_direction_keyframes():
    css = _css()
    # 2 pure-zoom + 16 zoom×pan = 18 keyframes. Spot-check both zooms,
    # an orthogonal axis, and a diagonal (the feature this expansion adds).
    for name in (
        "fx-kb-in", "fx-kb-out",
        "fx-kb-in-left", "fx-kb-out-right",
        "fx-kb-in-up", "fx-kb-out-down",
        "fx-kb-in-up-left", "fx-kb-out-up-right",
        "fx-kb-in-down-left", "fx-kb-out-down-right",
    ):
        assert f"@keyframes {name}" in css


def test_player_css_direction_override_rules():
    css = _css()
    # Two-class selectors so the directional animation-name outranks the
    # single-class base rule. Every combo (incl. the default "in") gets one.
    for name in (
        "fx-kb-in", "fx-kb-out",
        "fx-kb-out-up-right", "fx-kb-in-down-left",
    ):
        assert f"img.fx-ken-burns.{name}" in css


def test_player_css_full_zoom_pan_matrix():
    css = _css()
    # Exhaustively assert all 18 keyframes + override rules exist so the
    # device renders every combo the CMS can emit.
    for zoom in ("in", "out"):
        assert f"@keyframes fx-kb-{zoom}" in css
        assert f"img.fx-ken-burns.fx-kb-{zoom}" in css
        for pan in _KB_PANS:
            name = f"fx-kb-{zoom}-{pan.replace('_', '-')}"
            assert f"@keyframes {name}" in css
            assert f"img.fx-ken-burns.{name}" in css


# ── protocol doc ────────────────────────────────────────────────────

def test_player_js_header_documents_fit_and_effect():
    js = _js()
    head = js[:2000]
    assert "fit" in head
    assert "effect" in head


# ── Ken Burns parser behaviour (executed, not just scraped) ─────────


def _extract_kb_parser(js: str) -> str:
    """Slice the KB_PANS const + kbDirectionClass() out of player.js so we
    can eval the *real* parser in node, rather than re-implementing it."""
    pans_start = js.index("const KB_PANS")
    pans_end = js.index("];", pans_start) + 2
    pans_src = js[pans_start:pans_end]

    fn_start = js.index("function kbDirectionClass")
    brace = js.index("{", fn_start)
    depth = 0
    i = brace
    while i < len(js):
        if js[i] == "{":
            depth += 1
        elif js[i] == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    fn_src = js[fn_start:i + 1]
    return pans_src + "\n" + fn_src


@pytest.mark.skipif(shutil.which("node") is None, reason="node not available")
def test_kb_direction_parser_behaviour():
    parser_src = _extract_kb_parser(_js())
    cases = {
        # default / pure zoom
        "": "fx-kb-in",
        "in": "fx-kb-in",
        "out": "fx-kb-out",
        # new authoring default (zoom out + diagonal)
        "out_up_right": "fx-kb-out-up-right",
        # zoom + orthogonal pan
        "in_left": "fx-kb-in-left",
        "out_down": "fx-kb-out-down",
        # diagonals
        "in_down_left": "fx-kb-in-down-left",
        "out_up_left": "fx-kb-out-up-left",
        # legacy bare-pan aliases (1.4) render as zoom-in pans
        "left": "fx-kb-in-left",
        "down": "fx-kb-in-down",
        # case / whitespace tolerance
        "  OUT_UP_RIGHT  ": "fx-kb-out-up-right",
        # unknown / malformed -> safe fallback
        "diagonal": "fx-kb-in",
        "in_sideways": "fx-kb-in",
        "out_": "fx-kb-in",
        "zoomy": "fx-kb-in",
    }
    harness = parser_src + "\n" + textwrap.dedent(
        f"""
        const cases = {list(cases.keys())!r};
        const out = cases.map(t => kbDirectionClass(t));
        process.stdout.write(JSON.stringify(out));
        """
    )
    proc = subprocess.run(
        ["node", "-e", harness],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    import json

    got = json.loads(proc.stdout)
    expected = list(cases.values())
    assert got == expected, dict(zip(cases.keys(), got))
