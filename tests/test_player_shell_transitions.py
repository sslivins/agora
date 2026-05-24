"""Contract test: player.js / player.css support the full CMS transition set.

The CMS (cms.schemas.asset.SLIDE_TRANSITIONS) is the source of truth for
which transition IDs are valid on the wire. The Pi player.js shell must
accept all of them; unknown values fall back to ``cut``. This test pins
the JS<->CMS contract by asserting:

  * Every wire ID appears in player.js' ``KNOWN_TRANSITIONS`` array.
  * Every non-trivial mode (anything that isn't ``cut`` or ``fade``,
    both of which use the opacity-only path) has a matching ``.tx-<id>``
    rule in player.css so it actually renders something different.

If the CMS adds a new ID, this test fails until the player side ships.
That's intentional — the player is allowed to lag the CMS for ONE
release (unknown values fall back to ``cut``) but we want CI noise the
moment they diverge.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAYER_JS = REPO_ROOT / "player" / "shell" / "player.js"
PLAYER_CSS = REPO_ROOT / "player" / "shell" / "player.css"


# Mirrors cms.schemas.asset.SLIDE_TRANSITIONS as of CMS PR adding the
# expanded set. Updated here intentionally — keeping the list duplicated
# rather than imported decouples the Pi tests from the CMS package.
WIRE_TRANSITIONS = (
    "cut",
    "fade",
    "fade_black",
    "dissolve",
    "push",
    "wipe",
    "zoom",
)

# Modes that require their own CSS rules. ``cut`` is JS-only (durMs=0)
# and ``fade`` uses the default opacity transition on .layer.active.
CSS_BACKED_MODES = tuple(m for m in WIRE_TRANSITIONS if m not in ("cut", "fade"))


def test_player_js_knows_every_wire_transition():
    text = PLAYER_JS.read_text(encoding="utf-8")
    match = re.search(r"KNOWN_TRANSITIONS\s*=\s*\[([^\]]*)\]", text)
    assert match, "KNOWN_TRANSITIONS array not found in player.js"
    body = match.group(1)
    for wire in WIRE_TRANSITIONS:
        assert f'"{wire}"' in body, (
            f"player.js KNOWN_TRANSITIONS is missing '{wire}'. "
            f"Got: {body.strip()}"
        )


def test_player_css_has_rule_for_each_non_trivial_mode():
    text = PLAYER_CSS.read_text(encoding="utf-8")
    for mode in CSS_BACKED_MODES:
        if mode == "fade_black":
            # fade_black is sequenced JS-side and intentionally has no
            # bespoke CSS rule. Skip.
            continue
        assert f".tx-{mode}" in text, (
            f"player.css is missing a .tx-{mode} rule for the '{mode}' "
            f"transition. Each non-trivial mode needs its own CSS so "
            f"the rendered effect actually differs from a plain fade."
        )


def test_fade_black_is_documented_in_swap_to_docstring():
    """fade_black has no CSS hook — pin a docstring breadcrumb so future
    readers don't think it's missing by accident."""
    text = PLAYER_JS.read_text(encoding="utf-8")
    assert "fade_black" in text, "fade_black must be referenced in player.js"
    # Sanity: the two-stage sequencing branch must exist.
    assert 'mode === "fade_black"' in text, (
        "fade_black requires a dedicated JS branch (two-stage opacity "
        "fade through the black stage background); the simple class-flip "
        "path can't produce a pause-on-black."
    )
