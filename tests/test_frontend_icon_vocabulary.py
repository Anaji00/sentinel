"""The UI's marks are drawn, not typed.

116 emoji glyphs were carrying meaning across 19 components. Emoji are the
wrong instrument for a console: they render as a different picture per
platform, they carry a colour the design system does not control, they cannot
inherit `currentColor` so they never reflect severity, and their accessible
name is whatever the font vendor chose -- a screen reader announced the anchor
in a vessel row as "anchor", a boat part, rather than "sanctioned vessel".

These tests hold the line. They do not check that the UI is pretty; they check
that a claim the UI makes visually is backed by something the code controls.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"

# The pictographic ranges. Deliberately excludes arrows, box drawing and maths
# operators: `→` in prose and `Σ` in a methodology formula are typography, not
# iconography, and replacing them with SVG would be worse.
PICTOGRAPHIC = re.compile("[\U0001f000-\U0001faff☀-➿⬀-⯿️]")

# Comments describing this problem naturally quote the glyph being removed.
COMMENT = re.compile(r"^\s*(//|\*|/\*)")


def _sources() -> list[Path]:
    return [
        p
        for p in sorted(SRC.rglob("*.ts*"))
        if "__tests__" not in p.parts and "node_modules" not in p.parts
    ]


def test_no_pictographic_glyphs_in_the_interface() -> None:
    offenders: list[str] = []
    for path in _sources():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
            if COMMENT.match(line):
                continue
            for match in PICTOGRAPHIC.finditer(line):
                glyph = match.group()
                try:
                    name = unicodedata.name(glyph)
                except ValueError:
                    name = f"U+{ord(glyph):05X}"
                offenders.append(f"{path.relative_to(SRC)}:{lineno} {name}")

    assert not offenders, (
        "emoji carry meaning the design system cannot control. Use a component "
        "from components/ui/icons.tsx:\n  " + "\n  ".join(offenders)
    )


def test_no_element_was_left_empty_where_a_glyph_was_removed() -> None:
    """An empty `<button>` is a control with no affordance.

    Stripping a glyph out of JSX text leaves `<span></span>` behind, which type
    checks, renders, and says nothing. Four close controls shipped that way.
    """
    empty = re.compile(r"></(?:span|button|div|p|h[1-6]|td|th|li)>")
    offenders = [
        f"{path.relative_to(SRC)}:{lineno}"
        for path in _sources()
        for lineno, line in enumerate(path.read_text(encoding="utf-8").split("\n"), 1)
        if empty.search(line)
    ]
    assert not offenders, "elements rendering nothing:\n  " + "\n  ".join(offenders)


def test_every_icon_inherits_colour_and_size() -> None:
    """An icon that hardcodes its colour cannot express severity.

    The reason for leaving emoji was that they came pre-coloured. A drawn icon
    with `stroke="#22d3ee"` baked in repeats the mistake in SVG.
    """
    source = (SRC / "components" / "ui" / "icons.tsx").read_text(encoding="utf-8")
    literals = re.findall(
        r'''(?:stroke|fill)=["'](#[0-9a-fA-F]{3,8}|rgb[^"']*)["']''', source
    )
    assert not literals, f"icons must inherit currentColor, found: {literals}"


def test_custom_icons_are_drawn_rather_than_imported_when_the_concept_is_ours() -> None:
    """The three platform-specific marks exist and are not stock icons.

    A `vessel_dark` finding is about silence, not about a boat. Reaching for
    lucide's `Ship` there would draw the subject and drop the claim.
    """
    source = (SRC / "components" / "ui" / "icons.tsx").read_text(encoding="utf-8")
    for name in ("IconDarkVessel", "IconShipToShip", "IconChokepoint"):
        assert f"export const {name}" in source, f"{name} is missing"
        body = source.split(f"export const {name}")[1].split("export const")[0]
        assert "<svg" in body, f"{name} must be drawn here, not re-exported"


@pytest.mark.parametrize(
    "state, mark",
    [
        ("live_measurement", "IconRadar"),
        ("llm_inference", "IconModel"),
        ("disclosed_placeholder", "IconPending"),
    ],
)
def test_provenance_states_are_visually_distinct(state: str, mark: str) -> None:
    """Colour alone is not a distinction.

    The provenance badge is the one component whose whole job is to say where a
    number came from. All four of its states briefly rendered an empty box
    followed by text, leaving hue as the only separator.
    """
    source = (SRC / "components" / "ProvenanceBadge.tsx").read_text(encoding="utf-8")
    block = source.split(f"case '{state}':")[1].split("break;")[0]
    assert f"Mark = {mark}" in block, f"{state} does not set a distinct mark"


def test_simulated_is_not_drawn_as_deterministic() -> None:
    """A simulated series is not a computed one, and must not look like one."""
    source = (SRC / "components" / "ProvenanceBadge.tsx").read_text(encoding="utf-8")
    assert "isSynthetic ? IconSimulated : IconMethodology" in source
