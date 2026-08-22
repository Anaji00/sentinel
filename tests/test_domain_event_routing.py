"""
tests/test_domain_event_routing.py

Guards /api/v1/events/{domain}.

The endpoint projected a domain's JSONB column into `domain_data` but never
filtered by domain, so every domain route returned the newest events of ANY
type. /events/maritime answered with news headlines carrying an empty
domain_data, and the global map -- which plots that response -- had no
coordinates to draw even though 56,000 vessel positions were stored.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
ROUTE = REPO / "services" / "api_gateway" / "routes" / "events.py"


def _src() -> str:
    return ROUTE.read_text(encoding="utf-8")


def test_domain_query_filters_by_the_domain_column():
    """Projecting a column is not filtering by it."""
    src = _src()
    assert "IS NOT NULL" in src, (
        "domain branch does not require the domain column to be populated; "
        "every domain would return the same rows"
    )


def test_domain_query_excludes_empty_json_objects():
    """COALESCE(col,'{}') made absent data indistinguishable from present data."""
    src = _src()
    # The clause lives inside an f-string, so braces appear doubled in source.
    assert "NOT IN ('{}', 'null')" in src or "NOT IN ('{{}}', 'null')" in src, (
        "rows with an empty domain_data are not excluded"
    )


def test_domain_data_is_not_coalesced_to_an_empty_object():
    """An empty object reads as 'this event has no detail', not 'wrong domain'."""
    src = _src()
    domain_branch = src[src.index("target_column = DOMAIN_TO_COLUMN[domain]"):]
    assert "COALESCE({target_column}, '{{}}'::jsonb)" not in domain_branch, (
        "domain_data is still coalesced to {}, hiding the filter's absence"
    )


def test_every_mapped_domain_has_a_column():
    """A domain in the map with no column would interpolate None into SQL."""
    src = _src()
    block = src[src.index("DOMAIN_TO_COLUMN = {"):src.index("}", src.index("DOMAIN_TO_COLUMN = {"))]
    pairs = re.findall(r'"(\w+)"\s*:\s*"(\w+)"', block)
    assert pairs, "DOMAIN_TO_COLUMN did not parse"
    for domain, column in pairs:
        assert column, f"{domain} maps to an empty column"


def test_interpolated_column_never_comes_from_the_request():
    """The column name is interpolated, so it must come from the fixed map."""
    src = _src()
    assert "target_column = DOMAIN_TO_COLUMN[domain]" in src, (
        "column name must be looked up in the fixed mapping, never taken from input"
    )
    # An unmapped domain falls into the unfiltered branch rather than
    # interpolating an attacker-supplied string.
    assert "domain not in DOMAIN_TO_COLUMN" in src


def test_maritime_maps_to_vessel_data():
    """The map plots this response; a wrong column silently empties it."""
    src = _src()
    assert '"maritime": "vessel_data"' in src
