"""
tests/test_pattern_library_typing.py

Precedent retrieval was disabled in production by one type assumption.

    Error fetching similar patterns: 'str' object has no attribute 'isoformat'

`row["created_at"].isoformat()` assumed a datetime. When the column arrives as a
string -- which it does, depending on the cursor -- that raises AttributeError,
and the formatter caught only TypeError and ValueError. The exception escaped to
the caller's handler, which returns [] for any failure, so every scenario the
system generated was reasoned without the historical precedent the library
exists to supply. The log recorded an error three times a minute and the
pipeline reported success.

This is the same class as the guards that were blinded by signals becoming
objects: a shape changes, a narrow except swallows it, and a capability goes
missing without anything failing.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reasoning.pattern_library import _as_iso  # noqa: E402


def test_a_datetime_still_formats():
    assert _as_iso(datetime(2026, 9, 1, 13, 21, 49, tzinfo=timezone.utc)).startswith(
        "2026-09-01T13:21:49"
    )


def test_a_string_timestamp_no_longer_raises():
    """The production failure, verbatim."""
    assert _as_iso("2026-09-01T13:21:49+00:00") == "2026-09-01T13:21:49+00:00"


def test_a_missing_timestamp_is_empty_not_an_exception():
    assert _as_iso(None) == ""
    assert _as_iso("") == ""


def test_an_unexpected_type_degrades_rather_than_raising():
    """The next shape change should cost a malformed date, not the whole
    precedent library."""
    assert _as_iso(1756731709) == "1756731709"


def test_the_row_formatter_no_longer_lets_attributeerror_escape():
    """The narrow except is what turned a bad field into an empty result set."""
    source = (ROOT / "services" / "reasoning" / "pattern_library.py").read_text(encoding="utf-8")
    assert "except (TypeError, ValueError, AttributeError, KeyError, IndexError):" in source


def test_a_row_with_a_string_date_still_produces_a_pattern():
    """End to end through the formatter: one awkward field must not cost the
    scenario its precedent."""
    from services.reasoning.pattern_library import PatternLibrary

    library = PatternLibrary.__new__(PatternLibrary)
    row = {
        "scenario_id": "abc", "headline": "Tanker went dark", "status": "confirmed",
        "confidence_overall": 62, "rule_id": "r1", "correlation_tags": ["tanker"],
        "description": "d", "created_at": "2026-09-01T13:21:49+00:00",
    }
    formatted = library._format_pattern(row)
    assert formatted["headline"] == "Tanker went dark"
    assert formatted["date"] == "2026-09-01T13:21:49+00:00"
