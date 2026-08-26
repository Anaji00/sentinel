"""
tests/test_earnings_proximity.py

Size alone does not distinguish rebalancing from positioning.

The earnings calendar was collected, cached in Redis for a week, and read by
exactly one consumer -- into an LLM prompt string. Nothing in the scoring or
tagging of a trade consulted it, so a $2.4M block the day before a print ranked
identically to the same block in a quiet week, and the feed gave a reader no way
to tell them apart.

That is the purpose of the calendar: the forward-looking dates and estimates are
context for flow, not a results feed. A large trade with a report approaching is
the signal.
"""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def tradfi():
    spec = importlib.util.spec_from_file_location(
        "enrichment_tradfi_earnings", ROOT / "services/enrichment/enrichers/tradfi.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["enrichment_tradfi_earnings"] = module
    spec.loader.exec_module(module)
    return module


NOW = datetime(2026, 8, 25, tzinfo=timezone.utc)
BLOCK = 2_000_000.0


# -- reading the calendar -----------------------------------------------------

def test_days_until_counts_whole_days(tradfi):
    assert tradfi._days_until("2026-08-25", NOW) == 0
    assert tradfi._days_until("2026-08-26", NOW) == 1
    assert tradfi._days_until("2026-09-01", NOW) == 7


def test_a_past_report_is_negative(tradfi):
    """The print has happened; the anticipation trade is over."""
    assert tradfi._days_until("2026-08-20", NOW) == -5


def test_a_missing_or_malformed_date_is_not_a_guess(tradfi):
    for value in ("", None, "garbage", "2026-13-45", 12345):
        assert tradfi._days_until(value, NOW) is None


# -- the lift ------------------------------------------------------------------

def test_the_lift_is_largest_on_the_day(tradfi):
    assert tradfi._earnings_proximity_lift(0, BLOCK) == tradfi.EARNINGS_MAX_LIFT


def test_the_lift_tapers_with_distance(tradfi):
    lifts = [tradfi._earnings_proximity_lift(d, BLOCK) for d in (0, 1, 2, 3, 5)]
    assert lifts == sorted(lifts, reverse=True), "closer must never score lower"
    assert len(set(lifts)) == 5, "distinct distances collapsed to one value"


def test_nothing_outside_the_window(tradfi):
    edge = tradfi.EARNINGS_PROXIMITY_DAYS
    assert tradfi._earnings_proximity_lift(edge, BLOCK) == 0.0
    assert tradfi._earnings_proximity_lift(edge + 1, BLOCK) == 0.0


def test_nothing_for_a_report_already_past(tradfi):
    assert tradfi._earnings_proximity_lift(-1, BLOCK) == 0.0


def test_nothing_when_the_date_is_unknown(tradfi):
    assert tradfi._earnings_proximity_lift(None, BLOCK) == 0.0


def test_a_small_trade_is_not_positioning(tradfi):
    """Otherwise every retail ticket in earnings season lifts the feed."""
    assert tradfi._earnings_proximity_lift(1, 50_000) == 0.0
    assert tradfi._earnings_proximity_lift(1, tradfi.EARNINGS_MIN_NOTIONAL_USD) > 0


def test_the_lift_is_bounded_headroom_not_a_multiplier(tradfi):
    """The same saturation trap the rest of this enricher was fixed for."""
    for base in (0.2, 0.6, 0.95):
        assert tradfi._lift(base, tradfi.EARNINGS_MAX_LIFT) <= 1.0
    lifted = [tradfi._lift(b, tradfi.EARNINGS_MAX_LIFT) for b in (0.2, 0.6, 0.95)]
    assert lifted == sorted(lifted), "ordering lost"


# -- the reader must be able to see it ----------------------------------------

def test_the_headline_names_the_catalyst(tradfi):
    today = datetime.now(timezone.utc).date().isoformat()
    out = tradfi._equity_headline("DUMP", "HEI", 2_400_000, 0.91,
                                  {"report_date": today, "session": "amc"})
    assert "Earnings today" in out
    assert "after close" in out


def test_the_headline_is_unchanged_without_a_catalyst(tradfi):
    out = tradfi._equity_headline("SWEEP", "NVDA", 8_100_000, 0.77, None)
    assert "Earnings" not in out
    assert "NVDA" in out


def test_a_stale_report_date_is_not_announced(tradfi):
    out = tradfi._equity_headline("SWEEP", "NVDA", 8_100_000, 0.77,
                                  {"report_date": "2020-01-01"})
    assert "Earnings" not in out


def test_the_trade_is_tagged_not_only_scored():
    """A number cannot tell an analyst why the number moved."""
    src = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert 'tags.append("pre_earnings_positioning")' in src
    assert 'tags.append("earnings_imminent")' in src


def test_the_report_date_travels_with_the_event():
    src = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert 'earnings_report_date=(earnings or {}).get("report_date")' in src


def test_the_calendar_is_fetched_once_per_batch():
    """A GET per trade would add a round trip to the hot path."""
    src = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert "async def _fetch_earnings_calendar" in src
    assert "mget(keys)" in src


def test_an_unavailable_calendar_does_not_fail_enrichment():
    src = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    fn = src[src.index("async def _fetch_earnings_calendar"):]
    fn = fn[:fn.index("async def _enrich_equity_trade_batch")]
    assert "return {}" in fn
