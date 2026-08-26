"""
tests/test_market_series_integrity.py

The chart endpoint invented prices and could not use its own index.

/radar/market-series is the route behind every price chart in the product.
Three defects, measured against the running system:

  1. `price = fin.get("current_price") or ... or 100.0` plotted a flat $100 line
     for any event without a usable price -- a fabricated quote rendered beside
     real market data and indistinguishable from it. Volume had the same shape
     with a stand-in of 1000. Removing them dropped a 717-point response to 480
     genuine points.

  2. `WHERE LOWER(primary_entity_id) IN (...)` wraps the column in a function,
     which makes events_entity_time_idx unusable and turns the query into a full
     scan of the events hypertable on the request path.

  3. For any symbol without stored data the route called out to the US Treasury
     and Yahoo synchronously, uncached, on every request. Measured at 15-60
     seconds per call; warm requests now complete in under two.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

RADAR = ROOT / "services/api_gateway/routes/radar.py"


def _code() -> str:
    # Comments only. Triple-quoted strings are kept: the SQL lives in one, and
    # stripping them removes everything the query assertions are about.
    text = RADAR.read_text(encoding="utf-8")
    text = re.sub(r"^\s*#.*$", "", text, flags=re.M)
    return re.sub(r"^\s*--.*$", "", text, flags=re.M)


def _radar():
    import importlib.util
    spec = importlib.util.spec_from_file_location("radar_route_under_test", RADAR)
    module = importlib.util.module_from_spec(spec)
    sys.modules["radar_route_under_test"] = module
    spec.loader.exec_module(module)
    return module


# -- no invented market data --------------------------------------------------

def test_no_fabricated_price_default():
    """`or 100.0` drew a flat $100 line that looked like a real quote.

    Asserted on the assignment rather than the bare token: the file names the
    old expression where it explains why it was removed.
    """
    code = _code()
    assert not re.search(r"price\s*=.*or\s+100\.0", code)


def test_no_fabricated_volume_default():
    assert "or 1000)" not in _code()


def test_a_point_with_no_price_is_skipped_not_invented():
    code = _code()
    assert "price = _first_price(fin, cryp)" in code
    assert "if price is None:" in code


def test_first_price_returns_none_when_nothing_is_quoted():
    m = _radar()
    assert m._first_price({}, {}) is None
    assert m._first_price({"close": None}, {"price": ""}) is None


def test_first_price_prefers_the_financial_payload():
    m = _radar()
    assert m._first_price({"current_price": 231.4}, {"price": 9.9}) == 231.4
    assert m._first_price({}, {"price": 9.9}) == 9.9


def test_a_zero_price_is_not_a_quote():
    """A zero close is missing data, not a $0 market."""
    m = _radar()
    assert m._first_price({"close": 0.0}, {"price": 42.0}) == 42.0


def test_nan_is_never_reported_as_a_measurement():
    m = _radar()
    assert m._as_float(float("nan")) is None
    assert m._first_price({"close": float("nan")}, {}) is None


def test_as_float_tolerates_junk():
    m = _radar()
    for junk in (None, "", "abc", {}, []):
        assert m._as_float(junk) is None
    assert m._as_float("12.5") == 12.5


# -- the query must be able to use its index ----------------------------------

def test_the_entity_predicate_is_index_friendly():
    code = _code()
    assert "LOWER(primary_entity_id) IN" not in code, "the column is wrapped again"
    assert "primary_entity_id = ANY($1::text[])" in code


def test_the_query_is_time_bounded():
    """Unbounded, it scans the whole hypertable for a chart."""
    assert "occurred_at > NOW() - INTERVAL" in _code()


# -- external calls must not sit on the request path --------------------------

def test_the_spot_fetch_is_cached():
    code = _code()
    assert "async def fetch_on_the_spot_historical" in code
    assert "async def _fetch_on_the_spot_uncached" in code
    assert "sentinel:market_series:spot:" in code


def test_the_cache_ttl_is_short_enough_to_stay_current():
    m = _radar()
    assert 0 < m.ON_THE_SPOT_CACHE_TTL_SEC <= 300


def test_a_cache_failure_is_not_a_request_failure():
    """A Redis outage must degrade to the live fetch, not to an error."""
    code = _code()
    wrapper = code[code.index("async def fetch_on_the_spot_historical"):]
    wrapper = wrapper[:wrapper.index("async def _fetch_on_the_spot_uncached")]
    assert wrapper.count("except Exception:") >= 2


def test_a_malformed_cache_entry_is_ignored():
    code = _code()
    wrapper = code[code.index("async def fetch_on_the_spot_historical"):]
    assert "isinstance(parsed, list)" in wrapper[:1200]


# -- a chart failure must not be silent ---------------------------------------

def test_a_failed_candle_query_is_logged_above_debug():
    """It was logger.debug, so the charts backbone could fail without a trace."""
    code = _code()
    assert "Market-series DB query failed" in code
    assert 'logger.debug(f"TimescaleDB CAGG fallback' not in code


def test_the_serving_path_is_reported():
    """An empty chart and a missing ticker need different fixes."""
    code = _code()
    assert '"source": source' in code
