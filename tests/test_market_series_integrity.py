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
     scan of the events hypertable on the request path. Dropping the wrapper
     alone was not the answer either: the column holds six identifier
     namespaces in inconsistent casings, so a bare `=` silently missed every
     row a collector wrote in lower case. The predicate normalises and
     migration 0009 indexes the same expression, which is the pairing this
     file now pins.

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
    """Normalisation in the predicate must have an index that serves it.

    The rule is not "never wrap the column" -- primary_entity_id holds six
    identifier namespaces written in whatever casing each collector chose, so
    the predicate has to normalise or it misses rows. The rule is never to wrap
    it *without* a matching expression index, because that is what turns the
    query behind every chart into a scan of the hypertable.

    The two strings have to agree character for character: the planner matches
    an expression index by the expression, so `upper(x)` here and `UPPER(x)` in
    the migration would silently fall back to a scan.
    """
    code = _code()
    assert "LOWER(primary_entity_id) IN" not in code, "the column is wrapped again"
    assert "upper(primary_entity_id) = ANY($1::text[])" in code
    assert "upper(primary_entity_name) = ANY($1::text[])" in code

    # The optimisation fence. Without it the planner reads the LIMIT as licence
    # to walk the time index backwards and filter, which leaves both expression
    # indexes unused -- measured at 11.8s against 107ms with the fence.
    assert "WITH matched AS MATERIALIZED" in code, "the selection is not fenced"

    migrations = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")
    assert "events_entity_id_upper_time_idx" in migrations, "the predicate has no index"
    assert "ON events (upper(primary_entity_id), occurred_at DESC)" in migrations
    assert "ON events (upper(primary_entity_name), occurred_at DESC)" in migrations


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
    """A Redis outage must degrade to the live fetch, not to an error.

    This counted the literal string "except Exception:" and broke when those
    handlers gained a bound name so their suppression could be counted -- the
    behaviour it protects was unchanged. It now asks the AST how many handlers
    the wrapper has, which is the property, not the spelling.
    """
    import ast

    tree = ast.parse(_code())
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and n.name == "fetch_on_the_spot_historical"
    )
    handlers = [n for n in ast.walk(fn) if isinstance(n, ast.ExceptHandler)]
    assert len(handlers) >= 2, (
        f"only {len(handlers)} exception handler(s) around the cache read and "
        "write; a Redis outage would surface as a request failure."
    )
    # And none of them may re-raise: that is what would turn the outage into
    # an error rather than a fall-through to the live fetch.
    assert not [n for h in handlers for n in ast.walk(h) if isinstance(n, ast.Raise)]


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


# -- a bar needs a price, not a volume ----------------------------------------


def test_a_missing_volume_does_not_drop_the_bar():
    """The comment said "unconditionally persist". The guard was four lines up.

    `if close_p <= 0 or volume <= 0: return None` sat immediately above a block
    headed "Unconditionally persist closed bar to durable TimescaleDB
    tradfi_bars hypertable", so any bar whose volume was unknown never reached
    the INSERT.

    The macro tier serves the eleven GICS sector ETFs and every commodity future
    from Finnhub's quote endpoint, which reports no volume at all. It had
    already been corrected once for answering that with a hardcoded 1000.0 --
    the five-minute aggregate SUMs the column, so every macro bar read exactly
    4000 or 5000, the poll count rather than the market. Sending the honest
    None is what made this guard start dropping them.

    Measured: CL=F, BZ=F, GC=F, SI=F and NG=F stopped on 2026-09-04, the day the
    null was introduced, and tradfi_bars held not one sector ETF bar. The feed
    was publishing XLK at $188.00 once a minute throughout.
    """
    source = (
        ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py"
    ).read_text(encoding="utf-8")

    persist = source.index("INSERT INTO tradfi_bars")
    window = source[max(0, persist - 3000) : persist]
    assert "if close_p <= 0 or volume <= 0" not in window, (
        "the volume test must not gate persistence; a bar needs a price"
    )
    assert "if close_p <= 0:" in window, "a bar with no price is still not a bar"


def test_an_unknown_volume_is_written_as_null_not_zero():
    """Zero claims the ETF did not trade. That is the 1000.0 mistake inverted."""
    source = (
        ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py"
    ).read_text(encoding="utf-8")
    assert 'volume_db = None if p.get("volume") is None else volume' in source


def test_an_unknown_volume_cannot_erase_a_known_one():
    """Two tiers can write the same (ticker, time).

    Without COALESCE the last writer wins, and the winner is whichever feed
    polled last -- so a quote-sourced NULL would overwrite a real Alpaca volume.
    """
    source = (
        ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py"
    ).read_text(encoding="utf-8")
    assert "COALESCE(EXCLUDED.volume, tradfi_bars.volume)" in source


# -- a dead series is not a series --------------------------------------------


def test_a_price_series_is_checked_for_being_alive():
    """A hundred rows from a dead ticker looked exactly like a hundred live ones.

    `fetch_price_series` had no WHERE on time. The commodity series stopped on
    2026-09-04 and the discovery job kept publishing from them for 12.7 days:
    "Statistical Correlation Link: BZ=F <-> CL=F r=1.000, p=0.0000", under FDR
    control at alpha=0.05. A correlation between two frozen series is perfect by
    construction, because neither of them moves.
    """
    source = (
        ROOT / "services" / "correlation" / "statistical_discovery.py"
    ).read_text(encoding="utf-8")
    assert "MAX_SERIES_STALENESS_SEC" in source

    fetch = source.index("async def fetch_price_series")
    body = source[fetch : fetch + 4000]
    assert "seconds')::INTERVAL" in body, (
        "the series must be bounded in time, not only in row count"
    )
    assert "stale_series" in body, "a series going quiet should be counted"


def test_the_staleness_bound_survives_a_weekend_but_not_a_fortnight():
    import services.correlation.statistical_discovery as discovery

    assert discovery.MAX_SERIES_STALENESS_SEC >= 3 * 3600, (
        "an overnight gap is not a dead feed"
    )
    assert discovery.MAX_SERIES_STALENESS_SEC < 12 * 24 * 3600, (
        "12.7 days of silence must not read as a live series"
    )


def test_freshness_is_asked_once_about_the_newest_bar():
    """Filtering rows to the window would truncate healthy series too.

    Six hours of five-minute bars is 72, short of the 100 these tests want, so
    a row filter would weaken every live correlation in order to exclude the
    dead ones.
    """
    source = (
        ROOT / "services" / "correlation" / "statistical_discovery.py"
    ).read_text(encoding="utf-8")
    fetch = source.index("async def fetch_price_series")
    body = source[fetch : fetch + 4000]
    cagg = body.index("FROM tradfi_bars_5m")
    cagg_query = body[cagg : cagg + 260]
    assert "INTERVAL" not in cagg_query, (
        "the series query itself stays unbounded; the freshness check is separate"
    )


# -- vectors outliving their events -------------------------------------------


def test_vector_retention_matches_the_events_retention():
    """A point whose event has been dropped can still be returned by a search.

    Nothing in this platform had ever deleted a vector: 596,200 points had
    accumulated against an `events` hypertable that drops after 90 days, so a
    similarity hit could cite an event_id that no longer resolves. It also
    quietly falsified a comment in `find_similar`, which excludes position
    telemetry from results "for as long as they take to age out".
    """
    import services.correlation.soft_correlator as soft

    assert soft.VECTOR_RETENTION_SEC == 90 * 86400, (
        "the vector window and the events window are two halves of one policy"
    )


def test_the_prune_count_is_exact_not_estimated():
    """Qdrant's estimating counter is not usable on an unindexed payload field.

    Asked how many points were older than 30, 60, 80 and 90 days it answered
    298,097 every time -- almost exactly half the collection. The true answers
    are 731, 202, 167 and 167. The delete selects on the filter so it would
    still have removed the right points, but the log line and the zero-guard
    would both have been reading noise.
    """
    source = (
        ROOT / "services" / "correlation" / "soft_correlator.py"
    ).read_text(encoding="utf-8")
    prune = source.index("async def prune_expired_vectors")
    body = source[prune : prune + 3200]
    assert "exact=True" in body
    assert "exact=False" not in body


def test_the_prune_is_actually_scheduled():
    """The defect this closes is an absent mechanism, not a broken one."""
    source = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert "_vector_retention_loop" in source
    assert "safe_create_task(_vector_retention_loop())" in source
    assert "prune_expired_vectors()" in source
