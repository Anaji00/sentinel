"""Defects the 5 Nov 2026 stress test (tests/stress) found, each held in place.

Every case here failed before its fix, on inputs the real feeds could produce or on
evidence the shipped rules were written to refuse.
"""

import asyncio
import math
import pathlib
import subprocess
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.correlation_rules import max_distinct_clauses  # noqa: E402
from shared.models.events import MOVE_BLIND_TYPES  # noqa: E402
from shared.utils import options_flow as ofl  # noqa: E402
from shared.utils import stock_evidence as se  # noqa: E402
from shared.utils.equities import parse_occ_option_symbol  # noqa: E402
from shared.utils.quote_cache import parse_quote  # noqa: E402

UTC = timezone.utc


def test_an_option_symbol_with_an_impossible_date_is_not_a_contract() -> None:
    for sym in ("LGL261301C00100000", "JWX260005C00100000", "USF260230C00100000", "AAA260431P00100000"):
        assert parse_occ_option_symbol(sym) is None, sym
    assert parse_occ_option_symbol("AAPL280229C00100000")["expiry"] == "2028-02-29"     # a real leap day
    assert parse_occ_option_symbol("AAPL261218C00220000")["strike"] == 220.0


def test_a_cached_price_is_finite_or_nothing() -> None:
    for bad in ("inf", "1e999", "-inf", "nan", b"\xff\xfe", "-5", "0", ""):
        assert parse_quote(bad) is None, bad
    assert parse_quote("93.23") == 93.23 and parse_quote(b"7") == 7.0


def test_unknown_numbers_do_not_become_premium() -> None:
    nan, inf = float("nan"), float("inf")
    assert ofl.intrinsic("CALL", nan, 100.0) is None and ofl.intrinsic("PUT", 90.0, inf) is None
    assert ofl.extrinsic_premium(2.0, 10, "PUT", nan, 100.0) is None          # was the whole premium
    assert ofl.extrinsic_premium(2.0, -5, "CALL", 90.0, 100.0) is None        # a negative size is not a premium
    assert ofl.extrinsic_premium(inf, 10, "CALL", 90.0, 100.0) is None


def test_a_naive_timestamp_does_not_crash_the_side_read() -> None:
    aware = datetime(2026, 11, 5, 15, 0, tzinfo=UTC)
    naive = datetime(2026, 11, 5, 15, 0)
    assert ofl.aggressor(1.1, 1.0, 1.1, naive, aware) == ofl.aggressor(1.1, 1.0, 1.1, aware, aware) == "buy"
    assert ofl.aggressor(1.1, 1.0, 1.1, aware, naive) == "buy"


def test_a_bigger_premium_never_scores_lower() -> None:
    from services.enrichment.enrichers.tradfi import OPTIONS_PREMIUM_REFERENCE_USD, _options_premium_score

    ladder = [OPTIONS_PREMIUM_REFERENCE_USD * f for f in (0.5, 0.9, 0.99, 0.999, 1.0, 1.001, 1.3, 4.0)]
    scores = [_options_premium_score(p) for p in ladder]
    assert scores == sorted(scores) and max(scores) < 1.0


def test_a_print_with_a_bad_field_is_kept_or_counted_not_raised() -> None:
    from tests.stress.round2_enrichment import _enricher, _opt_payload, _raw
    from tests.stress.core import rng

    r = rng(7, "reg")
    enr = _enricher()

    def go(**over):
        return asyncio.run(enr.enrich(_raw(_opt_payload(r, **over))))

    for field in ("price", "volume", "premium_usd"):
        assert go(**{field: None}) is None and go(**{field: "abc"}) is None and go(**{field: float("nan")}) is None
    e = go(strike="abc", implied_volatility="N/A", open_interest="1,000", expiry="2026-13-45")
    fd = e.financial_data
    # An unreadable strike or expiry is recovered from the contract symbol; the rest are unknown.
    assert fd.strike == 100.0 and fd.expiry == "2026-12-18"
    assert fd.implied_volatility is None and fd.open_interest is None and math.isfinite(e.anomaly_score)
    bare = go(strike="abc", expiry="2026-13-45", option_symbol=None).financial_data
    assert bare.strike is None and bare.expiry is None
    assert go(implied_volatility=float("nan")).financial_data.implied_volatility is None
    assert go(open_interest=-5).financial_data.open_interest is None


def test_cashtags_after_a_space_are_found_and_amounts_are_not() -> None:
    from services.enrichment.enrichers.news import RE_TICKER_EXTRACT

    def found(text):
        return {g.upper() for groups in RE_TICKER_EXTRACT.findall(text) for g in groups if g}
    assert found("$tsla recall widens") == {"TSLA"} and found("shares of $NVDA rose") == {"NVDA"}
    assert found("a $5M fine and $100 billion more") == set()


def test_an_all_capitals_headline_has_no_prose_tickers() -> None:
    from tests.stress.round2_enrichment import Scorer, _raw
    from services.enrichment.enrichers.news import NewsEnricher

    news = NewsEnricher(Scorer(), None, None)
    e = asyncio.run(news.enrich(_raw({"title": "THE END OF AN ERA IN EUROPE AND ASIA", "summary": "", "reliability": 0.9}, source="bbc_world")))
    assert e is None or not {"END", "ERA"} & set(e.named_entities or [])


def test_the_next_session_skips_exchange_holidays() -> None:
    assert se.next_session(date(2026, 11, 25)) == date(2026, 11, 27)      # Thanksgiving is Thursday
    assert se.next_session(date(2026, 7, 2)) == date(2026, 7, 6)          # July 3 is the observed holiday
    assert se.next_session(date(2026, 11, 6)) == date(2026, 11, 9)        # an ordinary Friday
    assert se.reports_by_next_session("2026-11-27", date(2026, 11, 25))


def test_conviction_is_finite_and_not_negative() -> None:
    for v, want in ((float("nan"), 0.0), (-float("inf"), 0.0), (-1.0, 0.0), (0.3, 0.3), (9.0, se.MAX_FORECAST_CONVICTION), (float("inf"), 0.0)):
        assert se.capped_conviction(v) == want, v


def test_one_event_is_not_two_pieces_of_evidence() -> None:
    assert max_distinct_clauses([]) == 0
    assert max_distinct_clauses([{"a"}, {"a"}]) == 1
    assert max_distinct_clauses([{"a"}, {"a", "b"}]) == 2
    assert max_distinct_clauses([{"a", "b"}, {"a", "b"}, {"a", "b"}]) == 2
    assert max_distinct_clauses([{"a"}, set(), {"b"}]) == 2


def _fires(rule_id, beats, trigger):
    from tests.integration.market_scenarios import Scenario, run_scenario

    clusters, _, _ = asyncio.run(run_scenario(Scenario("t", "stress", "t", trigger, beats, rule_id)))
    return any(c.rule_id == rule_id for c in clusters)


def test_a_rule_asking_for_two_things_does_not_fire_on_one_event() -> None:
    from tests.integration.market_scenarios import Beat

    rule = "rule_crypto_equity_contagion"
    trigger = Beat("crypto_liquidation", "ZZ", 0, 0.8, source="t", tags=["exposes:ZZ"])
    one = Beat("market_anomaly", "ZZ", 20, 0.8, source="t", tags=["exposes:ZZ"])
    other = Beat("crypto_trade", "ZZ", 30, 0.8, source="t", tags=["exposes:ZZ"])
    assert not _fires(rule, [one], trigger)             # one market_anomaly answered both clauses
    assert _fires(rule, [one, other], trigger)           # a second, different observation is a convergence


def test_no_clause_filtering_on_a_move_lists_a_type_that_has_none() -> None:
    from services.correlation.main import SHIPPED_RULES

    bad = [f"{r['rule_id']}: {sorted(set(c['event_types']) & MOVE_BLIND_TYPES)}" for r in SHIPPED_RULES for c in r["correlations"]
           if c.get("min_abs_move_pct") and set(c["event_types"]) & MOVE_BLIND_TYPES]
    assert not bad, bad


def test_the_stress_rounds_find_no_defects() -> None:
    """The whole harness, one seed, in a subprocess so its test-only patch of the Redis double stays out of this session."""
    out = ROOT / "reports" / "System stress test - latest.md"
    done = subprocess.run([sys.executable, "-m", "tests.stress.run_stress", "--seeds", "1", "--out", str(out.relative_to(ROOT)), "--title", "System stress test: latest"],
                          cwd=ROOT, capture_output=True, text=True, timeout=900, env={**__import__("os").environ, "PYTHONUTF8": "1"})
    assert done.returncode == 0, (done.stdout[-1200:] + done.stderr[-600:])
