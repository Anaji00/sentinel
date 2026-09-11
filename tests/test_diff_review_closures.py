"""The nine open items from the first review pass, as tests.

Six were defects that pass introduced; three were gaps it exposed without
closing. Every measurement quoted here was taken against the running deployment
before the repair.
"""
import math
import pathlib

import numpy as np
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


# ── 231: coverage went stale on the paths that return without measuring ──────


def test_a_warm_detector_does_not_report_full_coverage_for_an_unmeasured_score():
    """Reproduced: score 0.0 carrying coverage 1.00 / basis "percentile"."""
    from shared.utils.streaming_detectors import RRCFDetector

    d = RRCFDetector(num_trees=4, window_size=64, shingle_size=4)
    for _ in range(120):
        d.insert(np.array([1.0, 1.0]))

    warm = d.coverage()
    assert warm["fraction"] > 0.0
    assert warm["basis"] in ("percentile", "warmup_curve")

    # A fresh shingle buffer: the next few inserts return 0.0 without measuring.
    d._shingle_buffer.clear()
    score = d.insert(np.array([9.9, 9.9]))
    cov = d.coverage()
    assert score == 0.0
    assert cov["fraction"] == 0.0, (
        "the previous call's coverage was left standing, so a consumer read "
        "the lowest possible anomaly score backed by full coverage"
    )
    assert cov["basis"] == "shingle_warmup"


def test_the_first_point_of_the_fallback_estimator_measures_nothing():
    from shared.utils.streaming_detectors import RRCFDetector

    d = RRCFDetector(num_trees=4, window_size=64, shingle_size=1)
    d._forest = None
    d._ema_mean = None
    d.insert(np.array([1.0, 2.0]))
    cov = d.coverage()
    assert cov["fraction"] == 0.0
    assert cov["basis"] == "cold_start"


def test_every_returning_path_leaves_the_coverage_describing_that_score():
    """The property, not the two instances of it."""
    from shared.utils.streaming_detectors import RRCFDetector

    d = RRCFDetector(num_trees=4, window_size=64, shingle_size=3)
    seen = []
    for i in range(40):
        score = d.insert(np.array([float(i % 7)]))
        cov = d.coverage()
        seen.append((score, cov["fraction"], cov["basis"]))
        assert cov["basis"] != "none", i
        if score == 0.0 and cov["basis"] in ("shingle_warmup", "cold_start"):
            assert cov["fraction"] == 0.0
    assert any(b in ("shingle_warmup", "cold_start") for _, _, b in seen)


# ── 232: the overnight exemption hid a crypto detector failure ───────────────

ENRICH = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")


def test_the_silence_monitor_groups_by_domain_as_well_as_type():
    """MARKET_ANOMALY is emitted by the crypto and the equity candle paths both."""
    # Scoped to the silence query: the score-diversity query below it groups by
    # type alone and is right to.
    j = ENRICH.index('"""', ENRICH.index("AS minutes_silent"))
    i = ENRICH.rindex("SELECT type,", 0, j)
    silence_query = ENRICH[i:j]
    assert "GROUP BY type, domain" in silence_query
    assert "END AS domain" in silence_query


def test_a_continuously_traded_domain_is_never_excused_by_the_closing_bell():
    from services.enrichment.main import (
        DETECTOR_SILENCE_CONTINUOUS_DOMAINS,
        DETECTOR_SILENCE_MARKET_HOURS_TYPES,
    )

    assert "market_anomaly" in DETECTOR_SILENCE_MARKET_HOURS_TYPES
    assert "crypto" in DETECTOR_SILENCE_CONTINUOUS_DOMAINS
    assert "prediction" in DETECTOR_SILENCE_CONTINUOUS_DOMAINS
    # Equities are still excused; that is what the exemption is for.
    assert "tradfi" not in DETECTOR_SILENCE_CONTINUOUS_DOMAINS
    assert "edomain not in DETECTOR_SILENCE_CONTINUOUS_DOMAINS" in ENRICH


def test_the_platform_already_recorded_that_this_type_needs_the_event():
    from shared.models.events import AMBIGUOUS_EVENT_TYPES, EventType

    assert EventType.MARKET_ANOMALY in AMBIGUOUS_EVENT_TYPES


# ── 233: mixed known and unknown sources were counted as one ─────────────────


def test_an_unknown_source_is_not_folded_into_a_known_one():
    """Measured: three all-unknown gave 3.0, one known plus two unknown gave 2.099."""
    from services.correlation.main import _independent_support

    all_unknown = [{"event_id": str(i)} for i in range(3)]
    mixed = [{"source": "reuters"}, {"event_id": "b"}, {"event_id": "c"}]

    assert _independent_support(all_unknown) == pytest.approx(3.0)
    assert _independent_support(mixed) == pytest.approx(3.0), (
        "the docstring promised an unknown source would not penalise history it "
        "cannot judge, and in the mixed case it did"
    )


def test_repeats_within_a_known_source_still_discount():
    from services.correlation.main import _independent_support

    one_source_thrice = [{"source": "reuters"}] * 3
    three_sources = [{"source": "reuters"}, {"source": "ap"}, {"source": "afp"}]

    assert _independent_support(one_source_thrice) == pytest.approx(1.0 + math.log1p(2))
    assert _independent_support(three_sources) == pytest.approx(3.0)
    assert _independent_support(one_source_thrice) < _independent_support(three_sources)


def test_the_boundary_case_that_made_this_easy_to_miss():
    """One sourced event plus nine unknown, for the 48 hours where both exist."""
    from services.correlation.main import _independent_support

    events = [{"source": "coinbase_candles"}] + [{"event_id": str(i)} for i in range(9)]
    assert _independent_support(events) == pytest.approx(10.0)


# ── 234: a Literal enum turned a wrong field into total loss of the brief ────


def test_an_unrecognised_classification_costs_one_field_not_the_brief():
    """A retry drops the grammar (`format="json"`), so prose still arrives."""
    from services.agents.macro_intelligence_engine import RatesRegimeBrief

    brief = RatesRegimeBrief(
        curve_state="2Y Yield: 4.390% | 10Y Yield: 4.800%",
        yield_spread_2y10y_bps=41.0,
        breakeven_inflation_bps=230.0,
        tips_yield=1.9,
        credit_spread_widening_signal="crypto",
        regime_summary="…",
        macro_risk_level="ELEVATED",
    )
    assert brief.curve_state == "Unclassified"
    assert brief.credit_spread_widening_signal == "Unclassified"
    assert brief.yield_spread_2y10y_bps == 41.0, (
        "the measured spread is the only field the regime derivation reads; "
        "dead-lettering the brief threw it away with the bad enum"
    )


def test_capitalisation_is_not_a_reason_to_lose_a_brief():
    from services.agents.macro_intelligence_engine import RatesRegimeBrief

    brief = RatesRegimeBrief(
        curve_state="inverted",
        yield_spread_2y10y_bps=-12.0,
        breakeven_inflation_bps=210.0,
        tips_yield=1.7,
        credit_spread_widening_signal="  severe stress  ",
        regime_summary="…",
        macro_risk_level="CRITICAL",
    )
    assert brief.curve_state == "Inverted"
    assert brief.credit_spread_widening_signal == "Severe Stress"


def test_the_regime_still_derives_from_the_measurement():
    from shared.utils.regime import regime_from_brief

    assert regime_from_brief({"yield_spread_2y10y_bps": -12.0}) == "inverted"
    # An unclassified curve_state must not change the answer: it is not read.
    assert regime_from_brief(
        {"curve_state": "Unclassified", "yield_spread_2y10y_bps": 41.0}
    ) == "normal_steepening"


# ── 236: coverage was attached to every score and read by nothing ────────────


def test_coverage_reaches_the_event():
    from shared.models.events import AnomalyBreakdown

    b = AnomalyBreakdown(composite_score=0.4, coverage_fraction=0.1, coverage_basis="warmup_curve")
    assert b.coverage_fraction == 0.1
    assert b.coverage_basis == "warmup_curve"
    # Absent stays absent: a path that does not report it is not penalised.
    assert AnomalyBreakdown(composite_score=0.4).coverage_fraction is None


def test_a_warm_up_score_ranks_below_a_measured_one():
    """The half of the repair that did not exist: the budget admitted on score alone."""
    from services.reasoning.main import _reasoning_priority

    class _Cluster:
        def __init__(self, coverage):
            self.alert_tier = "ALERT"
            self.confidence_score = 0.8
            self.supporting_event_ids = ["a", "b", "c"]
            self.metrics_summary = {"domain_count": 2, "evidence_coverage": coverage}

    measured = _reasoning_priority((_Cluster(1.0), None))
    warmup = _reasoning_priority((_Cluster(0.0), None))
    unknown = _reasoning_priority((_Cluster(None), None))

    assert warmup < measured
    assert unknown == measured, (
        "absent coverage is not zero coverage; a cluster from a path that does "
        "not report it must keep its priority exactly as before"
    )


def test_coverage_is_a_floor_not_a_gate():
    """A cold CRITICAL cross-domain cluster still outranks a warm ordinary one."""
    from services.reasoning.main import _reasoning_priority, REASONING_COVERAGE_FLOOR

    class _Cluster:
        def __init__(self, tier, domains, coverage):
            self.alert_tier = tier
            self.confidence_score = 0.8
            self.supporting_event_ids = ["a", "b", "c"]
            self.metrics_summary = {"domain_count": domains, "evidence_coverage": coverage}

    cold_critical = _reasoning_priority((_Cluster("CRITICAL", 3, 0.0), None))
    warm_monitor = _reasoning_priority((_Cluster("MONITOR", 1, 1.0), None))
    assert cold_critical > warm_monitor
    assert 0.0 < REASONING_COVERAGE_FLOOR < 1.0


def test_the_correlation_layer_carries_coverage_onto_the_cluster():
    from services.correlation.main import evidence_coverage

    class _B:
        coverage_fraction = 0.25

    class _E:
        anomaly_breakdown = _B()

    class _NoBreakdown:
        anomaly_breakdown = None

    assert evidence_coverage(_E()) == 0.25
    assert evidence_coverage(_NoBreakdown()) is None


# ── 238: the regime gated position size and nothing else ─────────────────────


def test_a_discovered_correlation_records_the_regime_it_was_learned_under():
    src = (ROOT / "services" / "correlation" / "statistical_discovery.py").read_text(encoding="utf-8")
    assert "learned_regime = await current_regime(self.redis)" in src
    # All three edge kinds: pearson and both Granger directions.
    assert src.count('"regime": learned_regime') == 3


def test_the_quant_engine_asks_the_shared_regime_rather_than_grepping_a_blob():
    src = (ROOT / "services" / "agents" / "quant_trading_engine.py").read_text(encoding="utf-8")
    assert "rates_regime = await current_regime(self.redis)" in src
    assert 'any(s in rates_regime.lower()' not in src, (
        "this searched a JSON document for the substring 'inverted', which "
        "matches curve_state prose and regime_summary prose alike"
    )
