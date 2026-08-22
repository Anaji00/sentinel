"""
tests/test_innovation_layer.py

Covers the capability additions from the investigation:

  INN-01  calibration against prediction-market odds
  INN-03  semantic retrieval query surface
  INN-04  out-of-sample edge survival tracking
  INN-05  regime-conditional performance scorecards
"""

import json
import time

import pytest

from services.correlation.edge_survival import (
    COHORT_KEY,
    SUMMARY_KEY,
    EdgeRegistration,
    EdgeSurvivalTracker,
)
from services.reasoning.market_calibration import (
    MarketCalibrationTracker,
    PairedForecast,
)


class FakeRedis:
    """Async Redis stand-in covering the hash ops these modules use."""

    def __init__(self):
        self.h = {}

    def pipeline(self):
        return FakePipe(self)

    async def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update(mapping)
        else:
            d[field] = value
        return 1

    async def hget(self, key, field):
        return self.h.get(key, {}).get(field)

    async def hgetall(self, key):
        return dict(self.h.get(key, {}))

    async def hlen(self, key):
        return len(self.h.get(key, {}))

    async def hdel(self, key, field):
        self.h.get(key, {}).pop(field, None)
        return 1

    async def hincrby(self, key, field, amount=1):
        d = self.h.setdefault(key, {})
        d[field] = int(d.get(field, 0)) + amount
        return d[field]

    async def expire(self, key, ttl):
        return True


class FakePipe:
    def __init__(self, r):
        self.r, self.ops = r, []

    def hset(self, key, field=None, value=None, mapping=None):
        self.ops.append(("hset", key, field, value, mapping)); return self

    def hdel(self, key, field):
        self.ops.append(("hdel", key, field, None, None)); return self

    def hincrby(self, key, field, amount=1):
        self.ops.append(("hincrby", key, field, amount, None)); return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl, None, None)); return self

    async def execute(self):
        for op, key, f, v, m in self.ops:
            if op == "hset":
                await self.r.hset(key, f, v, m)
            elif op == "hdel":
                await self.r.hdel(key, f)
            elif op == "hincrby":
                await self.r.hincrby(key, f, v)
        self.ops = []
        return []


# ── INN-04  EDGE SURVIVAL ────────────────────────────────────────────────────

def _reg(**kw):
    base = dict(edge_id="pearson:NVDA:TSM", source="NVDA", target="TSM",
                relation="STATISTICALLY_CORRELATED_WITH", method="pearson",
                coefficient=0.62, p_value=0.001)
    base.update(kw)
    return EdgeRegistration(**base)


@pytest.mark.anyio
async def test_edge_is_registered_at_discovery():
    t = EdgeSurvivalTracker(FakeRedis())
    assert await t.register(_reg()) is True


@pytest.mark.anyio
async def test_rediscovery_does_not_reset_the_clock():
    """Overwriting on re-discovery would mean no edge ever becomes due."""
    r = FakeRedis()
    t = EdgeSurvivalTracker(r)
    old = time.time() - (8 * 24 * 3600)
    await t.register(_reg(discovered_at=old))

    assert await t.register(_reg(discovered_at=time.time())) is False
    stored = json.loads(await r.hget(COHORT_KEY, "pearson:NVDA:TSM"))
    assert stored["discovered_at"] == pytest.approx(old)


@pytest.mark.anyio
async def test_only_edges_past_the_window_are_due():
    t = EdgeSurvivalTracker(FakeRedis())
    await t.register(_reg(edge_id="pearson:A:B", discovered_at=time.time()))
    await t.register(_reg(edge_id="pearson:C:D",
                          discovered_at=time.time() - 8 * 24 * 3600))

    due = await t.due_for_retest()
    assert [d.edge_id for d in due] == ["pearson:C:D"]


@pytest.mark.anyio
async def test_a_replicating_edge_survives():
    t = EdgeSurvivalTracker(FakeRedis())
    await t.register(_reg())
    v = await t.evaluate("pearson:NVDA:TSM", coefficient=0.48, p_value=0.004)

    assert v.survived is True
    assert v.reason == "replicated"
    # Decayed but intact: survival is persistence, not exact reproduction.
    assert v.coefficient_decay == pytest.approx(0.48 / 0.62, rel=1e-3)


@pytest.mark.anyio
async def test_a_vanished_relationship_does_not_survive():
    t = EdgeSurvivalTracker(FakeRedis())
    await t.register(_reg())
    v = await t.evaluate("pearson:NVDA:TSM", coefficient=0.03, p_value=0.71)
    assert v.survived is False
    assert v.reason == "decayed_below_threshold"


@pytest.mark.anyio
async def test_a_sign_flip_is_a_failure_even_when_strong():
    """A reversed relationship is not the relationship that was discovered."""
    t = EdgeSurvivalTracker(FakeRedis())
    await t.register(_reg(coefficient=0.62))
    v = await t.evaluate("pearson:NVDA:TSM", coefficient=-0.60, p_value=0.001)
    assert v.survived is False
    assert v.reason == "sign_reversed"


@pytest.mark.anyio
async def test_unavailable_series_is_distinguished_from_a_dead_edge():
    """"We could not look" and "it vanished" must not share a bucket."""
    t = EdgeSurvivalTracker(FakeRedis())
    await t.register(_reg())
    v = await t.evaluate("pearson:NVDA:TSM", coefficient=None, p_value=None)
    assert v.survived is False
    assert v.reason == "series_unavailable"


@pytest.mark.anyio
async def test_an_edge_is_judged_once_then_retired():
    r = FakeRedis()
    t = EdgeSurvivalTracker(r)
    await t.register(_reg())
    await t.evaluate("pearson:NVDA:TSM", coefficient=0.5, p_value=0.01)

    assert await r.hget(COHORT_KEY, "pearson:NVDA:TSM") is None
    assert await t.evaluate("pearson:NVDA:TSM", coefficient=0.5, p_value=0.01) is None


@pytest.mark.anyio
async def test_survival_report_aggregates_by_method():
    r = FakeRedis()
    t = EdgeSurvivalTracker(r)
    for i, (coef, p) in enumerate([(0.5, 0.01), (0.5, 0.01), (0.01, 0.9)]):
        eid = f"pearson:X{i}:Y{i}"
        await t.register(_reg(edge_id=eid))
        await t.evaluate(eid, coefficient=coef, p_value=p)

    rep = await t.survival_report()
    assert rep["tested"] == 3
    assert rep["survived"] == 2
    assert rep["survival_rate"] == pytest.approx(2 / 3, rel=1e-3)
    assert rep["by_method"]["pearson"]["survival_rate"] == pytest.approx(2 / 3, rel=1e-3)


@pytest.mark.anyio
async def test_untested_engine_reports_no_rate_rather_than_zero():
    """0% survival reads as catastrophic; it must not stand in for 'unmeasured'."""
    rep = await EdgeSurvivalTracker(FakeRedis()).survival_report()
    assert rep["tested"] == 0
    assert rep["survival_rate"] is None


def test_granger_edge_identity_preserves_direction():
    """A causing B is a different claim from B causing A."""
    ab = EdgeSurvivalTracker.edge_id("NVDA", "TSM", "granger")
    ba = EdgeSurvivalTracker.edge_id("TSM", "NVDA", "granger")
    assert ab != ba


# ── INN-01  MARKET CALIBRATION ───────────────────────────────────────────────

def _pf(**kw):
    base = dict(market_id="mkt-1", question="Will the Fed cut in March?",
                sentinel_probability=0.80, market_probability=0.55)
    base.update(kw)
    return PairedForecast(**base)


@pytest.mark.anyio
async def test_probabilities_outside_zero_one_are_rejected():
    """A conviction on another scale would be graded as a probability."""
    t = MarketCalibrationTracker(FakeRedis())
    assert await t.record_forecast(_pf(sentinel_probability=85.0)) is False
    assert await t.record_forecast(_pf(market_probability=-0.2)) is False
    assert await t.record_forecast(_pf()) is True


@pytest.mark.anyio
async def test_divergence_is_available_without_any_resolution():
    """The usable half today: no settled outcomes are required."""
    t = MarketCalibrationTracker(FakeRedis())
    await t.record_forecast(_pf(market_id="a", sentinel_probability=0.80, market_probability=0.55))
    await t.record_forecast(_pf(market_id="b", sentinel_probability=0.40, market_probability=0.45))

    rep = await t.divergence_report()
    assert rep["paired_forecasts"] == 2
    assert rep["mean_abs_divergence"] == pytest.approx((0.25 + 0.05) / 2, rel=1e-3)
    # Only the material disagreement is surfaced.
    assert [d["market_id"] for d in rep["material_disagreements"]] == ["a"]


@pytest.mark.anyio
async def test_signed_divergence_reveals_systematic_overconfidence():
    t = MarketCalibrationTracker(FakeRedis())
    for i in range(4):
        await t.record_forecast(_pf(market_id=f"m{i}",
                                    sentinel_probability=0.9, market_probability=0.6))
    rep = await t.divergence_report()
    assert rep["mean_signed_divergence"] > 0.2


@pytest.mark.anyio
async def test_skill_score_compares_sentinel_against_the_market():
    t = MarketCalibrationTracker(FakeRedis())
    # Sentinel says 0.9, market says 0.6, outcome is YES -> Sentinel was sharper.
    await t.record_forecast(_pf(market_id="m1", sentinel_probability=0.9, market_probability=0.6))
    await t.resolve("m1", outcome=1)

    rep = await t.skill_report()
    assert rep["resolved_count"] == 1
    assert rep["sentinel_brier"] == pytest.approx(0.01, rel=1e-3)
    assert rep["market_brier"] == pytest.approx(0.16, rel=1e-3)
    assert rep["skill_score"] > 0, "Sentinel forecast better; skill must be positive"


@pytest.mark.anyio
async def test_market_beating_sentinel_yields_negative_skill():
    t = MarketCalibrationTracker(FakeRedis())
    await t.record_forecast(_pf(market_id="m1", sentinel_probability=0.2, market_probability=0.8))
    await t.resolve("m1", outcome=1)
    assert (await t.skill_report())["skill_score"] < 0


@pytest.mark.anyio
async def test_invalid_outcome_is_rejected_not_coerced():
    """A mis-signed outcome inverts the skill score."""
    t = MarketCalibrationTracker(FakeRedis())
    await t.record_forecast(_pf(market_id="m1"))
    assert await t.resolve("m1", outcome=2) is None
    assert await t.resolve("m1", outcome=-1) is None


@pytest.mark.anyio
async def test_skill_report_states_why_it_is_empty():
    """Nulls with a reason, not a zero that reads as a measurement."""
    rep = await MarketCalibrationTracker(FakeRedis()).skill_report()
    assert rep["resolved_count"] == 0
    assert rep["skill_score"] is None
    assert "resolution feed" in rep["note"]


@pytest.mark.anyio
async def test_resolved_forecast_leaves_the_pending_pool():
    r = FakeRedis()
    t = MarketCalibrationTracker(r)
    await t.record_forecast(_pf(market_id="m1"))
    await t.resolve("m1", outcome=0)
    assert (await t.divergence_report())["paired_forecasts"] == 0


# ── INN-03  SEMANTIC RETRIEVAL SURFACE ───────────────────────────────────────

def test_search_routes_are_registered():
    from services.api_gateway.routes.main import app
    paths = {r.path for r in app.routes if hasattr(r, "path")}
    assert "/api/v1/search/similar/{event_id}" in paths
    assert "/api/v1/search/status" in paths


def test_search_does_not_load_an_embedding_model_in_the_gateway():
    """A ~420MB model in the API container is unaffordable on a CPU-only host."""
    import inspect
    from services.api_gateway.routes import search

    src = inspect.getsource(search)
    assert "SentenceTransformer" not in src
    assert "sentence_transformers" not in src
    # Retrieval works from the vector already stored with the event.
    assert "with_vectors=True" in src


def test_similarity_floor_allows_an_honest_empty_result():
    """Unfiltered nearest-neighbour always returns `limit` rows, similar or not."""
    from services.api_gateway.routes.search import MIN_SIMILARITY
    assert 0.0 < MIN_SIMILARITY < 1.0


# ── INN-05  REGIME-CONDITIONAL SCORECARDS ────────────────────────────────────

def test_scorecard_key_partitions_by_strategy_and_regime():
    import inspect
    from services.agents.base import SentinelAgent

    src = inspect.getsource(SentinelAgent._scorecard_key)
    # The unpartitioned key must be preserved or existing history is orphaned.
    assert 'f"sentinel:agents:scorecard:{self.name}"' in src
    assert "strategy" in src and "regime" in src


def test_conditional_scorecard_falls_back_and_reports_its_source():
    """A thin partition is too noisy to size on; the caller must know which
    card it received."""
    import inspect
    from services.agents.base import SentinelAgent

    src = inspect.getsource(SentinelAgent.get_conditional_scorecard)
    assert "min_samples" in src
    for source in ("strategy_regime", "strategy", "global"):
        assert source in src


def test_kelly_uses_the_conditional_scorecard():
    """Pooling regimes biases every position size Kelly produces."""
    import inspect
    from services.agents import quant_trading_engine as Q

    src = inspect.getsource(Q.QuantTradingEngine._process_trading_advisory)
    assert "get_conditional_scorecard" in src, "Kelly still uses the pooled scorecard"
    assert "kelly_basis" in inspect.getsource(Q), "sizing basis is not surfaced"


def test_scorecard_updates_write_the_partition():
    """Without partitioned writes the conditional cards stay permanently empty."""
    import inspect
    from services.agents.base import SentinelAgent

    src = inspect.getsource(SentinelAgent.update_scorecard)
    assert "strategy" in src
    assert "current_regime" in src
