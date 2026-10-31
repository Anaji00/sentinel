"""The analyst's arithmetic: priors from the model's rank, ensembles, and citation checks."""

import pathlib

import pytest
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import analyst as an  # noqa: E402


def test_the_models_rank_is_a_modest_prior() -> None:
    assert an.prior_from_percentile(0.5) == 0.5
    assert abs(an.prior_from_percentile(1.0) - 0.7) < 1e-12
    assert abs(an.prior_from_percentile(0.0) - 0.3) < 1e-12
    assert an.prior_from_percentile(None) == 0.5


def test_samples_average_then_meet_the_prior() -> None:
    samples = [{"direction": "up", "probability": 0.8}, {"direction": "up", "probability": 0.6},
               {"direction": "down", "probability": 0.6}]
    # p_up: 0.8, 0.6, 0.4 -> mean 0.6; with a neutral prior -> 0.55.
    direction, conviction, p = an.combine(samples, 0.5)
    assert direction == "up" and abs(p - 0.55) < 1e-9 and abs(conviction - 0.55) < 1e-9
    d2, c2, p2 = an.combine([{"direction": "flat", "probability": 0.9}], 0.3)
    assert d2 == "down" and abs(p2 - 0.4) < 1e-9 and abs(c2 - 0.6) < 1e-9
    assert an.combine([], 0.5) is None
    assert an.p_up({"direction": "up", "probability": "junk"}) == 0.5


def test_a_claim_must_cite_what_it_was_given() -> None:
    assert an.verified({"cited": ["F1", "F3"]}, n_facts=3)
    assert not an.verified({"cited": ["F7"]}, n_facts=3)                     # a fact it was never shown
    assert not an.verified({"cited": [], "rationale": "strong momentum"}, n_facts=3)
    assert an.verified({"cited": [], "rationale": "per F2 the rank is high"}, n_facts=3)


def test_facts_are_numbered_in_a_fixed_order_and_skip_unknowns() -> None:
    lines = an.facts("NVDA", 0.91, "monthly", {"last 5 sessions": 0.034, "last 21 sessions": None},
                     [("2026-10-28", "Nvidia unveils chip", 0.4)], [("2026-09-02", "NVDA", "Export curbs eased")],
                     "Company News & Trading (NVDA)")
    assert lines[0].startswith("F1: The alert") and lines[1].startswith("F2: The stock model ranks NVDA at percentile 91 of 100")
    assert any("+3.4%" in x for x in lines) and not any("21 sessions" in x for x in lines)
    assert lines[-1].startswith("F5: Similar past event")
    system, user = an.prompt("NVDA", lines)
    assert "S&P 500" in system and "F5:" in user


# ── The agent, with its stores and model faked ───────────────────────────────

import asyncio  # noqa: E402
import json  # noqa: E402
from typing import Any, Dict, List, Optional  # noqa: E402

from services.agents import analyst_agent as aa  # noqa: E402
from services.agents.base import InferenceShed  # noqa: E402


class _Raw:
    def __init__(self, scores: Dict[str, Any]) -> None:
        self.kv: Dict[str, str] = {"scores": json.dumps(scores)}
        self.sets: Dict[str, set] = {}
        self.lists: Dict[str, List[str]] = {}

    async def get(self, key: str) -> Optional[str]:
        return self.kv["scores"] if "factor" in key else self.kv.get(key)

    async def sismember(self, key: str, m: str) -> bool:
        return m in self.sets.get(key, set())

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def sadd(self, key: str, m: str) -> None:
        self.sets.setdefault(key, set()).add(m)

    async def expire(self, key: str, s: int) -> None:
        return None

    async def lpush(self, key: str, v: str) -> None:
        self.lists.setdefault(key, []).insert(0, v)

    async def ltrim(self, key: str, a: int, b: int) -> None:
        return None

    async def hget(self, key: str, field: str) -> Optional[str]:
        # Every test stock is a $10bn company: calls are kept to $1bn and up
        # since 30 Oct 2026 (market_cap.is_large_cap), and unknown is not large.
        return "1e10"


class _Db:
    async def query(self, sql: str, *args: Any) -> List[Dict[str, Any]]:
        if "daily_bars" in sql:
            return [{"close": 100.0 - i} for i in range(22)]
        if "factor_scores_daily" in sql:
            return [{"percentile": 0.1}]
        return [{"created_at": "2026-10-28 13:00", "headline": "Beats estimates", "positive": 0.6, "negative": 0.1}]


def _agent(answers: List[Any], scores: Dict[str, Any]) -> Any:
    a = aa.AnalystAgent.__new__(aa.AnalystAgent)
    a.redis = type("R", (), {"raw": _Raw(scores)})()
    a.db = _Db()
    a.model = "qwen2.5:7b"
    a.name = "analyst_agent"          # set by SentinelAgent.__init__, which this bypasses
    a.logger = __import__("logging").getLogger("t")
    a.recorded = []
    it = iter(answers)

    async def infer(*args: Any, **kw: Any) -> Any:
        x = next(it)
        if isinstance(x, BaseException):
            raise x
        return an.AnalystView(**x)

    async def record(**kw: Any) -> str:
        a.recorded.append(kw)
        return "id"

    async def similar(event_id: str, k: int = 4) -> list:
        return [("2026-09-01", "NVDA", "Export curbs eased")]

    a._execute_with_telemetry = infer
    a.record_prediction = record
    a._similar = similar
    return a


_MSG = {"primary_entity_id": "NVDA", "trigger_event_id": "e1", "summary_headline": "Company News & Trading (NVDA)"}


def test_the_agent_grounds_samples_and_records_one_graded_forecast() -> None:
    # The model answers {rationale, cited, p_beat} since 30 Oct 2026: one
    # probability of beating the market instead of a direction plus a chance
    # the direction is right, which could be read either way.
    good = {"p_beat": 0.7, "cited": ["F2"], "rationale": "rank"}
    uncited = {"p_beat": 0.1, "cited": [], "rationale": "vibes"}
    a = _agent([good, uncited, good], {"NVDA": {"percentile": 0.9}})
    out = asyncio.run(a.analyse(_MSG))
    assert out is not None and out["samples"] == 2                    # the uncited answer was dropped
    # samples 0.7; prior 0.66 -> 0.68
    assert out["direction"] == "up" and abs(out["p_beat_market"] - 0.68) < 1e-9
    rec = a.recorded[0]
    assert rec["ticker"] == "NVDA" and rec["entry_price"] == 100.0 and rec["time_horizon_hours"] == 168
    assert a.redis.raw.lists[aa.VIEWS_KEY]
    # The same stock is not analysed twice in a day.
    assert asyncio.run(a.analyse(_MSG)) is None and len(a.recorded) == 1


def test_the_agent_skips_unranked_stocks_and_stops_when_the_budget_sheds() -> None:
    a = _agent([], {"AAPL": {"percentile": 0.5}})
    assert asyncio.run(a.analyse(_MSG)) is None                       # NVDA is not ranked
    assert asyncio.run(a.analyse({"primary_entity_id": "Strait of Hormuz"})) is None
    b = _agent([InferenceShed("budget")], {"NVDA": {"percentile": 0.9}})
    assert asyncio.run(b.analyse(_MSG)) is None and b.recorded == []


def test_when_the_live_ranks_lapse_the_last_stored_rank_is_the_prior() -> None:
    good = {"p_beat": 0.6, "cited": ["F2"], "rationale": ""}            # the {rationale, cited, p_beat} shape
    a = _agent([good, good, good], {})
    out = asyncio.run(a.analyse(_MSG))
    assert out is not None and out["rank"] == 0.1 and abs(out["prior"] - 0.34) < 1e-9


def test_market_context_comes_last_and_only_when_it_matters() -> None:
    from shared.utils import gpr

    lines = an.facts("NVDA", 0.5, "monthly", {}, [], [], "x", ["Geopolitical risk elevated"])
    assert lines[-1] == "F3: Geopolitical risk elevated"
    calm = {"as_of": "2026-10-26", "ma7": 100.0, "year_mean": 110.0, "z": -0.4, "elevated": False}
    a = _agent([], {"NVDA": {"percentile": 0.9}})
    a.redis.raw.kv[gpr.LATEST_KEY] = __import__("json").dumps(calm)
    assert asyncio.run(a._context(a.redis.raw)) == []
    a.redis.raw.kv[gpr.LATEST_KEY] = __import__("json").dumps({**calm, "z": 2.1, "elevated": True})
    assert "elevated" in asyncio.run(a._context(a.redis.raw))[0]


def test_p_beat_is_read_directly_and_the_old_shape_still_parses():
    assert an.p_up({"p_beat": 0.3}) == 0.3                       # 30% chance of beating, unambiguous
    assert an.p_up({"p_beat": "junk"}) == 0.5
    assert an.p_up({"direction": "down", "probability": 0.7}) == pytest.approx(0.3)
    fields = list(an.AnalystView.model_fields)
    assert fields.index("rationale") < fields.index("p_beat")    # the argument is written before the number


def test_a_hosted_analyst_samples_several_stocks_per_call():
    import asyncio
    from services.agents.analyst_agent import AnalystAgent

    agent = AnalystAgent.__new__(AnalystAgent)
    calls = []

    async def fake(message, system, user, schema=None, **kw):
        calls.append(user)
        return an.AnalystBatch(views=[
            an.BatchView(ticker="AAA", rationale="F1 says so", cited=["F1"], p_beat=0.6),
            an.BatchView(ticker="BBB", rationale="F3 only", cited=["F3"], p_beat=0.4),   # BBB has two facts: dropped
            an.BatchView(ticker="ZZZ", rationale="F1", cited=["F1"], p_beat=0.9)])       # not asked: ignored

    agent._execute_with_telemetry = fake
    agent.model = "groq/openai/gpt-oss-120b"
    items = [("AAA", {"message": {}, "facts": ["F1. a"]}), ("BBB", {"message": {}, "facts": ["F1. b", "F2. c"]})]
    out = asyncio.run(agent._sample_batch(items))
    assert len(calls) == an_samples() and "STOCK AAA" in calls[0] and "STOCK BBB" in calls[0]
    assert len(out["AAA"]) == an_samples() and out["BBB"] == []


def an_samples():
    from services.agents import analyst_agent
    return analyst_agent.SAMPLES
