"""The stock correlation agent batches pairs: on 29 Oct 2026 it made 794 calls,
one stock/macro pair each, 68% of the day's hosted calls. Pairs arriving
together now share one inference; each keeps its own prompt section and is
answered, or left unanswered, by its own key."""
import asyncio
import pathlib
import sys
from typing import Any, List

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents import stock_correlation_agent as sca  # noqa: E402


def _assessment(macro: str, equity: str) -> sca.DynamicCorrelationAssessment:
    return sca.DynamicCorrelationAssessment(macro_asset=macro, equity_ticker=equity, correlation_type="x",
                                            detected_covariance=0.1, conviction=0.6, transmission_channel="x",
                                            agentic_rationale="x", impact_severity="LOW")


def _agent(reply: Any, calls: List[dict]) -> sca.StockCorrelationAgent:
    a = sca.StockCorrelationAgent.__new__(sca.StockCorrelationAgent)

    async def execute(**kw: Any) -> Any:
        calls.append(kw)
        return reply

    a._execute_with_telemetry = execute
    return a


def test_keys_ignore_case_and_space():
    assert sca.pair_key(" cl ", "xom") == sca.pair_key("CL", "XOM") == "CL|XOM"


def test_the_batch_prompt_numbers_each_pair_and_asks_for_one_answer_each():
    p = sca.batch_prompt(["Analyze CL and XOM", "Analyze GC and NEM"])
    assert "PAIR 1\nAnalyze CL and XOM" in p and "PAIR 2\nAnalyze GC and NEM" in p and "2 in all" in p


def test_a_batch_is_one_call_answered_by_key():
    calls: List[dict] = []
    reply = sca.DynamicCorrelationBatch(assessments=[_assessment("GC", "NEM"), _assessment("cl", "xom"),
                                                      _assessment("BTC", "MSTR")])
    a = _agent(reply, calls)
    items = [("CL|XOM", {"message": {}, "section": "s1"}), ("GC|NEM", {"message": {}, "section": "s2"}),
             ("SI|PAAS", {"message": {}, "section": "s3"})]
    out = asyncio.run(a._assess_batch(items))
    assert len(calls) == 1 and calls[0]["schema"] is sca.DynamicCorrelationBatch
    assert set(out) == {"CL|XOM", "GC|NEM"}, "an unrequested pair is dropped; an unanswered one is absent"


def test_one_pair_is_asked_exactly_as_before():
    calls: List[dict] = []
    a = _agent(_assessment("CL", "XOM"), calls)
    out = asyncio.run(a._assess_batch([("CL|XOM", {"message": {}, "section": "own prompt"})]))
    assert calls[0]["schema"] is sca.DynamicCorrelationAssessment and calls[0]["user_prompt"] == "own prompt"
    assert out["CL|XOM"].equity_ticker == "XOM"


def test_the_handler_submits_to_the_batcher_and_checks_its_own_section():
    src = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")
    body = src.split("async def handle_message")[1]
    assert "await self._batcher.submit(" in body and "self._execute_with_telemetry(" not in body
    assert "user_prompt, movers, known=listed" in body, "each answer is fact-checked against its own pair"


# ── limited to macro (29 Oct 2026 review) ────────────────────────────────────

def test_the_pair_is_the_stocks_measured_macro_exposure_not_a_rotation():
    rows = [{"macro": "cl=f", "rel": "INVERSE_EXPOSURE_TO", "weight": -0.3},
            {"macro": "finnhub_equities", "rel": "POSITIVE_EXPOSURE_TO", "weight": 0.9},
            {"macro": "GC=F", "rel": "POSITIVE_EXPOSURE_TO", "weight": 0.6}]
    assert [e["macro"] for e in sca.macro_exposures(rows)] == ["GC=F", "CL=F"], "strongest first, non-macro dropped"
    body = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8").split("async def handle_message")[1]
    assert "_next_macro_asset(" not in body and "await self._macro_exposures(ticker)" in body
    assert "no_macro_exposure" in body


def test_no_stock_to_stock_edges_and_only_measured_sympathy_movers():
    src = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")
    body = src.split("async def handle_message")[1]
    # Sympathy movers returned on 30 Oct 2026 at the user's request, but only
    # from measured peers (sympathy_peers): a model-named mover is dropped, and
    # an edge carries the measured correlation, not the model's conviction.
    assert "COMPETES_WITH" not in body
    assert "if sm.ticker in peers]" in body and '"confidence_basis": "measured_corr"' in body
    assert '"model_asserted"' not in body
    # Exposures come from the measured table since 30 Oct 2026; the graph's
    # exposure edges were sector priors at weight 1.0.
    assert "_EXPOSURE_CYPHER" not in src and "FROM macro_exposures" in src
