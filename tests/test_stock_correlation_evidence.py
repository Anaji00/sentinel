"""One stock_correlation_agent call weighs a labelled evidence card (5 Nov 2026): the
macro channel that moved, the stock's tape, the two stored signals with a measured
next-session reading, and what would size the move. Plus the fixes alongside it."""
import asyncio
import pathlib
from datetime import date

import pytest

from shared.utils import stock_evidence as se

ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")


def _closes(n: int = 30, last: float = 102.0) -> list:
    return [last] + [100.0 + (k % 2) * 0.5 for k in range(n)]


def test_a_yield_moves_in_basis_points_and_anything_else_in_a_log_return():
    y = se.macro_stats("US10Y", [4.30, 4.20, 4.25])
    assert y["last"] == pytest.approx(10.0) and y["five"] == pytest.approx(5.0)
    o = se.macro_stats("CL=F", _closes())
    assert o["last"] == pytest.approx(0.0198, abs=1e-3) and o["z"] > 3
    assert se.macro_stats("CL=F", [100.0]) is None


def test_the_channel_is_the_one_that_transmitted_not_the_largest_coefficient():
    exposures = [{"macro": "GC=F", "beta": 0.9, "t": 5.0}, {"macro": "US10Y", "beta": -0.002, "t": -4.0},
                 {"macro": "ZW=F", "beta": 0.5, "t": 3.0}]
    stats = {"GC=F": {"last": 0.0001}, "US10Y": {"last": 10.0}, "ZW=F": None}
    ranked = se.rank_channels(exposures, stats)
    assert [c["macro"] for c in ranked] == ["US10Y", "GC=F"], "largest implied move first; no move, no channel"
    assert ranked[0]["implied_bps"] == pytest.approx(-200.0)


def test_excess_returns_align_on_common_sessions():
    stock = [("2026-11-03", 100.0), ("2026-11-04", 110.0), ("2026-11-05", 110.0)]
    spy = [("2026-11-03", 100.0), ("2026-11-05", 100.0)]
    tape = se.stock_tape(stock, spy)
    assert tape["d1"] == pytest.approx(0.0953, abs=1e-3), "the session SPY lacks is not a session"
    assert se.stock_tape(stock[:1], spy) is None


def test_ranks_refuse_a_thin_cross_section_and_handle_ties():
    assert se.percentile_ranks({"A": 1.0}) == {}
    ranks = se.percentile_ranks({f"T{k}": k for k in range(60)} | {"NAN": float("nan"), "NONE": None})
    assert ranks["T0"] == 0.0 and ranks["T59"] == 1.0 and "NAN" not in ranks and "NONE" not in ranks


def test_the_evidence_baseline_needs_both_signals_and_a_real_lean():
    assert se.evidence_direction(0.9, 0.2) == "up" and se.evidence_direction(0.1, 0.8) == "down"
    assert se.evidence_direction(0.55, 0.45) is None, "a small lean is no call"
    assert se.evidence_direction(0.9, None) is None and se.evidence_direction(None, 0.1) is None


def test_earnings_by_the_next_session_counts_across_a_weekend():
    friday = date(2026, 11, 6)
    assert se.next_session(friday) == date(2026, 11, 9)
    assert se.reports_by_next_session("2026-11-09", friday) is True
    assert se.reports_by_next_session(date(2026, 11, 6), friday) is True
    assert se.reports_by_next_session("2026-11-10", friday) is False
    assert se.reports_by_next_session("2026-11-05", friday) is False
    assert se.reports_by_next_session(None, friday) is False and se.reports_by_next_session("soon", friday) is False


def _channel() -> dict:
    return {"macro": "CL=F", "beta": -0.2, "t": -4.0, "implied_bps": -40.0,
            "stats": {"last": 0.02, "five": 0.03, "z": 1.5, "pct": 0.02}}


def test_the_card_labels_what_each_item_has_been_measured_to_say_and_omits_what_is_missing():
    card = se.build_card("XOM", [_channel()], tape={"d1": 0.01, "d5": 0.02, "d20": -0.03, "vol": 0.015},
                         cp_rank=0.9, short_rank=0.2, options={"atm_iv": 0.32}, report_day="2026-11-20",
                         surprise={"surprise_pct": 4.0, "period": "2026-09-30"}, factor_pct=0.7,
                         short_interest={"days_to_cover": 2.5, "change_pct": 3.0}, borrow_fee=0.4, insider_net=-2e6,
                         peers={"CVX": 0.012, "COP": -0.004}, headlines=["Oil   rises\nagain"])
    text = se.card_lines(card)
    assert [e["id"] for e in card] == [f"E{k + 1}" for k in range(len(card))]
    assert "implies -40 bps" in text and "+140 bps beyond" in text
    assert "[measured next-session signal] Call-put IV spread ranks 90" in text
    assert "[measured next-session signal] Short volume ratio ranks 20" in text
    assert "about 2.0% for one session" in text and 'Headline: "Oil rises again"' in text
    assert "sold $2.0M" in text and "monthly rank 70 of 100" in text and "0.40% a year" in text
    assert "Insiders" not in se.card_lines(se.build_card("XOM", [_channel()], insider_net=5000.0)), "too small to be a statement"
    bare = se.build_card("XOM", [_channel()])
    assert len(bare) == 1, "nothing stored, nothing shown, and never a zero"


def test_a_yield_is_stated_in_basis_points():
    assert se.change_text("US10Y", -5.0) == "-5.0 bp" and se.change_text("CL=F", -0.08) == "-8.00%"


def test_citations_are_ids_on_this_card_only():
    assert se.citations(["E1: oil", "e3", "see E1 and E12"]) == ["E1", "E3", "E12"]
    card = se.build_card("XOM", [_channel()])
    assert se.known_citations(["E1", "E9"], card) == ["E1"]


def test_the_output_schema_keeps_old_answers_valid_and_reads_citations():
    from services.agents.stock_correlation_agent import DynamicCorrelationAssessment as A

    base = dict(macro_asset="CL=F", equity_ticker="XOM", correlation_type="x", detected_covariance=0.1, conviction=0.6,
                transmission_channel="x", agentic_rationale="x", impact_severity="LOW")
    assert A(**base).evidence_for == [] and A(**base).key_risk == ""
    assert A(**base, evidence_for=["E1 oil", "E2"]).evidence_for == ["E1", "E2"]
    assert A(**base, evidence_against="E4").evidence_against == ["E4"]


def test_a_forecast_is_held_to_what_its_record_supports():
    assert se.capped_conviction(0.95) == se.MAX_FORECAST_CONVICTION and se.capped_conviction(0.3) == 0.3


def test_the_handler_spends_a_call_only_where_there_is_something_to_weigh():
    body = AGENT.split("async def handle_message")[1]
    move, earn, sample = (body.index(s) for s in ('"no_macro_move"', '"earnings_next_session"', "await sample_admitted("))
    assert move < earn < sample, "both refusals come before the hour's sample is spent"
    baseline, evidence = body.index('f"baseline:{target_macro}"'), body.index('f"evidence:{target_equity}"')
    assert baseline < move and evidence < move, "the no-model forecasts are graded on every pair"
    assert "uncited_direction" in body and "se.capped_conviction" in body


def test_the_evidence_baseline_is_graded_apart_from_the_agents_own_calls():
    from services.agents.prediction_scoring import side_label

    assert side_label("evidence:XOM") == "evidence" and side_label("baseline:CL=F") == "baseline"
    assert side_label("news") is None


# ── fixes shipped with it ────────────────────────────────────────────────────

class _Raw:
    def __init__(self):
        self.store, self.hash = {}, {}

    async def set(self, k, v, ex=None, nx=False):
        if nx and k in self.store:
            return None
        self.store[k] = v
        return True

    async def incr(self, k):
        self.store[k] = int(self.store[k]) + 1
        return self.store[k]

    async def get(self, k):
        return self.store.get(k)

    async def delete(self, k):
        self.store.pop(k, None)

    async def hgetall(self, k):
        return self.hash.get(k, {})

    async def hsetnx(self, k, f, v):
        self.hash.setdefault(k, {}).setdefault(f, v)

    async def hexists(self, k, f):
        return f in self.hash.get(k, {})

    async def hdel(self, k, f):
        self.hash.get(k, {}).pop(f, None)


class _Redis:
    def __init__(self):
        self.raw = _Raw()


def test_the_next_alert_says_how_many_the_last_window_withheld_once():
    from services.correlation import main as m

    rule, r = "rule_maritime_chokepoint_evasion", _Redis()
    run = asyncio.run
    assert run(m._episode_is_open(r, rule, "Red Sea")) is False
    assert run(m._prior_episode_repeats(r, rule, "Red Sea")) == 0
    for _ in range(3):
        assert run(m._episode_is_open(r, rule, "Red Sea")) is True
    r.raw.store.pop(m.EPISODE_KEY.format(rule=rule, region="red_sea"))   # the window closes
    assert run(m._episode_is_open(r, rule, "Red Sea")) is False
    assert run(m._prior_episode_repeats(r, rule, "Red Sea")) == 3
    assert run(m._prior_episode_repeats(r, rule, "Red Sea")) == 0, "reported once"
    assert run(m._prior_episode_repeats(r, "rule_other", "Red Sea")) == 0


def test_a_retired_rule_stays_retired_after_its_record_ages_out_until_reinstated():
    from shared.utils.rule_feedback import RULE_FEEDBACK_KEY, deprecated_by_evidence, reinstate_rule

    r, run = _Redis(), asyncio.run
    key = f"{RULE_FEEDBACK_KEY}:rule_x"
    r.raw.hash[key] = {b"total": b"34", b"negative": b"33"}
    assert run(deprecated_by_evidence(r, "rule_x")) is True
    r.raw.hash.pop(key)                                          # the 30-day TTL passes
    assert run(deprecated_by_evidence(r, "rule_x")) is True, "nobody has looked at it yet"
    run(reinstate_rule(r, "rule_x"))
    assert run(deprecated_by_evidence(r, "rule_x")) is False


def test_a_thin_ranking_never_becomes_the_universe():
    from services.agents.sympathy_job import _UNIVERSE_SQL

    assert "HAVING count(*) >= 800" in _UNIVERSE_SQL and "max(as_of)" not in _UNIVERSE_SQL
    engine = (ROOT / "services/agents/factor_engine.py").read_text(encoding="utf-8")
    guard, write = engine.index("len(scores) < MIN_SCORES_TO_REBUILD"), engine.index("await raw.set(FACTOR_SCORES_KEY")
    keep = engine.index("self._keep_and_grade(report.get")
    assert guard < write < keep, "withheld before the live key and the stored history"
