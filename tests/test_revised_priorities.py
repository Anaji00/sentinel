"""Round 27's revised priorities.

1. Alerts graded against the same stock's headline days, not other stocks'.
2. Hawkes forecasts name no stock, fire once a pair an hour, and take the same
   tier ceilings as every other path.
3. B1 shrinks names with a recent alert from a family graded to precede bigger
   moves (size, not direction).
4. A hosted model's headline read, forward only, as a 30-day feature.
6. The alerts' expected-move model, walk-forward against a family-mean baseline.
8. The quant engine's model-written advisory retired on its record.
"""
import pathlib
import random
import sys
from datetime import date, datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import alert_move_model as amm  # noqa: E402
from shared.utils import auto_trade as at  # noqa: E402
from shared.utils import llm_news as ln  # noqa: E402
from shared.utils import producer_grades as pg  # noqa: E402


# ── 1. paired grading ────────────────────────────────────────────────────────

def test_a_family_on_calm_stocks_beats_their_own_headline_days():
    # Calm stocks: headline days 0.8. Alerts on them at 1.0 lose to a pooled
    # baseline of noisy stocks (1.4) but beat their own stocks' headline days.
    base = [("CALM", 0.8)] * 5 + [("NOISY", 2.0)] * 5
    rows = [("Family", "CALM", 1.0 + 0.01 * (k % 3)) for k in range(40)]
    pooled = pg.lift_table([(g, r) for g, _, r in rows], [b for _, b in base])
    paired = pg.paired_lift_table(rows, base)
    assert pooled[0]["verdict"] == "below a headline"
    assert paired[0]["verdict"] == "beats a headline" and paired[0]["baseline_ratio"] == 0.8


def test_alerts_on_stocks_without_enough_headline_days_are_not_paired():
    paired = pg.paired_lift_table([("F", "X", 1.0)] * 20, [("X", 1.0), ("X", 1.0)])
    assert paired == []


def test_move_flagging_needs_both_bars():
    grades = {
        "correlation_families_1d": [{"group": "A", "n": 100, "verdict": "moves more"},
                                    {"group": "B", "n": 100, "verdict": "no difference"}],
        "family_lift_1d": [{"group": "A", "n": 100, "verdict": "beats a headline"},
                           {"group": "B", "n": 100, "verdict": "beats a headline"}],
    }
    assert pg.move_flagging_families(grades) == {"A"}
    assert pg.move_flagging_families(None) == set()


# ── 2. Hawkes ────────────────────────────────────────────────────────────────

def _hawkes_block() -> str:
    src = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    return src.split('rule_id="HAWKES_EXCITATION"')[0][-2500:] + src.split('rule_id="HAWKES_EXCITATION"')[1][:4000]


def test_hawkes_names_no_stock_and_is_capped_and_deduped():
    block = _hawkes_block()
    assert 'primary_entity_id="HAWKES"' in block
    assert "event.primary_entity.id" not in block, "an arbitrary event's stock is back on the forecast"
    assert "exposure_tags(event.tags)" not in block
    assert "AlertTier.INTELLIGENCE" not in block
    assert "_tier_supported_by(_fc_conf" in block and "_tier_after_grades(" in block
    assert "nx=True, ex=HAWKES_REFIRE_SEC" in block


# ── 3. B1 risk input ─────────────────────────────────────────────────────────

def test_flagged_names_shrink_within_their_leg():
    w = at.alert_risk_weights(["AAA", "BBB", "CCC", "DDD"], {"AAA"}, 0.5)
    tw = at.target_weights(["AAA", "BBB"], ["CCC", "DDD"], 0.5, weights=w)
    assert round(tw["AAA"], 6) == round(0.5 * 0.5 / 1.5, 6) and round(tw["BBB"], 6) == round(0.5 / 1.5, 6)
    assert round(sum(v for v in tw.values() if v > 0), 6) == 0.5, "the leg's total is unchanged"
    assert tw["CCC"] == tw["DDD"] == -0.25


def test_the_scale_is_clamped_and_one_is_off():
    assert at.alert_risk_weights(["A"], {"A"}, 3.0) == {"A": 1.0}
    assert at.alert_risk_weights(["A"], {"A"}, -1.0) == {"A": 0.0}


# ── 4. LLM headline read ─────────────────────────────────────────────────────

def test_prompt_numbers_headlines_and_parse_keeps_valid_indexes():
    p = ln.prompt([("AAPL", "Apple beats estimates"), ("XOM", "Exxon cuts output")])
    assert "0. AAPL: Apple beats estimates" in p and "1. XOM:" in p
    ans = ln.Scores(scores=[ln.Scored(i=0, s=1), ln.Scored(i=0, s=-1), ln.Scored(i=1, s=-1), ln.Scored(i=5, s=1)])
    assert ln.parse(ans, 2) == {0: 1, 1: -1}


def test_feature_is_a_trailing_mean_with_a_minimum_count():
    day = date(2026, 10, 29)
    t = lambda d: datetime(2026, 10, d, 15, tzinfo=timezone.utc)  # noqa: E731
    scores = [("AAA", t(28), 1), ("AAA", t(20), -1), ("AAA", t(27), 1), ("BBB", t(28), 1),
              ("CCC", datetime(2026, 9, 1, tzinfo=timezone.utc), 1), ("CCC", t(28), 1)]
    rows = ln.feature_rows(scores, day)
    assert rows == [("AAA", day, ln.FEATURE, round(1 / 3, 4))], "BBB has one headline; CCC's first is outside 30 days"


def test_cutoffs_parse_per_model():
    c = ln.parse_cutoffs("gemini/x=2025-01-31;groq/y=bad")
    assert ln.cutoff_of("gemini/x", c) == date(2025, 1, 31) and ln.cutoff_of("groq/y", c) is None


def test_the_job_scores_forward_only_and_hosted_only():
    src = (ROOT / "services/agents/llm_news_job.py").read_text(encoding="utf-8")
    assert "INTERVAL '{LOOKBACK_HOURS} hours'" in src and "hosted_llm.is_hosted(MODEL)" in src


# ── 6. expected-move model ───────────────────────────────────────────────────

def _alerts(n: int, signal: bool) -> list:
    rng = random.Random(7)
    start = datetime(2026, 6, 1, 14, tzinfo=timezone.utc)
    out = []
    for k in range(n):
        fam = rng.choice(["A", "B", "C"])
        recent = rng.randint(0, 6)
        noise = rng.gauss(0, 0.3)
        target = (0.25 * recent if signal else 0.0) + noise
        out.append(amm.Alert(at=start + timedelta(hours=6 * k), ticker="T", family=fam, confidence=rng.random(),
                             domains=rng.randint(1, 3), log_usual=rng.gauss(-4, 0.3), recent_alerts=recent,
                             target=target))
    return out


def test_the_model_validates_on_real_structure_and_beats_the_family_baseline():
    rep = amm.walk_forward(_alerts(1400, signal=True))
    assert rep["validated"] and rep["mean_ic"] > rep["baseline_ic"], rep


def test_the_model_does_not_validate_on_noise():
    rep = amm.walk_forward(_alerts(1400, signal=False))
    assert not rep["validated"], rep


def test_too_few_alerts_is_reported_not_fitted():
    assert amm.walk_forward(_alerts(50, signal=True))["reason"] == "too few alerts"


# ── 8. quant advisory ────────────────────────────────────────────────────────

def test_the_quant_advisory_is_retired_by_default(monkeypatch):
    from shared.utils.llm_policy import agent_disabled, llm_disabled_for
    monkeypatch.delenv("LLM_DISABLED_AGENTS", raising=False)
    monkeypatch.delenv("AGENTS_DISABLED", raising=False)
    assert llm_disabled_for("quant_trading_engine") and not agent_disabled("quant_trading_engine")


# ── blueprint corrections ────────────────────────────────────────────────────

def _agent_with_card(brier: float, made: int):
    import asyncio as _a
    from services.agents.analyst_agent import AnalystAgent
    from services.agents.prediction_scoring import AgentScorecard

    a = AnalystAgent.__new__(AnalystAgent)          # any concrete agent; the method is the base class's
    a.name, a.logger = "x", __import__("logging").getLogger("t")

    async def card(*_, **__):
        return AgentScorecard(agent_name="x", brier_score=brier, predictions_made=made)

    a.get_scorecard = card
    return a, _a


def test_a_demoted_agent_counts_half_at_admission():
    from services.agents.base import DEMOTED_BUDGET_WEIGHT
    from services.agents.prediction_scoring import CALIBRATION_MIN_RESOLVED
    a, aio = _agent_with_card(0.30, CALIBRATION_MIN_RESOLVED + 5)
    assert aio.run(a._budget_weight()) == DEMOTED_BUDGET_WEIGHT
    b, _ = _agent_with_card(0.30, 1)
    assert aio.run(b._budget_weight()) == 1.0, "an unproven agent is not penalised"
    c, _ = _agent_with_card(0.20, CALIBRATION_MIN_RESOLVED + 5)
    assert aio.run(c._budget_weight()) == 1.0


def test_stock_correlation_assessments_carry_a_gradable_direction():
    from services.agents.stock_correlation_agent import DynamicCorrelationAssessment as D
    base = dict(macro_asset="CL", equity_ticker="XOM", correlation_type="x", detected_covariance=0.1,
                conviction=0.6, transmission_channel="x", agentic_rationale="x", impact_severity="LOW")
    assert D(**base, expected_direction="UP").expected_direction == "up"
    assert D(**base, expected_direction="sideways").expected_direction == "neutral"
    assert D(**base).expected_direction == "neutral"
    src = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")
    assert "expected_direction=brief.expected_direction" in src


def test_graph_proposals_use_the_supervisor_shape_and_allowed_predicates():
    from shared.models.ontology import is_valid_predicate
    q = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert '"relationship": "CORRELATED_PEER"' not in q and '"relation_type": "GRANGER_CAUSES"' in q
    c = (ROOT / "services/agents/customer_links_job.py").read_text(encoding="utf-8")
    assert '"action": "LINK_ENTITY"' in c and '"relation_type": "SUPPLIER_TO"' in c
    assert is_valid_predicate("GRANGER_CAUSES") and is_valid_predicate("SUPPLIER_TO")


# ── 5. latency: stale articles are archived, not raised live ─────────────────

def test_only_recent_articles_become_live_events():
    src = (ROOT / "services/correlation/stock_news_scorer.py").read_text(encoding="utf-8")
    assert "LIVE_EVENT_MAX_AGE_HOURS = 6" in src
    assert "if at >= event_cut:" in src and "if at >= live_cut:" not in src
