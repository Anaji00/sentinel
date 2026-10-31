"""Model-written rules on probation (shared/utils/rule_probation.py) and the
rule agent / correlation engine wiring around them."""
import pathlib

from shared.utils import rule_probation as rp

ROOT = pathlib.Path(__file__).resolve().parents[1]
ORDER = ["CRITICAL", "INTELLIGENCE", "ELEVATED", "ALERT", "WATCH"]   # highest first, as the engine


def _rule(name="Block Into Options", **kw):
    r = {"rule_name": name, "trigger_event_type": ["equity_block"], "conditions": {"min_anomaly": 0.4},
         "correlations": [{"event_types": ["options_flow"], "hours": 24, "min_anomaly": 0.5, "same_entity": True}]}
    r.update(kw)
    return r


def test_ids_come_from_the_definition_not_the_models_name():
    a, b = _rule(rule_id="syn_1"), _rule(rule_id="syn_1", conditions={"min_anomaly": 0.6})
    assert rp.synth_rule_id(a) != rp.synth_rule_id(b)            # the old key collision
    assert rp.synth_rule_id(a) == rp.synth_rule_id(_rule(rule_id="other", rule_name="Renamed"))
    assert rp.synth_rule_id(a).startswith(rp.SYNTH_PREFIX)


def test_probation_caps_the_tier_until_the_family_is_graded_in():
    r = _rule(probation=True)
    assert rp.capped_tier("CRITICAL", r, set(), ORDER) == "WATCH"
    assert rp.capped_tier("WATCH", r, set(), ORDER) == "WATCH"
    assert rp.capped_tier("CRITICAL", r, {"Block Into Options"}, ORDER) == "CRITICAL"
    assert rp.capped_tier("CRITICAL", _rule(), set(), ORDER) == "CRITICAL"   # shipped / legacy rules untouched


def test_graded_out_removes_only_retired_probation_rules():
    grades = {"correlation_families_1d": [{"group": "Block Into Options", "n": 80, "verdict": "no difference"}],
              "family_days": {"Block Into Options": 12}}
    rules = {"syn_a": _rule(probation=True), "rule_x": _rule(), "syn_b": _rule("Other", probation=True)}
    assert rp.graded_out(rules, grades) == ["syn_a"]
    grades["family_days"] = {"Block Into Options": 2}            # one or two days is not evidence
    assert rp.graded_out(rules, grades) == []
    assert rp.graded_out(rules, None) == []


def test_the_agent_and_engine_are_wired():
    agent = (ROOT / "services" / "agents" / "rule_agent.py").read_text(encoding="utf-8")
    assert "rule_probation.synth_rule_id(body)" in agent and 'body["probation"] = True' in agent
    assert "SYNTH_DAILY_CAP" in agent and "self._prune_by_grades(decoded)" in agent
    engine = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert "rule_probation.capped_tier(" in engine and '"probation": _probation' in engine
    analyst = (ROOT / "services" / "agents" / "analyst_agent.py").read_text(encoding="utf-8")
    assert '.get("probation")' in analyst


def test_a_rule_named_for_one_instrument_is_rejected():
    from services.agents import rule_agent as ra
    for name in ("MSFT Headline‑Equity Block Convergence", "BTC Volume Anomaly Cross-Asset Spillover",
                 "ETHUSDT Volume Anomaly Equity Sympathy Cascade"):
        assert ra._instrument_in_name(name), name
    for name in ("Crypto Liquidation & Peer Spillover", "CPI Print Into Rates Repricing",
                 "Turkish Straits Vessel and Flight Anomaly Convergence", "ADS-B Gap Over Chokepoint"):
        assert ra._instrument_in_name(name) is None, name
