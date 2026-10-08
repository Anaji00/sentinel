"""Which agents run, which may call the model, and the deterministic paths that replace it."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.deterministic_briefs import (  # noqa: E402
    credit_signal, curve_state, rates_brief_fields, vol_surface_fields,
)
from shared.utils.llm_policy import agent_disabled, llm_disabled_for  # noqa: E402


def test_the_stock_tier_keeps_its_model_by_default(monkeypatch) -> None:
    monkeypatch.delenv("AGENTS_DISABLED", raising=False)
    monkeypatch.delenv("LLM_DISABLED_AGENTS", raising=False)
    for name in ("quant_trading_engine", "stock_correlation_agent"):
        assert not agent_disabled(name) and not llm_disabled_for(name)
    assert agent_disabled("adversarial_wargamer") and agent_disabled("rule_synthesizer")
    assert llm_disabled_for("radar_agent") and llm_disabled_for("macro_intelligence_engine")


def test_the_environment_overrides_the_defaults(monkeypatch) -> None:
    monkeypatch.setenv("AGENTS_DISABLED", "")
    monkeypatch.setenv("LLM_DISABLED_AGENTS", "quant_trading_engine")
    assert not agent_disabled("adversarial_wargamer")
    assert llm_disabled_for("quant_trading_engine") and not llm_disabled_for("radar_agent")


def test_curve_state_follows_the_measured_spread() -> None:
    assert curve_state(-25.0, []) == "Inverted"
    assert curve_state(4.0, []) == "Flat"
    assert curve_state(47.0, []) == "Normal Steepening"
    assert curve_state(47.0, [-30.0, 5.0]) == "Disinverted"


def test_credit_stress_needs_a_prior_reading() -> None:
    assert credit_signal(0.70, None) == "Unclassified"
    assert credit_signal(0.70, 0.70) == "Stable"
    assert credit_signal(0.70 * 0.992, 0.70) == "Moderate Widening"
    assert credit_signal(0.70 * 0.98, 0.70) == "Severe Stress"


def test_a_rates_brief_invents_no_hedging() -> None:
    f = rates_brief_fields(47.0, [], 239.0, 2.92, 0.70, None)
    assert f["curve_state"] == "Normal Steepening" and f["macro_risk_level"] == "LOW"
    assert f["credit_spread_widening_signal"] == "Unclassified"
    assert f["recommended_hedging"] == []


def test_vol_surface_without_measurements_claims_nothing() -> None:
    f = vol_surface_fields("NVDA", None, None)
    assert f["volatility_regime"] == "Unmeasured" and f["tail_risk_conviction"] == 0.0
    g = vol_surface_fields("NVDA", 2.2, 450.0)
    assert g["volatility_regime"] == "Defensive" and 0 < g["tail_risk_conviction"] <= 1


def test_radar_escalation_is_the_threshold_rule() -> None:
    from services.agents.radar_agent import deterministic_radar_decision

    yes = deterministic_radar_decision("WDC", 4.2, 1_200_000.0)
    no = deterministic_radar_decision("WDC", 2.1, 1_200_000.0)
    assert yes.investigate and not no.investigate
    assert "4.20" in yes.rationale and "1.20M" in yes.rationale


def test_the_reasoning_critique_pass_is_off_unless_asked_for(monkeypatch) -> None:
    from services.reasoning.scenario_generator import critique_enabled

    monkeypatch.delenv("REASONING_CRITIQUE", raising=False)
    assert critique_enabled() is False
    monkeypatch.setenv("REASONING_CRITIQUE", "1")
    assert critique_enabled() is True


def test_deterministic_briefs_satisfy_the_agent_schemas() -> None:
    """A field the schema requires and the builder omits fails every brief."""
    from services.agents.macro_intelligence_engine import RatesRegimeBrief, VolatilitySurfaceBrief
    from shared.utils.deterministic_briefs import rates_brief_fields, vol_surface_fields

    for pc, skew in ((1.4, 600.0), (None, None), (0.5, None)):
        brief = VolatilitySurfaceBrief(**vol_surface_fields("NVDA", pc, skew))
        assert brief.analytical_summary.startswith("NVDA:")
    RatesRegimeBrief(**rates_brief_fields(25.0, [10.0, 20.0, 30.0], 230.0, 1.9, 1.02, 1.0))


def test_reasoning_writes_scenarios_only_from_the_floor_tier(monkeypatch) -> None:
    from services.reasoning.main import tier_warrants_synthesis

    monkeypatch.delenv("REASONING_MIN_TIER", raising=False)
    assert not tier_warrants_synthesis("ALERT") and not tier_warrants_synthesis("ELEVATED")
    assert tier_warrants_synthesis("INTELLIGENCE") and tier_warrants_synthesis("CRITICAL")
    monkeypatch.setenv("REASONING_MIN_TIER", "ALERT")
    assert tier_warrants_synthesis("ELEVATED") and not tier_warrants_synthesis("WATCH")
