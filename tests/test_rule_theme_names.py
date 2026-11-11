"""A rule name may claim a market theme only if a clause filters on it (the 'US2Y Yield Drop' flood, 9 to 11 Nov 2026)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.rule_agent import DynamicRule, _is_reusable_rule, _theme_in_name  # noqa: E402


def _rule(name: str, tags=None) -> DynamicRule:
    return DynamicRule(
        rule_id="syn_theme", rule_name=name, trigger_event_type="market_anomaly",
        conditions={"min_anomaly": 0.25},
        correlations=[{"event_types": ["headline", "equity_block"], "hours": 24, "min_anomaly": 0.25,
                       "shared_tags": True, "follows_trigger": True, "within_minutes": 240, "tags": tags}],
        alert_tier="INTELLIGENCE", tags=["macro"],
    )


def test_a_yield_name_with_no_yield_filter_is_rejected() -> None:
    reason = _is_reusable_rule(_rule("US10Y Yield Drop and Zillow Valuation Surge"))
    assert reason and "yield" in reason


def test_the_same_name_is_accepted_when_a_clause_filters_on_the_theme() -> None:
    assert _is_reusable_rule(_rule("Treasury Yield Drop and Housing Surge", tags=["yield"])) is None


def test_a_short_unrelated_tag_does_not_cover_a_theme() -> None:
    assert _is_reusable_rule(_rule("Gold Price Upswing Miner Revenue Convergence", tags=["old"])) is not None
    assert _is_reusable_rule(_rule("Gold Price Upswing Miner Revenue Convergence", tags=["gold"])) is None


def test_a_name_without_a_theme_word_is_unaffected() -> None:
    assert _theme_in_name("Options Print Convergence") is None
    assert _is_reusable_rule(_rule("Equity Block Convergence")) is None


def test_theme_words_match_whole_words_only() -> None:
    assert _theme_in_name("Soilless Farming Boom") is None          # contains 'oil' but is not 'oil'
    assert _theme_in_name("Crude Oil Spike") == "oil" or _theme_in_name("Crude Oil Spike") == "crude"
