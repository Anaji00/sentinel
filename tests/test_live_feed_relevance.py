"""
tests/test_live_feed_relevance.py

Two things reached a person's feed that should never have.

    EVENT HEADLINE:  Transfer: $6 USDC
    Anomaly score: 0.00. Provenance tags: crypto, transfer, baseline_data

Nothing was wrong with the scoring here. The enricher had already decided this
was noise, tagged it `baseline_data`, and scored it 0.000. Both live-feed
publishers ignored the label and broadcast every enriched event, so a six-dollar
transfer arrived rendered identically to a $16.44M whale movement.

    EVENT HEADLINE:  AGENT [SUPERVISOR]: Intelligence Brief Synthesized
    SUMMARY:         {'agent': 'supervisor', 'action': 'single_commit',
                      'entity_id': '8881EF'}
    ANOMALY SCORE:   0.85

That is a graph-write confirmation. The publisher fell back to
`str(res_dict)[:200]` when an agent returned no prose, and to a hardcoded 0.85
when it stated no confidence -- so bookkeeping was presented as an executive
summary and ranked above most measured findings. The headline promised a brief
the payload never contained.

Both are consumer-side: the producers were right and the reader ignored them.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.live_feed import (  # noqa: E402
    LIVE_FEED_MIN_SCORE, agent_narrative, worth_broadcasting,
)


# -- the $6 transfer -----------------------------------------------------------

def test_the_six_dollar_transfer_stays_out():
    """Verbatim, including the tags it actually carried."""
    assert not worth_broadcasting({
        "headline": "Transfer: $6 USDC", "anomaly_score": 0.0,
        "tags": ["crypto", "transfer", "baseline_data", "suspect_wallet"],
    })


def test_a_whale_transfer_still_goes_through():
    assert worth_broadcasting({
        "headline": "Whale Transfer: $16.44M DAI", "anomaly_score": 0.833,
        "tags": ["crypto", "dai", "whale_transfer"],
    })


def test_the_baseline_tag_wins_over_a_score():
    """The enricher's label is a verdict, not a hint. Something tagged baseline
    that also scored well is a scoring bug, not a reason to broadcast."""
    assert not worth_broadcasting({"anomaly_score": 0.9, "tags": ["baseline_data"]})


def test_routine_telemetry_and_plumbing_are_excluded():
    """Aviation and vessel routine pings, and venue-to-venue crypto flow."""
    for tag in ("routine_telemetry", "infrastructure_flow"):
        assert not worth_broadcasting({"anomaly_score": 0.5, "tags": [tag]})


def test_a_floor_score_is_excluded_whatever_its_tags():
    assert not worth_broadcasting({"anomaly_score": 0.0, "tags": ["crypto"]})
    assert not worth_broadcasting({"anomaly_score": 0.01, "tags": []})


def test_an_unscored_event_is_not_evidence_of_importance():
    assert not worth_broadcasting({"tags": ["crypto"]})
    assert not worth_broadcasting({"anomaly_score": "abc"})


def test_malformed_input_does_not_raise():
    for value in (None, [], "abc", 42):
        assert worth_broadcasting(value) is False


def test_the_floor_is_low_enough_to_be_a_floor():
    """This is a guard against zero, not a second opinion on ranking."""
    assert 0.0 < LIVE_FEED_MIN_SCORE <= 0.1


# -- the supervisor "brief" ----------------------------------------------------

def test_bookkeeping_is_not_a_brief():
    """The exact payload that reached the feed."""
    assert agent_narrative({
        "agent": "supervisor", "action": "single_commit", "entity_id": "8881EF",
    }) is None


def test_a_real_brief_is_returned():
    assert agent_narrative({"summary": "Crude and QQQ decoupled sharply."}) \
        == "Crude and QQQ decoupled sharply."


def test_alternative_prose_fields_are_accepted():
    for field in ("rationale", "narrative", "brief", "assessment", "headline"):
        assert agent_narrative({field: "something worth reading"})


def test_blank_prose_counts_as_none():
    """An empty string is not a brief, and would publish an empty card."""
    assert agent_narrative({"summary": "   "}) is None
    assert agent_narrative({"summary": None, "rationale": ""}) is None


def test_a_non_dict_result_is_handled():
    for value in (None, "text", 42, []):
        assert agent_narrative(value) is None


# -- wiring --------------------------------------------------------------------

def test_both_publishers_apply_the_same_rule():
    """Two sites broadcasting on different rules is how one gets forgotten."""
    source = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert source.count("worth_broadcasting(") >= 2


def test_agents_decline_rather_than_invent_a_summary():
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    executable = [
        ln for ln in source.splitlines()
        if "str(res_dict)[:200]" in ln and not ln.lstrip().startswith("#")
    ]
    assert not executable, f"still dumping the raw result dict: {executable}"
    assert "_NoBriefToPublish" in source


def test_no_agent_confidence_is_asserted():
    """0.85 by default ranked every silent agent above most real findings."""
    source = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert 'res_dict.get("anomaly_score") or 0.85' not in source

    from services.agents.base import AGENT_UNSTATED_CONFIDENCE
    assert AGENT_UNSTATED_CONFIDENCE < 0.5
