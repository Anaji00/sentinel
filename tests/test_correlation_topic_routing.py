"""
tests/test_correlation_topic_routing.py

The stock correlation agent's entire output was being discarded by its reader.

Found by auditing the DLQ end to end while the system was live. `failed_events`
held CorrelationCluster validation failures marked permanently_failed:

    5 validation errors for CorrelationCluster
    rule_id           Field required
    rule_name         Field required
    alert_tier        Field required
    trigger_event_id  Field required
    description       Field required
    input_value={'agent': 'stock_correlat...commended_hedging': []}

stock_correlation_agent declares Topics.CORRELATIONS as its output_topic, so its
cross-asset analysis lands on the same topic the reasoning consumer reads, and
that consumer called CorrelationCluster(**raw_data) on everything it saw. The
agent's payload shares none of the five required fields, so every message raised,
went to the DLQ, exhausted its retries and was written off as permanently failed.

The agent ran, won an inference slot on a host that affords few of them,
produced its analysis, published it -- and the consumer threw all of it away.
That is this codebase's signature defect with the roles reversed: not finished
code nothing calls, but finished output nothing accepts.

The DLQ itself came out of the same audit well: 754 retries in thirty minutes
against three permanent failures, so transient errors are genuinely recovered.
These three were the exception, and they were not transient.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "reasoning" / "main.py").read_text(encoding="utf-8")

AGENT_PAYLOAD = {
    "agent": "stock_correlation_agent",
    "target_macro": "CL=F",
    "target_equity": "XOM",
    "recommended_hedging": [],
}
CLUSTER_PAYLOAD = {
    "correlation_id": "abc",
    "rule_id": "r1",
    "rule_name": "Cross-domain",
    "alert_tier": "CRITICAL",
    "trigger_event_id": "e1",
    "description": "d",
}


def _routes_as_agent_analysis(payload: dict) -> bool:
    """The discriminator the consumer now applies."""
    return "agent" in payload and "correlation_id" not in payload


def test_agent_analysis_is_recognised_before_cluster_parsing():
    assert _routes_as_agent_analysis(AGENT_PAYLOAD)


def test_a_real_cluster_is_still_parsed_as_one():
    """The guard must not swallow the traffic the consumer exists for."""
    assert not _routes_as_agent_analysis(CLUSTER_PAYLOAD)


def test_a_cluster_carrying_an_agent_field_is_still_a_cluster():
    """correlation_id is what makes it a cluster; an agent attribution
    alongside it must not divert a genuine cluster."""
    both = {**CLUSTER_PAYLOAD, "agent": "someone"}
    assert not _routes_as_agent_analysis(both)


def test_the_guard_runs_before_the_constructor():
    """Ordering is the whole fix. After the constructor it would never run."""
    guard = SOURCE.index('if "agent" in raw_data and "correlation_id" not in raw_data:')
    construct = SOURCE.index("cluster = CorrelationCluster(**raw_data)")
    assert guard < construct


def test_the_analysis_is_kept_rather_than_dropped():
    """Discarding it quietly would fix the DLQ noise and lose the same work.
    It is cached the way intel briefs on this topic already are."""
    block = SOURCE.split('if "agent" in raw_data and "correlation_id" not in raw_data:')[1][:900]
    assert "sentinel:agents:correlation_analysis:" in block
    assert "redis_client.raw.set" in block


def test_a_cache_failure_does_not_drop_the_message_loudly():
    """Redis being briefly unavailable must not resurrect the crash this
    replaced."""
    block = SOURCE.split('if "agent" in raw_data and "correlation_id" not in raw_data:')[1][:900]
    assert "except Exception" in block


def test_the_agent_still_publishes_to_this_topic():
    """If the agent's output_topic changes, this guard is dead code and the
    test should say so rather than passing silently."""
    agent_source = (ROOT / "services" / "agents" / "stock_correlation_agent.py").read_text(encoding="utf-8")
    assert "return Topics.CORRELATIONS" in agent_source
