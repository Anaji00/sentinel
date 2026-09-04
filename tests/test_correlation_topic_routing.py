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


AGENT_SOURCE = (ROOT / "services" / "agents" / "stock_correlation_agent.py").read_text(encoding="utf-8")


def test_the_agent_no_longer_publishes_onto_the_cluster_contract():
    """The producer moved, which is what the previous version of this test
    existed to force a decision about.

    That test asserted the agent still published to CORRELATIONS, on the
    grounds that if it ever stopped, the consumer's discriminator would be
    dead code and something should say so rather than pass silently. It has
    now said so, and this is the decision.

    The guard fixed the reasoning consumer and only the reasoning consumer.
    Eight consumer groups read sentinel.correlations, and the alert manager --
    which had no such guard -- was still constructing CorrelationCluster from
    this payload and raising five validation errors at a time, live, months
    later. Requiring every reader of a topic to carry a discriminator for a
    shape that is not the topic's contract makes correctness the obligation of
    everyone who subscribes, and the alert manager is the proof that it does
    not hold.

    The other half is where the analysis went. The guard cached it to
    sentinel:agents:correlation_analysis:{agent}, and nothing in the tree ever
    read that key -- no readers, and no such keys live in Redis. The work was
    still being discarded; it was simply being discarded quietly, in a Redis
    key instead of the DLQ, which is why the noise stopped and the loss did
    not.

    MACRO_DECOUPLING carries macro-versus-equity relationship findings, follows
    the agents.* convention for agent output, and has two live consumers that
    already read heterogeneous topics. The analysis now reaches readers.
    """
    assert "Topics.MACRO_DECOUPLING" in AGENT_SOURCE
    assert "return Topics.CORRELATIONS" not in AGENT_SOURCE


def test_the_consumer_guard_is_retained_as_a_contract_defence():
    """Kept deliberately, not left behind.

    With the producer moved there is no live sender of this shape, so the
    guard defends the contract rather than handling traffic: it costs one dict
    lookup and it is the difference between a future misrouted publish being
    absorbed and it filling the DLQ again. The cost of removing it is paid by
    whoever reintroduces the defect.
    """
    assert 'if "agent" in raw_data and "correlation_id" not in raw_data:' in SOURCE
