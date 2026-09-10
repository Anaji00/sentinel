"""Every Kafka topic has a producer and a consumer, and both use the constant.

This is a static contract check, so it runs in CI without a broker. It exists
because this platform has repeatedly shipped topics that only one side of.

  * `alerts.outbound` accumulated 143,580 messages against zero consumer groups
    while three services wrote to it;
  * `SYSTEM_HEARTBEAT` had two subscribers and no producer anywhere in the tree,
    so each was a consumer-group assignment and a rebalance participant for a
    stream that never carried a message;
  * `CONSENSUS_REPORTS`, `ONTOLOGY_UPDATES`, `RATES_REGIME` and `VOL_SURFACE`
    were each produced by an agent and read by nobody -- four of the seven
    agents that call a model were spending the platform's scarcest resource to
    publish into an empty room.

Both halves matter. A produced topic with no consumer is work thrown away; a
consumed topic with no producer is a consumer group holding partition
assignments and joining every rebalance for nothing.
"""
import collections
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.kafka import Topics  # noqa: E402

SERVICES = ROOT / "services"

# Topics deliberately declared without a live producer or consumer, each with
# the reason. Anything not listed here must have both.
INTENTIONAL = {
    # Kept as a constant only so an external reference does not break on
    # import; heartbeats travel through Redis.
    "SYSTEM_HEARTBEAT": "heartbeats travel through Redis, not Kafka",
    # Every producer was removed once it was established that nothing consumed
    # it; the constant remains as the name an alerting consumer would use.
    "ALERTS": "no consumer by design -- see the alerts.outbound finding",
    # The dead-letter queue is written by many services and drained by the DLQ
    # worker, which subscribes dynamically rather than through a topic list.
    "DLQ": "drained by the dlq-worker",
}


def _scan():
    """Producers and consumers per topic constant, across the service tree."""
    prod = collections.defaultdict(set)
    cons = collections.defaultdict(set)

    for f in SERVICES.rglob("*.py"):
        if "__pycache__" in str(f):
            continue
        text = f.read_text(encoding="utf-8")
        svc = f.relative_to(SERVICES).parts[0]

        # send(Topics.X, ...) and send(topic=Topics.X, ...)
        for m in re.finditer(r"\.send\(\s*(?:topic\s*=\s*)?Topics\.([A-Z_]+)", text):
            prod[m.group(1)].add(svc)
        # An agent's declared output topic: the base class publishes there.
        for m in re.finditer(
            r"def output_topic[^\n]*\n(?:[^\n]*\n){0,6}?\s*return\s+Topics\.([A-Z_]+)", text
        ):
            prod[m.group(1)].add(svc)
        # Subscriptions, however they are spelled.
        for m in re.finditer(r"(?:input_topics|topics)\s*=\s*\[([^\]]*)\]", text, re.S):
            for tm in re.finditer(r"Topics\.([A-Z_]+)", m.group(1)):
                cons[tm.group(1)].add(svc)
        for m in re.finditer(r"message\.topic\s*==\s*Topics\.([A-Z_]+)", text):
            cons[m.group(1)].add(svc)

    return prod, cons


PROD, CONS = _scan()


def test_every_produced_topic_has_a_consumer():
    orphans = {
        t: sorted(p)
        for t, p in PROD.items()
        if not CONS.get(t) and t not in INTENTIONAL
    }
    assert not orphans, (
        "topics written by somebody and read by nobody: "
        + "; ".join(f"{t} (from {', '.join(s)})" for t, s in sorted(orphans.items()))
        + ". Either give it a consumer or stop producing it."
    )


def test_every_consumed_topic_has_a_producer():
    orphans = {
        t: sorted(c)
        for t, c in CONS.items()
        if not PROD.get(t) and t not in INTENTIONAL
    }
    assert not orphans, (
        "topics read by somebody and written by nobody: "
        + "; ".join(f"{t} (by {', '.join(s)})" for t, s in sorted(orphans.items()))
        + ". Each is a consumer group holding assignments for an empty stream."
    )


def test_topics_are_referenced_by_constant_not_by_string():
    """A literal topic name is how this platform previously ended up querying a
    topic that did not exist while the real one accumulated unread."""
    values = {
        getattr(Topics, n): n
        for n in dir(Topics)
        if not n.startswith("_") and isinstance(getattr(Topics, n), str)
    }
    offenders = []
    for f in SERVICES.rglob("*.py"):
        if "__pycache__" in str(f):
            continue
        for i, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            for value, name in values.items():
                if f'"{value}"' in line or f"'{value}'" in line:
                    offenders.append(f"{f.relative_to(ROOT)}:{i} -> Topics.{name}")
    assert not offenders, (
        "topic names written as string literals: " + "; ".join(offenders[:10])
    )


def test_the_intentional_list_does_not_hide_a_real_orphan():
    """An exemption must name a topic that actually exists."""
    names = {n for n in dir(Topics) if not n.startswith("_")}
    unknown = set(INTENTIONAL) - names
    assert not unknown, f"exemptions for topics that do not exist: {sorted(unknown)}"


@pytest.mark.parametrize("topic", sorted(INTENTIONAL))
def test_exempted_topics_are_still_declared(topic):
    assert hasattr(Topics, topic)
