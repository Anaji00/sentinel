"""Phase 4.13.3: what a producer emits must parse as what the consumer expects.

The topic contracts next door check that every produced topic has a consumer and
every consumed topic has a producer. That is the wiring. This is the payload:
five services call `Model(**raw_data)` directly on whatever arrives, so a
producer changing a field name is a validation error at the far end of a Kafka
topic, hours later, in someone else's service.

That is not hypothetical here. The telemetry worker read five keys the wargamer
never sent and filled its table with well-formed rows carrying almost nothing;
the alert manager called `CorrelationCluster(**payload)` on an agent assessment
sharing none of its five required fields and dropped four messages in thirty
minutes; and the reasoning consumer hit the identical defect from the DLQ side
months earlier and was fixed alone, leaving the alert manager still raising.

So these read real messages off the broker and parse them with the model the
consumer actually uses. A topic with no traffic is skipped and says so -- an
empty topic proves nothing either way, and reporting it as a pass is how a
contract test stops being one.
"""
import asyncio
import json
import os
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.integration

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SAMPLE_TIMEOUT_MS = int(os.getenv("CONTRACT_SAMPLE_MS", "8000"))
MAX_SAMPLES = 25


def _stack_is_up() -> bool:
    import socket

    host, _, port = KAFKA.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except OSError:
        return False


requires_stack = pytest.mark.skipif(not _stack_is_up(), reason=f"no broker at {KAFKA}")


def _contracts():
    """(topic, model) pairs, named the way the consuming service names them."""
    from shared.kafka import Topics
    from shared.models.events import (
        CorrelationCluster,
        NormalizedEvent,
        RawEvent,
    )

    return [
        # enrichment: RawEvent(**raw_data)
        (Topics.RAW_TRADFI, RawEvent),
        (Topics.RAW_CRYPTO, RawEvent),
        (Topics.RAW_NEWS, RawEvent),
        (Topics.RAW_CYBER, RawEvent),
        (Topics.RAW_MARITIME, RawEvent),
        (Topics.RAW_AVIATION, RawEvent),
        # correlation: NormalizedEvent(**raw_data)
        (Topics.ENRICHED_EVENTS, NormalizedEvent),
        # reasoning and alert manager: CorrelationCluster(**raw_data)
        (Topics.CORRELATIONS, CorrelationCluster),
    ]


async def _sample(topic, limit=MAX_SAMPLES):
    """The newest messages on a topic, without joining a consumer group.

    Subscribed rather than manually assigned: `partitions_for_topic` returns
    None for a topic the consumer has never subscribed to, and assigning an
    empty list then fails deep inside the group coordinator as a
    `CancelledError` that looks like a broken broker. Subscribing gives a real
    assignment, and `group_id=None` keeps this out of the offsets the smoke
    test checks.
    """
    import contextlib

    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=KAFKA, group_id=None, enable_auto_commit=False
    )
    await consumer.start()
    try:
        tps = list(consumer.assignment())
        if not tps:
            return []
        ends = await consumer.end_offsets(tps)
        begins = await consumer.beginning_offsets(tps)
        if sum(ends.values()) == sum(begins.values()):
            return []
        for tp in tps:
            consumer.seek(tp, max(begins[tp], ends[tp] - limit))

        out = []
        batch = await consumer.getmany(timeout_ms=SAMPLE_TIMEOUT_MS, max_records=limit)
        for msgs in batch.values():
            out.extend(m.value for m in msgs)
        return out
    finally:
        # aiokafka cancels its own committed-offset task on stop and lets the
        # CancelledError out. Suppressed here and nowhere else.
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.stop()


@requires_stack
@pytest.mark.anyio
@pytest.mark.parametrize(
    "topic,model",
    _contracts(),
    ids=[t for t, _ in _contracts()],
)
async def test_live_messages_parse_as_the_consumer_parses_them(topic, model):
    raw = await _sample(topic)
    if not raw:
        pytest.skip(
            f"{topic} carried no messages in the sampled window. An empty topic "
            "proves nothing; reporting it as a pass is how a contract test stops "
            "being one."
        )

    failures = []
    for blob in raw:
        try:
            payload = json.loads(blob.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            failures.append(f"not JSON: {e}")
            continue
        if not isinstance(payload, dict):
            failures.append(f"not an object: {type(payload).__name__}")
            continue
        try:
            model(**payload)
        except Exception as e:
            # First line only; a pydantic report for 30 messages is unreadable.
            failures.append(f"{model.__name__}: {str(e).splitlines()[0]}")

    assert not failures, (
        f"{len(failures)} of {len(raw)} messages on {topic} do not parse as "
        f"{model.__name__}, which is what its consumer calls on every message:\n  "
        + "\n  ".join(sorted(set(failures))[:5])
    )


@requires_stack
@pytest.mark.anyio
async def test_enriched_events_carry_the_fields_correlation_indexes_on():
    """Parsing is not enough: the correlation window keys on these."""
    from shared.kafka import Topics

    raw = await _sample(Topics.ENRICHED_EVENTS)
    if not raw:
        pytest.skip("no enriched events in the sampled window")

    missing = {"type": 0, "occurred_at": 0, "anomaly_score": 0}
    for blob in raw:
        try:
            payload = json.loads(blob.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            continue
        for field in missing:
            if payload.get(field) is None:
                missing[field] += 1

    absent = {k: v for k, v in missing.items() if v}
    assert not absent, (
        f"of {len(raw)} enriched events: {absent} are missing a field the "
        "correlation store sorts, filters or prunes on."
    )
