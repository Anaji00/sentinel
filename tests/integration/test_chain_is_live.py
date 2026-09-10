"""Phase 4.13.2: every hop of the chain, not just the first two.

The smoke test next door carries one event from Kafka through enrichment into
Timescale, which is the boundary that matters for a query. The plan asks for the
whole chain -- collector payload, Kafka, enrichment, Timescale, correlation
engine, alert broadcast -- and the last two hops were untested.

Deliberately not "publish a probe and wait for an alert about it". A correlation
needs corroborating evidence in a 48-hour window and an alert needs a tier the
rule earns, so a probe that produces no cluster is the correct behaviour and
would read as a broken platform. What can be asserted without inventing a
finding is that each hop is *moving*: its consumer group is committing, and its
output is growing. A stalled hop shows up as a committed offset that does not
advance while the log end does -- which is exactly what an audit found on the
radar orchestrator, holding a partition at one offset for sixty seconds while
lag grew.
"""
import asyncio
import contextlib
import os
import pathlib
import sys
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.integration

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# Long enough for a slow hop to show movement, short enough to stay a test.
OBSERVE_SEC = float(os.getenv("CHAIN_OBSERVE_SEC", "45"))


def _stack_is_up() -> bool:
    import socket

    host, _, port = KAFKA.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except OSError:
        return False


requires_stack = pytest.mark.skipif(not _stack_is_up(), reason=f"no broker at {KAFKA}")


async def _log_end(topic):
    from aiokafka import AIOKafkaConsumer

    c = AIOKafkaConsumer(topic, bootstrap_servers=KAFKA, group_id=None)
    await c.start()
    try:
        tps = list(c.assignment())
        if not tps:
            return None
        return sum((await c.end_offsets(tps)).values())
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await c.stop()


@requires_stack
@pytest.mark.anyio
async def test_enrichment_is_still_producing():
    """Enrichment output is continuous, so a short window is a fair test.

    Measured on this deployment: 7.5 events a second. A 45-second hold is not a
    quiet stretch, it is a stopped service -- and its heartbeat, which counts
    messages consumed, would look identical either way.
    """
    before = await _log_end("enriched.events")
    if before is None:
        pytest.skip("enriched.events has no partitions")

    await asyncio.sleep(OBSERVE_SEC)
    after = await _log_end("enriched.events")

    assert after > before, (
        f"enrichment produced nothing in {OBSERVE_SEC:.0f}s (offset held at "
        f"{before}). At 7.5/s that is a stopped service, not a quiet market."
    )


@requires_stack
@pytest.mark.anyio
async def test_correlation_is_still_consuming():
    """Consumption, not production -- the distinction is the whole point.

    This asserted that `sentinel.correlations` grew within the same 45-second
    window, and it failed against a healthy platform: 106 clusters in the hour,
    the newest two seconds old, but a mean gap of 14s with a **maximum gap of
    585s**. A correlation needs corroborating evidence in its window, so the
    producer is bursty by construction and a fixed short window fails whenever
    it lands in a quiet stretch. The test reported the platform broken because
    the test was wrong -- the same mistake as inventing a consumer group name,
    made twice in one audit.

    What is invariant is the other side: correlation consumes `enriched.events`
    continuously, so its committed offset must advance. That is also the
    sharper check. This audit already found the radar orchestrator holding a
    partition at one offset for sixty seconds while the log end moved and its
    lag grew -- a stall that a production-side assertion cannot see at all.
    """
    from aiokafka import TopicPartition
    from aiokafka.admin import AIOKafkaAdminClient

    group = "correlation-engine"

    async def committed():
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
        await admin.start()
        try:
            offsets = await admin.list_consumer_group_offsets(group)
        finally:
            await admin.close()
        return {
            tp: md.offset
            for tp, md in offsets.items()
            if isinstance(tp, TopicPartition)
            and tp.topic == "enriched.events"
            and md.offset >= 0
        }

    before = await committed()
    if not before:
        pytest.skip(
            f"{group} holds no committed offsets on enriched.events yet; it has "
            "not completed a batch since joining."
        )

    await asyncio.sleep(OBSERVE_SEC)
    after = await committed()

    advanced = [tp for tp, off in after.items() if off > before.get(tp, -1)]
    assert advanced, (
        f"{group} committed nothing on any of {len(before)} partition(s) in "
        f"{OBSERVE_SEC:.0f}s. Offsets before={sorted(before.values())} "
        f"after={sorted(after.values())}. A consumer that stops committing "
        "keeps its heartbeat and stops being part of the chain."
    )


@requires_stack
@pytest.mark.anyio
async def test_the_consumers_on_the_chain_are_committing():
    """Lag zero is not health; a group that never commits has none either."""
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
    await admin.start()
    try:
        groups = {g[0] for g in await admin.list_consumer_groups()}
    finally:
        await admin.close()

    for group in ("enrichment-service", "correlation-engine", "alert-manager"):
        assert group in groups, (
            f"{group} has never joined the broker, so that hop of the chain is "
            f"not consuming however healthy it looks. Present: {sorted(groups)}"
        )


@requires_stack
@pytest.mark.anyio
async def test_the_last_hop_reaches_a_store_a_reader_can_open():
    """The alert manager records before it attempts delivery.

    That ordering is the fix for a defect this audit found: unconfigured, it
    returned False from `_send_telegram` and dropped the alert entirely -- a
    service whose whole job is not missing things was the one component that
    could lose them silently. So the store is checkable without any outbound
    credential being present.
    """
    from shared.db import get_redis

    redis = await get_redis()
    if redis is None:
        pytest.skip("redis unavailable")

    raw = getattr(redis, "raw", redis)
    try:
        exists = await raw.exists("sentinel:alerts:recent")
    except Exception as e:
        pytest.skip(f"redis not readable: {e}")

    if not exists:
        pytest.skip(
            "no alerts recorded yet. The key is written on the first alert that "
            "clears its rule's tier, which needs corroborating evidence rather "
            "than uptime -- an empty store here is not a failure."
        )
    length = await raw.llen("sentinel:alerts:recent")
    assert length > 0
