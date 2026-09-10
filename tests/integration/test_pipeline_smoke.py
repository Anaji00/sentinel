"""One event through the real pipeline: Kafka to enrichment to TimescaleDB.

Every other test in this repository runs against doubles. That is what let a
producer be constructed and never started, a fake Redis miss three commands its
subject depended on, and a session token skip its expiry check -- each of them
green against a mock and wrong against the thing itself.

This is deliberately not Testcontainers. The stack is already defined in
docker-compose.yml with its profiles, resource ceilings, healthchecks and
service dependencies, and those are part of what needs testing: the Kafka
healthcheck that spawned a JVM and timed out under load, and the model
keep-alive that one service silently un-pinned, would both have passed against
containers a test framework started with defaults. Running against the compose
stack tests the configuration that actually ships.

Skipped, loudly, when the stack is not up. A smoke test that quietly passes
because it could not reach anything is worse than no smoke test.
"""
import asyncio
import json
import os
import pathlib
import re
import sys
import time
import uuid
from datetime import datetime, timezone

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.integration

# No default that points somewhere plausible-but-wrong.
#
# A default of localhost:9092 let this connect to the listener the broker
# advertises as localhost, so bootstrap succeeded and every later request was
# redirected to 127.0.0.1 inside the test container -- a broken test that reads
# exactly like a broken broker. The internal listener is kafka:29092 and .env
# already says so.
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# How long the pipeline is given to carry one event end to end. Generous,
# because enrichment batches and the correlation window are not tuned for a
# single message in an idle system.
PIPELINE_TIMEOUT_SEC = float(os.getenv("SMOKE_TIMEOUT_SEC", "90"))

# Consumer group ids exactly as the services declare them. Kept as a module
# constant so the quoting lives in one place.
GROUP_ID_RE = re.compile(r"""group_id\s*=\s*['"]([^'"]+)['"]""")


def _stack_is_up() -> bool:
    """Whether a broker is actually listening. Cheap and honest."""
    import socket

    host, _, port = KAFKA.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except OSError:
        return False


requires_stack = pytest.mark.skipif(
    not _stack_is_up(),
    reason=(
        f"no Kafka broker at {KAFKA}. Start the stack with "
        "`COMPOSE_PROFILES=collectors,agents docker compose up -d` to run this."
    ),
)


@pytest.fixture
def probe_ticker():
    """A ticker no collector will ever emit, so the row can only be ours."""
    return f"ZZTEST{uuid.uuid4().hex[:6].upper()}"


@requires_stack
@pytest.mark.anyio
async def test_a_raw_event_reaches_the_database_enriched(probe_ticker):
    """Publish one raw tradfi event; assert an enriched row appears.

    The assertion is on the database rather than on an intermediate topic
    because that is the boundary that matters: an event that reaches Kafka and
    not Timescale is invisible to every query the product makes.
    """
    from aiokafka import AIOKafkaProducer

    from shared.db import get_timescale
    from shared.kafka import Topics

    # The source has to be one the enricher routes, and the trade has to be
    # big enough to survive the persistence floor. Neither is decoration:
    #
    # `source` is the only thing TradFiEnricher.enrich_batch dispatches on, and
    # an unrecognised one fell off the end of the chain to a bare `return
    # None`. The first version of this test used "smoke_test" and failed for
    # ninety seconds with a message blaming enrichment, the DB writer and the
    # batch commit -- all three innocent. That silent drop is now counted
    # (`enrichment.tradfi.unrouted_source`), which is the actual fix; using a
    # real source here is what makes this test exercise the pipeline instead.
    #
    # `_enrich_equity_trade_batch` then discards anything scoring under 0.35,
    # deliberately: an unremarkable trade is not worth a row. A quarter-billion
    # dollar block clears it under any scoring the collector could produce, so
    # the test does not depend on how the scorer is tuned.
    event = {
        "event_id": str(uuid.uuid4()),
        "source": "alpaca_extended_hours",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": {
            "ticker": probe_ticker,
            "price": 123.45,
            "volume": 2_000_000,
            "notional_usd": 246_900_000.0,
            "trade_type": "BLOCK",
            "event_subtype": "equity_block",
        },
    }

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(Topics.RAW_TRADFI, event, key=probe_ticker.encode())
    finally:
        await producer.stop()

    db = await get_timescale()
    deadline = time.monotonic() + PIPELINE_TIMEOUT_SEC
    row = None
    while time.monotonic() < deadline:
        rows = await db.query(
            "SELECT event_id, type, anomaly_score FROM events "
            "WHERE primary_entity_id = $1 ORDER BY occurred_at DESC LIMIT 1",
            probe_ticker,
        )
        if rows:
            row = rows[0]
            break
        await asyncio.sleep(2)

    assert row is not None, (
        f"{probe_ticker} never reached the events table within "
        f"{PIPELINE_TIMEOUT_SEC:.0f}s. The event was accepted by Kafka, so the "
        "break is downstream: enrichment, the DB writer, or the batch commit."
    )
    # An enriched event carries a score. Zero is a legitimate score; None means
    # the scorer never ran.
    assert row["anomaly_score"] is not None


@requires_stack
@pytest.mark.anyio
async def test_the_services_that_should_be_consuming_are_consuming():
    """Every declared consumer group exists on the broker.

    A service can look healthy, log heartbeats and consume nothing -- that is
    what the unstarted producer and the wedged dispatch loop both looked like
    from outside. A group that has never joined has no offsets at all.
    """
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
    await admin.start()
    try:
        groups = {g[0] for g in await admin.list_consumer_groups()}
    finally:
        await admin.close()

    # Derived from the source, not guessed.
    #
    # This hardcoded "enrichment-service-group" and failed against a live,
    # healthy broker because the service registers as "enrichment-service".
    # A test that invents the name it is checking reports the platform broken
    # when the test is wrong -- which is exactly what it did, twice, while a
    # real defect sat next to it.
    import pathlib as _p
    import re as _re

    declared = set()
    for f in (_p.Path(ROOT) / "services").rglob("*.py"):
        if "__pycache__" in str(f):
            continue
        for m in _re.finditer(GROUP_ID_RE, f.read_text(encoding="utf-8")):
            declared.add(m.group(1))

    assert declared, "no consumer group ids found in the service tree"
    missing = {g for g in declared if g not in groups}
    assert not missing, (
        f"declared consumer groups that have never joined the broker: {sorted(missing)}. "
        f"A group with no offsets is a service that is not consuming, however "
        f"healthy its heartbeat looks. Present: {sorted(groups)}"
    )


@requires_stack
@pytest.mark.anyio
async def test_the_resident_model_is_not_scheduled_to_expire():
    """Whatever is loaded is pinned, and only one thing is loaded.

    Both halves matter and neither is visible until something is slow. A model
    with a finite expiry is a service sending its own keep_alive and paying an
    89s reload on the next burst. A second resident model is a second
    llama.cpp runner started with NumThreads matching the whole six-core quota,
    which is the thread-barrier collapse recorded in docker-compose.yml --
    506% of 600% CPU and every inference timing out having produced nothing.

    This test previously ended its assertion with `or True`, so it passed
    against any expiry, any count, and an empty response.
    """
    import aiohttp

    base = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/api/ps", timeout=10) as resp:
                if resp.status != 200:
                    pytest.skip(f"ollama /api/ps returned {resp.status}")
                data = await resp.json()
    except Exception as e:
        pytest.skip(f"ollama not reachable at {base}: {e}")

    models = data.get("models") or []
    if not models:
        pytest.skip("no model loaded yet; nothing has asked for one since start")

    assert len(models) == 1, (
        "more than one resident model: "
        f"{[m.get('name') for m in models]}. One slot is configured precisely "
        "so this cannot happen; two runners do not fit six cores."
    )

    # KEEP_ALIVE=-1 is reported as an expiry far enough out that it is never
    # reached, rather than as a sentinel, so this asks the question that
    # actually matters: will it still be here for the next burst?
    m = models[0]
    raw = str(m.get("expires_at") or "")
    assert raw, f"{m.get('name')} reports no expiry at all"
    expires = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    hours = (expires - datetime.now(timezone.utc)).total_seconds() / 3600
    assert hours > 1.0, (
        f"{m.get('name')} expires in {hours:.2f}h ({raw}). A finite keep_alive "
        "means some caller is un-pinning what the agents pinned with -1, and "
        "the reload it causes costs more than every request it serves."
    )
