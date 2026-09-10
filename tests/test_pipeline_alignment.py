"""Batch bookkeeping: producers that are started, offsets that are scoped,
and results that stay attached to the work they came from.

Four defects of one shape. In each, a list was appended to conditionally and
then zipped against a list that had not been filtered the same way, so after
the first skipped item every result was attributed to the wrong thing --
silently, because the values were all of the right type.

  * enrichment produce: `produce_tasks` skips events below the fan-out floor
    (6,969 of 17,506 in a measured half hour) and was zipped against the
    unfiltered `batch_to_write`;
  * enrichment enrich: `enrich_tasks` skips topics with no registered enricher
    and was zipped against the unfiltered `raw_events_by_topic.items()`;
  * agent dispatch: `tasks` is appended to only for messages that parse, and
    was zipped against the full `msg_list`, so one poison pill misdirected the
    dead-letter payload of every dispatch after it;
  * the enrichment commit ran inside the per-partition loop and committed every
    assigned partition, including ones that had failed.

Plus a producer that was constructed and never started, whose every send
therefore raised into a `gather(..., return_exceptions=True)` that nobody
inspected.
"""
import ast
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENRICHMENT = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
AGENT_BASE = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")


# ── the unstarted producer ────────────────────────────────────────────────

@pytest.mark.anyio
async def test_an_unstarted_producer_refuses_to_send():
    """The precondition that made the omission fatal rather than cosmetic.

    Constructed inside the loop, because aiokafka's producer requires a running
    one at construction -- which is itself why the missing start() was easy to
    miss: the object builds fine and only fails when used.
    """
    from shared.kafka import SentinelProducer

    producer = SentinelProducer(service_name="test-unstarted")
    with pytest.raises(RuntimeError, match="not started"):
        await producer.send("dead.letter", {"hello": "world"})


def test_the_dead_letter_producer_is_started():
    assert "await dlq.start()" in ENRICHMENT, (
        "dlq was constructed and never started; every dead-letter write raised "
        "RuntimeError into a gather(return_exceptions=True) nobody read."
    )


def test_the_dead_letter_producer_is_closed():
    assert "await dlq.close()" in ENRICHMENT


def test_the_dead_letter_producer_starts_before_it_is_used():
    start = ENRICHMENT.index("await dlq.start()")
    first_send = ENRICHMENT.index("dlq.send(")
    assert start < first_send, "dlq.start() must precede the first dlq.send()"


# ── offsets are committed for the partition that succeeded ────────────────

def test_enrichment_commits_a_specific_partition():
    assert "await consumer.commit({tp: messages[-1].offset + 1})" in ENRICHMENT, (
        "a bare commit() inside the per-partition loop commits every assigned "
        "partition, including ones whose batch failed"
    )


def test_no_bare_global_commit_remains_in_the_enrichment_batch_loop():
    assert "await consumer.commit()\n" not in ENRICHMENT


def test_agents_commit_explicit_offsets():
    assert "await self._consumer.commit(offsets)" in AGENT_BASE
    assert "msgs[-1].offset + 1" in AGENT_BASE


# ── results stay attached to the work they came from ──────────────────────

def test_produce_results_are_not_zipped_against_the_unfiltered_batch():
    assert "zip(batch_to_write, produce_results)" not in ENRICHMENT, (
        "batch_to_write is filtered by the fan-out floor before producing; "
        "zipping against it misattributes every failure after the first skip"
    )
    assert "zip(produced_events, produce_results)" in ENRICHMENT


def test_enrich_results_are_not_zipped_against_the_unfiltered_topics():
    assert "zip(results, list(raw_events_by_topic.items()))" not in ENRICHMENT
    assert "zip(results, enrich_tasks)" in ENRICHMENT


def test_a_topic_without_an_enricher_is_reported_rather_than_skipped():
    assert "No enricher registered for topic" in ENRICHMENT
    assert "enrichment_unrouted_topic_total" in ENRICHMENT


def test_agent_tasks_are_not_zipped_against_the_unfiltered_message_list():
    assert "zip(tasks, msg_list)" not in AGENT_BASE, (
        "tasks is appended to only for messages that parse; one poison pill "
        "offsets the pairing for the rest of the batch"
    )
    assert "for task, msg in tasks:" in AGENT_BASE


def test_agent_tasks_carry_their_message():
    """The structural property, checked on the AST rather than on the text."""
    tree = ast.parse(AGENT_BASE)
    found = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "append":
            if isinstance(func.value, ast.Name) and func.value.id == "tasks":
                if node.args and isinstance(node.args[0], ast.Tuple):
                    found = True
    assert found, "tasks.append(...) should append a (task, msg) tuple"


# ── one live-feed publisher ───────────────────────────────────────────────

def test_the_live_feed_is_published_exactly_once():
    assert ENRICHMENT.count('publish("sentinel:events:live"') == 1, (
        "two publishers on this channel delivered every event twice to every "
        "subscriber, and any UI counting them counted double"
    )
