"""Keeps repeated position fixes from re-proposing facts that never change.

A vessel or aircraft emits a fix every few seconds carrying the same
registration country and flag state each time. Every one of those was
re-proposing the same node and the same edges onto Kafka, and the supervisor
MERGEs idempotently, so none of it changed anything.

Measured: enrichment fell from 14 events/s to 3.6/s once aviation began writing
to the graph, and its backlog became almost entirely aviation -- 39,615 of about
40,000 -- while aviation produces ~700 fixes a minute at up to three sends each.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.enrichment.graph_writer import GraphWriter, _PROPOSAL_MEMO_MAX  # noqa: E402


class RecordingProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, payload, key=None):
        self.sent.append((topic, payload, key))


@pytest.fixture
def writer():
    return GraphWriter(RecordingProducer())


@pytest.mark.anyio
async def test_repeated_fixes_do_not_re_propose_the_same_aircraft(writer):
    for _ in range(50):
        await writer.upsert_aircraft("a1b2c3", {"callsign": "UAL42",
                                                "origin_country": "United States",
                                                "region": "NORTH ATLANTIC"})
    # One node proposal, one registration edge, one region edge -- not 150.
    assert len(writer.producer.sent) <= 3, (
        f"{len(writer.producer.sent)} sends for 50 identical fixes"
    )


@pytest.mark.anyio
async def test_repeated_fixes_do_not_re_propose_the_same_vessel(writer):
    for _ in range(50):
        await writer.upsert_vessel("271044408", {"name": "LADY JASMIN",
                                                 "flag_state": "LR",
                                                 "region": "BLACK SEA"})
    assert len(writer.producer.sent) <= 4, (
        f"{len(writer.producer.sent)} sends for 50 identical fixes"
    )


@pytest.mark.anyio
async def test_a_genuine_region_change_still_proposes(writer):
    """Registration is fixed for the life of an asset; location is not."""
    await writer.upsert_vessel("271044408", {"name": "LADY JASMIN", "flag_state": "LR",
                                             "region": "BLACK SEA"})
    before = len(writer.producer.sent)
    await writer.upsert_vessel("271044408", {"name": "LADY JASMIN", "flag_state": "LR",
                                             "region": "TURKISH STRAITS"})
    assert len(writer.producer.sent) > before, "a vessel changing region proposed nothing"


@pytest.mark.anyio
async def test_returning_to_a_known_region_is_silent(writer):
    await writer.upsert_vessel("1", {"name": "V", "flag_state": "LR", "region": "BLACK SEA"})
    await writer.upsert_vessel("1", {"name": "V", "flag_state": "LR", "region": "TURKISH STRAITS"})
    settled = len(writer.producer.sent)
    for _ in range(20):
        await writer.upsert_vessel("1", {"name": "V", "flag_state": "LR", "region": "BLACK SEA"})
    assert len(writer.producer.sent) == settled, "oscillating between known regions re-proposed"


@pytest.mark.anyio
async def test_distinct_assets_are_tracked_separately(writer):
    await writer.upsert_aircraft("aaa111", {"callsign": "A", "origin_country": "X", "region": "R"})
    first = len(writer.producer.sent)
    await writer.upsert_aircraft("bbb222", {"callsign": "B", "origin_country": "Y", "region": "R"})
    assert len(writer.producer.sent) > first, "a second aircraft was mistaken for the first"


@pytest.mark.anyio
async def test_the_memo_is_bounded(writer):
    """Aircraft and vessel populations are large and unbounded over time."""
    for i in range(_PROPOSAL_MEMO_MAX + 500):
        writer._already_proposed("k", i)
    assert len(writer._seen) <= _PROPOSAL_MEMO_MAX, "the dedup memo grew without bound"
