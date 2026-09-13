"""Consumer lag was a null, and retention headroom was not measured at all.

`consumer_lag` reached every heartbeat through
`getattr(self, "_consumer_lag", None)` against an attribute no statement in the
tree assigns, so the field has been `null` on every heartbeat this platform has
ever published -- while the health surface it feeds exists specifically so a
consumer that is far behind cannot report itself healthy.

The second measurement is the one that was missing outright. Kafka deletes by
retention, not by consumption. A consumer that falls far enough behind has its
unread backlog deleted underneath it, and the visible symptom is lag *falling*.
Only the distance between the consumer's position and the oldest message the
broker still holds distinguishes "caught up" from "the backlog was dropped".
"""
import asyncio
from types import SimpleNamespace

import pytest

from shared.kafka import SentinelConsumer


class _TP(SimpleNamespace):
    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (self.topic, self.partition) == (other.topic, other.partition)


def _tp(topic, partition):
    return _TP(topic=topic, partition=partition)


class FakeAioKafka:
    """The slice of the aiokafka consumer surface lag_report uses."""

    def __init__(self, assignment, positions, begins, ends, raises=False):
        self._assignment = assignment
        self._positions = positions
        self._begins = begins
        self._ends = ends
        self._raises = raises

    def assignment(self):
        if self._raises:
            raise RuntimeError("broker unreachable")
        return self._assignment

    async def end_offsets(self, partitions):
        return {tp: self._ends[tp] for tp in partitions}

    async def beginning_offsets(self, partitions):
        return {tp: self._begins[tp] for tp in partitions}

    async def position(self, tp):
        return self._positions[tp]


def _consumer(fake, started=True):
    obj = SentinelConsumer.__new__(SentinelConsumer)
    obj._c = fake
    obj._started = started
    return obj


A0 = _tp("raw.events", 0)
A1 = _tp("raw.events", 1)


def test_lag_is_the_distance_to_the_end():
    fake = FakeAioKafka(
        assignment={A0}, positions={A0: 100}, begins={A0: 0}, ends={A0: 250}
    )
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["lag"] == 150


def test_headroom_is_the_distance_from_the_oldest_surviving_message():
    fake = FakeAioKafka(
        assignment={A0}, positions={A0: 100}, begins={A0: 40}, ends={A0: 250}
    )
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["retention_headroom"] == 60
    assert report["at_retention_edge"] is False


def test_a_consumer_whose_backlog_is_being_deleted_is_flagged():
    # position == begin with work still outstanding: the next message this
    # consumer would read is the oldest the broker still has, so anything
    # older was deleted unread.
    fake = FakeAioKafka(
        assignment={A0}, positions={A0: 900}, begins={A0: 900}, ends={A0: 5000}
    )
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["at_retention_edge"] is True
    assert report["retention_headroom"] == 0
    assert report["lag"] == 4100


def test_a_caught_up_consumer_at_the_edge_is_not_flagged():
    # An empty topic has position == begin == end. Nothing was lost; there is
    # simply nothing there, and flagging it would cry wolf on every idle topic.
    fake = FakeAioKafka(
        assignment={A0}, positions={A0: 0}, begins={A0: 0}, ends={A0: 0}
    )
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["at_retention_edge"] is False
    assert report["lag"] == 0


def test_totals_are_summed_across_partitions():
    fake = FakeAioKafka(
        assignment={A0, A1},
        positions={A0: 100, A1: 200},
        begins={A0: 50, A1: 150},
        ends={A0: 130, A1: 260},
    )
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["lag"] == 30 + 60
    assert report["retention_headroom"] == 50 + 50
    assert len(report["partitions"]) == 2


def test_an_unstarted_consumer_reports_nothing_rather_than_zero():
    # Zero lag and unknown lag are different claims, and a health surface that
    # cannot tell them apart grades a dead consumer as caught up.
    fake = FakeAioKafka(assignment={A0}, positions={A0: 0}, begins={A0: 0}, ends={A0: 0})
    report = asyncio.run(_consumer(fake, started=False).lag_report())
    assert report["lag"] is None
    assert report["retention_headroom"] is None


def test_an_unassigned_consumer_reports_nothing():
    fake = FakeAioKafka(assignment=set(), positions={}, begins={}, ends={})
    assert asyncio.run(_consumer(fake).lag_report())["lag"] is None


def test_a_broker_failure_does_not_break_the_heartbeat():
    # This runs inside the heartbeat loop. A metrics failure must not be able to
    # stop the heartbeat it is attached to.
    fake = FakeAioKafka(assignment={A0}, positions={}, begins={}, ends={}, raises=True)
    report = asyncio.run(_consumer(fake).lag_report())
    assert report["lag"] is None
    assert report["partitions"] == []
