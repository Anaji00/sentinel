"""Batching pays by charging one prompt evaluation instead of N.

On this host prompt evaluation is 44-67% of a call, so a batcher that mostly
flushes a single item saves nothing while looking like an optimisation. The
batcher recorded nothing about how full its batches ran, which made "should
more agents batch?" unanswerable except by guessing -- and building more
batching on a guess is the shape this audit exists to find.

`singletons` is the number that decides it: flushes that carried one item and
therefore paid full price.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")


def test_the_batcher_counts_how_full_it_runs():
    for counter in ("self.flushes", "self.items_batched",
                    "self.largest_batch", "self.singleton_flushes"):
        assert counter in SRC, counter


def test_a_single_item_flush_is_counted_separately():
    """A batch of one is the case that makes batching pointless."""
    assert "if len(batch) == 1:" in SRC
    assert "self.singleton_flushes += 1" in SRC


def test_the_snapshot_reports_the_average_fill():
    block = SRC[SRC.index("def snapshot(self) -> Dict[str, Any]:"):]
    block = block[: block.index("\n    async def ", 10)]
    for key in ("flushes", "avg_fill", "largest", "singletons", "max_items"):
        assert f'"{key}"' in block, key


def test_the_counters_reach_the_heartbeat():
    """snapshot() existed on the candidate buffer and reached nothing once."""
    assert '"batchers": {' in SRC
    assert "isinstance(b, InferenceBatcher)" in SRC


def test_counting_happens_after_the_inflight_guard():
    """Counting a batch that was never drained would inflate the fill rate."""
    drain = SRC.index("batch, self._pending = self._pending, []")
    guard = SRC.index("if self._inflight or not self._pending:")
    count = SRC.index("self.flushes += 1")
    assert guard < drain < count
