"""
tests/test_gap_dedup_survives_restart.py

"464 flight_dark events in 6 hours -- the null model is no longer silent" was
wrong, and the shape of the data said so:

    14:00    2        17:00  200        19:00-21:00   none
    15:00    1        18:00  262        22:00          15
    16:00    2

462 of the 464 landed in the two hours of repeated redeploys, then nothing for
three hours. The newest container's first scan emitted 15 events in a single
second. The steady-state rate is the one at 14:00-16:00: one or two an hour.

The deduplication set lived in process memory, so every restart emptied it and
the next scan re-reported every aircraft already in a gap as though it had just
gone dark. Those events are indistinguishable in the database from real ones,
which is what made a deployment artifact look like a working detector.

A second defect sat underneath. The set was written *before* the percentile
test, so an aircraft whose gap was ordinary for its airspace was still marked as
reported -- and when it later went genuinely dark, it was skipped as a duplicate
of an alert that had never been raised. The suppression outlived its reason.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "enrichment" / "aviation_gap_detector.py").read_text(encoding="utf-8")


def test_dedup_is_not_process_memory():
    """An in-memory set is empty after every deploy, and the first scan then
    re-reports the entire standing backlog."""
    assert "self._seen_gaps = set()" not in SOURCE
    assert "_seen_gaps" not in SOURCE


def test_dedup_lives_where_it_survives_a_restart():
    assert 'self._seen_key = "sentinel:aviation:seen_gaps"' in SOURCE
    assert "hexists(self._seen_key" in SOURCE


def test_an_aircraft_is_marked_only_when_an_event_is_emitted():
    """Marking before the decision suppressed alerts that were never raised."""
    mark = SOURCE.index("hset(self._seen_key")
    for gate in ("if len(samples) < _MIN_GAP_SAMPLES:", "if rank < _NOTABLE_PERCENTILE:"):
        assert SOURCE.index(gate) < mark, f"{gate} must be decided before marking"


def test_returning_to_normal_clears_the_record():
    """Otherwise an aircraft reported once could never be reported again."""
    block = SOURCE.split("if gap_hours < threshold:")[1][:400]
    assert "hdel(self._seen_key" in block


def test_the_record_expires_so_a_second_disappearance_still_counts():
    from services.enrichment.aviation_gap_detector import SEEN_GAP_TTL_SEC

    assert SEEN_GAP_TTL_SEC >= 86400, "must outlive a deploy"
    assert SEEN_GAP_TTL_SEC <= 30 * 86400, "must not suppress forever"


def test_a_redis_failure_does_not_suppress_a_real_event():
    """If the store is unreachable, reporting a duplicate is the safer error."""
    block = SOURCE.split("if await self.redis.raw.hexists(self._seen_key, dedup_key):")[1][:200]
    assert "except Exception" in block


def test_the_percentile_bar_is_still_what_decides():
    """This changes what is suppressed, not what qualifies."""
    from services.enrichment.aviation_gap_detector import _NOTABLE_PERCENTILE

    assert 0.5 < _NOTABLE_PERCENTILE < 1.0
