"""
tests/test_routine_positions_stay_out.py

Position fixes were most of what the platform stored and distributed.

Measured on 3 October: vessel and aircraft positions were 82% of events, at a
mean anomaly of 0.05, fanned out to ten agent inboxes and stored in `events`.
No vessel or aircraft node had any edge to a company, and the cascade engine
already discarded positions under 0.30. They still pass through the enrichers,
which keep last_seen and the vessel tables current; a routine one is now neither
stored as an event nor fanned out.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.main import ROUTINE_POSITION_FLOOR, _is_routine_position


def _e(etype, score, tags=()):
    return SimpleNamespace(type=SimpleNamespace(value=etype), anomaly_score=score, tags=list(tags))


def test_a_quiet_position_is_routine():
    for t in ("vessel_position", "flight_position", "vessel_static"):
        assert _is_routine_position(_e(t, 0.05)), t


def test_a_scoring_position_is_kept():
    assert not _is_routine_position(_e("vessel_position", ROUTINE_POSITION_FLOOR))
    assert not _is_routine_position(_e("flight_position", 0.8))


def test_a_tagged_position_is_kept_whatever_it_scored():
    assert not _is_routine_position(_e("vessel_position", 0.0, ["sanctions_risk"]))
    assert not _is_routine_position(_e("flight_position", 0.0, ["emergency"]))
    assert not _is_routine_position(_e("vessel_position", 0.0, ["dark_vessel"]))


def test_findings_are_never_routine():
    for t in ("vessel_sts", "vessel_spoof", "flight_anomaly", "vessel_dark", "equity_block", "macro_release"):
        assert not _is_routine_position(_e(t, 0.0)), t


def test_the_floor_matches_the_cascade_engines():
    src = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    assert 'in ("vessel_position", "flight_position") and score < 0.30' in src
    assert ROUTINE_POSITION_FLOOR == 0.30


def test_routine_positions_are_dropped_after_the_vessel_tables_are_written():
    src = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    body = src.split("async def main():")[1]
    assert body.index("write_vessel_positions_batch(") < body.index("_is_routine_position(e)")
    assert body.index("_is_routine_position(e)") < body.index("await db.write_events_batch(batch_to_write)")
