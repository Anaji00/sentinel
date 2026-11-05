"""A ship's score follows what it does; its status raises it a little, and only once in full.

Over 14 days to 4 Nov 2026, 293 of 357 stored ships reached 0.9 and OSHONE averaged
0.88 over 3,332 reports from one spot, because flag and status were detector inputs
and also took a ship out of the routine band.
"""
import asyncio
import json
from pathlib import Path

from services.enrichment.anomaly_scorer import DETECTOR_POINT_WIDTH, DynamicAnomalyScorer
from services.enrichment.enrichers.maritime import _FLAG_NOTES
from shared.utils.regions import ROUTINE_TELEMETRY_CEILING
from shared.utils.sanctions import sanctions_strength
from shared.utils.vessel_score import (
    ENTERED_RESTRICTED_LIFT,
    ODD_MOTION_CEILING,
    ODD_MOTION_CUT,
    ODD_MOTION_FLOOR,
    entered_restricted,
    position_score,
)

ROOT = Path(__file__).resolve().parents[1]
ROUTINE_FLOOR = 0.30  # ROUTINE_POSITION_FLOOR: what it takes to be stored


def test_ordinary_motion_stays_routine_whatever_the_status() -> None:
    """The detector's score is a percentile: ordinary motion reaches 0.9 a tenth of the time."""
    assert position_score(0.9, "Black Sea") <= ROUTINE_TELEMETRY_CEILING
    assert position_score(0.9, "Black Sea", restricted=True) <= ROUTINE_TELEMETRY_CEILING
    assert position_score(0.9, "Black Sea", listed=True) < 0.5
    assert position_score(0.9, "Black Sea", watched=True) < ROUTINE_FLOOR


def test_a_plain_ship_is_held_in_the_routine_band_even_at_the_top() -> None:
    assert position_score(0.99, "Black Sea") <= ROUTINE_TELEMETRY_CEILING


def test_odd_motion_lifts_a_status_ship_but_stays_below_the_findings() -> None:
    assert position_score(ODD_MOTION_CUT, None, restricted=True) == ODD_MOTION_FLOOR
    assert position_score(1.0, None, listed=True) == ODD_MOTION_CEILING
    assert ODD_MOTION_CEILING < 0.82  # a spoofed identity outranks unusual motion
    assert position_score(0.985, None, watched=True) < position_score(0.995, None, watched=True)


def test_a_ship_identified_by_number_is_stored_but_stays_below_the_alerting_bands() -> None:
    score = position_score(0.5, "Black Sea", identified=True)
    assert ROUTINE_FLOOR <= score < 0.5
    assert score > position_score(0.5, "Black Sea", listed=True)
    assert position_score(0.99, None, identified=True) >= ODD_MOTION_FLOOR


def test_entering_a_restricted_status_is_stored_and_staying_in_it_is_not() -> None:
    entered = position_score(0.2, "Black Sea", restricted=True, entered=True)
    staying = position_score(0.2, "Black Sea", restricted=True)
    assert entered >= ROUTINE_FLOOR > staying
    assert entered < 0.7, "news, not an alarm"
    assert ENTERED_RESTRICTED_LIFT < 0.55  # MAX_TOTAL_LIFT_SHARE


def test_when_a_ship_counts_as_having_entered_a_restricted_status() -> None:
    assert entered_restricted({"nav_code": 0}, 3)
    assert not entered_restricted({"nav_code": 3}, 3)
    assert not entered_restricted({"nav_code": 3}, 0), "leaving it is not entering it"
    assert entered_restricted({}, 3), "first sighting"
    assert not entered_restricted({"speed": 1.0}, 3), "a row from before the deploy says nothing"
    assert not entered_restricted(None, None)


def test_only_an_exact_name_match_counts_as_listed() -> None:
    assert sanctions_strength(["sanctioned_ofac", "sanctioned_kw:alexandra"]) == "listed"
    assert sanctions_strength(["sanctioned_ofac", "sanctioned_kw:x", "sanctions_adjacent_flag_state"]) == "listed"
    assert sanctions_strength(["sanctioned_ofac", "sanctioned_fuzzy:ignatova, anastasia"]) == "similar"
    assert sanctions_strength(["sanctions_adjacent_flag_state"]) == "flag_state"
    assert sanctions_strength([]) == sanctions_strength(None) == ""


def test_no_label_calls_a_ship_sanctioned() -> None:
    """The list is matched on names, and the flag state is only a country."""
    assert set(_FLAG_NOTES) == {"identified", "listed", "similar", "flag_state"}
    for note in _FLAG_NOTES.values():
        assert "sanctioned" not in note and "emergency" not in note


def test_the_detector_sees_motion_only() -> None:
    src = (ROOT / "services/enrichment/enrichers/maritime.py").read_text(encoding="utf-8")
    call = src[src.index("score_kinematic_event_batch("):]
    assert "extra_features" not in call[: call.index(")")]
    assert "extra_features_list.append" not in src


class _Raw:
    def __init__(self, stored: dict) -> None:
        self.stored = stored

    async def hgetall(self, _key: str) -> dict:
        return self.stored


def _restore(points: list) -> tuple:
    scorer = DynamicAnomalyScorer.__new__(DynamicAnomalyScorer)
    seen: list = []

    class _Detector:
        name = "maritime"

        def warm(self, pts: list) -> int:
            seen.extend(pts)
            return len(pts)

    scorer._rrcf_detectors = {"maritime": _Detector()}
    scorer.redis = type("R", (), {"raw": _Raw({"maritime": json.dumps(points)})})()
    return asyncio.run(scorer.restore_detector_state()), seen


def test_saved_points_of_the_old_width_are_not_replayed() -> None:
    restored, seen = _restore([[0.0] * 8] * 3)
    assert restored == {} and seen == []


def test_saved_points_of_the_current_width_still_are() -> None:
    restored, seen = _restore([[0.0] * DETECTOR_POINT_WIDTH["maritime"]] * 3)
    assert restored == {"maritime": 3} and len(seen) == 3
