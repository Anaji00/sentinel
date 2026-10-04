"""
tests/test_decay_counts_observed_time.py

An outage read as a forecast failing, and decay was charged twice.

Decay measured wall-clock time. On 1 October the platform came back after nine
days down, and its first sweeps denied 64 scenarios "with no predicted signal in
330-390h against a stated 72h horizon" -- each one published as a rule_failure
verdict, and those verdicts retired three rules. Only 239 of the preceding 720
hours had the platform collecting at all.

And each sweep subtracted the *total* overdue decay from the stored confidence,
so a scenario that survived one decay lost the same amount again thirty minutes
later: scenario 51b4778a held 63 for thirty sweeps, then went 63 -> 14 in one.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reasoning.scenario_tracker import (
    EXPIRY_DECAY_PER_HORIZON,
    _applied_decay,
    _expiry_decay,
)

NOW = datetime(2026, 10, 1, 19, 0, tzinfo=timezone.utc)
HYPS = [{"time_horizon": "72h"}]


def _hours(start, count):
    base = start.replace(minute=0, second=0, microsecond=0)
    return [base + timedelta(hours=i) for i in range(count)]


def _scenario(days_ago):
    return {"created_at": NOW - timedelta(days=days_ago)}


def test_wall_clock_behaviour_is_unchanged_without_observed_hours():
    points, note = _expiry_decay(_scenario(14), HYPS, NOW)
    assert points == round((14 * 24 / 72 - 1) * EXPIRY_DECAY_PER_HORIZON)
    assert "unfulfilled forecast" in note


def test_an_outage_does_not_count_against_the_forecast():
    """Created 13 days ago; collecting for its first 80 hours, then down,
    then back for the last three -- 83 observed hours, 1.15 horizons."""
    created = NOW - timedelta(days=13)
    observed = _hours(created, 80) + _hours(NOW - timedelta(hours=3), 3)
    points, note = _expiry_decay({"created_at": created}, HYPS, NOW, observed_hours=observed)
    wall, _ = _expiry_decay({"created_at": created}, HYPS, NOW)
    assert points == 2
    assert wall >= 40
    assert "83h of collection" in note


def test_hours_before_the_scenario_existed_do_not_count():
    created = NOW - timedelta(hours=10)
    observed = _hours(NOW - timedelta(days=20), 480)
    assert _expiry_decay({"created_at": created}, HYPS, NOW, observed_hours=observed) == (0, "")


def test_no_observed_hours_means_no_decay():
    assert _expiry_decay(_scenario(14), HYPS, NOW, observed_hours=[]) == (0, "")


def test_decay_already_taken_is_not_taken_again():
    created = NOW - timedelta(days=10)
    observed = _hours(created, 240)
    total, _ = _expiry_decay({"created_at": created}, HYPS, NOW, observed_hours=observed)
    again, note = _expiry_decay(
        {"created_at": created}, HYPS, NOW, observed_hours=observed, already_applied=total,
    )
    assert total > 0
    assert (again, note) == (0, "")


def test_only_the_increase_is_charged():
    created = NOW - timedelta(days=10)
    observed = _hours(created, 240)
    total, _ = _expiry_decay({"created_at": created}, HYPS, NOW, observed_hours=observed)
    points, note = _expiry_decay(
        {"created_at": created}, HYPS, NOW, observed_hours=observed, already_applied=total - 5,
    )
    assert points == 5
    assert note.endswith("-5 for an unfulfilled forecast")


def test_applied_decay_is_read_from_the_history_notes():
    history = [
        {"confidence": 63, "notes": "watch signal fired"},
        {"confidence": 51, "notes": "No predicted signal in 100h against a stated 72h horizon (1.4 horizons); -12 for an unfulfilled forecast"},
        json.dumps({"confidence": 37, "notes": "No predicted signal in 120h ...; -14 for an unfulfilled forecast"}),
        "not json",
        None,
    ]
    assert _applied_decay(history) == 26
    assert _applied_decay(None) == 0


def test_an_over_decayed_scenario_takes_no_more():
    """The compounding already took more than the total; nothing further."""
    created = NOW - timedelta(days=10)
    observed = _hours(created, 240)
    total, _ = _expiry_decay({"created_at": created}, HYPS, NOW, observed_hours=observed)
    assert _expiry_decay(
        {"created_at": created}, HYPS, NOW, observed_hours=observed, already_applied=total * 3,
    ) == (0, "")


def test_the_tracker_reads_history_and_skips_decay_when_hours_are_unknown():
    src = (ROOT / "services" / "reasoning" / "scenario_tracker.py").read_text(encoding="utf-8")
    assert "created_at, updated_at, confidence_history" in src
    assert "if not watch_hits_by_index and observed_hours is not None:" in src
    assert 'already_applied=_applied_decay(scenario.get("confidence_history"))' in src


def test_returned_decay_is_not_counted_as_taken():
    """The repair of the outage denials hands the decay back in a note."""
    history = [
        {"notes": "No predicted signal in 360h against a stated 72h horizon (5.0 horizons); -48 for an unfulfilled forecast"},
        {"notes": "Restored: denied on wall-clock time across an outage; +48 returned to an unobserved horizon"},
    ]
    assert _applied_decay(history) == 0
