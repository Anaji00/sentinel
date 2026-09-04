"""
tests/test_calibration_harness_query.py

The threshold calibrator had never once read an outcome.

Found by totalling a day of database logs rather than by anything failing:

    ERROR: column "payload" does not exist at character 49    x96
    STATEMENT: SELECT scenario_id, status, confidence_overall, payload
               FROM scenarios WHERE LOWER(status) IN ('confirmed','denied')

`payload` is not a column on `scenarios` and never has been. The exception is
caught, logged, and an empty list returned, so evaluate_threshold_combination
scored (0, 0, 0) and every threshold in the system stayed at its default.
Four to nine failures an hour, all day, for a component whose entire purpose is
to learn from confirmed and denied outcomes.

212 confirmed scenarios were in the table while this ran.

A second bug sat behind it and would have prevented calibration even if the
query had worked: `is_positive = status == "CONFIRMED"` against a column that
stores "confirmed". Every true positive would have been counted as a negative,
so precision would have been zero on real data -- a plausible-looking number
that happens to be meaningless.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "reasoning" / "calibration_harness.py").read_text(encoding="utf-8")


def test_the_query_no_longer_selects_a_column_that_does_not_exist():
    assert "confidence_overall, payload FROM scenarios" not in SOURCE


def test_the_anomaly_score_comes_from_the_triggering_event():
    """A scenario comes from a correlation, and that correlation names the
    event that triggered it. The score was always reachable."""
    assert "LEFT JOIN correlations c ON s.correlation_id = c.correlation_id" in SOURCE
    assert "LEFT JOIN events e ON e.event_id = c.trigger_event_id" in SOURCE


def test_a_scenario_without_a_reachable_event_is_still_counted():
    """LEFT JOIN, not INNER: losing every outcome whose correlation was pruned
    would reintroduce the same silence by a different route."""
    assert "INNER JOIN" not in SOURCE
    assert 'float(anomaly) if anomaly is not None else 0.5' in SOURCE


def test_the_status_comparison_is_case_insensitive():
    """The column stores "confirmed"; the comparison demanded "CONFIRMED"."""
    assert 'is_positive = status == "CONFIRMED"' not in SOURCE
    assert '.strip().upper() == "CONFIRMED"' in SOURCE


def test_a_null_confidence_does_not_raise():
    """float(None) is how three other components in this system died."""
    assert 'float(r.get("confidence_overall") or 50)' in SOURCE


def test_the_evaluator_still_receives_the_shape_it_expects():
    """The consumer reads item["payload"]["anomaly_score"], so the join result
    is packed back into that shape rather than changing both sides."""
    assert '"payload": {"anomaly_score":' in SOURCE
    assert 'item.get("payload", {}).get("anomaly_score", 0.5)' in SOURCE


def test_redis_remains_the_preferred_source():
    """The database path is the fallback; it should not have become primary."""
    assert SOURCE.index("outcomes_history") < SOURCE.index("FROM scenarios s")
