"""
tests/test_score_diversity_monitor.py

Seven detectors were found emitting a single score, and every one of them was
invisible until somebody counted:

    crypto_transfer   39,262 events   1 score   category overwrote the measurement
    flight_dark        9,056 events   1 score   absolute threshold measuring the ocean
    bgp_anomaly          219 events   1 score   TypeError killed the novelty edge
    flight_anomaly        85 events   1 score   sanctions overwrote the kinematics
    earnings_report      183 events   1 score   baseline for a report not yet made
    earnings_surprise     10 events   1 score   EMA seeded with its own observation
    market_anomaly       ~45 at 1.00            divisor saturating at five sigma

The causes have nothing in common. A category replacing a measurement, a
threshold in the wrong units, an exception swallowed at debug, an arithmetic
cliff, a placeholder, and a self-referential average -- no single fix addresses
them and no review would have caught them as one class.

The *symptom* is identical every time, and it is one SQL query. So the system
now runs that query on itself. This does not diagnose: reading the arithmetic is
still the work. It only says which detector has stopped discriminating, which is
the part that otherwise goes unnoticed for months.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")


def _flat(rows, exempt):
    """The predicate the loop applies."""
    return [
        r for r in rows
        if int(r.get("distinct_scores") or 0) <= 1 and str(r.get("type")) not in exempt
    ]


def test_a_flat_detector_is_reported():
    rows = [{"type": "bgp_anomaly", "n": 219, "distinct_scores": 1}]
    assert _flat(rows, set())


def test_a_discriminating_detector_is_not():
    rows = [{"type": "market_anomaly", "n": 204, "distinct_scores": 128}]
    assert not _flat(rows, set())


def test_a_legitimately_constant_type_is_exempt():
    """vessel_static is a registration record. A name and a callsign are not an
    anomaly, and 0.000 is the correct answer every time."""
    rows = [{"type": "vessel_static", "n": 131, "distinct_scores": 1}]
    assert not _flat(rows, {"vessel_static"})


def test_a_missing_count_does_not_raise():
    rows = [{"type": "x", "n": 40, "distinct_scores": None}]
    assert _flat(rows, set())


# -- the loop's own guardrails -------------------------------------------------

def test_quiet_detectors_are_not_accused():
    """A detector that fired twice may honestly have two identical answers.
    Crying flat on a sample of two would train the reader to ignore this."""
    from services.enrichment.main import SCORE_DIVERSITY_MIN_EVENTS

    assert SCORE_DIVERSITY_MIN_EVENTS >= 20
    assert "HAVING COUNT(*) >= {SCORE_DIVERSITY_MIN_EVENTS}" in SOURCE


def test_the_check_runs_on_a_schedule_not_once_at_startup():
    from services.enrichment.main import SCORE_DIVERSITY_INTERVAL_SEC

    assert SCORE_DIVERSITY_INTERVAL_SEC >= 600
    assert "while True:" in SOURCE.split("_score_diversity_loop")[1][:1200]


def test_the_loop_is_actually_wired():
    """The defect this codebase keeps producing is finished code nothing calls.
    A monitor for that class must not become an instance of it."""
    assert 'name="score-diversity"' in SOURCE
    assert "_score_diversity_loop(timescale)" in SOURCE


def test_the_handle_exists_before_the_task_is_registered():
    """Registering the task above the assignment is an UnboundLocalError at
    startup, which is how the reference-data loop first failed."""
    body = SOURCE.split("async def main(")[1]
    assert body.index("timescale = await get_timescale()") < body.index("_score_diversity_loop(timescale)")


def test_a_query_failure_does_not_kill_enrichment():
    """A monitor that can take down the service it watches is worse than none."""
    block = SOURCE.split("async def _score_diversity_loop")[1].split("async def ")[0]
    assert "except Exception" in block


def test_it_reports_rather_than_diagnoses():
    """Seven causes, no two alike. Claiming to know why would be wrong six
    times out of seven."""
    block = SOURCE.split("async def _score_diversity_loop")[1].split("async def ")[0]
    assert "logger.warning" in block
    assert "read its arithmetic" in block
