"""The telemetry worker must read the keys the wargamer actually publishes.

Five of the six columns in agent_predictions read names that appear in no
message the adversarial wargamer sends. Measured across all 30 rows that had
ever been written: prediction_id was the literal string "wargame_sim" every
time, correlation_id was empty every time, confidence was 0.0 every time, and
both jsonb columns were empty. Only predicted_target carried anything.

The empty correlation_id is the one that matters most: it is the join back to
the cluster that caused the prediction, so every prediction was stored orphaned
from its own cause.

Nothing reads this table yet, which is why a write-only path could stay wrong
indefinitely without a single error being raised anywhere.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "telemetry_worker_main", ROOT / "services" / "telemetry-worker" / "main.py"
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["telemetry_worker_main"] = _mod
_spec.loader.exec_module(_mod)

# A message taken verbatim off agents.predictions.output.
LIVE_MESSAGE = {
    "simulation_run_id": "sim_1788155687",
    "primary_vulnerability_isolated": "Financial Infrastructure Nodes",
    "cascade_failure_probability": 10,
    "predicted_next_target_entity_id": "Market Anomaly Detection Systems",
    "remediation_recommendation": "Deploy additional security measures and monitor market activity closely.",
    "agent": "adversarial_wargamer",
    "agent_run_id": "wargame_1788155687",
    "source_correlation_id": "340c49a4-6ba4-4280-8944-124152fe861f",
}


def test_confidence_is_no_longer_always_zero():
    assert _mod._prediction_confidence(LIVE_MESSAGE) == 0.10


def test_confidence_is_scaled_to_zero_one():
    assert _mod._prediction_confidence({"cascade_failure_probability": 100}) == 1.0
    assert _mod._prediction_confidence({"cascade_failure_probability": 0}) == 0.0
    assert _mod._prediction_confidence({"cascade_failure_probability": 250}) == 1.0


def test_an_explicit_confidence_still_wins_if_a_producer_sends_one():
    assert _mod._prediction_confidence({"simulation_confidence": 0.42}) == 0.42


def test_a_junk_value_does_not_raise():
    """This runs inside a Kafka consume loop; an exception here drops the batch."""
    assert _mod._prediction_confidence({"cascade_failure_probability": "n/a"}) == 0.0
    assert _mod._prediction_confidence({}) == 0.0


def test_the_remediation_string_becomes_the_recommendation_list():
    recs = _mod._recommendations(LIVE_MESSAGE)
    assert recs == [LIVE_MESSAGE["remediation_recommendation"]]


def test_an_explicit_list_is_preferred():
    assert _mod._recommendations({"preemptive_recommendations": ["a", "b"]}) == ["a", "b"]


def test_nothing_to_recommend_is_an_empty_list_not_a_string():
    assert _mod._recommendations({}) == []


def test_the_insert_reads_the_published_key_names():
    """The correlation join, and the id, come from the source itself."""
    source = (ROOT / "services/telemetry-worker/main.py").read_text(encoding="utf-8")
    insert = source[source.index("INSERT INTO agent_predictions"):]
    insert = insert[:insert.index("MetricsCollector")]
    executable = "\n".join(
        line for line in insert.splitlines() if not line.strip().startswith("#")
    )
    assert "source_correlation_id" in executable, (
        "correlation_id is read from a key the wargamer never sends, so every "
        "prediction is stored orphaned from its cluster"
    )
    assert "simulation_run_id" in executable
    assert "json.dumps" not in executable, (
        "a jsonb parameter is pre-serialised; the pool's codec encodes it again"
    )
