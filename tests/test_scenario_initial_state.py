"""P6 Phase 0 (owner-approved 21 Nov 2026): the generator's first probabilities and confidence are kept once, before the tracker rewrites them."""
import pathlib
from types import SimpleNamespace

from shared.utils.scenario_initial_state import INITIAL_STATE_VERSION, build_initial_state

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_probabilities_labels_and_confidence_are_kept_in_order():
    hyps = [SimpleNamespace(label="A", probability=55), SimpleNamespace(label="B", probability=30), {"label": "C", "probability": "15"}]
    s = build_initial_state(hyps, 62, "11111111-1111-1111-1111-111111111111")
    assert s == {"version": INITIAL_STATE_VERSION, "probabilities": [55, 30, 15], "labels": ["A", "B", "C"], "confidence_overall": 62,
                 "correlation_id": "11111111-1111-1111-1111-111111111111"}


def test_missing_or_malformed_values_stay_null_and_nothing_is_invented():
    s = build_initial_state([SimpleNamespace(label=None, probability=None), {"probability": "n/a"}], "high", None)
    assert s["probabilities"] == [None, None] and s["confidence_overall"] is None and s["correlation_id"] is None
    assert build_initial_state(None, None)["probabilities"] == []


def test_the_migration_adds_a_nullable_column_and_the_insert_writes_it_once():
    mig = (ROOT / "shared/db/migrate.py").read_text(encoding="utf-8")
    block = mig[mig.index('"0101_scenario_initial_state"'):]
    assert "ADD COLUMN IF NOT EXISTS initial_state JSONB" in block.split('"transactional"')[0] and "NOT NULL" not in block.split('"transactional"')[0]
    main = (ROOT / "services/reasoning/main.py").read_text(encoding="utf-8")
    insert = main[main.index("INSERT INTO scenarios ("):main.index("logger.info(\"✅ Intelligence Synthesis Saved")]
    assert "initial_state" in insert and "$15" in insert and "build_initial_state(scenario.hypotheses" in insert
    # the tracker's UPDATEs never touch it, so the first values survive every later pass
    tracker = (ROOT / "services/reasoning/scenario_tracker.py").read_text(encoding="utf-8")
    assert "initial_state" not in tracker
