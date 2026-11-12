"""The STS anomaly score varies with its evidence, and the scenario prompt carries no numeric example to copy."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.sts_score import MAX_SCORE, sts_score  # noqa: E402


def test_more_evidence_scores_higher_and_the_ordinary_case_stays_near_the_old_constant() -> None:
    brush = sts_score(0.5, 0.45, [2.5, 2.8], False, False)
    ordinary = sts_score(4.6, 0.2, [1.0, 0.8], False, True)
    long_sanctioned = sts_score(14.0, 0.05, [0.1, 0.2], True, True)
    assert brush < ordinary < long_sanctioned <= MAX_SCORE
    assert abs(ordinary - 0.78) < 0.05            # the value every event used to get


def test_each_factor_moves_the_score_the_right_way_and_inputs_are_bounded() -> None:
    base = dict(dwell_hours=4.0, separation_nm=0.2, speeds_knots=[1.0, 1.0], sanctioned=False, in_named_region=False)
    s0 = sts_score(**base)
    assert sts_score(**{**base, "dwell_hours": 10.0}) > s0 and sts_score(**{**base, "separation_nm": 0.05}) > s0
    assert sts_score(**{**base, "speeds_knots": [0.1, 0.1]}) > s0 and sts_score(**{**base, "sanctioned": True}) > s0
    assert sts_score(**{**base, "in_named_region": True}) > s0 and sts_score(-5, -1, [], True, True) <= MAX_SCORE
    assert len({sts_score(d, 0.2, [1.0], False, False) for d in (0.5, 1, 2, 4, 8, 16)}) == 6


def test_the_event_builder_uses_the_score_not_a_constant() -> None:
    src = (ROOT / "services/enrichment/enrichers/maritime.py").read_text(encoding="utf-8")
    assert "anomaly_score=0.78" not in src and "anomaly_score=sts_score(" in src


def test_the_scenario_prompt_has_no_numeric_example_to_anchor_on() -> None:
    src = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    assert '"probability": 45' not in src and '"confidence_overall": 62' not in src
    assert "from the weight of the evidence" in src and "carries no information" in src
