import json

import pytest

from shared.utils.evidence_bundle import grades_for, model_row, regime_of, section

GRADES = {
    "correlations_5d": [{"group": "Rule A", "verdict": "moves more", "n": 50, "mean_ratio": 1.2, "t": 3.1, "q": 0.01}],
    "correlation_families_5d": [{"group": "Fam", "verdict": "no difference", "n": 400, "mean_ratio": 1.0, "t": 0.2, "q": 0.8}],
}
FAMILY = {"Rule A": "Fam", "Rule B": "Fam", "Rule C": "Other"}.get


def test_section_status_and_data_rules():
    assert section("ok", {"x": 1}) == {"status": "ok", "data": {"x": 1}, "note": None}
    assert section("missing", {"ignored": 1}, "none")["data"] is None
    assert section("error")["status"] == "error"
    with pytest.raises(ValueError):
        section("slow")


def test_regime_is_the_newest_alert_that_carries_one():
    rows = [{"metrics_summary": json.dumps({"z": 3}), "detected_at": "t3", "rule_name": "x"},
            {"metrics_summary": {"regime": {"label": "UNEXPLAINED_DISLOCATION", "action": "watch", "reasons": ["a"], "missing": [], "basis": "b"}},
             "detected_at": "t2", "rule_name": "Peers"},
            {"metrics_summary": {"regime": {"label": "OLD"}}, "detected_at": "t1", "rule_name": "Peers"}]
    r = regime_of(rows)
    assert r["label"] == "UNEXPLAINED_DISLOCATION" and r["alert_at"] == "t2" and r["reasons"] == ["a"]
    assert regime_of([]) is None and regime_of([{"metrics_summary": "not json"}]) is None


def test_grades_prefer_the_rule_then_its_family_and_skip_ungraded():
    out = grades_for(["Rule A", "Rule B", "Rule A", "Rule C"], GRADES, FAMILY)
    assert [(g["rule"], g["graded"], g["verdict"]) for g in out] == [("Rule A", "rule", "moves more"), ("Rule B", "family", "no difference")]
    assert grades_for(["Rule A"], None, FAMILY) == []


def test_model_row_needs_a_finite_percentile():
    scores = {"AAA": {"percentile": 0.8123456, "quintile": 5}, "BBB": {"percentile": float("nan")}}
    assert model_row(scores, "AAA", "2026-11-06") == {"percentile": 0.8123, "quintile": 5, "as_of": "2026-11-06"}
    assert model_row(scores, "BBB", "x") is None and model_row(scores, "ZZZ", "x") is None and model_row(None, "AAA", "x") is None
