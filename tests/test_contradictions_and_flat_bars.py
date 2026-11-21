"""Layer audit 18 Nov, findings 12, 14 and 10: contradiction flags, flat-bar report, narrative confidence refresh. All report or text only."""
from datetime import date

from shared.utils import call_contradictions as cc
from shared.utils import flat_bars as fb
from shared.utils import llm_output_checks as lo

D = date(2026, 11, 12)


def _c(i, agent, direction, ticker="AAA"):
    return {"id": i, "agent_name": agent, "ticker": ticker, "day": D, "direction": direction}


def test_sibling_and_self_contradictions_are_flagged_not_removed() -> None:
    calls = [_c(1, "stock_correlation_agent.baseline", "up"), _c(2, "stock_correlation_agent.evidence", "down"),
             _c(3, "quant", "up"), _c(4, "quant", "down"), _c(5, "other", "up", "BBB"), _c(6, "macro", "flat")]
    f = cc.flag_contradictions(calls)
    assert f[1] == ["sibling_contradiction"] and f[2] == ["sibling_contradiction"]
    assert f[3] == ["self_contradiction"] and f[4] == ["self_contradiction"]
    assert 5 not in f and 6 not in f
    s = cc.summarize(f, len(calls))
    assert s["calls"] == 6 and s["flagged"] == 4 and s["self_contradiction"] == 2 and s["sibling_contradiction"] == 2


def test_agreeing_siblings_and_other_tickers_are_not_flagged() -> None:
    assert cc.flag_contradictions([_c(1, "a.x", "up"), _c(2, "a.y", "long"), _c(3, "a.z", "down", "ZZZ")]) == {}


def test_flat_share_counts_zero_volume_and_carried_closes() -> None:
    bars = [(10.0, 100.0), (10.0, 0.0), (10.5, 50.0), (10.5, 0.0), (11.0, 0.0)]
    assert fb.flat_share(bars) == 3 / 4
    assert fb.flat_share([(1.0, 5.0)]) == 0.0
    r = fb.flat_report({"A": bars, "B": [(1.0, 1.0), (2.0, 1.0)]}, ["A", "B", "C"], threshold=0.5)
    assert r["names"] == 2 and r["no_bars"] == 1 and r["over_threshold"] == 1 and r["worst"][0][0] == "A"


def test_the_narrative_confidence_sentence_follows_the_stored_field() -> None:
    text = "Leading hypothesis: X (71%). Overall confidence 85%. Significance."
    assert lo.with_confidence(text, 62) == "Leading hypothesis: X (71%). Overall confidence 62%. Significance."
    assert lo.confidence_mismatch(lo.with_confidence(text, 62), 62) is None
    assert lo.with_confidence("no sentence here", 50) == "no sentence here"


def test_the_tracker_refreshes_the_narrative_after_an_update() -> None:
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "services" / "reasoning" / "scenario_tracker.py").read_text(encoding="utf-8")
    assert "_REFRESH_NARRATIVE_SQL, new_confidence, scenario_id" in src
