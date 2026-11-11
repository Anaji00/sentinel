"""Screens say no more than the record supports (backend session's list, 6 Nov 2026)."""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_model_ranking_shows_the_gate_the_scoring_change_and_crowns_no_one() -> None:
    page = " ".join(_read("components/ModelRanking.tsx").split())
    assert "needs 0.95" in page and "since 5 Nov 2026" in page and "not comparable" in page
    assert "inside the noise" in page
    assert "net_sharpe_on_log_returns" not in page, "the old log-return figure is not headlined"
    assert "best model" not in page.lower()


def test_paper_trading_does_not_present_equity_versus_spy_as_validation() -> None:
    panel = _read("components/AutoTradePanel.tsx")
    assert "Sharpe near 0.4" in panel and "24 months" in panel and "is not evidence that it works" in panel
    assert "This beat holding" not in panel


def test_an_unconfirmed_order_is_never_called_rejected_or_failed() -> None:
    words = _read("lib/trackWords.ts")
    assert "Not confirmed, being checked" in words and "Already sent today" in words
    panel = _read("components/AutoTradePanel.tsx")
    assert "orderStatusWords" in panel and "Orders not confirmed yet" in panel


def test_track_record_leads_with_chance_and_withholds_rates_on_few_calls() -> None:
    words = _read("lib/trackWords.ts")
    assert "MIN_GRADED = 20" in words and "too few to grade" in words
    page = _read("components/TrackRecord.tsx")
    assert page.count("tooFewToGrade(") >= 4 and page.count("HIT_RATE_CAUTION") >= 2
    assert "no forecasting agent had beaten chance" in page


def test_flow_wording_still_says_late_and_incomplete_and_keeps_unknown_beside_known() -> None:
    assert "15 minutes late" in _read("components/DarkPoolFlowPanel.tsx")
    assert "Side not known" in _read("lib/whatChanged.ts")


def test_idea_rows_use_the_backend_verdict_and_never_invent_worse_than_chance() -> None:
    words = _read("lib/trackWords.ts")
    assert "skill_verdict" in words and "n_independent" in words and "chance_hit" in words and "hit_called" in words
    page = _read("components/TrackRecord.tsx")
    assert "skillLine(s, label)" in page, "trade-idea rows are worded from the verdict, not from the row count"
    assert "worse than chance" in words and words.count("worse than chance") >= 2
