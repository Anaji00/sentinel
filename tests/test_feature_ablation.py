"""Registered feature trials C3 and B1: the rule scorer, the feature rebuild, the ablation and the paired verdicts."""
from datetime import date, datetime, timezone

import pytest

from shared.utils import factor_model as fm
from shared.utils import feature_ablation as fa

D = date(2026, 11, 10)


def at(day: int) -> datetime:
    return datetime(2026, 11, day, 15, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("headline,expected", [
    ("Morgan Stanley Raises Price Target on Apple to $250", 1),
    ("Wedbush LIFTS its price target for Tesla", 1),
    ("Citi boosts Nvidia price target to 200", 1),
    ("Barclays Lowers Price Target on Intel to $20", -1),
    ("JPMorgan cuts Boeing price target", -1),
    ("Goldman Upgrades Ford to Buy", 1),
    ("Piper Downgrades Zoom to Neutral", -1),
    ("Apple reports record quarterly revenue", None),
    ("Analyst maintains price target on Apple", None),
    ("Raises dividend by 10%", None),
    ("Company cuts workforce by 5%", None),
    ("", None),
])
def test_rule_scorer_on_hand_written_headlines(headline, expected):
    assert fa.rule_score(headline) == expected


def test_lowers_and_raises_have_opposite_signs():
    assert fa.rule_score("X Raises Price Target to $9") == 1
    assert fa.rule_score("X Lowers Price Target to $9") == -1


def test_conflicting_patterns_are_left_to_the_model():
    assert fa.rule_score("Firm Upgrades Stock but Lowers Price Target") is None
    assert fa.classes_matched("Firm Upgrades Stock but Lowers Price Target") == ["lower_pt", "upgrade"]


def test_non_matching_headlines_keep_their_stored_score_in_version_two():
    rows = [("AAPL", at(5), "Apple Raises Price Target", 0), ("AAPL", at(6), "Apple launches a phone", -1),
            ("AAPL", at(7), "Apple Lowers Price Target", 1), ("MSFT", at(7), "Microsoft signs a deal", 1),
            ("MSFT", at(8), "Microsoft Downgrades outlook note", 0)]
    out = fa.rescored(rows)
    assert [s for _t, _a, s in out] == [1, -1, -1, 1, -1]
    for (_t, _a, h, old), (_t2, _a2, new) in zip(rows, out):
        if fa.rule_score(h) is None:
            assert new == old


def test_feature_rebuild_matches_the_job_and_only_matched_headlines_move_it():
    rows = [("AAPL", at(5), "Apple Raises Price Target", 0), ("AAPL", at(6), "Apple launches a phone", -1),
            ("MSFT", at(7), "Microsoft signs a deal", 1), ("MSFT", at(8), "Microsoft expands cloud", 0)]
    v1 = fa.rebuild_feature([(t, a, s) for t, a, _h, s in rows], D)
    v2 = fa.rebuild_feature(fa.rescored(rows), D)
    assert v1 == {"AAPL": -0.5, "MSFT": 0.5}
    assert v2["MSFT"] == v1["MSFT"]            # no matched headline: identical
    assert v2["AAPL"] == 0.0                   # (+1 + -1) / 2 instead of (0 + -1) / 2


def test_agreement_counts_per_class():
    rows = [("A", at(5), "A Raises Price Target", 1), ("A", at(5), "A Raises Price Target", 0), ("A", at(5), "A Raises Price Target", -1),
            ("A", at(5), "A Lowers Price Target", -1), ("A", at(5), "A cuts price target", 0), ("A", at(5), "plain news", 1)]
    out = fa.agreement(rows)
    assert out["raise_pt"]["n"] == 3 and out["raise_pt"]["agree"] == pytest.approx(1 / 3) and out["raise_pt"]["opposite"] == pytest.approx(1 / 3)
    assert out["lower_pt"]["n"] == 2 and out["lower_pt"]["agree"] == 0.5 and out["lower_pt"]["zero"] == 0.5
    assert out["upgrade"]["n"] == 0


def test_matched_share():
    m, n, s = fa.matched_share(["X Raises Price Target", "plain", "Y Downgrades Z", "plain two"])
    assert (m, n, s) == (2, 4, 0.5)


def test_ablation_drops_exactly_the_named_column():
    cols = ["a", "llm_news_30", "b", "ev_comment_letter"]
    out = fa.ablate(cols, "llm_news_30")
    assert out == ["a", "b", "ev_comment_letter"] and cols == ["a", "llm_news_30", "b", "ev_comment_letter"]
    assert fa.ablate(cols, "ev_comment_letter") == ["a", "llm_news_30", "b"]


def test_ablating_a_column_the_model_never_had_raises():
    with pytest.raises(KeyError):
        fa.ablate(list(fm.FEATURES), "llm_news_30")


def test_neither_feature_is_in_the_live_monthly_feature_set():
    assert not fa.is_model_input("llm_news_30", fm.FEATURES)
    assert not fa.is_model_input("ev_comment_letter", fm.FEATURES)
    assert fa.is_model_input("mom_12_1", fm.FEATURES)


def test_paired_diffs_use_shared_finite_calendars():
    assert fa.paired_diffs({0: 1.0, 5: 0.5, 10: float("nan")}, {0: 0.8, 5: 0.6, 10: 1.0, 15: 1.0}) == pytest.approx({0: 0.2, 5: -0.1})


def test_c3_verdict_keeps_calls_only_when_removal_costs_enough_on_six():
    costly = {o: -0.05 for o in range(6)} | {6: 0.0, 7: 0.1}
    assert fa.c3_verdict(costly)["keep_calls"] is True
    five = {o: -0.05 for o in range(5)} | {5: -0.02, 6: 0.0, 7: 0.1}
    assert fa.c3_verdict(five)["keep_calls"] is False
    assert fa.c3_verdict({o: 0.0 for o in range(8)})["keep_calls"] is False
    assert fa.c3_verdict({0: -0.5})["keep_calls"] is False                       # fewer calendars than the bar needs


def test_b1_verdict_needs_both_sharpe_and_call_cut():
    flat = {o: -0.01 for o in range(8)}
    assert fa.b1_verdict(flat, 0.23)["adopt"] is True
    assert fa.b1_verdict(flat, 0.19)["adopt"] is False
    worse = {o: -0.05 for o in range(8)}
    assert fa.b1_verdict(worse, 0.5)["adopt"] is False
    assert fa.b1_verdict({o: -0.05 for o in range(2)} | {o: 0.0 for o in range(2, 8)}, 0.3)["adopt"] is True
    assert fa.b1_verdict(flat, float("nan"))["adopt"] is False
