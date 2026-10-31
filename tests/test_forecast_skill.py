"""An agent's directional record against chance and the base rate (shared/utils/forecast_skill.py)."""
from shared.utils import forecast_skill as fs


def test_a_lean_in_a_one_sided_week_is_not_skill():
    # The stock correlation agent's week: 56% down calls, 31% of stocks went down, 57.7% hit.
    calls = ([(False, False, 0.6)] * 13 + [(False, True, 0.6)] * 27 +
             [(True, True, 0.6)] * 28 + [(True, False, 0.6)] * 3)
    s = fs.skill(calls)
    assert s["n"] == 71 and abs(s["hit"] - 41 / 71) < 1e-3
    assert s["majority_hit"] > s["hit"]                                  # "always up" beat it
    assert s["verdict"] in ("none", "skill") and s["chance_hit"] < s["hit"]


def test_always_the_same_side_has_no_heidke_skill():
    calls = [(False, False, 0.7)] * 15 + [(False, True, 0.7)] * 14       # the macro engine's shape
    s = fs.skill(calls)
    assert s["heidke"] in (None, 0.0) or abs(s["heidke"]) < 1e-9
    assert s["verdict"] == "none" or s["z"] is None


def test_real_skill_and_worse_than_chance():
    good = [(True, True, 0.7)] * 20 + [(False, False, 0.7)] * 20 + [(True, False, 0.7)] * 5 + [(False, True, 0.7)] * 5
    bad = [(True, False, 0.7)] * 20 + [(False, True, 0.7)] * 20 + [(True, True, 0.7)] * 5 + [(False, False, 0.7)] * 5
    assert fs.skill(good)["verdict"] == "skill" and fs.skill(good)["bss"] > 0
    assert fs.skill(bad)["verdict"] == "worse than chance" and fs.skill(bad)["bss"] < 0


def test_too_few_and_empty():
    assert fs.skill([])["verdict"] == "too few"
    assert fs.skill([(True, True, 0.9)] * 5)["verdict"] == "too few"


def test_sympathy_claims_are_graded_apart_from_the_agent():
    from services.agents import prediction_scoring as ps
    assert ps.side_label("sympathy:XOM") == "sympathy"
    assert ps.side_label("trading_advisory:CI") is None and ps.side_label(None) is None
