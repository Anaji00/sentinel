from shared.utils.correlation_funnel import build


def test_families_are_grouped_by_rule_family_with_tier_splits_and_shares():
    rows = [{"rule_name": "Geopolitical Cascade (Msft)", "alert_tier": 1, "n": 30, "entities": 5},
            {"rule_name": "Geopolitical Cascade (Aapl)", "alert_tier": 2, "n": 10, "entities": 7},
            {"rule_name": "US2Y Yield Drop", "alert_tier": 2, "n": 60, "entities": 40}]
    out = build(rows)
    assert out["total"] == 100 and out["tier_totals"] == {"1": 30, "2": 70}
    g, u = out["families"][1], out["families"][0]
    assert u["family"] == "US2Y Yield Drop" and u["share"] == 0.6 and u["tier2"] == 60 and u["tier1"] == 0
    assert g["family"] == "Geopolitical Cascade" and g["correlations"] == 40 and g["rules"] == 2 and g["entities_max_per_rule"] == 7


def test_an_empty_window_is_empty_not_a_division_error():
    out = build([])
    assert out["total"] == 0 and out["tier_totals"] == {} and out["families"] == [] and out["tier_names"]["1"] == "WATCH"
