import pytest

from shared.utils.screener import build_row, select, volume_vs_20d


def row(t, chg=None, vol=None, pct=None, ev=None, cap=None):
    return {"ticker": t, "change_pct": chg, "volume_vs_20d": vol, "model_percentile": pct,
            "next_event": {"kind": "earnings", "date": ev} if ev else None, "market_cap": cap}


def test_volume_ratio_is_latest_over_prior_20_and_needs_history():
    vs = [100.0] * 20 + [250.0]
    assert volume_vs_20d(vs) == 2.5
    assert volume_vs_20d([100.0] * 10) is None and volume_vs_20d([0.0] * 20 + [5.0]) is None and volume_vs_20d([]) is None


def test_missing_values_sort_last_in_both_directions():
    rows = [row("A", 1.0), row("B"), row("C", 3.0), row("D", -2.0)]
    assert [r["ticker"] for r in select(rows, "change_pct", "desc", 10, None)] == ["C", "A", "D", "B"]
    assert [r["ticker"] for r in select(rows, "change_pct", "asc", 10, None)] == ["D", "A", "C", "B"]


def test_min_cap_excludes_unknown_caps():
    rows = [row("A", 1.0, cap=2e9), row("B", 2.0), row("C", 3.0, cap=5e8)]
    assert [r["ticker"] for r in select(rows, "change_pct", "desc", 10, 1e9)] == ["A"]


def test_next_event_soonest_first_when_ascending():
    rows = [row("A", ev="2026-12-01"), row("B", ev="2026-11-15"), row("C")]
    assert [r["ticker"] for r in select(rows, "next_event", "asc", 10, None)] == ["B", "A", "C"]


def test_limit_and_bad_sort():
    assert len(select([row(str(i), float(i)) for i in range(9)], "change_pct", "desc", 3, None)) == 3
    with pytest.raises(ValueError):
        select([], "nope", "desc", 3, None)


def test_build_row_nulls_when_unknown():
    r = build_row("ZZ", {}, [], None, "2026-11-12", None, None)
    assert r["price"] is None and r["model_percentile"] is None and r["model_as_of"] is None and r["market_cap"] is None


def test_a_sector_filter_and_the_new_row_keys():
    rows = [dict(row("A", 1.0), sector="Energy"), dict(row("B", 2.0), sector="Financials"), dict(row("C", 3.0))]
    assert [r["ticker"] for r in select(rows, "change_pct", "desc", 10, None, "Energy")] == ["A"]
    assert [r["ticker"] for r in select(rows, "change_pct", "desc", 10, None, None)] == ["C", "B", "A"]
    r = build_row("ZZ", {}, [], None, None, None, None, sector="Energy", industry="Crude Petroleum")
    assert r["sector"] == "Energy" and r["industry"] == "Crude Petroleum" and build_row("ZZ", {}, [], None, None, None, None)["sector"] is None
