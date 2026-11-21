"""Trial A3 statistics: point-in-time breadth change, quintile spread, date-clustered bootstrap (shared/utils/breadth_change.py)."""
import math
from datetime import date, timedelta

from shared.utils import breadth_change as bc


def _rows(tk, series):
    return [{"ticker": tk, "report_month": m, "first_public": p, "positions": n} for m, p, n in series]


def test_first_public_copy_is_kept_when_a_later_file_restates_a_month():
    rows = _rows("A", [(date(2024, 3, 1), date(2024, 5, 30), 100), (date(2024, 3, 1), date(2024, 8, 30), 5)])
    assert bc.build_panel(rows)["A"] == [(date(2024, 3, 1), date(2024, 5, 30), 100)]


def test_change_uses_only_months_public_by_the_asof_date():
    rows = _rows("A", [(date(2024, 3, 1), date(2024, 5, 30), 100), (date(2024, 6, 1), date(2024, 8, 30), 150)])
    entries = bc.build_panel(rows)["A"]
    assert bc.breadth_change(entries, date(2024, 7, 15)) is None             # June not yet public; March has no base
    assert bc.breadth_change(entries, date(2024, 9, 5)) == 0.5


def test_stale_missing_or_tiny_base_gives_none():
    entries = bc.build_panel(_rows("A", [(date(2024, 3, 1), date(2024, 5, 30), 100), (date(2024, 6, 1), date(2024, 8, 30), 90)]))["A"]
    assert bc.breadth_change(entries, date(2025, 3, 1)) is None              # newest month older than 135 days
    tiny = bc.build_panel(_rows("B", [(date(2024, 3, 1), date(2024, 5, 30), 4), (date(2024, 6, 1), date(2024, 8, 30), 8)]))["B"]
    assert bc.breadth_change(tiny, date(2024, 9, 5)) is None                 # base under 10 positions
    gap = bc.build_panel(_rows("C", [(date(2024, 4, 1), date(2024, 5, 30), 50), (date(2024, 6, 1), date(2024, 8, 30), 60)]))["C"]
    assert bc.breadth_change(gap, date(2024, 9, 5)) is None                  # no month exactly three earlier


def test_quintile_spread_orders_by_feature_and_needs_enough_names():
    feats = {f"T{i:03d}": float(i) for i in range(200)}
    rets = {t: (0.01 if f >= 160 else -0.01 if f < 40 else 0.0) for t, f in feats.items()}
    assert math.isclose(bc.quintile_spread(feats, rets), 0.02)
    assert bc.quintile_spread({k: v for k, v in list(feats.items())[:50]}, rets) is None


def test_ties_are_broken_by_ticker_deterministically():
    feats = {f"T{i:03d}": 0.0 for i in range(100)}
    rets = {f"T{i:03d}": float(i) for i in range(100)}
    assert bc.quintile_spread(feats, rets) == bc.quintile_spread(dict(reversed(list(feats.items()))), rets)


def test_bootstrap_passes_a_steady_effect_and_fails_noise():
    steady = [(date(2020 + i // 12, i % 12 + 1, 1), 0.01 + (0.002 if i % 2 else -0.002)) for i in range(36)]
    r = bc.clustered_bootstrap(steady, draws=500, seed=1)
    assert r["passes"] and r["mean_bps"] > 0 and r["ci95_bps"][0] > 0
    flip = [(d, s if i < 18 else -s) for i, (d, s) in enumerate(steady)]
    assert not bc.clustered_bootstrap(flip, draws=500, seed=1)["passes"]       # opposite signs in the two halves
    assert bc.clustered_bootstrap(steady[:4])["passes"] is False


def test_rebalance_dates_and_forward_excess():
    sessions = [date(2024, 1, 1) + timedelta(days=i) for i in range(100)]
    d = bc.rebalance_dates(sessions, date(2024, 1, 10))
    assert d[0] == date(2024, 1, 10) and (d[1] - d[0]).days == 21
    assert bc.rebalance_dates(sessions, date(2030, 1, 1)) == []
    stock = {date(2024, 1, 1): 100.0, date(2024, 2, 1): 110.0}
    bench = {date(2024, 1, 1): 100.0, date(2024, 2, 1): 105.0}
    assert math.isclose(bc.forward_log_excess(stock, bench, date(2024, 1, 1), date(2024, 2, 1)), math.log(1.1) - math.log(1.05))
    assert bc.forward_log_excess(stock, bench, date(2024, 1, 1), date(2024, 3, 1)) is None
