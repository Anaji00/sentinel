"""The cross-sectional model finds planted signal, refuses noise, and never peeks."""

import pathlib
import sys
from datetime import date, timedelta

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import factor_model as fm  # noqa: E402


def _rows(n=90, T=820, signal=0.0, seed=3):
    rng = np.random.default_rng(seed)
    m = rng.normal(0, 0.01, T)
    drift = np.zeros((T, n))
    for t in range(1, T):            # slowly varying per-stock drift: momentum is real if signal > 0
        drift[t] = 0.995 * drift[t - 1] + rng.normal(0, 0.0004, n)
    r = m[:, None] + signal * drift + rng.normal(0, 0.015, (T, n))
    c = 50 * np.cumprod(1 + r, axis=0)
    spy = 400 * np.cumprod(1 + m)
    d0 = date(2021, 1, 4)
    rows = []
    for t in range(T):
        d = d0 + timedelta(days=t)
        rows.append(("SPY", d, float(spy[t]), float(spy[t] * 1.003), 5e7))
        for j in range(n):
            rows.append((f"S{j:03d}", d, float(c[t, j]), float(c[t, j] * 1.004), float(1e6 + 1e4 * j)))
    return rows


def _panel(n=90, T=820, signal=0.0, seed=3):
    return fm.build_panel(_rows(n, T, signal, seed))


def test_planted_momentum_is_found_and_validated() -> None:
    rep = fm.walk_forward(_panel(signal=1.0), "momentum")
    assert rep.mean_ic and rep.mean_ic > 0.02 and rep.ic_t and rep.ic_t > 2
    gbm = fm.walk_forward(_panel(signal=1.0), "gbm")
    assert gbm.mean_ic and gbm.mean_ic > 0.01


def test_noise_is_not_validated() -> None:
    for name in ("momentum", "ridge"):
        rep = fm.walk_forward(_panel(signal=0.0, seed=11), name)
        assert not rep.validated, (name, rep)


def test_features_use_only_the_past() -> None:
    p = _panel(n=45, T=400)
    f1 = fm.features(p)
    p.close[-1] *= 1.5     # change only the last day
    f2 = fm.features(p)
    for name in ("mom_12_1", "mom_6_1", "vol_63"):
        assert np.allclose(f1[name][:-1], f2[name][:-1], equal_nan=True), name


def test_the_market_is_never_ranked() -> None:
    scores = fm.score_latest(_panel(signal=1.0), "momentum")
    assert scores and "SPY" not in scores
    assert {v["quintile"] for v in scores.values()} <= {1, 2, 3, 4, 5}


def test_run_all_returns_plain_results_for_a_separate_process() -> None:
    p = _panel(signal=1.0)
    rows = [(t, d, float(p.close[i, j]), float(p.high[i, j]), float(p.volume[i, j]))
            for i, d in enumerate(p.dates) for j, t in enumerate(p.tickers)]
    out = fm.run_all(rows, ("momentum",))
    assert out["chosen"] == "momentum" and isinstance(out["models"][0], dict)
    assert out["scores"] and "SPY" not in out["scores"]


def test_the_deflated_sharpe_penalises_choosing_among_many() -> None:
    one = fm.deflated_sharpe(0.15, 150, 0.0, 3.0, [0.15])
    many = fm.deflated_sharpe(0.15, 150, 0.0, 3.0, [0.15, 0.1, -0.05, 0.12, 0.02, 0.08, -0.1, 0.05])
    assert one is not None and many is not None and many < one
    assert fm.deflated_sharpe(0.0, 150, 0.0, 3.0, [0.0]) <= 0.5


def test_the_ranker_learns_planted_signal_when_available() -> None:
    import pytest

    if not fm.lightgbm_available():
        pytest.skip("lightgbm not installed here")
    rep = fm.walk_forward(_panel(signal=1.0), "lgbm_rank")
    assert rep.mean_ic and rep.mean_ic > 0.01


def test_the_service_loads_the_same_panel_in_batches_without_row_tuples() -> None:
    """The engine fills the panel a batch of tickers at a time; it must equal
    the panel built from the full row list, minus names too short to rank."""
    import asyncio
    from services.agents import factor_engine as fe

    rows = _rows(n=12, T=300)
    rows.append(("NEWCO", rows[-1][1], 10.0, 10.5, 1e5))   # one bar: never rankable

    class DB:
        async def query(self, sql, *args):
            if "GROUP BY ticker" in sql:
                counts = {}
                for t, *_ in rows:
                    counts[t] = counts.get(t, 0) + 1
                return [{"ticker": t} for t in sorted(counts) if counts[t] >= args[1]]
            if "DISTINCT day" in sql:
                return [{"day": d} for d in sorted({r[1] for r in rows})]
            if "short_interest" in sql or "2.02" in sql or "company_profile" in sql:
                return []
            wanted = set(args[1])
            return [{"ticker": t, "day": d, "close": c, "high": h, "volume": v}
                    for t, d, c, h, v in rows if t in wanted]

    old_batch = fe.LOAD_BATCH
    fe.LOAD_BATCH = 5
    try:
        panel = asyncio.run(fe.FactorEngine(DB(), None)._panel())
    finally:
        fe.LOAD_BATCH = old_batch
    ref = fm.build_panel([r for r in rows if r[0] != "NEWCO"])
    assert "NEWCO" not in panel.tickers
    assert panel.tickers == ref.tickers and panel.dates == ref.dates
    assert np.allclose(panel.close, ref.close, equal_nan=True)
    assert np.allclose(panel.high, ref.high, equal_nan=True)


def test_short_interest_is_visible_only_after_publication_and_expires() -> None:
    days = [date(2026, 1, 1) + timedelta(days=k) for k in range(120)]
    rows = [("AAA", date(2026, 1, 15), 3.0), ("AAA", date(2026, 1, 31), 5.0),
            ("BBB", date(2026, 1, 15), 2.0), ("ZZZ", date(2026, 1, 15), 9.0)]
    m = fm.point_in_time(days, ["AAA", "BBB"], rows, lag_days=14, max_age_days=45)
    i = {d: k for k, d in enumerate(days)}
    assert np.isnan(m[i[date(2026, 1, 28)], 0])               # settled, not yet published
    assert m[i[date(2026, 1, 29)], 0] == 3.0
    assert m[i[date(2026, 2, 14)], 0] == 5.0                  # successor published
    assert m[i[date(2026, 2, 12)], 1] == 2.0
    assert np.isnan(m[i[date(2026, 3, 15)], 1])               # 45 days on, no successor
    assert np.isnan(m[i[date(2026, 4, 25)], 0])


def test_a_name_without_short_interest_is_ranked_neutral_not_dropped() -> None:
    p = _panel(n=60, T=420)
    T, N = p.close.shape
    dtc = np.full((T, N), np.nan)
    dtc[:, :30] = np.arange(30)[None, :]
    p.extra["days_to_cover"] = dtc
    feats = fm.features(p)
    target = fm.forward_excess(p)
    X, y, _r, _d = fm.design(p, feats, target, [400])
    assert len(y) >= 55                                        # the 30 uncovered names stay in
    col = X[:, fm.FEATURES.index("dtc")]
    assert (col == 0.0).sum() >= 29


def test_the_release_is_measured_from_the_close_before_it() -> None:
    from datetime import datetime, timezone
    from services.agents.factor_engine import base_row

    days = [date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7)]
    pre_market = datetime(2026, 10, 6, 11, 0, tzinfo=timezone.utc)     # 07:00 New York
    after_close = datetime(2026, 10, 6, 20, 5, tzinfo=timezone.utc)    # 16:05 New York
    assert base_row(days, pre_market) == 0
    assert base_row(days, after_close) == 1
    assert base_row(days, "2026-10-06T20:05:00+00:00") == 1            # as the DB client returns it
    assert base_row(days, datetime(2026, 10, 4, 12, tzinfo=timezone.utc)) == -1


def test_the_earnings_reaction_exists_only_after_it_happened_and_is_market_adjusted() -> None:
    days = [date(2026, 1, 1) + timedelta(days=k) for k in range(100)]
    n = len(days)
    spy = np.full(n, 100.0)
    spy[12:] = 101.0                                   # market +1% over the window
    aaa = np.full(n, 50.0)
    aaa[12:] = 55.0                                    # stock +10%
    p = fm.Panel(days, ["AAA", "SPY"], np.column_stack([aaa, spy]), np.column_stack([aaa, spy]),
                 np.ones((n, 2)))
    out = fm.announcement_returns(p, [("AAA", 10)], window=2, hold=60)
    assert np.isnan(out[11, 0])                        # not before the second close
    assert abs(out[12, 0] - (np.log(1.1) - np.log(1.01))) < 1e-12
    assert np.isfinite(out[71, 0]) and np.isnan(out[72, 0])


def test_peer_means_leave_the_name_out_and_need_enough_peers() -> None:
    x = np.array([[1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0]])
    groups = np.array([7, 7, 7, 7, 7, 9, 9])
    out = fm._peer_mean(x, groups)
    assert out[0, 0] == (2 + 3 + 4 + 5) / 4
    assert out[0, 4] == (1 + 2 + 3 + 4) / 4
    assert np.isnan(out[0, 5]) and np.isnan(out[0, 6])     # one peer is not an industry
    x[0, 1] = np.nan
    assert fm._peer_mean(x, groups)[0, 0] == (3 + 4 + 5) / 3


def test_untradable_names_are_never_ranked() -> None:
    p = _panel(n=60, T=420)
    p.close[:, 5] = 1.5                      # a penny stock
    p.volume[:, 6] = 10.0                    # a name nobody trades
    ok = fm._eligible(p, 400)
    assert not ok[p.tickers.index("S005")] and not ok[p.tickers.index("S006")]
    assert ok[p.tickers.index("S007")]


def test_horizons_are_walked_and_deflated_against_every_trial() -> None:
    p = _panel(n=80, T=820, signal=1.0)
    out = fm.run_panel(p, ["momentum", "ridge"], horizons=("weekly", "monthly"))
    assert set(out["horizons"]) == {"weekly", "monthly"}
    assert out["trials"] == 4
    assert out["primary_horizon"] in ("weekly", "monthly")
    for h, v in out["horizons"].items():
        assert v["horizon_days"] == fm.HORIZON_SPECS[h].horizon
        assert all(m["deflated_sharpe"] is not None for m in v["models"] if m["sr_period"] is not None)
    assert out["scores"] == out["horizons"][out["primary_horizon"]]["scores"]


def test_a_float32_panel_stays_float32_and_ranks_like_float64() -> None:
    p64 = _panel(n=60, T=420, signal=1.0)
    p32 = fm.Panel(p64.dates, p64.tickers, p64.close.astype(np.float32), p64.high.astype(np.float32),
                   p64.volume.astype(np.float32))
    f32, f64 = fm.features(p32), fm.features(p64)
    assert all(v.dtype == np.float32 for k, v in f32.items() if k not in ("dtc", "ear", "ind_mom", "ind_rev_21"))
    X32, *_ = fm.design(p32, f32, fm.forward_excess(p32), [400])
    X64, *_ = fm.design(p64, f64, fm.forward_excess(p64), [400])
    assert X32.shape == X64.shape
    agree = np.mean(np.abs(X32 - X64) < 0.02)
    assert agree > 0.99


def test_the_book_holds_names_inside_the_buffer() -> None:
    pred = np.arange(10, dtype=float)                 # positions 8, 9 are the top quintile
    cols = np.arange(100, 110)
    fresh = fm._buffered_book(pred, cols, set(), 1)
    assert sorted(fresh) == [8, 9]
    held = fm._buffered_book(pred, cols, {106, 109}, 1)   # 106 is at pct 0.67: inside the top 40%
    assert sorted(held) == [6, 9]
    dropped = fm._buffered_book(pred, cols, {102}, 1)     # 102 has fallen out of the buffer
    assert sorted(dropped) == [8, 9]
    assert sorted(fm._buffered_book(pred, cols, set(), -1)) == [0, 1]


def test_deflation_counts_every_trial_but_compares_like_with_like() -> None:
    same = [0.35, 0.10, 0.15, -0.05]
    few = fm.deflated_sharpe(0.35, 40, 0.0, 3.0, same)
    many = fm.deflated_sharpe(0.35, 40, 0.0, 3.0, same, n_trials=12)
    assert many < few                                   # more trials, a higher bar
    polluted = fm.deflated_sharpe(0.35, 40, 0.0, 3.0, same + [-1.4, -1.2, -1.0])
    assert polluted < many                              # pooling incomparable trials inflates it


def test_names_that_are_not_stocks_are_never_ranked() -> None:
    p = _panel(n=60, T=420)
    mask = np.zeros(len(p.tickers), dtype=bool)
    mask[p.tickers.index("S010")] = True
    p.extra["not_a_stock"] = mask
    ok = fm._eligible(p, 400)
    assert not ok[p.tickers.index("S010")] and ok[p.tickers.index("S011")]
