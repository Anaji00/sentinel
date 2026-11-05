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


def test_the_ranker_is_trained_on_the_whole_day_not_its_top_30() -> None:
    """LightGBM's LambdaRank defaults weigh only each group's top 30 positions;
    a quintile book needs the bottom ordered as well as the top."""
    import pytest

    if not fm.lightgbm_available():
        pytest.skip("lightgbm not installed here")
    params = fm._make("lgbm_rank").get_params()
    assert params["lambdarank_truncation_level"] >= 2000
    assert params["label_gain"] == list(range(fm.RANK_LABELS))


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
            if ("short_interest" in sql or "2.02" in sql or "company_profile" in sql or "fundamentals" in sql
                    or "issuer_ticker" in sql or "finra_short_volume" in sql or "vendor_shares" in sql
                    or "option_iv_history" in sql or "stock_news" in sql):
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


def test_the_book_reports_its_tilts_and_an_industry_neutral_spread() -> None:
    p = _panel(n=80, T=820, signal=1.0)
    p.extra["industry"] = np.array([k % 4 for k in range(len(p.tickers))])
    r = fm.walk_forward(p, "momentum")
    for k in ("net_beta", "size_tilt_log_dollar_volume", "momentum_tilt", "long_top_industry_share",
              "industry_neutral_gross_bps", "gross_bps", "size_neutral_gross_bps"):
        assert k in r.exposures, k
    assert r.exposures["momentum_tilt"] > 0               # a momentum book is long momentum
    assert 0 < r.exposures["long_top_industry_share"] <= 1


def test_reported_shares_are_scaled_by_later_splits() -> None:
    from services.agents.factor_engine import split_adjusted_shares

    obs = [(date(2024, 3, 31), 100.0), (date(2024, 6, 30), 101.0), (date(2024, 9, 30), 404.0),
           (date(2024, 12, 31), 405.0)]
    adj = dict(split_adjusted_shares(obs))
    assert adj[date(2024, 3, 31)] == 400.0 and adj[date(2024, 6, 30)] == 404.0     # a 4-for-1 after them
    assert adj[date(2024, 9, 30)] == 404.0                                          # buyback drift is not a split


def test_fundamentals_are_visible_only_after_the_filing_lag_and_feed_the_features() -> None:
    from services.agents.factor_engine import FUNDAMENTAL_LAG_DAYS, fundamental_arrays

    p = _panel(n=20, T=820)
    t = p.tickers[3]
    end = p.dates[300]
    rows = [{"ticker": t, "concept": "Assets", "period_end": end, "value": 1000.0},
            {"ticker": t, "concept": "StockholdersEquity", "period_end": end, "value": 400.0},
            {"ticker": t, "concept": "NetIncomeLoss", "period_end": end, "value": 50.0},
            {"ticker": t, "concept": "NetCashProvidedByUsedInOperatingActivities", "period_end": end, "value": 80.0},
            {"ticker": t, "concept": "EntityCommonStockSharesOutstanding", "period_end": end, "value": 10.0}]
    arrays = fundamental_arrays(p.dates, p.tickers, rows)
    p.extra.update(arrays)
    j = p.tickers.index(t)
    before = p.dates.index(end) + FUNDAMENTAL_LAG_DAYS - 1
    after = p.dates.index(end) + FUNDAMENTAL_LAG_DAYS
    assert np.isnan(arrays["assets"][before, j]) and arrays["assets"][after, j] == 1000.0
    f = fm.features(p)
    assert abs(f["roa"][after, j] - 0.05) < 1e-6
    assert abs(f["accruals"][after, j] - (-30 / 1000)) < 1e-6
    assert abs(f["bm"][after, j] - 400 / (p.close[after, j] * 10)) < 1e-6


def test_fundamentals_are_ranked_for_the_board_but_not_trained_on() -> None:
    """Context features reach the scored output; the model's design never sees them."""
    assert not set(fm.CONTEXT_FEATURES) & set(fm.FEATURES)
    assert not set(fm.CONTEXT_FEATURES) & fm.OPTIONAL_FEATURES
    p = _panel(signal=1.0)
    feats = fm.features(p)
    rng = np.random.default_rng(1)
    feats["bm"] = rng.normal(size=feats["mom_12_1"].shape).astype(feats["mom_12_1"].dtype)
    scores = fm.score_latest(p, "momentum", feats=feats)
    ranks = [v["features"].get("bm") for v in scores.values()]
    assert scores and all(r is not None and -0.5 <= r <= 0.5 for r in ranks)
    assert all("roa" not in v["features"] for v in scores.values())    # absent stays absent


def test_the_primary_horizon_is_the_one_with_evidence_not_the_best_sharpe() -> None:
    """8 Oct 2026: monthly Sharpe 0.92 at t 1.59 outranked weekly 0.83 at t 2.81."""
    per_h = {
        "monthly": {"support": None, "net_sharpe": 0.92},
        "weekly": {"support": "supported", "net_sharpe": 0.83},
        "daily": {"support": "supported", "net_sharpe": 0.50},
    }
    assert fm.rank_horizons(per_h) == ["weekly", "daily", "monthly"]
    per_h["daily"]["support"] = "validated"
    assert fm.rank_horizons(per_h)[0] == "daily"


def test_composites_rank_within_industry_and_carry_their_signs() -> None:
    """Quality counts high accruals against a name; value is industry-relative."""
    T, N = 3, 8
    good = np.tile(np.arange(N, dtype=np.float32), (T, 1))
    comp = fm._composite([good, good], [1, 1])
    assert np.all(np.diff(comp[0]) > 0)
    flipped = fm._composite([good], [-1])
    assert np.all(np.diff(flipped[0]) < 0)
    ind = np.array([0, 0, 0, 0, 1, 1, 1, 1])
    rel = fm._industry_relative(comp, ind)
    assert abs(float(np.nanmean(rel[0, :4]))) < 0.2 and abs(float(np.nanmean(rel[0, 4:]))) < 0.2
    assert set(["gpa", "cfp", "sp", "value_ind", "quality_ind"]) <= set(fm.features(_panel()))


def test_overnight_and_intraday_returns_are_split_point_in_time() -> None:
    """A stock that gains only between the close and the next open has positive
    overnight momentum and no intraday return; the past never changes."""
    p = _panel()
    T, N = p.close.shape
    up = np.full((T, N), np.nan, dtype=np.float32)
    up[:] = p.close                                      # opens equal closes: no overnight move...
    j = 3
    closes = 100.0 * np.exp(0.002 * np.arange(T))       # ...except name j, which gaps up 0.2% every night
    p.close[:, j] = closes
    up[1:, j] = closes[1:]                               # opens at the new level, flat through the day
    up[1:, :] = np.where(np.arange(N) == j, up[1:, :], p.close[:-1, :])
    p.extra["open"] = up
    f = fm.features(p)
    assert f["mom_on_12_1"][-1, j] > 0.4 and abs(f["id_21"][-1, j]) < 1e-5
    q = fm.Panel(p.dates[:-5], p.tickers, p.close[:-5], p.high[:-5], p.volume[:-5], {"open": up[:-5]})
    g = fm.features(q)
    for name in ("mom_on_12_1", "id_21"):
        assert np.allclose(f[name][:-5], g[name], equal_nan=True), name


def test_the_average_combines_every_candidate_by_rank() -> None:
    """Each member fitted on the same rows; the prediction is their mean rank."""
    p = _panel(signal=1.0)
    rep = fm.walk_forward(p, "avg")
    assert rep.dates > 0 and rep.mean_ic is not None
    est = fm._make("avg")
    names = [n for n, _ in est.members]
    assert names[-3:] == ["gbm", "ridge", "momentum"]
    scores = fm.score_latest(p, "avg")
    assert scores and all(-0.5 <= v["score"] <= 0.5 for v in scores.values())


def test_the_universe_is_the_most_liquid_names_and_shorts_must_be_borrowable(monkeypatch) -> None:
    p = _panel(n=90)
    monkeypatch.setattr(fm, "TOP_LIQUID_NAMES", 30)
    p.extra.pop("_tradable", None)
    mask = fm.tradable(p)
    assert mask[-1].sum() <= 30 + 1                       # +1: the market, never ranked anyway
    pred = np.linspace(-1, 1, 10)
    cols = np.arange(10)
    p.extra["borrowable"] = np.array([k % 2 == 0 for k in range(p.close.shape[1])])
    shortable = fm._shortable_pred(p, pred, cols)
    short = fm._buffered_book(shortable, cols, set(), -1)
    assert all(cols[k] % 2 == 0 for k in short)            # never a name the broker will not lend


def test_context_interactions_are_point_in_time_and_switch_with_the_regime() -> None:
    """Beta's sign in the score flips with the market's trend; nothing reads ahead."""
    p = _panel()
    T = len(p.dates)
    p.extra["ctx_TLT"] = (100.0 * np.exp(0.0005 * np.arange(T))).astype(np.float32)
    p.extra["ctx_HYG"] = (80.0 * np.exp(-0.0002 * np.arange(T))).astype(np.float32)
    f = fm.features(p)
    for k in ("beta_x_trend", "ratebeta_x_rates", "mom_x_vol", "beta_x_credit"):
        assert np.isfinite(f[k][-1]).sum() > 10, k
    q = fm.Panel(p.dates[:-7], p.tickers, p.close[:-7], p.high[:-7], p.volume[:-7],
                 {"ctx_TLT": p.extra["ctx_TLT"][:-7], "ctx_HYG": p.extra["ctx_HYG"][:-7]})
    g = fm.features(q)
    for k in ("beta_x_trend", "ratebeta_x_rates", "mom_x_vol", "beta_x_credit"):
        assert np.allclose(f[k][:-7], g[k], equal_nan=True), k
    z = fm._expanding_z(np.array([1.0] * 300 + [5.0]))
    assert z[-1] > 3 and not np.isfinite(z[0])


def test_the_tradable_subset_keeps_every_ranked_column_and_the_market() -> None:
    panel = _panel(signal=1.0)
    small = fm.tradable_subset(panel)
    mask = fm.tradable(panel)
    kept = {panel.tickers[i] for i in range(len(panel.tickers)) if mask[:, i].any()}
    assert set(small.tickers) >= kept and small.close.shape[1] == len(small.tickers)


def test_training_targets_strip_the_market_then_the_industry() -> None:
    days = [date(2026, 1, 1) + timedelta(days=k) for k in range(3)]
    # SPY +10%; A and B (one industry, with C, D) +20% and +10%; E unknown industry.
    close = np.array([[100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
                      [110.0, 120.0, 110.0, 110.0, 110.0, 110.0],
                      [110.0, 120.0, 110.0, 110.0, 110.0, 110.0]])
    p = fm.Panel(days, ["SPY", "A", "B", "C", "D", "E"], close, close, np.ones_like(close),
                 {"industry": np.array([-1, 0, 0, 0, 0, -1])})
    beta = np.full(close.shape, 2.0)
    feats = {"beta_126": beta}
    m = np.log(1.1)
    ex = fm.training_target(p, 1, feats, "excess")
    adj = fm.training_target(p, 1, feats, "beta_adj")
    ind = fm.training_target(p, 1, feats, "industry")
    assert abs(ex[0, 1] - (np.log(1.2) - m)) < 1e-12
    assert abs(adj[0, 1] - (np.log(1.2) - 2 * m)) < 1e-12           # beta 2 owes twice the market's move
    assert abs(ind[0, 1] - (np.log(1.2) - np.log(1.1))) < 1e-6       # less its peers' (B, C, D) average
    assert abs(ind[0, 5] - adj[0, 5]) < 1e-6                         # no industry: beta-adjusted only (float32)
    # Without a beta there is nothing to adjust by: the plain excess return.
    assert np.allclose(fm.training_target(p, 1, {}, "industry")[0], ex[0], equal_nan=True)


def test_share_issuance_is_the_years_log_change_in_shares() -> None:
    T = 300
    days = [date(2025, 1, 1) + timedelta(days=k) for k in range(T)]
    close = np.full((T, 2), 10.0)
    shares = np.column_stack([np.full(T, 100.0), np.linspace(100.0, 130.0, T)])
    p = fm.Panel(days, ["AAA", "BBB"], close, close, np.ones_like(close), {"shares_adj": shares})
    f = fm.features(p)
    assert np.isnan(f["issuance"][251, 0])                      # not before a year of shares
    assert abs(f["issuance"][260, 0]) < 1e-9                    # unchanged count
    assert abs(f["issuance"][299, 1] - np.log(shares[299, 1] / shares[47, 1])) < 1e-5
    assert "issuance" in fm.CONTEXT_FEATURES and "issuance" not in fm.FEATURES


def test_the_two_sided_ranker_and_the_two_model_average_learn_an_order() -> None:
    if not fm.lightgbm_available():
        return
    rng = np.random.default_rng(5)
    days, per = 30, 200
    X = rng.normal(size=(days * per, len(fm.FEATURES)))
    d = np.repeat(np.arange(days), per)
    signal = X[:, 0] - 0.5 * X[:, 1]
    y = np.concatenate([fm._rank(signal[d == k] + rng.normal(scale=1.0, size=per)) for k in range(days)])
    test = rng.normal(size=(per, len(fm.FEATURES)))
    truth = test[:, 0] - 0.5 * test[:, 1]
    for name in ("lgbm_rank2", "avg2"):
        est = fm._make(name)
        fm._fit(name, est, X, y, d)
        pred = fm._predict(name, est, test)
        assert pred.shape == (per,) and np.all(np.isfinite(pred))
        assert fm._spearman(pred, truth) > 0.5, name


def test_a_read_timeout_is_retried_and_a_lasting_one_still_fails() -> None:
    import asyncio

    from services.agents.factor_engine import _PatientReads

    class Db:
        calls = 0

        def __init__(self, fail_first: int) -> None:
            self.fail_first = fail_first

        async def query(self, sql: str, *params: object) -> list:
            self.calls += 1
            if self.calls <= self.fail_first:
                raise asyncio.TimeoutError()
            return [{"sql": sql}]

        async def execute(self, sql: str) -> str:
            return "written once"

    db = Db(fail_first=2)
    patient = _PatientReads(db, attempts=4, wait_sec=0)
    assert asyncio.run(patient.query("q")) == [{"sql": "q"}] and db.calls == 3
    assert asyncio.run(patient.execute("w")) == "written once"       # writes are not retried

    down = Db(fail_first=99)
    try:
        asyncio.run(_PatientReads(down, attempts=4, wait_sec=0).query("q"))
    except asyncio.TimeoutError:
        assert down.calls == 4
    else:
        raise AssertionError("a timeout that lasts must fail the run")


def test_a_session_still_being_written_is_not_ranked() -> None:
    """95 names in the newest session published 94 scores over the usual 1,000."""
    from datetime import date, timedelta

    import numpy as np

    from shared.utils import factor_model as fm

    T, N = 30, 200
    dates = [date(2026, 9, 1) + timedelta(days=i) for i in range(T)]
    close = np.full((T, N), 10.0)
    close[-1, 95:] = np.nan
    panel = fm.Panel(dates, [f"T{i}" for i in range(N)], close, close.copy(), close.copy(),
                     {"by_day": np.zeros((T, N)), "by_name": np.zeros(N), "_tradable": np.ones((T, N), dtype=bool)})
    done = fm.complete_sessions(panel)
    assert done.dates == dates[:-1] and done.close.shape == (T - 1, N)
    assert done.extra["by_day"].shape == (T - 1, N) and done.extra["by_name"].shape == (N,)
    assert "_tradable" not in done.extra                       # the cached mask had the dropped row

    close[-1, :] = 10.0
    close[-1, 180:] = np.nan                                   # a normal day's missing names
    assert fm.complete_sessions(panel) is panel
