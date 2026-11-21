"""Book constructions of the 13 Nov trial (shared/utils/book_strategies.py): each variant's selection and weights on small hand-built inputs,
S0 against the live `b1_book` / `target_weights`, and the simulator on data where the answer is known."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import auto_trade as at  # noqa: E402
from shared.utils import book_strategies as bs  # noqa: E402


def _cs(n=100, seed=0, borrow=True, **over):
    rng = np.random.default_rng(seed)
    d = dict(cols=np.arange(n), score=rng.normal(size=n), borrowable=np.full(n, borrow), industry=np.full(n, -1),
             cap=np.full(n, 5e9), vol=np.full(n, 0.02), dtc=np.full(n, np.nan), adv=np.full(n, 5e7), beta=np.full(n, 1.0))
    d.update(over)
    return bs.CrossSection(**d)


def test_s0_reproduces_b1_book_and_target_weights_exactly() -> None:
    for seed in range(5):
        n = 300
        cs = _cs(n, seed, borrowable=np.random.default_rng(seed + 9).random(n) < 0.7)
        names = [f"T{i:03d}" for i in range(n)]
        pct = bs.percentiles(cs.score)
        longs, shorts = at.b1_book(dict(zip(names, pct)), {names[i] for i in np.flatnonzero(cs.borrowable)})
        lo, sh = bs.select_book(bs.S0, cs)
        assert [names[i] for i in lo] == longs and [names[i] for i in sh] == shorts
        want = at.target_weights(longs, shorts, bs.LEG)
        got = bs.strategy_book(bs.S0, cs)
        assert {names[i]: got[i] for i in np.flatnonzero(got)} == want or all(
            abs(got[i] - want.get(names[i], 0.0)) < 1e-12 for i in range(n))
        assert abs(got[got > 0].sum() - 0.5) < 1e-12 and abs(got[got < 0].sum() + 0.5) < 1e-12


def test_b1_excludes_the_top_percent_and_unborrowable_shorts() -> None:
    cs = _cs(200, 1)
    pct = bs.percentiles(cs.score)
    lo, _ = bs.select_book(bs.S0, cs)
    assert pct[lo].max() < 0.99 and pct[lo].min() >= 0.90
    cs2 = _cs(200, 1, borrowable=np.zeros(200, dtype=bool))
    assert len(bs.select_book(bs.S0, cs2)[1]) == 0


def test_s1_industry_neutral_takes_equal_counts_inside_each_group_and_pools_small_groups() -> None:
    n = 60
    industry = np.repeat([10, 20, 30], 20)                         # three groups of 20
    score = np.tile(np.arange(20.0), 3)
    cs = _cs(n, 2, score=score, industry=industry)
    lo, sh = bs.select_book(bs.Strategy("x", select="industry"), cs)
    for g in (10, 20, 30):
        in_g = lambda idx: int((industry[idx] == g).sum())          # noqa: E731
        assert in_g(lo) == in_g(sh) == 2                             # p .925 and .975 are in the long band; .025 and .075 in the short
    labels = bs.industry_labels(np.array([1, 1, 1, 2, 2, 2, 2, 2, 2, -1]), min_group=5)
    assert list(labels) == [-1, -1, -1, 2, 2, 2, 2, 2, 2, -1]       # group 1 (3 names) and the unknown are pooled


def test_group_neutral_equalises_to_the_smaller_leg_when_shorts_are_scarce() -> None:
    n = 40
    score = np.arange(float(n))
    borrow = np.ones(n, dtype=bool)
    borrow[:2] = False                                               # the two worst names cannot be borrowed
    lo, sh = bs.group_neutral_select(score, np.zeros(n, dtype=int), borrow)
    assert len(lo) == len(sh) and len(lo) >= 1
    assert score[lo].min() > score[sh].max()


def test_s2_size_neutral_uses_cap_terciles_and_drops_small_caps() -> None:
    cap = np.concatenate([np.full(30, 2e9), np.full(30, 20e9), np.full(30, 200e9), np.full(10, 5e8), [np.nan]])
    labels, ok = bs.cap_terciles(cap)
    assert ok.sum() == 90 and (labels[:30] == 0).all() and (labels[30:60] == 1).all() and (labels[60:90] == 2).all()
    assert (labels[90:] == -1).all()
    n = len(cap)
    cs = _cs(n, 3, cap=cap)
    lo, sh = bs.select_book(bs.Strategy("x", select="size"), cs)
    assert (ok[lo]).all() and (ok[sh]).all() and len(lo) == len(sh)


def test_s3_breadth_variants() -> None:
    cs = _cs(500, 4)
    pct = bs.percentiles(cs.score)
    b1_l, b1_s = bs.select_book(bs.S0, cs)
    dl, ds = bs.select_book(bs.Strategy("x", select="decile"), cs)
    ql, qs = bs.select_book(bs.Strategy("x", select="quintile"), cs)
    xl, xs = bs.select_book(bs.Strategy("x", select="double"), cs)
    assert pct[dl].max() == 1.0 and len(dl) > len(b1_l) and len(ds) == len(b1_s)
    assert len(ql) > 1.9 * len(b1_l) and len(qs) > 1.9 * len(b1_s)
    assert len(xl) == 2 * len(b1_l) and len(xs) == 2 * len(b1_s)
    assert pct[xl].max() < 0.99                                      # still leaves out the top 1%


def test_s4_inverse_volatility_favours_the_quiet_name_and_limits_it() -> None:
    w = bs.inverse_vol_weights(np.array([0.01, 0.02, 0.04, 0.04]))
    assert abs(w.sum() - 1) < 1e-12 and w[0] == w.max() and w[0] > w[1] > w[2]
    w2 = bs.inverse_vol_weights(np.array([0.0001] + [0.05] * 9))
    assert abs(w2.sum() - 1) < 1e-12 and w2[0] == w2.max() and abs(w2[0] - 0.3) < 1e-9     # unclipped it would hold about 99.98%; the limit is 3 / 10
    w3 = bs.inverse_vol_weights(np.array([0.001, 0.001, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05]))
    assert abs(w3.sum() - 1) < 1e-12 and w3.max() <= 3.0 / 8 + 1e-9
    assert bs.inverse_vol_weights(np.array([np.nan, np.nan])).tolist() == [0.5, 0.5]


def test_s5_conviction_weights_are_bounded_and_rise_with_extremity() -> None:
    ext = np.array([0.90, 0.92, 0.94, 0.96, 0.98])
    w = bs.conviction_weights(ext)
    assert abs(w.sum() - 1) < 1e-12 and (np.diff(w) > 0).all()
    assert w.max() / w.min() <= 3.0 + 1e-9                           # clip(.5, 1.5) bounds the ratio
    assert np.allclose(bs.conviction_weights(np.full(4, 0.9)), 0.25)


def test_s6_band_leaves_small_changes_and_trades_entries_exits_and_flips() -> None:
    cur = np.array([0.010, 0.010, 0.010, 0.0, 0.010, -0.01])
    tgt = np.array([0.011, 0.020, 0.0, 0.010, 0.010, 0.01])
    out = bs.apply_no_trade_band(cur, tgt, 0.25)
    assert out[0] == cur[0]                                          # 10% change: skipped
    assert out[1] == tgt[1]                                          # 100% change: trades
    assert out[2] == 0.0 and out[3] == 0.010                         # exit and entry
    assert out[5] == 0.01                                            # side change
    assert np.array_equal(bs.apply_no_trade_band(cur, tgt, 0.0), tgt)


def test_s6_and_s7_rebalance_schedules() -> None:
    assert sorted(bs.rebalance_rows(bs.S0, 100, 200)) == [100, 121, 142, 163, 184]
    wk = bs.rebalance_rows(bs.Strategy("x", cadence=5), 100, 130)
    assert sorted(wk) == [100, 105, 110, 115, 120, 125, 130]
    tr = bs.rebalance_rows(bs.Strategy("x", tranches=4), 100, 150)
    assert tr[100] == [0] and tr[105] == [1] and tr[110] == [2] and tr[115] == [3] and tr[121] == [0]


def test_s9_blend_is_the_mean_percentile() -> None:
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([4.0, 3.0, 2.0, 1.0])
    assert np.allclose(bs.blend_scores(a, b), 0.5)                   # opposite orders cancel
    assert np.allclose(bs.blend_scores(a, a), bs.percentiles(a))


def test_s10_drops_crowded_shorts_and_keeps_unknown_ones() -> None:
    idx = np.array([0, 1, 2, 3])
    dtc = np.array([3.0, 9.0, np.nan, 8.0])
    assert bs.dtc_filter(idx, dtc, 8.0).tolist() == [0, 2, 3]        # 9 dropped; unknown and exactly 8 kept
    cs = _cs(300, 5, dtc=np.where(np.arange(300) < 150, 12.0, 2.0))
    _, sh0 = bs.select_book(bs.S0, cs)
    _, sh10 = bs.select_book(bs.Strategy("x", dtc_max=8.0), cs)
    assert set(sh10) == {i for i in sh0 if i >= 150}


def test_s11_liquidity_limit_binds_only_when_the_account_is_large() -> None:
    cs = _cs(300, 6, adv=np.full(300, 5e6))
    base = bs.strategy_book(bs.S0, cs)
    small = bs.strategy_book(bs.Strategy("x", adv_share=0.02, account=1e5), cs)
    assert np.allclose(small, base)                                  # $100k: never binds
    big = bs.strategy_book(bs.Strategy("x", adv_share=0.02, account=2.5e8), cs)
    assert np.abs(big).max() <= 0.02 * 5e6 / 2.5e8 + 1e-12 and np.abs(big).sum() < np.abs(base).sum()
    noadv = bs.strategy_book(bs.Strategy("x", adv_share=0.02), _cs(300, 6, adv=np.full(300, np.nan)))
    assert not noadv.any()                                           # no volume figure: no position


def _market(T=260, N=40, seed=0):
    rng = np.random.default_rng(seed)
    ret = rng.normal(0.0003, 0.01, (T, N))
    spy = ret[:, 0] * 0.5 + rng.normal(0, 0.004, T)
    return ret, spy


def test_simulation_s0_matches_a_hand_computed_period_and_charges_costs_and_the_short_fee() -> None:
    ret, spy = _market()
    n = ret.shape[1]
    score = np.arange(float(n))
    cs = bs.CrossSection(np.arange(n), score, np.ones(n, dtype=bool), np.full(n, -1), np.full(n, 5e9), np.full(n, .01), np.full(n, np.nan),
                         np.full(n, 5e7), np.full(n, 1.0))
    lo, sh = bs.select_book(bs.S0, cs)
    start, nb = 50, 3
    res = bs.simulate(bs.S0, lambda t: cs, ret, spy, start, nb, cost_bps=0.0, fee_bps=0.0)
    # no costs, no fee, static scores: the book is rebuilt to equal weights every 21 sessions; first block starts at start + 22
    w = bs.strategy_book(bs.S0, cs)
    nav, pos = 1.0, w.copy()
    for t in range(start + 1, start + 22):                           # drift to the first evaluation block
        nav += float((pos * ret[t]).sum()); pos = pos * (1 + ret[t])
    pos = w * nav                                                    # rebalance on start + 21
    nav0 = nav
    for t in range(start + 22, start + 22 + 21):
        pnl = float((pos * ret[t]).sum()); pos = pos * (1 + ret[t]); nav += pnl
    assert abs(res.block_returns[0] - (nav / nav0 - 1)) < 1e-9
    costly = bs.simulate(bs.S0, lambda t: cs, ret, spy, start, nb)
    assert costly.block_returns.mean() < res.block_returns.mean()
    assert (costly.block_turnover > 0).all() and (costly.block_cost >= 0).all()
    assert len(lo) and len(sh)


def test_simulation_cadence_and_hedge_run_and_the_hedge_removes_market_exposure() -> None:
    T, N = 400, 60
    rng = np.random.default_rng(11)
    spy = rng.normal(0, 0.01, T)
    beta = np.where(np.arange(N) < 30, 1.6, 0.2)                    # the high scores are the high-beta names
    ret = beta * spy[:, None] + rng.normal(0, 0.002, (T, N))
    score = np.arange(float(N))
    cs = bs.CrossSection(np.arange(N), score, np.ones(N, dtype=bool), np.full(N, -1), np.full(N, 5e9), np.full(N, .01), np.full(N, np.nan),
                         np.full(N, 5e7), np.where(np.arange(N) < 30, 1.6, 0.2))
    plain = bs.simulate(bs.S0, lambda t: cs, ret, spy, 60, 8, cost_bps=0.0, fee_bps=0.0)
    hedged = bs.simulate(bs.Strategy("h", hedge=True), lambda t: cs, ret, spy, 60, 8, cost_bps=0.0, fee_bps=0.0)
    mkt = np.array([np.prod(1 + spy[60 + 22 + 21 * b: 60 + 43 + 21 * b]) - 1 for b in range(8)])
    assert abs(np.corrcoef(plain.block_returns, mkt)[0, 1]) > 0.8     # net beta about -0.7: the book is the market's mirror
    assert abs(np.corrcoef(hedged.block_returns, mkt)[0, 1]) < 0.5    # hedged to about zero
    for st in (bs.Strategy("w", cadence=5), bs.Strategy("b", cadence=5, band=0.25), bs.Strategy("t", tranches=4)):
        r = bs.simulate(st, lambda t: cs, ret, spy, 60, 4)
        assert len(r.block_returns) == 4 and r.max_drawdown <= 0 and np.isfinite(r.block_returns).all()


def test_a_missing_cross_section_holds_the_book() -> None:
    ret, spy = _market()
    n = ret.shape[1]
    cs = _cs(n, 0, cols=np.arange(n))
    res = bs.simulate(bs.S0, lambda t: cs if t == 50 else None, ret, spy, 50, 2, cost_bps=0.0)
    assert len(res.block_returns) == 2 and res.block_turnover.sum() == 0.0      # only the first build traded, before the window


def test_row_stats_volatility_beta_and_liquidity() -> None:
    T, N = 200, 4
    rng = np.random.default_rng(1)
    spy = rng.normal(0, 0.01, T)
    ret = np.column_stack([2.0 * spy, 0.5 * spy, rng.normal(0, 0.03, T), np.full(T, np.nan)])
    dv = np.full((T, N), 1e6)
    vol, beta, adv = bs.row_stats(ret, spy, dv, 150, np.arange(N))
    assert abs(beta[0] - 2.0) < 1e-6 and abs(beta[1] - 0.5) < 1e-6
    assert vol[2] > vol[0] and np.isnan(vol[3]) and np.isnan(beta[3]) and adv[0] == 1e6


def test_sharpe_bootstrap_and_bh() -> None:
    assert bs.sharpe([0.01]) == 0.0 and bs.sharpe([0.01, 0.01, 0.01]) == 0.0
    r = np.array([0.02, -0.01, 0.03, 0.0, 0.01, 0.02])
    assert abs(bs.sharpe(r) - r.mean() / r.std(ddof=1) * np.sqrt(12)) < 1e-12
    rng = np.random.default_rng(2)
    base = [rng.normal(0.004, 0.03, 60) for _ in range(3)]
    bins = [np.arange(60) for _ in range(3)]
    better = [b + 0.02 for b in base]
    hit = bs.paired_bootstrap(better, base, bins, draws=2000)
    same = bs.paired_bootstrap(base, base, bins, draws=2000)
    assert hit["mean_diff"] > 0.5 and hit["p"] < 0.05
    assert same["mean_diff"] == 0.0 and same["p"] >= 0.5
    assert bs.paired_bootstrap(better, base, bins, draws=500) == bs.paired_bootstrap(better, base, bins, draws=500)   # seeded
    q = bs.bh_adjusted([0.001, 0.04, 0.03, 0.5])
    assert q[0] == 0.004 and abs(q[1] - 0.0533333) < 1e-5 and abs(q[2] - 0.0533333) < 1e-5 and q[3] == 0.5
    assert bs.bh_adjusted([]) == []
