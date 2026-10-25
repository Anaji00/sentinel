"""Portfolio constructions judged on out-of-sample history, not paper trades."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import portfolio_lab as pl  # noqa: E402


def test_ranking_within_industry_spreads_each_side_across_industries() -> None:
    pred = np.array([0.9, 0.8, 0.7, 0.6, 0.5, 0.1, 0.2, 0.3, 0.4, 0.45])
    industry = np.array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2])
    rel = pl.industry_relative(pred, industry)
    assert rel[0] == 0.9 and rel[9] == 0.9                 # best of each industry rank alike
    assert np.isfinite(rel).all()
    small = pl.industry_relative(np.array([0.3, 0.1, 0.2]), np.array([7, -1, 8]))
    assert list(small) == [2.5 / 3, 0.5 / 3, 1.5 / 3]      # tiny and unknown industries pooled


def _history(sector_bet: bool, dates: int = 40, n: int = 200, seed: int = 0):
    """Predictions that know each stock's own move; the sector one optionally
    also leans the whole of industry 1 long, and industry 1's move is a coin flip."""
    rng = np.random.default_rng(seed)
    industry = np.array([i % 10 for i in range(n)])
    out = []
    for d in range(dates):
        own = rng.normal(0, 0.02, n)
        sector = np.where(industry == 1, rng.choice([-0.05, 0.05]), 0.0)
        raw = own + sector
        pred = own + (np.where(industry == 1, 1.0, 0.0) if sector_bet else 0.0)
        out.append((d * 21, np.arange(n), pred, raw))
    return out, industry


def test_a_sector_bet_shows_up_as_industry_variance_and_neutral_removes_it() -> None:
    preds, industry = _history(sector_bet=True)
    res = pl.compare(preds, industry, None, 21, 0.001)
    live, neutral = res["live"], res["industry_neutral"]
    assert live["industry_share_of_variance"] > 0.5
    assert neutral["industry_share_of_variance"] < 0.2
    assert live["max_industry_tilt"] > neutral["max_industry_tilt"]
    assert neutral["sharpe"] > live["sharpe"]
    assert "industry-neutral" in pl.verdict(res)


def test_too_little_history_says_so() -> None:
    preds, industry = _history(sector_bet=False, dates=2)
    res = pl.compare(preds, industry, None, 21, 0.001)
    assert res["live"]["net_bps"] is None
    assert pl.verdict(res).startswith("Not enough")


def test_regimes_are_labelled_from_past_data_only_and_ic_is_split_by_them() -> None:
    up = np.exp(np.linspace(0, 0.3, 200))
    labels = pl.regime_labels(up)
    assert labels[10] is None and labels[150].startswith("up")
    down = up[::-1].copy()
    assert pl.regime_labels(down)[150].startswith("down")
    preds = [(150, np.arange(60), np.arange(60, dtype=float), np.arange(60, dtype=float))]
    res = pl.ic_by_regime(preds, up)
    (lab, cell), = res.items()
    assert lab.startswith("up") and cell["dates"] == 1 and cell["mean_ic"] == 1.0


def test_a_rank_weighted_book_uses_the_whole_ordering() -> None:
    preds, _industry = _history(sector_bet=False)
    rw = pl.rank_weighted(preds, 21, 0.001)
    assert rw["periods"] == 40 and rw["net_bps"] > 0 and rw["sharpe"] > 0


def test_hedged_books_strip_the_market_and_keep_the_short_alpha() -> None:
    # High-beta names ranked best carry only market (excess = (beta - 1) m);
    # low-beta names ranked worst also lag by 2% a period: the edge is short-side.
    rng = np.random.default_rng(3)
    n, dates = 100, 60
    b = np.where(np.arange(n) < 50, 0.5, 1.5)
    beta = np.tile(b, (dates * 21 + 1, 1))
    market = np.full(dates * 21 + 1, np.nan)
    preds = []
    for d in range(dates):
        m = rng.normal(0, 0.05)
        market[d * 21] = m
        raw = (b - 1) * m + np.where(np.arange(n) < 20, -0.02, 0.0) + rng.normal(0, 0.002, n)
        preds.append((d * 21, np.arange(n), b.copy(), raw))
    res = pl.compare(preds, np.zeros(n, dtype=int), beta, 21, 0.0, market)
    live, bn, sh = res["live"], res["beta_neutral"], res["short_hedged"]
    # The live book is long beta: its result swings with the market.
    assert sh["gross_bps"] > 150 and bn["gross_bps"] > 150
    assert sh["sharpe"] > 3 * abs(live["sharpe"] or 0.0) + 1.0
    assert "beta_neutral" not in pl.compare(preds, np.zeros(n, dtype=int), None, 21, 0.0, market)


def test_hedged_books_report_no_market_beta() -> None:
    rng = np.random.default_rng(4)
    n = 100
    b = np.where(np.arange(n) < 50, 0.5, 1.5)
    beta = np.tile(b, (22 * 30, 1))
    market = rng.normal(0, 0.05, 22 * 30)
    preds = [(d * 21, np.arange(n), b.copy(), (b - 1) * market[d * 21] + rng.normal(0, 0.01, n)) for d in range(30)]
    res = pl.compare(preds, np.zeros(n, dtype=int), beta, 21, 0.0, market)
    assert res["live"]["net_beta"] > 0.5
    assert res["beta_neutral"]["net_beta"] == 0.0 and res["short_hedged"]["net_beta"] == 0.0


def test_meta_sizing_goes_live_only_when_runs_agree_and_reads_live_bars() -> None:
    import math
    from shared.utils import meta_label as ml
    assert not ml.confirmed([{"base": 2.0, "meta": 2.2}])
    assert ml.confirmed([{"base": 2.15, "meta": 2.13}, {"base": 2.0, "meta": 2.2}, {"base": 2.1, "meta": 2.3}])
    assert not ml.confirmed([{"base": 2.0, "meta": 2.2}, {"base": 2.1, "meta": 2.05}])
    closes = [100 * math.exp(0.001 * k) for k in range(300)]
    spy = [400 * math.exp(0.0005 * k + 0.01 * math.sin(k)) for k in range(300)]
    x = ml.live_features(0.97, -1, closes, [1e6] * 300, spy)
    assert x is not None and len(x) == 8 and abs(x[0] - 0.47) < 1e-9 and x[-1] == -1.0
    model = {"coef": [0.0] * 8, "intercept": 0.0}
    assert abs(ml.probability(model, x) - 0.5) < 1e-12 and ml.probability(model, x[:3]) is None


def test_adoption_follows_the_measured_gain_and_the_worst_period() -> None:
    lab = {"industry_neutral": {"sharpe": 2.29}, "cluster_neutral": {"sharpe": 2.49},
           "adjusted": {"base": {"sharpe": 2.42, "worst_bps": -573.7},
                        "momentum_vol_scaled": {"sharpe": 2.69, "worst_bps": -292.8},
                        "sentiment_short_tilt": {"sharpe": 2.36, "worst_bps": -577.7}}}
    assert pl.adoptions(lab) == {"cluster_neutral": True, "momentum_vol_scale": True, "sentiment_short_tilt": False}
    deeper = {**lab, "adjusted": {**lab["adjusted"], "momentum_vol_scaled": {"sharpe": 2.69, "worst_bps": -600.0}}}
    assert pl.adoptions(deeper)["momentum_vol_scale"] is False
    assert pl.adoptions({}) == {"cluster_neutral": False, "momentum_vol_scale": False, "sentiment_short_tilt": False}


def test_momentum_scale_shrinks_after_a_volatile_stretch_and_tone_tilts_the_short_leg() -> None:
    rng = np.random.default_rng(5)
    mom = np.concatenate([rng.normal(0, 0.005, 400), rng.normal(0, 0.02, 200)])
    k = pl.momentum_vol_scale(mom)
    assert k[300] > 0.8 and k[-1] == 0.5                       # calm: about 1; storm: the floor
    tone = np.concatenate([np.zeros(200) + rng.normal(0, 0.01, 200), np.full(60, 0.2)])
    w = pl.sentiment_tilt(tone)
    assert w[100] < 1.3 and w[-1] == 1.5 and np.isnan(np.full(3, np.nan)).all()


def test_return_clusters_find_the_themes_names_move_in() -> None:
    rng = np.random.default_rng(6)
    T, per = 130, 40
    themes = rng.normal(0, 0.02, (T, 3))
    r = np.concatenate([themes[:, [j]] + rng.normal(0, 0.003, (T, per)) for j in range(3)], axis=1)
    close = 100 * np.exp(np.cumsum(r, axis=0))
    labels = pl.return_clusters(close, T - 1, np.arange(3 * per), k=3, components=3)
    for j in range(3):
        block = labels[j * per:(j + 1) * per]
        assert (block == np.bincount(block).argmax()).mean() > 0.9   # each theme lands in one cluster
    assert len(set(labels.tolist())) == 3
