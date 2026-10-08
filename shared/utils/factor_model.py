"""Cross-sectional stock ranking from daily bars, validated walk-forward before use.

Event signals measured on this platform have not yet shown an edge. What the
literature has measured most often, over the longest samples, is the
cross-section: on a given day, which stocks are likely to beat which others
over the next week. Gu, Kelly & Xiu (2020) compared linear models, trees and
neural networks on that problem; every method agreed the dominant predictors
are variations on momentum, liquidity and volatility, and trees beat linear
models by capturing their interactions.

Features (all from data on or before the scoring date):
  mom_12_1   return from 12 months to 1 month ago (Jegadeesh & Titman)
  mom_6_1    return from 6 months to 1 month ago
  rev_5      last week's return (short-term reversal)
  rev_21     last month's return
  hi52       price over its 52-week high (George & Hwang 2004)
  max_21     largest daily return in the last month (Bali, Cakici & Whitelaw 2011)
  vol_63     realised volatility, 3 months
  ivol_63    idiosyncratic volatility net of SPY, 3 months
  beta_126   beta to SPY, 6 months
  dvol_21    log dollar volume, 1 month (liquidity)
  vtrend     1-month over 6-month volume (attention)
  amihud_21  log Amihud illiquidity, 1 month
  resmom_12_1  residual momentum: 12-1 month return net of beta x SPY
               (Blitz, Huij & Martens 2011), less exposed to market crashes
  up_share_21  share of up days in a month       } price-volume features in
  rsv_21       close within its 1-month range     } the style of Qlib's
  skew_63      skew of daily returns, 3 months    } Alpha158 set, the
  pv_corr_21   correlation of return and volume   } common LightGBM baseline

Every feature and the target are ranked within each date, so the model learns
an ordering, not a level, and a market-wide day moves nothing. The target is
the next-H-day return net of SPY.

Validation is walk-forward with a purge: each fold trains only on dates whose
label window closed before the fold begins, plus an embargo, then scores the
next block. Reported per model: mean daily rank IC, its t-statistic on
non-overlapping dates, the share of folds with positive IC, and a top-minus-
bottom quintile portfolio net of a per-side cost on turnover. A model is
`validated` only if it clears every bar; the gate is the same for the tree,
the linear baseline and plain momentum, and the best validated one is used.

Caveats stated with every report: the universe is today's listings
(survivorship), and adjusted prices know about later splits.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

FEATURES = [
    "mom_12_1", "mom_6_1", "rev_5", "rev_21", "hi52", "max_21",
    "vol_63", "ivol_63", "beta_126", "dvol_21", "vtrend", "amihud_21",
    "resmom_12_1", "up_share_21", "rsv_21", "skew_63", "pv_corr_21",
    "dtc", "ear", "ind_mom", "ind_rev_21",
]
# Features from sources that do not cover every name: a name without one is
# ranked at the middle rather than dropped (ETFs and recent listings have no
# short-interest history, and dropping them would also drop every name on a
# day before the source began).
OPTIONAL_FEATURES = {"dtc", "ear", "ind_mom", "ind_rev_21"}
HORIZON = 5                 # trading days ahead
SAMPLE_EVERY = 5            # sample one date in five: non-overlapping labels
EMBARGO = 5                 # trading days between train labels and a fold
MIN_HISTORY = 260           # bars a name needs before it has every feature
MIN_NAMES = 40              # a cross-section thinner than this is not ranked
FOLD_DAYS = 63              # one quarter per walk-forward fold
MIN_TRAIN_DATES = 40        # sampled training dates before the first fold
COST_BPS_PER_SIDE = 10.0

# The gate. All must hold out of sample.
GATE_MIN_IC = 0.01
GATE_MIN_IC_T = 2.0
GATE_MIN_POSITIVE_FOLDS = 0.55
GATE_MIN_NET_SHARPE = 0.5

@dataclass(frozen=True)
class HorizonSpec:
    """How one holding horizon is labelled, sampled and walked forward.

    Labels never overlap within a horizon: test dates are `test_every` apart,
    at least the horizon itself, and the embargo is one horizon."""
    name: str
    horizon: int            # trading days ahead
    test_every: int         # out-of-sample date spacing
    train_every: int        # training date spacing (daily trains on one day in five)
    embargo: int
    min_train_dates: int


HORIZON_SPECS: Dict[str, HorizonSpec] = {
    "daily": HorizonSpec("daily", 1, 1, 5, 1, 60),
    "weekly": HorizonSpec("weekly", 5, 5, 5, 5, 40),
    "monthly": HorizonSpec("monthly", 21, 21, 21, 21, 12),
}
DEFAULT_HORIZONS = ("monthly", "weekly", "daily")

# Tradable names only. The daily bars carry IEX volume, measured at ~2.6% of the
# consolidated tape (AAPL 1.15M shares a day against FINRA's 45M), so a $3M
# consolidated dollar-volume floor is about $75k here. Microcaps below it are
# where long-short anomalies look best on paper and cannot be traded.
MIN_PRICE = 3.0
IEX_VOLUME_SHARE = 0.025
MIN_CONSOLIDATED_DOLLAR_VOLUME = 3e6
MIN_FEED_DOLLAR_VOLUME = MIN_CONSOLIDATED_DOLLAR_VOLUME * IEX_VOLUME_SHARE


CAVEATS = [
    "survivorship: the universe is current listings, so names that later delisted are missing",
    "adjusted prices: splits and dividends are applied as of today",
    "costs are a flat per-side estimate on turnover, not a fill simulation",
]


@dataclass
class Panel:
    dates: List[date]
    tickers: List[str]
    close: np.ndarray       # [T, N], NaN where absent
    high: np.ndarray
    volume: np.ndarray
    # Point-in-time [T, N] inputs from other sources, by name: "days_to_cover"
    # (FINRA short interest, visible from publication, not settlement).
    extra: Dict[str, np.ndarray] = field(default_factory=dict)


def build_panel(rows: Sequence[Tuple[str, date, float, float, float]]) -> Panel:
    """(ticker, day, close, high, volume) rows -> dense panel."""
    dates = sorted({r[1] for r in rows})
    tickers = sorted({r[0] for r in rows})
    di = {d: i for i, d in enumerate(dates)}
    ti = {t: i for i, t in enumerate(tickers)}
    shape = (len(dates), len(tickers))
    close = np.full(shape, np.nan)
    high = np.full(shape, np.nan)
    volume = np.full(shape, np.nan)
    for t, d, c, h, v in rows:
        i, j = di[d], ti[t]
        close[i, j] = c
        high[i, j] = h if h else c
        volume[i, j] = v if v is not None else np.nan
    return Panel(dates, tickers, close, high, volume)


SHORT_INTEREST_AVAILABLE_LAG_DAYS = 14   # FINRA publishes ~8 business days after settlement
SHORT_INTEREST_MAX_AGE_DAYS = 45         # a figure older than this without a successor is dropped


def point_in_time(dates: Sequence[date], tickers: Sequence[str],
                  rows: Sequence[Tuple[str, date, float]],
                  lag_days: int = SHORT_INTEREST_AVAILABLE_LAG_DAYS,
                  max_age_days: int = SHORT_INTEREST_MAX_AGE_DAYS) -> np.ndarray:
    """(ticker, as_of, value) observations -> [T, N], each value visible from
    as_of + lag_days until its successor is visible or it is max_age_days old."""
    out = np.full((len(dates), len(tickers)), np.nan)
    if not rows or not dates:
        return out
    dord = np.array([d.toordinal() for d in dates])
    ti = {t: j for j, t in enumerate(tickers)}
    by: Dict[str, List[Tuple[date, float]]] = {}
    for t, d, v in rows:
        if t in ti and v is not None and np.isfinite(v):
            by.setdefault(t, []).append((d, float(v)))
    for t, obs in by.items():
        obs.sort()
        j = ti[t]
        starts = [int(np.searchsorted(dord, d.toordinal() + lag_days)) for d, _ in obs]
        for k, (d, v) in enumerate(obs):
            end = int(np.searchsorted(dord, d.toordinal() + lag_days + max_age_days))
            if k + 1 < len(obs):
                end = min(end, starts[k + 1])
            out[starts[k]:end, j] = v
    return out


EAR_WINDOW = 2       # sessions from the last close before the release
EAR_HOLD = 60        # trading days the reaction stays a feature: the drift window


def announcement_returns(panel: "Panel", events: Sequence[Tuple[str, int]], market: str = "SPY",
                         window: int = EAR_WINDOW, hold: int = EAR_HOLD) -> np.ndarray:
    """(ticker, base row) events -> [T, N] market-adjusted reaction, log return
    from the base row's close to `window` closes later, visible from that close
    for `hold` rows. The base row is the last close before the release was
    accepted, so the value never exists before the reaction it measures."""
    c = panel.close
    T = c.shape[0]
    out = np.full(c.shape, np.nan, dtype=c.dtype)
    ti = {t: j for j, t in enumerate(panel.tickers)}
    m = c[:, ti[market]] if market in ti else None
    for t, b in sorted(events, key=lambda e: e[1]):
        j = ti.get(t)
        e = b + window
        if j is None or b < 0 or e >= T:
            continue
        with np.errstate(all="ignore"):
            r = np.log(c[e, j] / c[b, j])
            if m is not None:
                r -= np.log(m[e] / m[b])
        if np.isfinite(r):
            out[e:min(T, e + hold), j] = r
    return out


MIN_PEERS = 3


def _peer_mean(x: np.ndarray, groups: np.ndarray) -> np.ndarray:
    """Per row, the mean of x over the name's industry peers, itself excluded;
    NaN where fewer than MIN_PEERS peers have a value or the industry is unknown."""
    out = np.full(x.shape, np.nan, dtype=x.dtype)
    for g in np.unique(groups):
        if g < 0:
            continue
        cols = np.flatnonzero(groups == g)
        if len(cols) <= MIN_PEERS:
            continue
        sub = x[:, cols]
        ok = np.isfinite(sub)
        total = np.where(ok, sub, 0.0).sum(axis=1, keepdims=True)
        count = ok.sum(axis=1, keepdims=True)
        peers = count - ok
        with np.errstate(all="ignore"):
            loo = (total - np.where(ok, sub, 0.0)) / peers
        out[:, cols] = np.where(peers >= MIN_PEERS, loo, np.nan)
    return out


def _rank(x: np.ndarray) -> np.ndarray:
    """Cross-sectional rank in [-0.5, 0.5]; NaN stays NaN."""
    out = np.full_like(x, np.nan, dtype=float)
    ok = np.isfinite(x)
    n = int(ok.sum())
    if n < 2:
        return out
    order = x[ok].argsort().argsort().astype(float)
    out[ok] = order / (n - 1) - 0.5
    return out


def _rolling(fn, a: np.ndarray, w: int) -> np.ndarray:
    """fn over trailing windows of length w along axis 0 (first w-1 rows NaN)."""
    T = a.shape[0]
    out = np.full(a.shape, np.nan, dtype=a.dtype)
    for i in range(w - 1, T):
        with np.errstate(all="ignore"):
            out[i] = fn(a[i - w + 1: i + 1])
    return out


def _rolling_pair(fn, a: np.ndarray, b: np.ndarray, w: int) -> np.ndarray:
    """fn over trailing windows of two aligned [T, N] arrays stacked on axis 2."""
    T = a.shape[0]
    out = np.full(a.shape, np.nan, dtype=a.dtype)
    for i in range(w - 1, T):
        with np.errstate(all="ignore"):
            out[i] = fn(np.stack([a[i - w + 1: i + 1], b[i - w + 1: i + 1]], axis=2))
    return out


def features(panel: Panel, market: str = "SPY") -> Dict[str, np.ndarray]:
    """Raw feature matrices [T, N] from the panel, each using data <= its row."""
    c, h, v = panel.close, panel.high, panel.volume
    with np.errstate(all="ignore"):
        r = np.vstack([np.full((1, c.shape[1]), np.nan, dtype=c.dtype), c[1:] / c[:-1] - 1.0])

        def lag(a, k):
            return np.vstack([np.full((k, a.shape[1]), np.nan, dtype=a.dtype), a[:-k]]) if k else a

        f: Dict[str, np.ndarray] = {
            "mom_12_1": lag(c, 21) / lag(c, 252) - 1.0,
            "mom_6_1": lag(c, 21) / lag(c, 126) - 1.0,
            "rev_5": c / lag(c, 5) - 1.0,
            "rev_21": c / lag(c, 21) - 1.0,
            "hi52": c / _rolling(lambda w: np.nanmax(w, axis=0), h, 252),
            "max_21": _rolling(lambda w: np.nanmax(w, axis=0), r, 21),
            "vol_63": _rolling(lambda w: np.nanstd(w, axis=0), r, 63),
            "dvol_21": np.log(_rolling(lambda w: np.nanmean(w, axis=0), c * v, 21)),
            "vtrend": np.log(
                _rolling(lambda w: np.nanmean(w, axis=0), v, 21)
                / _rolling(lambda w: np.nanmean(w, axis=0), v, 126)
            ),
            "amihud_21": np.log(_rolling(
                lambda w: np.nanmean(w, axis=0), np.abs(r) / (c * v + 1e-9), 21) + 1e-12),
        }
        beta = np.full(c.shape, np.nan, dtype=c.dtype)
        ivol = np.full(c.shape, np.nan, dtype=c.dtype)
        if market in panel.tickers:
            m = r[:, panel.tickers.index(market)]
            for i in range(126, c.shape[0]):
                mw = m[i - 125: i + 1]
                rw = r[i - 125: i + 1]
                ok_m = np.isfinite(mw)
                var = np.nanvar(mw[ok_m]) if ok_m.sum() > 20 else np.nan
                if not var or not np.isfinite(var):
                    continue
                mc = mw - np.nanmean(mw)
                cov = np.nanmean((rw - np.nanmean(rw, axis=0)) * mc[:, None], axis=0)
                b = cov / var
                beta[i] = b
                res = rw[-63:] - b * mw[-63:, None]
                ivol[i] = np.nanstd(res, axis=0)
        f["beta_126"] = beta
        f["ivol_63"] = ivol

        # Residual momentum: the 12-1 month return net of beta times the
        # market's over the same span, with the beta known at the start.
        if market in panel.tickers:
            mc = c[:, panel.tickers.index(market)][:, None]
            mkt_12_1 = lag(mc, 21) / lag(mc, 252) - 1.0
            f["resmom_12_1"] = f["mom_12_1"] - lag(beta, 21) * mkt_12_1
        else:
            f["resmom_12_1"] = f["mom_12_1"]
        f["up_share_21"] = _rolling(lambda w: np.nanmean(w > 0, axis=0), r, 21)
        lo21 = _rolling(lambda w: np.nanmin(w, axis=0), c, 21)
        hi21 = _rolling(lambda w: np.nanmax(w, axis=0), c, 21)
        f["rsv_21"] = (c - lo21) / (hi21 - lo21 + 1e-12)

        def _skew(w):
            mu = np.nanmean(w, axis=0)
            sd = np.nanstd(w, axis=0)
            return np.nanmean((w - mu) ** 3, axis=0) / (sd ** 3 + 1e-18)

        f["skew_63"] = _rolling(_skew, r, 63)
        dv = np.vstack([np.full((1, v.shape[1]), np.nan, dtype=v.dtype), np.log((v[1:] + 1) / (v[:-1] + 1))])

        def _corr(stack):
            a, b = stack[:, :, 0], stack[:, :, 1]
            am, bm = a - np.nanmean(a, axis=0), b - np.nanmean(b, axis=0)
            return np.nanmean(am * bm, axis=0) / (np.nanstd(a, axis=0) * np.nanstd(b, axis=0) + 1e-18)

        f["pv_corr_21"] = _rolling_pair(_corr, r, dv, 21)
        # Days-to-cover (Hong, Li, Ni, Scheinkman & Yan 2015): high values
        # preceded underperformance. Logged: the distribution is long-tailed.
        dtc = panel.extra.get("days_to_cover")
        f["dtc"] = np.log1p(np.clip(dtc, 0, None)) if dtc is not None else np.full(c.shape, np.nan, dtype=c.dtype)
        # Earnings announcement return (post-earnings drift: Chan, Jegadeesh &
        # Lakonishok 1996; Brandt, Kishore, Santa-Clara & Venkatachalam 2008).
        ear = panel.extra.get("earnings_reaction")
        f["ear"] = ear if ear is not None else np.full(c.shape, np.nan, dtype=c.dtype)
        # Industry momentum (Moskowitz & Grinblatt 1999): the peers' 12-1 month
        # return, the name itself left out. Within-industry reversal (Da, Liu &
        # Schaumburg 2014): last month's return net of its peers', which
        # reverses more reliably than the raw return.
        ind = panel.extra.get("industry")
        if ind is not None:
            f["ind_mom"] = _peer_mean(f["mom_12_1"], ind)
            f["ind_rev_21"] = f["rev_21"] - _peer_mean(f["rev_21"], ind)
        else:
            f["ind_mom"] = np.full(c.shape, np.nan, dtype=c.dtype)
            f["ind_rev_21"] = np.full(c.shape, np.nan, dtype=c.dtype)
    return f


def forward_excess(panel: Panel, horizon: int = HORIZON, market: str = "SPY") -> np.ndarray:
    """Next-`horizon`-day log return net of the market's, [T, N]; NaN at the end."""
    c = panel.close
    with np.errstate(all="ignore"):
        fwd = np.vstack([np.log(c[horizon:] / c[:-horizon]), np.full((horizon, c.shape[1]), np.nan, dtype=c.dtype)])
    if market in panel.tickers:
        fwd = fwd - fwd[:, [panel.tickers.index(market)]]
    return fwd


def tradable(panel: Panel) -> np.ndarray:
    """[T, N]: price at least MIN_PRICE and 21-day mean dollar volume above the
    floor, each from data up to that row. Cached on the panel."""
    cached = panel.extra.get("_tradable")
    if cached is not None:
        return cached
    c, v = panel.close, panel.volume
    with np.errstate(all="ignore"):
        dv = _rolling(lambda w: np.nanmean(w, axis=0), c * v, 21)
        mask = (c >= MIN_PRICE) & (dv >= MIN_FEED_DOLLAR_VOLUME)
    panel.extra["_tradable"] = mask
    return mask


def _eligible(panel: Panel, upto: int, market: str = "SPY") -> np.ndarray:
    """Tradable names with enough history at row `upto`; the market is never ranked."""
    ok = np.sum(np.isfinite(panel.close[: upto + 1]), axis=0) >= MIN_HISTORY
    ok &= tradable(panel)[upto]
    # Funds and non-common securities (shared.utils.asset_universe): ranked
    # among companies, a 2x fund's leverage and decay read as signal.
    excluded = panel.extra.get("not_a_stock")
    if excluded is not None:
        ok &= ~excluded
    if market in panel.tickers:
        ok[panel.tickers.index(market)] = False
    return ok


def _cross_section(feats: Dict[str, np.ndarray], i: int, hist_ok: np.ndarray) -> np.ndarray:
    """Row i's features, ranked across eligible names; optional ones neutral when absent."""
    cols = []
    for name in FEATURES:
        col = _rank(np.where(hist_ok, feats[name][i], np.nan))
        if name in OPTIONAL_FEATURES:
            col = np.where(hist_ok & ~np.isfinite(col), 0.0, col)
        cols.append(col)
    return np.column_stack(cols)


def design(panel: Panel, feats: Dict[str, np.ndarray], target: np.ndarray, rows: Sequence[int],
           return_cols: bool = False):
    """Rank-normalised (X, y, raw_y, date_index) over the given date rows, and
    with return_cols the panel column (ticker) of each row as a fifth array."""
    xs, ys, raws, idx, cols = [], [], [], [], []
    for i in rows:
        hist_ok = _eligible(panel, i)
        X = _cross_section(feats, i, hist_ok)
        ok = hist_ok & np.all(np.isfinite(X), axis=1)
        if ok.sum() < MIN_NAMES:
            continue
        yr = target[i]
        y = _rank(np.where(ok, yr, np.nan))
        keep = ok & np.isfinite(y)
        xs.append(X[keep])
        ys.append(y[keep])
        raws.append(yr[keep])
        idx.append(np.full(int(keep.sum()), i))
        cols.append(np.flatnonzero(keep))
    if not xs:
        empty = (np.empty((0, len(FEATURES))), np.empty(0), np.empty(0), np.empty(0, dtype=int))
        return empty + (np.empty(0, dtype=int),) if return_cols else empty
    out = (np.vstack(xs), np.concatenate(ys), np.concatenate(raws), np.concatenate(idx))
    return out + (np.concatenate(cols),) if return_cols else out


# A name stays in the book until it leaves the top (or bottom) 40%, rather than
# the moment it leaves the quintile. Rebuilding quintiles every period turned
# over ~76% of the book a week across the full listed universe, and at 10 bps a
# side that cost most of the edge. The buffer trades less for nearly the same
# exposure; it is the portfolio rule every candidate is measured under.
HOLD_BUFFER = 0.40


def _buffered_book(pred: np.ndarray, cols: np.ndarray, held: set, side: int) -> List[int]:
    """Positions (into pred) of this period's long (side=1) or short (-1) book."""
    n = len(pred)
    q = max(1, n // 5)
    pct = pred.argsort().argsort() / max(1, n - 1)
    edge = pct if side > 0 else 1 - pct
    keep = [k for k in range(n) if cols[k] in held and edge[k] >= 1 - HOLD_BUFFER]
    keep = sorted(keep, key=lambda k: -edge[k])[:q]
    chosen = set(keep)
    for k in np.argsort(-edge):
        if len(keep) >= q:
            break
        if k not in chosen:
            keep.append(int(k))
            chosen.add(int(k))
    return keep


def _spearman(a: np.ndarray, b: np.ndarray) -> Optional[float]:
    if len(a) < MIN_NAMES:
        return None
    ra, rb = a.argsort().argsort().astype(float), b.argsort().argsort().astype(float)
    sa, sb = ra.std(), rb.std()
    if sa == 0 or sb == 0:
        return None
    return float(np.mean((ra - ra.mean()) * (rb - rb.mean())) / (sa * sb))


def _make(model: str):
    if model == "gbm":
        from sklearn.ensemble import HistGradientBoostingRegressor

        return HistGradientBoostingRegressor(
            max_depth=3, learning_rate=0.05, max_iter=200, min_samples_leaf=200,
            l2_regularization=1.0, random_state=7,
        )
    if model == "ridge":
        from sklearn.linear_model import Ridge

        return Ridge(alpha=10.0)
    if model == "lgbm_rank":
        # LambdaRank: optimises the day's ordering directly, the thing a
        # long-short book trades on. Learning-to-rank roughly doubled rank IC
        # over regression in the cross-sectional studies it was tested in.
        from lightgbm import LGBMRanker

        return LGBMRanker(
            objective="lambdarank", n_estimators=300, learning_rate=0.05, num_leaves=15,
            min_child_samples=200, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
            reg_lambda=1.0, random_state=7, verbose=-1,
        )
    return None   # "momentum": no fit, the mom_12_1 rank itself


def lightgbm_available() -> bool:
    try:
        import lightgbm  # noqa: F401
        return True
    except Exception:
        return False


def _fit(model_name: str, est, X: np.ndarray, y: np.ndarray, d: np.ndarray) -> None:
    """Fit, with per-date groups and integer relevance for the ranker."""
    if model_name == "lgbm_rank":
        # Rows arrive grouped by date (design appends date by date).
        _, counts = np.unique(d, return_counts=True)
        order = np.argsort(d, kind="stable")
        labels = np.clip(((y[order] + 0.5) * 5).astype(int), 0, 4)   # quintile 0..4
        est.fit(X[order], labels, group=counts)
    else:
        est.fit(X, y)


@dataclass
class ModelReport:
    model: str
    folds: int
    dates: int
    mean_ic: Optional[float]
    ic_t: Optional[float]
    positive_fold_share: Optional[float]
    ls_gross_bps: Optional[float]           # mean top-minus-bottom per period
    ls_net_bps: Optional[float]
    net_sharpe: Optional[float]             # annualised, periods of HORIZON days
    turnover: Optional[float]
    validated: bool
    reasons: List[str] = field(default_factory=list)
    # For the Deflated Sharpe Ratio across candidates, set by run_all.
    sr_period: Optional[float] = None       # net Sharpe per holding period
    skew: Optional[float] = None
    kurtosis: Optional[float] = None
    periods: int = 0
    deflated_sharpe: Optional[float] = None
    # Volatility-managed (Moreira & Muir 2017): each period scaled by the
    # inverse of the trailing realised variance; informational, not gated.
    vol_managed_sharpe: Optional[float] = None


def _predict(model_name: str, est, X: np.ndarray) -> np.ndarray:
    if model_name == "momentum":
        return X[:, FEATURES.index("mom_12_1")]
    return est.predict(X)


def _spec(horizon: int, spec: Optional[HorizonSpec]) -> HorizonSpec:
    if spec is not None:
        return spec
    return HorizonSpec(f"{horizon}d", horizon, SAMPLE_EVERY, SAMPLE_EVERY, EMBARGO, MIN_TRAIN_DATES)


def walk_forward(panel: Panel, model_name: str, horizon: int = HORIZON,
                 spec: Optional[HorizonSpec] = None,
                 feats: Optional[Dict[str, np.ndarray]] = None) -> ModelReport:
    """Out-of-sample evaluation, refit each fold on purged history."""
    sp = _spec(horizon, spec)
    horizon = sp.horizon
    feats = feats if feats is not None else features(panel)
    target = forward_excess(panel, horizon)
    T = len(panel.dates)
    train_sampled = list(range(MIN_HISTORY, T - horizon, sp.train_every))
    sampled = list(range(MIN_HISTORY, T - horizon, sp.test_every))
    if len(train_sampled) < sp.min_train_dates + 4:
        return ModelReport(model_name, 0, 0, None, None, None, None, None, None, None, False,
                           ["not enough history to validate"])
    fold_starts = list(range(train_sampled[sp.min_train_dates], T - horizon, FOLD_DAYS))
    ics: List[float] = []
    fold_ic: List[float] = []
    ls: List[float] = []
    turns: List[float] = []
    prev_long: set = set()
    prev_short: set = set()
    for k, start in enumerate(fold_starts):
        end = fold_starts[k + 1] if k + 1 < len(fold_starts) else T - horizon
        train_rows = [i for i in train_sampled if i + horizon + sp.embargo <= start]
        test_rows = [i for i in sampled if start <= i < end]
        if len(train_rows) < sp.min_train_dates or not test_rows:
            continue
        est = _make(model_name)
        if est is not None:
            X, y, _r, _d = design(panel, feats, target, train_rows)
            if len(y) < 1000:
                continue
            _fit(model_name, est, X, y, _d)
        this_fold: List[float] = []
        for i in test_rows:
            X, y, raw, _d, cols = design(panel, feats, target, [i], return_cols=True)
            if len(y) < MIN_NAMES:
                continue
            pred = _predict(model_name, est, X)
            ic = _spearman(pred, raw)
            if ic is None:
                continue
            ics.append(ic)
            this_fold.append(ic)
            q = max(1, len(pred) // 5)
            long_pos = _buffered_book(pred, cols, prev_long, 1)
            short_pos = _buffered_book(pred, cols, prev_short, -1)
            ls.append(float(np.mean(raw[long_pos]) - np.mean(raw[short_pos])) * 1e4)
            # Turnover on tickers, not row positions: the eligible set changes.
            long_set = {int(cols[k]) for k in long_pos}
            short_set = {int(cols[k]) for k in short_pos}
            if prev_long:
                turns.append(1 - len(long_set & prev_long) / q / 2 - len(short_set & prev_short) / q / 2)
            prev_long, prev_short = long_set, short_set
        if this_fold:
            fold_ic.append(float(np.mean(this_fold)))

    if len(ics) < 8:
        return ModelReport(model_name, len(fold_ic), len(ics), None, None, None, None, None, None, None,
                           False, ["too few out-of-sample dates"])
    ics_a = np.array(ics)
    mean_ic = float(ics_a.mean())
    ic_t = float(mean_ic / (ics_a.std(ddof=1) / math.sqrt(len(ics_a)))) if ics_a.std(ddof=1) > 0 else 0.0
    pos = float(np.mean(np.array(fold_ic) > 0)) if fold_ic else 0.0
    turnover = float(np.mean(turns)) if turns else 1.0
    gross = float(np.mean(ls))
    # Both legs turn over; each side pays the per-side cost on what changed.
    net_series = np.array(ls) - 2 * 2 * COST_BPS_PER_SIDE * turnover
    net = float(net_series.mean())
    periods = 252 / horizon
    sd = float(net_series.std(ddof=1)) if len(net_series) > 1 else 0.0
    sharpe = float(net / sd * math.sqrt(periods)) if sd > 0 else 0.0
    sr_p = float(net / sd) if sd > 0 else 0.0
    cen = net_series - net_series.mean()
    m2 = float(np.mean(cen ** 2)) or 1e-18
    skew = float(np.mean(cen ** 3) / m2 ** 1.5)
    kurt = float(np.mean(cen ** 4) / m2 ** 2)
    vm = []
    for k in range(12, len(net_series)):
        trail = net_series[k - 12:k]
        var = float(np.var(trail))
        if var > 0:
            vm.append(net_series[k] / var)
    vm_sharpe = None
    if len(vm) > 2 and float(np.std(vm, ddof=1)) > 0:
        vm_sharpe = round(float(np.mean(vm) / np.std(vm, ddof=1) * math.sqrt(periods)), 2)
    reasons = []
    if mean_ic < GATE_MIN_IC:
        reasons.append(f"mean IC {mean_ic:.3f} below {GATE_MIN_IC}")
    if ic_t < GATE_MIN_IC_T:
        reasons.append(f"IC t {ic_t:.2f} below {GATE_MIN_IC_T}")
    if pos < GATE_MIN_POSITIVE_FOLDS:
        reasons.append(f"positive in {pos:.0%} of folds, needs {GATE_MIN_POSITIVE_FOLDS:.0%}")
    if sharpe < GATE_MIN_NET_SHARPE:
        reasons.append(f"net Sharpe {sharpe:.2f} below {GATE_MIN_NET_SHARPE}")
    return ModelReport(
        model_name, len(fold_ic), len(ics), round(mean_ic, 4), round(ic_t, 2), round(pos, 3),
        round(gross, 1), round(net, 1), round(sharpe, 2), round(turnover, 3), not reasons, reasons,
        sr_period=round(sr_p, 5), skew=round(skew, 4), kurtosis=round(kurt, 4), periods=len(net_series),
        vol_managed_sharpe=vm_sharpe,
    )


def score_latest(panel: Panel, model_name: str, horizon: int = HORIZON,
                 spec: Optional[HorizonSpec] = None,
                 feats: Optional[Dict[str, np.ndarray]] = None) -> Dict[str, Dict[str, Any]]:
    """Fit on all purged history and rank the newest cross-section.

    Returns {ticker: {score, quintile, features: {name: rank}}}; quintile 5 is
    the most favoured. Ranks, not return forecasts: the model learned an order."""
    sp = _spec(horizon, spec)
    horizon = sp.horizon
    feats = feats if feats is not None else features(panel)
    target = forward_excess(panel, horizon)
    T = len(panel.dates)
    last = T - 1
    train_rows = [i for i in range(MIN_HISTORY, T - horizon, sp.train_every) if i + horizon + sp.embargo <= last]
    est = _make(model_name)
    if est is not None:
        X, y, _r, _d = design(panel, feats, target, train_rows)
        if len(y) < 1000:
            return {}
        _fit(model_name, est, X, y, _d)
    hist_ok = _eligible(panel, last)
    X = _cross_section(feats, last, hist_ok)
    ok = hist_ok & np.all(np.isfinite(X), axis=1)
    if ok.sum() < MIN_NAMES:
        return {}
    pred = _predict(model_name, est, X[ok])
    names = [panel.tickers[j] for j in np.flatnonzero(ok)]
    order = pred.argsort().argsort()
    n = len(pred)
    out = {}
    for k, t in enumerate(names):
        pct = order[k] / max(1, n - 1)
        out[t] = {
            "score": round(float(pred[k]), 5),
            "percentile": round(float(pct), 4),
            "quintile": int(min(5, 1 + pct * 5)),
            "features": {f: round(float(X[ok][k][j]), 3) for j, f in enumerate(FEATURES)},
        }
    return out


__all__ = [
    "CAVEATS", "DEFAULT_HORIZONS", "FEATURES", "HORIZON_SPECS", "HorizonSpec", "OPTIONAL_FEATURES", "tradable", "announcement_returns", "point_in_time", "GATE_MIN_IC_T", "HORIZON", "ModelReport", "Panel", "build_panel",
    "features", "forward_excess", "score_latest", "walk_forward",
]


GATE_MIN_DSR = 0.95
_EULER = 0.5772156649


def _norm_ppf(p: float) -> float:
    try:
        from scipy.stats import norm

        return float(norm.ppf(p))
    except ImportError:  # Acklam-free fallback: bisection on erf
        lo, hi = -10.0, 10.0
        for _ in range(100):
            mid = (lo + hi) / 2
            if 0.5 * math.erfc(-mid / math.sqrt(2)) < p:
                lo = mid
            else:
                hi = mid
        return (lo + hi) / 2


def deflated_sharpe(sr: float, periods: int, skew: float, kurt: float, trial_srs: Sequence[float],
                    n_trials: Optional[int] = None) -> Optional[float]:
    """Probability the true Sharpe exceeds what the best of these trials would
    show by luck (Bailey & Lopez de Prado 2014). Per-period Sharpe throughout.

    `trial_srs` sets the dispersion of Sharpe estimates among comparable
    trials; `n_trials` (default: their count) is how many were tried in all."""
    n = int(n_trials) if n_trials else len(trial_srs)
    if periods < 3 or n < 1:
        return None
    var = float(np.var(trial_srs, ddof=1)) if len(trial_srs) > 1 else 0.0
    var = max(var, 1.0 / periods)          # floor: the sampling variance of one SR
    if n > 1:
        sr0 = math.sqrt(var) * ((1 - _EULER) * _norm_ppf(1 - 1 / n) + _EULER * _norm_ppf(1 - 1 / (n * math.e)))
    else:
        sr0 = 0.0
    denom = 1 - skew * sr + (kurt - 1) / 4 * sr * sr
    if denom <= 0:
        return None
    z = (sr - sr0) * math.sqrt(periods - 1) / math.sqrt(denom)
    return float(0.5 * math.erfc(-z / math.sqrt(2)))


def run_all(rows: Sequence[Tuple[str, date, float, float, float]], candidates: Sequence[str]) -> Dict[str, Any]:
    """Build, validate every candidate, choose, and score -- in one call.

    Plain inputs and outputs so it can run in a separate process: the work is
    CPU-bound NumPy and Python that holds the GIL, and run in a thread of an
    asyncio service it starved that service's event loop until its heartbeat
    lapsed and the container was recycled."""
    return run_panel(build_panel(rows), candidates)


def run_panel(panel: Panel, candidates: Sequence[str],
              horizons: Sequence[str] = ("weekly",)) -> Dict[str, Any]:
    """run_all on a panel already built -- the service fills one in chunks, so
    the history never exists as millions of Python row tuples at once.

    Every candidate is walked forward at every horizon, and every one of those
    is a trial: the deflated Sharpe of each is judged against all of them, their
    Sharpe ratios put on one annual scale and returned to the candidate's own
    period. Trying three horizons and keeping the best is exactly the search the
    deflation exists to price. The primary horizon is the best validated one,
    or, with none validated, the best net Sharpe -- published and marked."""
    if len(panel.tickers) < MIN_NAMES or len(panel.dates) < MIN_HISTORY + 60:
        return {"skipped": "not enough daily history yet",
                "universe": len(panel.tickers), "trading_days": len(panel.dates)}
    feats = features(panel)
    by_h: Dict[str, List[ModelReport]] = {}
    for h in horizons:
        sp = HORIZON_SPECS[h]
        by_h[h] = []
        for name in candidates:
            try:
                by_h[h].append(walk_forward(panel, name, spec=sp, feats=feats))
            except Exception as e:  # one model failing must not lose the others
                by_h[h].append(ModelReport(name, 0, 0, None, None, None, None, None, None, None,
                                           False, [f"error: {type(e).__name__}: {e}"]))
    # Every candidate at every horizon counts toward the number of trials. The
    # dispersion of Sharpe estimates is taken among the same horizon's
    # candidates: a daily book destroyed by its own turnover is not a draw from
    # the same distribution as a monthly one, and pooling them let the daily
    # losses set every horizon's bar -- every deflated Sharpe read 0.0.
    annual = [r.sr_period for reps in by_h.values() for r in reps if r.sr_period is not None]
    for h, reps in by_h.items():
        trial = [r.sr_period for r in reps if r.sr_period is not None]
        for r in reps:
            if r.sr_period is None:
                continue
            r.deflated_sharpe = deflated_sharpe(r.sr_period, r.periods, r.skew or 0.0, r.kurtosis or 3.0, trial,
                                                n_trials=len(annual))
            if r.deflated_sharpe is not None:
                r.deflated_sharpe = round(r.deflated_sharpe, 4)
            if r.validated and (r.deflated_sharpe is None or r.deflated_sharpe < GATE_MIN_DSR):
                r.validated = False
                r.reasons.append(f"deflated Sharpe {r.deflated_sharpe} below {GATE_MIN_DSR} "
                                 f"across {len(trial)} trials")

    per_h: Dict[str, Dict[str, Any]] = {}
    chosen_by_h: Dict[str, Optional[ModelReport]] = {}
    for h, reps in by_h.items():
        validated = [r for r in reps if r.validated]
        pool = validated or [r for r in reps if r.dates]
        chosen = max(pool, key=lambda r: ((r.net_sharpe or -9), (r.ic_t or -9))) if pool else None
        chosen_by_h[h] = chosen
        per_h[h] = {"horizon_days": HORIZON_SPECS[h].horizon, "models": [r.__dict__ for r in reps],
                    "chosen": chosen.model if chosen else None,
                    "validated": bool(chosen and chosen.validated),
                    "net_sharpe": chosen.net_sharpe if chosen else None,
                    "deflated_sharpe": chosen.deflated_sharpe if chosen else None}
    ranked = sorted(per_h, key=lambda h: (per_h[h]["validated"], per_h[h]["net_sharpe"] or -9), reverse=True)
    primary = ranked[0] if ranked else None
    for h in per_h:
        ch = chosen_by_h[h]
        per_h[h]["scores"] = (score_latest(panel, ch.model, spec=HORIZON_SPECS[h], feats=feats)
                              if ch else {})
    head = per_h.get(primary) or {}
    return {
        "as_of": str(panel.dates[-1]),
        "universe": len(panel.tickers),
        "tradable": int(tradable(panel)[-1].sum()),
        "trading_days": len(panel.dates),
        "primary_horizon": primary,
        "horizon_days": head.get("horizon_days", HORIZON),
        "trials": len(annual),
        "models": head.get("models", []),
        "chosen": head.get("chosen"),
        "validated": bool(head.get("validated")),
        "scores": head.get("scores", {}),
        "horizons": per_h,
    }
