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
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from shared.utils.bar_feed import DAILY_BAR_VOLUME_SHARE

# Removed 24 Oct 2026 (trials P1 and P2, MODEL_TRIALS.md), judged on four
# offsets of the monthly test calendar over 11 years: the four regime
# interactions (trial E's, adopted on five years) carried a quarter of the
# ranker's splits yet predicted little alone, and dropping them lifted the mean
# net Sharpe 0.68 -> 0.96 (IC t 2.40 -> 3.27), better on every offset; ivol_63,
# resmom_12_1 and amihud_21 near-duplicated vol_63, mom_12_1 and dvol_21
# (rank correlations 0.96, 0.91, -0.87) and added nothing. Together 0.97, IC t
# 3.34. All seven are still computed for the board and the stock pages.
FEATURES = [
    "mom_12_1", "mom_6_1", "rev_5", "rev_21", "hi52", "max_21",
    "vol_63", "beta_126", "dvol_21", "vtrend",
    "up_share_21", "rsv_21", "skew_63", "pv_corr_21",
    "dtc", "ear", "ind_mom", "ind_rev_21",
    # Adopted 9 Oct 2026 after pre-registered trials on the top-1,000 universe:
    # overnight momentum and the intraday month (Lou, Polk & Skouras 2019), and
    # quality vs industry with earnings yield from filing-dated SEC facts.
    # Together weekly net Sharpe 0.71 -> 0.86, monthly 0.67 -> 1.13 (deflated
    # 0.32 -> 0.65); each alone helped less.
    "mom_on_12_1", "id_21", "quality_ind", "ey",
    # Adopted 9 Oct 2026 (trial G, pre-registered): FINRA off-exchange short
    # volume share, 20-day level and its change. Weekly Sharpe 0.90 -> 1.00
    # (deflated 0.53 -> 0.59, IC t 2.26 -> 2.38); monthly 1.24 -> 1.25 (deflated
    # 0.744 -> 0.748), a marginal gain. The standardized earnings surprise
    # (`sue`) lowered both horizons and stays board context.
    "svr_20", "svr_chg",
    # Adopted 18 Oct 2026 (trial L, pre-registered): option-implied measures
    # rebuilt from daily option bars (shared/utils/option_iv.py) -- the
    # call-put IV spread (Cremers & Weinbaum 2010), the skew (Xing, Zhang & Zhao
    # 2010), put/call volume (Pan & Poteshman 2006), realised minus implied
    # volatility (Bali & Hovakimian 2009) and the month's change in implied
    # (An, Ang, Bali & Cakici 2014). On the 595 option names from April 2024:
    # weekly IC 0.018 -> 0.023 (t 0.89 -> 1.21), long-short 14 -> 28 bps a
    # week; monthly IC 0.046 -> 0.056 (t 1.24 -> 1.55), 113 -> 232 bps a month.
    # Daily fell (0.012 -> 0.010) and the daily horizon stays withheld.
    "opt_cp_spread", "opt_skew", "opt_log_pcr", "opt_vol_spread", "opt_iv_change",
]
# Computed and ranked for the signal board, not trained on. Added to the model
# on 8 October 2026, the five lowered the weekly walk-forward (IC 0.025 -> 0.022,
# t 3.2 -> 2.8, net Sharpe 1.0 -> 0.83): at a one-week horizon they added noise.
CONTEXT_FEATURES = ["bm", "roa", "asset_growth", "accruals", "gpa", "cfp", "sp", "value_ind",
                    # Standardized earnings surprise from SEC facts: rejected
                    # by trial G (9 Oct 2026), shown for context.
                    "sue",
                    # Net share issuance, computed for trial M2 (22 Oct 2026); board context
                    # until a walk-forward adopts it.
                    "issuance"]
# Features from sources that do not cover every name: a name without one is
# ranked at the middle rather than dropped (ETFs and recent listings have no
# short-interest history, and dropping them would also drop every name on a
# day before the source began).
OPTIONAL_FEATURES = {"dtc", "ear", "ind_mom", "ind_rev_21", "mom_on_12_1", "id_21", "quality_ind", "ey",
                     "beta_x_trend", "ratebeta_x_rates", "mom_x_vol", "beta_x_credit", "svr_20", "svr_chg",
                     "opt_cp_spread", "opt_skew", "opt_log_pcr", "opt_vol_spread", "opt_iv_change"}
# Models that split on a missing value themselves. For them an absent optional
# feature stays missing instead of being set to the middle rank: "no
# short-interest history" is not the same fact as "average short interest", and
# a tree can learn the difference. Ridge and the rank average cannot take a
# missing value, so they keep the neutral fill.
NAN_NATIVE_MODELS = {"gbm", "lgbm_rank", "lgbm_rank2", "avg2"}
RANK_LABELS = 5                 # the ranker's relevance grades: the day's quintiles
RANK_TRUNCATION = 5000          # above any day's ranked names, so every pair counts
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

# Tradable names only: a $3M consolidated dollar-volume floor. Microcaps below it
# are where long-short anomalies look best on paper and cannot be traded. The
# floor is scaled by the daily bars' feed (shared.utils.bar_feed): consolidated
# from 8 Oct 2026, IEX (~2.5% of the tape) before.
MIN_PRICE = 3.0
MIN_CONSOLIDATED_DOLLAR_VOLUME = 3e6
MIN_FEED_DOLLAR_VOLUME = MIN_CONSOLIDATED_DOLLAR_VOLUME * DAILY_BAR_VOLUME_SHARE
# An institutional universe: the most liquid stocks by consolidated dollar volume
# each day, from data up to that day. Measured 9 Oct 2026 on consolidated
# history: the weekly book earned Sharpe 4.3 across every name above the floor
# and ~1.2 inside the top thousand with real costs -- the rest lived in small,
# hard-to-borrow names whose spreads and borrow a desk cannot get.
TOP_LIQUID_NAMES = int(os.getenv("FACTOR_TOP_LIQUID_NAMES", "1000"))


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


def _expanding_z(x: np.ndarray, min_n: int = 252) -> np.ndarray:
    """Each value against the mean and spread of the series up to and including it."""
    out = np.full(x.shape, np.nan, dtype=np.float32)
    ok = np.isfinite(x)
    xs = np.where(ok, x, 0.0)
    n = np.cumsum(ok)
    s1, s2 = np.cumsum(xs), np.cumsum(xs * xs)
    with np.errstate(all="ignore"):
        mean = s1 / n
        sd = np.sqrt(np.maximum(s2 / n - mean * mean, 0.0))
        z = (x - mean) / sd
    keep = ok & (n >= min_n) & (sd > 0)
    out[keep] = z[keep]
    return out


def _rolling_beta(r: np.ndarray, f: np.ndarray, w: int = 126) -> np.ndarray:
    """[T, N] beta of each name's daily returns to one factor's, trailing w rows."""
    out = np.full(r.shape, np.nan, dtype=np.float32)
    for i in range(w, r.shape[0]):
        fw = f[i - w + 1: i + 1]
        okf = np.isfinite(fw)
        if okf.sum() < w // 2:
            continue
        fc = np.where(okf, fw - np.nanmean(fw), 0.0)
        var = np.mean(fc[okf] ** 2)
        if not var:
            continue
        rw = r[i - w + 1: i + 1]
        with np.errstate(all="ignore"):
            out[i] = np.nanmean((rw - np.nanmean(rw, axis=0)) * fc[:, None], axis=0) / var
    return out


def _window_sum(x: np.ndarray, w: int, min_frac: float = 0.8) -> np.ndarray:
    """Trailing sums over w rows, NaN where fewer than min_frac of them are known."""
    ok = np.isfinite(x)
    cs = np.cumsum(np.where(ok, x, 0.0), axis=0, dtype=np.float64)
    cn = np.cumsum(ok, axis=0)
    out = np.full(x.shape, np.nan, dtype=np.float32)
    if x.shape[0] < w:
        return out
    tot = cs[w - 1:] - np.vstack([np.zeros((1, x.shape[1])), cs[:-w]])
    cnt = cn[w - 1:] - np.vstack([np.zeros((1, x.shape[1]), dtype=cn.dtype), cn[:-w]])
    out[w - 1:] = np.where(cnt >= min_frac * w, tot, np.nan)
    return out


def _trailing_mean(x: np.ndarray, w: int) -> np.ndarray:
    """Mean of the known values among the last w rows, NaN when none is."""
    ok = np.isfinite(x)
    cs = np.cumsum(np.where(ok, x, 0.0), axis=0, dtype=np.float64)
    cn = np.cumsum(ok, axis=0)
    tot, cnt = cs.copy(), cn.copy()
    tot[w:] -= cs[:-w]
    cnt[w:] -= cn[:-w]
    with np.errstate(all="ignore"):
        return np.where(cnt > 0, tot / np.maximum(cnt, 1), np.nan).astype(np.float32)


def _ffill_rows(x: np.ndarray, limit: int) -> np.ndarray:
    """Each column's last known value carried forward up to `limit` rows."""
    out = np.full(x.shape, np.nan, dtype=np.float32)
    last = np.full(x.shape[1], np.nan, dtype=np.float32)
    age = np.full(x.shape[1], limit + 1)
    for i in range(x.shape[0]):
        ok = np.isfinite(x[i])
        last = np.where(ok, x[i], last)
        age = np.where(ok, 0, age + 1)
        out[i] = np.where(age <= limit, last, np.nan)
    return out


def option_features(close: np.ndarray, atm_iv: Optional[np.ndarray], skew: Optional[np.ndarray],
                    cp_spread: Optional[np.ndarray], log_pcr: Optional[np.ndarray]) -> Dict[str, np.ndarray]:
    """_option_features_dense on the columns with any option reading only
    (about 500 of 5,600), the rest left missing: the nightly run has little
    memory to spare for full-width temporaries."""
    given = [x for x in (atm_iv, skew, cp_spread, log_pcr) if x is not None]
    nan = np.full(close.shape, np.nan, dtype=np.float32)
    names = ("opt_cp_spread", "opt_skew", "opt_log_pcr", "opt_vol_spread", "opt_iv_change")
    if not given:
        return {k: nan.copy() for k in names}
    has = np.zeros(close.shape[1], dtype=bool)
    for x in given:
        has |= np.isfinite(x).any(axis=0)
    cols = np.flatnonzero(has)

    def sub(x: Optional[np.ndarray]) -> Optional[np.ndarray]:
        return x[:, cols] if x is not None else None

    part = _option_features_dense(close[:, cols], sub(atm_iv), sub(skew), sub(cp_spread), sub(log_pcr))
    out: Dict[str, np.ndarray] = {}
    for k in names:
        full = nan.copy()
        full[:, cols] = part[k]
        out[k] = full
    return out


def _option_features_dense(close: np.ndarray, atm_iv: Optional[np.ndarray], skew: Optional[np.ndarray],
                           cp_spread: Optional[np.ndarray], log_pcr: Optional[np.ndarray]) -> Dict[str, np.ndarray]:
    """Trial L's five option-implied features from [T, N] daily measures
    (NaN where a name has no options that day): the spread, skew and volume
    ratio each over their last 5 sessions; 21-day realised volatility less
    implied; implied now less 21 sessions ago. Implied is carried over up to
    3 sessions without a reading."""
    nan = np.full(close.shape, np.nan, dtype=np.float32)
    out = {"opt_cp_spread": _trailing_mean(cp_spread, 5) if cp_spread is not None else nan,
           "opt_skew": _trailing_mean(skew, 5) if skew is not None else nan,
           "opt_log_pcr": _trailing_mean(log_pcr, 5) if log_pcr is not None else nan}
    if atm_iv is None:
        out.update(opt_vol_spread=nan, opt_iv_change=nan)
        return out
    atm = _ffill_rows(atm_iv, 3)
    with np.errstate(all="ignore"):
        lr = np.vstack([np.full((1, close.shape[1]), np.nan), np.diff(np.log(close), axis=0)])
        ok = np.isfinite(lr)
        s1 = np.cumsum(np.where(ok, lr, 0.0), axis=0)
        s2 = np.cumsum(np.where(ok, lr * lr, 0.0), axis=0)
        n = np.cumsum(ok, axis=0).astype(np.float64)
        w = 21
        a1, a2, an = s1.copy(), s2.copy(), n.copy()
        a1[w:] -= s1[:-w]
        a2[w:] -= s2[:-w]
        an[w:] -= n[:-w]
        var = np.where(an > 1, a2 / np.maximum(an, 1) - (a1 / np.maximum(an, 1)) ** 2, np.nan)
        rv = np.sqrt(np.clip(var, 0, None)) * math.sqrt(252)
        rv[:w] = np.nan
        prev = np.full(atm.shape, np.nan, dtype=np.float32)
        prev[w:] = atm[:-w]
        out.update(opt_vol_spread=(rv - atm).astype(np.float32), opt_iv_change=(atm - prev).astype(np.float32))
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
        # Fundamentals from SEC XBRL, each visible 90 days after its period ends
        # (services/agents/factor_engine.py builds the point-in-time arrays):
        # value (Fama & French 1992), earnings yield (Basu 1977), profitability
        # (Fama & French 2015; Novy-Marx 2013), asset growth (Cooper, Gulen &
        # Schill 2008) and accruals (Sloan 1996). Market value uses split-
        # corrected shares: prices are adjusted for later splits, reported
        # share counts are not.
        x = panel.extra
        nan = np.full(c.shape, np.nan, dtype=c.dtype)
        eq, assets, ni, cfo, sh = (x.get(k) for k in ("equity", "assets", "net_income", "cfo", "shares_adj"))
        mcap = c * sh if sh is not None else None
        f["bm"] = np.where(mcap > 0, eq / mcap, np.nan) if (eq is not None and mcap is not None) else nan
        f["ey"] = np.where(mcap > 0, ni / mcap, np.nan) if (ni is not None and mcap is not None) else nan
        f["roa"] = np.where(assets > 0, ni / assets, np.nan) if (ni is not None and assets is not None) else nan
        f["accruals"] = (np.where(assets > 0, (ni - cfo) / assets, np.nan)
                         if (ni is not None and cfo is not None and assets is not None) else nan)
        ag = x.get("asset_growth")
        f["asset_growth"] = ag if ag is not None else nan
        # Net share issuance (Pontiff & Woodgate 2008): the log change in
        # split-corrected shares over a year. Firms that issue go on to trail;
        # firms that buy back go on to lead -- one of the most persistent marks
        # of an overpriced stock (Stambaugh, Yu & Yuan's mispricing score).
        if sh is not None and sh.shape[0] > 252:
            with np.errstate(all="ignore"):
                prev = np.vstack([np.full((252, sh.shape[1]), np.nan, dtype=sh.dtype), sh[:-252]])
                f["issuance"] = np.where((sh > 0) & (prev > 0), np.log(sh / prev), np.nan).astype(c.dtype)
        else:
            f["issuance"] = nan
        # Overnight and intraday returns (Lou, Polk & Skouras 2019): momentum is
        # earned almost entirely overnight, other effects intraday. Needs the
        # consolidated opens loaded from 8 Oct 2026; IEX opens could not carry it.
        # Context (adopted only through a pre-registered trial): the market's
        # trend, volatility, rates and credit, each scored against its own past,
        # interacted with the stock traits they are known to switch -- beta in
        # a rally, duration when yields move, momentum in high volatility
        # (Daniel & Moskowitz 2016), beta when credit widens.
        tlt, hyg = x.get("ctx_TLT"), x.get("ctx_HYG")
        if market in panel.tickers:
            mc = c[:, panel.tickers.index(market)].astype(np.float64)
            with np.errstate(all="ignore"):
                mr = np.diff(np.log(mc), prepend=np.nan)
                trend_z = _expanding_z(np.concatenate([np.full(126, np.nan), np.log(mc[126:] / mc[:-126])]))
                vol = np.array([np.nanstd(mr[max(0, i - 20): i + 1]) if i >= 20 else np.nan for i in range(len(mr))])
            vol_z = _expanding_z(vol)
            beta_rank = _row_rank(f["beta_126"])
            f["beta_x_trend"] = (beta_rank * trend_z[:, None]).astype(np.float32)
            f["mom_x_vol"] = (_row_rank(f["mom_12_1"]) * -vol_z[:, None]).astype(np.float32)
            if hyg is not None:
                with np.errstate(all="ignore"):
                    hyg_z = _expanding_z(np.concatenate([np.full(63, np.nan), np.log(hyg[63:] / hyg[:-63])]))
                f["beta_x_credit"] = (beta_rank * hyg_z[:, None]).astype(np.float32)
            else:
                f["beta_x_credit"] = nan
            if tlt is not None:
                with np.errstate(all="ignore"):
                    tr = np.diff(np.log(tlt.astype(np.float64)), prepend=np.nan)
                    tlt_z = _expanding_z(np.concatenate([np.full(63, np.nan), np.log(tlt[63:] / tlt[:-63])]))
                f["ratebeta_x_rates"] = (_row_rank(_rolling_beta(r, tr)) * tlt_z[:, None]).astype(np.float32)
            else:
                f["ratebeta_x_rates"] = nan
            del beta_rank
        else:
            for k in ("beta_x_trend", "mom_x_vol", "beta_x_credit", "ratebeta_x_rates"):
                f[k] = nan
        op = x.get("open")
        if op is not None:
            with np.errstate(all="ignore"):
                on = np.vstack([np.full((1, c.shape[1]), np.nan, dtype=c.dtype), np.log(op[1:] / c[:-1])])
                intra = np.log(c / op)
            mom_on = _window_sum(on, 231)
            f["mom_on_12_1"] = np.vstack([np.full((21, c.shape[1]), np.nan, dtype=np.float32), mom_on[:-21]])
            f["id_21"] = _window_sum(intra, 21)
            del on, intra, mom_on
        else:
            f["mom_on_12_1"] = nan
            f["id_21"] = nan
        # Gross profitability (Novy-Marx 2013), cash flow and sales to price
        # (Lakonishok, Shleifer & Vishny 1994), then the two composites practice
        # trades rather than any one ratio: value (book, earnings, cash flow and
        # sales yields) and quality (gross profitability, ROA, cash flow to
        # assets, low accruals), each ranked against its own industry (Asness,
        # Porter & Stevens 2000) so a bank's book is not compared with a
        # software company's.
        rev, gp = x.get("revenue"), x.get("gross_profit")
        f["gpa"] = np.where(assets > 0, gp / assets, np.nan) if (gp is not None and assets is not None) else nan
        f["cfp"] = np.where(mcap > 0, cfo / mcap, np.nan) if (cfo is not None and mcap is not None) else nan
        f["sp"] = np.where(mcap > 0, rev / mcap, np.nan) if (rev is not None and mcap is not None) else nan
        cfoa = (np.where(assets > 0, cfo / assets, np.nan).astype(c.dtype)
                if (cfo is not None and assets is not None) else nan)
        f["value_ind"] = _industry_relative(_composite([f["bm"], f["ey"], f["cfp"], f["sp"]], [1, 1, 1, 1]), ind)
        f["quality_ind"] = _industry_relative(
            _composite([f["gpa"], f["roa"], cfoa, f["accruals"]], [1, 1, 1, -1]), ind)
        # Standardized unexpected earnings (Bernard & Thomas 1989), from
        # filing-dated quarterly net income (shared/utils/fundamentals_pit.py).
        su = x.get("sue")
        f["sue"] = su if su is not None else nan
        # Off-exchange short volume share (Diether, Lee & Werner 2009): the
        # 20-day share of FINRA-reported volume sold short, and its change
        # against the prior 120 days. Heavy shorting preceded underperformance.
        sv, ov = x.get("short_volume"), x.get("offex_volume")
        if sv is not None and ov is not None:
            s20, o20 = _window_sum(sv, 20, 0.75), _window_sum(ov, 20, 0.75)
            s120, o120 = _window_sum(sv, 120, 0.67), _window_sum(ov, 120, 0.67)
            with np.errstate(all="ignore"):
                f["svr_20"] = np.where(o20 > 0, s20 / o20, np.nan).astype(np.float32)
                f["svr_chg"] = (f["svr_20"] - np.where(o120 > 0, s120 / o120, np.nan)).astype(np.float32)
            del s20, o20, s120, o120
        else:
            f["svr_20"] = nan
            f["svr_chg"] = nan
        f.update(option_features(c, x.get("opt_atm_iv"), x.get("opt_iv_skew"), x.get("opt_cp_iv_spread"),
                                 x.get("opt_log_pcr")))
    return f


def _row_rank(x: np.ndarray) -> np.ndarray:
    out = np.full(x.shape, np.nan, dtype=np.float32)
    for i in range(x.shape[0]):
        out[i] = _rank(x[i])
    return out


def _composite(parts: List[np.ndarray], signs: List[int]) -> np.ndarray:
    """Mean of the parts' cross-sectional ranks, signed; NaN where none is known.
    Accumulated one rank at a time: the model's container has little headroom."""
    total = np.zeros(parts[0].shape, dtype=np.float32)
    count = np.zeros(parts[0].shape, dtype=np.float32)
    for part, sign in zip(parts, signs):
        r = _row_rank(part)
        ok = np.isfinite(r)
        total += np.where(ok, sign * r, 0.0).astype(np.float32)
        count += ok
        del r, ok
    with np.errstate(all="ignore"):
        return np.where(count > 0, total / count, np.nan).astype(np.float32)


def _industry_relative(x: np.ndarray, ind: Optional[np.ndarray]) -> np.ndarray:
    """x less its industry peers' mean; x itself where the industry is unknown."""
    if ind is None:
        return x
    peers = _peer_mean(x, ind)
    return np.where(np.isfinite(peers), x - peers, x).astype(np.float32)


def forward_excess(panel: Panel, horizon: int = HORIZON, market: str = "SPY") -> np.ndarray:
    """Next-`horizon`-day log return net of the market's, [T, N]; NaN at the end."""
    c = panel.close
    with np.errstate(all="ignore"):
        fwd = np.vstack([np.log(c[horizon:] / c[:-horizon]), np.full((horizon, c.shape[1]), np.nan, dtype=c.dtype)])
    if market in panel.tickers:
        fwd = fwd - fwd[:, [panel.tickers.index(market)]]
    return fwd


def forward_beta_adjusted(panel: Panel, horizon: int, beta: np.ndarray, market: str = "SPY") -> np.ndarray:
    """Trial F's target: the next-`horizon`-day log return net of beta times the
    market's, [T, N]. Beta is each name's trailing 126-day beta, known at the row,
    so the target looks no further ahead than the plain excess return does."""
    c = panel.close
    with np.errstate(all="ignore"):
        fwd = np.vstack([np.log(c[horizon:] / c[:-horizon]), np.full((horizon, c.shape[1]), np.nan, dtype=c.dtype)])
    if market not in panel.tickers:
        return fwd
    m = fwd[:, [panel.tickers.index(market)]]
    b = np.where(np.isfinite(beta), beta, 1.0)
    return fwd - b * m


TARGET_MODES = ("excess", "beta_adj", "industry")


def training_target(panel: Panel, horizon: int, feats: Dict[str, np.ndarray], mode: str = "excess") -> np.ndarray:
    """What a model learns to rank, [T, N]:

      excess     the next-`horizon`-day return net of SPY (the live target)
      beta_adj   net of beta times SPY: a high-beta name no longer looks like a
                 winner merely because the market rose (trial F)
      industry   the beta-adjusted return less its industry peers': the part of
                 a stock's move that neither the market nor its industry explains

    Evaluation never changes with it: every book is scored on forward_excess,
    so targets are compared on the same outcome."""
    if mode == "excess" or "beta_126" not in feats:
        return forward_excess(panel, horizon)
    adj = forward_beta_adjusted(panel, horizon, feats["beta_126"])
    if mode == "industry":
        return _industry_relative(adj, panel.extra.get("industry"))
    return adj


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
    if TOP_LIQUID_NAMES > 0:
        fund = panel.extra.get("not_a_stock")
        ranked = np.where(mask & (~fund if fund is not None else True), dv, np.nan)
        for i in range(ranked.shape[0]):
            row = ranked[i]
            ok = np.isfinite(row)
            if ok.sum() > TOP_LIQUID_NAMES:
                cut = np.partition(row[ok], -TOP_LIQUID_NAMES)[-TOP_LIQUID_NAMES]
                mask[i] &= np.where(np.isfinite(row), row >= cut, True)
    panel.extra["_tradable"] = mask
    return mask


def tradable_subset(panel: Panel, keep_always: Sequence[str] = ("SPY",)) -> Panel:
    """The panel cut to the columns that are tradable on at least one day, plus
    `keep_always`. Every candidate model ranks only tradable names, so its
    walk-forward reads the same rows; the cut takes the panel from ~5,200
    columns to ~2,200 and the feature set's memory with it (the weekly
    portfolio lab peaked at 2,228 MB of the factor model's 2,432 MB on the full
    panel, 17 Oct 2026). Features averaged over industry peers see only
    tradable peers, a small difference the lab accepts."""
    mask = tradable(panel)
    keep = np.flatnonzero(mask.any(axis=0) | np.isin(np.array(panel.tickers), list(keep_always)))
    n = len(panel.tickers)

    def cut(v: Any) -> Any:
        if isinstance(v, np.ndarray) and v.ndim == 2 and v.shape[1] == n:
            return v[:, keep]
        if isinstance(v, np.ndarray) and v.ndim == 1 and v.shape[0] == n:
            return v[keep]
        return v
    return Panel(dates=panel.dates, tickers=[panel.tickers[i] for i in keep], close=panel.close[:, keep],
                 high=panel.high[:, keep], volume=panel.volume[:, keep],
                 extra={k: cut(v) for k, v in panel.extra.items() if not k.startswith("_")})


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


def _cross_section(feats: Dict[str, np.ndarray], i: int, hist_ok: np.ndarray, nan_ok: bool = False) -> np.ndarray:
    """Row i's features, ranked across eligible names. An absent optional feature
    is neutral (0.0), or left missing for a model that handles it (`nan_ok`)."""
    cols = []
    for name in FEATURES:
        col = _rank(np.where(hist_ok, feats[name][i], np.nan))
        if name in OPTIONAL_FEATURES and not nan_ok:
            col = np.where(hist_ok & ~np.isfinite(col), 0.0, col)
        cols.append(col)
    return np.column_stack(cols)


_REQUIRED = None


def _complete(X: np.ndarray) -> np.ndarray:
    """Rows with every required feature; optional ones may be missing."""
    global _REQUIRED
    if _REQUIRED is None or len(_REQUIRED) != X.shape[1]:
        _REQUIRED = np.array([f not in OPTIONAL_FEATURES for f in FEATURES])
    return np.all(np.isfinite(X[:, _REQUIRED]), axis=1)


def design(panel: Panel, feats: Dict[str, np.ndarray], target: np.ndarray, rows: Sequence[int],
           return_cols: bool = False, nan_ok: bool = False):
    """Rank-normalised (X, y, raw_y, date_index) over the given date rows, and
    with return_cols the panel column (ticker) of each row as a fifth array."""
    xs, ys, raws, idx, cols = [], [], [], [], []
    for i in rows:
        hist_ok = _eligible(panel, i)
        X = _cross_section(feats, i, hist_ok, nan_ok)
        ok = hist_ok & _complete(X)
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


def _shortable_pred(panel: Panel, pred: np.ndarray, cols: np.ndarray) -> np.ndarray:
    """pred with names that cannot be borrowed pushed out of the short book."""
    ok = panel.extra.get("borrowable")
    if ok is None:
        return pred
    return np.where(ok[cols], pred, np.inf)


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

        # The whole day, every quintile weighted alike. LightGBM's defaults
        # optimise only the top 30 of each day's ~1,000 names (truncation 30,
        # gains 2^label - 1), so the ranker learnt to find a few winners and
        # never to order the short side: walk-forward IC 0.008 monthly, -0.001
        # weekly. With the whole day and linear gains (16 Oct 2026, same panel):
        # 0.062 (t 2.39) and 0.030 (t 1.89).
        return LGBMRanker(
            objective="lambdarank", n_estimators=300, learning_rate=0.05, num_leaves=15,
            min_child_samples=200, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
            reg_lambda=1.0, random_state=7, verbose=-1,
            lambdarank_truncation_level=RANK_TRUNCATION, label_gain=list(range(RANK_LABELS)),
        )
    if model == "avg":
        return _RankAverage()
    if model == "avg2":
        # The two learned models that lead every walk-forward, averaged (trial).
        return _RankAverage(["lgbm_rank", "gbm"] if lightgbm_available() else ["gbm"])
    if model == "lgbm_rank2":
        return _TwoSidedRanker()
    return None   # "momentum": no fit, the mom_12_1 rank itself


class _RankAverage:
    """Every other candidate, fitted on the same rows, their cross-sectional
    ranks averaged with equal weight (Rapach, Strauss & Zhou 2010: simple
    combinations beat the single best forecaster out of sample, which picking
    one model per horizon by its own backtest cannot)."""

    def __init__(self, names: Optional[List[str]] = None) -> None:
        names = names or (["lgbm_rank"] if lightgbm_available() else []) + ["gbm", "ridge", "momentum"]
        self.members = [(n, _make(n)) for n in names]

    def fit(self, X: np.ndarray, y: np.ndarray, d: np.ndarray) -> None:
        for name, est in self.members:
            if est is not None:
                _fit(name, est, X, y, d)

    def predict(self, X: np.ndarray) -> np.ndarray:
        ranks = [_rank(_predict(name, est, X)) for name, est in self.members]
        return np.nanmean(np.vstack(ranks), axis=0)


class _TwoSidedRanker:
    """Two LambdaRank models, one ranking the day's winners and one its losers,
    their ranks averaged (trial).

    LambdaRank optimises NDCG, which weights the head of each day's list: one
    ranker learns to order the top with care and the bottom hardly at all. The
    book's money sits mostly in the bottom (the short leg earned 195 bps a month
    against the long leg's -83 after beta, 2023-26), so a second ranker is fitted
    on the reversed order, and the bottom becomes its head."""

    def __init__(self) -> None:
        self.up = _make("lgbm_rank")
        self.down = _make("lgbm_rank")

    def fit(self, X: np.ndarray, y: np.ndarray, d: np.ndarray) -> None:
        _fit("lgbm_rank", self.up, X, y, d)
        _fit("lgbm_rank", self.down, X, -y, d)

    def predict(self, X: np.ndarray) -> np.ndarray:
        return (_rank(self.up.predict(X)) - _rank(self.down.predict(X))) / 2


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
        labels = np.clip(((y[order] + 0.5) * RANK_LABELS).astype(int), 0, RANK_LABELS - 1)   # quintile 0..4
        est.fit(X[order], labels, group=counts)
    elif model_name in ("avg", "avg2", "lgbm_rank2"):
        est.fit(X, y, d)
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
    # What the long-short book leans on, out of sample: beta, size and momentum
    # tilts (long minus short), the largest single-industry share of each leg,
    # and the gross spread when predictions are demeaned within industry.
    # Diagnostics, not a candidate: they decide nothing and add no trial.
    exposures: Dict[str, Any] = field(default_factory=dict)


def _predict(model_name: str, est, X: np.ndarray) -> np.ndarray:
    if model_name == "momentum":
        return X[:, FEATURES.index("mom_12_1")]
    return est.predict(X)


def _record_exposures(expo: Dict[str, List[float]], panel: Panel, feats: Dict[str, np.ndarray], i: int,
                      cols: np.ndarray, pred: np.ndarray, raw: np.ndarray, long_pos: List[int],
                      short_pos: List[int]) -> None:
    """One period's tilts of the book, and its industry-neutral gross spread."""
    lc, sc = cols[long_pos], cols[short_pos]
    with np.errstate(all="ignore"):
        for name, key in (("beta_126", "net_beta"), ("dvol_21", "size_tilt_log_dollar_volume"),
                          ("mom_12_1", "momentum_tilt")):
            f = feats.get(name)
            if f is None:
                continue
            a, b = np.nanmean(f[i, lc]), np.nanmean(f[i, sc])
            if np.isfinite(a) and np.isfinite(b):
                expo.setdefault(key, []).append(float(a - b))
    ind = panel.extra.get("industry")
    if ind is None:
        return
    groups = ind[cols]
    for leg, pos in (("long_top_industry_share", long_pos), ("short_top_industry_share", short_pos)):
        g = groups[pos]
        g = g[g >= 0]
        if len(g):
            expo.setdefault(leg, []).append(float(np.bincount(g).max() / len(pos)))
    neutral = pred.astype(float).copy()
    for gval in np.unique(groups):
        if gval < 0:
            continue
        mask = groups == gval
        if mask.sum() >= 3:
            neutral[mask] -= neutral[mask].mean()
    q = max(1, len(neutral) // 5)
    order = np.argsort(neutral)
    expo.setdefault("industry_neutral_gross_bps", []).append(
        float(np.mean(raw[order[-q:]]) - np.mean(raw[order[:q]])) * 1e4)
    expo.setdefault("gross_bps", []).append(float(np.mean(raw[long_pos]) - np.mean(raw[short_pos])) * 1e4)
    # Size-neutral: predictions demeaned within dollar-volume quintiles, so the
    # spread cannot come from simply being long big, liquid names and short
    # small, thin ones.
    dv = feats.get("dvol_21")
    if dv is not None:
        size = dv[i, cols]
        ok = np.isfinite(size)
        if ok.sum() >= 25:
            edges = np.nanquantile(size[ok], [0.2, 0.4, 0.6, 0.8])
            bucket = np.where(ok, np.searchsorted(edges, np.where(ok, size, 0.0)), -1)
            sn = pred.astype(float).copy()
            for b in range(5):
                mask = bucket == b
                if mask.sum() >= 3:
                    sn[mask] -= sn[mask].mean()
            order = np.argsort(sn)
            expo.setdefault("size_neutral_gross_bps", []).append(
                float(np.mean(raw[order[-q:]]) - np.mean(raw[order[:q]])) * 1e4)


def _spec(horizon: int, spec: Optional[HorizonSpec]) -> HorizonSpec:
    if spec is not None:
        return spec
    return HorizonSpec(f"{horizon}d", horizon, SAMPLE_EVERY, SAMPLE_EVERY, EMBARGO, MIN_TRAIN_DATES)


def walk_forward(panel: Panel, model_name: str, horizon: int = HORIZON,
                 spec: Optional[HorizonSpec] = None,
                 feats: Optional[Dict[str, np.ndarray]] = None,
                 collect: Optional[List[Tuple[int, int, int, float, float]]] = None,
                 collect_preds: Optional[List[Tuple[int, np.ndarray, np.ndarray, np.ndarray]]] = None,
                 target_mode: str = "excess") -> ModelReport:
    """Out-of-sample evaluation, refit each fold on purged history.

    `target_mode` (training_target) changes what each fold is trained on; the
    out-of-sample book is always scored on the return net of SPY.

    `collect`, when given, receives every out-of-sample position the book took:
    (date row, panel column, side, prediction rank, forward excess return) --
    the primary signals a sizing layer (shared/utils/meta_label.py) learns from."""
    sp = _spec(horizon, spec)
    horizon = sp.horizon
    feats = feats if feats is not None else features(panel)
    target = forward_excess(panel, horizon)
    train_target = target if target_mode == "excess" else training_target(panel, horizon, feats, target_mode)
    T = len(panel.dates)
    train_sampled = list(range(MIN_HISTORY, T - horizon, sp.train_every))
    sampled = list(range(MIN_HISTORY, T - horizon, sp.test_every))
    if len(train_sampled) < sp.min_train_dates + 4:
        return ModelReport(model_name, 0, 0, None, None, None, None, None, None, None, False,
                           ["not enough history to validate"])
    fold_starts = list(range(train_sampled[sp.min_train_dates], T - horizon, FOLD_DAYS))
    # The market's own forward move, for the long leg's alpha after beta: a
    # low-beta book trails SPY in a rally while earning more than its risk.
    mkt_fwd = None
    if "SPY" in panel.tickers:
        mc = panel.close[:, panel.tickers.index("SPY")]
        with np.errstate(all="ignore"):
            mkt_fwd = np.concatenate([np.log(mc[horizon:] / mc[:-horizon]), np.full(horizon, np.nan)])
    beta_f = feats.get("beta_126")
    ics: List[float] = []
    expo: Dict[str, List[float]] = {}
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
            X, y, _r, _d = design(panel, feats, train_target, train_rows, nan_ok=model_name in NAN_NATIVE_MODELS)
            if len(y) < 1000:
                continue
            _fit(model_name, est, X, y, _d)
        this_fold: List[float] = []
        for i in test_rows:
            X, y, raw, _d, cols = design(panel, feats, target, [i], return_cols=True,
                                         nan_ok=model_name in NAN_NATIVE_MODELS)
            if len(y) < MIN_NAMES:
                continue
            pred = _predict(model_name, est, X)
            if collect_preds is not None:
                # Every eligible name's prediction and outcome, for index-aware
                # portfolios (enhanced indexing) built outside the book rule.
                collect_preds.append((i, cols.copy(), np.asarray(pred, dtype=np.float32), raw.astype(np.float32)))
            ic = _spearman(pred, raw)
            if ic is None:
                continue
            ics.append(ic)
            this_fold.append(ic)
            q = max(1, len(pred) // 5)
            long_pos = _buffered_book(pred, cols, prev_long, 1)
            # Shorts only where the broker will lend (panel.extra["borrowable"]):
            # on consolidated history most of the short leg's paper profit sat
            # in names marked hard to borrow.
            short_pos = _buffered_book(_shortable_pred(panel, pred, cols), cols, prev_short, -1)
            ls.append(float(np.mean(raw[long_pos]) - np.mean(raw[short_pos])) * 1e4)
            expo.setdefault("long_leg_bps", []).append(float(np.mean(raw[long_pos])) * 1e4)
            # Against the average eligible stock as well as SPY: 2021-26 the
            # average stock in the top thousand trailed SPY by ~30 bps a week
            # (a few megacaps carried the index), so a long book can pick well
            # among stocks and still trail the index.
            univ = float(np.mean(raw))
            expo.setdefault("long_vs_universe_bps", []).append((float(np.mean(raw[long_pos])) - univ) * 1e4)
            expo.setdefault("short_vs_universe_bps", []).append((univ - float(np.mean(raw[short_pos]))) * 1e4)
            if mkt_fwd is not None and beta_f is not None and np.isfinite(mkt_fwd[i]):
                b = np.nan_to_num(beta_f[i, cols[long_pos]], nan=1.0)
                expo.setdefault("long_beta", []).append(float(np.mean(b)))
                expo.setdefault("long_alpha_bps", []).append(
                    float(np.mean(raw[long_pos] + (1.0 - b) * mkt_fwd[i])) * 1e4)
            expo.setdefault("short_leg_bps", []).append(-float(np.mean(raw[short_pos])) * 1e4)
            if collect is not None:
                pr = _rank(pred)
                collect.extend((i, int(cols[k]), 1, float(pr[k]), float(raw[k])) for k in long_pos)
                collect.extend((i, int(cols[k]), -1, float(pr[k]), float(raw[k])) for k in short_pos)
            _record_exposures(expo, panel, feats, i, cols, pred, raw, long_pos, short_pos)
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
        exposures={k: round(float(np.mean(v)), 4) for k, v in expo.items() if v},
    )


def score_latest(panel: Panel, model_name: str, horizon: int = HORIZON,
                 spec: Optional[HorizonSpec] = None,
                 feats: Optional[Dict[str, np.ndarray]] = None,
                 target_mode: str = "excess") -> Dict[str, Dict[str, Any]]:
    """Fit on all purged history and rank the newest cross-section.

    Returns {ticker: {score, quintile, features: {name: rank}}}; quintile 5 is
    the most favoured. Ranks, not return forecasts: the model learned an order."""
    sp = _spec(horizon, spec)
    horizon = sp.horizon
    feats = feats if feats is not None else features(panel)
    target = training_target(panel, horizon, feats, target_mode)
    T = len(panel.dates)
    last = T - 1
    train_rows = [i for i in range(MIN_HISTORY, T - horizon, sp.train_every) if i + horizon + sp.embargo <= last]
    est = _make(model_name)
    if est is not None:
        X, y, _r, _d = design(panel, feats, target, train_rows, nan_ok=model_name in NAN_NATIVE_MODELS)
        if len(y) < 1000:
            return {}
        _fit(model_name, est, X, y, _d)
    hist_ok = _eligible(panel, last)
    X = _cross_section(feats, last, hist_ok, nan_ok=model_name in NAN_NATIVE_MODELS)
    ok = hist_ok & _complete(X)
    if ok.sum() < MIN_NAMES:
        return {}
    pred = _predict(model_name, est, X[ok])
    cols = np.flatnonzero(ok)
    names = [panel.tickers[j] for j in cols]
    order = pred.argsort().argsort()
    n = len(pred)
    context = {c: _rank(np.where(hist_ok, feats[c][last], np.nan)) for c in CONTEXT_FEATURES if c in feats}
    # What moved each name's score, for a linear model: coefficient x the
    # name's standardized rank, exact rather than estimated. Tree models get none.
    coef = getattr(est, "coef_", None)
    contrib = (X[ok] * np.asarray(coef, dtype=np.float64)) if coef is not None and len(coef) == X.shape[1] else None
    out = {}
    for k, t in enumerate(names):
        pct = order[k] / max(1, n - 1)
        ranks = {f: round(float(X[ok][k][j]), 3) for j, f in enumerate(FEATURES)}
        drivers = None
        if contrib is not None:
            order_k = np.argsort(-np.abs(contrib[k]))[:6]
            drivers = [[FEATURES[j], round(float(contrib[k][j]), 5), round(float(X[ok][k][j]), 3)] for j in order_k]
        ranks.update({c: round(float(v[cols[k]]), 3) for c, v in context.items() if np.isfinite(v[cols[k]])})
        out[t] = {
            "score": round(float(pred[k]), 5),
            "percentile": round(float(pct), 4),
            "quintile": int(min(5, 1 + pct * 5)),
            "features": ranks,
            "drivers": drivers,
        }
    return out


__all__ = [
    "CAVEATS", "CONTEXT_FEATURES", "DEFAULT_HORIZONS", "FEATURES", "HORIZON_SPECS", "HorizonSpec", "OPTIONAL_FEATURES", "tradable", "tradable_subset", "announcement_returns", "point_in_time", "GATE_MIN_IC_T", "HORIZON", "ModelReport", "Panel", "build_panel",
    "features", "forward_beta_adjusted", "forward_excess", "score_latest", "training_target", "walk_forward",
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


def _support(chosen: Optional[ModelReport]) -> Optional[str]:
    """The idea layer's tier for a horizon's chosen model: validated, supported or None."""
    from shared.utils.trade_plan import model_support
    return model_support(chosen.__dict__) if chosen is not None else None


def rank_horizons(per_h: Dict[str, Dict[str, Any]]) -> List[str]:
    """Horizons best first: evidence tier, then net Sharpe.

    Ranked on Sharpe alone whenever no horizon passed the deflated-Sharpe gate,
    which on 8 October 2026 made monthly (IC t 1.59) primary over weekly (t 2.81,
    positive in every fold). The tiers are trade_plan's, so the primary horizon is
    one the idea layer will act on."""
    tier = {"validated": 2, "supported": 1}
    return sorted(per_h, key=lambda h: (tier.get(per_h[h].get("support") or "", 0),
                                        per_h[h].get("net_sharpe") or -9), reverse=True)


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
    for h in per_h:
        per_h[h]["support"] = _support(chosen_by_h[h])
    ranked = rank_horizons(per_h)
    primary = ranked[0] if ranked else None
    for h in per_h:
        ch = chosen_by_h[h]
        per_h[h]["scores"] = (score_latest(panel, ch.model, spec=HORIZON_SPECS[h], feats=feats)
                              if ch else {})
    # Trial F in shadow: the weekly model trained on beta-adjusted returns. It
    # improved the weekly beta-one book in testing (-18 to +8 bps a week) but
    # missed its pre-registered t >= 1.5, so it is graded forward beside the
    # live weekly scores (shared/utils/ic_monitor.py) and never traded.
    if chosen_by_h.get("weekly") is not None and "weekly" in per_h:
        per_h["weekly"]["trial_f_scores"] = score_latest(panel, chosen_by_h["weekly"].model,
                                                         spec=HORIZON_SPECS["weekly"], feats=feats,
                                                         target_mode="beta_adj")
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
