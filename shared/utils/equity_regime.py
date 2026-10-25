"""
shared/utils/equity_regime.py

The stock market's own regime, read from inside it. The macro layer reads rates
and credit; between 5 and 15 October 2026 it rated risk LOW while chips fell
12.7% and last year's winners lagged last year's losers on eight of nine
sessions. This gauge reads what that week was made of. Pure; the factor engine
computes it nightly from its panel and the model portfolio sizes its overlay by
it.

  momentum factor  the daily return of the top minus the bottom fifth of
                   12-1 momentum among tradable names, ranked on the day before
  drawdown         that factor's fall from its high of the last quarter
  vol ratio        its last month's volatility against its last year's
  dispersion       the cross-section's daily spread of returns, month against year
  bear             SPY below where it was two years ago (Daniel & Moskowitz's
                   "panic state" begins after market declines)

Momentum crashes are partly forecastable: they follow market declines in high
volatility and arrive with a rebound in last year's losers (Daniel & Moskowitz,
"Momentum Crashes", 2016). The levels below are rules, set before testing; the
portfolio lab measures on history whether they separate good months from bad.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import numpy as np

EQUITY_REGIME_KEY = "sentinel:regime:equity"     # the latest gauge, written by the factor engine
DRAWDOWN_DAYS = 63
VOL_SHORT, VOL_LONG = 21, 252
BEAR_DAYS = 504
# Chosen on the monthly and weekly walk-forwards of the live ridge model (16 Oct
# 2026), against the first settings (-5%, x1.5). Those flagged 42% of days and
# the book earned *more* in them, so scaling down cost Sharpe (monthly 1.47 ->
# 1.45, weekly 0.44 -> 0.39). At -8% and x1.75 (22% of days) the book earned
# 118 against 254 bps a month and -20 against +30 a week, and scaling by the
# gauge raised Sharpe to 1.50 and 0.56 and cut the worst month from -7.8% to
# -4.7%. The best of three settings tried on the same history, on four stressed
# months: modest evidence, re-measured weekly in the portfolio lab.
STRESS_DRAWDOWN = -0.08         # the momentum factor 8% below its quarter's high
STRESS_VOL_RATIO = 1.75         # its month's volatility three-quarters again its year's
PANIC_SPY_VOL_PCTILE = 0.8      # SPY's month's volatility in the top fifth of its year
MULTIPLIER = {"calm": 1.0, "stressed": 0.6, "panic": 0.35}
HISTORY_SHOWN = 63


def momentum_12_1(close: np.ndarray) -> np.ndarray:
    """[T, N] the factor model's mom_12_1 (shared/utils/factor_model.py): the
    return from 12 months to 1 month ago. Computed here from closes so the
    gauge does not need the whole feature set in memory."""
    out = np.full(close.shape, np.nan, dtype=np.float32)
    with np.errstate(all="ignore"):
        out[252:] = close[231:-21] / close[:-252] - 1.0
    return out


def momentum_factor(close: np.ndarray, mom: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """[T] daily top-minus-bottom-fifth return, ranked on the day before.

    `close` [T, N], `mom` [T, N] the momentum feature, `mask` [T, N] tradable."""
    T = close.shape[0]
    out = np.full(T, np.nan)
    with np.errstate(all="ignore"):
        r = close[1:] / close[:-1] - 1.0
    for i in range(1, T):
        m, ri = mom[i - 1], r[i - 1]
        ok = mask[i - 1] & np.isfinite(m) & np.isfinite(ri)
        n = int(ok.sum())
        if n < 50:
            continue
        idx = np.flatnonzero(ok)
        order = idx[np.argsort(m[ok], kind="stable")]
        q = n // 5
        out[i] = float(ri[order[-q:]].mean() - ri[order[:q]].mean())
    return out


def dispersion(close: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """[T] cross-sectional standard deviation of tradable names' daily returns."""
    T = close.shape[0]
    out = np.full(T, np.nan)
    with np.errstate(all="ignore"):
        r = close[1:] / close[:-1] - 1.0
    for i in range(1, T):
        ri = r[i - 1]
        ok = mask[i - 1] & np.isfinite(ri) & (np.abs(ri) < 0.5)
        if ok.sum() >= 50:
            out[i] = float(np.std(ri[ok]))
    return out


def _vol(x: np.ndarray) -> Optional[float]:
    x = x[np.isfinite(x)]
    return float(np.std(x, ddof=1)) if len(x) > 5 else None


def level_at(i: int, mom_ls: np.ndarray, spy: np.ndarray) -> Optional[Dict[str, Any]]:
    """The gauge at row i from data up to row i, or None without a year of history."""
    if i < VOL_LONG or not np.isfinite(spy[i]):
        return None
    f = np.nan_to_num(mom_ls[: i + 1], nan=0.0)
    cum = np.cumsum(np.log1p(f))
    window = cum[max(0, i - DRAWDOWN_DAYS): i + 1]
    drawdown = float(math.expm1(window[-1] - window.max()))
    v_s, v_l = _vol(mom_ls[i - VOL_SHORT + 1: i + 1]), _vol(mom_ls[i - VOL_LONG + 1: i + 1])
    vol_ratio = v_s / v_l if v_s and v_l else None
    with np.errstate(all="ignore"):
        spy_r = np.diff(np.log(spy[: i + 1]))
    spy_vols = [_vol(spy_r[j - VOL_SHORT: j]) for j in range(max(VOL_SHORT, len(spy_r) - VOL_LONG), len(spy_r) + 1)]
    spy_vols = [v for v in spy_vols if v is not None]
    spy_hot = bool(spy_vols) and spy_vols[-1] >= float(np.quantile(spy_vols, PANIC_SPY_VOL_PCTILE))
    bear = bool(i >= BEAR_DAYS and np.isfinite(spy[i - BEAR_DAYS]) and spy[i] < spy[i - BEAR_DAYS])
    if bear and spy_hot:
        level = "panic"
    elif drawdown <= STRESS_DRAWDOWN or (vol_ratio is not None and vol_ratio >= STRESS_VOL_RATIO):
        level = "stressed"
    else:
        level = "calm"
    return {"level": level, "multiplier": MULTIPLIER[level], "momentum_drawdown": round(drawdown, 4),
            "momentum_vol_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
            "bear_market": bear, "spy_vol_top_fifth": spy_hot}


def gauge(panel: Any, mask: np.ndarray, market: str = "SPY") -> Dict[str, Any]:
    """Today's gauge with the last quarter of its inputs, for the page."""
    if market not in panel.tickers:
        return {"level": None, "note": "no market series"}
    spy = panel.close[:, panel.tickers.index(market)]
    mom_ls = momentum_factor(panel.close, momentum_12_1(panel.close), mask)
    disp = dispersion(panel.close, mask)
    i = len(panel.dates) - 1
    now = level_at(i, mom_ls, spy) or {"level": None}
    d_s, d_l = np.nanmean(disp[i - VOL_SHORT + 1: i + 1]), np.nanmean(disp[i - VOL_LONG + 1: i + 1])
    now["dispersion_ratio"] = round(float(d_s / d_l), 2) if np.isfinite(d_s) and np.isfinite(d_l) and d_l > 0 else None
    start = max(1, i - HISTORY_SHOWN + 1)
    now["history"] = [{"day": str(panel.dates[j])[:10],
                       "momentum": round(float(mom_ls[j]), 5) if np.isfinite(mom_ls[j]) else None,
                       "dispersion": round(float(disp[j]), 5) if np.isfinite(disp[j]) else None}
                      for j in range(start, i + 1)]
    # Barroso & Santa-Clara's continuous scale for the overlay, applied when
    # the lab adopts it (portfolio_lab.adoptions).
    from shared.utils.portfolio_lab import momentum_vol_scale, sentiment_tilt
    now["momentum_vol_scale"] = round(float(momentum_vol_scale(mom_ls)[i]), 3)
    # The short leg's weight from market news tone, applied if the lab adopts it.
    tone = panel.extra.get("ctx_news_tone")
    if tone is not None:
        now["short_tilt"] = round(float(sentiment_tilt(np.asarray(tone, dtype=float))[i]), 3)
    now["as_of"] = str(panel.dates[i])[:10]
    return now


def labels(mom_ls: np.ndarray, spy: np.ndarray) -> List[Optional[str]]:
    """The level at every row, each from data up to that row (for the lab)."""
    return [(level_at(i, mom_ls, spy) or {}).get("level") for i in range(len(spy))]


__all__ = ["EQUITY_REGIME_KEY", "MULTIPLIER", "dispersion", "gauge", "labels", "level_at", "momentum_12_1",
           "momentum_factor"]
