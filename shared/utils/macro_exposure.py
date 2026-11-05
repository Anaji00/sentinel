"""
shared/utils/macro_exposure.py

Measured exposures of stocks to macro instruments: for each stock and each
instrument, the stock's daily return regressed on SPY's return and the
instrument's daily change over WINDOW sessions. Kept when the instrument's
t-statistic is at least MIN_T.

The graph's exposure edges were sector priors: CCJ -> BZ=F, CL=F and NG=F
at weight 1.0 "via sector Energy" (source domain_bridge_prior), which the
stock correlation agent read as measured and ranked first. Nothing measured
rates at all. Daily data in a multi-factor regression with the market, as
for market betas; the market term keeps an index move from passing as an
exposure to whatever moved with it.

A yield's change is in basis points; every other instrument's is a log return.
Pure: closes in, exposure rows out.
"""
from __future__ import annotations

import math
from datetime import date
from typing import List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

WINDOW = 252
MIN_OBS = 150
MIN_T = 3.0
BASELINE_MIN_MOVE = 0.002     # under 0.2% on the session, the baseline makes no call
YIELDS = ("US02Y", "US2Y", "US10Y", "US30Y")

ExposureRow = Tuple[str, str, float, float, float, int]   # (ticker, macro, beta, t, partial corr, sessions)


def exposure_direction(exposure: Optional[float], move: Optional[float], min_move: float = BASELINE_MIN_MOVE) -> Optional[str]:
    """The no-model call: the sign of measured exposure (beta or partial correlation,
    same sign) x the macro's last move. None when either is missing or the move is too
    small to say anything. Pure."""
    if exposure is None or move is None or not exposure or abs(move) < min_move:
        return None
    return "up" if exposure * move > 0 else "down"


def changes(symbol: str, rows: Sequence[Tuple[date, float]]) -> pd.Series:
    """Daily change of one instrument: basis points for a yield, else a log return."""
    s = pd.Series({d: c for d, c in rows if c is not None}).sort_index().astype(float)
    if symbol.upper() in YIELDS:
        return (s.diff() * 100.0).iloc[1:]
    s = s[s > 0]
    return np.log(s).diff().iloc[1:]


def returns(rows: Sequence[Tuple[date, float]]) -> pd.Series:
    s = pd.Series({d: c for d, c in rows if c and c > 0}).sort_index().astype(float)
    return np.log(s).diff().iloc[1:]


def exposures(stocks: Mapping[str, pd.Series], spy: pd.Series, factors: Mapping[str, pd.Series],
              window: int = WINDOW, min_obs: int = MIN_OBS, min_t: float = MIN_T) -> List[ExposureRow]:
    """(ticker, macro, beta, t, partial correlation, sessions) for every pair whose
    instrument term has |t| >= min_t, controlling for the market."""
    spy = spy.tail(window)
    out: List[ExposureRow] = []
    for macro, x_all in factors.items():
        x_all = x_all.reindex(spy.index)
        for ticker, y_all in stocks.items():
            df = pd.DataFrame({"y": y_all.reindex(spy.index), "m": spy, "x": x_all}).dropna()
            n = len(df)
            if n < min_obs or df["x"].std() == 0:
                continue
            X = np.column_stack([np.ones(n), df["m"].to_numpy(), df["x"].to_numpy()])
            y = df["y"].to_numpy()
            try:
                coef, *_ = np.linalg.lstsq(X, y, rcond=None)
                resid = y - X @ coef
                s2 = float(resid @ resid) / (n - 3)
                cov = s2 * np.linalg.inv(X.T @ X)
            except np.linalg.LinAlgError:
                continue
            se = math.sqrt(cov[2, 2]) if cov[2, 2] > 0 else 0.0
            if not se:
                continue
            t = float(coef[2]) / se
            if abs(t) < min_t or not math.isfinite(t):
                continue
            # The partial correlation, from t: bounded, and comparable across instruments.
            pc = t / math.sqrt(t * t + (n - 3))
            out.append((str(ticker), str(macro), round(float(coef[2]), 6), round(t, 2), round(pc, 4), n))
    return out


__all__ = ["MIN_OBS", "MIN_T", "WINDOW", "YIELDS", "ExposureRow", "changes", "exposures", "returns"]
