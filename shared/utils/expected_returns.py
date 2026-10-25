"""
shared/utils/expected_returns.py

From a model's rank to how far, not only which way: the expected excess
return, the volatility around it and the probability the stock beats SPY,
per stock and horizon. Pure; the recommendation engine applies it.

  alpha = IC x sigma_h x z        Grinold & Kahn's forecast refinement: the
                                  model's skill (its out-of-sample rank IC,
                                  shrunk), the stock's own volatility over the
                                  horizon, and its standardized score (the
                                  normal quantile of its percentile)
  + event overlay                 the measured abnormal move of each proven
                                  event family active on the stock
  sigma (forecast)                volatility persists (Corsi's HAR): a blend of
                                  the last month, quarter and year, pulled
                                  toward implied volatility less the usual
                                  volatility risk premium, plus the stock's own
                                  earnings jump when results fall inside the
                                  horizon. Implied volatility carries that
                                  jump; realised does not, and without it every
                                  option across earnings would look rich.

Calibration is measured, not assumed. On the walk-forward history (trial K,
17 Oct 2026) realised excess returns rose with alpha decile by decile at both
horizons, but the scale was off in opposite directions -- monthly moves came
out 1.81 times alpha, weekly 0.54 times -- and the average liquid stock lagged
SPY (an intercept of -1.2% a month, -0.29% a week, as mega-caps led
2023-26). So alpha is a raw score and `calibrated` maps it through each
horizon's measured slope and half its intercept (how long mega-cap leadership
lasts is not known). The portfolio lab re-measures both weekly (`calibration`)
and the engine uses its latest; CALIBRATION_DEFAULTS are trial K's numbers.

The tails are not linear (trials K2 and K3, 17 Oct 2026). The top 1% of raw
alpha (+311 bps a month) realised +233, where the line said about +500; the
bottom 1% (-568) realised -1,467, past the line's -1,100. Weekly the same
shape: +200 realised +66, -307 realised -287. So the map is the measured
bucket means themselves (`knots_bps`: raw, realised, count), made monotone
(pool-adjacent-violators), read by linear interpolation and held flat past the
outermost bucket: a forecast never exceeds what the most extreme names in the
calibration sample actually did. A short's larger edge is kept; a long's
saturation is respected.
"""
from __future__ import annotations

import math
from statistics import NormalDist
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

TRADING_DAYS = 252
VRP_DISCOUNT = 0.85              # implied volatility overstates what follows by about 15% on average
HAR_WEIGHTS = (0.5, 0.3, 0.2)    # month, quarter, year
IMPLIED_WEIGHT = 0.5             # how far the forecast leans to implied (less the premium)
DEFAULT_EARNINGS_MOVE = 0.05     # a typical two-day earnings move when the stock's own is unknown
# (slope, intercept per horizon) from trial K; the daily horizon borrows weekly's.
CALIBRATION_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "monthly": {"slope": 1.809, "intercept": -0.0111, "knots_bps": [
        [-567.5, -1466.7, 306], [-228.1, -454.3, 306], [-161.1, -365.5, 3057], [-41.3, -190.5, 3057],
        [-18.7, -123.4, 3057], [-6.9, -69.0, 3057], [-1.1, -133.7, 3057], [1.1, -156.1, 3057],
        [6.1, -69.6, 3057], [16.2, -109.3, 3057], [35.6, -49.1, 3057], [115.5, 55.2, 3057],
        [173.8, -35.0, 306], [310.7, 232.5, 306]]},
    "weekly": {"slope": 0.535, "intercept": -0.00275, "knots_bps": [
        [-306.9, -286.8, 1453], [-115.5, -177.5, 1452], [-79.9, -87.7, 14523], [-19.0, -27.5, 14523],
        [-9.7, -35.0, 14523], [-4.9, -35.0, 14523], [-1.5, -30.2, 14523], [1.5, -20.4, 14523],
        [4.7, -21.9, 14523], [9.1, -17.4, 14523], [17.0, -9.5, 14523], [62.4, -1.8, 14523],
        [96.2, 15.3, 1452], [199.7, 65.6, 1453]]},
    "daily": {"slope": 0.535, "intercept": -0.00057},
}
# Raw-alpha percentiles bounding each bucket of the measured map.
KNOT_EDGES = (0, 1, 2, 10, 20, 30, 40, 50, 60, 70, 80, 90, 98, 99, 100)
INTERCEPT_WEIGHT = 0.5
_ND = NormalDist()


def z_of(percentile: Optional[float]) -> Optional[float]:
    """The normal quantile of a cross-sectional percentile in (0, 1)."""
    if percentile is None:
        return None
    p = min(max(float(percentile), 0.001), 0.999)
    return _ND.inv_cdf(p)


def _vol(returns: Sequence[float]) -> Optional[float]:
    xs = [r for r in returns if r is not None and math.isfinite(r)]
    if len(xs) < 10:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def daily_returns(closes: Sequence[float]) -> list:
    return [math.log(b / a) for a, b in zip(closes, closes[1:]) if a and b and a > 0 and b > 0]


def har_vol(closes: Sequence[float]) -> Optional[float]:
    """Annualised volatility forecast from the last 21, 63 and 252 sessions."""
    r = daily_returns(closes)
    parts = [(w, _vol(r[-n:])) for w, n in zip(HAR_WEIGHTS, (21, 63, 252))]
    parts = [(w, v) for w, v in parts if v is not None]
    if not parts:
        return None
    tot = sum(w for w, _v in parts)
    return math.sqrt(TRADING_DAYS) * sum(w * v for w, v in parts) / tot


def forecast_vol(closes: Sequence[float], atm_iv: Optional[float] = None, earnings_inside: bool = False,
                 earnings_move: Optional[float] = None, horizon_days: int = 21) -> Optional[float]:
    """Annualised volatility expected over the horizon (see the module note)."""
    har = har_vol(closes)
    if har is None and not atm_iv:
        return None
    base = har if har is not None else atm_iv * VRP_DISCOUNT
    if atm_iv:
        base = (1 - IMPLIED_WEIGHT) * base + IMPLIED_WEIGHT * atm_iv * VRP_DISCOUNT
    if not earnings_inside:
        return base
    jump = earnings_move if earnings_move else DEFAULT_EARNINGS_MOVE
    t = max(horizon_days, 1) / TRADING_DAYS
    return math.sqrt(base * base + jump * jump / t)


def alpha(ic: Optional[float], sigma_annual: Optional[float], z: Optional[float], horizon_days: int) -> Optional[float]:
    """The raw Grinold-Kahn score IC x sigma_h x z (a fraction): ordered right,
    scaled by `calibrated`."""
    if ic is None or sigma_annual is None or z is None:
        return None
    sigma_h = sigma_annual * math.sqrt(horizon_days / TRADING_DAYS)
    return ic * sigma_h * z


def monotone(knots: Sequence[Sequence[float]]) -> List[Tuple[float, float]]:
    """(raw, realised, count) knots sorted by raw, realised made
    non-decreasing by count-weighted pool-adjacent-violators."""
    pts = sorted((float(k[0]), float(k[1]), max(float(k[2]) if len(k) > 2 else 1.0, 1.0)) for k in knots)
    blocks: List[List[float]] = []          # [sum of w*y, sum of w, members]
    for _x, y, w in pts:
        blocks.append([y * w, w, 1.0])
        while len(blocks) > 1 and blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]:
            s, ww, m = blocks.pop()
            blocks[-1] = [blocks[-1][0] + s, blocks[-1][1] + ww, blocks[-1][2] + m]
    ys: List[float] = []
    for s, ww, m in blocks:
        ys.extend([s / ww] * int(m))
    return [(x, y) for (x, _y, _w), y in zip(pts, ys)]


def _interp(x: float, pts: Sequence[Tuple[float, float]]) -> float:
    if x <= pts[0][0]:
        return pts[0][1]
    if x >= pts[-1][0]:
        return pts[-1][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 if x1 == x0 else y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return pts[-1][1]


def calibrated(raw: Optional[float], horizon: str, cal: Optional[Any] = None) -> Optional[float]:
    """Expected excess return over SPY from a raw alpha: through the measured
    map when there is one (see the module note), else slope x raw; either way
    with half the sample's intercept (its average shortfall to SPY) taken back
    out of the forecast. `cal` is a lab calibration dict, a (slope, intercept)
    pair, or None for the horizon's default."""
    if raw is None:
        return None
    c = cal if cal is not None else CALIBRATION_DEFAULTS.get(horizon, CALIBRATION_DEFAULTS["weekly"])
    if isinstance(c, (tuple, list)):
        slope, intercept = float(c[0]), float(c[1])
        return slope * raw + INTERCEPT_WEIGHT * intercept
    intercept = float(c.get("intercept") or 0.0)
    knots = c.get("knots_bps") or []
    if len(knots) >= 3:
        return _interp(raw * 1e4, monotone(knots)) / 1e4 - (1 - INTERCEPT_WEIGHT) * intercept
    return float(c["slope"]) * raw + INTERCEPT_WEIGHT * intercept


def calibration(raw: Sequence[float], realized: Sequence[float]) -> Optional[Dict[str, Any]]:
    """Least-squares slope and intercept of realised excess on raw alpha, and
    the decile means, from out-of-sample pairs (the portfolio lab)."""
    pairs = [(a, min(max(y, -1.0), 1.0)) for a, y in zip(raw, realized)
             if a is not None and y is not None and math.isfinite(a) and math.isfinite(y)]
    n = len(pairs)
    if n < 500:
        return None
    ma = sum(a for a, _y in pairs) / n
    my = sum(y for _a, y in pairs) / n
    var = sum((a - ma) ** 2 for a, _y in pairs)
    if var <= 0:
        return None
    slope = sum((a - ma) * (y - my) for a, y in pairs) / var
    srt = sorted(pairs)
    dec = []
    for k in range(10):
        part = srt[k * n // 10:(k + 1) * n // 10]
        dec.append([round(sum(a for a, _y in part) / len(part) * 1e4, 1), round(sum(y for _a, y in part) / len(part) * 1e4, 1)])
    knots = []
    for lo, hi in zip(KNOT_EDGES, KNOT_EDGES[1:]):
        part = srt[lo * n // 100:hi * n // 100]
        if part:
            knots.append([round(sum(a for a, _y in part) / len(part) * 1e4, 1),
                          round(sum(y for _a, y in part) / len(part) * 1e4, 1), len(part)])
    return {"slope": round(slope, 4), "intercept": round(my - slope * ma, 6), "pairs": n,
            "deciles_bps": dec, "knots_bps": knots}


def with_events(base: Optional[float], event_bps: Iterable[float]) -> Optional[float]:
    """The model's alpha plus the measured moves of active proven families."""
    extra = sum(float(b) for b in event_bps if b is not None) / 1e4
    if base is None:
        return extra if extra else None
    return base + extra


def prob_beats(alpha_h: Optional[float], sigma_annual: Optional[float], horizon_days: int,
               hurdle: float = 0.0) -> Optional[float]:
    """P(excess return over the horizon > hurdle) under a normal with mean alpha
    and the stock's horizon volatility."""
    if alpha_h is None or not sigma_annual:
        return None
    sd = sigma_annual * math.sqrt(horizon_days / TRADING_DAYS)
    if sd <= 0:
        return None
    return 1.0 - _ND.cdf((hurdle - alpha_h) / sd)


def kelly_stock(alpha_h: Optional[float], sigma_annual: Optional[float], horizon_days: int,
                cost: float = 0.0, fraction: float = 0.5, cap: float = 0.05) -> float:
    """Half-Kelly weight for a stock position: (|alpha| - cost) / variance over
    the horizon, capped at `cap` of the account."""
    if alpha_h is None or not sigma_annual:
        return 0.0
    var = sigma_annual * sigma_annual * horizon_days / TRADING_DAYS
    edge = abs(alpha_h) - cost
    if edge <= 0 or var <= 0:
        return 0.0
    return round(min(cap, fraction * edge / var), 4)


__all__ = ["KNOT_EDGES", "monotone", "CALIBRATION_DEFAULTS", "alpha", "calibrated", "calibration", "daily_returns",
           "forecast_vol", "har_vol", "kelly_stock", "prob_beats", "with_events", "z_of"]
