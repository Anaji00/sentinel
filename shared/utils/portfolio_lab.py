"""
shared/utils/portfolio_lab.py

How would each portfolio construction have done, judged on history rather than
on paper trades still to come? Pure: the factor engine re-runs the monthly
model's walk-forward, collecting every out-of-sample date's predictions and
realized excess returns, and hands them here.

Three constructions on the same predictions:
  live              the top and bottom fifth of the whole universe (the model
                    portfolio's rule)
  industry_neutral  the top and bottom fifth of every industry, so no sector
                    can dominate a side (shared/utils/model_portfolio.py)
  meta_sized        the live book with each pick sized by the probability it
                    pays its costs (shared/utils/meta_label.py), trained only on
                    earlier picks

And a light risk model for each: the book's return split into its industry
component (the mean return of the industries it is long minus those it is
short) and the stock-specific rest. The industry share of the book's variance
says, in one number, how much of a construction is a sector bet; net beta and
the largest single-industry tilt say where else its risk sits.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

PredRow = Tuple[int, np.ndarray, np.ndarray, np.ndarray]   # (date row, columns, predictions, excess returns)
PORTFOLIO_LAB_KEY = "sentinel:portfolio_lab"                 # the latest report, written by the factor engine
LAB_EVERY_DAYS = 7


def _quintiles(score: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Positions of the top and bottom fifth by score."""
    q = max(1, len(score) // 5)
    order = np.argsort(score, kind="stable")
    return order[-q:], order[:q]


def industry_relative(pred: np.ndarray, industry: np.ndarray, min_group: int = 5) -> np.ndarray:
    """Each prediction's percentile within its industry; industries with fewer
    than `min_group` names, and unknown ones (< 0), are ranked together."""
    out = np.full(len(pred), np.nan)
    ind = np.where(industry < 0, -1, industry)
    labels, counts = np.unique(ind, return_counts=True)
    small = set(labels[counts < min_group].tolist()) | {-1}
    group = np.where(np.isin(ind, list(small)), -1, ind)
    for g in np.unique(group):
        idx = np.flatnonzero(group == g)
        order = idx[np.argsort(pred[idx], kind="stable")]
        out[order] = (np.arange(len(order)) + 0.5) / len(order)
    return out


def _industry_component(raw: np.ndarray, industry: np.ndarray, long_pos: np.ndarray, short_pos: np.ndarray) -> float:
    """The part of the long-short return explained by industry means."""
    means = {g: float(np.nanmean(raw[industry == g])) for g in np.unique(industry)}
    il = np.mean([means[g] for g in industry[long_pos]]) if len(long_pos) else 0.0
    ish = np.mean([means[g] for g in industry[short_pos]]) if len(short_pos) else 0.0
    return float(il - ish)


def _max_industry_tilt(industry: np.ndarray, long_pos: np.ndarray, short_pos: np.ndarray) -> float:
    """The largest single industry's net weight (long share minus short share)."""
    if not len(long_pos) or not len(short_pos):
        return 0.0
    tilt: Dict[int, float] = {}
    for g in industry[long_pos]:
        tilt[int(g)] = tilt.get(int(g), 0.0) + 1.0 / len(long_pos)
    for g in industry[short_pos]:
        tilt[int(g)] = tilt.get(int(g), 0.0) - 1.0 / len(short_pos)
    return float(max(abs(v) for v in tilt.values()))


def _stats(xs: Sequence[float], periods_per_year: float, cost_series: Sequence[float]) -> Dict[str, Optional[float]]:
    if len(xs) < 3:
        return {"periods": len(xs), "gross_bps": None, "net_bps": None, "sharpe": None, "t": None}
    a = np.array(xs, dtype=float)
    net = a - np.array(cost_series, dtype=float)
    sd = float(net.std(ddof=1))
    return {
        "periods": len(a),
        "gross_bps": round(float(a.mean()) * 1e4, 1),
        "net_bps": round(float(net.mean()) * 1e4, 1),
        "sharpe": round(float(net.mean() / sd * math.sqrt(periods_per_year)), 2) if sd > 0 else None,
        "t": round(float(net.mean() / (sd / math.sqrt(len(net)))), 2) if sd > 0 else None,
    }


def compare(preds: Sequence[PredRow], industry: np.ndarray, beta: Optional[np.ndarray], horizon: int,
            cost_per_side: float, market: Optional[np.ndarray] = None) -> Dict[str, Any]:
    """Live and industry-neutral books on the same out-of-sample predictions,
    and, given `market` ([T] SPY's forward log return over the horizon) and
    `beta`, two hedged ones:

      beta_neutral   the long leg scaled to the short leg's beta, so the book
                     carries no market exposure (the live long leg ran at beta
                     1.37 against the short's ~0.8, 19 Oct 2026)
      short_hedged   the short leg alone against SPY of matching beta: the
                     model's shorts, the market for the longs (the monthly
                     model's long leg earned -62 bps a month after beta, its
                     short leg +206)

    `raw` in each prediction row is the stock's return minus SPY's, so a leg's
    own return adds the market back. `industry`: [N] code per panel column
    (-1 unknown); `beta`: [T, N] trailing beta or None. Costs: each side pays
    `cost_per_side` on the share of names that changed, both legs (the short
    leg only, for short_hedged), entering and leaving."""
    out: Dict[str, Any] = {}
    names = ["live", "industry_neutral"]
    if market is not None and beta is not None:
        names += ["beta_neutral", "short_hedged"]
    for name in names:
        rets: List[float] = []
        costs: List[float] = []
        ind_part: List[float] = []
        tilts: List[float] = []
        betas: List[float] = []
        prev: Dict[int, set] = {1: set(), -1: set()}
        for row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
            ok = np.isfinite(pred) & np.isfinite(raw)
            if ok.sum() < 50:
                continue
            c, p, r = cols[ok], pred[ok], raw[ok]
            ind = industry[c]
            score = industry_relative(p, ind) if name == "industry_neutral" else p
            long_pos, short_pos = _quintiles(score)
            if name in ("beta_neutral", "short_hedged"):
                b = beta[row, c]
                bl, bs = float(np.nanmean(b[long_pos])), float(np.nanmean(b[short_pos]))
                m = float(market[row])
                if not (np.isfinite(bl) and np.isfinite(bs) and np.isfinite(m)) or bl <= 0:
                    continue
                if name == "beta_neutral":
                    k = bs / bl
                    rets.append(float(k * (r[long_pos].mean() + m) - (r[short_pos].mean() + m)))
                else:
                    rets.append(float(bs * m - (r[short_pos].mean() + m)))
                betas.append(0.0)              # hedged to the short leg's beta by construction
            else:
                rets.append(float(r[long_pos].mean() - r[short_pos].mean()))
            ind_part.append(_industry_component(r, ind, long_pos, short_pos))
            tilts.append(_max_industry_tilt(ind, long_pos, short_pos))
            if beta is not None and name not in ("beta_neutral", "short_hedged"):
                b = beta[row, c]
                betas.append(float(np.nanmean(b[long_pos]) - np.nanmean(b[short_pos])))
            now = {1: set(c[long_pos].tolist()), -1: set(c[short_pos].tolist())}
            if prev[1]:
                turn = sum(1 - len(now[s] & prev[s]) / max(1, len(now[s])) for s in (1, -1)) / 2
            else:
                turn = 1.0
            costs.append((2 if name == "short_hedged" else 2 * 2) * cost_per_side * turn)
            prev = now
        stats = _stats(rets, 252 / horizon, costs)
        a, ip = np.array(rets), np.array(ind_part)
        share = float(np.var(ip) / np.var(a)) if len(a) > 2 and np.var(a) > 0 else None
        out[name] = {
            **stats,
            "industry_share_of_variance": round(min(1.0, share), 3) if share is not None else None,
            "industry_bps": round(float(ip.mean()) * 1e4, 1) if len(ip) else None,
            "max_industry_tilt": round(float(np.mean(tilts)), 3) if tilts else None,
            "net_beta": round(float(np.nanmean(betas)), 3) if betas else None,
        }
    return out


CLUSTERS = 30
CLUSTER_WINDOW = 126
CLUSTER_COMPONENTS = 10


def return_clusters(close: np.ndarray, row: int, cols: np.ndarray, k: int = CLUSTERS,
                    window: int = CLUSTER_WINDOW, components: int = CLUSTER_COMPONENTS, seed: int = 0) -> np.ndarray:
    """A label per column from how the names moved over the `window` sessions
    to `row`: k-means on their loadings on the largest principal components of
    standardised daily returns. Statistical themes, not filing codes: on
    19 Oct 2026 six SIC industries (semiconductors, plastics, circuit boards,
    instruments) were one theme. Names without the history get -1."""
    lo = max(0, row - window)
    with np.errstate(all="ignore"):
        r = np.diff(np.log(close[lo:row + 1, cols].astype(np.float64)), axis=0)
    ok = np.isfinite(r).mean(axis=0) >= 0.8
    labels = np.full(len(cols), -1)
    if ok.sum() < k * 3 or r.shape[0] < 30:
        return labels
    x = np.where(np.isfinite(r[:, ok]), r[:, ok], 0.0)
    x = (x - x.mean(axis=0)) / (x.std(axis=0) + 1e-12)
    _u, s, vt = np.linalg.svd(x, full_matrices=False)
    load = (vt[:components].T * s[:components])
    load /= np.linalg.norm(load, axis=1, keepdims=True) + 1e-12
    # Farthest-point start: each new centre the name least like those chosen,
    # so no theme starts with two centres and none with none.
    rng = np.random.default_rng(seed)
    picked = [int(rng.integers(len(load)))]
    for _ in range(1, k):
        picked.append(int(np.argmin((load @ load[picked].T).max(axis=1))))
    centers = load[picked]
    for _ in range(25):
        assign = np.argmax(load @ centers.T, axis=1)
        new = np.stack([load[assign == j].mean(axis=0) if (assign == j).any() else centers[j] for j in range(k)])
        new /= np.linalg.norm(new, axis=1, keepdims=True) + 1e-12
        if np.allclose(new, centers):
            break
        centers = new
    labels[np.flatnonzero(ok)] = assign
    return labels


def _book_stats(rets: List[float], costs: List[float], horizon: int) -> Dict[str, Any]:
    out = dict(_stats(rets, 252 / horizon, costs))
    net = np.array(rets) - np.array(costs)
    out["worst_bps"] = round(float(net.min()) * 1e4, 1) if len(net) else None
    return out


def cluster_neutral(preds: Sequence[PredRow], close: np.ndarray, horizon: int, cost_per_side: float) -> Dict[str, Any]:
    """The book ranked within statistical return themes (return_clusters)
    rather than SIC industries; same quintiles, costs and periods."""
    rets: List[float] = []
    costs: List[float] = []
    prev: Dict[int, set] = {1: set(), -1: set()}
    for row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        if ok.sum() < 50:
            continue
        c, p, r = cols[ok], pred[ok], raw[ok]
        score = industry_relative(p, return_clusters(close, row, c))
        long_pos, short_pos = _quintiles(score)
        rets.append(float(r[long_pos].mean() - r[short_pos].mean()))
        now = {1: set(c[long_pos].tolist()), -1: set(c[short_pos].tolist())}
        turn = (sum(1 - len(now[s] & prev[s]) / max(1, len(now[s])) for s in (1, -1)) / 2) if prev[1] else 1.0
        costs.append(2 * 2 * cost_per_side * turn)
        prev = now
    return _book_stats(rets, costs, horizon)


def momentum_vol_scale(mom_ls: np.ndarray, window: int = 126, bounds: Tuple[float, float] = (0.5, 1.5)) -> np.ndarray:
    """[T] Barroso & Santa-Clara's scale: the momentum factor's median realised
    volatility to date over its last `window` sessions' realised volatility,
    bounded. Known at each row from data up to it."""
    T = len(mom_ls)
    vol = np.full(T, np.nan)
    for i in range(window, T):
        w = mom_ls[i - window + 1:i + 1]
        w = w[np.isfinite(w)]
        if len(w) > window // 2:
            vol[i] = float(np.std(w, ddof=1))
    out = np.full(T, 1.0)
    for i in range(T):
        if np.isfinite(vol[i]):
            hist = vol[:i + 1][np.isfinite(vol[:i + 1])]
            out[i] = float(np.clip(np.median(hist) / vol[i], *bounds)) if vol[i] > 0 else 1.0
    return out


def sentiment_tilt(tone: np.ndarray, smooth: int = 21, history: int = 252, slope: float = 0.5,
                   bounds: Tuple[float, float] = (0.5, 1.5)) -> np.ndarray:
    """[T] the short leg's weight from market news tone: its `smooth`-session
    mean as a z-score against the last `history` sessions (at least 63), times
    `slope`, around 1 and bounded; 1 where tone is unknown. High sentiment
    leans on the short side (Stambaugh, Yu & Yuan 2012)."""
    T = len(tone)
    m = np.full(T, np.nan)
    for i in range(T):
        w = tone[max(0, i - smooth + 1):i + 1]
        w = w[np.isfinite(w)]
        if len(w) >= smooth // 2:
            m[i] = float(w.mean())
    out = np.full(T, 1.0)
    for i in range(T):
        h = m[max(0, i - history + 1):i + 1]
        h = h[np.isfinite(h)]
        if len(h) >= 63 and np.isfinite(m[i]) and h.std(ddof=1) > 0:
            z = (m[i] - h.mean()) / h.std(ddof=1)
            out[i] = float(np.clip(1.0 + slope * z, *bounds))
    return out


def adjusted_books(preds: Sequence[PredRow], industry: np.ndarray, horizon: int, cost_per_side: float,
                   book_scale: Optional[np.ndarray] = None, short_weight: Optional[np.ndarray] = None
                   ) -> Dict[str, Any]:
    """The industry-neutral book as is, with its size scaled by `book_scale`
    [T], and with its short leg weighted by `short_weight` [T], each on the
    same periods (rows where the adjustment is known)."""
    base: List[float] = []
    scaled: List[float] = []
    tilted: List[float] = []
    costs: List[float] = []
    for row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        if ok.sum() < 50:
            continue
        c, p, r = cols[ok], pred[ok], raw[ok]
        long_pos, short_pos = _quintiles(industry_relative(p, industry[c]))
        rl, rs = float(r[long_pos].mean()), float(r[short_pos].mean())
        k = float(book_scale[row]) if book_scale is not None else 1.0
        w = float(short_weight[row]) if short_weight is not None else 1.0
        cost = 4 * cost_per_side * 0.3          # roughly the book's measured turnover
        base.append(rl - rs)
        scaled.append(k * (rl - rs))
        tilted.append(rl - w * rs)
        costs.append(cost)
    out: Dict[str, Any] = {"base": _book_stats(base, costs, horizon)}
    if book_scale is not None:
        out["momentum_vol_scaled"] = _book_stats(scaled, [c * 1.0 for c in costs], horizon)
    if short_weight is not None:
        out["sentiment_short_tilt"] = _book_stats(tilted, costs, horizon)
    return out


# Each statistical theme's members, written nightly by the factor engine
# (return_clusters on the ranked names): what the book and the trade ideas rank
# within once adoptions() says themes beat SIC industries.
RETURN_CLUSTERS_KEY = "sentinel:portfolio:return_clusters"
ADOPT_MIN_SHARPE_GAIN = 0.10


def adoptions(lab: Dict[str, Any]) -> Dict[str, bool]:
    """Which of the lab's tested changes the live books apply, from its latest
    run: a change goes live when it beats what it would replace by
    ADOPT_MIN_SHARPE_GAIN on the same periods, and a scaling also may not
    deepen the worst period. On 21 Oct 2026 (33 months): ranking within return
    themes 2.49 against industries' 2.29; momentum-volatility scaling 2.69
    against 2.42 with the worst month -2.9% against -5.7%; the sentiment tilt
    2.36, no better."""
    def sharpe(d: Any) -> Optional[float]:
        return (d or {}).get("sharpe") if isinstance(d, dict) else None

    adj = lab.get("adjusted") or {}
    base = adj.get("base") or {}
    out = {"cluster_neutral": False, "momentum_vol_scale": False, "sentiment_short_tilt": False}
    cn, ind = sharpe(lab.get("cluster_neutral")), sharpe(lab.get("industry_neutral"))
    if cn is not None and ind is not None:
        out["cluster_neutral"] = cn >= ind + ADOPT_MIN_SHARPE_GAIN
    for key, name in (("momentum_vol_scaled", "momentum_vol_scale"), ("sentiment_short_tilt", "sentiment_short_tilt")):
        s, b = sharpe(adj.get(key)), sharpe(base)
        if s is None or b is None:
            continue
        worse_worst = (adj[key].get("worst_bps") is not None and base.get("worst_bps") is not None
                       and adj[key]["worst_bps"] < base["worst_bps"])
        out[name] = s >= b + ADOPT_MIN_SHARPE_GAIN and not worse_worst
    return out


def rank_weighted(preds: Sequence[PredRow], horizon: int, cost_per_side: float) -> Dict[str, Any]:
    """Every ranked name held in proportion to its centred rank, a dollar on
    each side, instead of the top and bottom fifth at equal weight: the book
    uses the whole ordering rather than two buckets of it. Costs on the weight
    that changes hands each period."""
    rets: List[float] = []
    costs: List[float] = []
    prev: Dict[int, float] = {}
    for _row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        if ok.sum() < 50:
            continue
        c, p, r = cols[ok], pred[ok], raw[ok]
        rk = (np.argsort(np.argsort(p)) + 0.5) / len(p) - 0.5
        w = rk / np.abs(rk).sum() * 2.0            # a dollar long, a dollar short
        rets.append(float(w @ r))
        now = dict(zip(c.tolist(), w.tolist()))
        traded = sum(abs(now.get(k, 0.0) - prev.get(k, 0.0)) for k in set(now) | set(prev))
        costs.append(traded * cost_per_side)
        prev = now
    return _stats(rets, 252 / horizon, costs)


def _spearman(a: np.ndarray, b: np.ndarray) -> Optional[float]:
    if len(a) < 3:
        return None
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    sa, sb = ra.std(), rb.std()
    return float(np.mean((ra - ra.mean()) * (rb - rb.mean())) / (sa * sb)) if sa > 0 and sb > 0 else None


def regime_labels(spy_close: np.ndarray, trend_days: int = 63, vol_days: int = 21) -> List[Optional[str]]:
    """Per row, from data up to that row: the market's trend over the last
    quarter (up/down) and whether its last month's volatility is above the
    median of every month before it (stormy/calm)."""
    r = np.diff(np.log(spy_close), prepend=np.nan)
    vols: List[float] = []
    out: List[Optional[str]] = []
    for i in range(len(spy_close)):
        if i < max(trend_days, vol_days) or not np.isfinite(spy_close[i - trend_days]) or spy_close[i - trend_days] <= 0:
            out.append(None)
            continue
        trend = "up" if spy_close[i] > spy_close[i - trend_days] else "down"
        v = float(np.nanstd(r[i - vol_days + 1:i + 1]))
        vols.append(v)
        out.append(f"{trend}, {'stormy' if v > float(np.median(vols)) else 'calm'}")
    return out


def ic_by_regime(preds: Sequence[PredRow], spy_close: np.ndarray) -> Dict[str, Dict[str, Any]]:
    """The model's out-of-sample IC within each market regime: does its edge
    hold in falling or volatile markets, or only in calm rising ones?"""
    labels = regime_labels(spy_close)
    by: Dict[str, List[float]] = {}
    for row, _cols, pred, raw in preds:
        ok = np.isfinite(pred) & np.isfinite(raw)
        lab = labels[row] if row < len(labels) else None
        ic = _spearman(pred[ok], raw[ok]) if lab else None
        if lab and ic is not None:
            by.setdefault(lab, []).append(ic)
    out: Dict[str, Dict[str, Any]] = {}
    for lab, ics in sorted(by.items()):
        a = np.array(ics)
        sd = float(a.std(ddof=1)) if len(a) > 1 else 0.0
        out[lab] = {"dates": len(a), "mean_ic": round(float(a.mean()), 4),
                    "t": round(float(a.mean() / (sd / math.sqrt(len(a)))), 2) if sd > 0 else None}
    return out


def by_equity_regime(preds: Sequence[PredRow], levels: Sequence[Optional[str]], multipliers: Dict[str, float],
                     horizon: int, cost_per_side: float) -> Dict[str, Any]:
    """The live book's out-of-sample periods split by the equity-regime gauge
    on the day each was formed (shared/utils/equity_regime.py), and the book
    with its size scaled by the gauge's multiplier against the book unscaled.
    Costs are charged on a full turnover each period, the conservative case."""
    per: Dict[str, List[float]] = {}
    raw_ls: List[float] = []
    scaled: List[float] = []
    for row, _cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        lab = levels[row] if row < len(levels) else None
        if ok.sum() < 50 or lab is None:
            continue
        long_pos, short_pos = _quintiles(pred[ok])
        r = raw[ok]
        net = float(r[long_pos].mean() - r[short_pos].mean()) - 4 * cost_per_side
        per.setdefault(lab, []).append(net)
        raw_ls.append(net)
        scaled.append(net * multipliers.get(lab, 1.0))
    ppy = 252 / horizon

    def sharpe(xs: List[float]) -> Optional[float]:
        a = np.array(xs)
        return round(float(a.mean() / a.std(ddof=1) * math.sqrt(ppy)), 2) if len(a) > 2 and a.std(ddof=1) > 0 else None
    return {
        "levels": {k: {"periods": len(v), "net_bps": round(float(np.mean(v)) * 1e4, 1)} for k, v in sorted(per.items())},
        "unscaled_sharpe": sharpe(raw_ls), "scaled_sharpe": sharpe(scaled),
        "worst_unscaled_bps": round(min(raw_ls) * 1e4, 1) if raw_ls else None,
        "worst_scaled_bps": round(min(scaled) * 1e4, 1) if scaled else None,
    }


def alpha_calibration(panel: Any, preds: Sequence[PredRow], horizon: int) -> Optional[Dict[str, Any]]:
    """Raw alpha (the IC known before each date x the stock's trailing
    volatility over the horizon x its rank's normal quantile) against the
    realised excess return, across the out-of-sample dates."""
    from statistics import NormalDist
    from shared.utils import expected_returns as er

    nd = NormalDist()
    with np.errstate(all="ignore"):
        d = np.diff(np.log(panel.close), axis=0, prepend=np.nan)
    ics: List[float] = []
    raw_all: List[float] = []
    real_all: List[float] = []
    for row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        if ok.sum() < 50:
            continue
        c, p, y = cols[ok], pred[ok], raw[ok]
        if ics:
            z = np.array([nd.inv_cdf(v) for v in (np.argsort(np.argsort(p)) + 0.5) / len(p)])
            sig = np.nanstd(d[max(0, row - 62): row + 1, c], axis=0) * math.sqrt(252)
            a = np.array([er.alpha(float(np.mean(ics)), float(s_), float(z_), horizon) or np.nan
                          for s_, z_ in zip(sig, z)])
            good = np.isfinite(a)
            raw_all += a[good].tolist()
            real_all += y[good].tolist()
        ics.append(_spearman(p, y) or 0.0)
    return er.calibration(raw_all, real_all)


def verdict(results: Dict[str, Any]) -> str:
    """Which construction history favours, in a sentence, and how firmly."""
    live, neutral = results.get("live") or {}, results.get("industry_neutral") or {}
    if live.get("net_bps") is None or neutral.get("net_bps") is None:
        return "Not enough out-of-sample history to compare the constructions."
    better = "industry-neutral" if (neutral.get("sharpe") or -9) > (live.get("sharpe") or -9) else "live"
    meta = results.get("meta_sized") or {}
    sizing = ""
    if meta.get("meta", {}).get("sharpe") is not None and meta.get("base", {}).get("sharpe") is not None:
        helps = meta["meta"]["sharpe"] > meta["base"]["sharpe"]
        sizing = (" Sizing picks by their probability of paying " + ("raises" if helps else "does not raise") +
                  f" the Sharpe ({meta['base']['sharpe']} to {meta['meta']['sharpe']}).")
    return (f"History favours the {better} construction: net {live['net_bps']} vs {neutral['net_bps']} bps a "
            f"period, Sharpe {live['sharpe']} vs {neutral['sharpe']}, with industry bets explaining "
            f"{live.get('industry_share_of_variance')} vs {neutral.get('industry_share_of_variance')} of the "
            f"variance.{sizing}")


def run(panel: Any, model_name: str, horizon_models: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """The whole lab on one panel: the chosen monthly model's walk-forward,
    re-run to collect its out-of-sample predictions and picks, then the
    constructions compared and the sizing layer trained on earlier picks.
    `horizon_models` names the model chosen at each other horizon, so each
    horizon's forecast calibration is measured on the model that ranks it
    (weekly defaults to the monthly model; daily is calibrated only when named)."""
    from shared.utils import factor_model as fm
    from shared.utils import meta_label as ml

    from shared.utils import equity_regime as er

    panel = fm.tradable_subset(panel)
    spec = fm.HORIZON_SPECS["monthly"]
    # Before the features, so the gauge's temporary arrays are freed before
    # the feature set fills memory.
    eq_labels = None
    mom_ls = None
    if "SPY" in panel.tickers:
        mom_ls = er.momentum_factor(panel.close, er.momentum_12_1(panel.close), fm.tradable(panel))
        eq_labels = er.labels(mom_ls, panel.close[:, panel.tickers.index("SPY")])
    feats = fm.features(panel)
    preds: List[PredRow] = []
    picks: List[Tuple[int, int, int, float, float]] = []
    fm.walk_forward(panel, model_name, spec=spec, feats=feats, collect=picks, collect_preds=preds)
    industry = panel.extra.get("industry")
    if industry is None:
        industry = np.full(len(panel.tickers), -1)
    cost = fm.COST_BPS_PER_SIDE / 1e4
    market = None
    if "SPY" in panel.tickers:
        spy = panel.close[:, panel.tickers.index("SPY")].astype(np.float64)
        with np.errstate(all="ignore"):
            market = np.concatenate([np.log(spy[spec.horizon:] / spy[:-spec.horizon]), np.full(spec.horizon, np.nan)])
    res: Dict[str, Any] = compare(preds, np.asarray(industry), feats.get("beta_126"), spec.horizon, cost, market)
    res["rank_weighted"] = rank_weighted(preds, spec.horizon, cost)
    # Research items from the 19 Oct live audit: statistical themes for the
    # book's neutrality, momentum-volatility scaling, a sentiment-led short leg.
    res["cluster_neutral"] = cluster_neutral(preds, panel.close, spec.horizon, cost)
    tone = panel.extra.get("ctx_news_tone")
    res["adjusted"] = adjusted_books(
        preds, np.asarray(industry), spec.horizon, cost,
        book_scale=momentum_vol_scale(mom_ls) if mom_ls is not None else None,
        short_weight=sentiment_tilt(np.asarray(tone, dtype=float)) if tone is not None else None)
    if "SPY" in panel.tickers and picks:
        P = ml.as_picks(picks)
        X = ml.design(P, feats, ml.market_variance_ratio(panel.close[:, panel.tickers.index("SPY")]))
        res["meta_sized"] = ml.walk_forward(P, X, cost=2 * cost, horizon=spec.horizon, embargo=spec.embargo)
        # The classifier on all of it, for the trade ideas once runs confirm it.
        res["meta_model"] = ml.fit_final(P, X, 2 * cost)
        res["ic_by_regime"] = ic_by_regime(preds, panel.close[:, panel.tickers.index("SPY")])
        if eq_labels is not None:
            res["equity_regime"] = by_equity_regime(preds, eq_labels, er.MULTIPLIER, spec.horizon, cost)
    # How far the forecasts run, not only which way: each horizon's raw
    # Grinold-Kahn alpha against what followed (shared/utils/expected_returns.py).
    res["alpha_calibration"] = {"monthly": alpha_calibration(panel, preds, spec.horizon)}
    others = dict(horizon_models or {})
    others.setdefault("weekly", model_name)
    for h in ("weekly", "daily"):
        if not others.get(h) or h not in fm.HORIZON_SPECS:
            continue
        h_preds: List[PredRow] = []
        hspec = fm.HORIZON_SPECS[h]
        fm.walk_forward(panel, others[h], spec=hspec, feats=feats, collect_preds=h_preds)
        res["alpha_calibration"][h] = alpha_calibration(panel, h_preds, hspec.horizon)
        res.setdefault("calibration_models", {"monthly": model_name})[h] = others[h]
        del h_preds
    res["verdict"] = verdict(res)
    res["model"] = model_name
    res["horizon"] = "monthly"
    res["as_of"] = str(panel.dates[-1])
    return res


__all__ = ["RETURN_CLUSTERS_KEY", "adoptions", "return_clusters", "momentum_vol_scale", "sentiment_tilt",
           "cluster_neutral", "adjusted_books", "LAB_EVERY_DAYS", "PORTFOLIO_LAB_KEY", "by_equity_regime", "compare",
           "ic_by_regime", "rank_weighted", "industry_relative", "regime_labels", "run", "verdict"]
