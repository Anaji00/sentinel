"""Book constructions compared with B1 in the registered 13 Nov trial (Upgrade pre-registrations - 12 Nov, "Book strategy trial").
Simulation only: nothing here touches the live trader, its sizing, caps, kill switch or execution.

A `CrossSection` is one day's ranked names (model score, borrowability, industry, cap, trailing volatility, days to cover, liquidity,
beta). `strategy_book` turns it into signed target weights of NAV for a `Strategy`: S0 is B1 itself (`auto_trade.b1_book` on percentile =
rank / (n - 1), equal weights, each leg 0.5), the others change the selection, the weights, the cadence, or add a hedge.
`simulate` is a daily-path simulator: positions drift between rebalances, every variant pays the same costs, and the evaluation window is
cut into 21-session blocks. Statistics (paired date-cluster bootstrap, Benjamini-Hochberg) are at the end. Pure; no I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from shared.utils.auto_trade import B1_LONG, B1_SHORT_MAX
from shared.utils.b1_variants import SHORT_FEE_BPS
from shared.utils.model_portfolio import MIN_INDUSTRY_GROUP

COST_BPS_PER_SIDE = 10.0          # the walk-forward's per-side rate (factor_model.COST_BPS_PER_SIDE)
LEG = 0.5                         # each leg's share of NAV, as target_weights(leg=0.5)
BLOCK = 21                        # sessions per evaluation block
MIN_CAP_USD = 1_000_000_000.0     # the trader's market-cap floor (mc.MODEL_MIN_MARKET_CAP_USD)
INVVOL_CAP = 3.0                  # S4: a name's weight at most this many times the equal share
CONVICTION_SLOPE = 0.5            # S5: weight = clip(1 + slope * z, lo, hi)
CONVICTION_BOUNDS = (0.5, 1.5)
BAND_FRACTION = 0.25              # S6: a held position within this share of its new target is left alone
DTC_MAX = 8.0                     # S10: FINRA days to cover above this drops a short
ADV_SHARE = 0.02                  # S11: a position at most this share of the name's average daily dollar volume
REFERENCE_ACCOUNT = 100_000.0     # S11's reference account size
STAGGER = 5                       # S7: sessions between tranches
EPS = 1e-9


@dataclass
class CrossSection:
    """One date's ranked names; every array has one entry per name. `cols` are panel columns."""
    cols: np.ndarray
    score: np.ndarray              # higher is better
    borrowable: np.ndarray         # bool
    industry: np.ndarray           # int, -1 unknown
    cap: np.ndarray                # USD, NaN unknown
    vol: np.ndarray                # trailing daily volatility, NaN unknown
    dtc: np.ndarray                # days to cover, NaN unknown
    adv: np.ndarray                # trailing average daily dollar volume, NaN unknown
    beta: np.ndarray               # trailing beta to the market, NaN unknown


@dataclass(frozen=True)
class Strategy:
    name: str
    select: str = "b1"             # b1 | industry | size | decile | quintile | double
    weight: str = "equal"          # equal | invvol | conviction
    cadence: int = BLOCK           # sessions between rebalances
    band: float = 0.0              # no-trade band (share of the target weight); 0 = none
    tranches: int = 1              # staggered books, each a share of NAV
    hedge: bool = False            # SPY hedge of the book's beta (simulation only)
    dtc_max: Optional[float] = None
    adv_share: Optional[float] = None
    account: float = REFERENCE_ACCOUNT


S0 = Strategy("S0")
STRATEGIES: Tuple[Strategy, ...] = (
    S0,
    Strategy("S1", select="industry"),
    Strategy("S2", select="size"),
    Strategy("S3a", select="decile"),
    Strategy("S3b", select="quintile"),
    Strategy("S3c", select="double"),
    Strategy("S4", weight="invvol"),
    Strategy("S5", weight="conviction"),
    Strategy("S6_bi", cadence=10),
    Strategy("S6_wk", cadence=5),
    Strategy("S6_mo_band", band=BAND_FRACTION),
    Strategy("S6_bi_band", cadence=10, band=BAND_FRACTION),
    Strategy("S6_wk_band", cadence=5, band=BAND_FRACTION),
    Strategy("S7", tranches=4),
    Strategy("S8", hedge=True),
    Strategy("S9"),                                     # same book rule; the runner feeds it the blended score
    Strategy("S10", dtc_max=DTC_MAX),
    Strategy("S11", adv_share=ADV_SHARE),
)
CAPACITY_CELLS: Tuple[Strategy, ...] = (
    Strategy("S11_25m", adv_share=ADV_SHARE, account=25e6),
    Strategy("S11_250m", adv_share=ADV_SHARE, account=250e6),
)


# ---------------------------------------------------------------- selection

def percentiles(score: np.ndarray) -> np.ndarray:
    """Rank / (n - 1), the percentile the live scores carry (factor_model.score_latest)."""
    n = len(score)
    return score.argsort().argsort() / max(1, n - 1)


def blend_scores(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """S9: the mean of two models' percentiles over the same names."""
    return (percentiles(a) + percentiles(b)) / 2.0


def b1_select(pct: np.ndarray, borrowable: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """(long, short) positions by `auto_trade.b1_book`'s rule: 90th to 99th percentile long, bottom tenth short if borrowable."""
    long_ = (pct >= B1_LONG[0]) & (pct < B1_LONG[1])
    short = (pct <= B1_SHORT_MAX) & borrowable & ~long_
    return np.flatnonzero(long_), np.flatnonzero(short)


def band_select(pct: np.ndarray, borrowable: np.ndarray, long_lo: float, long_hi: float, short_hi: float) -> Tuple[np.ndarray, np.ndarray]:
    long_ = (pct >= long_lo) & (pct < long_hi)
    short = (pct <= short_hi) & borrowable & ~long_
    return np.flatnonzero(long_), np.flatnonzero(short)


def count_select(score: np.ndarray, borrowable: np.ndarray, mult: int = 2) -> Tuple[np.ndarray, np.ndarray]:
    """S3c: `mult` times B1's count in each leg; longs run down from the 0.99 cut, shorts up among the borrowable."""
    pct = percentiles(score)
    n_long, n_short = (len(x) for x in b1_select(pct, borrowable))
    below_cut = np.flatnonzero(pct < B1_LONG[1])
    longs = below_cut[np.argsort(-score[below_cut], kind="stable")][: mult * n_long]
    cand = np.flatnonzero(borrowable)
    shorts = cand[np.argsort(score[cand], kind="stable")][: mult * n_short]
    return np.sort(longs), np.sort(shorts)


def industry_labels(industry: np.ndarray, min_group: int = MIN_INDUSTRY_GROUP) -> np.ndarray:
    """Group labels with unknown industries and groups under `min_group` names pooled as -1 (`model_portfolio.industry_relative`)."""
    labels = np.asarray(industry).astype(int).copy()
    for g in np.unique(labels):
        if g >= 0 and int((labels == g).sum()) < min_group:
            labels[labels == g] = -1
    labels[labels < 0] = -1
    return labels


def cap_terciles(cap: np.ndarray, floor: float = MIN_CAP_USD) -> Tuple[np.ndarray, np.ndarray]:
    """(label 0 small / 1 mid / 2 large, eligible): terciles of the best-known cap among names at or above `floor`; others are not eligible."""
    ok = np.isfinite(cap) & (cap >= floor)
    labels = np.full(len(cap), -1, dtype=int)
    idx = np.flatnonzero(ok)
    if len(idx):
        order = np.argsort(cap[idx], kind="stable")
        labels[idx[order]] = np.minimum(2, (np.arange(len(idx)) * 3) // len(idx))
    return labels, ok


def group_neutral_select(score: np.ndarray, labels: np.ndarray, borrowable: np.ndarray,
                         eligible: Optional[np.ndarray] = None) -> Tuple[np.ndarray, np.ndarray]:
    """S1 and S2: within each group, percentile (i + 0.5) / n, B1's bands applied inside the group, and the long and short counts equalised
    to the smaller by dropping the worse-ranked. A group with no pick on one side adds none on either."""
    ok = np.ones(len(score), dtype=bool) if eligible is None else np.asarray(eligible, dtype=bool)
    longs: List[int] = []
    shorts: List[int] = []
    for g in np.unique(labels[ok]):
        idx = np.flatnonzero(ok & (labels == g))
        n = len(idx)
        order = idx[np.argsort(score[idx], kind="stable")]           # worst first
        p = (np.arange(n) + 0.5) / n
        lg = order[(p >= B1_LONG[0] - EPS) & (p < B1_LONG[1] - EPS)]
        sh = order[(p <= B1_SHORT_MAX + EPS) & borrowable[order]]
        k = min(len(lg), len(sh))
        if k:
            longs.extend(lg[::-1][:k].tolist())                       # best first
            shorts.extend(sh[:k].tolist())
    return np.array(sorted(longs), dtype=int), np.array(sorted(shorts), dtype=int)


def dtc_filter(short_idx: np.ndarray, dtc: np.ndarray, max_dtc: float) -> np.ndarray:
    """S10: drop shorts whose days to cover exceeds `max_dtc`; one with no figure is kept."""
    d = dtc[short_idx]
    return short_idx[~(np.isfinite(d) & (d > max_dtc))]


# ---------------------------------------------------------------- weights

def equal_weights(n: int) -> np.ndarray:
    return np.full(n, 1.0 / n) if n else np.zeros(0)


def inverse_vol_weights(vol: np.ndarray, cap_mult: float = INVVOL_CAP) -> np.ndarray:
    """S4: shares proportional to 1 / volatility (an unknown one takes the leg's median), each limited to `cap_mult` times the equal share."""
    n = len(vol)
    if n == 0:
        return np.zeros(0)
    v = np.where(np.isfinite(vol) & (vol > 0), vol, np.nan)
    v = np.where(np.isfinite(v), v, np.nanmedian(v) if np.isfinite(v).any() else 1.0)
    w = (1.0 / v) / (1.0 / v).sum()
    limit = cap_mult / n
    for _ in range(n):                                   # water-filling: capped names keep the limit, the rest share what is left
        over = w > limit + EPS
        if not over.any():
            break
        free = ~over & (w < limit - EPS)
        w = np.where(over, limit, w)
        if not free.any():
            break
        w[free] += (1.0 - w.sum()) * w[free] / w[free].sum()
    return w / w.sum()


def conviction_weights(extremity: np.ndarray) -> np.ndarray:
    """S5: weight clip(1 + slope * z, lo, hi), z the extremity (percentile for longs, 1 - percentile for shorts) standardised within the leg."""
    n = len(extremity)
    if n == 0:
        return np.zeros(0)
    sd = float(np.std(extremity))
    if sd <= 0:
        return equal_weights(n)
    z = (extremity - float(np.mean(extremity))) / sd
    w = np.clip(1.0 + CONVICTION_SLOPE * z, *CONVICTION_BOUNDS)
    return w / w.sum()


def liquidity_limit(weight: np.ndarray, adv: np.ndarray, account: float, share: float = ADV_SHARE) -> np.ndarray:
    """S11: a weight limited to `share` of the name's average daily dollar volume at `account` dollars; no volume figure means no position.
    The excess is not redistributed."""
    room = np.where(np.isfinite(adv) & (adv > 0), share * adv / account, 0.0)
    return np.minimum(weight, room)


def _leg(st: Strategy, idx: np.ndarray, side: int, cs: CrossSection, pct: np.ndarray) -> np.ndarray:
    if st.weight == "invvol":
        return inverse_vol_weights(cs.vol[idx])
    if st.weight == "conviction":
        return conviction_weights(pct[idx] if side > 0 else 1.0 - pct[idx])
    return equal_weights(len(idx))


def select_book(st: Strategy, cs: CrossSection) -> Tuple[np.ndarray, np.ndarray]:
    """(long, short) positions into the cross-section for the strategy's selection rule, after the S10 filter."""
    pct = percentiles(cs.score)
    if st.select == "industry":
        lo, sh = group_neutral_select(cs.score, industry_labels(cs.industry), cs.borrowable)
    elif st.select == "size":
        labels, ok = cap_terciles(cs.cap)
        lo, sh = group_neutral_select(cs.score, labels, cs.borrowable, eligible=ok)
    elif st.select == "decile":
        lo, sh = band_select(pct, cs.borrowable, 0.90, 1.01, 0.10)
    elif st.select == "quintile":
        lo, sh = band_select(pct, cs.borrowable, 0.80, 1.01, 0.20)
    elif st.select == "double":
        lo, sh = count_select(cs.score, cs.borrowable)
    elif st.select == "b1":
        lo, sh = b1_select(pct, cs.borrowable)
    else:
        raise ValueError(f"unknown selection {st.select!r}")
    if st.dtc_max is not None:
        sh = dtc_filter(sh, cs.dtc, st.dtc_max)
    return lo, sh


def strategy_book(st: Strategy, cs: CrossSection) -> np.ndarray:
    """Signed target weights of NAV, one per name of the cross-section: longs sum to LEG, shorts to -LEG (less where S11 limits a name)."""
    pct = percentiles(cs.score)
    lo, sh = select_book(st, cs)
    w = np.zeros(len(cs.score))
    if len(lo):
        w[lo] = LEG * _leg(st, lo, 1, cs, pct)
    if len(sh):
        w[sh] = -LEG * _leg(st, sh, -1, cs, pct)
    if st.adv_share is not None:
        w = np.sign(w) * liquidity_limit(np.abs(w), cs.adv, st.account, st.adv_share)
    return w


def apply_no_trade_band(current: np.ndarray, target: np.ndarray, band: float) -> np.ndarray:
    """S6: a held position whose target is on the same side and within `band` of the target weight keeps its current weight; entries, exits
    and side changes trade."""
    if band <= 0:
        return target
    keep = (current != 0) & (target != 0) & (np.sign(current) == np.sign(target)) & (np.abs(target - current) < band * np.abs(target))
    return np.where(keep, current, target)


# ---------------------------------------------------------------- per-row inputs

def row_stats(ret: np.ndarray, spy: np.ndarray, dollar_volume: np.ndarray, row: int, cols: np.ndarray,
              vol_window: int = 63, beta_window: int = 126, adv_window: int = 21) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(volatility, beta, average daily dollar volume) of `cols` from data up to and including `row`. `ret` is [T, N] with NaN where unknown."""
    with np.errstate(all="ignore"):
        seg = ret[max(0, row - vol_window + 1): row + 1][:, cols]
        vol = np.nanstd(seg, axis=0)
        vol = np.where(np.isfinite(seg).sum(axis=0) >= vol_window // 2, vol, np.nan)
        lo = max(0, row - beta_window + 1)
        r, m = ret[lo: row + 1][:, cols], spy[lo: row + 1]
        ok = np.isfinite(r) & np.isfinite(m)[:, None]
        n = ok.sum(axis=0)
        mm = np.where(ok, m[:, None], np.nan)
        dm = mm - np.nanmean(mm, axis=0)
        dr = np.where(ok, r, np.nan) - np.nanmean(np.where(ok, r, np.nan), axis=0)
        beta = np.nansum(dm * dr, axis=0) / np.nansum(dm * dm, axis=0)
        beta = np.where(n >= beta_window // 2, beta, np.nan)
        adv = np.nanmean(dollar_volume[max(0, row - adv_window + 1): row + 1][:, cols], axis=0)
    return vol, beta, adv


# ---------------------------------------------------------------- simulation

@dataclass
class SimResult:
    block_returns: np.ndarray
    block_turnover: np.ndarray       # traded notional per block as a multiple of NAV
    block_cost: np.ndarray           # per-side trading cost paid in the block, as a share of NAV
    max_drawdown: float
    mean_longs: float
    mean_shorts: float


def rebalance_rows(st: Strategy, start: int, last: int) -> Dict[int, List[int]]:
    """{row: tranches rebalanced on it}. Tranche k starts STAGGER * k sessions after `start` and repeats every `cadence`."""
    out: Dict[int, List[int]] = {}
    for k in range(st.tranches):
        for row in range(start + STAGGER * k, last + 1, st.cadence):
            out.setdefault(row, []).append(k)
    return out


def simulate(st: Strategy, section: Callable[[int], Optional[CrossSection]], ret: np.ndarray, spy: np.ndarray, start: int, n_blocks: int,
             cost_bps: float = COST_BPS_PER_SIDE, fee_bps: float = SHORT_FEE_BPS) -> SimResult:
    """Daily path from `start`. `ret` is [T, N] simple returns (NaN = 0), `spy` the market's. Evaluation rows are start + BLOCK + 1 onward in
    `n_blocks` blocks of BLOCK sessions; earlier rows only build the book. Cost is `cost_bps` on every unit traded; the short fee accrues daily."""
    if st.hedge and st.tranches != 1:
        raise ValueError("the hedge is defined for a single book")
    N = ret.shape[1]
    first_eval = start + BLOCK
    last = first_eval + BLOCK * n_blocks
    due = rebalance_rows(st, start, last)
    pos = np.zeros((st.tranches, N))
    spy_pos = 0.0
    nav = 1.0
    fee_day = fee_bps / 1e4 / BLOCK
    daily = np.zeros(last + 1)
    traded = np.zeros(last + 1)
    cost_paid = np.zeros(last + 1)
    longs: List[int] = []
    shorts: List[int] = []
    r_clean = np.nan_to_num(ret, nan=0.0)
    s_clean = np.nan_to_num(spy, nan=0.0)
    for t in range(start, last + 1):
        nav0 = nav
        if t > start:
            pnl = float((pos * r_clean[t]).sum()) + spy_pos * s_clean[t]
            short_gross = float(-pos[pos < 0].sum())
            pos *= 1.0 + r_clean[t]
            spy_pos *= 1.0 + s_clean[t]
            nav += pnl - fee_day * short_gross
        for k in due.get(t, ()):
            cs = section(t)
            if cs is None:
                continue
            cap_k = nav / st.tranches
            current = pos[k] / cap_k
            target = np.zeros(N)
            target[cs.cols] = strategy_book(st, cs)
            target = apply_no_trade_band(current, target, st.band)
            trade = float(np.abs(target - current).sum()) * cap_k
            pos[k] = target * cap_k
            if st.hedge:
                beta = np.where(np.isfinite(cs.beta), cs.beta, 1.0)
                hedge_target = -float((target[cs.cols] * beta).sum()) * cap_k
                trade += abs(hedge_target - spy_pos)
                spy_pos = hedge_target
            c = cost_bps / 1e4 * trade
            nav -= c
            traded[t] += trade / nav0
            cost_paid[t] += c / nav0
            if t > first_eval:
                longs.append(int((target > 0).sum()))
                shorts.append(int((target < 0).sum()))
        daily[t] = nav / nav0 - 1.0
    rows = np.arange(first_eval + 1, last + 1)
    d = daily[rows]
    blocks = (1.0 + d).reshape(n_blocks, BLOCK).prod(axis=1) - 1.0
    nav_path = np.cumprod(1.0 + d)
    peak = np.maximum.accumulate(np.concatenate([[1.0], nav_path]))[1:]
    return SimResult(blocks, traded[rows].reshape(n_blocks, BLOCK).sum(axis=1), cost_paid[rows].reshape(n_blocks, BLOCK).sum(axis=1),
                     float((nav_path / peak - 1.0).min()) if len(d) else 0.0,
                     float(np.mean(longs)) if longs else 0.0, float(np.mean(shorts)) if shorts else 0.0)


# ---------------------------------------------------------------- statistics

def sharpe(blocks: Sequence[float]) -> float:
    """Net Sharpe of 21-session block returns, annualised by sqrt(252 / 21), as the earlier trials report it."""
    r = np.asarray(blocks, dtype=float)
    if len(r) < 3 or np.std(r) == 0:
        return 0.0
    return float(np.mean(r) / np.std(r, ddof=1) * math.sqrt(252.0 / BLOCK))


def at_cost(res: SimResult, cost_bps: float, base_bps: float = COST_BPS_PER_SIDE) -> np.ndarray:
    """Block returns re-priced at another per-side rate (the cost is additive within a block; compounding inside it is ignored)."""
    return res.block_returns + res.block_cost * (1.0 - cost_bps / base_bps)


def _weighted_sharpe(x: np.ndarray, w: np.ndarray) -> np.ndarray:
    """Sharpe per row of x [rows, K] under integer weights w [rows, K]; NaN in x carries weight 0."""
    w = np.where(np.isfinite(x), w, 0.0)
    xz = np.where(np.isfinite(x), x, 0.0)
    n = w.sum(axis=-1)
    m = (w * xz).sum(axis=-1) / np.maximum(n, 1)
    var = (w * (xz - m[..., None]) ** 2).sum(axis=-1) / np.maximum(n - 1, 1)
    with np.errstate(all="ignore"):
        out = m / np.sqrt(var) * math.sqrt(252.0 / BLOCK)
    return np.where((n >= 3) & (var > 0), out, 0.0)


def paired_bootstrap(variant: Sequence[np.ndarray], base: Sequence[np.ndarray], bins: Sequence[np.ndarray],
                     draws: int = 10_000, seed: int = 20261113, chunk: int = 1000) -> Dict[str, float]:
    """Mean over calendars of (variant Sharpe - base Sharpe) and its one-sided date-cluster bootstrap p. Blocks are grouped by `bins` (calendar
    time, shared across calendars) and the bins are resampled with replacement; p is the share of draws whose deviation from the observed
    mean reaches the observed mean (the draws re-centred on it)."""
    C = len(variant)
    uniq = np.unique(np.concatenate([np.asarray(b) for b in bins]))
    K = len(uniq)
    col = {int(b): i for i, b in enumerate(uniq)}
    V, B = np.full((C, K), np.nan), np.full((C, K), np.nan)
    for c in range(C):
        for r_v, r_b, b in zip(variant[c], base[c], bins[c]):
            V[c, col[int(b)]], B[c, col[int(b)]] = r_v, r_b
    ones = np.ones((C, K))
    obs = float(np.mean(_weighted_sharpe(V, ones) - _weighted_sharpe(B, ones)))
    rng = np.random.default_rng(seed)
    boot = np.empty(draws)
    for s in range(0, draws, chunk):
        m = min(chunk, draws - s)
        counts = np.stack([np.bincount(rng.integers(0, K, K), minlength=K) for _ in range(m)]).astype(float)   # [m, K]
        w = counts[:, None, :]
        d = _weighted_sharpe(np.broadcast_to(V, (m, C, K)), np.broadcast_to(w, (m, C, K))) \
            - _weighted_sharpe(np.broadcast_to(B, (m, C, K)), np.broadcast_to(w, (m, C, K)))
        boot[s: s + m] = d.mean(axis=1)
    p = float(np.mean((boot - obs) >= obs))
    return {"mean_diff": obs, "p": p, "n_bins": float(K)}


def bh_adjusted(pvalues: Sequence[float]) -> List[float]:
    """Benjamini-Hochberg adjusted p-values (q-values), in the input order."""
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    q = [1.0] * m
    running = 1.0
    for rank in range(m, 0, -1):
        i = order[rank - 1]
        running = min(running, pvalues[i] * m / rank)
        q[i] = running
    return q


__all__ = ["BAND_FRACTION", "BLOCK", "CAPACITY_CELLS", "COST_BPS_PER_SIDE", "CrossSection", "LEG", "S0", "STRATEGIES", "SimResult", "Strategy",
           "apply_no_trade_band", "at_cost", "b1_select", "bh_adjusted", "blend_scores", "cap_terciles", "conviction_weights", "count_select",
           "dtc_filter", "equal_weights", "group_neutral_select", "industry_labels", "inverse_vol_weights", "liquidity_limit",
           "paired_bootstrap", "percentiles", "rebalance_rows", "row_stats", "select_book", "sharpe", "simulate", "strategy_book"]
