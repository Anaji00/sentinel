"""Shared pieces of the registered offline trials of `reports/Quant methods and trading bot research - 18 Nov.md` section 4, run 19 Nov 2026.
Simulation and throwaway model hooks only: nothing here touches the live model, trader, sizing, caps, kill switch or execution, and
`shared/utils/factor_model.py` is not edited (the extra `training_target` modes and the seed-bagged model are installed into the imported module
at run time by `install_hooks`, in the process of the trial only).

Details the report left open, fixed here BEFORE any trial ran (written 19 Nov 2026, 05:5x UTC):

* Fee conversion. A flat fee of F% a year on short notional is charged as F * 100 / 12 basis points per 21-session block on short notional (the
  existing simulator's `fee_bps` unit: 44 bps a block is 5.3% a year), accrued daily. 0.5% = 4.17, 2.0% = 16.67, 5.3% = 44.0 (the stored cells' value).
* Common calendars. A cell whose first scored session is later than S0's is compared with S0 re-simulated on the same start rows: each calendar
  starts at `calendar_start(max(first scored row of the cell, of S0), offset)`. In this run all cells start at the same row, so this changes nothing.
* Paired statistic and BH. As the 13 Nov trial: per calendar, variant Sharpe minus S0 Sharpe (net, 21-session blocks, sqrt(12)), mean over calendars,
  one-sided date-cluster bootstrap p (10,000 draws, seed 20261113). BH q is computed over the 17 re-run cells plus every new cell that has a p.
* Trial count for the deflated Sharpe is 18 (S0 and the 17 cells) plus the new cells actually run.
* Block matrices for PBO are keyed by absolute block-end row; cells are aligned on the rows they share.
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils import book_strategies as bs  # noqa: E402

OFFSETS = [0, 5, 10, 15, 2, 7, 12, 17]
EXTRA_OFFSETS = [1, 4, 6, 9, 11, 13, 16, 19]          # T8: eight more start offsets, so that there are sixteen start dates
MIN_HISTORY = 260
BLOCK = bs.BLOCK
FEE_BPS = {"0.5%": 0.5 * 100 / 12, "2.0%": 2.0 * 100 / 12, "5.3%": 44.0}
BOOTSTRAP_SEED = 20261113
BOOTSTRAP_DRAWS = 10_000
BH_Q = 0.10
DD_TRIP = 0.15                      # T7: de-gross at this drawdown from the trailing peak
DD_RECOVER = 0.5                    # T7: re-gross once half of the drawdown is recovered
DD_GROSS = 0.5
VETO_WINDOW = 3                     # sessions an event keeps a name out of a rebuilt book (the live trader looks back three days)
VETO_8K_ITEMS = ("3.01", "1.03", "4.02")
VETO_Z = 3.0
VETO_RET_DAYS = 5
VETO_Z_WINDOW = 252


# ---------------------------------------------------------------- calendars

def calendar_start(first_pred: int, offset: int) -> int:
    """First monthly rebalance row of a calendar (MIN_HISTORY + offset + 21 k) at or after the first scored row."""
    base = MIN_HISTORY + offset
    k = max(0, math.ceil((first_pred - base) / BLOCK))
    return base + k * BLOCK


def n_blocks_for(start: int, T: int) -> int:
    return (T - 1 - (start + BLOCK)) // BLOCK


# ---------------------------------------------------------------- the simulator with the T5 and T7 hooks

def simulate_ext(st: bs.Strategy, section: Callable[[int], Optional[bs.CrossSection]], ret: np.ndarray, spy: np.ndarray, start: int, n_blocks: int,
                 cost_bps: float = bs.COST_BPS_PER_SIDE, fee_bps: float = 44.0, veto_long: Optional[np.ndarray] = None,
                 veto_short: Optional[np.ndarray] = None, dd_policy: bool = False) -> bs.SimResult:
    """`bs.simulate` with two optional hooks, and otherwise the same loop, costs and block cut (checked equal in the selftest).

    veto_long / veto_short: [T, N] bool, True where an exit order for a long (short) in that name executes at that session's close. A held
    position is closed there (cost charged) and stays flat until the next rebuild; a rebuild at row r leaves out a name whose veto fell in
    rows r - VETO_WINDOW + 1 .. r on that side (the live trader's three-day lookback).
    dd_policy: T7. After each day's return, if NAV is DD_TRIP below its peak the book is cut to DD_GROSS of its size (cost charged on what is
    sold); it is restored to full size once NAV has recovered DD_RECOVER of the fall from the peak to the trough. Rebuilds while cut are made at
    DD_GROSS of the target."""
    if st.hedge and st.tranches != 1:
        raise ValueError("the hedge is defined for a single book")
    N = ret.shape[1]
    first_eval = start + BLOCK
    last = first_eval + BLOCK * n_blocks
    due = bs.rebalance_rows(st, start, last)
    pos = np.zeros((st.tranches, N))
    spy_pos = 0.0
    nav = 1.0
    peak = 1.0
    trough = 1.0
    gross_mult = 1.0
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
            # veto exits at this close
            if veto_long is not None or veto_short is not None:
                sold = 0.0
                if veto_long is not None and veto_long[t].any():
                    hit = (pos > 0) & veto_long[t][None, :]
                    sold += float(pos[hit].sum())
                    pos[hit] = 0.0
                if veto_short is not None and veto_short[t].any():
                    hit = (pos < 0) & veto_short[t][None, :]
                    sold += float(-pos[hit].sum())
                    pos[hit] = 0.0
                if sold > 0:
                    c = cost_bps / 1e4 * sold
                    nav -= c
                    traded[t] += sold / nav0
                    cost_paid[t] += c / nav0
            if dd_policy:
                peak = max(peak, nav)
                if gross_mult == 1.0 and nav / peak - 1.0 <= -DD_TRIP:
                    sold = float(np.abs(pos).sum()) * (1.0 - DD_GROSS)
                    pos *= DD_GROSS
                    c = cost_bps / 1e4 * sold
                    nav -= c
                    traded[t] += sold / nav0
                    cost_paid[t] += c / nav0
                    gross_mult, trough = DD_GROSS, nav
                elif gross_mult < 1.0:
                    trough = min(trough, nav)
                    if nav >= trough + DD_RECOVER * (peak - trough):
                        bought = float(np.abs(pos).sum()) * (1.0 / gross_mult - 1.0)
                        pos /= gross_mult
                        c = cost_bps / 1e4 * bought
                        nav -= c
                        traded[t] += bought / nav0
                        cost_paid[t] += c / nav0
                        gross_mult = 1.0
        for k in due.get(t, ()):
            cs = section(t)
            if cs is None:
                continue
            cap_k = nav / st.tranches
            current = pos[k] / cap_k
            target = np.zeros(N)
            target[cs.cols] = bs.strategy_book(st, cs) * gross_mult
            if veto_long is not None:
                recent = veto_long[max(0, t - VETO_WINDOW + 1): t + 1].any(axis=0)
                target = np.where((target > 0) & recent, 0.0, target)
            if veto_short is not None:
                recent = veto_short[max(0, t - VETO_WINDOW + 1): t + 1].any(axis=0)
                target = np.where((target < 0) & recent, 0.0, target)
            target = bs.apply_no_trade_band(current, target, st.band)
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
    pk = np.maximum.accumulate(np.concatenate([[1.0], nav_path]))[1:]
    return bs.SimResult(blocks, traded[rows].reshape(n_blocks, BLOCK).sum(axis=1), cost_paid[rows].reshape(n_blocks, BLOCK).sum(axis=1),
                        float((nav_path / pk - 1.0).min()) if len(d) else 0.0,
                        float(np.mean(longs)) if longs else 0.0, float(np.mean(shorts)) if shorts else 0.0)


# ---------------------------------------------------------------- inputs and providers

@dataclass
class Inputs:
    """Everything the simulations read from the panel, saved by trial_quant_scores.py so that the trials need no database."""
    dates: List[str]
    tickers: List[str]
    close: np.ndarray            # [T, N] float32
    ret: np.ndarray              # [T, N] simple returns, NaN unknown
    spy: np.ndarray
    dv: np.ndarray               # close * volume
    industry: np.ndarray
    dtc: Optional[np.ndarray]
    borrowable: np.ndarray
    caps: np.ndarray
    flag: np.ndarray             # [T, N] bool: flat (open=high=low=close) or zero-volume bar that session
    tradable: np.ndarray         # [T, N] bool: the model's tradable mask (before the flat exclusion)

    @staticmethod
    def load(path: Path) -> "Inputs":
        z = np.load(path, allow_pickle=True)
        dtc = z["dtc"] if z["has_dtc"] else None
        return Inputs(list(z["dates"]), list(z["tickers"]), z["close"], z["ret"].astype(np.float64), z["spy"].astype(np.float64), z["dv"],
                      z["industry"], dtc, z["borrowable"], z["caps"], z["flag"], z["tradable"])


Scores = Dict[int, Tuple[np.ndarray, np.ndarray]]


class Market:
    """Builds `CrossSection`s from a scores dict {row: (cols, pred)} for one model run."""

    def __init__(self, inp: Inputs):
        self.inp = inp

    def provider(self, scores: Scores, average_sessions: int = 1) -> Callable[[int], Optional[bs.CrossSection]]:
        """The base provider; with average_sessions k > 1, each name's score is the mean of its percentiles over the rebuild row and the
        k - 1 sessions before it (names present in the rebuild row only; sessions with no score are skipped)."""
        inp = self.inp
        cache: Dict[int, Optional[bs.CrossSection]] = {}

        def pct_map(row: int) -> Optional[Dict[int, float]]:
            hit = scores.get(row)
            if hit is None:
                return None
            cols, pred = hit
            p = bs.percentiles(np.asarray(pred, dtype=np.float64))
            return dict(zip(np.asarray(cols).tolist(), p.tolist()))

        def get(row: int) -> Optional[bs.CrossSection]:
            if row not in scores:
                return None
            if row not in cache:
                cols, pred = scores[row]
                cols = np.asarray(cols)
                score = np.asarray(pred, dtype=np.float64)
                if average_sessions > 1:
                    maps = [m for m in (pct_map(row - k) for k in range(average_sessions)) if m is not None]
                    score = np.array([np.mean([m[c] for m in maps if c in m]) for c in cols.tolist()])
                vol, beta, adv = bs.row_stats(inp.ret, inp.spy, inp.dv, row, cols)
                cache[row] = bs.CrossSection(cols, score, inp.borrowable[cols], inp.industry[cols], inp.caps[cols], vol,
                                             inp.dtc[row, cols] if inp.dtc is not None else np.full(len(cols), np.nan), adv, beta)
            return cache[row]
        return get

    def blend(self, base: Callable[[int], Optional[bs.CrossSection]], other: Scores) -> Callable[[int], Optional[bs.CrossSection]]:
        """S9: the mean percentile of the base and a second model's scores over the names both rank."""
        from shared.utils import factor_model as fm

        cache: Dict[int, Optional[bs.CrossSection]] = {}

        def get(row: int) -> Optional[bs.CrossSection]:
            if row in cache:
                return cache[row]
            cs = base(row)
            out = None
            if cs is not None and row in other:
                cols_b, pred_b = other[row]
                common, ia, ib = np.intersect1d(cs.cols, np.asarray(cols_b), return_indices=True)
                if len(common) >= fm.MIN_NAMES:
                    out = bs.CrossSection(common, bs.blend_scores(cs.score[ia], np.asarray(pred_b, dtype=np.float64)[ib]), cs.borrowable[ia],
                                          cs.industry[ia], cs.cap[ia], cs.vol[ia], cs.dtc[ia], cs.adv[ia], cs.beta[ia])
            cache[row] = out
            return out
        return get


# ---------------------------------------------------------------- running a cell on the calendars

@dataclass
class CellRun:
    name: str
    offsets: List[int]
    starts: Dict[int, int]
    n_blocks: Dict[int, int]
    results: Dict[int, bs.SimResult]


def run_cell(name: str, st: bs.Strategy, provider: Callable[[int], Optional[bs.CrossSection]], inp: Inputs, first_pred: int,
             offsets: Sequence[int] = OFFSETS, fee_bps: float = 44.0, **hooks) -> CellRun:
    """One cell on every calendar. A calendar too short for six blocks is skipped (counted by its absence)."""
    T = len(inp.dates)
    starts, nbl, res = {}, {}, {}
    for off in offsets:
        start = calendar_start(first_pred, off)
        n = n_blocks_for(start, T)
        if n < 6:
            continue
        starts[off], nbl[off] = start, n
        res[off] = simulate_ext(st, provider, inp.ret, inp.spy, start, n, fee_bps=fee_bps, **hooks)
    return CellRun(name, [o for o in offsets if o in res], starts, nbl, res)


def paired(cell: CellRun, base: CellRun, base_bin: int, draws: int = BOOTSTRAP_DRAWS) -> dict:
    """Per-calendar Sharpe of the cell and of S0 on the same calendars, wins, mean difference, bootstrap p, and the drawdown comparisons."""
    offs = [o for o in cell.offsets if o in base.results and base.starts[o] == cell.starts[o] and base.n_blocks[o] == cell.n_blocks[o]]
    rows = {}
    for o in offs:
        r, b = cell.results[o], base.results[o]
        rows[o] = {"sharpe": bs.sharpe(r.block_returns), "sharpe_s0": bs.sharpe(b.block_returns), "mean_net": float(np.mean(r.block_returns)),
                   "max_dd": r.max_drawdown, "max_dd_s0": b.max_drawdown, "worst_block": float(np.min(r.block_returns)),
                   "worst_block_s0": float(np.min(b.block_returns)), "turnover": float(np.mean(r.block_turnover))}
    d = [rows[o]["sharpe"] - rows[o]["sharpe_s0"] for o in offs]
    out = {"calendars": len(offs), "per_calendar": {str(o): rows[o] for o in offs}, "wins": int(sum(x > 0 for x in d)),
           "losses_by_0_10": int(sum(x <= -0.10 for x in d)), "wins_by_0_10": int(sum(x >= 0.10 for x in d)),
           "mean_diff": float(np.mean(d)) if d else float("nan"), "mean_sharpe": float(np.mean([rows[o]["sharpe"] for o in offs])) if offs else float("nan"),
           "mean_sharpe_s0": float(np.mean([rows[o]["sharpe_s0"] for o in offs])) if offs else float("nan"),
           "max_dd": float(np.mean([rows[o]["max_dd"] for o in offs])) if offs else float("nan"),
           "max_dd_s0": float(np.mean([rows[o]["max_dd_s0"] for o in offs])) if offs else float("nan"),
           "worst_block": float(np.mean([rows[o]["worst_block"] for o in offs])) if offs else float("nan"),
           "worst_block_s0": float(np.mean([rows[o]["worst_block_s0"] for o in offs])) if offs else float("nan"),
           "turnover": float(np.mean([rows[o]["turnover"] for o in offs])) if offs else float("nan")}
    if offs:
        bins = [(cell.starts[o] + BLOCK + 1 + BLOCK * np.arange(cell.n_blocks[o]) - base_bin) // BLOCK for o in offs]
        pb = bs.paired_bootstrap([cell.results[o].block_returns for o in offs], [base.results[o].block_returns for o in offs], bins,
                                 draws=draws, seed=BOOTSTRAP_SEED)
        out["p"] = pb["p"]
    return out


def block_series(cell: CellRun, offset: int) -> Tuple[np.ndarray, np.ndarray]:
    """(block-end rows, block returns) of a cell on one calendar."""
    start = cell.starts[offset]
    rows = start + BLOCK + BLOCK * (1 + np.arange(cell.n_blocks[offset]))
    return rows, cell.results[offset].block_returns


# ---------------------------------------------------------------- T10: PBO and the minimum track record

def pbo_cscv(matrix: np.ndarray, slices: int = 16) -> dict:
    """Probability of backtest overfitting by combinatorially symmetric cross-validation (Bailey, Borwein, Lopez de Prado and Zhu).
    `matrix` is [blocks, cells] of net block returns. The first blocks beyond a multiple of `slices` are dropped. Performance is the Sharpe of
    the pooled blocks. Returns PBO (share of splits where the in-sample best cell ranks at or below the OOS median), the logits, the
    slope of OOS on IS performance for the in-sample best, and the share of splits where it loses money out of sample."""
    from itertools import combinations

    T, K = matrix.shape
    per = T // slices
    if per < 2 or K < 3:
        return {"error": "too little data", "blocks": T, "cells": K}
    m = matrix[T - per * slices:]
    sl = m.reshape(slices, per, K)
    n = np.full((slices, K), float(per))
    s1, s2 = sl.sum(axis=1), (sl ** 2).sum(axis=1)

    def sharpe_of(idx: Sequence[int]) -> np.ndarray:
        nn, a, b = n[list(idx)].sum(axis=0), s1[list(idx)].sum(axis=0), s2[list(idx)].sum(axis=0)
        mean = a / nn
        var = (b - nn * mean ** 2) / (nn - 1)
        with np.errstate(all="ignore"):
            return np.where(var > 0, mean / np.sqrt(var), 0.0)

    logits, is_best, oos_best, ranks = [], [], [], []
    everything = set(range(slices))
    for combo in combinations(range(slices), slices // 2):
        rest = sorted(everything - set(combo))
        ins, oos = sharpe_of(combo), sharpe_of(rest)
        best = int(np.argmax(ins))
        rank = float((oos < oos[best]).sum() + 0.5 * ((oos == oos[best]).sum() - 1)) + 1.0     # 1 = worst, K = best (ties share)
        w = rank / (K + 1.0)
        logits.append(math.log(w / (1.0 - w)))
        is_best.append(float(ins[best]))
        oos_best.append(float(oos[best]))
        ranks.append(rank)
    lg, x, y = np.array(logits), np.array(is_best), np.array(oos_best)
    slope = float(np.polyfit(x, y, 1)[0]) if np.std(x) > 0 else float("nan")
    r2 = float(np.corrcoef(x, y)[0, 1] ** 2) if np.std(x) > 0 and np.std(y) > 0 else float("nan")
    return {"pbo": float(np.mean(lg <= 0)), "splits": len(lg), "blocks_used": per * slices, "cells": K, "logit_mean": float(lg.mean()),
            "logit_median": float(np.median(lg)), "logit_quartiles": [float(np.percentile(lg, 25)), float(np.percentile(lg, 75))],
            "slope_oos_on_is": slope, "r2": r2, "prob_loss_oos": float(np.mean(y < 0)), "mean_oos_of_is_best": float(y.mean()),
            "mean_is_of_is_best": float(x.mean()), "mean_rank_of_is_best": float(np.mean(ranks))}


def min_track_record(blocks: np.ndarray, sr_target: float = 0.0, z: float = 1.645) -> dict:
    """Bailey and Lopez de Prado's minimum track record length, in 21-session blocks and years, for the sample Sharpe against `sr_target`
    (per block), one-sided 95%: 1 + (1 - skew SR + (kurt - 1) / 4 SR^2) (z / (SR - SR*))^2. Also the normal-returns figure (z / SR)^2."""
    r = np.asarray(blocks, dtype=float)
    sd = float(r.std(ddof=1))
    sr = float(r.mean() / sd)
    cen = r - r.mean()
    m2 = float(np.mean(cen ** 2))
    skew, kurt = float(np.mean(cen ** 3) / m2 ** 1.5), float(np.mean(cen ** 4) / m2 ** 2)
    out = {"sr_block": sr, "sr_annual": sr * math.sqrt(12), "skew": skew, "kurtosis": kurt, "blocks_observed": len(r)}
    gap = sr - sr_target
    if gap <= 0:
        out.update({"min_blocks": None, "min_years": None, "normal_years": None})
        return out
    factor = 1.0 - skew * sr + (kurt - 1.0) / 4.0 * sr * sr
    blocks_needed = 1.0 + factor * (z / gap) ** 2
    out.update({"min_blocks": blocks_needed, "min_years": blocks_needed * BLOCK / 252.0, "normal_years": (z / gap) ** 2 * BLOCK / 252.0,
                "variance_factor": factor})
    return out


# ---------------------------------------------------------------- T5 veto matrices

def divergence_events(inp: Inputs, ret_days: int = VETO_RET_DAYS, z_window: int = VETO_Z_WINDOW, z_min: float = VETO_Z) -> Tuple[np.ndarray, np.ndarray]:
    """(veto_long, veto_short) [T, N] for the divergence veto. A name's spread is its `ret_days`-session return minus the median of its
    industry's (industries under MIN_INDUSTRY_GROUP names pooled with the unknown ones as one group); z is the spread over the standard
    deviation of that name's own spread in the trailing `z_window` sessions (mean not removed). z at or below -z_min vetoes a long,
    at or above +z_min a short. Seen at the close of row t, executed at the close of row t + 1."""
    T, N = inp.ret.shape
    r = np.nan_to_num(inp.ret, nan=0.0)
    cum = np.cumsum(np.log1p(np.clip(r, -0.99, None)), axis=0)
    r5 = np.full((T, N), np.nan)
    r5[ret_days:] = np.expm1(cum[ret_days:] - cum[:-ret_days])
    r5 = np.where(np.isfinite(inp.close), r5, np.nan)
    labels = bs.industry_labels(inp.industry)
    spread = np.full((T, N), np.nan)
    for g in np.unique(labels):
        idx = np.flatnonzero(labels == g)
        with np.errstate(all="ignore"):
            med = np.nanmedian(r5[:, idx], axis=1)
        spread[:, idx] = r5[:, idx] - med[:, None]
    ok = np.isfinite(spread)
    x = np.where(ok, spread, 0.0)
    c2, cn = np.cumsum(x * x, axis=0), np.cumsum(ok, axis=0).astype(float)

    def window(c: np.ndarray) -> np.ndarray:
        out = c.copy()
        out[z_window:] = c[z_window:] - c[:-z_window]
        return out
    s2, n = window(c2), window(cn)
    with np.errstate(all="ignore"):
        sd = np.sqrt(np.maximum(s2 / n, 0.0))
        z = np.where((n >= z_window // 2) & (sd > 0), spread / sd, np.nan)
    long_seen = np.isfinite(z) & (z <= -z_min)
    short_seen = np.isfinite(z) & (z >= z_min)
    vl, vs = np.zeros((T, N), dtype=bool), np.zeros((T, N), dtype=bool)
    vl[1:], vs[1:] = long_seen[:-1], short_seen[:-1]
    return vl, vs


def eight_k_events(inp: Inputs, filings: Sequence[Tuple[str, "object"]]) -> np.ndarray:
    """veto_long [T, N] for the 8-K veto applied to longs only. `filings` are (ticker, acceptance time as a timezone-aware datetime) of 8-Ks
    naming item 3.01, 1.03 or 4.02. A filing accepted before 16:00 New York time is seen at that day's close, later ones at the next session's;
    the exit executes at the close after that (one session of delay). Filings of names not in the panel are ignored."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    T, N = len(inp.dates), len(inp.tickers)
    col = {t: j for j, t in enumerate(inp.tickers)}
    days = np.array([np.datetime64(d) for d in inp.dates])
    ny = ZoneInfo("America/New_York")
    out = np.zeros((T, N), dtype=bool)
    for ticker, ts in filings:
        j = col.get(str(ticker).upper())
        if j is None:
            continue
        if isinstance(ts, str):
            from datetime import datetime
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        local = ts.astimezone(ny)
        d = local.date() + (timedelta(days=1) if local.hour >= 16 else timedelta(0))
        seen = int(np.searchsorted(days, np.datetime64(d)))         # first session on or after that date
        if seen + 1 < T:
            out[seen + 1, j] = True
    return out


# ---------------------------------------------------------------- deflated Sharpe

def deflated(cell_blocks: Dict[str, np.ndarray], target: str, n_trials: int) -> Optional[dict]:
    """Deflated Sharpe of `target`'s pooled block returns among all cells' pooled per-block Sharpes, counting `n_trials`."""
    from shared.utils import factor_model as fm

    r = cell_blocks[target]
    sd = float(r.std(ddof=1))
    if len(r) < 8 or sd <= 0:
        return None
    cen = r - r.mean()
    m2 = float(np.mean(cen ** 2)) or 1e-18
    trials = [float(v.mean() / v.std(ddof=1)) for v in cell_blocks.values() if len(v) > 8 and v.std(ddof=1) > 0]
    per_cal = len(r) / 8.0
    dsr = fm.deflated_sharpe(float(r.mean() / sd), int(per_cal), float(np.mean(cen ** 3) / m2 ** 1.5), float(np.mean(cen ** 4) / m2 ** 2), trials,
                             n_trials=n_trials)
    return {"cell": target, "dsr": dsr, "per_block_sr": float(r.mean() / sd), "periods_used": int(per_cal), "trials": n_trials}
