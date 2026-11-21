"""Registered book strategy trial: S1-S11 against B1 on the eight RB1 calendars (Upgrade pre-registrations - 12 Nov, "Book strategy trial"). Read-only,
simulation only. Run in its own capped container, never inside a service container, and not between 22:00 and 06:00 UTC (it builds the whole panel):

    MSYS_NO_PATHCONV=1 docker run --rm --name sentinel-book-trials --memory 8g --network sentinel-network --env-file .env \
        -v <repo>/shared:/app/shared:ro -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app \
        sentinel-agents-heavy python scripts/run_book_strategies.py [--offsets 0,5,10,15,2,7,12,17] [--out /tmp/book.json]

`--selftest` runs the whole pipeline on a synthetic market with a planted signal and needs no database.
Bar: beats S0's net Sharpe on at least 6 of 8 calendars, by at least 0.10 on average, date-bootstrap one-sided p < 0.05, and survives
Benjamini-Hochberg at q < 0.10 over the 17 cells.
"""
import argparse
import asyncio
import json
import logging
import math
import sys
from pathlib import Path
from typing import Callable, Dict, Optional, Sequence

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils import book_strategies as bs  # noqa: E402
from shared.utils.quiet_failures import dropped, swallowed  # noqa: E402

logger = logging.getLogger("scripts.run_book_strategies")
BAR_CALENDARS = 6
BAR_MEAN_GAIN = 0.10
BAR_P = 0.05
BH_Q = 0.10
OFFSETS = [0, 5, 10, 15, 2, 7, 12, 17]
MIN_HISTORY = 260                      # factor_model.MIN_HISTORY: the calendars' row origin
Provider = Callable[[int], Optional[bs.CrossSection]]


def calendar_start(first_pred: int, offset: int) -> int:
    """First monthly rebalance row of a calendar (MIN_HISTORY + offset + 21 k) at or after the first scored row."""
    base = MIN_HISTORY + offset
    k = max(0, math.ceil((first_pred - base) / bs.BLOCK))
    return base + k * bs.BLOCK


def run_cells(cells: Sequence[bs.Strategy], providers: Dict[str, Provider], ret: np.ndarray, spy: np.ndarray, offsets: Sequence[int],
              first_pred: int, T: int) -> Dict[str, Dict[int, Optional[dict]]]:
    """{cell: {offset: result}}; a failing cell/calendar is counted and left None, never aborting the run."""
    out: Dict[str, Dict[int, Optional[dict]]] = {c.name: {} for c in cells}
    for off in offsets:
        start = calendar_start(first_pred, off)
        n_blocks = (T - 1 - (start + bs.BLOCK)) // bs.BLOCK
        if n_blocks < 6:
            dropped("scripts.run_book_strategies.short_calendar", f"offset {off}: {n_blocks} blocks", logger)
            continue
        for st in cells:
            try:
                res = bs.simulate(st, providers.get(st.name, providers["base"]), ret, spy, start, n_blocks)
                out[st.name][off] = {"res": res, "start": start, "n_blocks": n_blocks}
            except Exception as exc:
                swallowed("scripts.run_book_strategies.cell", exc, logger, detail=f"{st.name} offset {off}", variant=st.name)
                out[st.name][off] = None
        print(f"calendar {off}: " + " ".join(f"{c.name}={bs.sharpe(out[c.name][off]['res'].block_returns):.2f}"
                                             for c in cells if out[c.name].get(off)), flush=True)
    return out


def analyse(raw: Dict[str, Dict[int, Optional[dict]]], cells: Sequence[bs.Strategy], draws: int, base_bin: int) -> Dict[str, dict]:
    """Per-cell summary against S0 with the bar's three conditions; BH is applied by the caller over the test cells."""
    summary: Dict[str, dict] = {}
    s0 = raw["S0"]
    for st in cells:
        per = raw[st.name]
        offs = [o for o in per if per[o] and s0.get(o)]
        rows = {}
        for o in offs:
            r, b = per[o]["res"], s0[o]["res"]
            rows[o] = {"sharpe": bs.sharpe(r.block_returns), "sharpe_s0": bs.sharpe(b.block_returns),
                       "sharpe_5bps": bs.sharpe(bs.at_cost(r, 5.0)), "sharpe_s0_5bps": bs.sharpe(bs.at_cost(b, 5.0)),
                       "mean_net": float(np.mean(r.block_returns)), "turnover": float(np.mean(r.block_turnover)),
                       "max_dd": r.max_drawdown, "longs": r.mean_longs, "shorts": r.mean_shorts}
        d = [rows[o]["sharpe"] - rows[o]["sharpe_s0"] for o in offs]
        d5 = [rows[o]["sharpe_5bps"] - rows[o]["sharpe_s0_5bps"] for o in offs]
        entry = {"calendars": len(offs), "per_calendar": {str(o): rows[o] for o in offs}, "wins": int(sum(x > 0 for x in d)),
                 "mean_diff": float(np.mean(d)) if d else float("nan"), "mean_diff_5bps": float(np.mean(d5)) if d5 else float("nan"),
                 "mean_sharpe": float(np.mean([rows[o]["sharpe"] for o in offs])) if offs else float("nan"),
                 "mean_net_block": float(np.mean([rows[o]["mean_net"] for o in offs])) if offs else float("nan"),
                 "turnover": float(np.mean([rows[o]["turnover"] for o in offs])) if offs else float("nan"),
                 "max_dd": float(np.mean([rows[o]["max_dd"] for o in offs])) if offs else float("nan"),
                 "longs": float(np.mean([rows[o]["longs"] for o in offs])) if offs else float("nan"),
                 "shorts": float(np.mean([rows[o]["shorts"] for o in offs])) if offs else float("nan")}
        if st.name != "S0" and offs:
            bins = [(per[o]["start"] + bs.BLOCK + 1 + bs.BLOCK * np.arange(per[o]["n_blocks"]) - base_bin) // bs.BLOCK for o in offs]
            pb = bs.paired_bootstrap([per[o]["res"].block_returns for o in offs], [s0[o]["res"].block_returns for o in offs], bins, draws=draws)
            entry["p"] = pb["p"]
        summary[st.name] = entry
    return summary


def verdicts(summary: Dict[str, dict], tests: Sequence[str], n_calendars: int) -> None:
    """Adds the BH q-value and the candidate flags in place. A cell with fewer calendars than the bar needs cannot be a candidate."""
    ps = [summary[n].get("p", 1.0) for n in tests]
    for n, q in zip(tests, bs.bh_adjusted(ps)):
        s = summary[n]
        need = min(BAR_CALENDARS, n_calendars)
        s["q"] = q
        s["bar_wins"] = s["calendars"] >= need and s["wins"] >= need
        s["bar_gain"] = s["mean_diff"] >= BAR_MEAN_GAIN
        s["bar_p"] = s.get("p", 1.0) < BAR_P
        s["candidate"] = bool(s["bar_wins"] and s["bar_gain"] and s["bar_p"] and q < BH_Q)


def deflated(raw: Dict[str, Dict[int, Optional[dict]]], summary: Dict[str, dict], tests: Sequence[str]) -> Optional[dict]:
    """Deflated-Sharpe caution for the test cell with the largest mean paired gain (18 trials: the 17 cells and S0)."""
    from shared.utils import factor_model as fm

    def pooled(name: str) -> np.ndarray:
        return np.concatenate([v["res"].block_returns for v in raw[name].values() if v])

    ranked = [n for n in tests if summary[n]["calendars"]]
    if not ranked:
        return None
    best = max(ranked, key=lambda n: summary[n]["mean_diff"])
    r = pooled(best)
    sd = float(r.std(ddof=1))
    if len(r) < 8 or sd <= 0:
        return None
    cen = r - r.mean()
    m2 = float(np.mean(cen ** 2)) or 1e-18
    trials = [float(pooled(n).mean() / pooled(n).std(ddof=1)) for n in ["S0", *tests] if summary[n]["calendars"] and pooled(n).std(ddof=1) > 0]
    per_calendar = len(r) / max(1, summary[best]["calendars"])
    dsr = fm.deflated_sharpe(float(r.mean() / sd), int(per_calendar), float(np.mean(cen ** 3) / m2 ** 1.5), float(np.mean(cen ** 4) / m2 ** 2),
                             trials, n_trials=len(tests) + 1)
    return {"cell": best, "dsr": dsr, "per_block_sr": float(r.mean() / sd), "blocks_per_calendar": per_calendar, "trials": len(tests) + 1}


def markdown(summary: Dict[str, dict], tests: Sequence[str]) -> str:
    head = "| Cell | Calendars won of 8 | Mean paired Sharpe diff | at 5 bps | p | BH q | Mean Sharpe | Net / 21 sessions | Turnover / 21 sessions | Max DD | Longs / shorts | Candidate |"
    lines = [head, "|" + "---|" * 12]
    for n in ["S0", *tests]:
        s = summary[n]
        if not s["calendars"]:
            lines.append(f"| {n} | not run | | | | | | | | | | |")
            continue
        lines.append(
            f"| {n} | {s['wins'] if n != 'S0' else '-'} of {s['calendars']} | {s['mean_diff']:+.3f} | {s['mean_diff_5bps']:+.3f} | "
            f"{s['p']:.3f} | {s['q']:.3f} | {s['mean_sharpe']:.2f} | {s['mean_net_block'] * 100:+.2f}% | {s['turnover']:.2f}x | "
            f"{s['max_dd'] * 100:.1f}% | {s['longs']:.0f} / {s['shorts']:.0f} | {'YES' if s['candidate'] else 'no'} |"
            if n != "S0" else
            f"| S0 | - | - | - | - | - | {s['mean_sharpe']:.2f} | {s['mean_net_block'] * 100:+.2f}% | {s['turnover']:.2f}x | {s['max_dd'] * 100:.1f}% | "
            f"{s['longs']:.0f} / {s['shorts']:.0f} | control |")
    return "\n".join(lines)


def finish(raw: Dict[str, Dict[int, Optional[dict]]], cells: Sequence[bs.Strategy], tests: Sequence[str], offsets: Sequence[int], draws: int,
           base_bin: int) -> dict:
    summary = analyse(raw, cells, draws, base_bin)
    verdicts(summary, tests, len(offsets))
    dsr = deflated(raw, summary, tests)
    return {"summary": summary, "deflated": dsr, "table": markdown(summary, tests)}


# ---------------------------------------------------------------- synthetic selftest

def selftest() -> None:
    rng = np.random.default_rng(5)
    T, N = 900, 120
    spy = rng.normal(0.0003, 0.008, T)
    latent = rng.normal(0, 1, (T, N))
    ret = 0.6 * spy[:, None] + rng.normal(0, 0.012, (T, N))
    fwd = np.zeros((T, N))
    for t in range(T - 21):
        fwd[t] = ret[t + 1: t + 22].sum(axis=0)
    score = 0.35 * (fwd - fwd.mean(axis=1, keepdims=True)) / fwd.std(axis=1, keepdims=True) + latent    # a noisy look at the next month
    dv = np.full((T, N), 5e7)
    ind = rng.integers(10, 16, N)
    cache: Dict[int, bs.CrossSection] = {}

    def base(row: int) -> Optional[bs.CrossSection]:
        if row < 400 or row > T - 22:
            return None
        if row not in cache:
            cols = np.arange(N)
            vol, beta, adv = bs.row_stats(ret, spy, dv, row, cols)
            cache[row] = bs.CrossSection(cols, score[row], np.ones(N, dtype=bool), ind, np.full(N, 5e9), vol, np.full(N, np.nan), adv, beta)
        return cache[row]

    cells = [*bs.STRATEGIES, *bs.CAPACITY_CELLS]
    tests = [c.name for c in bs.STRATEGIES if c.name != "S0"]
    raw = run_cells(cells, {"base": base}, ret, spy, [0, 7], 400, T)
    out = finish(raw, cells, tests, [0, 7], 400, 400)
    s0 = out["summary"]["S0"]
    assert s0["calendars"] == 2 and s0["mean_sharpe"] > 0, s0           # the planted signal is found
    assert all(np.isfinite(out["summary"][n]["mean_diff"]) for n in tests if out["summary"][n]["calendars"]), "a cell did not run"
    assert out["summary"]["S6_wk"]["turnover"] > s0["turnover"] > 0
    json.dumps(out["summary"])
    print(out["table"])
    print("selftest ok")


# ---------------------------------------------------------------- real run

async def load_caps(tickers: Sequence[str]) -> Optional[np.ndarray]:
    from shared.db import get_redis
    from shared.utils import market_cap as mc

    try:
        redis = await get_redis()
        table = await getattr(redis, "raw", redis).hgetall(mc.CAPS_KEY) or {}
    except Exception as exc:
        swallowed("scripts.run_book_strategies.caps", exc, logger)
        return None
    if not table:
        dropped("scripts.run_book_strategies.no_caps", "market-cap table is empty: S2 not measurable", logger)
        return None
    caps = {(k.decode() if isinstance(k, bytes) else str(k)).upper(): v for k, v in table.items()}
    out = np.full(len(tickers), np.nan)
    for j, t in enumerate(tickers):
        try:
            v = caps.get(t)
            out[j] = float(v.decode() if isinstance(v, bytes) else v) if v is not None else np.nan
        except (TypeError, ValueError):
            continue
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="lgbm_rank")
    ap.add_argument("--blend-model", default="gbm")
    ap.add_argument("--offsets", default=",".join(str(o) for o in OFFSETS))
    ap.add_argument("--draws", type=int, default=10_000)
    ap.add_argument("--out", default="/tmp/book_strategies.json")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    offsets = [int(x) for x in args.offsets.split(",")]
    from services.agents.factor_engine import FactorEngine
    from shared.db import get_redis, get_timescale
    from shared.utils import factor_model as fm

    db, redis = await get_timescale(), await get_redis()
    panel = await FactorEngine(db, redis)._panel()
    if panel is None:
        print("no panel")
        return
    panel = fm.tradable_subset(panel)
    if panel.extra.get("borrowable") is None:
        print("no borrowable list: S0 cannot reproduce B1, run stopped")
        return
    feats = fm.features(panel)
    # Every session scored, with the monthly model B1 uses; the model on a date is the fold's, whatever the calendar.
    spec = fm.HorizonSpec("monthly_daily", 21, 1, 21, 21, 12)
    scores: Dict[str, Dict[int, tuple]] = {}
    for name in (args.model, args.blend_model):
        preds: list = []
        rep = await asyncio.to_thread(fm.walk_forward, panel, name, spec=spec, feats=feats, collect_preds=preds)
        scores[name] = {i: (cols, pred) for i, cols, pred, _ in preds}
        print(name, "scored rows", len(preds), "IC", rep.mean_ic, flush=True)
        del preds
    del feats
    T = len(panel.dates)
    first_pred = min(scores[args.model])
    with np.errstate(all="ignore"):
        ret = np.vstack([np.full((1, panel.close.shape[1]), np.nan), panel.close[1:] / panel.close[:-1] - 1.0]).astype(np.float64)
    spy = ret[:, panel.tickers.index("SPY")].copy()
    dv = (panel.close * panel.volume).astype(np.float64)
    industry = panel.extra.get("industry")
    industry = np.asarray(industry) if industry is not None else np.full(len(panel.tickers), -1)
    dtc = panel.extra.get("days_to_cover")
    borrowable = np.asarray(panel.extra["borrowable"], dtype=bool)
    caps = await load_caps(panel.tickers)
    if caps is None:
        caps = np.full(len(panel.tickers), np.nan)
    print("caps known", int(np.isfinite(caps).sum()), "of", len(caps), "| industry known", int((industry >= 0).sum()),
          "| dtc coverage", None if dtc is None else float(np.isfinite(dtc).mean()), flush=True)
    base_cache: Dict[int, Optional[bs.CrossSection]] = {}
    blend_cache: Dict[int, Optional[bs.CrossSection]] = {}

    def base(row: int) -> Optional[bs.CrossSection]:
        if row not in scores[args.model]:
            return None
        if row not in base_cache:
            cols, pred = scores[args.model][row]
            cols = np.asarray(cols)
            vol, beta, adv = bs.row_stats(ret, spy, dv, row, cols)
            base_cache[row] = bs.CrossSection(cols, np.asarray(pred, dtype=np.float64), borrowable[cols], industry[cols], caps[cols], vol,
                                              dtc[row, cols] if dtc is not None else np.full(len(cols), np.nan), adv, beta)
        return base_cache[row]

    def blend(row: int) -> Optional[bs.CrossSection]:
        if row in blend_cache:
            return blend_cache[row]
        cs = base(row)
        out = None
        if cs is not None and row in scores[args.blend_model]:
            cols_b, pred_b = scores[args.blend_model][row]
            common, ia, ib = np.intersect1d(cs.cols, np.asarray(cols_b), return_indices=True)
            if len(common) >= fm.MIN_NAMES:
                out = bs.CrossSection(common, bs.blend_scores(cs.score[ia], np.asarray(pred_b, dtype=np.float64)[ib]), cs.borrowable[ia],
                                      cs.industry[ia], cs.cap[ia], cs.vol[ia], cs.dtc[ia], cs.adv[ia], cs.beta[ia])
        blend_cache[row] = out
        return out

    cells = [*bs.STRATEGIES, *bs.CAPACITY_CELLS]
    tests = [c.name for c in bs.STRATEGIES if c.name != "S0"]
    raw = run_cells(cells, {"base": base, "S9": blend}, ret, spy, offsets, first_pred, T)
    out = finish(raw, cells, tests, offsets, args.draws, first_pred + bs.BLOCK + 1)
    out["meta"] = {"offsets": offsets, "rows": T, "first_scored_row": first_pred, "last_date": str(panel.dates[-1]),
                   "caps_known": int(np.isfinite(caps).sum()), "names": len(panel.tickers), "tests": tests}
    Path(args.out).write_text(json.dumps(out, indent=1, default=float))
    print(out["table"])
    print(json.dumps(out["deflated"], default=float))


if __name__ == "__main__":
    asyncio.run(main())
