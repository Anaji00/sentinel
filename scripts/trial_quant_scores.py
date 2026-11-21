"""Stage 1 of the registered trials of `reports/Quant methods and trading bot research - 18 Nov.md` section 4 (run 19 Nov 2026): builds the panel
once, saves what the simulations need (`inputs.npz`), and runs one purged walk-forward per cell, saving every session's scores (`scores_<cell>.pkl`)
and the walk-forward's own report (`report_<cell>.json`). Read-only; nothing in the live model, trader or services is touched. Run in its own capped
container, never inside a service container, and not between 22:00 and 06:00 UTC (the nightly model run):

    MSYS_NO_PATHCONV=1 docker run -d --name sentinel-quant-trials --memory 6g --cpus 8 --network sentinel-network --env-file .env \
        -v <repo>/shared:/app/shared:ro -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -v <out>:/out \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/trial_quant_scores.py --out /out [--cells base,gbm,...]

Cells (all `monthly_daily` spec: horizon 21, every session scored, embargo 21, fold 63 sessions, seed 7 unless stated):
  base    lgbm_rank, train_every 21, min_train_dates 12: B1's model, the control for T2, T3, T6 and the flat cell.
  gbm     HistGradientBoosting, as the 13 Nov trial's S9 blend partner (needed to re-score S9).
  t2_7    train_every 7, min_train_dates 36;  t2_3  train_every 3, min_train_dates 84;  t2_1  train_every 1, min_train_dates 252 (run only if t2_7 shows
          any sign: registered here as "mean paired Sharpe difference above 0 and IC not below base's").
          The first fold starts at the same session (row 575, the first fold with enough purged history) in every one of them.
  s5      five-seed rank average (trial_quant_harness).
  t6_mh   multi-horizon label, embargo 42, min_train_dates 12 (first scored row is also 575: 13 purged rows).
  t6_vs   volatility-scaled label, embargo 21.
  flat    lgbm_rank with the training panel and candidate set restricted: a name is not eligible on a session if its bar that session or either
          of the two before it is flat (open = high = low = close) or has zero volume. The exclusion is applied through the tradable mask, which
          the walk-forward reads for both the training cross-sections and the scored candidates. Features are built on the unmodified panel, as live.
`--selftest` is `trial_quant_harness.py`'s.
"""
import argparse
import asyncio
import dataclasses
import json
import logging
import pickle
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import trial_quant_harness as hx  # noqa: E402
from shared.utils import factor_model as fm  # noqa: E402

logger = logging.getLogger("scripts.trial_quant_scores")



def SPEC(train_every: int, min_train: int, embargo: int = 21) -> fm.HorizonSpec:
    return fm.HorizonSpec("monthly_daily", 21, 1, train_every, embargo, min_train)


CELLS = {
    "base": ("lgbm_rank", SPEC(21, 12), "excess"),
    "gbm": ("gbm", SPEC(21, 12), "excess"),
    "flat": ("lgbm_rank", SPEC(21, 12), "excess"),
    "t2_7": ("lgbm_rank", SPEC(7, 36), "excess"),
    "s5": (hx.SEED_MODEL, SPEC(21, 12), "excess"),
    "t6_mh": ("lgbm_rank", SPEC(21, 12, embargo=42), "multi_horizon"),
    "t6_vs": ("lgbm_rank", SPEC(21, 12), "vol_scaled"),
    "t2_3": ("lgbm_rank", SPEC(3, 84), "excess"),
    "t2_1": ("lgbm_rank", SPEC(1, 252), "excess"),
    # Noise floor (20 Nov): the base model refitted with other seeds, nothing else changed (seed 7 is "base").
    **{f"seed{k}": (f"{hx.SEED_PREFIX}{k}", SPEC(21, 12), "excess") for k in (8, 9, 10, 11)},
}
DEFAULT_CELLS = ["base", "gbm", "flat", "t2_7", "s5", "t6_mh", "t6_vs", "t2_3"]

_FLAG_SQL = """
    SELECT ticker, day FROM daily_bars
    WHERE day > $1::date AND close > 0 AND ticker = ANY($2::text[])
      AND ((open = close AND high = close AND low = close) OR coalesce(volume, 0) = 0)
"""
_FILINGS_SQL = """
    SELECT primary_entity_id AS ticker, occurred_at FROM signal_event_archive
    WHERE 'form:8-K' = ANY(tags) AND financial_data->'items' ?| ARRAY['3.01','1.03','4.02'] AND occurred_at IS NOT NULL
"""


async def flag_matrix(db, panel: fm.Panel) -> np.ndarray:
    from datetime import timedelta

    di = {d: i for i, d in enumerate(panel.dates)}
    ti = {t: j for j, t in enumerate(panel.tickers)}
    flag = np.zeros(panel.close.shape, dtype=bool)
    since = panel.dates[0] - timedelta(days=1)
    for k in range(0, len(panel.tickers), 400):
        for r in await db.query(_FLAG_SQL, since, panel.tickers[k:k + 400]) or []:
            i, j = di.get(r["day"]), ti.get(str(r["ticker"]).upper())
            if i is not None and j is not None:
                flag[i, j] = True
    return flag


def last3(flag: np.ndarray) -> np.ndarray:
    out = flag.copy()
    out[1:] |= flag[:-1]
    out[2:] |= flag[:-2]
    return out


async def save_inputs(out: Path, db, redis, panel: fm.Panel) -> None:
    from scripts.run_book_strategies import load_caps

    with np.errstate(all="ignore"):
        close = panel.close
        ret = np.vstack([np.full((1, close.shape[1]), np.nan), close[1:] / close[:-1] - 1.0]).astype(np.float32)
    spy = ret[:, panel.tickers.index("SPY")].copy()
    dv = (panel.close * panel.volume).astype(np.float32)
    industry = panel.extra.get("industry")
    industry = np.asarray(industry) if industry is not None else np.full(len(panel.tickers), -1)
    dtc = panel.extra.get("days_to_cover")
    caps = await load_caps(panel.tickers)
    caps = caps if caps is not None else np.full(len(panel.tickers), np.nan)
    flag = await flag_matrix(db, panel)
    filings = [(str(r["ticker"]).upper(), r["occurred_at"]) for r in await db.query(_FILINGS_SQL) or [] if r["ticker"]]
    with open(out / "filings_8k.pkl", "wb") as fh:
        pickle.dump(filings, fh)
    np.savez_compressed(out / "inputs.npz", dates=np.array([str(d) for d in panel.dates]), tickers=np.array(panel.tickers), close=close.astype(np.float32),
                        ret=ret, spy=spy, dv=dv, industry=industry, has_dtc=np.array(dtc is not None),
                        dtc=dtc.astype(np.float32) if dtc is not None else np.zeros(1), borrowable=np.asarray(panel.extra["borrowable"], dtype=bool),
                        caps=caps, flag=flag, tradable=fm.tradable(panel).copy())
    f3 = last3(flag)
    tr = fm.tradable(panel)
    print(f"inputs saved: {len(panel.dates)} sessions x {len(panel.tickers)} names; tradable name-days {int(tr.sum())}; flagged (own bar) "
          f"{int((flag & tr).sum())} = {(flag & tr).sum() / max(1, tr.sum()):.2%}; flagged in last 3 sessions {int((f3 & tr).sum())} = "
          f"{(f3 & tr).sum() / max(1, tr.sum()):.2%}; 8-K veto filings {len(filings)}", flush=True)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/out")
    ap.add_argument("--cells", default=",".join(DEFAULT_CELLS))
    ap.add_argument("--refresh-inputs", action="store_true")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    from services.agents.factor_engine import FactorEngine
    from shared.db import get_redis, get_timescale

    db, redis = await get_timescale(), await get_redis()
    panel = await FactorEngine(db, redis)._panel()
    if panel is None:
        print("no panel")
        return
    panel = fm.tradable_subset(panel)
    if panel.extra.get("borrowable") is None:
        print("no borrowable list: stopped")
        return
    print(f"panel {len(panel.dates)} x {len(panel.tickers)}, last date {panel.dates[-1]}", flush=True)
    if args.refresh_inputs or not (out / "inputs.npz").exists():
        await save_inputs(out, db, redis, panel)
    flag = np.load(out / "inputs.npz")["flag"]
    hx.install()
    feats = fm.features(panel)
    original_mask = fm.tradable(panel).copy()
    print("features built", flush=True)
    for name in [c for c in args.cells.split(",") if c]:
        if (out / f"scores_{name}.pkl").exists():
            print(f"{name}: already done", flush=True)
            continue
        model, spec, target_mode = CELLS[name]
        mask = fm.tradable(panel)
        mask[:] = original_mask & ~last3(flag) if name == "flat" else original_mask
        t0 = time.time()
        preds: List = []
        rep = await asyncio.to_thread(fm.walk_forward, panel, model, spec=spec, feats=feats, collect_preds=preds, target_mode=target_mode)
        scores: Dict[int, tuple] = {int(i): (np.asarray(cols), np.asarray(pred)) for i, cols, pred, _ in preds}
        with open(out / f"scores_{name}.pkl", "wb") as fh:
            pickle.dump(scores, fh)
        (out / f"report_{name}.json").write_text(json.dumps({**dataclasses.asdict(rep), "scored_rows": len(scores), "first_row": min(scores) if scores else None,
                                                             "seconds": round(time.time() - t0)}, default=str, indent=1))
        print(f"{name}: scored rows {len(scores)} IC {rep.mean_ic} t {rep.ic_t} first row {min(scores) if scores else None} in {time.time() - t0:.0f}s", flush=True)
        del preds, scores
    mask = fm.tradable(panel)
    mask[:] = original_mask


if __name__ == "__main__":
    asyncio.run(main())
