"""Registered feature trials C3 (ablate llm_news_30 / ev_comment_letter) and B1 (rule-scored analyst price-target headlines), 13 Nov 2026
("Upgrade pre-registrations - 12 Nov", areas C and B). Read-only; nothing live changes. Run in its own capped container, one run at a time,
never inside a service container, and not between 22:00 and 06:00 UTC (the nightly model run):

    MSYS_NO_PATHCONV=1 docker run --rm --name sentinel-feature-trials --memory 8g --network sentinel-network --env-file .env \
        -v <repo>/shared:/app/shared:ro -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app \
        sentinel-agents-heavy python scripts/run_feature_trials.py --which c3|b1|both [--offsets 0,5,10,15,2,7,12,17] [--out /tmp/x.json]

`--selftest` needs no database: it checks the helpers and the paired walk-forward bookkeeping on a synthetic market with a planted signal.

What is measured, and what cannot be. The walk-forward ablation of a feature needs the feature to be a column of the model. The live monthly
model (`factor_model.FEATURES`) has neither `llm_news_30` nor `ev_comment_letter`: they are written to stock_features_daily for the weekly
researcher screen and for pre-registered trials, and no code reads them into the model. The script checks this against the code it runs and,
when the feature is not a model input, reports the ablation as a no-op instead of running a walk-forward that would return an identical
model. If a feature were a model input the paired walk-forward below runs (same purged walk-forward, S0 = the B1 book rule and net-of-cost
simulation as scripts/run_book_strategies.py).
"""
import argparse
import asyncio
import json
import logging
import math
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils import book_strategies as bs  # noqa: E402
from shared.utils import feature_ablation as fa  # noqa: E402
from shared.utils import llm_news as ln  # noqa: E402
from shared.utils.quiet_failures import dropped, swallowed  # noqa: E402

logger = logging.getLogger("scripts.run_feature_trials")
OFFSETS = [0, 5, 10, 15, 2, 7, 12, 17]
FEATURES_UNDER_TEST = ("llm_news_30", "ev_comment_letter")
NEWS_MODEL = "groq/openai/gpt-oss-120b"
TICKER_RE = re.compile(r"^[A-Z]{1,5}$")
MIN_HISTORY_SESSIONS = 260        # factor_model.MIN_HISTORY: the walk-forward's first row; a feature needs at least this much stored history


def rank_corr(a: Sequence[float], b: Sequence[float]) -> float:
    """Spearman correlation with tied ranks averaged; NaN when either side is constant."""
    def ranks(x: np.ndarray) -> np.ndarray:
        _, inv, cnt = np.unique(x, return_inverse=True, return_counts=True)
        csum = np.cumsum(cnt)
        return (csum - (cnt - 1) / 2.0)[inv]
    x, y = ranks(np.asarray(a, float)), ranks(np.asarray(b, float))
    if x.std() == 0 or y.std() == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


# ---------------------------------------------------------------- the paired walk-forward (used only for a feature that is a model input)

def paired_calendars(full: Dict[int, tuple], variant: Dict[int, tuple], first_row: int, borrowable: np.ndarray, ret: np.ndarray, spy: np.ndarray,
                     offsets: Sequence[int], T: int, draws: int) -> dict:
    """Net Sharpe of S0 per calendar for two score sets {row: (cols, pred)}, the paired differences (variant - full) and the date-bootstrap p
    that the full model beats the variant. A failing calendar is counted and left out, never aborting the run."""
    def section(table: Dict[int, tuple]):
        cache: Dict[int, Optional[bs.CrossSection]] = {}

        def get(row: int) -> Optional[bs.CrossSection]:
            if row not in table:
                return None
            if row not in cache:
                cols, pred = table[row]
                cols = np.asarray(cols)
                n = len(cols)
                vol, beta, adv = bs.row_stats(ret, spy, np.full(ret.shape, 5e7), row, cols)
                cache[row] = bs.CrossSection(cols, np.asarray(pred, dtype=np.float64), borrowable[cols], np.full(n, -1), np.full(n, np.nan), vol,
                                             np.full(n, np.nan), adv, beta)
            return cache[row]
        return get

    out: Dict[str, Any] = {"per_calendar": {}}
    rows_f, rows_v, bins = [], [], []
    for off in offsets:
        base = 260 + off
        start = base + max(0, math.ceil((first_row - base) / bs.BLOCK)) * bs.BLOCK
        n_blocks = (T - 1 - (start + bs.BLOCK)) // bs.BLOCK
        if n_blocks < 6:
            dropped("scripts.run_feature_trials.short_calendar", f"offset {off}: {n_blocks} blocks", logger)
            continue
        try:
            rf = bs.simulate(bs.S0, section(full), ret, spy, start, n_blocks)
            rv = bs.simulate(bs.S0, section(variant), ret, spy, start, n_blocks)
        except Exception as exc:
            swallowed("scripts.run_feature_trials.calendar", exc, logger, detail=f"offset {off}")
            continue
        out["per_calendar"][off] = {"full": bs.sharpe(rf.block_returns), "variant": bs.sharpe(rv.block_returns), "start": start, "n_blocks": n_blocks}
        rows_f.append(rf.block_returns)
        rows_v.append(rv.block_returns)
        bins.append((start + bs.BLOCK + 1 + bs.BLOCK * np.arange(n_blocks) - first_row) // bs.BLOCK)
        print(f"calendar {off}: full {out['per_calendar'][off]['full']:.2f} variant {out['per_calendar'][off]['variant']:.2f}", flush=True)
    out["diffs"] = fa.paired_diffs({o: v["variant"] for o, v in out["per_calendar"].items()}, {o: v["full"] for o, v in out["per_calendar"].items()})
    if rows_f:
        out["bootstrap_full_beats_variant"] = bs.paired_bootstrap(rows_f, rows_v, bins, draws=draws)
    return out


# ---------------------------------------------------------------- selftest

def selftest() -> None:
    assert fa.rule_score("Citi Raises Price Target on X") == 1 and fa.rule_score("Citi Lowers Price Target on X") == -1
    assert math.isclose(rank_corr([1, 2, 3, 4], [10, 20, 30, 40]), 1.0) and math.isclose(rank_corr([1, 2, 3], [3, 2, 1]), -1.0)
    rng = np.random.default_rng(7)
    T, N = 900, 120
    spy = rng.normal(0.0003, 0.008, T)
    ret = 0.6 * spy[:, None] + rng.normal(0, 0.012, (T, N))
    fwd = np.zeros((T, N))
    for t in range(T - 21):
        fwd[t] = ret[t + 1: t + 22].sum(axis=0)
    z = (fwd - fwd.mean(axis=1, keepdims=True)) / fwd.std(axis=1, keepdims=True)
    full = {r: (np.arange(N), 0.5 * z[r] + rng.normal(0, 1, N)) for r in range(400, T - 22)}
    junk = {r: (np.arange(N), rng.normal(0, 1, N)) for r in range(400, T - 22)}
    res = paired_calendars(full, junk, 400, np.ones(N, dtype=bool), ret, spy, [0, 7], T, 300)
    assert len(res["diffs"]) == 2 and all(d < 0 for d in res["diffs"].values()), res["diffs"]      # removing a real signal costs Sharpe
    same = paired_calendars(full, full, 400, np.ones(N, dtype=bool), ret, spy, [0, 7], T, 300)
    assert all(d == 0.0 for d in same["diffs"].values())
    assert fa.c3_verdict(same["diffs"], 2)["keep_calls"] is False
    json.dumps(res, default=float)
    print("selftest ok")


# ---------------------------------------------------------------- C3

async def run_c3(db: Any, offsets: Sequence[int], draws: int) -> dict:
    from shared.utils import factor_model as fm

    out: Dict[str, Any] = {"feature_list_size": len(fm.FEATURES), "features": {}}
    for name in FEATURES_UNDER_TEST:
        entry: Dict[str, Any] = {"model_input": fa.is_model_input(name, fm.FEATURES)}
        try:
            rows = await db.query("SELECT count(*) AS n, count(DISTINCT ticker) AS t, min(day) AS lo, max(day) AS hi, count(DISTINCT day) AS days "
                                  "FROM stock_features_daily WHERE feature = $1", name) or []
            if rows:
                r = rows[0]
                entry["stored"] = {"rows": int(r["n"]), "tickers": int(r["t"]), "first_day": str(r["lo"]), "last_day": str(r["hi"]), "days": int(r["days"])}
        except Exception as exc:
            swallowed("scripts.run_feature_trials.coverage", exc, logger, detail=name)
        if not entry["model_input"]:
            entry["result"] = "no-op: not a column of the monthly model, so the ablated model is the full model (difference exactly 0 on every calendar)"
            entry["diffs"] = {str(o): 0.0 for o in offsets}
            entry["verdict"] = fa.c3_verdict({o: 0.0 for o in offsets}, len(offsets))
        else:
            entry["result"] = await walk_forward_ablation(db, name, offsets, draws)
        out["features"][name] = entry
        print(name, json.dumps({k: v for k, v in entry.items() if k != "result"}, default=str), flush=True)
    return out


async def walk_forward_ablation(db: Any, name: str, offsets: Sequence[int], draws: int) -> dict:
    """Full model against the model without `name`: the same purged walk-forward, scored on every session, S0 net of cost per calendar."""
    from services.agents.factor_engine import FactorEngine
    from shared.db import get_redis
    from shared.utils import factor_model as fm

    panel = await FactorEngine(db, await get_redis())._panel()
    if panel is None:
        return {"error": "no panel"}
    panel = fm.tradable_subset(panel)
    borrowable = panel.extra.get("borrowable")
    if borrowable is None:
        return {"error": "no borrowable list"}
    feats = fm.features(panel)
    spec = fm.HorizonSpec("monthly_daily", 21, 1, 21, 21, 12)
    saved = list(fm.FEATURES)
    tables: Dict[str, Dict[int, tuple]] = {}
    ics: Dict[str, Optional[float]] = {}
    try:
        for label, cols in (("full", saved), ("ablated", fa.ablate(saved, name))):
            fm.FEATURES[:] = cols
            preds: list = []
            rep = await asyncio.to_thread(fm.walk_forward, panel, "lgbm_rank", spec=spec, feats=feats, collect_preds=preds)
            tables[label] = {i: (c, p) for i, c, p, _ in preds}
            ics[label] = rep.mean_ic
            print(label, "scored rows", len(preds), "mean IC", rep.mean_ic, flush=True)
    finally:
        fm.FEATURES[:] = saved
    T = len(panel.dates)
    with np.errstate(all="ignore"):
        ret = np.vstack([np.full((1, panel.close.shape[1]), np.nan), panel.close[1:] / panel.close[:-1] - 1.0]).astype(np.float64)
    spy = ret[:, panel.tickers.index("SPY")].copy()
    res = paired_calendars(tables["full"], tables["ablated"], min(tables["full"]), np.asarray(borrowable, dtype=bool), ret, spy, offsets, T, draws)
    res["mean_ic"] = ics
    res["verdict"] = fa.c3_verdict(res["diffs"], len(offsets))
    return res


# ---------------------------------------------------------------- B1

def _ts(v: Any) -> datetime:
    return v if isinstance(v, datetime) else datetime.fromisoformat(str(v).replace("Z", "+00:00"))


async def run_b1(db: Any, redis: Any, offsets: Sequence[int]) -> dict:
    from shared.utils import factor_model as fm
    from shared.utils import market_cap as mc

    out: Dict[str, Any] = {"regexes": {"raise": fa.RAISE_RE.pattern, "lower": fa.LOWER_RE.pattern, "upgrade": fa.UPGRADE_RE.pattern,
                                       "downgrade": fa.DOWNGRADE_RE.pattern, "flags": "IGNORECASE"}}
    scored = await db.query("SELECT s.news_id, s.ticker, s.created_at, s.score, n.headline FROM stock_news_llm s JOIN stock_news n ON n.id = s.news_id "
                            "WHERE s.model = $1", NEWS_MODEL) or []
    rows = [(str(r["ticker"]).upper(), _ts(r["created_at"]), str(r["headline"]), int(r["score"])) for r in scored]
    stale_n = int((await db.query("SELECT count(*) AS n FROM stock_news_llm WHERE model = $1", NEWS_MODEL + ln.STALE_SUFFIX))[0]["n"])
    first = min(r[1] for r in rows)
    last = max(r[1] for r in rows)
    out["stored"] = {"scored": len(rows), "stale_recorded": stale_n, "first": str(first), "last": str(last)}
    print("scored headlines", len(rows), "stale recorded", stale_n, flush=True)

    # (a) non-matching headlines are identical by construction: checked, not assumed.
    v2 = fa.rescored(rows)
    untouched = [(a[3], b[2]) for a, b in zip(rows, v2) if fa.rule_score(a[2]) is None]
    out["a_non_matching"] = {"headlines": len(untouched), "identical": all(o == n for o, n in untouched)}
    out["agreement"] = fa.agreement(rows)
    matched = [r for r in rows if fa.rule_score(r[2]) is not None]
    out["matched_among_scored"] = {"matched": len(matched), "scored": len(rows), "share": len(matched) / len(rows),
                                   "old_score_zero": sum(1 for r in matched if r[3] == 0) / max(1, len(matched))}
    print("agreement", json.dumps(out["agreement"]), flush=True)

    # The feature in both versions, daily over the stored window.
    v1s = [(t, a, s) for t, a, _h, s in rows]
    days = [first.date() + timedelta(days=k) for k in range((last.date() - first.date()).days + 1)]
    per_day: List[dict] = []
    for d in days:
        a, b = fa.rebuild_feature(v1s, d), fa.rebuild_feature(v2, d)
        if len(a) < 5:
            continue
        ks = sorted(a)
        changed = sum(1 for k in ks if abs(a[k] - b[k]) > 1e-9)
        per_day.append({"day": str(d), "names": len(ks), "changed": changed,
                        "rank_corr": rank_corr([a[k] for k in ks], [b[k] for k in ks])})
    out["feature_versions"] = {"days": len(per_day), "stock_days": sum(p["names"] for p in per_day), "changed": sum(p["changed"] for p in per_day),
                               "mean_rank_corr": float(np.nanmean([p["rank_corr"] for p in per_day])) if per_day else float("nan")}
    stored = await db.query("SELECT ticker, day, value FROM stock_features_daily WHERE feature = 'llm_news_30' AND day = "
                            "(SELECT max(day) FROM stock_features_daily WHERE feature = 'llm_news_30')") or []
    if stored:
        d = stored[0]["day"] if isinstance(stored[0]["day"], date) else date.fromisoformat(str(stored[0]["day"]))
        mine = fa.rebuild_feature(v1s, d)
        same = sum(1 for r in stored if str(r["ticker"]) in mine and abs(mine[str(r["ticker"])] - float(r["value"])) < 1e-3)
        out["rebuild_vs_stored_last_day"] = {"day": str(d), "stored_names": len(stored), "rebuilt_names": len(mine), "equal": same}

    # The calls saved: headlines the job would score now (its admission rule), less the stale ones it records and skips.
    large = await mc.large_caps(redis, mc.MODEL_MIN_MARKET_CAP_USD)
    if not large:
        dropped("scripts.run_feature_trials.no_caps", "no market-cap table: the admission share is not measurable", logger)
        out["admission"] = None
    else:
        since = first - timedelta(hours=1)
        pool = await db.query("SELECT n.id, n.symbols[1] AS ticker, n.created_at, n.headline FROM stock_news n WHERE n.created_at > $1 "
                              "AND cardinality(n.symbols) = 1 AND n.symbols[1] ~ '^[A-Z]{1,5}$' AND n.symbols[1] = ANY($2::text[])",
                              since, sorted(large)) or []
        history = await db.query("SELECT symbols[1] AS ticker, created_at, headline FROM stock_news WHERE cardinality(symbols) = 1 "
                                 "AND symbols[1] = ANY($1::text[]) AND created_at > $2", sorted({str(r["ticker"]).upper() for r in pool}),
                                 since - timedelta(days=3)) or []
        stale = ln.stale_ids(pool, history)
        fresh = [r for r in pool if r["id"] not in stale]
        week_ago = last - timedelta(days=7)
        adm = {"large_caps": len(large), "single_stock_headlines": len(pool), "stale_skipped": len(stale), "would_score": len(fresh)}
        for label, subset in (("window", fresh), ("last_7_days", [r for r in fresh if _ts(r["created_at"]) > week_ago])):
            m, n, share = fa.matched_share([str(r["headline"]) for r in subset])
            adm[label] = {"matched": m, "would_score": n, "share": share}
        out["admission"] = adm
        print("admission", json.dumps(adm, default=float), flush=True)

    # (b) the walk-forward with each feature version. llm_news_30 is not a model column and has days of history, not years.
    cov = out.get("feature_versions", {}).get("days", 0)
    out["walk_forward"] = {"feature_is_model_input": fa.is_model_input(fa.ln.FEATURE, fm.FEATURES), "feature_days_of_history": cov,
                           "needed_sessions": MIN_HISTORY_SESSIONS,
                           "measurable": bool(fa.is_model_input(fa.ln.FEATURE, fm.FEATURES) and cov >= MIN_HISTORY_SESSIONS),
                           "diffs": {str(o): 0.0 for o in offsets}}
    cut = (out["admission"] or {}).get("window", {}).get("share", float("nan")) if out.get("admission") else float("nan")
    out["verdict"] = fa.b1_verdict({o: 0.0 for o in offsets}, cut, len(offsets))
    out["verdict"]["sharpe_condition_is_vacuous"] = not out["walk_forward"]["measurable"]
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--which", choices=("c3", "b1", "both"), default="both")
    ap.add_argument("--offsets", default=",".join(str(o) for o in OFFSETS))
    ap.add_argument("--draws", type=int, default=10_000)
    ap.add_argument("--out", default="/tmp/feature_trials.json")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    offsets = [int(x) for x in args.offsets.split(",")]
    from shared.db import get_redis, get_timescale

    db, redis = await get_timescale(), await get_redis()
    result: Dict[str, Any] = {"offsets": offsets}
    for which, fn in (("c3", lambda: run_c3(db, offsets, args.draws)), ("b1", lambda: run_b1(db, redis, offsets))):
        if args.which in (which, "both"):
            try:
                result[which] = await fn()
            except Exception as exc:
                swallowed("scripts.run_feature_trials.test", exc, logger, detail=which)
                result[which] = {"error": repr(exc)}
    Path(args.out).write_text(json.dumps(result, indent=1, default=float))
    print(json.dumps(result, indent=1, default=float))


if __name__ == "__main__":
    asyncio.run(main())
