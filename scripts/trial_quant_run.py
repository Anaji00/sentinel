"""Stage 2 of the registered trials of `reports/Quant methods and trading bot research - 18 Nov.md` section 4 (run 19 Nov 2026): every simulation, from the
files `trial_quant_scores.py` saved. Offline (no database, no inference), simulation only; nothing live is touched.

    MSYS_NO_PATHCONV=1 docker run --rm --memory 6g --cpus 4 -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro -v <out>:/out \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/trial_quant_run.py --data /out [--tests t1,t2,t3,t5,t6,t7,t8,flat,t10]

Order of work: T1 (the 17 stored cells and S0 re-scored at three flat short fees, which also regenerates the 13 Nov per-calendar series), T10 on those
18 cells (the registered "so far" set), then every new cell, then T10 again on all cells, the deflated Sharpe with the running trial count, and BH q over the
17 stored cells plus every new cell that has a p. Bars are the report's; the fixed details are in `trial_quant_common.py`. Both documents were written before
any trial ran. Result: `<data>/results.json` and `<data>/tables.md`.

Readings, as registered (report section 4) plus the details fixed before the run:
  T1   B1 passes if S0's mean net Sharpe at 2.0% a year is at least 0.30 and positive on all eight calendars. Descriptive otherwise.
  T10  PBO above 0.5: the in-sample best of the set is no better than chance, and the S5 shadow bar must not be relaxed.
  T2   t2_7, t2_3 (t2_1 only if t2_7 shows any sign: mean paired difference above 0 and IC not below base's): mean net Sharpe at least +0.10 over base,
       more calendars won than lost by at least six of eight, IC t not more than 0.3 below base's, BH q below 0.10.
  T3   cells s5 and avg5 (percentile averaged over the rebuild row and the four sessions before it): +0.05, six of eight, BH q below 0.10.
  T6   t6_mh, t6_vs: as T2.
  T7   S0 with the drawdown rule: mean worst block and mean maximum drawdown each at least 20% smaller in size than S0's, and mean Sharpe not below S0's by
       more than 0.05.
  T5   S0 + divergence veto and S0 + 8-K veto on longs only: kept if it beats S0 by at least 0.10 on six or more calendars; the veto is removed if S0 beats
       it by at least 0.10 on six or more. Either direction is reported.
  T8   Variance across the sixteen start offsets (the eight registered and eight more, EXTRA_OFFSETS) of block Sharpe: S7 at least 30% below S0's, with S7's
       mean Sharpe not lower by more than 0.05. The eight-offset figure is reported beside it.
  flat S0 on scores trained and ranked with flat or zero-volume bars (that session and the two before) excluded: +0.05, six of eight, BH q below 0.10.
"""
import argparse
import json
import pickle
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import trial_quant_common as qc  # noqa: E402
from shared.utils import book_strategies as bs  # noqa: E402

NEW_CELL_TRIALS_REGISTERED = 10        # the report's count (T2 2, T3 2, T5 2, T6 2, T7 1, T8 1); the flat cell is added when it runs


def load_scores(data: Path, name: str) -> Optional[dict]:
    p = data / f"scores_{name}.pkl"
    if not p.exists():
        return None
    with open(p, "rb") as fh:
        return pickle.load(fh)


def load_report(data: Path, name: str) -> dict:
    p = data / f"report_{name}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def sharpe_list(cell: qc.CellRun) -> Dict[str, float]:
    return {str(o): bs.sharpe(cell.results[o].block_returns) for o in cell.offsets}


def selftest() -> None:
    rng = np.random.default_rng(11)
    T, N = 900, 120
    spy = rng.normal(0.0003, 0.008, T)
    latent = rng.normal(0, 1, (T, N))
    ret = 0.6 * spy[:, None] + rng.normal(0, 0.012, (T, N))
    fwd = np.zeros((T, N))
    for t in range(T - 21):
        fwd[t] = ret[t + 1: t + 22].sum(axis=0)
    score = 0.35 * (fwd - fwd.mean(axis=1, keepdims=True)) / fwd.std(axis=1, keepdims=True) + latent
    close = (50 * np.cumprod(1 + ret, axis=0)).astype(np.float32)
    inp = qc.Inputs([str(np.datetime64("2018-01-01") + i) for i in range(T)], [f"T{i}" for i in range(N)], close, ret, spy, np.full((T, N), 5e7),
                    rng.integers(10, 16, N), None, np.ones(N, dtype=bool), np.full(N, 5e9), np.zeros((T, N), dtype=bool), np.ones((T, N), dtype=bool))
    scores = {r: (np.arange(N), score[r].astype(np.float32)) for r in range(400, T - 22)}
    mk = qc.Market(inp)
    base = mk.provider(scores)
    first = 400
    s0 = qc.run_cell("S0", bs.S0, base, inp, first, offsets=[0, 7])
    for off in s0.offsets:                      # the extended simulator without hooks equals the registered one
        ref = bs.simulate(bs.S0, base, inp.ret, inp.spy, s0.starts[off], s0.n_blocks[off])
        assert np.allclose(ref.block_returns, s0.results[off].block_returns) and np.isclose(ref.max_drawdown, s0.results[off].max_drawdown)
    cheap = qc.run_cell("S0", bs.S0, base, inp, first, offsets=[0, 7], fee_bps=qc.FEE_BPS["0.5%"])
    assert np.mean(cheap.results[0].block_returns) > np.mean(s0.results[0].block_returns)
    vl, vs = qc.divergence_events(inp)
    assert vl.shape == vs.shape == close.shape and vl.any() and vs.any() and not vl[0].any()
    veto = qc.run_cell("V", bs.S0, base, inp, first, offsets=[0, 7], veto_long=vl, veto_short=vs)
    assert not np.allclose(veto.results[0].block_returns, s0.results[0].block_returns)
    dd = qc.run_cell("DD", bs.S0, base, inp, first, offsets=[0, 7], dd_policy=True)
    assert dd.results[0].block_returns.shape == s0.results[0].block_returns.shape
    avg = qc.run_cell("A", bs.S0, mk.provider(scores, 5), inp, first, offsets=[0, 7])
    assert avg.results[0].block_returns.shape == s0.results[0].block_returns.shape
    pr = qc.paired(veto, s0, first + qc.BLOCK + 1, draws=300)
    assert 0 <= pr["p"] <= 1 and pr["calendars"] == 2
    noise = rng.normal(0, 0.03, (160, 12))
    res = qc.pbo_cscv(noise)
    assert 0.2 < res["pbo"] < 0.8, res                       # no skill anywhere: PBO near one half
    skill = noise.copy()
    skill[:, 3] += 0.03
    assert qc.pbo_cscv(skill)["pbo"] < 0.1                    # one cell that is better throughout is not overfit
    mt = qc.min_track_record(rng.normal(0.01, 0.05, 120))
    assert mt["min_blocks"] and mt["min_years"] > 0
    ev = qc.eight_k_events(inp, [])
    assert ev.shape == close.shape and not ev.any()
    print("selftest ok")


def bar_model(cell: dict, base_ic_t: Optional[float], ic_t: Optional[float], gain: float) -> dict:
    ic_ok = base_ic_t is not None and ic_t is not None and ic_t >= base_ic_t - 0.3
    return {"gain_ok": cell["mean_diff"] >= gain, "wins_ok": cell["wins"] >= 6, "ic_ok": ic_ok if base_ic_t is not None else None}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="/out")
    ap.add_argument("--draws", type=int, default=qc.BOOTSTRAP_DRAWS)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    data = Path(args.data)
    inp = qc.Inputs.load(data / "inputs.npz")
    mk = qc.Market(inp)
    T = len(inp.dates)
    base_scores = load_scores(data, "base")
    gbm_scores = load_scores(data, "gbm")
    assert base_scores, "scores_base.pkl missing: run trial_quant_scores.py first"
    first_base = min(base_scores)
    base_bin = first_base + qc.BLOCK + 1
    prov_base = mk.provider(base_scores)
    prov_s9 = mk.blend(prov_base, gbm_scores) if gbm_scores else None
    out: dict = {"meta": {"sessions": T, "names": len(inp.tickers), "last_date": inp.dates[-1], "first_scored_row": first_base,
                          "scored_rows": len(base_scores)}}
    md: List[str] = []

    # ------------------------------------------------------------ T1: the 17 stored cells and S0 at three flat fees
    cells = [*bs.STRATEGIES, *bs.CAPACITY_CELLS]
    fees = qc.FEE_BPS
    runs: Dict[str, Dict[str, qc.CellRun]] = {}
    for st in cells:
        prov = prov_s9 if st.name == "S9" else prov_base
        if prov is None:
            continue
        runs[st.name] = {fname: qc.run_cell(st.name, st, prov, inp, first_base, fee_bps=fbps) for fname, fbps in fees.items()}
        print("T1", st.name, {f: round(float(np.mean(list(sharpe_list(r).values()))), 2) for f, r in runs[st.name].items()}, flush=True)
    t1 = {}
    for name, per in runs.items():
        row = {}
        for fname, r in per.items():
            sh = sharpe_list(r)
            s0sh = sharpe_list(runs["S0"][fname])
            diffs = [sh[o] - s0sh[o] for o in sh if o in s0sh]
            row[fname] = {"mean_sharpe": float(np.mean(list(sh.values()))), "per_calendar": sh, "min_calendar": float(min(sh.values())),
                          "positive_calendars": int(sum(v > 0 for v in sh.values())), "mean_diff_vs_s0": float(np.mean(diffs)) if name != "S0" else None,
                          "mean_net_block": float(np.mean([np.mean(r.results[o].block_returns) for o in r.offsets])),
                          "max_dd": float(np.mean([r.results[o].max_drawdown for o in r.offsets]))}
        t1[name] = row
    out["T1"] = t1
    s0_2 = t1["S0"]["2.0%"]
    out["T1_verdict"] = {"mean_sharpe_at_2pct": s0_2["mean_sharpe"], "all_calendars_positive_at_2pct": s0_2["min_calendar"] > 0,
                         "passes": bool(s0_2["mean_sharpe"] >= 0.30 and s0_2["min_calendar"] > 0)}
    md.append("## T1: S0 and the stored cells at flat short fees\n")
    md.append("| Cell | 0.5% a year | 2.0% | 5.3% (the trials' 44 bps a month) | Diff vs S0 at 0.5% / 2.0% / 5.3% | Calendars positive at 2.0% | Min calendar at 2.0% |\n|---|---|---|---|---|---|---|")
    for name, row in t1.items():
        d = " / ".join(f"{row[f]['mean_diff_vs_s0']:+.3f}" if row[f]["mean_diff_vs_s0"] is not None else "-" for f in fees)
        md.append(f"| {name} | " + " | ".join(f"{row[f]['mean_sharpe']:.3f}" for f in fees) + f" | {d} | {row['2.0%']['positive_calendars']} of 8 | {row['2.0%']['min_calendar']:+.2f} |")
    md.append("\nS0 per calendar (offsets " + ", ".join(str(o) for o in qc.OFFSETS) + "):\n")
    for f in fees:
        md.append(f"- {f}: " + ", ".join(f"{t1['S0'][f]['per_calendar'].get(str(o), float('nan')):+.2f}" for o in qc.OFFSETS))

    # ------------------------------------------------------------ the 17 cells' paired statistics at 5.3% (the 13 Nov accounting), for BH
    standard = {n: per["5.3%"] for n, per in runs.items()}
    s0_std = standard["S0"]
    tests17 = [c.name for c in bs.STRATEGIES if c.name != "S0"]
    pair17 = {n: qc.paired(standard[n], s0_std, base_bin, args.draws) for n in tests17 if n in standard}
    out["stored_cells"] = {n: {k: v for k, v in p.items() if k != "per_calendar"} for n, p in pair17.items()}
    md.append("\n## The 17 stored cells re-run at 5.3% (regenerated 13 Nov series)\n")
    md.append("| Cell | Wins of 8 | Mean paired diff | p | Mean Sharpe |\n|---|---|---|---|---|")
    for n, p in pair17.items():
        md.append(f"| {n} | {p['wins']} | {p['mean_diff']:+.3f} | {p['p']:.3f} | {p['mean_sharpe']:.3f} |")
    md.append(f"| S0 | - | - | - | {np.mean(list(sharpe_list(s0_std).values())):.3f} |")

    # ------------------------------------------------------------ T10 on the 18 cells so far
    def matrix(names: List[str], store: Dict[str, qc.CellRun], offset: int) -> np.ndarray:
        series = {n: dict(zip(*qc.block_series(store[n], offset))) for n in names if offset in store[n].results}
        rows = sorted(set.intersection(*[set(s) for s in series.values()]))
        return np.column_stack([[series[n][r] for r in rows] for n in names])

    names18 = ["S0", *tests17]
    t10_a = {}
    for o in qc.OFFSETS:
        t10_a[str(o)] = qc.pbo_cscv(matrix(names18, standard, o))
    out["T10_18_cells"] = t10_a
    pb = [v["pbo"] for v in t10_a.values() if "pbo" in v]
    out["T10_18_summary"] = {"pbo_calendar_0": t10_a["0"].get("pbo"), "pbo_mean": float(np.mean(pb)), "pbo_min": float(min(pb)), "pbo_max": float(max(pb))}
    mt = {}
    for label, r in (("5.3%", runs["S0"]["5.3%"]), ("2.0%", runs["S0"]["2.0%"]), ("0.5%", runs["S0"]["0.5%"])):
        mt[label] = {str(o): qc.min_track_record(qc.block_series(r, o)[1]) for o in r.offsets}
        pooled = np.concatenate([qc.block_series(r, o)[1] for o in r.offsets])
        mt[label]["pooled"] = qc.min_track_record(pooled)
    out["T10_mtrl"] = mt
    md.append("\n## T10: PBO (CSCV, 16 slices) on the 18 cells so far\n")
    md.append("| Calendar | PBO | Logit median | Slope OOS on IS | Share of splits with a loss OOS | Blocks used |\n|---|---|---|---|---|---|")
    for o, v in t10_a.items():
        if "pbo" in v:
            md.append(f"| {o} | {v['pbo']:.3f} | {v['logit_median']:+.2f} | {v['slope_oos_on_is']:+.2f} | {v['prob_loss_oos']:.2f} | {v['blocks_used']} |")

    # ------------------------------------------------------------ new cells
    new: Dict[str, dict] = {}
    runs_new: Dict[str, qc.CellRun] = {}
    base_rep = load_report(data, "base")
    s0_cache: Dict[int, qc.CellRun] = {first_base: s0_std}

    def s0_for(first: int) -> qc.CellRun:
        if first not in s0_cache:
            s0_cache[first] = qc.run_cell("S0", bs.S0, prov_base, inp, first)
        return s0_cache[first]

    def add(name: str, label: str, provider, scores_min: int, **hooks) -> None:
        first = max(first_base, scores_min)
        cell = qc.run_cell(name, bs.S0, provider, inp, first, **hooks)
        p = qc.paired(cell, s0_for(first), base_bin, args.draws)
        runs_new[name] = cell
        new[name] = {"label": label, "first_row": first, **{k: v for k, v in p.items()}}
        print("NEW", name, round(p["mean_diff"], 3), p["wins"], round(p["p"], 3), flush=True)

    for name, label in (("t2_7", "train_every 7"), ("t2_3", "train_every 3"), ("t2_1", "train_every 1")):
        sc = load_scores(data, name)
        if sc:
            add(name, label, mk.provider(sc), min(sc))
            new[name]["ic"], new[name]["ic_t"] = load_report(data, name).get("mean_ic"), load_report(data, name).get("ic_t")
    sc = load_scores(data, "s5")
    if sc:
        add("s5", "five seeds, rank average", mk.provider(sc), min(sc))
        new["s5"]["ic"], new["s5"]["ic_t"] = load_report(data, "s5").get("mean_ic"), load_report(data, "s5").get("ic_t")
    add("avg5", "percentile averaged over the rebuild row and 4 earlier sessions", mk.provider(base_scores, 5), first_base)
    for name, label in (("t6_mh", "multi-horizon label (10, 21, 42)"), ("t6_vs", "volatility-scaled label")):
        sc = load_scores(data, name)
        if sc:
            add(name, label, mk.provider(sc), min(sc))
            new[name]["ic"], new[name]["ic_t"] = load_report(data, name).get("mean_ic"), load_report(data, name).get("ic_t")
    sc = load_scores(data, "flat")
    if sc:
        add("flat", "flat and zero-volume bars excluded from training and candidates", mk.provider(sc), min(sc))
        new["flat"]["ic"], new["flat"]["ic_t"] = load_report(data, "flat").get("mean_ic"), load_report(data, "flat").get("ic_t")
    vl, vs = qc.divergence_events(inp)
    add("v_div", "S0 + divergence veto (both sides)", prov_base, first_base, veto_long=vl, veto_short=vs)
    new["v_div"]["events_long"], new["v_div"]["events_short"] = int(vl.sum()), int(vs.sum())
    with open(data / "filings_8k.pkl", "rb") as fh:
        filings = pickle.load(fh)
    el = qc.eight_k_events(inp, filings)
    add("v_8k", "S0 + 8-K veto on longs only", prov_base, first_base, veto_long=el)
    new["v_8k"]["events_long"] = int(el.sum())
    add("t7_dd", "S0 + drawdown rule (0.5x gross at -15%, 1.0x after half recovered)", prov_base, first_base, dd_policy=True)

    # BH over the 17 stored cells plus every new cell with a p
    ps_names = [*pair17.keys(), *new.keys()]
    qs = bs.bh_adjusted([(pair17[n]["p"] if n in pair17 else new[n]["p"]) for n in ps_names])
    qmap = dict(zip(ps_names, qs))
    for n in new:
        new[n]["q"] = qmap[n]
    for n in pair17:
        pair17[n]["q"] = qmap[n]
    out["stored_cells_q"] = {n: qmap[n] for n in pair17}
    base_ic_t = base_rep.get("ic_t")
    bars: Dict[str, dict] = {}
    for n in ("t2_7", "t2_3", "t2_1", "t6_mh", "t6_vs"):
        if n in new:
            b = bar_model(new[n], base_ic_t, new[n].get("ic_t"), 0.10)
            b["q_ok"] = new[n]["q"] < qc.BH_Q
            b["met"] = bool(b["gain_ok"] and b["wins_ok"] and b["ic_ok"] and b["q_ok"])
            bars[n] = b
    for n in ("s5", "avg5", "flat"):
        if n in new:
            b = bar_model(new[n], None, None, 0.05)
            b["q_ok"] = new[n]["q"] < qc.BH_Q
            b["met"] = bool(b["gain_ok"] and b["wins_ok"] and b["q_ok"])
            bars[n] = b
    for n in ("v_div", "v_8k"):
        keep = new[n]["wins_by_0_10"] >= 6
        remove = new[n]["losses_by_0_10"] >= 6
        bars[n] = {"keep_veto_justified": keep, "remove_veto_justified": remove, "met": keep}
    d = new["t7_dd"]
    dd_gain = 1.0 - abs(d["max_dd"]) / abs(d["max_dd_s0"])
    wb_gain = 1.0 - abs(d["worst_block"]) / abs(d["worst_block_s0"])
    bars["t7_dd"] = {"max_dd_improvement": dd_gain, "worst_block_improvement": wb_gain, "sharpe_diff": d["mean_diff"],
                     "met": bool(dd_gain >= 0.20 and wb_gain >= 0.20 and d["mean_diff"] >= -0.05)}
    out["new_cells"], out["bars"] = new, bars
    md.append("\n## New cells against S0 (same calendars, paired)\n")
    md.append("| Cell | What | Wins of 8 | Mean paired Sharpe diff | p | BH q | Mean Sharpe (S0) | IC t (base) | Max DD (S0) | Worst block (S0) | Bar met |\n|---|---|---|---|---|---|---|---|---|---|---|")
    for n, v in new.items():
        icd = f"{v.get('ic_t')} ({base_ic_t})" if v.get("ic_t") is not None else "-"
        md.append(f"| {n} | {v['label']} | {v['wins']} | {v['mean_diff']:+.3f} | {v['p']:.3f} | {v['q']:.3f} | {v['mean_sharpe']:.3f} ({v['mean_sharpe_s0']:.3f}) | {icd} | "
                  f"{v['max_dd'] * 100:.1f}% ({v['max_dd_s0'] * 100:.1f}%) | {v['worst_block'] * 100:.1f}% ({v['worst_block_s0'] * 100:.1f}%) | "
                  f"{'YES' if bars.get(n, {}).get('met') else 'no'} |")

    # ------------------------------------------------------------ T8: dispersion over sixteen start offsets
    offs16 = [*qc.OFFSETS, *qc.EXTRA_OFFSETS]
    s7 = [c for c in bs.STRATEGIES if c.name == "S7"][0]
    s0_16 = qc.run_cell("S0", bs.S0, prov_base, inp, first_base, offsets=offs16)
    s7_16 = qc.run_cell("S7", s7, prov_base, inp, first_base, offsets=offs16)
    sh0, sh7 = sharpe_list(s0_16), sharpe_list(s7_16)

    def var(d: Dict[str, float], keys: List[int]) -> float:
        return float(np.var([d[str(k)] for k in keys if str(k) in d], ddof=1))
    t8 = {"offsets": offs16, "s0": sh0, "s7": sh7, "var_s0_16": var(sh0, offs16), "var_s7_16": var(sh7, offs16), "var_s0_8": var(sh0, qc.OFFSETS),
          "var_s7_8": var(sh7, qc.OFFSETS), "mean_s0_16": float(np.mean(list(sh0.values()))), "mean_s7_16": float(np.mean(list(sh7.values())))}
    t8["variance_reduction_16"] = 1.0 - t8["var_s7_16"] / t8["var_s0_16"]
    t8["variance_reduction_8"] = 1.0 - t8["var_s7_8"] / t8["var_s0_8"]
    t8["mean_diff"] = t8["mean_s7_16"] - t8["mean_s0_16"]
    t8["met"] = bool(t8["variance_reduction_16"] >= 0.30 and t8["mean_diff"] >= -0.05)
    out["T8"] = t8

    # ------------------------------------------------------------ T10 again, on every cell
    all_runs = {**standard, **{n: r for n, r in runs_new.items()}}
    names_all = [n for n in all_runs if n not in {c.name for c in bs.CAPACITY_CELLS}]
    t10_b = {}
    for o in qc.OFFSETS:
        try:
            t10_b[str(o)] = qc.pbo_cscv(matrix(names_all, all_runs, o))
        except ValueError as exc:
            t10_b[str(o)] = {"error": str(exc)}
    out["T10_all_cells"] = t10_b
    pb2 = [v["pbo"] for v in t10_b.values() if "pbo" in v]
    out["T10_all_summary"] = {"cells": len(names_all), "pbo_calendar_0": t10_b["0"].get("pbo"), "pbo_mean": float(np.mean(pb2)) if pb2 else None}
    md.append("\n## T10 again on all cells\n")
    md.append(f"{len(names_all)} cells; PBO per calendar: " + ", ".join(f"{o}: {v['pbo']:.3f}" for o, v in t10_b.items() if "pbo" in v))

    # ------------------------------------------------------------ deflated Sharpe with the running trial count
    n_new = len(new) + 1                                  # the new cells run, plus T8's S7 cell as the report counts it
    n_trials = 18 + n_new
    pooled = {n: np.concatenate([qc.block_series(r, o)[1] for o in r.offsets]) for n, r in all_runs.items()}
    best = max(new, key=lambda n: new[n]["mean_diff"])
    out["deflated"] = {"trials": n_trials, "S0": qc.deflated(pooled, "S0", n_trials), "best_new_cell": qc.deflated(pooled, best, n_trials),
                       "best_new_cell_name": best, "S5_stored": qc.deflated(pooled, "S5", n_trials)}
    out["trial_count"] = {"stored": 18, "new_cells_run": len(new), "t8_registered": 1, "running": n_trials}
    (data / "results.json").write_text(json.dumps(out, indent=1, default=float))
    (data / "tables.md").write_text("\n".join(md))
    print("\n".join(md))
    print(json.dumps({k: out[k] for k in ("T1_verdict", "T10_18_summary", "T10_all_summary", "T8", "deflated", "trial_count", "bars")}, indent=1, default=float))


if __name__ == "__main__":
    main()
