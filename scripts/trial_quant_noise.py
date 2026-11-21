"""Noise floor of the 19 Nov trials (registered 20 Nov 2026, BEFORE the extra fits ran; descriptive, adds no trial to the deflated-Sharpe count).
Question: how much does S0's net Sharpe move when only the LightGBM seed changes? Cells: `base` (seed 7, the live seed) and `seed8` to `seed11`
(`trial_quant_scores.py`, nothing else changed). For each of the five fits S0 is simulated on the eight calendars at the 5.3% fee (the trials' accounting)
and at 2.0%. Reported: mean Sharpe per fit, the standard deviation of the mean across fits, and the paired mean difference of each other seed against
seed 7 (the "same model, different draw" difference) with its spread. Reading fixed now: an earlier cell's paired difference from base counts as
distinguishable from a refit only if its magnitude exceeds 2 x the standard deviation of the four seed-vs-seed-7 paired differences; the five-seed
average (cell s5) is compared with the mean of the five single fits. Inputs: the files of trial_quant_scores.py; offline.
    python scripts/trial_quant_noise.py --data /out
"""
import argparse
import json
import pickle
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import trial_quant_common as qc  # noqa: E402
from shared.utils import book_strategies as bs  # noqa: E402

FITS = ["base", "seed8", "seed9", "seed10", "seed11"]
EARLIER = ["t2_7", "t2_3", "t6_mh", "t6_vs", "flat", "s5"]


def sharpes(data: Path, inp: qc.Inputs, mk: qc.Market, name: str, fee: float) -> dict:
    with open(data / f"scores_{name}.pkl", "rb") as fh:
        scores = pickle.load(fh)
    run = qc.run_cell(name, bs.S0, mk.provider(scores), inp, max(min(scores), 575), fee_bps=fee)
    return {o: bs.sharpe(run.results[o].block_returns) for o in run.offsets}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="/out")
    args = ap.parse_args()
    data = Path(args.data)
    inp = qc.Inputs.load(data / "inputs.npz")
    mk = qc.Market(inp)
    out = {}
    for fname, fee in (("5.3%", 44.0), ("2.0%", qc.FEE_BPS["2.0%"])):
        per = {n: sharpes(data, inp, mk, n, fee) for n in FITS if (data / f"scores_{n}.pkl").exists()}
        means = {n: float(np.mean(list(v.values()))) for n, v in per.items()}
        diffs = {n: float(np.mean([per[n][o] - per["base"][o] for o in per["base"]])) for n in per if n != "base"}
        sd_diff = float(np.std(list(diffs.values()), ddof=1)) if len(diffs) > 1 else float("nan")
        earlier = {}
        for n in EARLIER:
            if (data / f"scores_{n}.pkl").exists():
                v = sharpes(data, inp, mk, n, fee)
                d = float(np.mean([v[o] - per["base"][o] for o in per["base"]]))
                earlier[n] = {"mean_sharpe": float(np.mean(list(v.values()))), "diff_vs_base": d,
                              "distinguishable_from_refit": bool(abs(d) > 2 * sd_diff)}
        out[fname] = {"fits": means, "per_calendar": per, "sd_of_mean_across_fits": float(np.std(list(means.values()), ddof=1)),
                      "range_of_mean": [min(means.values()), max(means.values())], "paired_diff_vs_seed7": diffs, "sd_paired_diff": sd_diff,
                      "mean_of_single_fits": float(np.mean(list(means.values()))), "earlier_cells": earlier}
    (data / "noise.json").write_text(json.dumps(out, indent=1, default=float))
    print(json.dumps({k: {x: v[x] for x in ("fits", "sd_of_mean_across_fits", "paired_diff_vs_seed7", "sd_paired_diff", "mean_of_single_fits", "earlier_cells")}
                      for k, v in out.items()}, indent=1, default=float))


if __name__ == "__main__":
    main()
