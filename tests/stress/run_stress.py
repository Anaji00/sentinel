"""Runs the stress rounds over several seeds and writes a report.

    python -m tests.stress.run_stress --seeds 1 2 3 --out "reports/System stress test.md"

A round is one layer of the system. Each seed regenerates every case, so a defect that
shows up on all seeds is a property of the code and one that shows up on one is a property
of an unlucky input. Offline: nothing is published and the live database is not touched.
"""
from __future__ import annotations

import argparse
import logging
import os
import pathlib
import sys
import time
from collections import OrderedDict
from typing import List

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYTHONUTF8", "1")

from tests.stress.core import DEFECT, NOTE, PASS, Ledger  # noqa: E402


def run_all(seeds: List[int]) -> List[Ledger]:
    logging.disable(logging.CRITICAL)
    from tests.stress import round1_collection, round2_enrichment, round3_correlation, round4_reasoning

    out: List[Ledger] = []
    for seed in seeds:
        for fn in (round1_collection.run, round2_enrichment.run, round3_correlation.run, round4_reasoning.run):
            t = time.time()
            ledger = fn(seed)
            ledger.seconds = time.time() - t   # type: ignore[attr-defined]
            out.append(ledger)
    return out


def render(ledgers: List[Ledger], seeds: List[int], title: str = "System stress test") -> str:
    layers: "OrderedDict[str, List[Ledger]]" = OrderedDict()
    for ledger in ledgers:
        layers.setdefault(ledger.layer, []).append(ledger)
    total_cases = sum(ledger.cases() for ledger in ledgers)
    lines = [f"# {title}", "",
             f"{total_cases:,} generated cases over {len(seeds)} seeds ({', '.join(map(str, seeds))}), four layers, offline against the real code.", "",
             "| Layer | Cases | Families | Pass | Note | Defect |", "|---|---:|---:|---:|---:|---:|"]
    for layer, ls in layers.items():
        t = {PASS: 0, NOTE: 0, DEFECT: 0}
        for ledger in ls:
            for k, v in ledger.tally().items():
                t[k] += v
        lines.append(f"| {layer} | {sum(x.cases() for x in ls):,} | {sum(len(x.findings) for x in ls)} | {t[PASS]} | {t[NOTE]} | {t[DEFECT]} |")
    for verdict, heading in ((DEFECT, "Defects"), (NOTE, "Notes")):
        merged: "OrderedDict[tuple, dict]" = OrderedDict()
        for ledger in ledgers:
            for f in ledger.findings:
                if f.verdict == verdict:
                    m = merged.setdefault((f.layer, f.family), {"seeds": [], "summary": f.summary, "examples": f.examples})
                    m["seeds"].append(ledger.seed)
        lines += ["", f"## {heading}", ""]
        if not merged:
            lines.append("None.")
        for (layer, family), m in merged.items():
            lines.append(f"- **{layer} | {family}** (seeds {sorted(set(m['seeds']))}): {m['summary']}")
            for e in m["examples"]:
                lines.append(f"  - `{e}`")
    lines += ["", "## What held", ""]
    seen = set()
    for ledger in ledgers:
        for f in ledger.findings:
            if f.verdict == PASS and (f.layer, f.family) not in seen:
                seen.add((f.layer, f.family))
                lines.append(f"- {f.layer} | {f.family}: {f.summary}")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3])
    ap.add_argument("--out", default="reports/System stress test.md")
    ap.add_argument("--title", default="System stress test")
    args = ap.parse_args()
    ledgers = run_all(args.seeds)
    path = ROOT / args.out
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(ledgers, args.seeds, args.title), encoding="utf-8")
    tally = {PASS: 0, NOTE: 0, DEFECT: 0}
    for ledger in ledgers:
        for k, v in ledger.tally().items():
            tally[k] += v
    print(f"{sum(x.cases() for x in ledgers):,} cases; {tally}; report: {path}")
    return 0 if tally[DEFECT] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
