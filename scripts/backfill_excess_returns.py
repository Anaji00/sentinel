"""Backfill the excess return of every resolved agent prediction, then print the market-neutral hit-rate report.

Registered 13 Nov 2026 (reports/Upgrade pre-registrations - 12 Nov.md, "Market-neutral grading"). Report only: it writes the
new agent_predictions columns (migration 0094) and nothing else. Idempotent (graded rows are never re-selected), bounded
(a row with no bars is parked after 3 attempts) and one row's failure does not stop the run.

Run inside an agents container:
    python scripts/backfill_excess_returns.py [--batch 500] [--max-batches 400] [--days 365] [--report-only]
"""
import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.excess_grading import grade_beta_pending, grade_pending  # noqa: E402
from shared.db import get_timescale  # noqa: E402
from shared.utils.excess_return import hit_report  # noqa: E402

_REPORT_SQL = """
    SELECT direction, (occurred_at AT TIME ZONE 'UTC')::date AS day, excess_spy, excess_sector, outcome_correct
    FROM agent_predictions
    WHERE excess_graded_at IS NOT NULL AND occurred_at > NOW() - make_interval(days => $1)
"""


def fmt(x, spec: str) -> str:
    return "n/a" if x is None else format(x, spec)


def print_table(title: str, rows) -> None:
    print(f"\n{title}")
    print(f"{'dir':<5}{'n':>7}{'days':>6}{'hit':>8}{'hit t':>8}{'mean bps':>10}{'ex t':>8}{'raw hit':>9}")
    for r in rows:
        print(f"{r['direction']:<5}{r['n']:>7}{r['days']:>6}{fmt(r['hit_rate'], '.3f'):>8}{fmt(r['hit_t'], '.2f'):>8}"
              f"{fmt(r['mean_excess_bps'], '.1f'):>10}{fmt(r['excess_t'], '.2f'):>8}{fmt(r['raw_hit_rate'], '.3f'):>9}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--max-batches", type=int, default=400)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--report-only", action="store_true")
    a = ap.parse_args()
    db = await get_timescale()
    if not a.report_only:
        total = {"selected": 0, "graded": 0, "no_bars": 0, "failed": 0}
        for i in range(a.max_batches):
            out = await grade_pending(db, a.batch)
            for k in total:
                total[k] += out[k]
            print(f"batch {i + 1}: {out}", flush=True)
            if out["selected"] == 0 or out["graded"] + out["no_bars"] + out["failed"] == 0:
                break
        print("backfill totals:", total)
        beta = {"selected": 0, "graded": 0, "no_beta": 0, "failed": 0}
        for i in range(a.max_batches):
            out = await grade_beta_pending(db, a.batch)
            for k in beta:
                beta[k] += out[k]
            print(f"beta batch {i + 1}: {out}", flush=True)
            if out["selected"] == 0 or out["graded"] + out["no_beta"] + out["failed"] == 0:
                break
        print("beta backfill totals:", beta)
    rows = await db.query(_REPORT_SQL, a.days) or []
    print(f"\ngraded rows in the last {a.days} days: {len(rows)}")
    print_table("Excess vs SPY (signed: positive = the call beat the market in its direction)",
                hit_report((r["direction"], r["day"], None if r["excess_spy"] is None else
                            r["excess_spy"] * (1 if str(r["direction"]).lower() in ("up", "long", "bullish") else -1),
                            r["outcome_correct"]) for r in rows))
    sector = [r for r in rows if r["excess_sector"] is not None]
    print_table(f"Excess vs sector ETF ({len(sector)} rows with a sector ETF)",
                hit_report((r["direction"], r["day"], r["excess_sector"] * (1 if str(r["direction"]).lower()
                            in ("up", "long", "bullish") else -1), r["outcome_correct"]) for r in sector))


if __name__ == "__main__":
    asyncio.run(main())
