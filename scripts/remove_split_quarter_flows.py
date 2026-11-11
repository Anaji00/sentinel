"""Remove archived 13F flow events that fall in a quarter in which the ticker split (reports/Hardening audit - phases 2 and 3.md).

13F share counts are unadjusted, so the flows built before split dates were known read a split as holders adding or
cutting. This finds the archived accumulation and distribution events whose ticker has a split between the previous
quarter end and the event's period, saves them to a CSV, and with --apply deletes exactly those rows by their
(event_id, created_at) key. Dry run by default. Run inside an agents container:

    python scripts/remove_split_quarter_flows.py [--apply]
"""
import argparse
import asyncio
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.ownership import SPLIT_DATES_KEY, previous_period, split_tickers_in  # noqa: E402

SELECT = """
    SELECT event_id, created_at, type, source, headline, tags, primary_entity_id AS ticker FROM signal_event_archive
    WHERE type = 'thirteen_f' AND ('institutional_accumulation' = ANY(tags) OR 'institutional_distribution' = ANY(tags))
"""
OUT = "/tmp/removed_split_quarter_flows.csv"


def period_of(tags) -> str:
    return next((t.split(":", 1)[1] for t in tags or [] if t.startswith("period:")), "")


def affected(rows, splits) -> list:
    """The rows whose ticker split between the previous quarter end and the row's period."""
    out = []
    for r in rows:
        period = period_of(r["tags"])
        if period and str(r["ticker"]).upper() in split_tickers_in(splits, previous_period(period), period):
            out.append(r)
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    raw = getattr(await get_redis(), "raw")
    splits = {(k.decode() if isinstance(k, bytes) else k): json.loads(v) for k, v in (await raw.hgetall(SPLIT_DATES_KEY)).items()}
    db = await get_timescale()
    rows = await db.query(SELECT)
    hit = affected(rows, splits)
    print(f"archived 13F flow events {len(rows)}; on a split quarter {len(hit)}")
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "created_at", "type", "source", "headline", "tags", "ticker"])
        for r in hit:
            w.writerow([r["event_id"], str(r["created_at"]), r["type"], r["source"], r["headline"], "|".join(r["tags"] or []), r["ticker"]])
    if not args.apply:
        print("dry run; saved", OUT)
        return
    for r in hit:
        await db.execute("DELETE FROM signal_event_archive WHERE event_id = $1 AND created_at = $2::timestamptz", r["event_id"], datetime.fromisoformat(str(r["created_at"])))
    left = affected(await db.query(SELECT), splits)
    total = await db.query("SELECT count(*) AS n FROM signal_event_archive WHERE type = 'thirteen_f' AND ('institutional_accumulation' = ANY(tags) OR 'institutional_distribution' = ANY(tags))")
    print(f"deleted {len(hit)}; split-quarter rows left {len(left)}; flow events now {total[0]['n']}; copy in {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
