"""Remove duplicate archived 13F flow events: the same ticker, period and direction archived by both the live and history paths.

Keeps the earliest-created row of each group, saves the others to a CSV, and with --apply deletes them in one
transaction with TimescaleDB's per-transaction decompression limit lifted (the archive is compressed; the default
limit of 100,000 rows stopped the earlier cleanup after 5 rows). Dry run by default. Run inside an agents container:

    python scripts/remove_duplicate_flows.py [--apply]
"""
import argparse
import asyncio
import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_timescale  # noqa: E402

SELECT = """
    SELECT event_id, created_at, source, headline, tags, primary_entity_id AS ticker FROM signal_event_archive
    WHERE type = 'thirteen_f' AND ('institutional_accumulation' = ANY(tags) OR 'institutional_distribution' = ANY(tags))
    ORDER BY created_at
"""
OUT = "/tmp/removed_duplicate_flows.csv"


def group_key(row) -> tuple:
    tags = row["tags"] or []
    period = next((t for t in tags if t.startswith("period:")), "")
    direction = "acc" if "institutional_accumulation" in tags else "dist"
    return (str(row["ticker"]).upper(), period, direction)


def duplicates(rows) -> list:
    """Every row after the first (earliest created) of its ticker, period and direction. `rows` must be time-ordered."""
    seen: set = set()
    out = []
    for r in rows:
        k = group_key(r)
        if not k[1]:
            continue                                    # no period tag: cannot tell it is the same quarter
        if k in seen:
            out.append(r)
        else:
            seen.add(k)
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    db = await get_timescale()
    rows = await db.query(SELECT)
    dup = duplicates(rows)
    print(f"archived 13F flow events {len(rows)}; duplicates {len(dup)}")
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "created_at", "source", "headline", "tags", "ticker"])
        for r in dup:
            w.writerow([r["event_id"], str(r["created_at"]), r["source"], r["headline"], "|".join(r["tags"] or []), r["ticker"]])
    for r in dup[:6]:
        print("  ", r["ticker"], r["source"], str(r["created_at"])[:10], [t for t in (r["tags"] or []) if t.startswith("period:")])
    if not args.apply or not dup:
        print("dry run; saved", OUT)
        return
    async with db._pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
            for r in dup:
                await conn.execute("DELETE FROM signal_event_archive WHERE event_id = $1 AND created_at = $2::timestamptz",
                                   r["event_id"], datetime.fromisoformat(str(r["created_at"])))
    left = duplicates(await db.query(SELECT))
    print(f"deleted {len(dup)}; duplicates left {len(left)}; copy in {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
