import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale

async def main():
    print("Connecting to TimescaleDB inside container...")
    db = await get_timescale()
    count = await db.query_one("SELECT count(*) FROM events;")
    print(f"Total events in TimescaleDB: {count['count'] if count else 0}")
    
    rows = await db.query("SELECT event_id, type, source, occurred_at, primary_entity_name FROM events ORDER BY occurred_at DESC LIMIT 5;")
    print("Recent 5 events in TimescaleDB:")
    for r in rows:
        print(f"  {r['event_id']} | {r['type']} | {r['source']} | {r['primary_entity_name']}")

if __name__ == "__main__":
    asyncio.run(main())
