"""Fill sentinel:corporate_actions:splits with every split and reverse split since 2016 (reports/Hardening audit - phases 2 and 3.md).

13F share counts are unadjusted, so the 13F flows need to know which tickers had a split between two quarter ends
(shared/utils/ownership.py split_tickers_in). The tradfi collector keeps split dates going forward from its checkpoint;
this fetches the years before it. Read-only calls to the broker's free corporate-actions feed, one Redis hash written,
merged with what is already there, so it can be re-run.

Run in a one-off container with the broker keys in its environment:
    python scripts/backfill_split_dates.py [--since 2016-01-01] [--dry-run]
"""
import argparse
import asyncio
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_redis  # noqa: E402
from shared.utils.corporate_actions import split_events  # noqa: E402
from shared.utils.ownership import SPLIT_DATES_KEY  # noqa: E402

URL = "https://data.alpaca.markets/v1/corporate-actions"
TYPES = "forward_split,reverse_split,unit_split"
WINDOW_DAYS = 90


def windows(since: date, until: date, days: int = WINDOW_DAYS):
    """(start, end) pairs covering since..until without gaps or overlaps."""
    out, start = [], since
    while start <= until:
        end = min(until, start + timedelta(days=days - 1))
        out.append((start, end))
        start = end + timedelta(days=1)
    return out


async def fetch(session: aiohttp.ClientSession, headers: dict, start: date, end: date) -> dict:
    merged: dict = {"corporate_actions": {}}
    token = None
    for _ in range(60):
        params = {"types": TYPES, "start": start.isoformat(), "end": end.isoformat(), "limit": 1000}
        if token:
            params["page_token"] = token
        async with session.get(URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"corporate actions HTTP {resp.status}: {(await resp.text())[:200]}")
            body = await resp.json()
        for k, v in (body.get("corporate_actions") or {}).items():
            merged["corporate_actions"].setdefault(k, []).extend(v or [])
        token = body.get("next_page_token")
        if not token:
            break
    return merged


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not (key and secret):
        raise SystemExit("no broker keys in the environment")
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    today = date.today()
    found: dict = {}
    async with aiohttp.ClientSession() as session:
        for start, end in windows(date.fromisoformat(args.since), today):
            body = await fetch(session, headers, start, end)
            for sym, dates in split_events(body, today).items():
                found[sym] = sorted(set(found.get(sym, [])) | set(dates))
            print(f"{start} to {end}: {len(found)} tickers with a split so far", flush=True)
            await asyncio.sleep(0.4)
    total = sum(len(v) for v in found.values())
    print(f"found {total} split dates on {len(found)} tickers since {args.since}")
    if args.dry_run:
        return
    raw = getattr(await get_redis(), "raw", None)
    for sym, dates in found.items():
        old = await raw.hget(SPLIT_DATES_KEY, sym)
        merged = sorted(set(json.loads(old) if old else []) | set(dates))
        await raw.hset(SPLIT_DATES_KEY, sym, json.dumps(merged))
    print(f"wrote {len(found)} tickers to {SPLIT_DATES_KEY}")


if __name__ == "__main__":
    asyncio.run(main())
