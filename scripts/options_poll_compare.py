"""What option polling costs, session by session. Read-only.

scripts/ is baked into the images rather than mounted, so it goes in over stdin:

    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-factor-model python - < scripts/options_poll_compare.py
    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-factor-model python - --days 21 --no-replay < scripts/options_poll_compare.py

Two views. The stored prints of each session: how many, how much premium, how many had a
readable side, a high score or a correlation. There is no durable alert log (Redis keeps the
last 500 for a week), so "correlated" stands in for prints that reached an alert.

The replay: Alpaca's full trade history for contracts with a stored print, against what
latestTrade could have shown at each cycle length (options_flow.poll_capture_weight). It
misses contracts whose every large trade went unseen, and gates on total premium where the
collector gates on extrinsic, so read the ratio between cycles rather than the absolute level.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

from shared.utils.options_flow import poll_capture_weight

# Cycle at each setting, calls included: 84s and 313s measured at 60 and 300, 163s expected at 150.
CYCLES_SEC = {"84s": 84.0, "163s": 163.0, "313s": 313.0}
MIN_PREMIUM_USD = 50_000.0
OUTAGE_EVENTS = 5_000                         # a whole-platform stop, not a quiet session
TRADES_URL = "https://data.alpaca.markets/v1beta1/options/trades"

SESSIONS_SQL = """
    SELECT (occurred_at AT TIME ZONE 'America/New_York')::date AS day,
           count(*) AS all_events,
           count(*) FILTER (WHERE type = 'options_flow') AS prints,
           coalesce(sum((financial_data->>'premium_usd')::float) FILTER (WHERE type = 'options_flow'), 0) AS premium,
           count(*) FILTER (WHERE type = 'options_flow' AND tags && ARRAY['buyer_initiated', 'seller_initiated']) AS side_read,
           count(*) FILTER (WHERE type = 'options_flow' AND anomaly_score >= 0.5) AS high_score,
           count(*) FILTER (WHERE type = 'options_flow' AND array_length(correlation_ids, 1) > 0) AS correlated
    FROM events WHERE occurred_at > now() - make_interval(days => $1)
    GROUP BY 1 HAVING count(*) FILTER (WHERE type = 'options_flow') > 0 ORDER BY 1"""

CONTRACTS_SQL = """
    SELECT DISTINCT financial_data->>'ticker' AS t, financial_data->>'strike' AS k,
           financial_data->>'expiry' AS e, financial_data->>'option_type' AS o
    FROM events WHERE type = 'options_flow' AND (occurred_at AT TIME ZONE 'America/New_York')::date = $1"""


def occ_symbol(ticker: str, expiry: str, kind: str, strike: str) -> str:
    return f"{ticker}{expiry[2:4]}{expiry[5:7]}{expiry[8:10]}{kind[0]}{int(round(float(strike) * 1000)):08d}"


def fetch_trades(symbols: List[str], day: date, headers: Dict[str, str]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    token: Optional[str] = None
    while True:
        query = {"symbols": ",".join(symbols), "start": f"{day}T13:00:00Z", "end": f"{day}T21:30:00Z",
                 "limit": 10000, "sort": "asc"}
        if token:
            query["page_token"] = token
        request = urllib.request.Request(f"{TRADES_URL}?{urllib.parse.urlencode(query)}", headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            body = json.load(response)
        for symbol, trades in (body.get("trades") or {}).items():
            out[symbol].extend(trades)
        token = body.get("next_page_token")
        if not token:
            return out


def replay(trades: List[Dict[str, Any]]) -> Dict[str, float]:
    """One contract's day: its large trades, and those each cycle would have caught."""
    out = {"large": 0.0, "premium": 0.0, **{c: 0.0 for c in CYCLES_SEC}, **{f"{c}_premium": 0.0 for c in CYCLES_SEC}}
    times = [datetime.fromisoformat(t["t"].replace("Z", "+00:00")) for t in trades]
    for i, trade in enumerate(trades):
        premium = float(trade["p"]) * float(trade["s"]) * 100.0
        if premium < MIN_PREMIUM_USD:
            continue
        gap = (times[i + 1] - times[i]).total_seconds() if i + 1 < len(trades) else None
        out["large"] += 1
        out["premium"] += premium
        for cycle, seconds in CYCLES_SEC.items():
            weight = poll_capture_weight(gap, seconds)
            out[cycle] += weight
            out[f"{cycle}_premium"] += weight * premium
    return out


async def replay_session(conn: asyncpg.Connection, day: date, headers: Dict[str, str]) -> Optional[Dict[str, float]]:
    rows = await conn.fetch(CONTRACTS_SQL, day)
    # Alpaca's trades endpoint refuses share-class symbols such as BRK.B.
    symbols = sorted({occ_symbol(r["t"], r["e"], r["o"], r["k"]) for r in rows
                      if r["t"] and r["k"] and r["e"] and r["o"] and "." not in r["t"]})
    totals: Dict[str, float] = defaultdict(float)
    for i in range(0, len(symbols), 100):
        try:
            batch = await asyncio.to_thread(fetch_trades, symbols[i:i + 100], day, headers)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            print(f"  {day}: replay stopped at contract {i} of {len(symbols)}: {e}")
            return None
        for trades in batch.values():
            trades.sort(key=lambda t: t["t"])
            for key, value in replay(trades).items():
                totals[key] += value
        await asyncio.sleep(0.5)    # the snapshot poller shares this rate limit
    return totals


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--no-replay", action="store_true")
    args = parser.parse_args()

    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    sessions = await conn.fetch(SESSIONS_SQL, args.days)
    headers = {"APCA-API-KEY-ID": os.environ["ALPACA_API_KEY"], "APCA-API-SECRET-KEY": os.environ["ALPACA_SECRET_KEY"],
               "accept": "application/json"}

    print(f"{'session':<11}{'status':<9}{'prints':>7}{'premium':>10}{'side read':>11}{'score>=.5':>11}{'correlated':>12}"
          f"{'large':>8}{'84s':>7}{'163s':>7}{'313s':>7}{'lost at 163s':>14}")
    today = datetime.now(timezone.utc).date()
    for s in sessions:
        status = "outage" if s["all_events"] < OUTAGE_EVENTS else "ok"
        line = (f"{s['day']!s:<11}{status:<9}{s['prints']:>7}{s['premium'] / 1e6:>9.1f}M"
                f"{100 * s['side_read'] / s['prints']:>10.1f}%{s['high_score']:>11}{s['correlated']:>12}")
        replay_day = not args.no_replay and status != "outage" and s["day"] < today
        totals = await replay_session(conn, s["day"], headers) if replay_day else None
        if totals and totals.get("large"):
            lost = 1 - totals["163s"] / totals["84s"] if totals["84s"] else 0.0
            line += f"{totals['large']:>8.0f}{totals['84s']:>7.0f}{totals['163s']:>7.0f}{totals['313s']:>7.0f}{100 * lost:>13.0f}%"
        print(line)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
