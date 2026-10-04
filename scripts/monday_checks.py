"""The questions the weekend could not answer, in one read-only pass.

Each change made between 3 and 4 October was deployed while US markets, futures
and EDGAR were closed, so its first real test is the next session. This prints
what each produced since a given time, and changes nothing:

    docker exec sentinel-api_gateway python /app/scripts/monday_checks.py
    docker exec sentinel-api_gateway python /app/scripts/monday_checks.py --since "2026-10-05 13:30"

`--since` is UTC and defaults to 13:30 today, the US cash open.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# (heading, SQL with $1 = since)
CHECKS = [
    ("Commodity and index-futures moves (item 23)",
     """SELECT primary_entity_id, count(*), round(max(abs((financial_data->>'change_pct_day')::float))::numeric, 2)
        FROM events WHERE type = 'price_anomaly' AND collected_at >= $1 GROUP BY 1 ORDER BY 2 DESC"""),
    ("Rules joining an OSINT exposure to a market move (item 23)",
     """SELECT rule_id, count(*), left(max(summary_headline), 90) FROM correlations
        WHERE detected_at >= $1 AND rule_id IN ('rule_physical_disruption_repricing', 'rule_news_financial_impact')
        GROUP BY 1"""),
    ("Block trades in the session (item 8)",
     """SELECT source, count(*), count(DISTINCT primary_entity_id), round(avg(anomaly_score)::numeric, 3)
        FROM events WHERE type = 'equity_block' AND collected_at >= $1 GROUP BY 1"""),
    ("Block-volume correlations (item 8)",
     """SELECT count(*) FROM correlations WHERE rule_id = 'rule_financial_block_volume_spike' AND detected_at >= $1"""),
    ("Form 4 codes, from the parsed filing (item 18)",
     """SELECT split_part(headline, ':', 1), count(*) FROM events
        WHERE type = 'insider_trade' AND collected_at >= $1 GROUP BY 1 ORDER BY 2 DESC"""),
    ("Airspace Disruption firings (item 20)",
     """SELECT count(*), left(max(summary_headline), 90) FROM correlations
        WHERE rule_id = 'rule_aviation_activity_surge' AND detected_at >= $1"""),
    ("Single-domain storms by kind, against the per-region baseline (item 29)",
     """SELECT regexp_replace(rule_id, '_cluster_.*', ''), count(*) FROM correlations
        WHERE rule_id ~ '_activity_cluster_' AND detected_at >= $1 GROUP BY 1"""),
    ("Semantic clusters, which now need two subjects or two domains (item 26)",
     """SELECT primary_domain, count(*), count(*) FILTER (WHERE summary_headline LIKE '%across 1 subject(s) in 1 domain(s)%')
        FROM correlations WHERE rule_id = 'SEMANTIC_001' AND detected_at >= $1 GROUP BY 1"""),
    ("Scenarios: copied worked example and horizon list (item 16)",
     """SELECT count(*),
               count(*) FILTER (WHERE hypotheses::text ILIKE '%6-hour AIS gap preceded%'),
               count(*) FILTER (WHERE hypotheses::text LIKE '%immediate | 24h%')
        FROM scenarios WHERE created_at >= $1"""),
    ("Agent predictions: stated conviction against outcome (item 27)",
     """SELECT agent_name, count(*), round(avg(confidence)::numeric, 3), count(resolved_at),
               count(*) FILTER (WHERE outcome_correct), round(avg(brier_score)::numeric, 3)
        FROM agent_predictions WHERE occurred_at >= $1 GROUP BY 1"""),
    ("Earnings events, now only for quoted names (item 28)",
     """SELECT type, count(*), string_agg(DISTINCT primary_entity_id, ',') FROM events
        WHERE source = 'finnhub_earnings' AND collected_at >= $1 GROUP BY 1"""),
    ("News reaching an instrument, by feed (item 13 and the feed changes)",
     """SELECT source, count(*), count(*) FILTER (WHERE tags::text LIKE '%exposes:%') FROM events
        WHERE type = 'headline' AND collected_at >= $1 GROUP BY 1 ORDER BY 3 DESC, 2 DESC LIMIT 25"""),
]


def _fmt(row: Any) -> str:
    return " | ".join("" if v is None else str(v) for v in row.values())


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", help="UTC, 'YYYY-MM-DD HH:MM' (default: 13:30 today)")
    args = p.parse_args()
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    else:
        since = datetime.now(timezone.utc).replace(hour=13, minute=30, second=0, microsecond=0)

    from shared.db import get_redis, get_timescale

    db = await get_timescale()
    print(f"since {since.isoformat()}\n")
    async with db._pool.acquire() as conn:
        for heading, sql in CHECKS:
            rows: List[Any] = await conn.fetch(sql, since, timeout=600)
            print(f"== {heading}")
            for r in rows or []:
                print("   " + _fmt(r))
            if not rows:
                print("   (none)")
            print()

    redis = await get_redis()
    clusters = [k async for k in redis.raw.scan_iter(match="sentinel:insider:clusters:*", count=500)]
    print("== Insider-buying clusters standing in Redis (item 18)")
    for k in clusters:
        raw = await redis.raw.get(k)
        try:
            c = json.loads(raw)
            print(f"   {c.get('ticker')}: {c.get('insider_count')} insiders, ${c.get('net_buy_usd', 0):,.0f} net")
        except (TypeError, ValueError):
            print(f"   {k}")
    if not clusters:
        print("   (none)")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
