"""
services/agents/source_health_job.py

Every half hour: each (source, event type) seen in the last LOOKBACK_DAYS,
judged against its own rhythm (shared/utils/source_health.py), into
REPORT_KEY for the API and the dashboard. A source newly silent or thin is
logged once per state change, so a quiet feed is seen the day it goes quiet.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from shared.utils import source_health as sh
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.source_health")

INTERVAL_SEC = 30 * 60
LOOKBACK_DAYS = 60
REPORT_KEY = "sentinel:source_health:report"
STATE_KEY = "sentinel:source_health:state"          # (source|type) -> last status, for change logging
_SQL = f"""
    WITH recent AS (
        SELECT source, type, occurred_at FROM events
        WHERE occurred_at > NOW() - INTERVAL '{sh.BASELINE_DAYS} days' AND occurred_at <= NOW()
    ), gaps AS (
        SELECT source, type,
               max(EXTRACT(EPOCH FROM occurred_at - prev) / 3600) AS max_gap_h
        FROM (SELECT source, type, occurred_at,
                     lag(occurred_at) OVER (PARTITION BY source, type ORDER BY occurred_at) AS prev FROM recent) g
        GROUP BY source, type
    ), days AS (
        SELECT source, type, min(n) AS min_day, count(*) AS n_days FROM (
            SELECT source, type, date(occurred_at) AS d, count(*) AS n FROM recent
            WHERE occurred_at < date_trunc('day', NOW()) AND occurred_at >= date_trunc('day', NOW()) - INTERVAL '{sh.BASELINE_DAYS - 1} days'
            GROUP BY 1, 2, 3) x GROUP BY source, type
    ), totals AS (
        SELECT source, type, max(occurred_at) AS last_seen,
               count(*) FILTER (WHERE occurred_at > NOW() - INTERVAL '{sh.BASELINE_DAYS} days') AS n_base,
               count(*) FILTER (WHERE occurred_at > NOW() - INTERVAL '24 hours') AS n_24h
        FROM events WHERE occurred_at > NOW() - INTERVAL '{LOOKBACK_DAYS} days' AND occurred_at <= NOW()
        GROUP BY source, type
    )
    SELECT t.*, g.max_gap_h, d.min_day, d.n_days FROM totals t
    LEFT JOIN gaps g USING (source, type) LEFT JOIN days d USING (source, type)
"""


def _ts(x: Any) -> datetime:
    t = x if isinstance(x, datetime) else datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


class SourceHealthJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(120)
        while True:
            try:
                rep = await self.run_once()
                logger.info("Source health: %s", rep["counts"])
            except Exception as e:
                swallowed("agents.source_health.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        rows = [{**dict(r), "last_seen": _ts(r["last_seen"]) if r["last_seen"] else None}
                for r in await self.db.query(_SQL) or []]
        assessed = sh.assess(rows, now)
        counts: Dict[str, int] = {}
        for a in assessed:
            counts[str(a["status"])] = counts.get(str(a["status"]), 0) + 1
        rep = {"generated_at": now.isoformat(), "counts": counts, "sources": assessed}
        await self.raw.set(REPORT_KEY, json.dumps(rep, default=str))
        before = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                  for k, v in (await self.raw.hgetall(STATE_KEY) or {}).items()}
        changed = {}
        for a in assessed:
            key, status = f"{a['source']}|{a['type']}", str(a["status"])
            if before.get(key) != status:
                changed[key] = status
                if status in ("silent", "thin"):
                    logger.warning("Source %s / %s is %s: %s", a["source"], a["type"], status, a["why"])
                elif before.get(key) in ("silent", "thin"):
                    logger.info("Source %s / %s recovered", a["source"], a["type"])
        if changed:
            await self.raw.hset(STATE_KEY, mapping=changed)
        return rep


__all__ = ["REPORT_KEY", "SourceHealthJob"]
