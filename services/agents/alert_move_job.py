"""
services/agents/alert_move_job.py

Weekly: the alert expected-move model's walk-forward (shared/utils/
alert_move_model.py) on the last WINDOW_DAYS of stock alerts, written to
REPORT_KEY. It reports; nothing reads a prediction until the report says
validated. Training runs in a worker thread, off the event loop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from shared.utils import alert_move_model as amm
from shared.utils import producer_grades as pg
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.alert_move")

WINDOW_DAYS = 120
REPORT_KEY = "sentinel:alert_move_model:report"
WEEK_KEY = "sentinel:alert_move_model:week"
INTERVAL_SEC = 6 * 3600

_ALERTS_SQL = f"""
    SELECT rule_name, primary_entity_id AS ticker, min(detected_at) AS at, max(confidence_score) AS confidence,
           max(coalesce((metrics_summary->>'domain_count')::int, 1)) AS domains
    FROM correlations
    WHERE detected_at > NOW() - INTERVAL '{WINDOW_DAYS} days' AND primary_entity_id ~ '^[A-Z]{{1,5}}$'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
    GROUP BY 1, 2, date(detected_at)
"""
_BARS_SQL = "SELECT ticker, day, open, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day"


def _ts(x: Any) -> datetime:
    return x if isinstance(x, datetime) else datetime.fromisoformat(str(x))


class AlertMoveJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(1800)
        while True:
            week = datetime.now(timezone.utc).strftime("%G-W%V")
            try:
                done = await self.raw.get(WEEK_KEY) if self.raw is not None else None
                if (done.decode() if isinstance(done, bytes) else done) != week:
                    rep = await self.run_once()
                    logger.info("Alert move model: %s", rep)
                    if self.raw is not None:
                        await self.raw.set(REPORT_KEY, json.dumps(rep))
                        await self.raw.set(WEEK_KEY, week)
            except Exception as e:
                swallowed("agents.alert_move", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def alerts(self) -> List[amm.Alert]:
        rows = await self.db.query(_ALERTS_SQL) or []
        tickers = sorted({str(r["ticker"]).upper() for r in rows} | {"SPY"})
        since = date.today() - timedelta(days=WINDOW_DAYS + 120)
        bars: Dict[str, Tuple[List[date], List[float], List[float]]] = defaultdict(lambda: ([], [], []))
        for b in await self.db.query(_BARS_SQL, tickers, since) or []:
            d, o, c = bars[str(b["ticker"]).upper()]
            d.append(b["day"])
            o.append(float(b["open"] or b["close"]))
            c.append(float(b["close"]))
        sd, so, sc = bars.get("SPY", ([], [], []))
        spy = {d: (o, c) for d, o, c in zip(sd, so, sc)}
        by_ticker: Dict[str, List[datetime]] = defaultdict(list)
        for r in rows:
            by_ticker[str(r["ticker"]).upper()].append(_ts(r["at"]))
        for v in by_ticker.values():
            v.sort()
        out: List[amm.Alert] = []
        for r in rows:
            t, at = str(r["ticker"]).upper(), _ts(r["at"])
            d, o, c = bars.get(t, ([], [], []))
            got = pg.move_ratio(d, o, c, spy, at, 1) if d else None
            if not got or got[0] <= 0:
                continue
            i = bisect_left(d, at.date())
            usual = amm.usual_move(d, o, c, spy, i)
            if not usual:
                continue
            prior = by_ticker[t]
            recent = bisect_left(prior, at) - bisect_left(prior, at - timedelta(days=7))
            out.append(amm.Alert(at=at, ticker=t, family=pg.rule_family(str(r["rule_name"])),
                                 confidence=float(r["confidence"] or 0.0), domains=int(r["domains"] or 1),
                                 log_usual=math.log(usual), recent_alerts=int(recent), target=math.log(got[0])))
        return out

    async def run_once(self) -> Dict[str, Any]:
        alerts = await self.alerts()
        rep = await asyncio.to_thread(amm.walk_forward, alerts)
        rep["generated_at"] = datetime.now(timezone.utc).isoformat()
        return rep


__all__ = ["AlertMoveJob"]
