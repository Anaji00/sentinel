"""
services/agents/alert_drift_job.py

Every six hours: the ev_alert_drift_5 trial feature (shared/utils/alert_drift.py)
for alerts since its first day, into stock_features_daily. Idempotent: each
pass rewrites the same rows.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from shared.utils import alert_drift as ad
from shared.utils import producer_grades as pg
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.alert_drift")

INTERVAL_SEC = 6 * 3600
SOURCE = "alert_drift"

_ALERTS_SQL = """
    SELECT primary_entity_id AS ticker, rule_name, min(detected_at) AS at FROM correlations
    WHERE detected_at >= $1 AND primary_entity_id ~ '^[A-Z]{1,5}$'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
    GROUP BY 1, 2, date(detected_at)
"""
_BARS_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day"
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source)
    SELECT t, d, $3, v, $4 FROM unnest($1::text[], $2::date[], $5::float8[]) AS x(t, d, v)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _ts(x: Any) -> datetime:
    return x if isinstance(x, datetime) else datetime.fromisoformat(str(x).replace("Z", "+00:00"))


class AlertDriftJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db

    async def run(self) -> None:
        await asyncio.sleep(600)
        while True:
            try:
                out = await self.run_once()
                if out.get("rows"):
                    logger.info("Alert drift feature: %s", out)
            except Exception as e:
                swallowed("agents.alert_drift.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, int]:
        rows = await self.db.query(_ALERTS_SQL, datetime.combine(ad.FIRST_DAY, datetime.min.time())) or []
        alerts = [(str(r["ticker"]).upper(), _ts(r["at"]), pg.rule_family(str(r["rule_name"]))) for r in rows]
        alerts = [a for a in alerts if a[2] in ad.FAMILIES]
        if not alerts:
            return {"alerts": 0, "rows": 0}
        bars: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
        for b in await self.db.query(_BARS_SQL, sorted({a[0] for a in alerts} | {"SPY"}),
                                     ad.FIRST_DAY - timedelta(days=14)) or []:
            d, c = bars[str(b["ticker"]).upper()]
            d.append(b["day"])
            c.append(float(b["close"]))
        sd, sc = bars.get("SPY", ([], []))
        spy_close = dict(zip(sd, sc))
        prior: Dict[Tuple[str, date], Optional[float]] = {}
        for t, at, _ in alerts:
            d, c = bars.get(t, ([], []))
            prior[(t, at.date())] = pg.prior_move(d, c, spy_close, at) if d else None
        out = ad.drift_rows([(t, at.date(), f) for t, at, f in alerts], prior)
        if out:
            t, d, v = (list(x) for x in zip(*out))
            await self.db.execute(_UPSERT_FEATURE, t, d, ad.FEATURE, SOURCE, v)
        return {"alerts": len(alerts), "rows": len(out)}


__all__ = ["AlertDriftJob"]
