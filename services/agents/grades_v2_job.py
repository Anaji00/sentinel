"""
services/agents/grades_v2_job.py

Once a day, re-grades correlation rule families with one unit per (family, ticker, date), a same-day random-stock control and a t over dates
(shared/utils/grades_v2.py), and stores the table under GRADES_V2_KEY. The API serves it as the extra `clustered` key of /predictions/producer-grades. Additive: no rule
is retired or gated from it. The computation is CPU-bound and runs in a worker thread so the event loop is never held.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from shared.utils import agent_clusters as ac
from shared.utils import grades_v2 as g2
from shared.utils.producer_grades import rule_family
from shared.utils.quiet_failures import swallowed
from shared.utils.signal_store import PRODUCER_GRADES_V2_KEY

logger = logging.getLogger("agent.grades_v2")

GRADES_V2_KEY = PRODUCER_GRADES_V2_KEY
INTERVAL_SEC = 24 * 3600
DAYS = 90
_SQL = """
SELECT rule_name, primary_entity_id AS t, detected_at AS at FROM correlations
WHERE detected_at > NOW() - ($1 || ' days')::interval AND primary_entity_id ~ '^[A-Z]{1,5}$'
"""
_AGENT_SQL = """
SELECT agent_name, horizon_hours AS h, date(occurred_at) AS d, direction, excess_spy, excess_beta FROM agent_predictions
WHERE resolved_at IS NOT NULL AND direction IN ('up', 'down') AND occurred_at > NOW() - ($1 || ' days')::interval
"""
_BARS = ("SELECT ticker, day, open, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 260 AND close > 0 AND open > 0 "
         "ORDER BY ticker, day")


class GradesV2Job:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(600)
        while True:
            try:
                logger.info("Grades v2: %s", await self.run_once())
            except Exception as exc:
                swallowed("agents.grades_v2.pass", exc, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, Any]:
        rows = await self.db.query(_SQL, str(DAYS)) or []
        units: Dict[Any, datetime] = {}
        for r in rows:
            at = r["at"]
            at = datetime.fromisoformat(at.replace("Z", "+00:00")) if isinstance(at, str) else at
            units.setdefault((rule_family(r["rule_name"]), r["t"], at.astimezone(timezone.utc).date()), at)
        tickers = sorted({k[1] for k in units} | {"SPY"})
        bars: Dict[str, Any] = {}
        for i in range(0, len(tickers), 400):
            for r in await self.db.query(_BARS, tickers[i:i + 400]) or []:
                b = bars.setdefault(r["ticker"], ([], [], []))
                b[0].append(r["day"] if isinstance(r["day"], date) else date.fromisoformat(str(r["day"])[:10]))
                b[1].append(float(r["open"]))
                b[2].append(float(r["close"]))
        agents = await self.agent_rows()
        spy = bars.get("SPY")
        if not spy or not units:
            return {"families": 0, "agent_rows": len(agents), "why": "no SPY bars" if not spy else "no correlations"}
        spy_by_day = {d: (o, c) for d, o, c in zip(*spy)}
        table = await asyncio.to_thread(g2.grade_units, units, bars, spy_by_day)
        blob = {"generated_at": datetime.now(timezone.utc).isoformat(), "window_days": DAYS, "units": len(units),
                "note": "One unit per (family, ticker, date); compared with the mean forward move of random stocks on the same date; t over dates. Not used to retire or gate any rule.",
                "rows": table,
                "agents": {"window_days": DAYS, "min_dates": ac.MIN_DATES, "rows": agents,
                           "note": "One observation per (agent, horizon, detection date): mean signed excess return over SPY, t over dates. Rows under the minimum dates are not judged, which is not the same as no skill."}}
        if self.raw is not None:
            await self.raw.set(GRADES_V2_KEY, json.dumps(blob), ex=3 * INTERVAL_SEC)
        return {"families": len({r["family"] for r in table}), "rows": len(table), "units": len(units), "agent_rows": len(agents)}


    async def agent_rows(self) -> List[Dict[str, Any]]:
        rows = await self.db.query(_AGENT_SQL, str(DAYS)) or []
        def calls(col: str) -> List[ac.Call]:
            return [(str(r["agent_name"]), int(r["h"]) if r["h"] is not None else None,
                     r["d"] if isinstance(r["d"], date) else date.fromisoformat(str(r["d"])[:10]),
                     ac.signed_excess(str(r["direction"]), None if r[col] is None else float(r[col]))) for r in rows]

        # Both bases: excess over SPY (kept for comparison) and beta-adjusted excess, which is the one to read (high-beta shorts in a falling market
        # look skilled on the first: macro engine +84 bps t 2.03 becomes +47 bps t 1.58, baseline +105 t 1.87 becomes +10 t 0.44).
        spy_rows, beta_rows = await asyncio.gather(asyncio.to_thread(ac.grade_agents, calls("excess_spy")),
                                                   asyncio.to_thread(ac.grade_agents, calls("excess_beta")))
        return [{**r, "basis": "excess_spy"} for r in spy_rows] + [{**r, "basis": "excess_beta"} for r in beta_rows]


__all__ = ["GRADES_V2_KEY", "GradesV2Job"]
