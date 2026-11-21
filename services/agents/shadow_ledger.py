"""
services/agents/shadow_ledger.py

Two record-only series about the trader's stored B1 book, written once a day to `book_shadow_ledger` and never read by the trader (requested 21 Nov 2026,
from the 19 Nov quant trials, reports/Quant trial results - 19 Nov.md): the book's estimated daily borrow cost at 2% a year ('borrow_2pct') and, per side and
per registered veto rule, the names the veto would have removed ('veto_counts'). No veto is applied, no order is sent, no sizing, cap, kill switch or execution
is touched. A failed write is counted with swallowed(), never reported as recorded.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.utils import shadow_ledger as sl
from shared.utils.quiet_failures import swallowed
from shared.utils.signal_store import AUTO_TRADE_BOOK_KEY

logger = logging.getLogger("agent.shadow_ledger")

INTERVAL_SEC = 3600
BOOK_KEY = AUTO_TRADE_BOOK_KEY
_EIGHT_K_ITEM = re.compile(r"\b(\d\.\d\d):")
_COUNT_SQL = "SELECT count(*) AS n FROM book_shadow_ledger WHERE day = $1::date AND kind = $2"
_INSERT_SQL = "INSERT INTO book_shadow_ledger (day, kind, payload) VALUES ($1::date, $2, $3::jsonb) ON CONFLICT (day, kind) DO NOTHING"
# The same look-back and sources as the trader's live vetoes (services/agents/auto_trader.py), read here only to count.
_VETO_8K_SQL = """
    SELECT primary_entity_id AS ticker, summary FROM events
    WHERE type = 'filing' AND summary LIKE 'Material 8-K Event%' AND created_at > NOW() - INTERVAL '3 days'
      AND primary_entity_id = ANY($1::text[])
"""
_VETO_DIVERGENCE_SQL = """
    SELECT primary_entity_id AS ticker, (metrics_summary->>'return')::float AS ret, (metrics_summary->>'z')::float AS z
    FROM correlations
    WHERE rule_id IN ('DIVERGENCE_NEWS', 'DIVERGENCE_UNEXPLAINED') AND detected_at > NOW() - INTERVAL '3 days'
      AND primary_entity_id = ANY($1::text[])
"""


class ShadowLedger:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(180)
        while True:
            try:
                logger.info("Shadow ledger: %s", await self.run_once())
            except Exception as exc:
                swallowed("agents.shadow_ledger.pass", exc, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def _payloads(self, stored: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        longs, shorts = list(stored.get("long") or []), list(stored.get("short") or [])
        out: Dict[str, Dict[str, Any]] = {"borrow_2pct": {**sl.borrow_cost(longs, shorts), "rebalanced_on": stored.get("rebalanced_on")}}
        names = [str(n).upper() for n in longs + shorts]
        try:
            eight = [{"ticker": r["ticker"], "items": _EIGHT_K_ITEM.findall(str(r["summary"] or ""))} for r in await self.db.query(_VETO_8K_SQL, names) or []]
            divs = [{"ticker": r["ticker"], "ret": r["ret"], "z": r["z"]} for r in await self.db.query(_VETO_DIVERGENCE_SQL, names) or []]
            out["veto_counts"] = {"rules": sl.veto_counts(longs, shorts, eight, divs), "rebalanced_on": stored.get("rebalanced_on")}
        except Exception as exc:
            swallowed("agents.shadow_ledger.veto_inputs", exc, logger)
        return out

    async def run_once(self) -> Dict[str, Any]:
        stored = _loads(await self.raw.get(BOOK_KEY)) if self.raw is not None else None
        if not stored or not (stored.get("long") or stored.get("short")):
            return {"recorded": 0, "failed": 0, "why": "no stored book"}
        day = datetime.now(timezone.utc).date()
        recorded, failed = 0, 0
        payloads = await self._payloads(stored)
        failed += 2 - len(payloads)                                  # a series whose inputs could not be read is counted as failed
        for kind, payload in payloads.items():
            try:
                done = await self.db.query_one(_COUNT_SQL, day, kind)
                if done and int(done.get("n") or 0) >= 1:
                    continue
                await self.db.execute(_INSERT_SQL, day, kind, payload)
                recorded += 1
            except Exception as exc:
                failed += 1
                swallowed("agents.shadow_ledger.write", exc, logger, detail=kind)
        return {"recorded": recorded, "failed": failed, "day": day.isoformat()}


def _loads(raw: Any) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except ValueError:
        return None


__all__: List[str] = ["ShadowLedger"]
