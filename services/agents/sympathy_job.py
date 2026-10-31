"""
services/agents/sympathy_job.py

Daily, for every stock the factor model ranks: measured sympathy peers
(shared/utils/sympathy.py) into sympathy_peers, and measured macro exposures
(shared/utils/macro_exposure.py, from macro_daily_bars) into macro_exposures.
Both are computed in a worker thread, and each table is replaced in one
statement, so a reader never sees a half-written set; a failed pass leaves
yesterday's rows in place.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from shared.utils import macro_exposure as mx
from shared.utils import market_cap as mc
from shared.utils import sympathy as sy
from shared.utils import text_peers as tp
from shared.utils.asset_universe import load_asset_meta
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.sympathy")

INTERVAL_SEC = 3600
RUN_HOUR_UTC = 23                         # after the nightly model and the day's bars
DAY_KEY = "sentinel:sympathy:day"

_UNIVERSE_SQL = """
    SELECT DISTINCT ticker FROM factor_scores_daily
    WHERE horizon = 'monthly' AND as_of = (SELECT max(as_of) FROM factor_scores_daily WHERE horizon = 'monthly')
"""
_BARS_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day"
# One statement: upsert today's set, then delete every pair it did not touch.
# (A DELETE CTE beside a plain INSERT does not see the insert, and the pairs
# that survive from yesterday collide with their own old rows.)
_REPLACE_SQL = """
    WITH ins AS (
        INSERT INTO sympathy_peers (ticker, peer, corr, n, as_of)
        SELECT * FROM unnest($1::text[], $2::text[], $3::float8[], $4::int[], $5::date[])
        ON CONFLICT (ticker, peer) DO UPDATE SET corr = EXCLUDED.corr, n = EXCLUDED.n, as_of = EXCLUDED.as_of
        RETURNING ticker, peer
    )
    DELETE FROM sympathy_peers s
    WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.ticker = s.ticker AND ins.peer = s.peer)
"""
_MACRO_SQL = "SELECT symbol, day, close FROM macro_daily_bars WHERE day >= $1 ORDER BY symbol, day"
SOURCE = "sympathy"
_REPLACE_TEXT_PEERS_SQL = """
    WITH ins AS (
        INSERT INTO text_peers (ticker, peer, sim, as_of)
        SELECT * FROM unnest($1::text[], $2::text[], $3::float8[], $4::date[])
        ON CONFLICT (ticker, peer) DO UPDATE SET sim = EXCLUDED.sim, as_of = EXCLUDED.as_of
        RETURNING ticker, peer
    )
    DELETE FROM text_peers s WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.ticker = s.ticker AND ins.peer = s.peer)
"""
_UPSERT_FEATURES_SQL = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source)
    SELECT t, d, $3, v, $5 FROM unnest($1::text[], $2::date[], $4::float8[]) AS x(t, d, v)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""
_REPLACE_EXPOSURES_SQL = """
    WITH ins AS (
        INSERT INTO macro_exposures (ticker, macro, beta, t, partial_corr, n, as_of)
        SELECT * FROM unnest($1::text[], $2::text[], $3::float8[], $4::float8[], $5::float8[], $6::int[], $7::date[])
        ON CONFLICT (ticker, macro) DO UPDATE SET beta = EXCLUDED.beta, t = EXCLUDED.t,
            partial_corr = EXCLUDED.partial_corr, n = EXCLUDED.n, as_of = EXCLUDED.as_of
        RETURNING ticker, macro
    )
    DELETE FROM macro_exposures s
    WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.ticker = s.ticker AND ins.macro = s.macro)
"""


class SympathyJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(300)
        while True:
            now = datetime.now(timezone.utc)
            today = now.date().isoformat()
            try:
                done = await self.raw.get(DAY_KEY) if self.raw is not None else None
                done = done.decode() if isinstance(done, bytes) else done
                # Nightly, and immediately on a first deploy (nothing stored yet).
                if done is None or (now.hour >= RUN_HOUR_UTC and done != today):
                    logger.info("Sympathy peers: %s", await self.run_once())
                    if self.raw is not None:
                        await self.raw.set(DAY_KEY, today, ex=3 * 86400)
            except Exception as e:
                swallowed("agents.sympathy.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, int]:
        # Stocks only, by the venue's asset classification: a fund is not a
        # sympathy mover, and with funds in NVDA's peers were five growth ETFs.
        meta = await load_asset_meta(self.raw) if self.raw is not None else {}
        stocks = {t.upper() for t, m in (meta or {}).items() if m and m[0] == "stock"}
        if not stocks:
            # No classification is not "every symbol is a stock": keep the stored set.
            return {"stocks": 0, "rows": 0, "skipped": 1}
        # And companies of $1bn and up: peers and exposures feed calls, which
        # are kept to that size (market_cap.large_caps); no table, no rebuild.
        large = await mc.large_caps(self.raw)
        if not large:
            return {"stocks": 0, "rows": 0, "skipped": 1}
        tickers = sorted({str(r["ticker"]).upper() for r in await self.db.query(_UNIVERSE_SQL) or []} & stocks & large)
        if len(tickers) < 50:
            # Missing ranks are not "no peers": keep the stored set.
            return {"stocks": len(tickers), "rows": 0, "skipped": 1}
        since = date.today() - timedelta(days=int(max(sy.WINDOW, mx.WINDOW) * 1.6) + 10)
        closes: Dict[str, List[Tuple[date, float]]] = defaultdict(list)
        for r in await self.db.query(_BARS_SQL, tickers + ["SPY"], since) or []:
            closes[str(r["ticker"]).upper()].append((r["day"], float(r["close"])))
        spy = closes.pop("SPY", [])
        rows = await asyncio.to_thread(lambda: sy.top_peers(sy.excess_returns(closes, spy)))
        if not rows:
            return {"stocks": len(tickers), "rows": 0, "skipped": 1}
        a, b, c, n = (list(x) for x in zip(*rows))
        await self.db.execute(_REPLACE_SQL, a, b, c, n, [date.today()] * len(rows))
        out = {"stocks": len(tickers), "rows": len(rows), "with_peers": len(set(a))}
        try:
            out["exposures"] = await self._exposures(closes, spy, since)
        except Exception as e:
            swallowed("agents.sympathy.exposures", e, logger)
        try:
            out.update(await self._text_peers(closes))
        except Exception as e:
            swallowed("agents.sympathy.text_peers", e, logger)
        return out

    async def _text_peers(self, closes: Dict[str, List[Tuple[date, float]]]) -> Dict[str, int]:
        """Text-based peers from 10-K business descriptions into text_peers, and
        each stock's 21-session momentum through them as a feature for today."""
        stored = await self.raw.hgetall(tp.TERMS_KEY) or {}
        docs = {}
        for k, v in stored.items():
            try:
                docs[(k.decode() if isinstance(k, bytes) else str(k)).upper()] = json.loads(v)["terms"]
            except (ValueError, KeyError, TypeError):
                continue
        if len(docs) < 50:
            return {"text_docs": len(docs)}             # too few to say what is common
        rows = await asyncio.to_thread(tp.peers, docs)
        if not rows:
            return {"text_docs": len(docs), "text_peers": 0}
        a, b, c = (list(x) for x in zip(*rows))
        await self.db.execute(_REPLACE_TEXT_PEERS_SQL, a, b, c, [date.today()] * len(rows))
        r21 = {}
        for t, v in closes.items():
            if len(v) > 21 and v[-22][1] > 0 and v[-1][1] > 0:
                r21[t] = math.log(v[-1][1] / v[-22][1])
        mom = tp.peer_momentum(rows, r21)
        if mom:
            await self.db.execute(_UPSERT_FEATURES_SQL, list(mom), [date.today()] * len(mom), tp.FEATURE,
                                  [float(x) for x in mom.values()], SOURCE)
        return {"text_docs": len(docs), "text_peers": len(rows), "text_peer_mom": len(mom)}

    async def _exposures(self, closes: Dict[str, List[Tuple[date, float]]], spy: List[Tuple[date, float]],
                         since: date) -> int:
        """Measured macro exposures into macro_exposures; the rows written."""
        macro: Dict[str, List[Tuple[date, float]]] = defaultdict(list)
        for r in await self.db.query(_MACRO_SQL, since) or []:
            macro[str(r["symbol"])].append((r["day"], float(r["close"])))
        # US02Y and US2Y are one series; kept under US2Y, the spelling live quotes use.
        macro.pop("US02Y", None)
        if not macro:
            return 0

        def compute() -> List[mx.ExposureRow]:
            return mx.exposures({t: mx.returns(v) for t, v in closes.items()}, mx.returns(spy),
                                {s: mx.changes(s, v) for s, v in macro.items()})

        rows = await asyncio.to_thread(compute)
        if not rows:
            return 0
        cols = [list(x) for x in zip(*rows)]
        await self.db.execute(_REPLACE_EXPOSURES_SQL, *cols, [date.today()] * len(rows))
        return len(rows)


__all__ = ["SympathyJob"]
