"""
services/agents/researcher.py

The weekly researcher (shared/utils/researcher.py holds the arithmetic). Each
Saturday it re-screens every stored stock feature and every combination tried
so far on the newest data, asks the judgment model for up to three new
combinations in a family chosen by Thompson sampling, screens those, and
writes:

  sentinel:research:board    every candidate's IC, incremental IC, q, verdict
  sentinel:research:queue    the candidates that cleared t >= 3 and q <= 0.10,
                             for a pre-registered trial (never adopted here)
  sentinel:research:bandit   each family's wins and losses
  sentinel:research:tests    how many tests have ever been run (BH counts them)

The universe is today's ranked stocks (the monthly factor scores), which
survivorship flatters a little; a screen, not a verdict, and the trial that
follows uses the model's own point-in-time universe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np

from shared.utils import researcher as rs
from shared.utils import stock_features as sf
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.researcher")

BOARD_KEY = "sentinel:research:board"
QUEUE_KEY = "sentinel:research:queue"
BANDIT_KEY = "sentinel:research:bandit"
TESTS_KEY = "sentinel:research:tests"
DONE_KEY = "sentinel:research:done_week"
RUN_WEEKDAY, RUN_HOUR_UTC = 5, 6                # Saturday morning, markets shut
START = date(2016, 1, 1)
MODEL = os.getenv("JUDGMENT_MODEL") or os.getenv("AGENT_MODEL", "qwen2.5:3b")
MIN_PRICE = 5.0

_MONTH_ENDS_SQL = """
    SELECT max(day) AS d FROM daily_bars WHERE ticker = 'SPY' AND day >= $1
    GROUP BY date_trunc('month', day) ORDER BY 1
"""
_STORED_UNIVERSE_SQL = """
    SELECT ticker FROM factor_scores_daily WHERE horizon = 'monthly'
      AND as_of = (SELECT max(as_of) FROM factor_scores_daily WHERE horizon = 'monthly') ORDER BY 1
"""
_CLOSES_SQL = "SELECT ticker, day, close FROM daily_bars WHERE day = ANY($1::date[]) AND ticker = ANY($2::text[])"
# The latest value inside each feature's validity window, at each month end.
_FEATURES_SQL = """
    WITH me AS (SELECT unnest($1::date[]) AS d),
         fs AS (SELECT * FROM unnest($2::text[], $3::int[]) AS t(feature, ttl))
    SELECT DISTINCT ON (f.feature, f.ticker, me.d) f.feature, f.ticker, me.d AS month_end, f.value
    FROM me CROSS JOIN fs
    JOIN stock_features_daily f ON f.feature = fs.feature AND f.day <= me.d AND f.day > me.d - fs.ttl
    WHERE f.ticker = ANY($4::text[])
    ORDER BY f.feature, f.ticker, me.d, f.day DESC
"""


def _decode(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


class Researcher:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(300)
        while True:
            try:
                now = datetime.now(timezone.utc)
                week = now.strftime("%G-W%V")
                if (now.weekday() == RUN_WEEKDAY and now.hour >= RUN_HOUR_UTC
                        and _decode(await self.raw.get(DONE_KEY)) != week):
                    out = await self.run_once()
                    await self.raw.set(DONE_KEY, week)
                    logger.info("Research week %s: %s", week, out)
            except Exception as e:
                swallowed("agents.researcher", e, logger)
            await asyncio.sleep(1800)

    async def _panels(self) -> Optional[Tuple[List[date], Dict[str, np.ndarray], np.ndarray, List[np.ndarray]]]:
        """Month ends, each feature's panel, the next month's return, and the
        two controls (12-1 momentum, 1-month reversal); dates x tickers."""
        from shared.utils.signal_store import factor_scores_key

        scores = json.loads(_decode(await self.raw.get(factor_scores_key("monthly"))) or "{}")
        tickers = sorted(scores) or [str(r["ticker"]) for r in await self.db.query(_STORED_UNIVERSE_SQL) or []]
        if len(tickers) < rs.MIN_NAMES:
            return None
        months = [r["d"] for r in await self.db.query(_MONTH_ENDS_SQL, START) or []]
        if len(months) < rs.MIN_MONTHS + 13:
            return None
        di, ti = {d: i for i, d in enumerate(months)}, {t: j for j, t in enumerate(tickers)}
        px = np.full((len(months), len(tickers)), np.nan)
        for r in await self.db.query(_CLOSES_SQL, months, tickers) or []:
            px[di[r["day"]], ti[r["ticker"]]] = float(r["close"])
        px[px < MIN_PRICE] = np.nan
        with np.errstate(invalid="ignore", divide="ignore"):
            ret = px[1:] / px[:-1] - 1
            fwd = np.vstack([ret, np.full((1, len(tickers)), np.nan)])           # month t -> t+1
            rev = np.vstack([np.full((1, len(tickers)), np.nan), ret])           # month t-1 -> t
            mom = np.full(px.shape, np.nan)
            mom[12:] = px[11:-1] / px[:-12] - 1                                  # t-12 -> t-1
        names = sorted(sf.REGISTRY)
        feats = {n: np.full(px.shape, np.nan) for n in names}
        rows = await self.db.query(_FEATURES_SQL, months, names, [sf.REGISTRY[n].ttl_days for n in names], tickers)
        for r in rows or []:
            feats[r["feature"]][di[r["month_end"]], ti[r["ticker"]]] = float(r["value"])
        # An 8-K flag outside its window means no filing, not an unknown.
        priced = np.isfinite(px)
        for n in names:
            if n.startswith("ev_8k_"):
                feats[n] = np.where(priced & ~np.isfinite(feats[n]), 0.0, feats[n])
        return months, feats, fwd, [mom, rev]

    def _screen(self, op: str, a: str, b: Optional[str], feats: Dict[str, np.ndarray], fwd: np.ndarray,
                controls: List[np.ndarray]) -> Dict[str, object]:
        sig = rs.build(op, feats[a], feats[b] if b else None)
        row: Dict[str, object] = {"candidate": rs.key(op, a, b), "op": op, "a": a, "b": b,
                                  "family": sf.REGISTRY[a].source}
        row.update(rs.summarize(rs.ic_series(sig, fwd), rs.ic_series(sig, fwd, controls)))
        return row

    async def _propose(self, family: str, board: List[Dict[str, object]]) -> List[Tuple[str, str, str]]:
        from shared.utils.ollama import OllamaClient

        system, user = rs.prompt(family, {k: v.description for k, v in sf.REGISTRY.items()}, board)
        async with aiohttp.ClientSession() as session:
            llm = OllamaClient(session, model=MODEL, redis_client=self.redis, service_name="researcher")
            try:
                got = await llm.infer(system, user, schema=rs.Ideas, temperature=0.7, model=MODEL, num_predict=500)
            except Exception as e:
                swallowed("agents.researcher.propose", e, logger)
                return []
        ideas = got.model_dump().get("ideas") or []
        return [v for v in (rs.valid(i, list(sf.REGISTRY)) for i in ideas) if v]

    async def run_once(self) -> Dict[str, object]:
        got = await self._panels()
        if got is None:
            return {"skipped": "not enough data"}
        _months, feats, fwd, controls = got
        board: Dict[str, Dict[str, object]] = {}
        for k, v in (await self.raw.hgetall(BOARD_KEY) or {}).items():
            try:
                board[_decode(k)] = json.loads(_decode(v))
            except ValueError:
                continue
        # Everything already on the board, plus every stored feature, re-screened on the newest data.
        todo = {rs.key("raw", n): ("raw", n, None) for n in sf.REGISTRY}
        for r in board.values():
            if r.get("op") in rs.OPS and r.get("a") in sf.REGISTRY and (r.get("b") in sf.REGISTRY or r.get("op") == "raw"):
                todo[str(r["candidate"])] = (str(r["op"]), str(r["a"]), r.get("b"))
        tests = int(_decode(await self.raw.get(TESTS_KEY)) or 0)
        new_board = {}
        for cand, (op, a, b) in todo.items():
            new_board[cand] = await asyncio.to_thread(self._screen, op, a, b, feats, fwd, controls)
            tests += cand not in board

        families = sorted({f.source for f in sf.REGISTRY.values()})
        record = {}
        for k, v in (await self.raw.hgetall(BANDIT_KEY) or {}).items():
            w, _, l = _decode(v).partition(",")
            record[_decode(k)] = (int(w or 0), int(l or 0))
        family = rs.pick_family(record, families, random.Random())
        ranked = sorted(new_board.values(), key=lambda r: -abs(float(r.get("inc_t") or 0)))
        proposals = [p for p in await self._propose(family, ranked) if rs.key(*p) not in new_board]
        paid = False
        for op, a, b in proposals:
            row = await asyncio.to_thread(self._screen, op, a, b, feats, fwd, controls)
            row["proposed"] = datetime.now(timezone.utc).date().isoformat()
            new_board[str(row["candidate"])] = row
            tests += 1
            paid |= abs(float(row.get("inc_t") or 0)) >= rs.BANDIT_T
        if proposals:
            w, l = record.get(family, (0, 0))
            await self.raw.hset(BANDIT_KEY, family, f"{w + int(paid)},{l + int(not paid)}")

        rs.judge(new_board, tests)
        await self.raw.set(TESTS_KEY, tests)
        await self.raw.delete(BOARD_KEY)
        await self.raw.hset(BOARD_KEY, mapping={k: json.dumps(v) for k, v in new_board.items()})
        queue = [r for r in new_board.values() if r.get("verdict") == "queue for a trial"]
        await self.raw.set(QUEUE_KEY, json.dumps(sorted(queue, key=lambda r: -abs(float(r["inc_t"])))))
        return {"screened": len(new_board), "proposed": len(proposals), "family": family, "paid": paid,
                "queued": [r["candidate"] for r in queue], "tests_ever": tests}


__all__ = ["BOARD_KEY", "QUEUE_KEY", "Researcher"]
