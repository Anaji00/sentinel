"""
services/agents/llm_news_job.py

Scores new single-stock headlines with a large hosted model (shared/utils/
llm_news.py), stores each answer in stock_news_llm with the model and its
training cutoff, and once a day writes each stock's trailing 30-day mean
(llm_news_30) into stock_features_daily for the researcher and the model's trials.

Forward only: headlines from the last LOOKBACK_HOURS, never a backfill, because
a model may remember what prices did before its cutoff. Hosted only: the local
models are BERT-to-7B sized, the tier that showed no skill, and the CPU is the
platform's scarcest resource. With no hosted key the job does nothing.

Paced: BATCH headlines a request, at most MAX_REQUESTS_PER_PASS a pass, every
INTERVAL_SEC; the hosted router's own quota buckets bound the day.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp

from shared.utils import hosted_llm
from shared.utils import llm_news as ln
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.llm_news")

INTERVAL_SEC = 15 * 60
LOOKBACK_HOURS = 48
MAX_REQUESTS_PER_PASS = 8
FEATURE_DAY_KEY = "sentinel:llm_news:feature_day"
SOURCE = "llm_news"
MODEL = os.getenv("NEWS_LLM_MODEL", "gemini/gemini-3.5-flash-lite")

_UNSCORED_SQL = f"""
    SELECT n.id, n.symbols[1] AS ticker, n.created_at, n.headline FROM stock_news n
    WHERE n.created_at > NOW() - INTERVAL '{LOOKBACK_HOURS} hours' AND cardinality(n.symbols) = 1
      AND n.symbols[1] ~ '^[A-Z]{{1,5}}$'
      AND NOT EXISTS (SELECT 1 FROM stock_news_llm s WHERE s.news_id = n.id AND s.model = $1)
    ORDER BY n.created_at DESC LIMIT $2
"""
_INSERT_SQL = """
    INSERT INTO stock_news_llm (news_id, ticker, created_at, score, model, cutoff)
    VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (news_id, model) DO NOTHING
"""
_WINDOW_SQL = f"""
    SELECT ticker, created_at, score FROM stock_news_llm
    WHERE model = $1 AND created_at > NOW() - INTERVAL '{ln.WINDOW_DAYS + 1} days'
"""
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


def _ts(v: Any) -> datetime:
    """The pool returns timestamps as ISO strings; asyncpg wants a datetime back."""
    return v if isinstance(v, datetime) else datetime.fromisoformat(str(v).replace("Z", "+00:00"))


class LlmNewsJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)
        self.cutoff = ln.cutoff_of(MODEL, ln.parse_cutoffs(os.getenv("NEWS_LLM_CUTOFFS", "")))

    async def run(self) -> None:
        if not hosted_llm.is_hosted(MODEL):
            logger.info("LLM news scoring off: NEWS_LLM_MODEL must name a hosted model, got %r", MODEL)
            return
        await asyncio.sleep(300)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient

            llm = OllamaClient(session, model=MODEL, redis_client=self.redis, service_name="llm_news")
            while True:
                try:
                    out = await self.run_once(llm)
                    if out.get("scored") or out.get("feature_rows"):
                        logger.info("LLM news: %s", out)
                except Exception as e:
                    swallowed("agents.llm_news", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self, llm: Any) -> Dict[str, int]:
        scored = requests = 0
        rows = await self.db.query(_UNSCORED_SQL, MODEL, ln.BATCH * MAX_REQUESTS_PER_PASS) or []
        for k in range(0, len(rows), ln.BATCH):
            batch = rows[k:k + ln.BATCH]
            try:
                answer = await llm.infer(ln.SYSTEM, ln.prompt([(str(r["ticker"]), str(r["headline"])) for r in batch]),
                                         ln.Scores, temperature=0.0, model=MODEL, max_retries=1,
                                         # gpt-oss counts its reasoning against this: 400 was
                                         # used up before twenty scores could close.
                                         num_predict=1500)
            except Exception as e:
                # Quota, 429 or an outage: stop this pass; the next one resumes.
                swallowed("agents.llm_news.batch", e, logger)
                break
            requests += 1
            for i, s in ln.parse(answer, len(batch)).items():
                r = batch[i]
                await self.db.execute(_INSERT_SQL, int(r["id"]), str(r["ticker"]).upper(), _ts(r["created_at"]), s,
                                      MODEL, self.cutoff)
                scored += 1
        out = {"scored": scored, "requests": requests}
        today = datetime.now(timezone.utc).date()
        if self.raw is not None and _s(await self.raw.get(FEATURE_DAY_KEY)) != today.isoformat():
            out["feature_rows"] = await self.write_features(today)
            await self.raw.set(FEATURE_DAY_KEY, today.isoformat())
        return out

    async def write_features(self, today: Any) -> int:
        window = [(str(r["ticker"]), _ts(r["created_at"]), int(r["score"]))
                  for r in await self.db.query(_WINDOW_SQL, MODEL) or []]
        rows: List[Any] = ln.feature_rows(window, today)
        for t, d, f, v in rows:
            await self.db.execute(_UPSERT_FEATURE, t, d, f, v, SOURCE)
        return len(rows)


__all__ = ["LlmNewsJob"]
