"""
services/agents/extract_audit_job.py

Weekly: a second model (AUDIT_MODEL, Gemini, so not the headline scorer
itself) re-reads a sample of each extractor's evidence and the agreement is
written to REPORT_KEY (shared/utils/extract_audit.py): headline reads,
13D intent, and 8-K guidance. Three to five calls a week.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import aiohttp

from services.agents.activist_13d_job import LOG_KEY as ACTIVIST_LOG_KEY
from shared.utils import extract_audit as ea
from shared.utils import filing_extract as fx
from shared.utils import llm_news as ln
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.extract_audit")

AUDIT_MODEL = os.getenv("EXTRACT_AUDIT_MODEL", "gemini/gemini-3.5-flash-lite")
NEWS_MODEL = os.getenv("NEWS_LLM_MODEL", "")
SAMPLE = 20
REPORT_KEY = "sentinel:extract_audit:report"
WEEK_KEY = "sentinel:extract_audit:week"
INTERVAL_SEC = 6 * 3600
_NEWS_SQL = """
    SELECT n.headline, n.symbols[1] AS ticker, s.score FROM stock_news_llm s JOIN stock_news n ON n.id = s.news_id
    WHERE s.model = $1 AND s.scored_at > NOW() - INTERVAL '7 days' ORDER BY random() LIMIT $2
"""
_GUIDANCE_SQL = """
    SELECT summary, tags FROM events WHERE type = 'filing' AND source = $1 AND created_at > NOW() - INTERVAL '30 days'
      AND 'filing_extract' = ANY(tags) AND summary NOT LIKE 'No forward guidance%' AND summary NOT LIKE 'Size against%'
    ORDER BY random() LIMIT $2
"""


class ExtractAuditJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.redis = redis
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(1800)
        async with aiohttp.ClientSession() as session:
            from shared.utils.ollama import OllamaClient

            llm = OllamaClient(session, model=AUDIT_MODEL, redis_client=self.redis, service_name="extract_audit")
            while True:
                week = datetime.now(timezone.utc).strftime("%G-W%V")
                try:
                    done = await self.raw.get(WEEK_KEY)
                    if (done.decode() if isinstance(done, bytes) else done) != week:
                        rep = await self.run_once(llm)
                        logger.info("Extraction audit: %s", {k: v for k, v in rep.items() if k != "examples"})
                        await self.raw.set(REPORT_KEY, json.dumps(rep, default=str))
                        await self.raw.set(WEEK_KEY, week)
                except Exception as e:
                    swallowed("agents.extract_audit.pass", e, logger)
                await asyncio.sleep(INTERVAL_SEC)

    async def _judge(self, llm: Any, system: str, texts: List[str]) -> Dict[int, str]:
        if not texts:
            return {}
        try:
            got = await llm.infer(system, ea.numbered(texts), ea.Verdicts, temperature=0.0, model=AUDIT_MODEL,
                                  max_retries=1, num_predict=300 + 20 * len(texts))
        except Exception as e:
            swallowed("agents.extract_audit.judge", e, logger)
            return {}
        return ea.labels(got, len(texts))

    async def run_once(self, llm: Any) -> Dict[str, Any]:
        rep: Dict[str, Any] = {"generated_at": datetime.now(timezone.utc).isoformat(), "auditor": AUDIT_MODEL,
                               "examples": {}}
        # Headline reads: the auditor scores the same headlines with the scorer's own prompt.
        rows = await self.db.query(_NEWS_SQL, NEWS_MODEL, SAMPLE) if NEWS_MODEL else []
        if rows:
            try:
                got = await llm.infer(ln.SYSTEM, ln.prompt([(str(r["ticker"]), str(r["headline"])) for r in rows]),
                                      ln.Scores, temperature=0.0, model=AUDIT_MODEL, max_retries=1, num_predict=1500)
                theirs = ln.parse(got, len(rows))
            except Exception as e:
                swallowed("agents.extract_audit.news", e, logger)
                theirs = {}
            pairs = [(str(int(rows[i]["score"])), str(s)) for i, s in theirs.items()]
            rep["headline_read"] = ea.agreement(pairs)
            rep["examples"]["headline_read"] = [(rows[i]["headline"][:120], int(rows[i]["score"]), s)
                                                for i, s in theirs.items() if int(rows[i]["score"]) != s][:5]
        # 13D intent, judged from the quote each reading kept.
        log = [json.loads(x) for x in (await self.raw.lrange(ACTIVIST_LOG_KEY, 0, 199) or [])]
        log = [x for x in log if x.get("quote")][:SAMPLE]
        verdicts = await self._judge(llm, ea.SYSTEM_13D, [str(x["quote"]) for x in log])
        pairs: List[Tuple[str, str]] = [("activist" if log[i]["activist"] else "investment", v) for i, v in verdicts.items()]
        rep["activist_13d"] = ea.agreement(pairs)
        rep["examples"]["activist_13d"] = [(log[i]["ticker"], log[i]["quote"][:120], v)
                                           for i, v in verdicts.items() if pairs and v != (
                                               "activist" if log[i]["activist"] else "investment")][:5]
        # 8-K guidance, judged from the quoted sentence.
        g = await self.db.query(_GUIDANCE_SQL, fx.EXTRACT_SOURCE, SAMPLE) or []
        direction = [next((t[len("guidance_"):] for t in (r["tags"] or []) if str(t).startswith("guidance_")), "none")
                     for r in g]
        verdicts = await self._judge(llm, ea.SYSTEM_GUIDANCE, [str(r["summary"]) for r in g])
        rep["guidance"] = ea.agreement([(direction[i], v) for i, v in verdicts.items()])
        rep["examples"]["guidance"] = [(str(g[i]["summary"])[:120], direction[i], v)
                                       for i, v in verdicts.items() if direction[i] != v][:5]
        return rep


__all__ = ["ExtractAuditJob"]
