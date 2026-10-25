"""
services/correlation/stock_news_scorer.py

FinBERT tone for every stock_news headline (shared/utils/stock_news.py),
newest first so live news is scored within minutes, then the history.

A strongly toned article about one or two stocks becomes an event: live ones
(published in the last LIVE_DAYS) go into `events`, where the signal engine
reads them and archives them; older ones go straight into the study's
archive. Same threshold either way, so the study measures what fires live.

Live headlines about one or two stocks are also embedded with the soft
correlator's sentence model (already loaded here) and summed per stock and
news day in Redis, for the news read trial M validated
(shared/utils/news_embed.py).

Runs here because this service already holds torch for its sentence model.
Three threads, so scoring the history cannot starve the correlation loop.
"""
from __future__ import annotations

import asyncio
import logging
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Optional, Sequence, Tuple

from shared.db import get_redis, get_timescale
from shared.utils import news_embed as ne
from shared.utils import news_embed_fit as nf
from shared.utils import stock_news as sn
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("correlation.stock_news")

MODEL = "ProsusAI/finbert"
THREADS = 3
BATCH = 64
LIVE_DAYS = 3
IDLE_SEC = 60
_UNSCORED_SQL = f"""
    SELECT id, created_at, symbols, headline FROM {sn.NEWS_TABLE}
    WHERE scored_at IS NULL ORDER BY created_at {{order}} LIMIT $1
"""
_SCORE_SQL = f"UPDATE {sn.NEWS_TABLE} SET positive = $2, negative = $3, neutral = $4, scored_at = NOW() WHERE id = $1"
_EVENT_SQL = """
    INSERT INTO events (event_id, type, source, occurred_at, collected_at, created_at, headline, tags, sentiment,
                        primary_entity_id, financial_data, anomaly_score)
    VALUES ($1::uuid, 'headline', $2, $3, NOW(), $4, $5, $6, $7, $8, $9::jsonb, $10)
    ON CONFLICT DO NOTHING
"""
# The same sums kept for good, for the news read's monthly refit.
_EMBED_GET_SQL = "SELECT n, v FROM news_embed_daily WHERE day = $1 AND ticker = $2"
_EMBED_PUT_SQL = """
    INSERT INTO news_embed_daily (day, ticker, n, v) VALUES ($1, $2, $3, $4)
    ON CONFLICT (day, ticker) DO UPDATE SET n = EXCLUDED.n, v = EXCLUDED.v
"""
_ARCHIVE_SQL = """
    INSERT INTO signal_event_archive (event_id, type, source, created_at, occurred_at, headline, summary, tags,
                                      sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'headline', $2, $3, $3, $4, NULL, $5, $6, $7, $8::jsonb, NULL)
    ON CONFLICT DO NOTHING
"""


class StockNewsScorer:
    """`newest_first` for the service (live news within minutes); a second
    worker can run oldest-first through the history, and the two meet in the
    middle (a row scored twice gets the same answer)."""

    def __init__(self, newest_first: bool = True, threads: int = THREADS,
                 embed: Optional[Callable[[List[str]], Optional[Sequence[Sequence[float]]]]] = None) -> None:
        self._tok: Any = None
        self._model: Any = None
        # Headlines -> unit vectors, or None while the sentence model loads.
        self._embed = embed
        self._sql = _UNSCORED_SQL.format(order="DESC" if newest_first else "ASC")
        self._threads = threads

    def _load(self) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        torch.set_num_threads(self._threads)
        self._tok = AutoTokenizer.from_pretrained(MODEL)
        self._model = AutoModelForSequenceClassification.from_pretrained(MODEL)
        self._model.eval()
        labels = {v.lower(): int(k) for k, v in self._model.config.id2label.items()}
        self._idx = (labels["positive"], labels["negative"], labels["neutral"])

    def score(self, headlines: List[str]) -> List[Tuple[float, float, float]]:
        """(positive, negative, neutral) per headline."""
        import torch
        if self._model is None:
            self._load()
        with torch.no_grad():
            x = self._tok(headlines, padding=True, truncation=True, max_length=64, return_tensors="pt")
            p = torch.softmax(self._model(**x).logits, dim=-1).tolist()
        ip, ineg, ineu = self._idx
        return [(round(r[ip], 4), round(r[ineg], 4), round(r[ineu], 4)) for r in p]

    async def run_once(self, db: Any) -> int:
        rows = await db.query(self._sql, BATCH) or []
        if not rows:
            return 0
        scores = await asyncio.to_thread(self.score, [str(r["headline"]) for r in rows])
        await db.execute_many(_SCORE_SQL, [(int(r["id"]), *sc) for r, sc in zip(rows, scores)])
        live_cut = datetime.now(timezone.utc) - timedelta(days=LIVE_DAYS)
        live, history = [], []
        for r, (pos, neg, _neu) in zip(rows, scores):
            ev = sn.event_row(r, pos, neg)
            if ev is None:
                continue
            at = r["created_at"]
            if isinstance(at, str):
                at = datetime.fromisoformat(at.replace("Z", "+00:00"))
            fd = {"ticker": ev["symbols"][0], "symbols": ev["symbols"], "tone": ev["tone"], "news_id": int(r["id"])}
            eid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"alpaca-news:{r['id']}"))
            if at >= live_cut:
                live.append((eid, sn.EVENT_SOURCE, at, datetime.now(timezone.utc), str(r["headline"]), ev["tags"],
                             ev["tone"], ev["symbols"][0], fd, min(1.0, abs(ev["tone"]))))
            else:
                history.append((eid, sn.EVENT_SOURCE, at, str(r["headline"]), ev["tags"], ev["tone"], ev["symbols"][0],
                                fd))
        if live:
            await db.execute_many(_EVENT_SQL, live)
        if history:
            await db.execute_many(_ARCHIVE_SQL, history)
        try:
            await self._embed_live(rows, live_cut)
        except Exception as e:
            swallowed("correlation.stock_news_scorer.embed", e, logger)
        return len(rows)

    async def _embed_live(self, rows: List[Any], live_cut: datetime) -> int:
        """Each live single-company headline's embedding, added to its stocks'
        sums for its news day."""
        if self._embed is None:
            return 0
        keep = []
        for r in rows:
            at = r["created_at"]
            if isinstance(at, str):
                at = datetime.fromisoformat(at.replace("Z", "+00:00"))
            syms = [str(s).upper() for s in (r["symbols"] or [])]
            if at >= live_cut and 1 <= len(syms) <= ne.MAX_SYMBOLS and r["headline"]:
                keep.append((at, syms, str(r["headline"])))
        if not keep:
            return 0
        vecs = await asyncio.to_thread(self._embed, [h for _a, _s, h in keep])
        if vecs is None:
            return 0
        raw = getattr(await get_redis(), "raw", None)
        batch: dict = {}
        for (at, syms, _h), vec in zip(keep, vecs):
            day = ne.news_day(at)
            key = ne.day_key(day)
            for sym in syms:
                if raw is not None:
                    cur = await raw.hget(key, sym)
                    total = ne.add(json.loads(cur) if cur else None, list(vec))
                    await raw.hset(key, sym, json.dumps(total))
                batch[(day, sym)] = ne.add(batch.get((day, sym)), list(vec))
            if raw is not None:
                await raw.expire(key, ne.DAY_TTL_SEC)
        db = await get_timescale()
        for (day, sym), add in batch.items():
            row = await db.query_one(_EMBED_GET_SQL, day, sym)
            if row:
                old = nf.unpack(bytes(row["v"])).tolist()
                add = {"n": int(row["n"]) + add["n"], "v": [a + b for a, b in zip(old, add["v"])]}
            await db.execute(_EMBED_PUT_SQL, day, sym, add["n"], nf.pack(add["v"]))
        return len(keep)

    async def run_forever(self) -> None:
        await asyncio.sleep(120)
        scored = 0
        while True:
            try:
                n = await self.run_once(await get_timescale())
                scored += n
                if n and scored % 10000 < BATCH:
                    logger.info("Stock news scored: %s so far", scored)
                if not n:
                    await asyncio.sleep(IDLE_SEC)
            except Exception as e:
                swallowed("correlation.stock_news_scorer", e, logger)
                await asyncio.sleep(IDLE_SEC)


__all__ = ["StockNewsScorer"]
