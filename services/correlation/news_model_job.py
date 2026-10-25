"""
services/correlation/news_model_job.py

Weekly: the news-embedding model's walk-forward (shared/utils/news_model.py)
on the archived headlines of the last 90 days and the vectors the soft
correlator already stored for them. Live scores are published only once the
walk-forward clears its gate.

Samples: each archived headline, for each instrument it names (exposes: tags)
or maps to (sector, country, commodity and zone instruments), with the
instrument's return net of SPY from the first close after the headline was
known to the close after that.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import numpy as np

from shared.db import get_timescale
from shared.utils import news_model as nm
from shared.utils.headline_mapping import map_headline
from shared.utils.quiet_failures import swallowed
from shared.utils.vector_index import EVENT_COLLECTION

logger = logging.getLogger("correlation.news_model")

WINDOW_DAYS = 90
EVERY_DAYS = 7
MAX_SAMPLES = 60_000
RETURN_CLIP = 0.2
NY = ZoneInfo("America/New_York")
_HEADLINES_SQL = f"""
    SELECT event_id, created_at, headline, tags FROM signal_event_archive
    WHERE type = 'headline' AND created_at > NOW() - INTERVAL '{WINDOW_DAYS} days' AND headline IS NOT NULL
"""
_CLOSES_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date ORDER BY ticker, day
"""


def instruments_of(headline: str, tags: Optional[List[str]]) -> Set[str]:
    """What a headline names or maps to, stocks and ETFs only."""
    out = {a.symbol for a in map_headline(headline or "")}
    out |= {t[len("exposes:"):].upper() for t in tags or [] if str(t).startswith("exposes:")}
    return {s for s in out if s and "=" not in s and "-" not in s and "/" not in s and len(s) <= 6}


def _as_dt(x: Any) -> datetime:
    """shared.db hands timestamps back as text."""
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    d = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _as_date(x: Any) -> date:
    return x if isinstance(x, date) and not isinstance(x, datetime) else date.fromisoformat(str(x)[:10])


def _session_close(d: date) -> datetime:
    return datetime.combine(d, time(16, 0), tzinfo=NY).astimezone(timezone.utc)


def next_session_return(days: List[date], closes: List[float], spy: Dict[date, float],
                        known_at: datetime) -> Optional[Tuple[date, float]]:
    """(entry session, return net of SPY) from the first close after `known_at`
    to the next close; None without both bars."""
    closes_at = [_session_close(d) for d in days]
    i = bisect.bisect_right(closes_at, known_at)
    if i + 1 >= len(days):
        return None
    d0, d1 = days[i], days[i + 1]
    s0, s1 = spy.get(d0), spy.get(d1)
    if not s0 or not s1 or not closes[i] or not closes[i + 1]:
        return None
    r = closes[i + 1] / closes[i] - 1.0 - (s1 / s0 - 1.0)
    return d0, float(max(-RETURN_CLIP, min(RETURN_CLIP, r)))


async def _vectors(client: Any, ids: List[str]) -> Dict[str, np.ndarray]:
    out: Dict[str, np.ndarray] = {}
    for i in range(0, len(ids), 256):
        points = await client.retrieve(collection_name=EVENT_COLLECTION, ids=ids[i:i + 256], with_vectors=True,
                                       with_payload=False)
        for p in points:
            if p.vector is not None:
                v = p.vector if not isinstance(p.vector, dict) else next(iter(p.vector.values()))
                out[str(p.id)] = np.asarray(v, dtype=np.float32)
    return out


async def build_samples(soft_correlator: Any) -> Tuple[np.ndarray, np.ndarray, np.ndarray, Dict[str, Any]]:
    db = await get_timescale()
    rows = await db.query(_HEADLINES_SQL) or []
    wanted: Dict[str, Tuple[datetime, Set[str]]] = {}
    for r in rows:
        inst = instruments_of(r["headline"], r["tags"])
        if inst:
            wanted[str(r["event_id"])] = (_as_dt(r["created_at"]), inst)
    vecs = await _vectors(soft_correlator._client, sorted(wanted))
    names = sorted({s for _t, inst in wanted.values() for s in inst} | {"SPY"})
    series: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
    start = datetime.now(timezone.utc).date() - timedelta(days=WINDOW_DAYS + 10)
    for r in await db.query(_CLOSES_SQL, names, start) or []:
        d, c = series[str(r["ticker"]).upper()]
        d.append(_as_date(r["day"]))
        c.append(float(r["close"]))
    spy = dict(zip(*series.get("SPY", ([], []))))
    X: List[np.ndarray] = []
    y: List[float] = []
    day: List[int] = []
    for eid, (at, inst) in wanted.items():
        v = vecs.get(eid)
        if v is None:
            continue
        for s in inst:
            if s not in series:
                continue
            got = next_session_return(series[s][0], series[s][1], spy, at)
            if got:
                X.append(v)
                y.append(got[1])
                day.append(got[0].toordinal())
        if len(y) >= MAX_SAMPLES:
            break
    meta = {"headlines": len(rows), "with_instruments": len(wanted), "with_vectors": len(vecs), "samples": len(y),
            "instruments": len({s for _t, inst in wanted.values() for s in inst})}
    Xm = np.vstack(X) if X else np.empty((0, 768), dtype=np.float32)
    return Xm, np.array(y, dtype=np.float32), np.array(day), meta


async def run_once(soft_correlator: Any, redis_client: Any) -> Dict[str, Any]:
    X, y, day, meta = await build_samples(soft_correlator)
    report: Dict[str, Any] = {**meta, "generated_at": datetime.now(timezone.utc).isoformat(), "window_days": WINDOW_DAYS}
    if len(y) >= 500:
        report.update(await asyncio.to_thread(nm.walk_forward, day, X, y))
        if report.get("validated"):
            report["weights"] = [round(float(w), 7) for w in await asyncio.to_thread(nm.ridge, X, y)]
    else:
        report.update(days=0, mean_ic=None, t=None, validated=False, note="fewer than 500 samples")
    await getattr(redis_client, "raw", redis_client).set(nm.NEWS_MODEL_KEY, json.dumps(report))
    logger.info("News model: %s samples, %s test days, IC %s (t %s), validated %s", report.get("samples"),
                report.get("days"), report.get("mean_ic"), report.get("t"), report.get("validated"))
    return report


async def run_forever(soft_correlator: Any, redis_client: Any) -> None:
    await asyncio.sleep(1800)
    raw = getattr(redis_client, "raw", redis_client)
    while True:
        try:
            prior = await raw.get(nm.NEWS_MODEL_KEY)
            age = None
            if prior:
                ran = json.loads(prior).get("generated_at")
                age = datetime.now(timezone.utc) - datetime.fromisoformat(ran) if ran else None
            if soft_correlator._client is not None and (age is None or age > timedelta(days=EVERY_DAYS)):
                await run_once(soft_correlator, redis_client)
        except Exception as e:
            swallowed("correlation.news_model", e, logger)
        await asyncio.sleep(6 * 3600)


_WEIGHTS_CACHE: Dict[str, Any] = {"at": 0.0, "w": None}


async def score_live(redis_client: Any, headline: str, tags: Optional[List[str]], embedding: List[float]) -> int:
    """Score a live headline's instruments with the validated weights; 0 when
    the model has not passed its gate. Returns the instruments scored."""
    import time as _time
    raw = getattr(redis_client, "raw", redis_client)
    if _time.time() - _WEIGHTS_CACHE["at"] > 3600:
        w = None
        blob = await raw.get(nm.NEWS_MODEL_KEY)
        if blob:
            rep = json.loads(blob)
            if rep.get("validated") and rep.get("weights"):
                w = np.array(rep["weights"], dtype=np.float32)
        _WEIGHTS_CACHE.update(at=_time.time(), w=w)
    w = _WEIGHTS_CACHE["w"]
    inst = instruments_of(headline, tags)
    if w is None or not inst or len(embedding) + 1 != len(w):
        return 0
    score = float(nm.predict(w, np.array([embedding], dtype=np.float32))[0])
    now = datetime.now(timezone.utc).isoformat()
    for s in inst:
        await raw.hset(nm.NEWS_SCORES_KEY, s, json.dumps({"score_bps": round(score * 1e4, 1), "headline": headline[:200],
                                                           "at": now}))
    await raw.expire(nm.NEWS_SCORES_KEY, 2 * 86400)
    return len(inst)


__all__ = ["build_samples", "instruments_of", "next_session_return", "run_forever", "run_once", "score_live"]
