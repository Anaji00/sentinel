"""
services/api_gateway/routes/signals.py

Trading signals, and the evidence that earned or refused them.

/signals         active signals with their evidence, the platform's own scored
                 track record of expired signals, and whether signals are halted.
/signals/study   every signal family and horizon the reaction study graded --
                 proven, unproven, contradicted -- with its numbers and the
                 reason, so nothing about a signal is asserted without its working.
/signals/convergence   instruments where independent families agree, weighted by
                 each family's own study record (shared.utils.evidence_fusion).
/signals/dossier/{ticker}   every factor seen for one instrument, with its record.

Read from what services/agents/signal_engine.py writes.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Query

from services.api_gateway.dependencies import get_db, get_redis_client
from shared.utils.evidence_fusion import (
    EVIDENCE_EVENT_TYPES, EVIDENCE_EVENTS_SQL, build_dossiers, convergence, family_record,
)
from shared.utils.signal_families import observations
from shared.utils.feature_flags import FeatureFlagManager
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.signal_store import ACTIVE_KEY, EVIDENCE_CACHE_PREFIX, OUTCOMES_KEY, STUDY_KEY

logger = logging.getLogger("api.signals")

router = APIRouter(prefix="/api/v1/signals", tags=["Trading Signals"])


VERDICT_ORDER = {
    "proven": 0, "unconfirmed": 1, "below_costs": 2, "low_hit_rate": 3,
    "contradicts_prior": 4, "no_edge": 5, "insufficient_data": 6, "unmeasurable": 7,
}


def _decode(v: Any) -> str:
    return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)


def track_record(outcomes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Hits, misses and mean return over scored signals, overall and per family."""
    scored = [o for o in outcomes if o.get("hit") is not None]
    by_family: Dict[str, Dict[str, Any]] = {}
    for o in scored:
        for fam in o.get("families") or []:
            row = by_family.setdefault(fam, {"signals": 0, "hits": 0, "return_bps": 0.0})
            row["signals"] += 1
            row["hits"] += int(bool(o["hit"]))
            row["return_bps"] += float(o.get("return_bps") or 0.0)
    for row in by_family.values():
        row["hit_rate"] = round(row["hits"] / row["signals"], 3)
        row["mean_return_bps"] = round(row.pop("return_bps") / row["signals"], 1)
    hits = sum(1 for o in scored if o["hit"])
    return {
        "closed": len(outcomes),
        "scored": len(scored),
        "hits": hits,
        "hit_rate": round(hits / len(scored), 3) if scored else None,
        "mean_return_bps": round(sum(float(o.get("return_bps") or 0) for o in scored) / len(scored), 1) if scored else None,
        "by_family": by_family,
    }


@router.get("")
async def signals(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    raw = getattr(redis_client, "raw", redis_client)
    active: List[Dict[str, Any]] = []
    outcomes: List[Dict[str, Any]] = []
    study_meta: Dict[str, Any] = {}
    try:
        for _sid, blob in (await raw.hgetall(ACTIVE_KEY) or {}).items():
            active.append(json.loads(_decode(blob)))
        outcomes = [json.loads(_decode(b)) for b in (await raw.lrange(OUTCOMES_KEY, 0, 499) or [])]
        study = await raw.get(STUDY_KEY)
        if study:
            parsed = json.loads(_decode(study))
            study_meta = {k: parsed.get(k) for k in ("generated_at", "episodes", "instruments", "verdicts", "lookback_days")}
    except Exception as exc:
        swallowed("api.signals.read", exc, logger)
    active.sort(key=lambda s: -float(s.get("probability") or 0))
    enabled = await FeatureFlagManager(redis_client).is_enabled("trade_signals")
    return {
        "enabled": enabled,
        "active": active,
        "track_record": track_record(outcomes),
        "recent_outcomes": outcomes[:25],
        "study": study_meta,
    }


@router.get("/study")
async def study(
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    raw = getattr(redis_client, "raw", redis_client)
    try:
        blob = await raw.get(STUDY_KEY)
    except Exception as exc:
        swallowed("api.signals.study", exc, logger)
        blob = None
    if not blob:
        return {"generated_at": None, "cells": [], "note": "The first study runs within a minute of the engine starting."}
    data = json.loads(_decode(blob))
    data["cells"] = sorted(
        data.get("cells", []),
        key=lambda c: (VERDICT_ORDER.get(c.get("verdict"), 9), c.get("family"), c.get("horizon")),
    )
    return data


EVIDENCE_CACHE_SEC = 120


def _event_row(r: Any) -> Dict[str, Any]:
    d = dict(r)
    fd = d.get("financial_data")
    if isinstance(fd, str):
        try:
            d["financial_data"] = json.loads(fd)
        except ValueError:
            d["financial_data"] = {}
    return d


async def _dossiers(db: Any, redis_client: Any, hours: int) -> Dict[str, Any]:
    """Every instrument's dossier over the window, cached briefly."""
    raw = getattr(redis_client, "raw", redis_client)
    key = f"{EVIDENCE_CACHE_PREFIX}{hours}"
    try:
        cached = await raw.get(key)
        if cached:
            return json.loads(_decode(cached))
    except Exception as exc:
        swallowed("api.signals.evidence_cache_read", exc, logger)

    now = datetime.now(timezone.utc)
    rows = await db.query(EVIDENCE_EVENTS_SQL, now - timedelta(hours=hours), now, EVIDENCE_EVENT_TYPES)
    cells: List[Dict[str, Any]] = []
    try:
        blob = await raw.get(STUDY_KEY)
        if blob:
            cells = json.loads(_decode(blob)).get("cells", [])
    except Exception as exc:
        swallowed("api.signals.evidence_study_read", exc, logger)

    def _compute() -> Dict[str, Any]:
        obs = [o for r in rows or [] for o in observations(_event_row(r))]
        built = build_dossiers(obs, family_record(cells))
        return {
            "generated_at": now.isoformat(),
            "window_hours": hours,
            "events": len(rows or []),
            "dossiers": {k: d.as_dict() for k, d in built.items()},
            "ranked": [d.instrument for d in convergence(built, limit=50)],
        }

    # Headline mapping over a day of events is CPU work; keep it off the loop.
    result = await asyncio.to_thread(_compute)
    try:
        await raw.set(key, json.dumps(result, default=str), ex=EVIDENCE_CACHE_SEC)
    except Exception as exc:
        swallowed("api.signals.evidence_cache_write", exc, logger)
    return result


@router.get("/convergence")
async def evidence_convergence(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(25, ge=1, le=50),
    db=Depends(get_db),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Instruments where two or more independent families agree on a side.

    Ranked by proven factors first, then weighted score. A stance resting only
    on unstudied families is labelled so; this is a ranking of evidence, and
    the signal engine alone may turn evidence into a trade signal."""
    data = await _dossiers(db, redis_client, hours)
    ranked = [data["dossiers"][t] for t in data["ranked"][:limit] if t in data["dossiers"]]
    return {
        "generated_at": data["generated_at"],
        "window_hours": hours,
        "events": data["events"],
        "instruments_seen": len(data["dossiers"]),
        "converging": ranked,
    }


@router.get("/dossier/{ticker}")
async def evidence_dossier(
    ticker: str,
    hours: int = Query(72, ge=1, le=168),
    db=Depends(get_db),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Every factor seen for one instrument over the window, with its study record."""
    data = await _dossiers(db, redis_client, hours)
    d = data["dossiers"].get(ticker.strip().upper())
    return {
        "generated_at": data["generated_at"],
        "window_hours": hours,
        "instrument": ticker.strip().upper(),
        "dossier": d,
    }
