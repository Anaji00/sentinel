"""
services/api_gateway/routes/changes.py

"What changed since you last looked": new trade ideas, new filings, new peer-divergence alerts with their regime label, the viewer's fired
price alerts, and whether the model has a newer date, since a timestamp the client sends (shared/utils/changes.py). Each source reports on
its own; one that cannot be read says so instead of reading as "nothing changed".
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils import changes as ch
from shared.utils import market_quotes as mq
from shared.utils.evidence_bundle import regime_of
from shared.utils.quiet_failures import swallowed
from shared.utils.regime_packet import action_fields
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cached_json
from shared.utils.signal_store import FACTOR_REPORT_KEY

logger = logging.getLogger("api.changes")
router = APIRouter(prefix="/api/v1", tags=["Changes"])

_CALLS_SQL = """
    SELECT day, horizon, ticker, direction, validated, expected_excess_bps, structure FROM recommendation_log
    WHERE day >= $1::date AND ($2::text[] IS NULL OR ticker = ANY($2::text[])) ORDER BY day DESC, ticker LIMIT 200
"""
_FILINGS_SQL = """
    SELECT primary_entity_id, primary_entity_name, headline, url, filing_data, occurred_at FROM events
    WHERE type = 'filing' AND occurred_at > $1 AND ($2::text[] IS NULL OR upper(coalesce(filing_data->>'ticker', primary_entity_id)) = ANY($2::text[]))
    ORDER BY occurred_at DESC LIMIT 200
"""
_REGIME_SQL = """
    SELECT detected_at, rule_name, primary_entity_id, metrics_summary FROM correlations
    WHERE 'divergence' = ANY(tags) AND detected_at > $1 AND ($2::text[] IS NULL OR primary_entity_id = ANY($2::text[]))
    ORDER BY detected_at DESC LIMIT 200
"""
_FEED_FILINGS_SQL = """
    SELECT event_id, primary_entity_id, primary_entity_name, headline, url, filing_data, occurred_at FROM events
    WHERE type = 'filing' AND occurred_at > $1 AND ($2::text[] IS NULL OR upper(coalesce(filing_data->>'ticker', primary_entity_id)) = ANY($2::text[]))
    ORDER BY occurred_at DESC LIMIT 100
"""
_FEED_DIVERGENCE_SQL = """
    SELECT correlation_id, detected_at, rule_name, primary_entity_id, summary_headline, metrics_summary FROM correlations
    WHERE 'divergence' = ANY(tags) AND detected_at > $1 AND ($2::text[] IS NULL OR primary_entity_id = ANY($2::text[]))
    ORDER BY detected_at DESC LIMIT 100
"""
_ALERTS_SQL = """
    SELECT id, ticker, direction, level, triggered_at, triggered_price FROM price_alerts
    WHERE owner = $1 AND triggered_at > $2 ORDER BY triggered_at DESC LIMIT 200
"""


def _iso(v: Any) -> Optional[str]:
    return None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))


def _fd(v: Any) -> Dict[str, Any]:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


@router.get("/changes")
async def changes(
    request: Request,
    since: str = Query(..., description="ISO timestamp of the viewer's last visit; clamped to 7 days back."),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers to limit calls, filings and alerts to."),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """{since, as_of, total, sections:{new_calls, filings, divergence_alerts, price_alerts_fired, model}}. Each of the first four is
    {status: ok|error, count, items (newest 20)}; model is {as_of, newer}. new_calls are trade ideas logged on or after the day of `since`;
    divergence_alerts carry the rule-based regime label; price_alerts_fired are the viewer's own. 'A gate changed since you looked' is not
    recorded historically and is not offered. Send `tickers` to limit it to followed or traded names."""
    if db is None:
        raise HTTPException(status_code=503, detail="The digest is temporarily unavailable.")
    now = datetime.now(timezone.utc)
    try:
        start = ch.parse_since(since, now)
    except ch.SinceInvalid as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    names = mq.clean_list(tickers) if tickers else None
    owner = str(getattr(request.state, "identity", None) or "anonymous")
    raw = getattr(redis, "raw", redis) if redis is not None else None

    async def compute() -> Dict[str, Any]:
        sections: Dict[str, Dict[str, Any]] = {}
        try:
            rows = await db.query(_CALLS_SQL, start.date(), names) or []
            sections["new_calls"] = ch.part([{"day": _iso(r["day"]), "horizon": r["horizon"], "ticker": r["ticker"],
                                              "side": ch.side(r["direction"]), "validated": r["validated"],
                                              "expected_excess_bps": r["expected_excess_bps"], "structure": r["structure"]}
                                             for r in rows], total=len(rows))
        except Exception as exc:
            swallowed("api_gateway.changes.calls", exc, logger)
            sections["new_calls"] = ch.failed("Trade ideas could not be read just now.")
        try:
            rows = await db.query(_FILINGS_SQL, start, names) or []
            items = []
            for r in rows:
                fd = _fd(r.get("filing_data"))
                items.append({"ticker": str(fd.get("ticker") or r.get("primary_entity_id") or ""),
                              "company_name": str(fd.get("company_name") or r.get("primary_entity_name") or ""),
                              "form_type": str(fd.get("form_type") or ""), "filed_at": _iso(r.get("occurred_at")),
                              # Plain, not the stored "SEC Filing: X (T) filed Form Y".
                              "headline": f"{r.get('primary_entity_name') or r.get('primary_entity_id') or ''} filed Form {fd.get('form_type') or ''}".strip(),
                              "url": fd.get("primary_doc_url") or r.get("url")})
            sections["filings"] = ch.part(items, total=len(rows))
        except Exception as exc:
            swallowed("api_gateway.changes.filings", exc, logger)
            sections["filings"] = ch.failed("Filings could not be read just now.")
        try:
            rows = await db.query(_REGIME_SQL, start, names) or []
            items = []
            for r in rows:
                reg = regime_of([r])
                items.append({"ticker": r["primary_entity_id"], "detected_at": _iso(r["detected_at"]), "rule": r["rule_name"],
                              "regime": {"label": reg["label"], "action": reg["action"], **action_fields(reg["action"])} if reg else None})
            sections["divergence_alerts"] = ch.part(items, total=len(rows))
        except Exception as exc:
            swallowed("api_gateway.changes.regime", exc, logger)
            sections["divergence_alerts"] = ch.failed("Divergence alerts could not be read just now.")
        try:
            rows = [r for r in await db.query(_ALERTS_SQL, owner, start) or [] if not names or str(r["ticker"]) in names]
            sections["price_alerts_fired"] = ch.part([{"id": r["id"], "ticker": r["ticker"], "direction": r["direction"], "level": r["level"],
                                                       "triggered_at": _iso(r["triggered_at"]), "triggered_price": r["triggered_price"]}
                                                      for r in rows], total=len(rows))
        except Exception as exc:
            swallowed("api_gateway.changes.price_alerts", exc, logger)
            sections["price_alerts_fired"] = ch.failed("Your price alerts could not be read just now.")
        model: Dict[str, Any] = {"as_of": None, "newer": None}
        if raw is not None:
            try:
                report = json.loads(await raw.get(FACTOR_REPORT_KEY) or "{}")
                gen = report.get("generated_at")
                model = {"as_of": report.get("as_of"), "generated_at": gen,
                         "newer": bool(gen and datetime.fromisoformat(str(gen).replace("Z", "+00:00")).astimezone(timezone.utc) > start)}
            except Exception as exc:
                swallowed("api_gateway.changes.model", exc, logger)
        return {"since": start.isoformat(), "as_of": now.isoformat(), "total": ch.total_changes(sections),
                "sections": {**sections, "model": model}}

    if raw is None:
        return await compute()
    # Cached per viewer, per minute-bucket of `since` and ticker list: the digest is cheap but opened often.
    key = f"api:changes:{owner}:{int(start.timestamp() // 60)}:{','.join(sorted(names)) if names else '*'}"
    return await cached_json(raw, key, compute, ttl=30, max_stale=120)



@router.get("/feed")
async def feed(
    request: Request,
    since: str = Query(..., description="ISO timestamp; clamped to 7 days back."),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers to limit the feed to."),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """One merged, newest-first list of filings, trade ideas, peer-divergence alerts (with their regime label) and the viewer's fired price alerts
    since `since`: {since, as_of, items:[{id, kind, ticker, at, at_precision, title, detail}], failed:[source,...]}. `id` is stable (kind:key: the event id,
    correlation id, alert id, or day:horizon:ticker:side for an idea), so a screen can mark each item seen. Ideas are dated to their day
    (at_precision 'day'). A source that could not be read is named in `failed` and its items are missing, which is not the same as none."""
    if db is None:
        raise HTTPException(status_code=503, detail="The feed is temporarily unavailable.")
    now = datetime.now(timezone.utc)
    try:
        start = ch.parse_since(since, now)
    except ch.SinceInvalid as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    names = mq.clean_list(tickers) if tickers else None
    owner = str(getattr(request.state, "identity", None) or "anonymous")
    items: List[Dict[str, Any]] = []
    bad: List[str] = []
    try:
        for r in await db.query(_FEED_FILINGS_SQL, start, names) or []:
            fd = _fd(r.get("filing_data"))
            t = str(fd.get("ticker") or r.get("primary_entity_id") or "")
            form = str(fd.get("form_type") or "")
            items.append(ch.feed_item("filing", r["event_id"], t or None, _iso(r.get("occurred_at")),
                                      f"{(r.get('primary_entity_name') or t)} filed Form {form}".strip(),
                                      detail={"form_type": form, "url": fd.get("primary_doc_url") or r.get("url")}))
    except Exception as exc:
        swallowed("api_gateway.feed.filings", exc, logger)
        bad.append("filings")
    try:
        for r in await db.query(_CALLS_SQL, start.date(), names) or []:
            side = ch.side(r["direction"])
            day = _iso(r["day"])
            items.append(ch.feed_item("call", f"{day}:{r['horizon']}:{r['ticker']}:{side}", r["ticker"], f"{day}T00:00:00+00:00",
                                      f"{(side or 'neutral').capitalize()} idea on {r['ticker']} ({r['horizon']})", precision="day",
                                      detail={"horizon": r["horizon"], "side": side, "validated": r["validated"],
                                              "expected_excess_bps": r["expected_excess_bps"], "structure": r["structure"]}))
    except Exception as exc:
        swallowed("api_gateway.feed.calls", exc, logger)
        bad.append("calls")
    try:
        for r in await db.query(_FEED_DIVERGENCE_SQL, start, names) or []:
            reg = regime_of([r])
            items.append(ch.feed_item("divergence", r["correlation_id"], r["primary_entity_id"], _iso(r["detected_at"]),
                                      str(r.get("summary_headline") or f"{r['primary_entity_id']} moving against its peers"),
                                      detail={"rule": r["rule_name"], "regime": {"label": reg["label"], "action": reg["action"], **action_fields(reg["action"])} if reg else None}))
    except Exception as exc:
        swallowed("api_gateway.feed.divergence", exc, logger)
        bad.append("divergence_alerts")
    try:
        for r in await db.query(_ALERTS_SQL, owner, start) or []:
            if names and str(r["ticker"]) not in names:
                continue
            items.append(ch.feed_item("price_alert", r["id"], r["ticker"], _iso(r["triggered_at"]),
                                      f"{r['ticker']} went {r['direction']} {r['level']}",
                                      detail={"direction": r["direction"], "level": r["level"], "triggered_price": r["triggered_price"]}))
    except Exception as exc:
        swallowed("api_gateway.feed.price_alerts", exc, logger)
        bad.append("price_alerts_fired")
    return {"since": start.isoformat(), "as_of": now.isoformat(), "items": ch.merge_feed(items, limit), "failed": bad}
