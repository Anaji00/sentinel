"""
services/api_gateway/routes/alerts.py

A viewer's price alerts, delivered in the app and, from the page, as browser notifications (shared/utils/price_alerts.py).
Evaluated on read from stored minute bars (high and low since the alert was last checked, with a 20-minute overlap for the free feed's
lag), so a crossing between two polls is not missed. Only tickers with minute bars can fire. The browser side polls GET /triggered.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_db_optional
from shared.utils import price_alerts as pa
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.alerts")
router = APIRouter(prefix="/api/v1/alerts", tags=["Price alerts"])

_COLUMNS = "id, ticker, direction, level, note, created_at, triggered_at, triggered_price, seen_at"
_EXTREMES_SQL = """
    SELECT max(high) AS high, min(low) AS low, max(time) AS at FROM tradfi_bars
    WHERE ticker = $1 AND time > GREATEST($2::timestamptz - INTERVAL '20 minutes', $3::timestamptz - INTERVAL '1 minute')
"""


class NewAlert(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=12)
    direction: str = Field(..., description="'above' or 'below'")
    level: float = Field(..., description="Price in the instrument's currency")
    note: Optional[str] = Field(None, max_length=pa.MAX_NOTE)


def _owner(request: Request) -> str:
    return str(getattr(request.state, "identity", None) or "anonymous")


def _iso(v: Any) -> Optional[str]:
    return None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))


def _shape(r: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": r["id"], "ticker": r["ticker"], "direction": r["direction"], "level": r["level"], "note": r.get("note"),
            "status": "triggered" if r.get("triggered_at") else "active", "created_at": _iso(r.get("created_at")),
            "triggered_at": _iso(r.get("triggered_at")), "triggered_price": r.get("triggered_price"), "seen_at": _iso(r.get("seen_at"))}


def _need_db(db: Any) -> Any:
    if db is None:
        raise HTTPException(status_code=503, detail="Alerts are temporarily unavailable.")
    return db


async def _evaluate(db: Any, owner: str) -> None:
    """Check this owner's active alerts against the bars since each was last checked and record the ones that fired."""
    active = await db.query("SELECT id, ticker, direction, level, created_at, checked_at FROM price_alerts "
                            "WHERE owner = $1 AND triggered_at IS NULL", owner) or []
    if not active:
        return
    started = datetime.now(timezone.utc)
    fired_ids: List[int] = []
    read_ids: List[int] = []                     # alerts whose bars were actually read: only these move their checked_at
    for a in active:
        checked, created = pa.as_dt(a["checked_at"]), pa.as_dt(a["created_at"])
        if checked is None or created is None:
            swallowed("api_gateway.alerts.timestamps", ValueError(f"alert {a['id']} has no usable timestamps"), logger)
            continue
        try:
            row = await db.query_one(_EXTREMES_SQL, a["ticker"], checked, created)
        except Exception as exc:
            swallowed("api_gateway.alerts.extremes", exc, logger)
            continue
        read_ids.append(a["id"])
        got = pa.evaluate([a], {a["ticker"]: (row.get("high"), row.get("low"), row.get("at"))} if row else {})
        if got:
            await db.execute("UPDATE price_alerts SET triggered_at = NOW(), triggered_price = $2, checked_at = $3 "
                             "WHERE id = $1 AND triggered_at IS NULL", a["id"], got[0]["price"], started)
            fired_ids.append(a["id"])
    # A failed bars query leaves checked_at where it was, so the crossing is looked for again next time instead of being skipped.
    quiet = [i for i in read_ids if i not in fired_ids]
    if quiet:
        await db.execute("UPDATE price_alerts SET checked_at = $2 WHERE id = ANY($1::bigint[])", quiet, started)


@router.post("")
async def create_alert(body: NewAlert, request: Request, db=Depends(get_db_optional),
                       user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Create an alert: {ticker, direction: above|below, level, note?}. Up to 50 active per viewer. Returns the alert."""
    db = _need_db(db)
    try:
        v = pa.validate_new(clean_ticker(body.ticker), body.direction, body.level, body.note)
    except pa.AlertInvalid as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    owner = _owner(request)
    try:
        n = await db.query_one("SELECT count(*) AS n FROM price_alerts WHERE owner = $1 AND triggered_at IS NULL", owner)
        if n and int(n["n"]) >= pa.MAX_ACTIVE_PER_OWNER:
            raise HTTPException(status_code=409, detail=f"You have {pa.MAX_ACTIVE_PER_OWNER} active alerts; remove one first.")
        row = await db.query_one(f"INSERT INTO price_alerts (owner, ticker, direction, level, note) VALUES ($1, $2, $3, $4, $5) "
                                 f"RETURNING {_COLUMNS}", owner, v["ticker"], v["direction"], v["level"], v["note"])
    except HTTPException:
        raise
    except Exception as exc:
        swallowed("api_gateway.alerts.create", exc, logger)
        raise HTTPException(status_code=503, detail="Alerts are temporarily unavailable.")
    return _shape(dict(row))


@router.get("")
async def list_alerts(request: Request, db=Depends(get_db_optional),
                      user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """The viewer's alerts, newest first (active and triggered, up to 200), after checking the active ones."""
    db = _need_db(db)
    owner = _owner(request)
    try:
        await _evaluate(db, owner)
        rows = await db.query(f"SELECT {_COLUMNS} FROM price_alerts WHERE owner = $1 ORDER BY created_at DESC LIMIT 200", owner) or []
    except Exception as exc:
        swallowed("api_gateway.alerts.list", exc, logger)
        raise HTTPException(status_code=503, detail="Alerts are temporarily unavailable.")
    return {"alerts": [_shape(dict(r)) for r in rows]}


@router.get("/triggered")
async def triggered_alerts(request: Request, db=Depends(get_db_optional),
                           user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Alerts that have fired and not been acknowledged, after checking the active ones. Poll about once a minute; show each (and use
    the browser's Notification API on the page), then POST /api/v1/alerts/{id}/ack so it is not shown again."""
    db = _need_db(db)
    owner = _owner(request)
    try:
        await _evaluate(db, owner)
        rows = await db.query(f"SELECT {_COLUMNS} FROM price_alerts WHERE owner = $1 AND triggered_at IS NOT NULL AND seen_at IS NULL "
                              "ORDER BY triggered_at", owner) or []
    except Exception as exc:
        swallowed("api_gateway.alerts.triggered", exc, logger)
        raise HTTPException(status_code=503, detail="Alerts are temporarily unavailable.")
    return {"triggered": [_shape(dict(r)) for r in rows]}


@router.post("/{alert_id}/ack")
async def ack_alert(alert_id: int, request: Request, db=Depends(get_db_optional),
                    user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Mark a fired alert as seen."""
    db = _need_db(db)
    row = await db.query_one("UPDATE price_alerts SET seen_at = NOW() WHERE id = $1 AND owner = $2 AND triggered_at IS NOT NULL "
                             "RETURNING id", alert_id, _owner(request))
    if not row:
        raise HTTPException(status_code=404, detail="No such fired alert.")
    return {"id": alert_id, "acknowledged": True}


@router.delete("/{alert_id}")
async def delete_alert(alert_id: int, request: Request, db=Depends(get_db_optional),
                       user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Remove one of the viewer's alerts, fired or not."""
    db = _need_db(db)
    row = await db.query_one("DELETE FROM price_alerts WHERE id = $1 AND owner = $2 RETURNING id", alert_id, _owner(request))
    if not row:
        raise HTTPException(status_code=404, detail="No such alert.")
    return {"id": alert_id, "deleted": True}
