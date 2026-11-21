"""
services/api_gateway/routes/me.py

The signed-in person's own settings: the stocks they follow, their town, and
what the home page should lead with. Keyed by the identity the gateway verified
from the session cookie -- never by anything the browser sends -- so one account
can neither read nor write another's.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from services.api_gateway.dependencies import get_db_optional
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("api-gateway-me")

router = APIRouter(prefix="/api/v1/me", tags=["Me"])

MAX_TICKERS = 30
MAX_FLAGS = 30
_FLAG = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_TICKER = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")
_STATE = re.compile(r"^[A-Z]{2}$")


class Town(BaseModel):
    """A police department or agency the person chose as theirs."""

    agency: str = Field(min_length=1, max_length=160)
    state: str

    @field_validator("state")
    @classmethod
    def _state(cls, v: str) -> str:
        v = v.strip().upper()
        if not _STATE.match(v):
            raise ValueError("state must be a two-letter code")
        return v


class NotifyPrefs(BaseModel):
    """What a person wants to be told about the stocks they follow. Each kind
    is on until turned off; email is off until turned on."""

    call: bool = True
    signal: bool = True
    filing: bool = True
    holders: bool = True
    email_digest: bool = False


class Preferences(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    town: Optional[Town] = None
    # What home leads with: stock calls, the civic watch, or both at equal weight.
    focus: Literal["stocks", "civic", "both"] = "both"
    # Whether the first-visit welcome has been answered.
    welcomed: bool = False
    notify: NotifyPrefs = Field(default_factory=NotifyPrefs)
    # When the person last looked (the digest's cut-off), kept on the server so it survives a change of device.
    last_seen_at: Optional[str] = None
    # Small seen/onboarding switches the screens keep per person: lower-case names, true or false.
    flags: Dict[str, bool] = Field(default_factory=dict)

    @field_validator("last_seen_at")
    @classmethod
    def _last_seen(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        try:
            ts = datetime.fromisoformat(str(v).strip().replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("last_seen_at must be an ISO timestamp")
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        if ts > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("last_seen_at is in the future")
        return ts.astimezone(timezone.utc).isoformat()

    @field_validator("flags")
    @classmethod
    def _flags(cls, v: Dict[str, bool]) -> Dict[str, bool]:
        if len(v) > MAX_FLAGS:
            raise ValueError(f"at most {MAX_FLAGS} flags")
        for k in v:
            if not _FLAG.match(str(k)):
                raise ValueError(f"flag names are lower-case letters, digits and underscores: {str(k)[:24]}")
        return {str(k): bool(x) for k, x in v.items()}

    @field_validator("tickers")
    @classmethod
    def _tickers(cls, v: List[str]) -> List[str]:
        out: List[str] = []
        for t in v:
            t = str(t).strip().upper()
            if not _TICKER.match(t):
                raise ValueError(f"not a stock symbol: {t[:12]}")
            if t not in out:
                out.append(t)
        if len(out) > MAX_TICKERS:
            raise ValueError(f"at most {MAX_TICKERS} stocks")
        return out


def owner_key(request: Request) -> str:
    """The verified identity that owns these settings: the session's email, or
    the gateway's own name for an API-key or development caller."""
    ident = getattr(getattr(request, "state", None), "identity", None)
    if not isinstance(ident, str) or not ident or ident == "anonymous":
        raise HTTPException(status_code=401, detail="Sign in to save settings.")
    return ident[len("session:"):].strip().lower() if ident.startswith("session:") else ident


def merge(stored: Optional[Dict[str, Any]], patch: Dict[str, Any]) -> Preferences:
    """The stored settings with a partial update applied, validated whole."""
    base = Preferences.model_validate(stored or {}).model_dump()
    base.update({k: v for k, v in patch.items() if k in Preferences.model_fields})
    return Preferences.model_validate(base)


_GET_SQL = "SELECT prefs FROM user_preferences WHERE owner = $1"
_PUT_SQL = """
    INSERT INTO user_preferences (owner, prefs, updated_at) VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (owner) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = NOW()
"""


async def _load(db: Any, owner: str) -> Optional[Dict[str, Any]]:
    row = await db.query_one(_GET_SQL, owner)
    if not row:
        return None
    raw = row["prefs"]
    # The pool's jsonb codec decodes to a dict; a row written before that was
    # understood holds a JSON string inside the JSON, read here all the same.
    return json.loads(raw) if isinstance(raw, str) else dict(raw)


@router.get("/preferences")
async def get_preferences(request: Request, db=Depends(get_db_optional)) -> Dict[str, Any]:
    owner = owner_key(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Settings store unavailable.")
    try:
        stored = await _load(db, owner)
    except Exception as e:
        swallowed("api_gateway.me.get_preferences", e, logger)
        raise HTTPException(status_code=503, detail="Settings store unavailable.")
    return {"saved": stored is not None, **Preferences.model_validate(stored or {}).model_dump()}


@router.patch("/preferences")
async def patch_preferences(patch: Dict[str, Any], request: Request, db=Depends(get_db_optional)) -> Dict[str, Any]:
    owner = owner_key(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Settings store unavailable.")
    try:
        prefs = merge(await _load(db, owner), patch)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    # A dict: the pool's jsonb codec encodes it (encoding here too stored a
    # JSON string inside the JSON column).
    await db.execute(_PUT_SQL, owner, prefs.model_dump())
    return {"saved": True, **prefs.model_dump()}


# ── The inbox ────────────────────────────────────────────────────────────────
#
# Written by services/alert_manager/notifications_job.py. Every query is scoped
# by the verified owner: one account can neither read nor mark another's.

INBOX_LIMIT = 50


class Notification(BaseModel):
    id: int
    kind: str
    ticker: Optional[str] = None
    title: str
    body: str
    url: str
    created_at: str
    read: bool


class Inbox(BaseModel):
    unread: int
    items: List[Notification]


class MarkRead(BaseModel):
    """Which rows to mark read: the listed ids, or everything."""

    ids: List[int] = Field(default_factory=list, max_length=INBOX_LIMIT)
    all: bool = False


_UNREAD_SQL = "SELECT count(*) AS n FROM notifications WHERE owner = $1 AND read_at IS NULL"
_INBOX_SQL = """
    SELECT id, kind, ticker, title, body, url, created_at, read_at FROM notifications
    WHERE owner = $1 ORDER BY created_at DESC, id DESC LIMIT $2
"""


@router.get("/notifications", response_model=Inbox)
async def get_notifications(request: Request, db=Depends(get_db_optional)) -> Inbox:
    owner = owner_key(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Notifications unavailable.")
    unread = await db.query_one(_UNREAD_SQL, owner)
    rows = await db.query(_INBOX_SQL, owner, INBOX_LIMIT)
    return Inbox(unread=int((unread or {}).get("n") or 0), items=[
        Notification(id=r["id"], kind=r["kind"], ticker=r["ticker"], title=r["title"], body=r["body"], url=r["url"],
                     created_at=str(r["created_at"]), read=r["read_at"] is not None)
        for r in rows or []
    ])


@router.post("/notifications/read")
async def mark_notifications_read(req: MarkRead, request: Request, db=Depends(get_db_optional)) -> Dict[str, bool]:
    owner = owner_key(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Notifications unavailable.")
    if req.all:
        await db.execute("UPDATE notifications SET read_at = NOW() WHERE owner = $1 AND read_at IS NULL", owner)
    elif req.ids:
        await db.execute(
            "UPDATE notifications SET read_at = NOW() WHERE owner = $1 AND id = ANY($2::bigint[]) AND read_at IS NULL",
            owner, req.ids,
        )
    return {"ok": True}
