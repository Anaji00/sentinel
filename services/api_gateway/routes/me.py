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
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from services.api_gateway.dependencies import get_db_optional
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("api-gateway-me")

router = APIRouter(prefix="/api/v1/me", tags=["Me"])

MAX_TICKERS = 30
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


class Preferences(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    town: Optional[Town] = None
    # What home leads with: stock calls, the civic watch, or both at equal weight.
    focus: Literal["stocks", "civic", "both"] = "both"
    # Whether the first-visit welcome has been answered.
    welcomed: bool = False

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
