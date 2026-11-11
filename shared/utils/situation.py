"""
shared/utils/situation.py

The life of a divergence, in four stages, so a stock that keeps diverging is one developing situation and not a stream
of unrelated alerts (reports/Divergence alert.md; reports/Hardening audit - twelve-layer brief.md).

  DEVELOPING  first flagged.
  CONFIRMED   flagged again in a later session: it survived a close.
  ESCALATING  the gap grew by ESCALATE_STEP standard deviations beyond the largest seen in this situation.
  RESOLVED    no longer flagged and the gap is back inside RESOLVE_Z, or the stock has moved with its reference.

Transitions are from measurements only. A resolved situation that returns starts a new one (DEVELOPING, count + 1).
Pure: a state and a measurement in, a state out.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

DEVELOPING, CONFIRMED, ESCALATING, RESOLVED = "DEVELOPING", "CONFIRMED", "ESCALATING", "RESOLVED"
ESCALATE_STEP = 1.5
RESOLVE_Z = 1.0
EXPIRE_DAYS = 5          # not seen for this many calendar days: over, whatever the gap


def _new(z: float, news: bool, session: str, number: int) -> Dict[str, Any]:
    return {"stage": DEVELOPING, "number": number, "first_session": session, "last_session": session, "sessions": 1,
            "peak_z": round(abs(z), 2), "last_z": round(z, 2), "news": bool(news)}


def on_detection(state: Optional[Dict[str, Any]], z: float, news: bool, session: str) -> Dict[str, Any]:
    """The state after the stock is flagged with gap z in `session` (an ISO date)."""
    if not state or state.get("stage") == RESOLVED:
        return _new(z, news, session, int((state or {}).get("number", 0)) + 1)
    s = dict(state)
    new_session = session != s["last_session"]
    if new_session:
        s["sessions"] = int(s["sessions"]) + 1
        if s["stage"] == DEVELOPING:
            s["stage"] = CONFIRMED
    if abs(z) >= float(s["peak_z"]) + ESCALATE_STEP:
        s["stage"] = ESCALATING
    s["peak_z"] = round(max(float(s["peak_z"]), abs(z)), 2)
    s["last_z"] = round(z, 2)
    s["last_session"] = session
    s["news"] = bool(s.get("news")) or bool(news)
    return s


def on_quiet(state: Optional[Dict[str, Any]], z: Optional[float], same_direction: bool, session: str, days_since_seen: int) -> Optional[Dict[str, Any]]:
    """The state when the stock is not flagged now. `z` is its current gap z, None if it cannot be measured.

    Returns None when there is nothing to change (no open situation, or already resolved)."""
    if not state or state.get("stage") == RESOLVED:
        return None
    closed = (z is not None and abs(z) < RESOLVE_Z) or same_direction or days_since_seen >= EXPIRE_DAYS
    if not closed:
        return None
    s = dict(state)
    s["stage"] = RESOLVED
    s["resolved_session"] = session
    if z is not None:
        s["last_z"] = round(z, 2)
    return s


def label(state: Dict[str, Any]) -> str:
    """One line for an alert: where the situation stands."""
    n = int(state.get("sessions", 1))
    base = {DEVELOPING: "new today", CONFIRMED: f"holding for {n} sessions", ESCALATING: f"widening, peak {state.get('peak_z')} sigma over {n} sessions",
            RESOLVED: "closed"}.get(state.get("stage"), "")
    return f"Situation {state.get('stage', '').lower()}: {base}"


__all__ = ["CONFIRMED", "DEVELOPING", "ESCALATING", "RESOLVED", "label", "on_detection", "on_quiet"]
