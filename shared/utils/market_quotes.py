"""A quote with its age: last price, when it is from, which close the day's move is measured from, and whether it is delayed.

One builder so hero, chart and compare agree: every one of them reads the same price, time and change base. Nothing is guessed: a field that
cannot be known is None, never 0. Pure; no I/O.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from shared.utils.market_session import Session, current_session
from shared.utils.recommendations import change_base

_NY = ZoneInfo("America/New_York")
DELAYED_AFTER_SEC = 20 * 60          # the free consolidated feed lags about 15 minutes; later than 20 minutes is behind even that
MAX_TICKERS = 50


def _dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _day(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def build_quote(ticker: str, bar: Optional[Tuple[Any, float]], daily: Sequence[Tuple[Any, float]], now: datetime,
                session: Optional[Session] = None) -> Dict[str, Any]:
    """One quote. `bar` is the newest minute bar (time, close) or None; `daily` is recent official closes (day, close), oldest first."""
    session = session or current_session(now)
    days = [d for d in (_day(x[0]) for x in daily) if d is not None]
    closes = [float(x[1]) for x in daily if _day(x[0]) is not None]
    bar_time = _dt(bar[0]) if bar else None
    price: Optional[float] = None
    as_of: Optional[datetime] = None
    source: Optional[str] = None
    if bar_time is not None and bar[1] is not None and float(bar[1]) > 0:
        price, as_of, source = float(bar[1]), bar_time, "minute_bar"
    elif closes:
        price, source = closes[-1], "daily_close"
        as_of = datetime.combine(days[-1], datetime.min.time(), tzinfo=_NY).replace(hour=16)
    today = now.astimezone(_NY).date()
    base, base_day = change_base(days, closes, today, session == Session.REGULAR) if closes else (None, None)
    change = round((price / base - 1.0) * 100.0, 2) if price is not None and base else None
    age = int((now - as_of).total_seconds()) if as_of is not None else None
    live_session = session in (Session.REGULAR, Session.PRE_MARKET, Session.AFTER_HOURS)
    delayed = (age is not None and age > DELAYED_AFTER_SEC) if (live_session and source == "minute_bar") else (None if age is None else False)
    return {
        "ticker": ticker,
        "price": round(price, 4) if price is not None else None,
        "as_of": as_of.astimezone(timezone.utc).isoformat() if as_of is not None else None,
        "age_sec": max(0, age) if age is not None else None,
        "source": source,
        "session": session.value,
        "delayed": delayed,
        "change_pct": change,
        "prev_close": round(base, 4) if base else None,
        "change_base_date": base_day.isoformat() if base and base_day else None,
    }


def clean_list(raw: str, limit: int = MAX_TICKERS) -> List[str]:
    """Upper-cased, de-duplicated tickers in order; anything that is not a plausible symbol is dropped."""
    out: List[str] = []
    for part in str(raw or "").replace(";", ",").split(","):
        t = part.strip().upper()
        if t and len(t) <= 12 and all(c.isalnum() or c in ".=^-" for c in t) and t not in out:
            out.append(t)
    return out[:limit]


__all__ = ["DELAYED_AFTER_SEC", "MAX_TICKERS", "build_quote", "clean_list"]
