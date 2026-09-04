"""
shared/utils/market_session.py

What time it is, and what that should change.

Nothing in this platform knew. The tradfi collector polled at the same rate at
03:00 on a Sunday as at the opening bell, the enrichers scored a Tuesday
lunchtime block against a baseline containing Saturday, and the agents spent the
same inference budget on both. The event model has carried `earnings_session`
since it was written, so the idea that sessions matter was already present --
nothing acted on it.

Two things follow from knowing the session, and they pull in opposite
directions, which is why one module answers both:

  - Collection. Polling a US equities feed every thirty seconds through a
    weekend spends quota to observe that nothing happened. The same thirty
    seconds at the open is arguably too slow.
  - Interpretation. A volume spike at 09:31 and one at 03:00 are different
    claims. Overnight and pre-market liquidity is thin, so the same absolute
    move is a smaller signal about conviction and a larger one about
    desperation.

Deliberately conservative about what it asserts
-----------------------------------------------
This computes sessions from the clock, not from a holiday calendar it does not
have. It knows weekends and daily session boundaries; it does not know that the
NYSE is shut on Thanksgiving. So `is_open` is really "would be open on an
ordinary week", and every caller treats a wrong answer as a pacing decision
rather than a fact about the market -- polling a closed market slowly costs
nothing, and the freshness tracker notices if a feed goes quiet when it should
not have.

Times are US/Eastern because that is where the equities and options feeds are.
Crypto never closes and is handled explicitly rather than by omission.
"""

import logging
from datetime import datetime, time as dtime, timezone, timedelta
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger("shared.market_session")


class Session(str, Enum):
    """Which part of the trading day it is."""
    CLOSED = "closed"            # weekend, or overnight on a weekday
    PRE_MARKET = "pre_market"    # 04:00-09:30 ET
    REGULAR = "regular"          # 09:30-16:00 ET
    AFTER_HOURS = "after_hours"  # 16:00-20:00 ET
    ALWAYS_OPEN = "always_open"  # crypto, FX


# US/Eastern, from the timezone database where it exists.
#
# zoneinfo is standard library and correct about DST transitions, leap seconds
# and the historical changes a hand-rolled offset gets wrong twice a year. The
# tradfi collector already did this correctly and privately; this is the same
# implementation, in one place, so every service answers the question the same
# way instead of one service knowing and the rest guessing.
#
# The fixed-offset fallback exists because a container without tzdata should
# degrade to being an hour wrong for part of the year rather than crash.
def _eastern_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        return timezone(timedelta(hours=-5))


def eastern_now(utc_now: Optional[datetime] = None) -> datetime:
    """The current wall clock in US/Eastern."""
    now = utc_now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(_eastern_tz())


_PRE_OPEN = dtime(4, 0)
_OPEN = dtime(9, 30)
_CLOSE = dtime(16, 0)
_POST_CLOSE = dtime(20, 0)


def current_session(utc_now: Optional[datetime] = None, asset_class: str = "equities") -> Session:
    """Which session an asset class is in right now.

    `asset_class` matters: crypto and FX do not close, and treating them as
    closed overnight would throttle the feeds that are busiest then.
    """
    if str(asset_class).lower() in ("crypto", "fx", "forex"):
        return Session.ALWAYS_OPEN

    et = eastern_now(utc_now)
    if et.weekday() >= 5:  # Saturday, Sunday
        return Session.CLOSED

    t = et.time()
    if _OPEN <= t < _CLOSE:
        return Session.REGULAR
    if _PRE_OPEN <= t < _OPEN:
        return Session.PRE_MARKET
    if _CLOSE <= t < _POST_CLOSE:
        return Session.AFTER_HOURS
    return Session.CLOSED


# How much to stretch a poll interval in each session.
#
# A multiplier rather than a table of intervals, so each collector keeps its own
# base rate -- the rate it needs when the market is busiest -- and this only
# says how much of that rate is worth paying for now. A collector that polls
# every 30s at the open polls every 8 minutes on a Sunday.
SESSION_POLL_MULTIPLIER = {
    Session.REGULAR: 1.0,
    Session.PRE_MARKET: 2.0,
    Session.AFTER_HOURS: 3.0,
    Session.CLOSED: 16.0,
    Session.ALWAYS_OPEN: 1.0,
}

# No interval is stretched past this, whatever the session. A feed that has not
# been polled for an hour cannot notice that it broke, and the freshness tracker
# would rightly call it stale.
MAX_IDLE_POLL_SEC = 900


def poll_interval(
    base_seconds: float,
    asset_class: str = "equities",
    utc_now: Optional[datetime] = None,
) -> float:
    """The interval to actually sleep, given the session.

    The base is the rate the collector wants when the market is busy. This only
    ever slows it down, never speeds it up: a collector that needs 30s at the
    open is not made to poll faster because something looks interesting, which
    would put rate-limit behaviour in the hands of the market.
    """
    try:
        base = float(base_seconds)
    except (TypeError, ValueError):
        return float(base_seconds)
    if base <= 0:
        return base

    session = current_session(utc_now, asset_class)
    multiplier = SESSION_POLL_MULTIPLIER.get(session, 1.0)
    return min(MAX_IDLE_POLL_SEC, base * multiplier)


def session_liquidity_factor(
    utc_now: Optional[datetime] = None,
    asset_class: str = "equities",
) -> float:
    """How much of normal liquidity this session carries, roughly.

    For interpretation rather than pacing. The same absolute volume means
    something different at 03:00 than at 10:00, and a detector that does not
    know the difference reads thin-book noise as conviction. Callers divide a
    normalised magnitude by this to ask "how large was this *for the session it
    happened in*".

    Deliberately coarse. These are order-of-magnitude adjustments and pretending
    to more precision than that would be inventing a microstructure model.
    """
    session = current_session(utc_now, asset_class)
    return {
        Session.REGULAR: 1.0,
        Session.PRE_MARKET: 0.15,
        Session.AFTER_HOURS: 0.10,
        Session.CLOSED: 0.05,
        Session.ALWAYS_OPEN: 1.0,
    }.get(session, 1.0)

# An is_open() predicate was written here and never called. current_session()
# answers the same question with more information, and every caller wanted the
# session rather than the boolean. Removed for the reason above.
