"""
shared/utils/price_moves.py

A futures move as an event, from bars the platform already stores.

Measured on 4 October: tradfi_bars held 741 crude, 687 Brent, 374 wheat and
286 corn bars in thirty days, and the events table held no event for any of
them. The candle path emits only for bars with volume on a watched equity, and
the macro tier serves futures through ETF proxies that report no volume -- so
every commodity bar was stored and dropped. Every OSINT bridge on the platform
points at a commodity (a tanker in Hormuz exposes CL=F), and the rules that
join an exposure to a market move had no market move to join.

A move is reported once per band per direction per day: crossing 1.5% is news,
the next ninety bars at 1.6% are not.
"""

from __future__ import annotations

from typing import Optional

from shared.utils.equities import asset_class

# Bands a day's move is reported at. 1.5 is the floor the physical-disruption
# rule already asks for (`min_abs_move_pct`), set for markets that move less
# than equities.
MOVE_BANDS_PCT = (1.5, 3.0, 5.0)

# Long enough to span a Friday close to a Tuesday open, so a quote republished
# over a long weekend still finds its session's record.
MOVE_KEY_TTL_SEC = 4 * 86400


def is_futures_move_subject(ticker: str) -> bool:
    """Commodities and index futures: what the macro tier serves as =F symbols."""
    sym = str(ticker or "").upper()
    return asset_class(sym) == "commodity" or sym.endswith("=F")


def day_move_pct(open_price: float, close_price: float) -> Optional[float]:
    """Percent from the session open, or None without two usable prices."""
    if not open_price or not close_price or open_price <= 0 or close_price <= 0:
        return None
    return round((close_price - open_price) / open_price * 100.0, 3)


def band_crossed(move_pct: Optional[float]) -> Optional[float]:
    """The largest band the move has reached, or None below the first."""
    if move_pct is None:
        return None
    reached = [b for b in MOVE_BANDS_PCT if abs(move_pct) >= b]
    return reached[-1] if reached else None


def move_score(move_pct: float) -> float:
    """1.5% scores 0.50, 3% 0.65, 5% 0.85; capped below certainty."""
    return round(min(0.90, 0.35 + 0.10 * abs(move_pct)), 3)


def band_key(ticker: str, session_open: float, band: float, move_pct: float) -> str:
    """Redis key that makes a band report once per direction per session.

    The session is named by its opening price, not the calendar day. The macro
    collector republishes its last quote whenever it restarts, and over the
    first weekend that was Friday's bar: keyed by day, Friday's 2.65% crude move
    reported again on Sunday as Sunday's, and rule_physical_disruption_repricing
    fired three times on it. A republished bar carries the same open, so it
    finds the record its first publication left.
    """
    sign = "up" if move_pct >= 0 else "down"
    return f"sentinel:price_move:{str(ticker).upper()}:{session_open:.6g}:{sign}:{band}"
