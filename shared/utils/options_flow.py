"""
shared/utils/options_flow.py

What an option print says about direction. Pure; collector-tradfi prices each
large print with these before publishing it, and confirms the next morning
whether it opened a position.

Over ten days to 16 Oct 2026, 84% of flagged option prints were in the money,
and deep in-the-money contracts expiring within two days carried $987M of the
$2.0B premium and the highest average anomaly score. Their premium is almost
all intrinsic value -- a stock substitute, a closing trade or a dividend play
-- and says little about where the buyer thinks the stock goes. And with no
trade side, a sale at the bid read the same as a purchase at the offer.

  extrinsic premium  what was paid above intrinsic value: the price of the
                     directional view itself
  aggressor          the quote rule (Lee & Ready): a print above the midpoint
                     was bought, below it sold; only when the quote is close
                     in time to the print
  opening            open interest the next morning up by at least half the
                     print's size: the trade opened a position

Buyer-initiated, opening option volume is what predicts the stock (Pan &
Poteshman, Review of Financial Studies 2006); the new families are built on it
and measured beside the old premium-sized sweeps.
"""
from __future__ import annotations

import bisect
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

PENDING_OI_KEY = "sentinel:options:pending_oi"     # prints awaiting next-morning open interest
MIN_EXTRINSIC_USD = 50_000.0     # the directional money, same floor the total premium had
QUOTE_MAX_LAG_SEC = 60.0
# The feed is 15 minutes delayed and polled, so the snapshot's quote is often
# minutes from the print (a median of 18 minutes from print to ingestion, 16 Oct
# 2026), and history serves no option quotes to look up. A quote this old still
# classifies a print made at or through it -- paying the ask or more is a buy,
# hitting the bid or less a sell -- but not one between, which needs the
# midpoint of a fresh quote.
STALE_QUOTE_MAX_LAG_SEC = 300.0
INTRINSIC_HEAVY = 0.8            # intrinsic value at least this share of the price
OPENING_SHARE = 0.5              # open interest up by at least half the print's size
PENDING_MAX_AGE_DAYS = 4


def intrinsic(option_type: str, strike: Optional[float], spot: Optional[float]) -> Optional[float]:
    """Per-share intrinsic value, or None without strike and spot."""
    if strike is None or spot is None or spot <= 0:
        return None
    if str(option_type).upper().startswith("C"):
        return max(0.0, spot - float(strike))
    return max(0.0, float(strike) - spot)


def extrinsic_premium(price: float, size: float, option_type: str, strike: Optional[float],
                      spot: Optional[float]) -> Optional[float]:
    """Dollars paid above intrinsic value across the print (100 shares a contract)."""
    iv = intrinsic(option_type, strike, spot)
    if iv is None or price <= 0:
        return None
    return max(0.0, price - iv) * size * 100.0


def intrinsic_share(price: float, option_type: str, strike: Optional[float], spot: Optional[float]) -> Optional[float]:
    iv = intrinsic(option_type, strike, spot)
    if iv is None or price <= 0:
        return None
    return min(1.0, iv / price)


def aggressor(price: float, bid: Optional[float], ask: Optional[float], trade_at: Optional[datetime],
              quote_at: Optional[datetime], max_lag_sec: float = QUOTE_MAX_LAG_SEC,
              stale_lag_sec: float = STALE_QUOTE_MAX_LAG_SEC) -> Optional[str]:
    """'buy', 'sell' or None: the print against the quote's midpoint when the
    quote was taken within `max_lag_sec` of the print; against the quote's
    edges only (at or through the ask or bid) when within `stale_lag_sec`;
    never against a crossed quote."""
    if not bid or not ask or ask < bid or trade_at is None or quote_at is None:
        return None
    lag = abs((quote_at - trade_at).total_seconds())
    if lag > stale_lag_sec:
        return None
    if lag > max_lag_sec:
        return "buy" if price >= ask else "sell" if price <= bid else None
    mid = (bid + ask) / 2.0
    if price > mid:
        return "buy"
    if price < mid:
        return "sell"
    return None


def side_miss_reason(bid: Optional[float], ask: Optional[float], trade_at: Optional[datetime],
                     quote_at: Optional[datetime], max_lag_sec: float = QUOTE_MAX_LAG_SEC,
                     stale_lag_sec: float = STALE_QUOTE_MAX_LAG_SEC) -> str:
    """Why aggressor() returned None for a print against this quote."""
    if not bid or not ask or ask < bid or trade_at is None or quote_at is None:
        return "no_quote"
    lag = abs((quote_at - trade_at).total_seconds())
    if lag > stale_lag_sec:
        return "quote_too_old"
    return "inside_stale_quote" if lag > max_lag_sec else "at_midpoint"


def poll_capture_weight(gap_sec: Optional[float], period_sec: float) -> float:
    """The chance a poll of unknown phase sees a trade through latestTrade, which
    shows it only until the contract's next trade. `gap_sec` is the time until that
    trade, None when there was no later one."""
    if gap_sec is None:
        return 1.0
    return min(1.0, max(0.0, gap_sec) / period_sec)


class QuoteTape:
    """Each active contract's quotes as the poller saw them, for a while.

    Measured live on 19 Oct 2026: the snapshot's quote is real-time (a median
    1.5 seconds old) while its trade is the feed's 15-minute-delayed print (the
    quote a median 972 seconds after the trade, never within 60). Compared with
    the snapshot's own quote, no print could be read (0 of 40 that morning).
    Kept here as polled, the quote that stood when the print happened is on
    record by the time the print arrives."""

    def __init__(self, keep: timedelta = timedelta(minutes=25)) -> None:
        self.keep = keep
        self._tape: Dict[str, Tuple[List[datetime], List[Tuple[float, float]]]] = {}

    def record(self, contract: str, at: Optional[datetime], bid: Optional[float], ask: Optional[float]) -> None:
        if at is None or not bid or not ask or ask < bid:
            return
        times, quotes = self._tape.setdefault(contract, ([], []))
        if times and at <= times[-1]:
            return
        times.append(at)
        quotes.append((float(bid), float(ask)))

    def at(self, contract: str, when: datetime) -> Optional[Tuple[float, float, datetime]]:
        """(bid, ask, quote time) of the recorded quote nearest `when`."""
        times, quotes = self._tape.get(contract, ([], []))
        if not times:
            return None
        i = bisect.bisect_left(times, when)
        best = min((k for k in (i - 1, i) if 0 <= k < len(times)), key=lambda k: abs(times[k] - when))
        return quotes[best][0], quotes[best][1], times[best]

    def prune(self, now: datetime) -> None:
        cut = now - self.keep
        for c in list(self._tape):
            times, quotes = self._tape[c]
            k = bisect.bisect_left(times, cut)
            if k >= len(times):
                del self._tape[c]
            elif k:
                del times[:k]
                del quotes[:k]

    def __len__(self) -> int:
        return len(self._tape)


def opened(oi_before: Optional[int], oi_after: Optional[int], size: float) -> Optional[bool]:
    """Did the print open a position? None when either count is missing."""
    if oi_before is None or oi_after is None or size <= 0:
        return None
    return (oi_after - oi_before) >= OPENING_SHARE * size


__all__ = ["QuoteTape", "STALE_QUOTE_MAX_LAG_SEC", "INTRINSIC_HEAVY", "MIN_EXTRINSIC_USD", "PENDING_MAX_AGE_DAYS",
           "PENDING_OI_KEY", "aggressor", "extrinsic_premium", "intrinsic", "intrinsic_share", "opened",
           "poll_capture_weight", "side_miss_reason"]
