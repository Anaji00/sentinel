"""
shared/utils/adaptive_poll.py

Poll slower when polling is not producing anything.

`shared/utils/market_session.poll_interval` already slows a collector by the
*clock*: pre-market, after-hours, closed. That is right for a feed whose subject
is the equity session, and it is wrong for one whose subject merely correlates
with it. SEC filings are the case in point -- measured over seven days, by hour
UTC:

    hour 20        527      <- 4pm ET, the post-close 8-K and Form 4 surge
    hours 10-21    107-212
    hours 2-3      163, 30
    hours 0,1,4-9,22   ZERO, every day

Filings *peak* in the hour the equity calendar calls AFTER_HOURS, which carries
a 3x slowdown. Pacing that feed by the market session would back it off exactly
when it is busiest, and speed it up through the nine hours a day that have never
produced a filing.

So this paces on yield instead. A cycle that produces nothing is evidence that
the next one probably will not either; a cycle that produces something is
evidence that the next one might. The rule is geometric backoff on empty cycles
and immediate reset on any yield, which is self-tuning: it needs no timezone, no
holiday calendar and no knowledge of what the feed is about, and it follows a
publication schedule that changes without anyone editing a table.

    poll = AdaptivePoll(base_seconds=90, max_seconds=900)
    ...
    await asyncio.sleep(poll.after(new_filings))

The cap matters as much as the backoff. A feed that has not been polled for an
hour cannot notice that it broke, and the freshness tracker would rightly call
it stale -- so the interval is bounded by the same 900s ceiling
`market_session` uses, for the same reason.
"""

from __future__ import annotations

from typing import Optional

# The longest any feed is left unpolled.
#
# Deliberately the same ceiling as MAX_IDLE_POLL_SEC in market_session: a
# collector that sleeps longer than this stops being able to distinguish "quiet"
# from "broken", which is the failure both mechanisms are bounded to avoid.
DEFAULT_MAX_POLL_SEC = 900.0

# How fast the interval grows per empty cycle. 2.0 reaches the ceiling quickly
# enough to matter overnight and slowly enough that a single empty cycle in a
# busy hour costs almost nothing.
DEFAULT_GROWTH = 2.0


class AdaptivePoll:
    """A poll interval that lengthens while a feed yields nothing.

    Stateful and deliberately simple: one counter and two numbers. It makes no
    network calls, reads no clock beyond what the caller passes, and has no
    opinion about what the feed contains -- so the same object paces an SEC
    firehose and a social scraper without either needing to explain itself.
    """

    def __init__(
        self,
        base_seconds: float,
        max_seconds: float = DEFAULT_MAX_POLL_SEC,
        growth: float = DEFAULT_GROWTH,
    ):
        self.base = max(1.0, float(base_seconds))
        self.max = max(self.base, float(max_seconds))
        self.growth = max(1.0, float(growth))
        self.empty_cycles = 0

    def after(self, yielded: Optional[int]) -> float:
        """The seconds to sleep, given what the cycle just produced.

        `yielded` is whatever the caller counts as a result -- filings, posts,
        quotes. None is treated as empty rather than as unknown: a cycle that
        cannot say what it produced is not evidence that it produced something.
        """
        try:
            produced = int(yielded or 0)
        except (TypeError, ValueError):
            produced = 0

        if produced > 0:
            # One result is enough. Backing off gradually after a hit would
            # keep a feed slow through the beginning of its busy period, which
            # is the half hour that matters most on a publication schedule.
            self.empty_cycles = 0
            return self.base

        self.empty_cycles += 1
        interval = self.base * (self.growth ** self.empty_cycles)
        return min(self.max, interval)

    @property
    def current(self) -> float:
        """The interval the last call to `after` would have returned."""
        if self.empty_cycles <= 0:
            return self.base
        return min(self.max, self.base * (self.growth ** self.empty_cycles))

    def snapshot(self) -> dict:
        """For a heartbeat, so a slow feed is visibly slow rather than silent."""
        return {
            "base_seconds": self.base,
            "interval_seconds": round(self.current, 1),
            "empty_cycles": self.empty_cycles,
            "backed_off": self.empty_cycles > 0,
        }


__all__ = ["DEFAULT_GROWTH", "DEFAULT_MAX_POLL_SEC", "AdaptivePoll"]
