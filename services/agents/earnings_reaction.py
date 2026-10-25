"""
services/agents/earnings_reaction.py

Post-earnings drift as an event family the reaction study can prove or reject.

For every earnings release (8-K Item 2.02, stamped at EDGAR acceptance) the
reaction is the stock's move against SPY from the last close before the release
to two closes later. A reaction of at least EARNINGS_REACTION_MIN becomes an
`earnings_reaction` event, stamped at that second close -- the moment it was
knowable -- carrying the reaction's sign. The study then measures whether
prices kept moving the same way over the next day and the next five (Chan,
Jegadeesh & Lakonishok 1996; Brandt, Kishore, Santa-Clara & Venkatachalam 2008:
the drift follows the market's own reaction to the announcement).

History is written once into the permanent archive; afterwards, each day's new
reactions go to both the archive and the live events table, so a proven family
can raise live signals.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("agent.earnings_reaction")

EARNINGS_REACTION_MIN = 0.02
HISTORY_DAYS = 5 * 365 + 30
RECENT_DAYS = 10
TICKER_BATCH = 200
INTERVAL_SEC = 6 * 3600
FIRST_RUN_DELAY_SEC = 180
# v2: history rebuilt once more so every archived reaction carries its size.
DONE_KEY = "sentinel:earnings_reaction:history_done:v2"
# Large-cap earnings drift has been absent since about 2006 (Martineau, "Rest in
# Peace Post-Earnings Announcement Drift", 2022) and this platform's study finds
# strong reactions reversing over five days. Each reaction is labelled by the
# stock's average daily dollar volume over the 20 sessions before the release,
# a point-in-time size measure, so the study measures small and large apart.
LARGE_DOLLAR_VOLUME = 50e6
SIZE_LOOKBACK = 20
_NY = ZoneInfo("America/New_York")

_RELEASES_SQL = """
    SELECT primary_entity_id AS ticker, occurred_at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND occurred_at > $1 AND primary_entity_id IS NOT NULL
"""
_BARS_SQL = """
    SELECT ticker, day, close, volume FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date
    ORDER BY ticker, day
"""
_ARCHIVE_SQL = """
    INSERT INTO signal_event_archive (event_id, type, source, created_at, occurred_at, headline, summary,
                                      tags, sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'earnings_reaction', 'sentinel_earnings_reaction', $2, $2, $3, $4, $5, NULL, $6, $7::jsonb, 'US')
    ON CONFLICT (event_id, created_at) DO UPDATE SET headline = EXCLUDED.headline, summary = EXCLUDED.summary,
        tags = EXCLUDED.tags, financial_data = EXCLUDED.financial_data
"""
_EVENTS_SQL = """
    INSERT INTO events (event_id, type, source, occurred_at, collected_at, created_at, headline, summary, tags,
                        primary_entity_id, financial_data, anomaly_score, region)
    VALUES ($1::uuid, 'earnings_reaction', 'sentinel_earnings_reaction', $2, NOW(), $2, $3, $4, $5, $6,
            $7::jsonb, $8, 'US')
    ON CONFLICT (event_id, occurred_at) DO UPDATE SET headline = EXCLUDED.headline, summary = EXCLUDED.summary,
        tags = EXCLUDED.tags, financial_data = EXCLUDED.financial_data, anomaly_score = EXCLUDED.anomaly_score
"""


def _close_ts(d: date) -> datetime:
    return datetime.combine(d, time(16, 0), tzinfo=_NY).astimezone(timezone.utc)


def reactions(releases: Sequence[Tuple[str, Any]], bars: Dict[str, Tuple[List[date], List[float]]],
              spy: Tuple[List[date], List[float]], min_abs: float = EARNINGS_REACTION_MIN,
              as_of: Optional[datetime] = None,
              volumes: Optional[Dict[str, List[float]]] = None) -> List[Dict[str, Any]]:
    """(ticker, accepted_at) releases -> reaction events, market-adjusted, point in time.

    `as_of` drops reactions whose second close has not happened yet: the daily
    table carries today's bar during the session, and its last price is not a
    close."""
    from services.agents.factor_engine import base_row

    spy_by = dict(zip(*spy))
    out: List[Dict[str, Any]] = []
    seen = set()
    for t, accepted in releases:
        days, closes = bars.get(t, ([], []))
        if len(days) < 5:
            continue
        b = base_row(days, accepted)
        e = b + 2
        if b < 0 or e >= len(days):
            continue
        if as_of is not None and _close_ts(days[e]) > as_of:
            continue
        s0, s1 = spy_by.get(days[b]), spy_by.get(days[e])
        if not s0 or not s1 or not closes[b]:
            continue
        ear = (closes[e] / closes[b] - 1.0) - (s1 / s0 - 1.0)
        if abs(ear) < min_abs or (t, days[e]) in seen:
            continue
        seen.add((t, days[e]))
        dv = None
        vols = (volumes or {}).get(t)
        if vols and b >= SIZE_LOOKBACK:
            pairs = [closes[j] * vols[j] for j in range(b - SIZE_LOOKBACK + 1, b + 1) if vols[j] and closes[j]]
            dv = sum(pairs) / len(pairs) if pairs else None
        out.append({"ticker": t, "known_at": _close_ts(days[e]), "ear": ear, "release_day": str(days[b + 1]),
                    "dollar_volume_20d": dv})
    return out


def _row(r: Dict[str, Any]) -> Tuple:
    eid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"earnings-reaction:{r['ticker']}:{r['known_at'].date()}"))
    side = "up" if r["ear"] > 0 else "down"
    headline = f"{r['ticker']} earnings reaction {r['ear'] * 100:+.1f}% vs SPY"
    summary = (f"{r['ticker']} moved {r['ear'] * 100:+.1f}% against SPY over the two sessions after its earnings "
               f"release; post-earnings drift tends to follow the reaction's direction.")
    tags = ["earnings_reaction", f"earnings_reaction_{side}", f"exposes:{r['ticker']}"]
    dv = r.get("dollar_volume_20d")
    if dv is not None:
        tags.append("size:large" if dv >= LARGE_DOLLAR_VOLUME else "size:small")
    # A dict: the pool's jsonb codec encodes it. json.dumps here stored a JSON
    # *string* that SQL's ->> could not read (fixed 10 Oct 2026, migration 0052).
    fd = {"ticker": r["ticker"], "ear": round(r["ear"], 5), "release_day": str(r["release_day"])}
    if dv is not None:
        fd["dollar_volume_20d"] = round(dv)
    return eid, r["known_at"], headline, summary, tags, r["ticker"], fd


class EarningsReactionEngine:
    def __init__(self, db: Any, redis_client: Any):
        self.db = db
        self.raw = getattr(redis_client, "raw", redis_client)

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                logger.info("Earnings reactions: %s", await self.run_once())
            except Exception as exc:
                logger.error("Earnings reaction run failed: %s", exc, exc_info=True)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, int]:
        history = not await self.raw.exists(DONE_KEY)
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=HISTORY_DAYS if history else RECENT_DAYS + 10)
        rel = [(str(r["ticker"]).upper(), r["occurred_at"]) for r in await self.db.query(_RELEASES_SQL, since) or []]
        if not rel:
            return {"releases": 0}
        bar_since = (since - timedelta(days=10)).date()
        spy: Tuple[List[date], List[float]] = ([], [])
        for r in await self.db.query(_BARS_SQL, ["SPY"], bar_since) or []:
            spy[0].append(r["day"])
            spy[1].append(float(r["close"]))
        by_ticker: Dict[str, List[Tuple[str, Any]]] = defaultdict(list)
        for t, at in rel:
            by_ticker[t].append((t, at))
        tickers = sorted(by_ticker)
        archived = live = 0
        recent_cut = now - timedelta(days=RECENT_DAYS)
        for k in range(0, len(tickers), TICKER_BATCH):
            chunk = tickers[k:k + TICKER_BATCH]
            bars: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
            vols: Dict[str, List[float]] = defaultdict(list)
            for r in await self.db.query(_BARS_SQL, chunk, bar_since) or []:
                d, c = bars[str(r["ticker"]).upper()]
                d.append(r["day"])
                c.append(float(r["close"]))
                vols[str(r["ticker"]).upper()].append(float(r.get("volume") or 0.0))
            evs = reactions([x for t in chunk for x in by_ticker[t]], bars, spy, as_of=now, volumes=vols)
            rows = [_row(e) for e in evs]
            if rows:
                await self.db.execute_many(_ARCHIVE_SQL, rows)
                archived += len(rows)
            fresh = [(*row, min(1.0, abs(e["ear"]) * 10)) for row, e in zip(rows, evs) if e["known_at"] > recent_cut]
            if fresh:
                await self.db.execute_many(_EVENTS_SQL, fresh)
                live += len(fresh)
        if history:
            await self.raw.set(DONE_KEY, "1")
        return {"releases": len(rel), "reactions_archived": archived, "live": live, "history_pass": int(history)}


__all__ = ["EARNINGS_REACTION_MIN", "EarningsReactionEngine", "reactions"]
