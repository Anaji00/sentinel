"""
services/agents/signal_engine.py

Trading signals that have earned the right to exist.

Two loops:

  The study (every six hours). Reads the last STUDY_LOOKBACK_DAYS of events,
  turns them into signal-family episodes (shared/utils/signal_families.py),
  measures what each episode's instruments did afterwards from the price bars,
  and grades every family and horizon (shared/utils/reaction_study.py). The full
  table -- proven, unproven and why -- is stored for the API, so the reasoning
  is visible rather than asserted.

  The live loop (every minute). New events become observations. An observation
  counts only if its family and horizon are `proven`. Proven observations on one
  instrument are combined; a signal is raised only when every proven family
  agrees on direction, the instrument has a live price, and the combined
  probability clears MIN_SIGNAL_PROBABILITY. Each signal is stored as a
  `trade_signal` event, and when it expires its outcome is measured from the
  bars and recorded, so the platform's own track record is scored too.

Nothing unmeasured, unproven or contradicted can raise a signal. Halted by the
`trade_signals` feature flag or the platform's master kill switch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from array import array
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.kafka import SentinelProducer, Topics
from shared.models import NormalizedEvent, RawEvent
from shared.models.events import EventType
from shared.utils.confidence_calibration import record_outcome
from shared.utils.feature_flags import FeatureFlagManager
from shared.utils.market_session import Session, instrument_closed_at, session_label_at
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.short_data import HARD_TO_BORROW_FEE
from shared.utils.reaction_study import (
    HORIZONS, SIZE_BENCHMARKS, CellResult, Series, combine, cost_bps, evaluate_cell, finalize, size_benchmark,
    trailing_dollar_volume,
    measure, pre_move, scaled_horizon_stats, shrunk_hit_rate,
)
from shared.utils.signal_families import (
    FAMILY_DESCRIPTIONS, PRICE_DIRECTED, episodes, observations,
)
from shared.utils.signal_store import (
    ACTIVE_KEY, AWAITING_KEY, CURSOR_KEY, EVIDENCE_KEY, MAX_NEEDS_BARS, NEEDS_BARS_KEY, OUTCOMES_KEY, STUDY_KEY,
)
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("agent.signal_engine")


STUDY_INTERVAL_SEC = 6 * 3600
LIVE_INTERVAL_SEC = 60
# The study reads the permanent archive (signal_event_archive), so its window
# is no longer capped by the 90-day retention on events; it deepens as history
# accrues, up to this.
STUDY_LOOKBACK_DAYS = int(os.getenv("SIGNAL_STUDY_LOOKBACK_DAYS", "365"))
# Event types with years of point-in-time history in the archive (EDGAR 8-Ks,
# stamped at acceptance), studied that far back at the daily horizons.
HISTORY_LOOKBACK_DAYS = int(os.getenv("SIGNAL_STUDY_HISTORY_DAYS", str(5 * 365 + 30)))
DEEP_HISTORY_TYPES = ["filing", "earnings_reaction", "insider_trade", "thirteen_f"]
ARCHIVE_INTERVAL_SEC = 3600
# Tape families (volume spikes, blocks) touch ~1,500 tickers with a few
# episodes each. Every instrument any other family points at is studied; the
# tape families share this many of their most frequent.
MAX_TAPE_INSTRUMENTS = 300
MIN_SIGNAL_PROBABILITY = 0.55
# A live price older than this makes an instrument untradeable right now.
MAX_PRICE_AGE = timedelta(days=3)
SIGNAL_SOURCE = "signal_engine"
OUTCOMES_KEPT = 2000
# How long an expired signal may wait for its exit price. A signal expiring
# after Friday's close is first priced at Monday's open; a long weekend adds a
# day. Past this the instrument has stopped printing and the signal is closed
# unpriced.
GRADE_GRACE = timedelta(days=5)

SIGNAL_EVENT_TYPES = [
    "headline", "social_signal", "vessel_dark", "vessel_sts", "supply_chain_metric",
    "filing", "regulatory_event", "insider_cluster", "earnings_surprise", "options_flow",
    "market_anomaly", "equity_block", "thirteen_f", "earnings_reaction", "insider_trade",
]

_EVENTS_SQL = """
    SELECT event_id::text AS event_id, type, source, created_at, occurred_at, headline,
           summary, tags, sentiment, primary_entity_id, financial_data, region
    FROM events
    WHERE created_at > $1 AND created_at <= $2
      AND type = ANY($3::text[])
      AND NOT (type = 'market_anomaly' AND source NOT IN ('alpaca_quant_radar', 'finra_short_volume'))
      AND NOT (type = 'options_flow'
               AND COALESCE((financial_data->>'premium_usd')::float, 0) < 50000)
"""

# The study's reads, from the archive rather than events.
_STUDY_EVENTS_SQL = _EVENTS_SQL.replace("FROM events", "FROM signal_event_archive")

# Copies the newest signal events into the archive. Idempotent: the archive's
# key is (event_id, created_at), so a re-copy is a no-op.
_ARCHIVE_TOPUP_SQL = """
    INSERT INTO signal_event_archive
    SELECT event_id::text, type, source, created_at, occurred_at, headline, summary,
           tags, sentiment, primary_entity_id, financial_data, region
    FROM events
    WHERE created_at > $1
      AND type = ANY($2::text[])
      AND NOT (type = 'market_anomaly' AND source NOT IN ('alpaca_quant_radar', 'finra_short_volume'))
      AND NOT (type = 'options_flow'
               AND COALESCE((financial_data->>'premium_usd')::float, 0) < 50000)
    ON CONFLICT DO NOTHING
"""

_SERIES_SQL = """
    SELECT extract(epoch FROM time_bucket('5 minutes', time))::float8 AS t,
           last(close, time) AS c
    FROM tradfi_bars
    WHERE ticker = $1 AND time > $2
    GROUP BY 1 ORDER BY 1
"""


# Adjusted daily closes from the permanent daily_bars table, for the stretch
# before the intraday series begins.
_DAILY_SQL = """
    SELECT day, close FROM daily_bars
    WHERE ticker = $1 AND day >= $2::date AND close > 0
    ORDER BY day
"""
_DOLLAR_VOLUME_SQL = """
    SELECT day, close * volume AS dv FROM daily_bars
    WHERE ticker = $1 AND day >= $2::date AND close > 0 AND volume > 0
    ORDER BY day
"""

try:
    from zoneinfo import ZoneInfo
    _NY = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    _NY = None


def _close_ts(day: Any) -> float:
    """The epoch of 16:00 New York on `day`: the moment the daily close existed.

    Stamped at the true close, not a fixed UTC hour, so an event at 16:30 ET is
    never entered at that day's 16:00 close -- which would be a price from
    before the event, and a reaction it could not have traded."""
    if _NY is not None:
        return datetime(day.year, day.month, day.day, 16, 0, tzinfo=_NY).timestamp()
    return datetime(day.year, day.month, day.day, 21, 0, tzinfo=timezone.utc).timestamp()


def merge_daily(daily: Optional[Series], intraday: Optional[Series]) -> Optional[Series]:
    """Daily closes strictly before the first intraday bar, then the intraday bars."""
    if not daily or not daily[0]:
        return intraday
    if not intraday or not intraday[0]:
        return daily
    first = intraday[0][0]
    times, closes = array("d"), array("d")
    for t, c in zip(*daily):
        if t < first:
            times.append(t)
            closes.append(c)
    times.extend(intraday[0])
    closes.extend(intraday[1])
    return (times, closes)


def _row(r: Any) -> Dict[str, Any]:
    d = dict(r)
    fd = d.get("financial_data")
    if isinstance(fd, str):
        try:
            d["financial_data"] = json.loads(fd)
        except ValueError:
            d["financial_data"] = {}
    return d


def _ts(v: Any) -> Optional[float]:
    if isinstance(v, datetime):
        return (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).timestamp()
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class SignalEngine:
    """Scheduled, not reactive: the reaction study and the live signal loop.

    Deliberately not a Kafka-consuming agent. It acts on no message -- it reads
    the events table on its own clock -- so a subscription would deserialise a
    topic only to discard it.
    """

    def __init__(self, db_client: Any, redis_client: Any) -> None:
        self.db = db_client
        self.redis = redis_client
        self.flags = FeatureFlagManager(redis_client)
        self._cells: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._producer: Optional[SentinelProducer] = None

    async def run(self) -> None:
        self._producer = SentinelProducer(service_name="signal-engine")
        await self._producer.start()
        safe_create_task(self._archive_loop(), name="signal-archive")
        study = safe_create_task(self._study_loop(), name="signal-study")
        live = safe_create_task(self._live_loop(), name="signal-live")
        try:
            await asyncio.gather(study, live)
        finally:
            study.cancel()
            live.cancel()
            await self._producer.close()

    # -- prices ---------------------------------------------------------------

    async def _series(self, instrument: str, since: datetime) -> Optional[Series]:
        rows = await self.db.query(_SERIES_SQL, instrument, since)
        if not rows:
            return None
        times, closes = array("d"), array("d")
        for r in rows:
            c = r.get("c")
            if c is None:
                continue
            # Bars stamped while the market was shut are replays of the last
            # print; measuring a reaction against them measures nothing.
            t = float(r["t"])
            if instrument_closed_at(instrument, datetime.fromtimestamp(t, timezone.utc)):
                continue
            times.append(t)
            closes.append(float(c))
        return (times, closes) if times else None

    async def _daily_series(self, instrument: str, since: datetime) -> Optional[Series]:
        try:
            rows = await self.db.query(_DAILY_SQL, instrument, since.date())
        except Exception as exc:
            swallowed("agents.signal_engine.daily_series", exc, logger)
            return None
        times, closes = array("d"), array("d")
        for r in rows or []:
            try:
                times.append(_close_ts(r["day"]))
                closes.append(float(r["close"]))
            except (TypeError, ValueError, KeyError, AttributeError):
                continue
        return (times, closes) if times else None

    async def _dollar_volume(self, instrument: str, since: datetime) -> Optional[Series]:
        try:
            rows = await self.db.query(_DOLLAR_VOLUME_SQL, instrument, since.date())
        except Exception as exc:
            swallowed("agents.signal_engine.dollar_volume", exc, logger)
            return None
        times, values = array("d"), array("d")
        for r in rows or []:
            times.append(_close_ts(r["day"]))
            values.append(float(r["dv"]))
        return (times, values) if times else None

    # -- the study ------------------------------------------------------------

    async def _archive_recent(self, hours: int = 72) -> None:
        """Copy the last few days of signal events into the permanent archive."""
        try:
            since = datetime.now(timezone.utc) - timedelta(hours=hours)
            await self.db.execute(_ARCHIVE_TOPUP_SQL, since, SIGNAL_EVENT_TYPES)
        except Exception as exc:
            swallowed("agents.signal_engine.archive", exc, logger)

    async def _archive_loop(self):
        while True:
            await self._archive_recent()
            await asyncio.sleep(ARCHIVE_INTERVAL_SEC)

    async def _study_loop(self):
        await asyncio.sleep(30)
        while True:
            try:
                summary = await self.run_study()
                logger.info("Reaction study: %s", summary)
            except Exception as exc:
                logger.error("Reaction study failed: %s", exc, exc_info=True)
            await asyncio.sleep(STUDY_INTERVAL_SEC)

    async def _load_observations(self, start: datetime, end: datetime,
                                 types: Optional[List[str]] = None, step_days: int = 1) -> list:
        obs = []
        day = start
        while day < end:
            nxt = min(end, day + timedelta(days=step_days))
            rows = await self.db.query(_STUDY_EVENTS_SQL, day, nxt, types or SIGNAL_EVENT_TYPES)
            for r in rows:
                obs.extend(observations(_row(r)))
            day = nxt
        return obs

    async def run_study(self) -> Dict[str, Any]:
        await self._archive_recent()
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=STUDY_LOOKBACK_DAYS)
        obs = await self._load_observations(since, now)
        # The EDGAR history is selected by acceptance time, so the one-year
        # window read one year of the five on record; the rest never became an
        # episode. Filings before the window are read here, measured at the
        # daily horizons on daily closes.
        deep_since = now - timedelta(days=HISTORY_LOOKBACK_DAYS)
        if deep_since < since:
            obs += await self._load_observations(deep_since, since, DEEP_HISTORY_TYPES, step_days=30)
        eps = episodes(obs)
        per_family: Dict[str, int] = {}
        by_inst: Dict[str, list] = {}
        for e in eps:
            per_family[e.family] = per_family.get(e.family, 0) + 1
            by_inst.setdefault(e.instrument, []).append(e)

        # Daily prices from the earliest episode, not from the lookback: the
        # archive is selected by when a row was written, and EDGAR history
        # written today reaches back five years. Read from `since`, every
        # filing older than a year found no price series and was dropped from
        # the measurement without a word -- most 8-K families sat at
        # "insufficient data" with thousands of filings on record.
        daily_since = min([since] + [e.known_at for e in eps]) - timedelta(days=7)
        bench_intraday = await self._series("SPY", since - timedelta(days=1))
        bench_merged = merge_daily(await self._daily_series("SPY", daily_since), bench_intraday)
        # Size-matched indexes for the daily horizons (reaction_study.size_benchmark).
        size_bench: Dict[str, Optional[Series]] = {"SPY": bench_merged}
        for _floor, etf in SIZE_BENCHMARKS:
            if etf not in size_bench:
                size_bench[etf] = merge_daily(await self._daily_series(etf, daily_since),
                                              await self._series(etf, since - timedelta(days=1)))
        measured: Dict[Tuple[str, str], list] = {}
        intel = {inst for inst, es in by_inst.items() if any(e.family not in PRICE_DIRECTED for e in es)}
        tape_only = sorted(
            (inst for inst in by_inst if inst not in intel), key=lambda i: -len(by_inst[i]),
        )[:MAX_TAPE_INSTRUMENTS]
        ranked = [(i, by_inst[i]) for i in sorted(intel)] + [(i, by_inst[i]) for i in tape_only]
        skipped_instruments = len(by_inst) - len(ranked)
        needs_bars: List[Tuple[str, int]] = []
        for inst, inst_eps in ranked:
            intraday = await self._series(inst, since - timedelta(days=1))
            # Years of adjusted daily closes ahead of the intraday bars, so the
            # 1d and 5d horizons can measure episodes from before minute
            # history began. The 1h horizon never reads daily points: its
            # volatility and its windows come from the intraday series alone.
            first = min(e.known_at for e in inst_eps)
            series = merge_daily(await self._daily_series(inst, min(since, first) - timedelta(days=7)), intraday)
            if series is None:
                if inst in intel and not inst.endswith("=F"):
                    needs_bars.append((inst, len(inst_eps)))
                continue
            marks = [e.known_at.timestamp() for e in inst_eps]
            dv = await self._dollar_volume(inst, min(since, first) - timedelta(days=40))
            matched = {e.known_at: size_bench.get(size_benchmark(trailing_dollar_volume(dv, e.known_at.timestamp())))
                       or bench_merged for e in inst_eps}
            for hname, h in HORIZONS.items():
                use = series if h >= timedelta(days=1) else intraday
                if use is None:
                    continue
                stats = scaled_horizon_stats(use, h, marks)
                for e in inst_eps:
                    bench = matched[e.known_at] if h >= timedelta(days=1) else bench_intraday
                    m = measure(e, h, use, bench, stats)
                    if m is not None:
                        measured.setdefault((e.family, hname), []).append(m)
            del series, intraday

        cells: List[CellResult] = []
        for fam, n in per_family.items():
            for hname in HORIZONS:
                cells.append(evaluate_cell(fam, hname, measured.get((fam, hname), []), n))
        finalize(cells)

        table = [c.as_dict() for c in cells]
        for row in table:
            row["description"] = FAMILY_DESCRIPTIONS.get(row["family"], row["family"])
        verdicts: Dict[str, int] = {}
        for c in cells:
            verdicts[c.verdict] = verdicts.get(c.verdict, 0) + 1
        payload = {
            "generated_at": now.isoformat(),
            "lookback_days": STUDY_LOOKBACK_DAYS,
            "history_lookback_days": {t: HISTORY_LOOKBACK_DAYS for t in DEEP_HISTORY_TYPES},
            "episodes": len(eps),
            "instruments": len(by_inst),
            "instruments_not_studied": skipped_instruments,
            "instruments_without_bars": len(needs_bars),
            "verdicts": verdicts,
            "cells": table,
        }
        await self.redis.raw.set(STUDY_KEY, json.dumps(payload, default=str))
        # Ask the market collector for the history these families point at.
        wanted = [i for i, _n in sorted(needs_bars, key=lambda x: -x[1])[:MAX_NEEDS_BARS]]
        await self.redis.raw.delete(NEEDS_BARS_KEY)
        if wanted:
            await self.redis.raw.sadd(NEEDS_BARS_KEY, *wanted)
        self._cells = {(c.family, c.horizon): c.as_dict() for c in cells}
        return {"episodes": len(eps), "cells": len(cells), "verdicts": verdicts}

    async def _proven(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        if not self._cells:
            blob = await self.redis.raw.get(STUDY_KEY)
            if blob:
                for c in json.loads(blob).get("cells", []):
                    self._cells[(c["family"], c["horizon"])] = c
        return {k: v for k, v in self._cells.items() if v.get("verdict") == "proven"}

    # -- the live loop --------------------------------------------------------

    async def _live_loop(self):
        await asyncio.sleep(45)
        while True:
            try:
                await self.live_tick()
                await self.expire_signals()
            except Exception as exc:
                logger.error("Signal live loop failed: %s", exc, exc_info=True)
            await asyncio.sleep(LIVE_INTERVAL_SEC)

    async def live_tick(self) -> int:
        raw = self.redis.raw
        now = datetime.now(timezone.utc)
        cursor_blob = await raw.get(CURSOR_KEY)
        cursor = (
            datetime.fromisoformat(cursor_blob.decode() if isinstance(cursor_blob, bytes) else cursor_blob)
            if cursor_blob else now - timedelta(minutes=10)
        )
        rows = await self.db.query(_EVENTS_SQL, cursor, now, SIGNAL_EVENT_TYPES)
        await raw.set(CURSOR_KEY, now.isoformat())
        proven = await self._proven()
        if not rows or not proven:
            return 0

        touched = set()
        recent_cache: Dict[str, Optional[Series]] = {}
        for r in rows:
            row = _row(r)
            for o in observations(row):
                for hname in HORIZONS:
                    cell = proven.get((o.family, hname))
                    if not cell:
                        continue
                    if o.instrument not in recent_cache:
                        recent_cache[o.instrument] = await self._series(o.instrument, now - timedelta(days=4))
                    recent = recent_cache[o.instrument]
                    if recent is None or now.timestamp() - recent[0][-1] > MAX_PRICE_AGE.total_seconds():
                        continue  # no live price: not tradeable now
                    direction = o.direction
                    if o.family in PRICE_DIRECTED:
                        pm = pre_move(recent, o.known_at)
                        if pm is None or abs(pm) < 0.001:
                            continue
                        direction = (1 if pm > 0 else -1) * int(cell.get("sign") or 1)
                    if direction == 0:
                        continue
                    evidence = {
                        "family": o.family, "direction": direction, "event_id": o.event_id,
                        "headline": (row.get("headline") or "")[:200],
                        "hit": shrunk_hit_rate(float(cell["hit_rate"]), int(cell["units"])),
                        "mean_bps": cell.get("mean_bps"), "units": cell.get("units"),
                        "q": cell.get("q"), "at": o.known_at.isoformat(),
                    }
                    key = EVIDENCE_KEY.format(instrument=o.instrument, horizon=hname)
                    await raw.zadd(key, {json.dumps(evidence): o.known_at.timestamp()})
                    await raw.zremrangebyscore(key, 0, now.timestamp() - HORIZONS[hname].total_seconds())
                    await raw.expire(key, int(HORIZONS[hname].total_seconds()) + 3600)
                    touched.add((o.instrument, hname))

        raised = 0
        for inst, hname in touched:
            raised += await self._evaluate(inst, hname, now)
        return raised

    async def _evaluate(self, instrument: str, hname: str, now: datetime) -> int:
        if not await self.flags.is_enabled("trade_signals", ticker=instrument):
            return 0
        raw = self.redis.raw
        items = [json.loads(x) for x in await raw.zrange(EVIDENCE_KEY.format(instrument=instrument, horizon=hname), 0, -1)]
        latest: Dict[str, Dict[str, Any]] = {}
        for ev in items:
            latest[ev["family"]] = ev
        directions = {ev["direction"] for ev in latest.values()}
        if len(directions) != 1:
            return 0  # proven families disagree: no signal
        direction = directions.pop()
        prob = combine([ev["hit"] for ev in latest.values()])
        if prob < MIN_SIGNAL_PROBABILITY:
            return 0
        signal_id = f"{instrument}:{hname}:{'long' if direction > 0 else 'short'}"
        if hname == "1h" and (session_label_at(instrument, now) != Session.REGULAR.name
                              or session_label_at(instrument, now + HORIZONS[hname]) != Session.REGULAR.name):
            # An hour's signal outside the hour it could be traded in: 102 of
            # 130 graded outcomes to 19 Oct 2026 were "no price at expiry",
            # most of them 1h signals raised after the close.
            dropped("agents.signal_engine.outside_session", f"{signal_id} at {now.isoformat()}", logger)
            return 0
        existing = await raw.hget(ACTIVE_KEY, signal_id)
        if existing:
            sig = json.loads(existing)
            sig["evidence"] = list(latest.values())
            sig["probability"] = round(prob, 3)
            await raw.hset(ACTIVE_KEY, signal_id, json.dumps(sig))
            return 0
        cost = cost_bps(instrument)
        if direction < 0:
            # A short pays its borrow over the horizon, and a name too costly to
            # borrow is not shorted at all: the filing families' edge sits
            # mostly in small issuers, where borrow is dearest.
            fee = await self._borrow_fee(instrument)
            if fee is not None and fee > HARD_TO_BORROW_FEE:
                dropped("agents.signal_engine.hard_to_borrow", f"{instrument}: borrow {fee:.1f}%/yr", logger)
                return 0
            cost += borrow_cost_bps(fee, HORIZONS[hname])
        recent = await self._series(instrument, now - timedelta(days=4))
        entry_price = float(recent[1][-1]) if recent else None
        spy_recent = await self._series("SPY", now - timedelta(days=4))
        expected_bps = round(sum(float(ev.get("mean_bps") or 0) for ev in latest.values()), 1)
        sig = {
            "id": signal_id, "instrument": instrument, "horizon": hname,
            "direction": "long" if direction > 0 else "short",
            "probability": round(prob, 3), "expected_bps": expected_bps,
            "cost_bps": round(cost, 1),
            "created_at": now.isoformat(),
            "expires_at": (now + HORIZONS[hname]).isoformat(),
            "entry_price": entry_price,
            # Graded net of the market, as the study judges the families.
            "spy_entry": float(spy_recent[1][-1]) if spy_recent else None,
            "evidence": list(latest.values()),
            "families": sorted(latest),
        }
        await raw.hset(ACTIVE_KEY, signal_id, json.dumps(sig))
        await self._publish(sig)
        logger.info("Trade signal %s p=%.2f from %s", signal_id, prob, ", ".join(sig["families"]))
        return 1

    async def _publish(self, sig: Dict[str, Any]) -> None:
        fams = ", ".join(sig["families"])
        side = "LONG" if sig["direction"] == "long" else "SHORT"
        event_type = EventType.TRADE_SIGNAL
        event = NormalizedEvent.model_validate({
            "type": event_type,
            "occurred_at": sig["created_at"],
            "source": SIGNAL_SOURCE,
            "primary_entity": {"id": sig["instrument"], "type": "instrument", "name": sig["instrument"]},
            "headline": (
                f"{sig['instrument']} {'leans up' if sig['direction'] == 'long' else 'leans down'} over the next "
                f"{sig['horizon']} ({sig['probability']:.0%} likely), from {fams.replace('_', ' ')}"
            ),
            "summary": (
                f"Raised because {len(sig['families'])} proven signal famil"
                f"{'y' if len(sig['families']) == 1 else 'ies'} agree ({fams}). Expected move "
                f"{sig['expected_bps']} bps against {sig['cost_bps']} bps of costs. Every family "
                "behind it passed the reaction study: enough independent episodes, false-discovery "
                "control, a held-out confirmation and the cost hurdle."
            ),
            "financial_data": {"ticker": sig["instrument"], "side": "BUY" if side == "LONG" else "SELL"},
            "tags": ["trade_signal", f"exposes:{sig['instrument']}", f"horizon:{sig['horizon']}",
                     f"direction:{sig['direction']}"] + [f"family:{f}" for f in sig["families"]],
            "anomaly_score": sig["probability"],
        })
        await self._producer.send(
            Topics.RAW_TRADFI,
            RawEvent(source=SIGNAL_SOURCE, occurred_at=event.occurred_at,
                     raw_payload={"normalized_event": event.model_dump(mode="json")}).model_dump(mode="json"),
            key=sig["instrument"],
        )

    async def _borrow_fee(self, instrument: str) -> Optional[float]:
        """The latest quoted annual borrow fee in percent, or None."""
        try:
            row = await self.db.query_one(_FEE_SQL, instrument.upper())
            return float(row["fee_rate"]) if row and row.get("fee_rate") is not None else None
        except Exception as exc:
            swallowed("agents.signal_engine.borrow_fee", exc, logger)
            return None

    async def expire_signals(self) -> int:
        """Take expired signals off the board, then grade every one whose exit
        price now exists.

        Grading used to happen on the first pass after expiry, which runs within
        a minute of it. The exit price is the first bar stamped at or after the
        expiry, and bars are stamped at the start of their five minutes, so that
        bar could not exist yet: every signal closed "no price at expiry" and the
        calibration behind the probabilities never received an outcome. Expired
        signals now wait in AWAITING_KEY until the bar exists, or GRADE_GRACE
        passes."""
        raw = self.redis.raw
        now = datetime.now(timezone.utc)
        for sid, blob in (await raw.hgetall(ACTIVE_KEY) or {}).items():
            sig = json.loads(blob)
            if _ts(sig["expires_at"]) > now.timestamp():
                continue
            # Keyed by creation too: a fresh signal with the same id may expire
            # while this one is still waiting.
            await raw.hset(AWAITING_KEY, f"{sid}@{sig['created_at']}", blob)
            await raw.hdel(ACTIVE_KEY, sid)
        closed = 0
        for wid, blob in (await raw.hgetall(AWAITING_KEY) or {}).items():
            sig = json.loads(blob)
            exp = _ts(sig["expires_at"])
            series = await self._series(sig["instrument"], datetime.fromtimestamp(exp, timezone.utc) - timedelta(hours=1))
            exit_px = exit_price(series, exp)
            if exit_px is None and now.timestamp() - exp < GRADE_GRACE.total_seconds():
                continue
            spy_exit = exit_price(await self._series("SPY", datetime.fromtimestamp(exp, timezone.utc) - timedelta(hours=1)),
                                  exp) if sig.get("spy_entry") else None
            outcome = grade(sig, exit_px, spy_exit)
            if outcome.get("hit") is not None:
                try:
                    await record_outcome(self.redis, float(sig["probability"]), bool(outcome["hit"]))
                except Exception as exc:
                    swallowed("agents.signal_engine.record_outcome", exc, logger)
            await raw.lpush(OUTCOMES_KEY, json.dumps(outcome))
            await raw.ltrim(OUTCOMES_KEY, 0, OUTCOMES_KEPT - 1)
            await raw.hdel(AWAITING_KEY, wid)
            closed += 1
        return closed


_FEE_SQL = "SELECT fee_rate FROM borrow_rates WHERE ticker = $1 AND fee_rate IS NOT NULL ORDER BY day DESC LIMIT 1"


def borrow_cost_bps(fee_pct_annual: Optional[float], horizon: timedelta) -> float:
    """Borrow paid over a holding period, in bps, from an annual fee in percent."""
    if not fee_pct_annual:
        return 0.0
    return float(fee_pct_annual) * 100.0 * horizon.total_seconds() / (365 * 86400)


def exit_price(series: Optional[Series], expires_ts: float) -> Optional[float]:
    """The first close stamped at or after the expiry, or None while none exists."""
    if series is None:
        return None
    for t, c in zip(*series):
        if t >= expires_ts:
            return float(c)
    return None


def grade(sig: Dict[str, Any], exit_px: Optional[float], spy_exit: Optional[float] = None) -> Dict[str, Any]:
    """A closed signal's outcome: its return in the signal's direction, net of
    SPY's move over the same window when both SPY prices are known, and a hit
    when that beats the signal's own costs."""
    outcome: Dict[str, Any] = {"id": sig["id"], "instrument": sig["instrument"], "horizon": sig["horizon"],
                               "direction": sig["direction"], "probability": sig["probability"],
                               "families": sig["families"], "created_at": sig["created_at"],
                               "expires_at": sig["expires_at"]}
    entry = sig.get("entry_price")
    if exit_px and entry:
        sign = 1 if sig["direction"] == "long" else -1
        move = math.log(exit_px / float(entry))
        spy_entry = sig.get("spy_entry")
        if spy_entry and spy_exit:
            move -= math.log(spy_exit / float(spy_entry))
            outcome["net_of"] = "SPY"
        ret_bps = 1e4 * sign * move
        outcome.update(exit_price=exit_px, return_bps=round(ret_bps, 1), hit=ret_bps > float(sig["cost_bps"]))
    else:
        outcome.update(return_bps=None, hit=None,
                       note="no entry price" if not entry else "no price within the grace period")
    return outcome


__all__ = ["GRADE_GRACE", "SignalEngine", "borrow_cost_bps", "exit_price", "grade"]
