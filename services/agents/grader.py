"""
services/agents/grader.py

Grades trade ideas and signal-board snapshots once their horizon has closed,
against daily closes, net of SPY (shared/utils/track_record.py). Idempotent:
a row is graded once, when the close it needs exists.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from shared.utils import paper_ledger as pl
from shared.utils import producer_grades as pg
from shared.utils import score_calibration as scal
from shared.utils.signal_store import PRODUCER_GRADES_KEY
from shared.utils import track_record as tr

logger = logging.getLogger("agent.grader")

INTERVAL_SEC = 30 * 60
FIRST_RUN_DELAY_SEC = 120
HORIZON_DAYS = {"daily": 1, "weekly": 5, "monthly": 21}

_RECS_SQL = """
    SELECT day, horizon, ticker, direction, entry_price, spy_entry_price FROM recommendation_log
    WHERE graded_at IS NULL AND entry_price > 0 AND day < CURRENT_DATE
"""
_MATRIX_SQL = """
    SELECT day, ticker, net, price, spy_price, graded_1d_at, graded_5d_at FROM signal_matrix_log
    WHERE (graded_1d_at IS NULL OR graded_5d_at IS NULL) AND price > 0 AND day < CURRENT_DATE
"""
_CLOSES_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date ORDER BY ticker, day
"""
_GRADE_REC_SQL = """
    UPDATE recommendation_log SET graded_at = NOW(), exit_day = $5::date, exit_price = $6, ret = $7,
        spy_ret = $8, excess = $9, signed_excess = $10, hit = $11
    WHERE day = $1::date AND horizon = $2 AND ticker = $3 AND direction = $4
"""
# Paper ledger (shared/utils/paper_ledger.py).
# The ledger trades ideas from the first one the current screen passed: the
# 05:00 UTC batch on 8 Oct predates it (leveraged ETFs and sub-$10 names).
LEDGER_START = "2026-10-08 06:00+00"
_IDEAS_SQL = f"""
    SELECT day, horizon, ticker, direction, created_at, model, stop_price FROM recommendation_log
    WHERE day >= CURRENT_DATE - 30 AND entry_price > 0 AND created_at >= '{LEDGER_START}'::timestamptz
    ORDER BY created_at
"""
_LIVE_TRADES_SQL = """
    SELECT idea_day, horizon, ticker, direction, idea_at, stop_price, notional_usd, status, exit_day
    FROM paper_trades WHERE idea_day >= CURRENT_DATE - 60
"""
_NEW_TRADE_SQL = """
    INSERT INTO paper_trades (idea_day, horizon, ticker, direction, idea_at, model, stop_price, notional_usd)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING
"""
_BARS_SQL = """
    SELECT ticker, day, open, high, low, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date ORDER BY ticker, day
"""
_FILL_SQL = """
    UPDATE paper_trades SET status = $5, entry_day = $6::date, entry_price = $7, spy_entry = $8, cost_bps = $9,
        adv_usd = $10, exit_day = $11::date, exit_price = $12, spy_exit = $13, exit_reason = $14,
        gross_excess = $15, net_excess = $16, pnl_usd = $17, scale = $18, pnl_scaled_usd = $19
    WHERE idea_day = $1::date AND horizon = $2 AND ticker = $3 AND direction = $4
"""
_GRADE_1D_SQL = "UPDATE signal_matrix_log SET excess_1d = $3, graded_1d_at = NOW() WHERE day = $1::date AND ticker = $2"
_GRADE_5D_SQL = "UPDATE signal_matrix_log SET excess_5d = $3, graded_5d_at = NOW() WHERE day = $1::date AND ticker = $2"


def _ts(x: Any) -> Any:
    """shared.db hands timestamps back as ISO text; asyncpg wants datetimes back."""
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x


def _prior_close(days: List[date], closes: List[float], d: date) -> Optional[float]:
    """The last close strictly before d: the market's level when an idea dated d was given."""
    import bisect
    i = bisect.bisect_left(days, d) - 1
    return closes[i] if i >= 0 else None


# Producer grading (shared/utils/producer_grades.py): daily, over 90 days.
_OPTION_IDEAS_SQL = """
    SELECT day, horizon, ticker, direction, option_idea FROM recommendation_log
    WHERE option_idea IS NOT NULL AND option_graded_at IS NULL
      AND (option_idea->>'expiry')::date < CURRENT_DATE
"""
# The stock's close on expiry day, or the last before it (a holiday).
_CLOSE_ON_SQL = """
    SELECT close FROM daily_bars WHERE ticker = $1 AND day <= $2::date AND day > $2::date - 7
    ORDER BY day DESC LIMIT 1
"""
_GRADE_OPTION_SQL = """
    UPDATE recommendation_log SET option_pnl = $5, option_return_on_risk = $6, option_graded_at = NOW()
    WHERE day = $1 AND horizon = $2 AND ticker = $3 AND direction = $4
"""
PRODUCER_WINDOW_DAYS = 90
PRODUCER_INTERVAL_SEC = 24 * 3600
CALIBRATION_WINDOW_DAYS = 45
# Each stock event's score against its stock's next session: the absolute move
# over its own mean absolute move of the 20 sessions before (1.0 = ordinary).
_CALIBRATION_SQL = f"""
    WITH ev AS (
        SELECT type, source, anomaly_score AS s, upper(financial_data->>'ticker') AS t,
               (occurred_at AT TIME ZONE 'America/New_York')::date AS d
        FROM events
        WHERE occurred_at > NOW() - INTERVAL '{CALIBRATION_WINDOW_DAYS} days'
          AND type IN ('equity_block', 'market_anomaly', 'options_flow', 'insider_trade', 'price_anomaly',
                       'filing', 'earnings_report')
          AND financial_data ? 'ticker' AND anomaly_score IS NOT NULL
    ),
    b0 AS (
        SELECT ticker, day, abs(ln(close / lag(close) OVER (PARTITION BY ticker ORDER BY day))) AS ar
        FROM daily_bars WHERE day > CURRENT_DATE - {CALIBRATION_WINDOW_DAYS + 45} AND close > 0
    ),
    b AS (
        SELECT ticker, day, ar,
               avg(ar) OVER (PARTITION BY ticker ORDER BY day ROWS BETWEEN 21 PRECEDING AND 2 PRECEDING) AS base
        FROM b0
    )
    SELECT ev.type, ev.source, ev.s, nb.ar / nb.base AS ratio
    FROM ev
    JOIN LATERAL (SELECT ar, base FROM b WHERE b.ticker = ev.t AND b.day > ev.d ORDER BY b.day LIMIT 1) nb ON true
    WHERE nb.base > 0
"""
_TICKER_RE = "^[A-Z]{1,5}([.][A-Z])?$"
_CORR_SQL = f"""
    SELECT rule_name, primary_entity_id AS ticker, min(detected_at) AS at FROM correlations
    WHERE detected_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days' AND primary_entity_id ~ '{_TICKER_RE}'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
    GROUP BY 1, 2, date(detected_at)
"""
# The naive baseline: a stock with a headline that day and no alert naming it,
# first headline of the day. Drawn from the stocks alerts name, so each alert
# can be set against its own stock's headline days (paired_lift_table); a
# random 3,000 across all stocks left most alert stocks with none.
_HEADLINE_ONLY_SQL = f"""
    SELECT ticker, at FROM (
        SELECT u.sym AS ticker, min(n.created_at) AS at
        FROM stock_news n CROSS JOIN LATERAL unnest(n.symbols) AS u(sym)
        WHERE n.created_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days' AND cardinality(n.symbols) <= 2
          AND u.sym ~ '{_TICKER_RE}'
          AND u.sym IN (SELECT DISTINCT primary_entity_id FROM correlations
                        WHERE detected_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days')
          AND NOT EXISTS (SELECT 1 FROM correlations c WHERE c.primary_entity_id = u.sym
                          AND date(c.detected_at) = date(n.created_at))
        GROUP BY u.sym, date(n.created_at)
    ) x ORDER BY random() LIMIT 20000
"""
# Scenarios carry no entity of their own; the correlation that produced one does.
_SCEN_SQL = f"""
    SELECT s.status, c.primary_entity_id AS ticker, s.created_at AS at
    FROM scenarios s JOIN correlations c ON c.correlation_id::text = s.correlation_id::text
    WHERE s.created_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days' AND c.primary_entity_id ~ '{_TICKER_RE}'
"""
_CALLS_SQL = f"""
    SELECT agent_name, ticker, direction, horizon_hours, occurred_at AS at FROM agent_predictions
    WHERE occurred_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days' AND ticker IS NOT NULL
      AND direction IN ('long', 'short', 'up', 'down', 'bullish', 'bearish')
"""
_CLAIMS_SQL = f"""
    SELECT coalesce(agent_name, 'unnamed') AS agent_name, predicted_target, occurred_at, outcome_correct FROM agent_predictions
    WHERE occurred_at > NOW() - INTERVAL '{PRODUCER_WINDOW_DAYS} days' AND ticker IS NULL
      AND resolved_at IS NOT NULL AND predicted_target IS NOT NULL AND predicted_target NOT IN ('', 'unknown')
    LIMIT 2000
"""
_PLACEBO_SQL = """
    SELECT 1 FROM events WHERE occurred_at > $2 AND occurred_at <= $2 + INTERVAL '24 hours'
      AND (upper(primary_entity_id) = upper($1) OR upper(primary_entity_name) = upper($1)) LIMIT 1
"""
_UP = {"long", "up", "bullish"}
HEADLINE_DAYS = "(headline days, no alert)"     # the reference row in family_follow_*


class Grader:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self._producers_at = 0.0

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                out = await self.run_once()
                if any(out.get(k) for k in ("ideas", "snapshots", "paper_opened", "paper_filled", "paper_closed")):
                    logger.info("Graded: %s", out)
            except Exception as exc:
                logger.error("Grading failed: %s", exc, exc_info=True)
            await asyncio.sleep(INTERVAL_SEC)

    async def _closes(self, tickers: List[str], since: date) -> Dict[str, Tuple[List[date], List[float]]]:
        out: Dict[str, Tuple[List[date], List[float]]] = defaultdict(lambda: ([], []))
        for r in await self.db.query(_CLOSES_SQL, sorted(set(tickers) | {"SPY"}), since) or []:
            d, c = out[str(r["ticker"]).upper()]
            d.append(r["day"])
            c.append(float(r["close"]))
        return out

    async def run_once(self) -> Dict[str, int]:
        out = await self._grade()
        try:
            out["options"] = await self._grade_options()
        except Exception as exc:
            logger.error("Option grading failed: %s", exc, exc_info=True)
        try:
            out.update(await self.ledger())
        except Exception as exc:
            logger.error("Paper ledger failed: %s", exc, exc_info=True)
        import time as _t
        if self.raw is not None and _t.time() - self._producers_at > PRODUCER_INTERVAL_SEC:
            try:
                grades = await self.producers()
                await self.raw.set(PRODUCER_GRADES_KEY, json.dumps(grades, default=str))
                self._producers_at = _t.time()
                out["producer_groups"] = len(grades.get("correlations", []))
            except Exception as exc:
                logger.error("Producer grading failed: %s", exc, exc_info=True)
            try:
                models = await self.anomaly_calibration()
                await self.raw.set(scal.ANOMALY_CALIBRATION_KEY, json.dumps(models, default=str))
                out["calibrated_producers"] = len(models.get("producers", {}))
            except Exception as exc:
                logger.error("Anomaly calibration failed: %s", exc, exc_info=True)
        return out

    async def _bars(self, tickers: List[str], since: date) -> Dict[str, Tuple[List[date], List[float], List[float]]]:
        out: Dict[str, Tuple[List[date], List[float], List[float]]] = defaultdict(lambda: ([], [], []))
        for b in await self.db.query(_BARS_SQL, sorted(set(tickers) | {"SPY"}), since) or []:
            d, o, c = out[str(b["ticker"]).upper()]
            d.append(b["day"])
            o.append(float(b["open"] or b["close"]))
            c.append(float(b["close"]))
        return out

    async def anomaly_calibration(self) -> Dict[str, Any]:
        """Each stock producer's anomaly score fitted to its next-session moves
        (shared/utils/score_calibration.py)."""
        samples: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        for r in await self.db.query(_CALIBRATION_SQL) or []:
            samples[scal.producer_key(str(r["type"]), str(r["source"]))].append((float(r["s"]), float(r["ratio"])))
        producers = {k: m for k, v in samples.items() if (m := scal.fit(v)) is not None}
        return {"generated_at": datetime.utcnow().isoformat(), "window_days": CALIBRATION_WINDOW_DAYS,
                "big_move": scal.BIG_MOVE, "producers": producers}

    async def producers(self) -> Dict[str, Any]:
        """Correlation rules, scenarios and agent calls graded against prices."""
        corr = await self.db.query(_CORR_SQL) or []
        scen = await self.db.query(_SCEN_SQL) or []
        calls = await self.db.query(_CALLS_SQL) or []
        news_only = await self.db.query(_HEADLINE_ONLY_SQL) or []
        tickers = [str(r["ticker"]).upper() for r in list(corr) + list(scen) + list(calls) + list(news_only)]
        since = date.today() - timedelta(days=PRODUCER_WINDOW_DAYS + 120)
        bars = await self._bars(tickers, since)
        sd, so, sc = bars.get("SPY", ([], [], []))
        spy = {d: (o, c) for d, o, c in zip(sd, so, sc)}

        def ratio(ticker: str, at: Any, k: int):
            d, o, c = bars.get(ticker.upper(), ([], [], []))
            return pg.move_ratio(d, o, c, spy, at, k) if d else None

        spy_close = {d: c for d, (_, c) in spy.items()}

        def prior(ticker: str, at: Any) -> Optional[float]:
            d, _, c = bars.get(ticker.upper(), ([], [], []))
            return pg.prior_move(d, c, spy_close, _ts(at)) if d else None

        out: Dict[str, Any] = {"generated_at": date.today().isoformat(), "window_days": PRODUCER_WINDOW_DAYS}
        for k in (1, 5):
            rows = [(str(r["rule_name"]), (ratio(r["ticker"], r["at"], k) or (None,))[0]) for r in corr]
            out[f"correlations_{k}d"] = pg.ratio_table(rows)
            out[f"correlation_families_{k}d"] = pg.ratio_table([(pg.rule_family(g), x) for g, x in rows])
            rows = [(str(r["status"]), (ratio(r["ticker"], r["at"], k) or (None,))[0]) for r in scen]
            out[f"scenarios_{k}d"] = pg.ratio_table(rows, min_n=5)
            base = [(str(r["ticker"]).upper(), (ratio(r["ticker"], r["at"], k) or (None,))[0]) for r in news_only]
            fam = [(pg.rule_family(str(r["rule_name"])), str(r["ticker"]).upper(),
                    (ratio(r["ticker"], r["at"], k) or (None,))[0]) for r in corr]
            out[f"family_lift_{k}d"] = pg.paired_lift_table(fam, base)
            # Direction, beside size: did the stock continue the session the
            # alert arrived on? Headline days are the reference row, since
            # small stocks reverse a day's move whether or not anything fired.
            follow = [(pg.rule_family(str(r["rule_name"])), prior(r["ticker"], r["at"]),
                       (ratio(r["ticker"], r["at"], k) or (None, None))[1]) for r in corr]
            follow += [(HEADLINE_DAYS, prior(r["ticker"], r["at"]),
                        (ratio(r["ticker"], r["at"], k) or (None, None))[1]) for r in news_only]
            out[f"family_follow_{k}d"] = pg.follow_table(follow)
        out["correlations"] = out["correlations_5d"]
        signed: Dict[str, List[float]] = defaultdict(list)
        for r in calls:
            k = max(1, round(float(r["horizon_hours"] or 24) / 24 * 5 / 7)) if float(r["horizon_hours"] or 24) > 24 else 1
            got = ratio(r["ticker"], r["at"], k)
            if got:
                sign = 1 if str(r["direction"]).lower() in _UP else -1
                signed[str(r["agent_name"])].append(sign * got[1])
        out["agent_calls"] = {a: tr.summarize(v) for a, v in signed.items()}
        claims: Dict[str, Tuple[List[bool], List[bool]]] = defaultdict(lambda: ([], []))
        for r in await self.db.query(_CLAIMS_SQL) or []:
            hits, placebo = claims[str(r["agent_name"])]
            hits.append(bool(r["outcome_correct"]))
            # The placebo is the day before the claim: an entity already active
            # appears again whether or not anyone predicted it.
            prior = await self.db.query(_PLACEBO_SQL, str(r["predicted_target"]), _ts(r["occurred_at"]) - timedelta(days=1))
            placebo.append(bool(prior))
        out["entity_claims"] = {a: pg.placebo_rate(h, p) for a, (h, p) in claims.items()}
        return out

    async def ledger(self) -> Dict[str, int]:
        """Open paper trades for new ideas, then fill, stop or close every live one."""
        live = await self.db.query(_LIVE_TRADES_SQL) or []
        held = {(r["ticker"], int(r["direction"]), r["horizon"]) for r in live if r["status"] in ("pending", "open")}
        known = {(r["idea_day"], r["horizon"], r["ticker"], int(r["direction"])) for r in live}
        opened = 0
        for r in await self.db.query(_IDEAS_SQL) or []:
            key = (r["ticker"], int(r["direction"]), r["horizon"])
            if key in held or (r["day"], r["horizon"], r["ticker"], int(r["direction"])) in known:
                continue
            await self.db.execute(_NEW_TRADE_SQL, r["day"], r["horizon"], r["ticker"], int(r["direction"]),
                                  _ts(r["created_at"]), r["model"], r["stop_price"], pl.NOTIONAL_USD)
            held.add(key)
            opened += 1
        todo = [r for r in await self.db.query(_LIVE_TRADES_SQL) or [] if r["status"] in ("pending", "open")]
        if not todo:
            return {"paper_opened": opened, "paper_filled": 0, "paper_closed": 0}
        since = min(r["idea_day"] for r in todo) - timedelta(days=45)
        bars: Dict[str, List[pl.Bar]] = defaultdict(list)
        for b in await self.db.query(_BARS_SQL, sorted({r["ticker"] for r in todo} | {"SPY"}), since) or []:
            bars[str(b["ticker"]).upper()].append(pl.Bar(b["day"], float(b["open"] or b["close"]), float(b["high"] or b["close"]),
                                                          float(b["low"] or b["close"]), float(b["close"]), float(b["volume"] or 0)))
        spy = {b.day: b for b in bars.get("SPY", [])}
        filled = closed = 0
        for r in todo:
            f = pl.simulate(bars.get(str(r["ticker"]).upper(), []), spy, r["idea_at"], int(r["direction"]),
                            r["horizon"], r["stop_price"], float(r["notional_usd"]))
            if f is None:
                continue
            status = "closed" if f.exit_day else "open"
            await self.db.execute(_FILL_SQL, r["idea_day"], r["horizon"], r["ticker"], int(r["direction"]), status,
                                  f.entry_day, f.entry_price, f.spy_entry, f.cost_bps, f.adv_usd, f.exit_day,
                                  f.exit_price, f.spy_exit, f.exit_reason, f.gross_excess, f.net_excess, f.pnl_usd,
                                  f.scale, f.pnl_scaled_usd)
            filled += r["status"] == "pending"
            closed += status == "closed"
        return {"paper_opened": opened, "paper_filled": filled, "paper_closed": closed}

    async def _grade_options(self) -> int:
        """Option ideas whose expiry has passed, settled on the stock's close
        that day (shared/utils/option_ev.settle)."""
        from shared.utils import option_ev as oe

        rows = await self.db.query(_OPTION_IDEAS_SQL) or []
        graded = 0
        for r in rows:
            idea = r["option_idea"]
            idea = json.loads(idea) if isinstance(idea, str) else idea
            try:
                expiry = date.fromisoformat(str(idea.get("expiry"))[:10])
            except (TypeError, ValueError):
                continue
            close = await self.db.query_one(_CLOSE_ON_SQL, str(r["ticker"]).upper(), expiry)
            if not close:
                continue
            got = oe.settle(idea, float(close["close"]))
            if got is None:
                continue
            await self.db.execute(_GRADE_OPTION_SQL, r["day"], r["horizon"], r["ticker"], r["direction"],
                                  got["pnl"], got["return_on_risk"])
            graded += 1
        return graded

    async def _grade(self) -> Dict[str, int]:
        recs = await self.db.query(_RECS_SQL) or []
        snaps = await self.db.query(_MATRIX_SQL) or []
        if not recs and not snaps:
            return {"ideas": 0, "snapshots": 0}
        since = min([r["day"] for r in recs] + [r["day"] for r in snaps])
        closes = await self._closes([r["ticker"] for r in recs] + [r["ticker"] for r in snaps], since)
        spy_days, spy_closes = closes.get("SPY", ([], []))
        graded_recs = graded_snaps = 0
        for r in recs:
            days, cl = closes.get(str(r["ticker"]).upper(), ([], []))
            fwd = tr.forward_close(days, cl, r["day"], HORIZON_DAYS.get(r["horizon"], 5))
            if not fwd:
                continue
            spy_fwd = tr.forward_close(spy_days, spy_closes, r["day"], HORIZON_DAYS.get(r["horizon"], 5))
            spy_in = r["spy_entry_price"] or _prior_close(spy_days, spy_closes, r["day"])
            g = tr.grade(int(r["direction"]), float(r["entry_price"]), fwd[1], spy_in, spy_fwd[1] if spy_fwd else None)
            await self.db.execute(_GRADE_REC_SQL, r["day"], r["horizon"], r["ticker"], r["direction"], fwd[0], fwd[1],
                                  g["ret"], g["spy_ret"], g["excess"], g["signed_excess"],
                                  None if g["hit"] is None else bool(g["hit"]))
            graded_recs += 1
        for r in snaps:
            days, cl = closes.get(str(r["ticker"]).upper(), ([], []))
            spy_in = r["spy_price"] or _prior_close(spy_days, spy_closes, r["day"])
            for k, done, sql in ((1, r["graded_1d_at"], _GRADE_1D_SQL), (5, r["graded_5d_at"], _GRADE_5D_SQL)):
                if done:
                    continue
                fwd = tr.forward_close(days, cl, r["day"], k)
                spy_fwd = tr.forward_close(spy_days, spy_closes, r["day"], k)
                if not fwd:
                    continue
                g = tr.grade(1, float(r["price"]), fwd[1], spy_in, spy_fwd[1] if spy_fwd else None)
                await self.db.execute(sql, r["day"], r["ticker"], g["excess"])
                graded_snaps += 1
        return {"ideas": graded_recs, "snapshots": graded_snaps}


__all__ = ["Grader", "HORIZON_DAYS"]
