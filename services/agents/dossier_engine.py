"""
services/agents/dossier_engine.py

The signal matrix for every anomalous ticker: every name with a scored market
event (block, sweep, volume or radar anomaly, filing, insider trade) in the
last three days gets every signal the platform can compute for it, each with
its evidence status and its as-of time (shared/utils/signal_matrix.py).

Batched: a handful of queries over all the names at once, not one per name,
so a few hundred tickers refresh in well under a minute. Published to Redis
for the gateway's board and ticker endpoints.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.utils import signal_matrix as sm
from shared.utils.signal_plain import explain_matrix
from shared.utils.asset_universe import load_asset_meta
from shared.utils.evidence_fusion import EVIDENCE_EVENT_TYPES, EVIDENCE_EVENTS_SQL, build_dossiers, family_record
from shared.utils.market_session import Session, current_session, eastern_now
from shared.utils.options_positioning import positioning_key
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.signal_families import observations
from shared.utils.signal_store import (
    FACTOR_REPORT_KEY, SIGNAL_MATRIX_INDEX_KEY, SIGNAL_MATRIX_KEY, STUDY_KEY, factor_scores_key,
)
from shared.utils.trade_plan import model_support

logger = logging.getLogger("agent.dossier_engine")

WINDOW_HOURS = 72
MAX_TICKERS = 400
SESSION_INTERVAL_SEC = 5 * 60
OFF_HOURS_INTERVAL_SEC = 30 * 60
FIRST_RUN_DELAY_SEC = 60
TTL_SEC = 6 * 3600

_ANOMALOUS_SQL = """
    SELECT upper(coalesce(financial_data->>'ticker', primary_entity_id)) AS ticker,
           max(anomaly_score) AS score, count(*) AS n, array_agg(DISTINCT type) AS types,
           max(created_at) AS last
    FROM events
    WHERE created_at > $1 AND anomaly_score >= 0.6
      AND type IN ('equity_block', 'options_flow', 'market_anomaly', 'filing', 'insider_trade',
                   'insider_cluster', 'price_anomaly', 'earnings_report')
      AND coalesce(financial_data->>'ticker', primary_entity_id) IS NOT NULL
    GROUP BY 1 ORDER BY max(anomaly_score) DESC, count(*) DESC LIMIT $2
"""
_PRICES_SQL = """
    SELECT ticker, day, high, low, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 400 ORDER BY ticker, day
"""
_SWEEPS_SQL = """
    SELECT upper(financial_data->>'ticker') AS ticker, upper(coalesce(financial_data->>'option_type', '')) AS ot,
           sum(coalesce((financial_data->>'premium_usd')::float, 0)) AS premium
    FROM events WHERE type = 'options_flow' AND created_at > NOW() - INTERVAL '5 days'
      AND upper(financial_data->>'ticker') = ANY($1::text[])
    GROUP BY 1, 2
"""
_INSIDER_SQL = """
    SELECT upper(coalesce(financial_data->>'ticker', primary_entity_id)) AS ticker, type, tags,
           coalesce((financial_data->>'premium_usd')::float, 0) AS usd, headline, created_at
    FROM events WHERE type IN ('insider_trade', 'insider_cluster') AND created_at > NOW() - INTERVAL '90 days'
      AND upper(coalesce(financial_data->>'ticker', primary_entity_id)) = ANY($1::text[])
"""
_NEWS_SQL = """
    SELECT substr(t, 9) AS ticker, count(*) AS n, avg(sentiment) AS tone FROM (
        SELECT unnest(tags) AS t, sentiment FROM events
        WHERE type = 'headline' AND created_at > NOW() - INTERVAL '72 hours'
    ) x WHERE t LIKE 'exposes:%' GROUP BY 1
"""
_OPTION_XS_SQL = """
    SELECT ticker, iv_skew, cp_iv_spread FROM option_daily
    WHERE day = (SELECT max(day) FROM option_daily)
"""
_OPTION_DAILY_SQL = """
    SELECT ticker, day, put_call_ratio FROM option_daily
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 45 ORDER BY ticker, day DESC
"""
_SHORT_SQL = """
    SELECT * FROM (
        SELECT ticker, settlement_date, days_to_cover, change_pct,
               row_number() OVER (PARTITION BY ticker ORDER BY settlement_date DESC) AS rn
        FROM short_interest WHERE ticker = ANY($1::text[])
    ) s WHERE rn <= 2
"""
_PROFILE_SQL = "SELECT ticker, sic, sic_description FROM company_profile WHERE ticker = ANY($1::text[])"
_SNAPSHOT_SQL = """
    INSERT INTO signal_matrix_log (day, ticker, net, lean, votes, proven_net, supported_net, published_net,
                                   anomaly_score, price, spy_price)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (day, ticker) DO NOTHING
"""
_EARNINGS_SQL = """
    SELECT primary_entity_id AS ticker, max(occurred_at) AS at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND occurred_at > NOW() - INTERVAL '120 days'
      AND primary_entity_id = ANY($1::text[])
    GROUP BY 1
"""


def _loads(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except (ValueError, AttributeError):
        return None


def _iso(v: Any) -> Optional[str]:
    return v.isoformat() if isinstance(v, datetime) else (str(v) if v else None)


def earnings_reaction(days: List[Any], closes: List[float], spy_by_day: Dict[Any, float],
                      accepted_at: Any) -> Tuple[Optional[float], Optional[str]]:
    """Market-adjusted return from the last close before the release to two closes later."""
    from services.agents.factor_engine import base_row

    if not accepted_at or len(days) < 5:
        return None, None
    b = base_row(list(days), accepted_at)
    e = b + 2
    if b < 0 or e >= len(days):
        return None, None
    s0, s1 = spy_by_day.get(days[b]), spy_by_day.get(days[e])
    r = closes[e] / closes[b] - 1.0
    if s0 and s1:
        r -= s1 / s0 - 1.0
    return r, str(days[e])


class DossierEngine:
    def __init__(self, db: Any, redis_client: Any):
        self.db = db
        self.raw = getattr(redis_client, "raw", redis_client)

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                summary = await self.run_once()
                logger.info("Signal matrix: %s", summary)
            except Exception as exc:
                logger.error("Signal matrix run failed: %s", exc, exc_info=True)
            open_now = current_session() == Session.REGULAR
            await asyncio.sleep(SESSION_INTERVAL_SEC if open_now else OFF_HOURS_INTERVAL_SEC)

    async def _dossiers(self, now: datetime) -> Dict[str, Any]:
        rows = await self.db.query(EVIDENCE_EVENTS_SQL, now - timedelta(hours=WINDOW_HOURS), now,
                                   EVIDENCE_EVENT_TYPES)
        cells = (_loads(await self.raw.get(STUDY_KEY)) or {}).get("cells", [])

        def _compute() -> Dict[str, Any]:
            obs = []
            for r in rows or []:
                d = dict(r)
                if isinstance(d.get("financial_data"), str):
                    d["financial_data"] = _loads(d["financial_data"]) or {}
                obs.extend(observations(d))
            return {k: v.as_dict() for k, v in build_dossiers(obs, family_record(cells)).items()}

        return await asyncio.to_thread(_compute)

    async def run_once(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        anomalous = await self.db.query(_ANOMALOUS_SQL, now - timedelta(hours=WINDOW_HOURS), MAX_TICKERS)
        heads = {str(r["ticker"]): r for r in anomalous or [] if r["ticker"]}
        tickers = sorted(heads)
        if not tickers:
            return {"skipped": "no anomalous tickers"}

        prices: Dict[str, Dict[str, list]] = defaultdict(lambda: {"day": [], "high": [], "low": [], "close": [], "volume": []})
        for r in await self.db.query(_PRICES_SQL, tickers + ["SPY"]) or []:
            d = prices[str(r["ticker"]).upper()]
            c = float(r["close"])
            d["day"].append(r["day"])
            d["close"].append(c)
            d["high"].append(float(r["high"] or c))
            d["low"].append(float(r["low"] or c))
            d["volume"].append(float(r["volume"] or 0.0))
        tickers = [t for t in tickers if t in prices and len(prices[t]["close"]) >= 22]
        spy = prices.get("SPY") or {"day": [], "close": []}
        spy_by_day = dict(zip(spy["day"], spy["close"]))

        sweeps: Dict[str, Dict[str, float]] = defaultdict(dict)
        for r in await self.db.query(_SWEEPS_SQL, tickers) or []:
            key = "put" if str(r["ot"]).startswith("P") else "call"
            sweeps[str(r["ticker"])][key] = sweeps[str(r["ticker"])].get(key, 0.0) + float(r["premium"] or 0)

        insiders: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "buyers": set(),
                                                                   "sellers": set(), "clusters": 0, "last": None})
        for r in await self.db.query(_INSIDER_SQL, tickers) or []:
            d = insiders[str(r["ticker"])]
            tags = set(r["tags"] or [])
            d["last"] = max(filter(None, [d["last"], _iso(r["created_at"])]), default=None)
            if r["type"] == "insider_cluster":
                d["clusters"] += 1
                continue
            if "planned_10b5_1" in tags:
                continue
            who = str(r["headline"] or "").rsplit(" by ", 1)[-1][:60]
            if "open_market_buy" in tags:
                d["buy"] += float(r["usd"] or 0)
                d["buyers"].add(who)
            elif "open_market_sale" in tags:
                d["sell"] += float(r["usd"] or 0)
                d["sellers"].add(who)

        news = {str(r["ticker"]).upper(): (int(r["n"]), float(r["tone"]) if r["tone"] is not None else None)
                for r in await self.db.query(_NEWS_SQL) or []}
        # Today's skew and call-put spread for every name the chains cover, so
        # each board row is ranked against the cross-section, not a fixed level.
        xs_pct: Dict[str, Dict[str, float]] = {}
        xs_rows = await self.db.query(_OPTION_XS_SQL) or []
        if len(xs_rows) >= sm.XS_MIN_NAMES:
            for key in ("iv_skew", "cp_iv_spread"):
                xs_pct[key] = sm.cross_section_pct({str(r["ticker"]): r[key] for r in xs_rows})
        opt_daily: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in await self.db.query(_OPTION_DAILY_SQL, tickers) or []:
            opt_daily[str(r["ticker"])].append(dict(r))
        shorts: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in await self.db.query(_SHORT_SQL, tickers) or []:
            shorts[str(r["ticker"])].append(dict(r))
        profiles = {str(r["ticker"]): r for r in await self.db.query(_PROFILE_SQL, tickers) or []}
        earnings = {str(r["ticker"]): r["at"] for r in await self.db.query(_EARNINGS_SQL, tickers) or []}

        report = _loads(await self.raw.get(FACTOR_REPORT_KEY)) or {}
        support: Dict[str, Optional[str]] = {}
        legs: Dict[str, Dict[str, Any]] = {}
        ranks: Dict[str, Dict[str, Any]] = {}
        for h, meta in (report.get("horizons") or {}).items():
            chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
            support[h] = model_support({**chosen, "validated": meta.get("validated")})
            legs[h] = chosen.get("exposures") or {}
            ranks[h] = _loads(await self.raw.get(factor_scores_key(h))) or {}
        meta_all = await load_asset_meta(self.raw)
        dossiers = await self._dossiers(now)

        today = str(eastern_now().date())
        spy_live = await self._live_price("SPY")
        board: List[Dict[str, Any]] = []
        pipe_payload: Dict[str, str] = {}
        for t in tickers:
            px = prices[t]
            as_of = str(px["day"][-1])
            live = await self._live_price(t)
            m = sm.Matrix(t)
            spy_aligned = [spy_by_day.get(d) for d in px["day"]]
            spy_series = spy_aligned if all(spy_aligned) else []
            closes, highs, lows = list(px["close"]), list(px["high"]), list(px["low"])
            # In the session the newest daily bar is yesterday's: today's live
            # quote is the latest close, and SPY's beside it keeps the excess
            # and beta series aligned (dropped when SPY has no live quote).
            if live is not None and str(px["day"][-1]) < today:
                closes.append(live)
                highs.append(max(live, closes[-2]))
                lows.append(min(live, closes[-2]))
                spy_series = spy_series + [spy_live] if spy_series and spy_live else []
                as_of = "live"
            pinfo = sm.price_signals(m, closes, highs, lows, px["volume"], spy_series, as_of)
            sm.model_signals(m, {h: ranks.get(h, {}).get(t) for h in ("daily", "weekly", "monthly")}, support,
                             report.get("as_of"), legs)
            sm.event_signals(m, dossiers.get(t))
            ranked = next((ranks.get(h, {}).get(t) for h in ("weekly", "monthly", "daily") if ranks.get(h, {}).get(t)), None)
            sm.fundamental_signals(m, (ranked or {}).get("features"), report.get("as_of"))
            pos = _loads(await self.raw.get(positioning_key(t))) or {}
            sm.options_signals(m, pos, opt_daily.get(t, []), sweeps.get(t, {}), pinfo.get("rv21"), pos.get("as_of"),
                               {k: v.get(t) for k, v in xs_pct.items()})
            sm.short_signals(m, shorts.get(t, []), None)
            ins = insiders.get(t)
            if ins:
                sm.insider_signals(m, ins["buy"], ins["sell"], len(ins["buyers"]), len(ins["sellers"]),
                                   ins["clusters"], ins["last"])
            ear, ear_day = earnings_reaction(px["day"], px["close"], spy_by_day, earnings.get(t))
            sm.filing_signals(m, {}, ear, ear_day)
            n, tone = news.get(t, (0, None))
            sm.news_signals(m, n, tone, None)
            dv = (sum(c * v for c, v in zip(px["close"][-21:], px["volume"][-21:])) / len(px["close"][-21:]))
            meta = (meta_all or {}).get(t)
            ex = sm.execution_signals(m, live or pinfo.get("price"), dv, pinfo.get("rv21"), meta)

            head = heads[t]
            prof = profiles.get(t) or {}
            doc = explain_matrix(m.as_dict())
            doc.update({
                "name": meta[3] if meta else None, "kind": meta[0] if meta else None,
                "industry": prof.get("sic_description"), "price": live or pinfo.get("price"),
                "price_as_of": as_of,
                "anomaly_score": float(head["score"]), "events_72h": int(head["n"]),
                "triggers": sorted(head["types"] or []), "last_event": _iso(head["last"]),
                "generated_at": now.isoformat(),
            })
            pipe_payload[t] = json.dumps(doc, default=str)
            by = doc["tally"]["by_status"]
            sig = {s["name"]: s for s in doc["signals"]}
            board.append({
                "ticker": t, "name": doc["name"], "industry": doc["industry"], "price": doc["price"],
                "ret_1d": (sig.get("return 1 day") or {}).get("value"),
                "ret_5d": (sig.get("return 1 week") or {}).get("value"),
                "anomaly_score": doc["anomaly_score"], "events_72h": doc["events_72h"], "triggers": doc["triggers"],
                "lean": doc["tally"]["lean"], "net": doc["tally"]["net"], "votes": doc["tally"]["weighted_votes"],
                "verdict": doc["verdict"]["headline"], "confidence": doc["verdict"]["confidence"],
                "why": doc["verdict"]["why"],
                "proven": by["proven"], "supported": by["supported"],
                "published": by["published"],
                "weekly_pct": (ranks.get("weekly", {}).get(t) or {}).get("percentile"),
                "cost_bps_1m": ex.get("cost_bps_1m"), "adv_usd": ex.get("adv_usd"),
                "iv_regime_ratio": (sig.get("implied / realised vol") or {}).get("value"),
                "dtc": (sig.get("days to cover") or {}).get("value"),
                "insider_net": (sig.get("open-market net, 90d (plans excluded)") or {}).get("value"),
                "signals": len(doc["signals"]),
            })
        # The day's first snapshot of each row, graded later (services/agents/grader.py).
        if current_session() == Session.REGULAR:
            snap_day = eastern_now().date()
            rows = [(snap_day, b["ticker"], b["net"], b["lean"], b["votes"],
                     b["proven"]["bull"] - b["proven"]["bear"], b["supported"]["bull"] - b["supported"]["bear"],
                     b["published"]["bull"] - b["published"]["bear"], b["anomaly_score"], b["price"], spy_live)
                    for b in board if b["price"]]
            try:
                if rows:
                    await self.db.execute_many(_SNAPSHOT_SQL, rows)
            except Exception as e:
                swallowed("agents.dossier_engine.snapshot", e, logger)
        if pipe_payload:
            await self.raw.delete(SIGNAL_MATRIX_KEY)
            await self.raw.hset(SIGNAL_MATRIX_KEY, mapping=pipe_payload)
            await self.raw.expire(SIGNAL_MATRIX_KEY, TTL_SEC)
        board.sort(key=lambda b: (-(b["proven"]["bull"] + b["proven"]["bear"]), -abs(b["net"]), -b["anomaly_score"]))
        await self.raw.set(SIGNAL_MATRIX_INDEX_KEY, json.dumps({
            "generated_at": now.isoformat(), "window_hours": WINDOW_HOURS, "tickers": len(board),
            "model_as_of": report.get("as_of"), "primary_horizon": report.get("primary_horizon"),
            "support": support, "board": board,
        }, default=str), ex=TTL_SEC)
        return {"tickers": len(board), "signals": sum(b["signals"] for b in board),
                "with_proven": sum(1 for b in board if b["proven"]["bull"] + b["proven"]["bear"])}

    async def _live_price(self, ticker: str) -> Optional[float]:
        try:
            return parse_quote(await self.raw.get(quote_key(ticker)))
        except Exception as e:
            swallowed("agents.dossier_engine.quote", e, logger)
            return None


__all__ = ["DossierEngine", "earnings_reaction"]
