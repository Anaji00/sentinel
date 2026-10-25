"""
services/agents/recommendation_engine.py

Trade ideas, refreshed through the session: stock ideas at each horizon from the
cross-sectional model, an option structure for each from the live chain, and
the signal engine's active intraday signals. Pure logic is in
shared.utils.recommendations; this gathers inputs, publishes, and logs.

Every idea is logged once per day per horizon, ticker and direction in
recommendation_log, with its entry, so the ideas can be graded against what
prices did -- the same discipline the reaction study applies to signals.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from shared.utils import expected_returns as er
from shared.utils import option_ev as oe
from shared.utils import recommendations as rc
from shared.utils import news_embed as ne
from shared.utils import meta_label as ml
from shared.utils import model_portfolio as mp
from shared.utils import trading_costs as tc
from shared.utils.book import build_book
from shared.utils.bar_feed import DAILY_BAR_VOLUME_SHARE
from shared.utils.signal_plain import idea_reasons
from shared.utils.asset_universe import load_asset_meta, refresh_asset_meta
from shared.utils.equities import parse_occ_option_symbol
from shared.utils.market_session import Session, current_session, eastern_now
from shared.utils.options_positioning import positioning_key
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.trade_plan import leg_earns, model_support
from shared.utils import portfolio_lab as pl
from shared.utils.equity_regime import EQUITY_REGIME_KEY
from shared.utils.portfolio_lab import PORTFOLIO_LAB_KEY
from shared.utils.signal_store import (
    ACTIVE_KEY, FACTOR_REPORT_KEY, RECOMMENDATIONS_KEY, SIGNAL_MATRIX_KEY, factor_scores_key,
)

logger = logging.getLogger("agent.recommendation_engine")

IDEAS_PER_SIDE = 6
CANDIDATES_PER_SIDE = 80   # ranked names screened before six a side are kept
SESSION_INTERVAL_SEC = 15 * 60
OFF_HOURS_INTERVAL_SEC = 2 * 3600
FIRST_RUN_DELAY_SEC = 90
RESULT_TTL_SEC = 2 * 86400
CHAIN_URL = "https://data.alpaca.markets/v1beta1/options/snapshots/{ticker}"
HORIZON_ORDER = ("daily", "weekly", "monthly")
# Ideas carried by the Signal Board's evidence alone (insider buying, unusual
# flow, filings, proven families), held a week and graded apart in the ledger.
EVENT_HORIZON = "events"
EVENT_META = {"horizon_days": 5, "validated": False, "chosen": "events", "models": []}

_PRICES_SQL = """
    SELECT ticker, day, high, low, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 400 ORDER BY ticker, day
"""
# Each stock's typical two-day earnings move: the archive keeps reactions of 2%
# or more, so releases without one count at 1% (expected_returns.forecast_vol).
_EARNINGS_MOVE_SQL = """
    WITH rel AS (SELECT primary_entity_id AS ticker, count(*) AS n FROM signal_event_archive
                 WHERE type = 'filing' AND summary LIKE '%2.02:%' AND primary_entity_id = ANY($1::text[])
                   AND created_at > NOW() - INTERVAL '3 years' GROUP BY 1),
         rx AS (SELECT primary_entity_id AS ticker, count(*) AS n, sum(abs((financial_data->>'ear')::float)) AS s
                FROM signal_event_archive WHERE type = 'earnings_reaction' AND primary_entity_id = ANY($1::text[])
                  AND created_at > NOW() - INTERVAL '3 years' GROUP BY 1)
    SELECT rel.ticker, (coalesce(rx.s, 0) + greatest(rel.n - coalesce(rx.n, 0), 0) * 0.01) / rel.n AS move
    FROM rel LEFT JOIN rx USING (ticker) WHERE rel.n >= 2
"""
_BORROW_SQL = """
    SELECT DISTINCT ON (ticker) ticker, fee_rate FROM borrow_rates
    WHERE ticker = ANY($1::text[]) AND fee_rate IS NOT NULL ORDER BY ticker, day DESC
"""
# Event signals long enough for a listed option: (expiry window, trading days held).
SIGNAL_OPTION_WINDOWS = {"5d": ((5, 16), 4), "30d": ((30, 70), 21)}
# The stock's drift beyond its excess: a broad equity premium plus cash.
MARKET_DRIFT = 0.08
# Trailing returns behind the alpha's volatility scale, as calibrated.
CALIBRATION_VOL_WINDOW = 63
_INDUSTRY_SQL = "SELECT ticker, sic_description FROM company_profile WHERE ticker = ANY($1::text[])"
_BOOK_SQL = "SELECT long_book, short_book FROM model_portfolio_state WHERE portfolio = $1"
# Each name's last results release (8-K item 2.02), for the earnings warning.
_LAST_RESULTS_SQL = """
    SELECT primary_entity_id AS ticker, max(occurred_at) AS at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND primary_entity_id = ANY($1::text[])
      AND occurred_at > NOW() - INTERVAL '200 days'
    GROUP BY 1
"""
_LOG_SQL = """
    INSERT INTO recommendation_log (day, horizon, ticker, direction, validated, entry_price, stop_price,
                                    expected_excess_bps, model, structure, option_idea, as_of, spy_entry_price)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::date, $13)
    ON CONFLICT (day, horizon, ticker, direction) DO NOTHING
"""


def _loads(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except (ValueError, AttributeError):
        return None


class RecommendationEngine:
    def __init__(self, db: Any, redis_client: Any):
        self.db = db
        self.redis = redis_client
        self.raw = getattr(redis_client, "raw", redis_client)
        key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
        secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
        self.headers = {"APCA-API-KEY-ID": key or "", "APCA-API-SECRET-KEY": secret or ""} if key and secret else None

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                summary = await self.run_once()
                logger.info("Recommendations: %s", summary)
            except Exception as exc:
                logger.error("Recommendation run failed: %s", exc, exc_info=True)
            open_now = current_session() == Session.REGULAR
            await asyncio.sleep(SESSION_INTERVAL_SEC if open_now else OFF_HOURS_INTERVAL_SEC)

    async def _prices(self, tickers: List[str]) -> Dict[str, Dict[str, List[float]]]:
        out: Dict[str, Dict[str, List[float]]] = {}
        for r in await self.db.query(_PRICES_SQL, tickers) or []:
            d = out.setdefault(str(r["ticker"]).upper(), {"day": [], "high": [], "low": [], "close": [], "volume": []})
            c = float(r["close"])
            d["day"].append(r["day"])
            d["close"].append(c)
            d["volume"].append(float(r["volume"] or 0.0))
            d["high"].append(float(r["high"] or c))
            d["low"].append(float(r["low"] or c))
        return out

    async def _chain(self, session: aiohttp.ClientSession, ticker: str, today: date,
                     window: tuple) -> List[tuple]:
        if self.headers is None:
            return []
        params = {"limit": "1000",
                  "expiration_date_gte": (today + timedelta(days=window[0])).isoformat(),
                  "expiration_date_lte": (today + timedelta(days=window[1])).isoformat()}
        async with session.get(CHAIN_URL.format(ticker=ticker), params=params, headers=self.headers) as resp:
            if resp.status != 200:
                return []
            snaps = (await resp.json()).get("snapshots") or {}
        out = []
        for sym, snap in snaps.items():
            parsed = parse_occ_option_symbol(sym)
            if parsed:
                out.append(({**parsed, "symbol": sym}, snap))
        return out

    async def _asset_meta(self) -> Dict[str, list]:
        """Alpaca asset metadata, refreshed when the cache has lapsed."""
        meta = await load_asset_meta(self.raw)
        if meta:
            return meta
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                await refresh_asset_meta(self.raw, session, self.headers)
        except Exception as e:
            swallowed("agents.recommendation_engine.asset_meta", e, logger)
        return await load_asset_meta(self.raw)

    async def _live_price(self, ticker: str, fallback: Optional[float]) -> Optional[float]:
        try:
            q = parse_quote(await self.raw.get(quote_key(ticker)))
        except Exception as e:
            q = None
            swallowed("agents.recommendation_engine.quote", e, logger)
        return q or fallback

    async def run_once(self) -> Dict[str, Any]:
        report = _loads(await self.raw.get(FACTOR_REPORT_KEY)) or {}
        horizons = report.get("horizons") or {}
        if not horizons:
            return {"skipped": "no multi-horizon model report yet"}
        today = eastern_now().date()
        plans: Dict[str, List[Dict[str, Any]]] = {}
        wanted: List[str] = []
        # Industries of every scored name, for ideas ranked within industry
        # like the headline book (model_portfolio.HEADLINE_PORTFOLIO).
        # What the lab adopted for the books applies to the stock ideas too:
        # ranking within return themes and momentum-volatility scaling
        # (portfolio_lab.adoptions).
        adopted = pl.adoptions(_loads(await self.raw.get(PORTFOLIO_LAB_KEY)) or {})
        themes = ({str(t).upper(): f"theme {v}" for t, v in (_loads(await self.raw.get(pl.RETURN_CLUSTERS_KEY)) or {}).items()}
                  if adopted.get("cluster_neutral") else {})
        gauge = _loads(await self.raw.get(EQUITY_REGIME_KEY)) or {}
        mom_scale = float(gauge.get("momentum_vol_scale") or 1.0) if adopted.get("momentum_vol_scale") else 1.0
        short_tilt = float(gauge.get("short_tilt") or 1.0) if adopted.get("sentiment_short_tilt") else 1.0
        sector_of: Dict[str, Optional[str]] = {}
        # Independent bets on each side of each horizon's ideas (rc.effective_bets).
        bets: Dict[str, Dict[str, Optional[float]]] = {}
        withheld: Dict[str, str] = {}
        supports: Dict[str, Optional[str]] = {}
        for h in HORIZON_ORDER:
            meta = horizons.get(h)
            if not meta:
                continue
            chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
            support = model_support({**chosen, "validated": meta.get("validated")})
            supports[h] = support
            if not support:
                # A horizon whose model loses after costs, or has no reliable
                # out-of-sample ranking, produces no ideas -- only the reason.
                plans[h] = []
                withheld[h] = (f"{meta.get('chosen')} at this horizon: net Sharpe {chosen.get('net_sharpe')}, "
                               f"IC t {chosen.get('ic_t')} -- below the out-of-sample bar after costs")
                continue
            scores = _loads(await self.raw.get(factor_scores_key(h))) or {}
            missing = sorted(t for t in scores if t not in sector_of)
            if themes:
                sector_of.update({t: themes.get(t.upper()) for t in missing})
                missing = []
            if missing:
                got = {str(r["ticker"]).upper(): r["sic_description"]
                       for r in await self.db.query(_INDUSTRY_SQL, missing) or []}
                sector_of.update({t: got.get(t.upper()) for t in missing})
            plans[h] = rc.stock_ideas(scores, n=CANDIDATES_PER_SIDE, industries=sector_of)
            # Only a side whose leg beat SPY out of sample becomes an idea.
            expo = chosen.get("exposures") or {}
            losing = [s for s in (1, -1) if not leg_earns(expo, s)]
            if losing:
                plans[h] = [i for i in plans[h] if i["direction"] not in losing]
                withheld[h] = "; ".join(
                    f"{'long' if s > 0 else 'short'} side withheld: its picks returned "
                    f"{expo.get('long_leg_bps' if s > 0 else 'short_leg_bps'):+.1f} bps a period against SPY out of sample"
                    for s in losing)
            wanted += [i["ticker"] for i in plans[h]]
        # The board's evidence on every name it covers: it can overrule a model
        # idea, and one-sided evidence can carry an idea of its own.
        boards: Dict[str, Dict[str, Any]] = {}
        verdicts: Dict[str, Dict[str, Any]] = {}
        for t, blob in (await self.raw.hgetall(SIGNAL_MATRIX_KEY) or {}).items():
            doc = _loads(blob)
            if doc:
                key = t.decode() if isinstance(t, bytes) else str(t)
                boards[key] = rc.evidence_balance(doc.get("signals") or [])
                verdicts[key] = doc.get("verdict") or {}
        plans[EVENT_HORIZON] = []
        for t, bal in boards.items():
            d = rc.event_led(bal)
            if d:
                plans[EVENT_HORIZON].append({"ticker": t, "direction": d, "score": bal["bull"] - bal["bear"],
                                             "evidence": bal["bull_reasons"] if d > 0 else bal["bear_reasons"]})
        plans[EVENT_HORIZON].sort(key=lambda i: -abs(i["score"]))
        wanted += [i["ticker"] for i in plans[EVENT_HORIZON]]
        overruled: List[Dict[str, Any]] = []
        prices = await self._prices(sorted(set(wanted) | {"SPY"}))
        last_results: Dict[str, date] = {}
        for r in await self.db.query(_LAST_RESULTS_SQL, sorted(set(wanted))) or []:
            at = r["at"]
            at = datetime.fromisoformat(at.replace("Z", "+00:00")) if isinstance(at, str) else at
            last_results[str(r["ticker"]).upper()] = at.date()
        industries = {str(r["ticker"]).upper(): r["sic_description"]
                      for r in await self.db.query(_INDUSTRY_SQL, sorted(set(wanted))) or []}
        earnings_move: Dict[str, float] = {}
        try:
            earnings_move = {str(r["ticker"]).upper(): float(r["move"])
                             for r in await self.db.query(_EARNINGS_MOVE_SQL, sorted(set(wanted))) or []}
        except Exception as e:
            swallowed("agents.recommendation_engine.earnings_move", e, logger)
        lab = _loads(await self.raw.get(PORTFOLIO_LAB_KEY)) or {}
        calib = {h: c for h, c in (lab.get("alpha_calibration") or {}).items() if c}
        # Sizing by the chance a pick pays (meta-labelling), for the monthly
        # ideas the lab's classifier was trained on, once consecutive lab runs
        # agree it raises the Sharpe (shared/utils/meta_label.py).
        meta_model = lab.get("meta_model") if ml.confirmed(lab.get("meta_history") or []) else None
        spy_px = (await self._prices(["SPY"])).get("SPY") if meta_model else None
        borrow: Dict[str, float] = {}
        try:
            borrow = {str(r["ticker"]).upper(): float(r["fee_rate"])
                      for r in await self.db.query(_BORROW_SQL, sorted(set(wanted))) or []}
        except Exception as e:
            swallowed("agents.recommendation_engine.borrow", e, logger)
        # Proven event families active on each name, with their measured moves.
        active_bps: Dict[str, List[float]] = {}
        for _sid, blob in (await self.raw.hgetall(ACTIVE_KEY) or {}).items():
            sig = _loads(blob)
            if sig and sig.get("instrument") and sig.get("expected_bps") is not None:
                sign = 1 if sig.get("direction") == "long" else -1
                active_bps.setdefault(str(sig["instrument"]).upper(), []).append(sign * float(sig["expected_bps"]))
        meta = await self._asset_meta()
        screened_out: Dict[str, int] = {}
        for h, cands in plans.items():
            kept = {1: [], -1: []}
            kept_returns: Dict[int, List[List[float]]] = {1: [], -1: []}
            for c in cands:
                px = prices.get(c["ticker"])
                last = px["close"][-1] if px and px["close"] else None
                dv = (sum(cl * v for cl, v in zip(px["close"][-21:], px["volume"][-21:])) / len(px["close"][-21:])
                      if px and px["close"] else None)
                why = rc.idea_screen(c["direction"], meta.get(c["ticker"]) if meta else None, last, dv,
                                     rc.realized_vol(px["close"]) if px else None)
                if why:
                    screened_out[why.split(" (")[0]] = screened_out.get(why.split(" (")[0], 0) + 1
                    continue
                if h != EVENT_HORIZON and c["ticker"] in boards:
                    against = rc.overruled(c["direction"], supports.get(h), boards[c["ticker"]])
                    if against:
                        overruled.append({"ticker": c["ticker"], "horizon": h, "direction": c["direction"],
                                          "why": against})
                        continue
                if len(kept[c["direction"]]) < IDEAS_PER_SIDE:
                    rets = rc.log_returns(px["close"]) if px and px["close"] else []
                    if h != EVENT_HORIZON and rc.too_close(rets, kept_returns[c["direction"]]):
                        screened_out["moves with an idea already on its side"] = \
                            screened_out.get("moves with an idea already on its side", 0) + 1
                        continue
                    kept[c["direction"]].append(c)
                    kept_returns[c["direction"]].append(rets)
            plans[h] = kept[1] + kept[-1]
            bets[h] = {"long": rc.effective_bets(kept_returns[1]), "short": rc.effective_bets(kept_returns[-1]),
                       "long_ideas": len(kept[1]), "short_ideas": len(kept[-1])}

        out: Dict[str, Any] = {"screened_out": screened_out, "overruled": overruled,
                               "generated_at": datetime.now(timezone.utc).isoformat(),
                               "as_of": report.get("as_of"), "primary_horizon": report.get("primary_horizon"),
                               "horizons": {}, "intraday": await self._intraday()}
        logged = 0
        spy_now = await self._live_price("SPY", None)
        timeout = aiohttp.ClientTimeout(total=20)
        # The news read (trial M, shared/utils/news_embed.py): each stock's company
        # news since its last close, as expected bps over a week against other
        # news names. Weekly ideas only, the horizon it was validated at.
        news_model = _loads(await self.raw.get(ne.MODEL_KEY)) or ne.packaged_model()
        news_today = ne.news_day(datetime.now(timezone.utc))
        news_days: Dict[date, Dict[str, Any]] = {}

        async def news_read(ticker: str, last_close: Any) -> Optional[float]:
            if not news_model or last_close is None:
                return None
            last = last_close if isinstance(last_close, date) else date.fromisoformat(str(last_close)[:10])
            if last >= today and eastern_now().hour < 16:
                # A bar for a session still open is not its close: the news
                # read counts from the last completed one.
                last = today - timedelta(days=1)
                while last.weekday() >= 5:
                    last -= timedelta(days=1)
            totals = []
            for d in ne.days_since(last, news_today)[-7:]:
                if d not in news_days:
                    got = await self.raw.hgetall(ne.day_key(d)) or {}
                    news_days[d] = {(k.decode() if isinstance(k, bytes) else str(k)): _loads(v) for k, v in got.items()}
                totals.append(news_days[d].get(ticker))
            return ne.expected_bps(ne.merge(totals), news_model)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for h, ideas in plans.items():
                meta = horizons.get(h) or EVENT_META
                chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
                leg_bps = chosen.get("ls_net_bps")
                expected = leg_bps / 2 if isinstance(leg_bps, (int, float)) else None
                ic = chosen.get("mean_ic") if h != EVENT_HORIZON else None
                window = rc.EXPIRY_WINDOWS.get(h, rc.EXPIRY_WINDOWS["weekly"])   # event ideas are held a week
                rows = []
                for idea in ideas:
                    t = idea["ticker"]
                    px = prices.get(t)
                    if not px or not px["close"]:
                        continue
                    price = await self._live_price(t, px["close"][-1])
                    hd = meta["horizon_days"]
                    rv = rc.realized_vol(px["close"])
                    pos = _loads(await self.raw.get(positioning_key(t))) or {}
                    nxt_e = rc.next_earnings(last_results.get(t), today)
                    chain: list = []
                    by_exp: Dict[str, List[rc.Leg]] = {}
                    try:
                        chain = await self._chain(session, t, today, window)
                        by_exp = rc.tradable_chain(chain, today, window)
                    except Exception as e:
                        swallowed("agents.recommendation_engine.chain", e, logger, detail=t)
                    atm = pos.get("atm_iv") or _atm_iv(by_exp)
                    # How far, not only which way (shared/utils/expected_returns.py).
                    sigma = er.forecast_vol(px["close"], atm, rc.earnings_inside(nxt_e, today, hd),
                                            earnings_move.get(t), hd)
                    # The alpha's scale is the trailing 63-day volatility the lab's
                    # calibration was fitted on (portfolio_lab.alpha_calibration);
                    # the forecast volatility prices the risk around it.
                    news_bps = await news_read(t, px["day"][-1]) if h == "weekly" else None
                    a = er.with_events(er.calibrated(er.alpha(ic, rc.realized_vol(px["close"], CALIBRATION_VOL_WINDOW),
                                                              er.z_of(idea.get("percentile")), hd),
                                                     h, calib.get(h)),
                                       active_bps.get(t, []) + ([news_bps] if news_bps else []))
                    if a is not None and a * idea["direction"] < 0:
                        # The forecast runs against the rank's side: events outweigh the
                        # model, or the calibration expects even this rank to trail SPY
                        # (top-ranked low-volatility names did, trial K2). No idea.
                        why = "forecast runs against the rank" if not active_bps.get(t) else "events outweigh the model"
                        screened_out[why] = screened_out.get(why, 0) + 1
                        continue
                    # Costs at the size the idea would be taken (shared/utils/trading_costs.py):
                    # the stock's own spread and square-root impact against its consolidated
                    # dollar volume, and for a short its quoted borrow over the hold.
                    adv = (sum(cl * v for cl, v in zip(px["close"][-21:], px["volume"][-21:]))
                           / len(px["close"][-21:]) / DAILY_BAR_VOLUME_SHARE)
                    daily_vol = rv / math.sqrt(252) if rv else None
                    spread = tc.effective_spread(px["high"], px["low"], px["close"])
                    borrow_cost = 0.0 if idea["direction"] > 0 else borrow.get(t, 0.0) / 100.0 * hd / 365.0
                    size = tc.sized(a, sigma, hd, adv, daily_vol, spread, borrow_cost)
                    size["weight"] = round(min(0.05, size["weight"] * mom_scale
                                               * (short_tilt if idea["direction"] < 0 else 1.0)), 4)
                    cost = size["cost"]
                    meta_p = None
                    if meta_model and h == "monthly" and spy_px and idea.get("percentile") is not None:
                        meta_p = ml.probability(meta_model, ml.live_features(
                            float(idea["percentile"]), idea["direction"], px["close"], px["volume"], spy_px["close"]))
                        if meta_p is not None:
                            # meta_label.size_from_probability: nothing at or below a coin flip.
                            size["weight"] = round(size["weight"] * max(0.0, min(1.0, 2.0 * meta_p - 1.0)), 4)
                    plan = rc.stock_plan(idea["direction"], price, rc.atr(px["high"], px["low"], px["close"]), hd,
                                         round(a * 1e4, 1) if a is not None else expected)
                    forecast = {"expected_excess_bps": round(a * 1e4, 1) if a is not None else None,
                                "vol_annual": round(sigma, 4) if sigma else None,
                                "prob_beats_spy": round(er.prob_beats(a, sigma, hd), 3) if a is not None and sigma else None,
                                "stock_weight": size["weight"],
                                "edge_after_costs": bool(a is not None and abs(a) > cost),
                                # Round trip at that weight of a REFERENCE_ACCOUNT_USD account.
                                "cost_bps": round(cost * 1e4, 1),
                                "spread_bps": round(2 * tc.half_spread(adv, spread) * 1e4, 1),
                                "news_bps": news_bps,
                                "meta_p": round(meta_p, 3) if meta_p is not None else None,
                                "capacity_usd": round(size["capacity_usd"], -3),
                                # Where the forecast puts the price at the horizon: its excess
                                # plus the market's usual drift.
                                "target": round(price * (1 + (a or 0.0) + MARKET_DRIFT * hd / 252), 2)
                                if price else None}
                    regime = rc.iv_regime(atm, rv)
                    option = None
                    try:
                        if sigma and price:
                            quotes, dte = rc.quotes_by_expiry(chain, today, window)
                            mu = (a or 0.0) * rc_annualize(hd) + MARKET_DRIFT
                            # Each expiry carries the earnings move only if the release
                            # falls before it, spread over its own life (oe.vol_to).
                            jump = (((nxt_e - today).days, earnings_move.get(t) or er.DEFAULT_EARNINGS_MOVE)
                                    if nxt_e else None)
                            option = rc.ev_option_idea(oe.best(idea["direction"], quotes, dte, price, mu,
                                                               er.forecast_vol(px["close"], atm), hd / 252.0,
                                                               MARKET_DRIFT, jump))
                        if option:
                            option.update(iv_regime=regime, atm_iv=round(atm, 4) if atm else None,
                                          realized_vol=round(rv, 4) if rv else None)
                    except Exception as e:
                        swallowed("agents.recommendation_engine.option_ev", e, logger, detail=t)
                    nxt = rc.next_earnings(last_results.get(t), today)
                    bal = boards.get(t) or {}
                    # For the cards: the last quarter of closes, the live price, and
                    # its move since the last completed close.
                    prev_close = px["close"][-1] if px["day"][-1] < today else (
                        px["close"][-2] if len(px["close"]) > 1 else None)
                    row = {**idea, **plan, "forecast": forecast, "realized_vol": round(rv, 4) if rv else None,
                           "spark": [round(float(c), 2) for c in px["close"][-63:]],
                           "price": round(float(price), 2) if price else None,
                           "change_pct": round((price / prev_close - 1.0) * 100, 2) if price and prev_close else None,
                           "cp_iv_spread": pos.get("cp_iv_spread"), "iv_skew": pos.get("iv_skew"),
                           "option": option,
                           # Why, in words: the model's own contributions (linear models).
                           "reasons": idea_reasons(idea.get("drivers"), idea["direction"]),
                           "earnings": {"next_estimated": str(nxt) if nxt else None,
                                        "last": str(last_results.get(t)) if last_results.get(t) else None,
                                        "inside_hold": rc.earnings_inside(nxt, today, meta["horizon_days"])},
                           # What the Signal Board says, and the event evidence each way.
                           "board": {"headline": (verdicts.get(t) or {}).get("headline"),
                                     "confidence": (verdicts.get(t) or {}).get("confidence"),
                                     "for": bal.get("bull_reasons" if idea["direction"] > 0 else "bear_reasons") or [],
                                     "against": bal.get("bear_reasons" if idea["direction"] > 0 else "bull_reasons") or []},
                           "risk": rc.risk_terms(idea["direction"], plan.get("entry"), plan.get("stop"), adv, rv)}
                    rows.append(row)
                    try:
                        await self.db.execute(
                            _LOG_SQL, today, h, t, idea["direction"], bool(meta.get("validated")),
                            plan["entry"], plan["stop"], plan["expected_excess_bps"], meta.get("chosen"),
                            option["structure"] if option else None, option if option else None,   # jsonb codec encodes (0052)
                            date.fromisoformat(report["as_of"]) if report.get("as_of") else today,
                            spy_now,
                        )
                        logged += 1
                    except Exception as e:
                        swallowed("agents.recommendation_engine.log", e, logger, detail=t)
                book = None
                try:
                    book = book_for(rows, prices, industries)
                except Exception as e:
                    swallowed("agents.recommendation_engine.book", e, logger, detail=h)
                out["horizons"][h] = {
                    "book": book,
                    "withheld": withheld.get(h),
                    "horizon_days": meta["horizon_days"], "validated": bool(meta.get("validated")),
                    "model": meta.get("chosen"), "net_sharpe": meta.get("net_sharpe"),
                    "deflated_sharpe": meta.get("deflated_sharpe"), "expected_excess_bps": expected,
                    "long_note": long_note(chosen.get("exposures") or {}) if h != EVENT_HORIZON else None,
                    "effective_bets": bets.get(h),
                    "ideas": rows,
                }
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await self._signal_options(session, out["intraday"], today)
        except Exception as e:
            swallowed("agents.recommendation_engine.signal_options", e, logger)
        out["consensus"] = consensus(out["horizons"])
        # The day's backdrop for the home page: the regime gauge, the sizing
        # dial the ideas carry (momentum-volatility scaling), and SPY today.
        spy_bars = (await self._prices(["SPY"])).get("SPY") or {}
        spy_prev = None
        if spy_bars.get("close"):
            spy_prev = spy_bars["close"][-1] if spy_bars["day"][-1] < today else (
                spy_bars["close"][-2] if len(spy_bars["close"]) > 1 else None)
        out["pulse"] = {"regime": gauge.get("level"), "sizing_scale": round(mom_scale, 3),
                        "momentum_vol_scale": gauge.get("momentum_vol_scale"),
                        "spy": round(float(spy_now), 2) if spy_now else None,
                        "spy_change_pct": round((spy_now / spy_prev - 1.0) * 100, 2) if spy_now and spy_prev else None}
        try:
            out["book_effective_bets"] = await self._book_bets()
        except Exception as e:
            swallowed("agents.recommendation_engine.book_bets", e, logger)
        await self.raw.set(RECOMMENDATIONS_KEY, json.dumps(out, default=str), ex=RESULT_TTL_SEC)
        return {h: len(v["ideas"]) for h, v in out["horizons"].items()} | {
            "options": sum(1 for v in out["horizons"].values() for i in v["ideas"] if i.get("option")),
            "intraday": len(out["intraday"]), "logged": logged}

    async def _signal_options(self, session: aiohttp.ClientSession, signals: List[Dict[str, Any]],
                              today: date) -> None:
        """An option for each event signal held long enough for a listed expiry,
        priced under the family's measured move (shared/utils/option_ev.py)."""
        held = [s for s in signals if SIGNAL_OPTION_WINDOWS.get(str(s.get("horizon")))]
        if not held:
            return
        prices = await self._prices(sorted({str(s["instrument"]).upper() for s in held}))
        for sig in held:
            t = str(sig["instrument"]).upper()
            px = prices.get(t)
            if not px or not px["close"]:
                continue
            window, hd = SIGNAL_OPTION_WINDOWS[str(sig["horizon"])]
            direction = 1 if sig.get("direction") == "long" else -1
            try:
                chain = await self._chain(session, t, today, window)
                quotes, dte = rc.quotes_by_expiry(chain, today, window)
                pos = _loads(await self.raw.get(positioning_key(t))) or {}
                sigma = er.forecast_vol(px["close"], pos.get("atm_iv") or _atm_iv(rc.tradable_chain(chain, today, window)),
                                        False, None, hd)
                price = await self._live_price(t, px["close"][-1])
                if not sigma or not price:
                    continue
                a = direction * float(sig.get("expected_bps") or 0.0) / 1e4
                pick = oe.best(direction, quotes, dte, price, a * rc_annualize(hd) + MARKET_DRIFT, sigma,
                               hd / 252.0, MARKET_DRIFT)
                sig["option"] = rc.ev_option_idea(pick)
                sig["forecast"] = {"expected_bps": round(a * 1e4, 1), "vol_annual": round(sigma, 4)}
            except Exception as e:
                swallowed("agents.recommendation_engine.signal_option", e, logger, detail=t)

    async def _book_bets(self) -> Optional[Dict[str, Any]]:
        """Independent bets in the headline model portfolio's two books, from
        their names' last 63 sessions of returns."""
        row = await self.db.query_one(_BOOK_SQL, mp.HEADLINE_PORTFOLIO)
        if not row:
            return None
        books = {side: [str(t).upper() for t in (_loads(row[col]) if isinstance(row[col], (str, bytes))
                                                 else row[col]) or []]
                 for side, col in (("long", "long_book"), ("short", "short_book"))}
        prices = await self._prices(sorted(set(books["long"]) | set(books["short"])))
        out: Dict[str, Any] = {"portfolio": mp.HEADLINE_PORTFOLIO}
        for side, names in books.items():
            series = [rc.log_returns(prices[t]["close"]) for t in names if t in prices and prices[t]["close"]]
            out[side] = rc.effective_bets(series)
            out[f"{side}_names"] = len(names)
        return out

    async def _intraday(self) -> List[Dict[str, Any]]:
        """The signal engine's live signals: measured families, net of SPY."""
        out = []
        for _sid, blob in (await self.raw.hgetall(ACTIVE_KEY) or {}).items():
            sig = _loads(blob)
            if sig:
                out.append(sig)
        return sorted(out, key=lambda s: -(s.get("probability") or 0))


def consensus(horizons: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One line per stock and direction across horizons: a name both the weekly
    and monthly models want, or one the events also back, says so once."""
    by: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for h, v in horizons.items():
        for i in v.get("ideas") or []:
            k = (i["ticker"], int(i["direction"]))
            e = by.setdefault(k, {"ticker": i["ticker"], "direction": int(i["direction"]), "horizons": [],
                                  "entry": i.get("entry"), "reasons": i.get("reasons") or [],
                                  "earnings": i.get("earnings"), "risk": i.get("risk"), "board": i.get("board")})
            e["horizons"].append(h)
            if not e["reasons"] and i.get("reasons"):
                e["reasons"] = i["reasons"]
    out = list(by.values())
    out.sort(key=lambda e: (-len(e["horizons"]), -e["direction"], e["ticker"]))
    return out


def long_note(expo: Dict[str, Any]) -> Optional[str]:
    """What a buy from this horizon has meant out of sample, in plain words."""
    vs_u, vs_spy = expo.get("long_vs_universe_bps"), expo.get("long_leg_bps")
    if vs_u is None or vs_spy is None:
        return None
    verdict = "beat" if vs_u > 0 else "trailed"
    tail = ("and the S&P 500 too" if vs_spy > 0 else
            "but the S&P 500, carried by a few very large companies, beat them; holding the index is the bar")
    return (f"Out of sample these picks {verdict} the average stock by {abs(vs_u) / 100:.2f}% a period, "
            f"{tail} ({vs_spy / 100:+.2f}% against it).")


def book_for(rows: List[Dict[str, Any]], prices: Dict[str, Dict[str, List[Any]]],
             industries: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The horizon's ideas as one book (shared.utils.book), on returns aligned by day."""
    spy = prices.get("SPY")
    if not spy:
        return None
    days = set(spy["day"])
    for r in rows:
        if r["ticker"] in prices:
            days &= set(prices[r["ticker"]]["day"])
    days_sorted = sorted(days)[-41:]
    if len(days_sorted) < 21:
        return None

    def rets(t: str) -> List[float]:
        by = dict(zip(prices[t]["day"], prices[t]["close"]))
        c = [by[d] for d in days_sorted]
        return [c[k] / c[k - 1] - 1.0 for k in range(1, len(c))]

    spy_r = rets("SPY")
    var_m = sum((x - sum(spy_r) / len(spy_r)) ** 2 for x in spy_r)
    ideas, returns = [], {}
    for r in rows:
        t = r["ticker"]
        if t not in prices:
            continue
        rr = rets(t)
        mr, ms = sum(spy_r) / len(spy_r), sum(rr) / len(rr)
        beta = sum((a - ms) * (b - mr) for a, b in zip(rr, spy_r)) / var_m if var_m else 1.0
        px = prices[t]
        dv = sum(c * v for c, v in zip(px["close"][-21:], px["volume"][-21:])) / len(px["close"][-21:])
        ideas.append({"ticker": t, "direction": r["direction"], "rv": r.get("realized_vol"), "beta": beta,
                      "adv_usd": dv / DAILY_BAR_VOLUME_SHARE, "industry": industries.get(t)})
        returns[t] = rr
    return build_book(ideas, returns)


def rc_annualize(horizon_days: int) -> float:
    """Multiplier from a horizon's return to an annual rate."""
    return 252.0 / max(horizon_days, 1)


def _atm_iv(by_exp: Dict[str, List[rc.Leg]]) -> Optional[float]:
    calls = [lg for legs in by_exp.values() for lg in legs if lg.right == "C" and lg.iv]
    if not calls:
        return None
    return min(calls, key=lambda lg: abs(lg.delta - 0.5)).iv


__all__ = ["RecommendationEngine"]
