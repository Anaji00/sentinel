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
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from shared.utils import recommendations as rc
from shared.utils.asset_universe import load_asset_meta, refresh_asset_meta
from shared.utils.equities import parse_occ_option_symbol
from shared.utils.market_session import Session, current_session, eastern_now
from shared.utils.options_positioning import positioning_key
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key
from shared.utils.trade_plan import model_support
from shared.utils.signal_store import (
    ACTIVE_KEY, FACTOR_REPORT_KEY, RECOMMENDATIONS_KEY, factor_scores_key,
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

_PRICES_SQL = """
    SELECT ticker, day, high, low, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 60 ORDER BY ticker, day
"""
_LOG_SQL = """
    INSERT INTO recommendation_log (day, horizon, ticker, direction, validated, entry_price, stop_price,
                                    expected_excess_bps, model, structure, option_idea, as_of)
    VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::date)
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
            d = out.setdefault(str(r["ticker"]).upper(), {"high": [], "low": [], "close": [], "volume": []})
            c = float(r["close"])
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
        withheld: Dict[str, str] = {}
        for h in HORIZON_ORDER:
            meta = horizons.get(h)
            if not meta:
                continue
            chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
            support = model_support({**chosen, "validated": meta.get("validated")})
            if not support:
                # A horizon whose model loses after costs, or has no reliable
                # out-of-sample ranking, produces no ideas -- only the reason.
                plans[h] = []
                withheld[h] = (f"{meta.get('chosen')} at this horizon: net Sharpe {chosen.get('net_sharpe')}, "
                               f"IC t {chosen.get('ic_t')} -- below the out-of-sample bar after costs")
                continue
            scores = _loads(await self.raw.get(factor_scores_key(h))) or {}
            plans[h] = rc.stock_ideas(scores, n=CANDIDATES_PER_SIDE)
            wanted += [i["ticker"] for i in plans[h]]
        prices = await self._prices(sorted(set(wanted)))
        meta = await self._asset_meta()
        screened_out: Dict[str, int] = {}
        for h, cands in plans.items():
            kept = {1: [], -1: []}
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
                if len(kept[c["direction"]]) < IDEAS_PER_SIDE:
                    kept[c["direction"]].append(c)
            plans[h] = kept[1] + kept[-1]

        out: Dict[str, Any] = {"screened_out": screened_out,
                               "generated_at": datetime.now(timezone.utc).isoformat(),
                               "as_of": report.get("as_of"), "primary_horizon": report.get("primary_horizon"),
                               "horizons": {}, "intraday": await self._intraday()}
        logged = 0
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for h, ideas in plans.items():
                meta = horizons[h]
                chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
                leg_bps = chosen.get("ls_net_bps")
                expected = leg_bps / 2 if isinstance(leg_bps, (int, float)) else None
                window = rc.EXPIRY_WINDOWS[h]
                rows = []
                for idea in ideas:
                    t = idea["ticker"]
                    px = prices.get(t)
                    if not px or not px["close"]:
                        continue
                    price = await self._live_price(t, px["close"][-1])
                    plan = rc.stock_plan(idea["direction"], price, rc.atr(px["high"], px["low"], px["close"]),
                                         meta["horizon_days"], expected)
                    rv = rc.realized_vol(px["close"])
                    pos = _loads(await self.raw.get(positioning_key(t))) or {}
                    option = None
                    try:
                        chain = await self._chain(session, t, today, window)
                        by_exp = rc.tradable_chain(chain, today, window)
                        atm = pos.get("atm_iv") or _atm_iv(by_exp)
                        regime = rc.iv_regime(atm, rv)
                        option = rc.build_option_idea(rc.choose_structure(idea["direction"], regime),
                                                      by_exp, today, window)
                        if option:
                            option.update(iv_regime=regime, atm_iv=round(atm, 4) if atm else None,
                                          realized_vol=round(rv, 4) if rv else None)
                    except Exception as e:
                        swallowed("agents.recommendation_engine.chain", e, logger, detail=t)
                    row = {**idea, **plan, "realized_vol": round(rv, 4) if rv else None,
                           "cp_iv_spread": pos.get("cp_iv_spread"), "iv_skew": pos.get("iv_skew"),
                           "option": option}
                    rows.append(row)
                    try:
                        await self.db.execute(
                            _LOG_SQL, today, h, t, idea["direction"], bool(meta.get("validated")),
                            plan["entry"], plan["stop"], plan["expected_excess_bps"], meta.get("chosen"),
                            option["structure"] if option else None, json.dumps(option) if option else None,
                            date.fromisoformat(report["as_of"]) if report.get("as_of") else today,
                        )
                        logged += 1
                    except Exception as e:
                        swallowed("agents.recommendation_engine.log", e, logger, detail=t)
                out["horizons"][h] = {
                    "withheld": withheld.get(h),
                    "horizon_days": meta["horizon_days"], "validated": bool(meta.get("validated")),
                    "model": meta.get("chosen"), "net_sharpe": meta.get("net_sharpe"),
                    "deflated_sharpe": meta.get("deflated_sharpe"), "expected_excess_bps": expected,
                    "ideas": rows,
                }
        await self.raw.set(RECOMMENDATIONS_KEY, json.dumps(out, default=str), ex=RESULT_TTL_SEC)
        return {h: len(v["ideas"]) for h, v in out["horizons"].items()} | {
            "options": sum(1 for v in out["horizons"].values() for i in v["ideas"] if i.get("option")),
            "intraday": len(out["intraday"]), "logged": logged}

    async def _intraday(self) -> List[Dict[str, Any]]:
        """The signal engine's live signals: measured families, net of SPY."""
        out = []
        for _sid, blob in (await self.raw.hgetall(ACTIVE_KEY) or {}).items():
            sig = _loads(blob)
            if sig:
                out.append(sig)
        return sorted(out, key=lambda s: -(s.get("probability") or 0))


def _atm_iv(by_exp: Dict[str, List[rc.Leg]]) -> Optional[float]:
    calls = [lg for legs in by_exp.values() for lg in legs if lg.right == "C" and lg.iv]
    if not calls:
        return None
    return min(calls, key=lambda lg: abs(lg.delta - 0.5)).iv


__all__ = ["RecommendationEngine"]
