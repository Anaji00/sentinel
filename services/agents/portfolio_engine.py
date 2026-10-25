"""
services/agents/portfolio_engine.py

Runs the model portfolio (shared/utils/model_portfolio.py) on paper: decides
its orders after each nightly model run, fills them at the next session's
open, and marks its value against SPY's after every completed session.
Idempotent: the state row says what is held, what is waiting to fill and how
far the value has been marked, so a restart picks up where it stopped.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from shared.utils import equity_regime as er
from shared.utils import model_portfolio as mp
from shared.utils import short_data as sd
from shared.utils.asset_universe import load_asset_meta
from shared.utils.signal_store import FACTOR_REPORT_KEY, factor_scores_key

logger = logging.getLogger("agent.portfolio")

INTERVAL_SEC = 30 * 60
FIRST_RUN_DELAY_SEC = 180
NY = ZoneInfo("America/New_York")
# A session is marked once nearly every name it needs has its bar, or once it
# is a few days old (the rest then count flat and the gap is recorded).
MIN_BAR_COVERAGE = 0.95
MAX_WAIT_DAYS = 3
FLAT_REASON = ("The monthly model no longer passes its evidence gates: the overlay is closed "
               "and the portfolio is plain SPY until it does.")

_STATE_SQL = "SELECT * FROM model_portfolio_state WHERE portfolio = $1"
_SAVE_SQL = """
    INSERT INTO model_portfolio_state (portfolio, long_book, short_book, pending, rebalanced_on, scores_as_of,
        marked_through, started_on, nav, spy_nav, sizing, updated_at)
    VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5::date, $6::date, $7::date, $8::date, $9, $10, $11::jsonb, NOW())
    ON CONFLICT (portfolio) DO UPDATE SET long_book = $2::jsonb, short_book = $3::jsonb, pending = $4::jsonb,
        rebalanced_on = $5::date, scores_as_of = $6::date, marked_through = $7::date, started_on = $8::date,
        nav = $9, spy_nav = $10, sizing = $11::jsonb, updated_at = NOW()
"""
_ORDER_SQL = """
    INSERT INTO model_portfolio_orders (portfolio, decided_on, ticker, action, kind, reason, percentile)
    VALUES ($1, $2::date, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING
"""
_FILL_SQL = """
    UPDATE model_portfolio_orders SET fill_day = $3::date, fill_price = $5
    WHERE portfolio = $1 AND decided_on = $2::date AND ticker = $4 AND fill_day IS NULL
"""
_NAV_SQL = """
    INSERT INTO model_portfolio_nav (portfolio, day, nav, spy_nav, ret, spy_ret, long_ret, short_ret, cost,
        borrow, n_long, n_short, missing, gross, core)
    VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
    ON CONFLICT (portfolio, day) DO NOTHING
"""
_SESSIONS_SQL = "SELECT day FROM daily_bars WHERE ticker = 'SPY' AND day >= $1::date ORDER BY day"
_SESSIONS_BETWEEN_SQL = "SELECT count(*) AS n FROM daily_bars WHERE ticker = 'SPY' AND day > $1::date AND day <= $2::date"
# The latest quoted borrow fees (shared/utils/short_data.py) on or before a day.
_FEES_SQL = """
    SELECT ticker, fee_rate FROM borrow_rates
    WHERE day = (SELECT max(day) FROM borrow_rates WHERE day <= $1::date) AND fee_rate IS NOT NULL
"""
_INDUSTRY_SQL = "SELECT ticker, sic_description FROM company_profile WHERE upper(ticker) = ANY($1::text[])"
_BARS_SQL = """
    SELECT ticker, day, open, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date
"""
_META_BARS_SQL = """
    SELECT ticker, day, close, volume FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > $2::date AND day <= $3::date ORDER BY ticker, day
"""
_CLOSES_SQL = """
    SELECT ticker, day, close FROM daily_bars
    WHERE ticker = ANY($1::text[]) AND day > $2::date AND day <= $3::date ORDER BY ticker, day
"""
# The equity-regime gauge counts only when this fresh.
REGIME_MAX_AGE = timedelta(days=5)


def _json(x: Any, default: Any) -> Any:
    """JSONB comes back from shared.db as text."""
    if x is None:
        return default
    if isinstance(x, (str, bytes)):
        try:
            return json.loads(x)
        except ValueError:
            return default
    return x


def _day(x: Any) -> Optional[date]:
    if x is None or isinstance(x, date):
        return x
    return date.fromisoformat(str(x)[:10])


class PortfolioEngine:
    def __init__(self, db: Any, redis: Any = None, portfolio: str = mp.PORTFOLIO, variant: str = "base"):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self.portfolio = portfolio
        # "base": the live book. "industry_neutral": the same scores ranked
        # within industry, run in shadow (shared/utils/model_portfolio.py).
        self.variant = variant

    async def run(self) -> None:
        await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                out = await self.run_once()
                if out.get("marked") or out.get("orders"):
                    logger.info("Model portfolio: %s", out)
            except Exception as exc:
                logger.error("Model portfolio failed: %s", exc, exc_info=True)
            await asyncio.sleep(INTERVAL_SEC)

    async def _state(self) -> Dict[str, Any]:
        row = await self.db.query_one(_STATE_SQL, self.portfolio) or {}
        return {
            "long": set(_json(row.get("long_book"), [])), "short": set(_json(row.get("short_book"), [])),
            "pending": _json(row.get("pending"), None), "rebalanced_on": _day(row.get("rebalanced_on")),
            "scores_as_of": _day(row.get("scores_as_of")), "marked_through": _day(row.get("marked_through")),
            "started_on": _day(row.get("started_on")), "nav": float(row.get("nav") or 1.0),
            "spy_nav": float(row.get("spy_nav") or 1.0), "sizing": _json(row.get("sizing"), []),
        }

    async def _save(self, st: Dict[str, Any]) -> None:
        await self.db.execute(
            _SAVE_SQL, self.portfolio, sorted(st["long"]), sorted(st["short"]),
            st["pending"] if st["pending"] else None, st["rebalanced_on"], st["scores_as_of"],
            st["marked_through"], st["started_on"], st["nav"], st["spy_nav"], st.get("sizing") or [])

    async def run_once(self) -> Dict[str, Any]:
        st = await self._state()
        out: Dict[str, Any] = {"marked": await self._mark(st)}
        out.update(await self._decide(st))
        await self._save(st)
        return out

    async def _mark(self, st: Dict[str, Any], today: Optional[date] = None) -> int:
        """Value every completed session not yet marked, applying a pending
        book at the first session after the day it was decided."""
        pending = st["pending"]
        if not st["started_on"] and not pending:
            return 0
        start = st["marked_through"] or _day(pending["decided_on"])
        today = today or datetime.now(NY).date()
        sessions = [_day(r["day"]) for r in await self.db.query(_SESSIONS_SQL, start) or []]
        if len(sessions) < 2:
            return 0
        names = st["long"] | st["short"] | set((pending or {}).get("long", [])) | set((pending or {}).get("short", []))
        bars: Dict[date, Dict[str, Tuple[Optional[float], Optional[float]]]] = defaultdict(dict)
        for r in await self.db.query(_BARS_SQL, sorted(names | {"SPY"}), sessions[0], sessions[-1]) or []:
            o = float(r["open"]) if r.get("open") else None
            c = float(r["close"]) if r.get("close") else None
            bars[_day(r["day"])][str(r["ticker"]).upper()] = (o, c)
        marked = 0
        fee_cache: Dict[date, Dict[str, float]] = {}
        for prev, d in zip(sessions, sessions[1:]):
            if d >= today:
                break
            prev_close = {t: c for t, (o, c) in bars[prev].items() if c}
            spy_pc, spy_c = prev_close.get("SPY"), bars[d].get("SPY", (None, None))[1]
            if not spy_pc or not spy_c:
                break
            fill = pending is not None and d > _day(pending["decided_on"])
            new_l = set(pending["long"]) if fill else st["long"]
            new_s = set(pending["short"]) if fill else st["short"]
            rec = mp.sizing_record(st.get("sizing") or [], d)
            weights = rec.get("weights") or None
            long_ret, miss_l = mp.leg_return(st["long"], new_l, prev_close, bars[d], weights)
            short_ret, miss_s = mp.leg_return(st["short"], new_s, prev_close, bars[d], weights)
            needed = len(st["long"] | new_l | st["short"] | new_s)
            missing = len(set(miss_l) | set(miss_s))
            if needed and missing / needed > 1 - MIN_BAR_COVERAGE and (today - d).days <= MAX_WAIT_DAYS:
                break
            if new_s and d not in fee_cache:
                fee_cache[d] = {str(r["ticker"]).upper(): float(r["fee_rate"])
                                for r in await self.db.query(_FEES_SQL, d) or []}
            gross, core = mp.sizing_for(st.get("sizing") or [], d)
            step = mp.day_step(st["nav"], st["spy_nav"], spy_c / spy_pc - 1.0, long_ret, short_ret,
                               mp.traded_fraction(st["long"], new_l), mp.traded_fraction(st["short"], new_s),
                               shorts_held=bool(new_s), gross=gross,
                               borrow_fee_annual=mp.book_fee(new_s, fee_cache.get(d, {})), core=core,
                               short_gross=gross * float(rec.get("short_tilt") or 1.0))
            await self.db.execute(_NAV_SQL, self.portfolio, d, step["nav"], step["spy_nav"], step["ret"],
                                  spy_c / spy_pc - 1.0, long_ret, short_ret, step["cost"], step["borrow"],
                                  len(new_l), len(new_s), missing, gross, core)
            if fill:
                changed = (st["long"] ^ new_l) | (st["short"] ^ new_s)
                rows = [(self.portfolio, _day(pending["decided_on"]), d, t, bars[d].get(t, (None, None))[0])
                        for t in sorted(changed)]
                if rows:
                    await self.db.execute_many(_FILL_SQL, rows)
                st["long"], st["short"], st["pending"], pending = new_l, new_s, None, None
                st["started_on"] = st["started_on"] or d
            st["nav"], st["spy_nav"], st["marked_through"] = step["nav"], step["spy_nav"], d
            marked += 1
        return marked

    async def _decide(self, st: Dict[str, Any]) -> Dict[str, Any]:
        """After a new model run: the full rebalance when due, otherwise only
        the exits a held name forces (unranked, or no longer borrowable)."""
        if self.raw is None or st["pending"]:
            return {}
        rep = _json(await self.raw.get(FACTOR_REPORT_KEY), {})
        as_of = _day(rep.get("as_of")) if rep else None
        if as_of is None or (st["scores_as_of"] and as_of <= st["scores_as_of"]):
            return {}
        scores = _json(await self.raw.get(factor_scores_key(mp.HORIZON)), {})
        meta = await load_asset_meta(self.raw)
        if not scores or not meta:
            return {}
        pct = {t: float(v["percentile"]) for t, v in scores.items() if v.get("percentile") is not None}
        if self.variant == "industry_neutral":
            # Within statistical return themes once the lab adopts them
            # (portfolio_lab.adoptions), else within SIC industries.
            groups = await self._themes() if (await self._adopted()).get("cluster_neutral") else None
            if not groups:
                groups = {str(r["ticker"]).upper(): r["sic_description"]
                          for r in await self.db.query(_INDUSTRY_SQL, sorted(pct)) or []}
            pct = mp.industry_relative(pct, groups)
        borrowable: Set[str] = {t for t, m in meta.items() if len(m) > 2 and m[1] and m[2]}
        # The broker's easy-to-borrow flag, narrowed by the quoted fee: a name
        # costing more than HARD_TO_BORROW_FEE a year is not shorted (or is covered).
        expensive = {str(r["ticker"]).upper() for r in await self.db.query(_FEES_SQL, as_of + timedelta(days=1)) or []
                     if float(r["fee_rate"]) > sd.HARD_TO_BORROW_FEE}
        borrowable -= expensive
        support = ((rep.get("horizons") or {}).get(mp.HORIZON) or {}).get("support")
        held = {1: st["long"], -1: st["short"]}
        why_flat: Optional[str] = None
        if not support:
            new: Dict[int, List[str]] = {1: [], -1: []}
            kind, why_flat = "rebalance", FLAT_REASON
        else:
            since = None
            if st["rebalanced_on"]:
                row = await self.db.query_one(_SESSIONS_BETWEEN_SQL, st["rebalanced_on"], as_of) or {}
                since = int(row.get("n") or 0)
            if mp.due(since):
                new = {1: mp.book(pct, held[1], 1), -1: mp.book(pct, held[-1], -1, borrowable)}
                kind = "rebalance"
                st["rebalanced_on"] = as_of
            else:
                new = {1: mp.maintain(held[1], pct), -1: mp.maintain(held[-1], pct, borrowable)}
                kind = "maintenance"
        ords = mp.orders(held, new, pct, borrowable, why_flat)
        st["scores_as_of"] = as_of
        try:
            sizing = await self._sizing(as_of, set(new[1]), set(new[-1]))
            st["sizing"] = (list(st.get("sizing") or []) + [{"decided_on": str(as_of), **sizing}])[-mp.SIZING_KEPT:]
        except Exception as exc:
            logger.warning("Model portfolio sizing not updated (%s): the last sizing stays in force", exc)
        if ords:
            await self.db.execute_many(_ORDER_SQL, [(self.portfolio, as_of, o.ticker, o.action, kind, o.reason,
                                                     o.percentile) for o in ords])
            st["pending"] = {"decided_on": str(as_of), "kind": kind, "long": new[1], "short": new[-1]}
        return {"orders": len(ords), "kind": kind, "as_of": str(as_of)}

    async def _adopted(self) -> Dict[str, bool]:
        from shared.utils import portfolio_lab as pl

        if self.raw is None:
            return {}
        return pl.adoptions(_json(await self.raw.get(pl.PORTFOLIO_LAB_KEY), {}))

    async def _themes(self) -> Dict[str, str]:
        from shared.utils import portfolio_lab as pl

        got = _json(await self.raw.get(pl.RETURN_CLUSTERS_KEY), {}) if self.raw is not None else {}
        return {str(t).upper(): f"theme {v}" for t, v in (got or {}).items()}

    async def _sizing(self, as_of: date, long: Set[str], short: Set[str]) -> Dict[str, Any]:
        """The overlay's size for the books taking effect after `as_of`
        (shared/utils/model_portfolio.py overlay_sizing)."""
        multiplier, level = 1.0, None
        if self.raw is not None:
            gauge = _json(await self.raw.get(er.EQUITY_REGIME_KEY), {})
            gauge_day = _day(gauge.get("as_of")) if gauge else None
            if gauge_day and as_of - gauge_day <= REGIME_MAX_AGE and gauge.get("multiplier") is not None:
                multiplier, level = float(gauge["multiplier"]), gauge.get("level")
                # Momentum's own volatility scales the overlay continuously
                # (Barroso & Santa-Clara), once the lab adopts it.
                if gauge.get("momentum_vol_scale") is not None and (await self._adopted()).get("momentum_vol_scale"):
                    multiplier *= float(gauge["momentum_vol_scale"])
        closes: Dict[str, List[Tuple[date, float]]] = defaultdict(list)
        names = sorted(long | short | {"SPY"})
        for r in await self.db.query(_CLOSES_SQL, names, as_of - timedelta(days=200), as_of) or []:
            if r.get("close"):
                closes[str(r["ticker"]).upper()].append((_day(r["day"]), float(r["close"])))
        rets: Dict[str, Dict[date, float]] = {}
        for t, series in closes.items():
            rets[t] = {d1: c1 / c0 - 1.0 for (d0, c0), (d1, c1) in zip(series, series[1:]) if c0 > 0}
        spy = rets.pop("SPY", {})
        out = mp.overlay_sizing(rets, spy, long, short, multiplier)
        out["regime"] = level
        adopted = await self._adopted()
        # The short leg leans with market sentiment once the lab adopts it.
        if adopted.get("sentiment_short_tilt") and gauge_day and as_of - gauge_day <= REGIME_MAX_AGE:
            out["short_tilt"] = float(gauge.get("short_tilt") or 1.0)
        # Each name sized by its modelled chance of paying, once two lab runs
        # confirm it (meta_label.confirmed).
        weights = await self._meta_weights(as_of, long, short)
        if weights:
            out["weights"] = weights
        return out

    async def _meta_weights(self, as_of: date, long: Set[str], short: Set[str]) -> Dict[str, float]:
        from shared.utils import meta_label as ml
        from shared.utils import portfolio_lab as pl

        lab = _json(await self.raw.get(pl.PORTFOLIO_LAB_KEY), {}) if self.raw is not None else {}
        model = lab.get("meta_model") if ml.confirmed(lab.get("meta_history") or []) else None
        if not model:
            return {}
        scores = _json(await self.raw.get(factor_scores_key(mp.HORIZON)), {})
        bars: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: {"close": [], "volume": []})
        for r in await self.db.query(_META_BARS_SQL, sorted(long | short | {"SPY"}), as_of - timedelta(days=400),
                                     as_of) or []:
            if r.get("close"):
                bars[str(r["ticker"]).upper()]["close"].append(float(r["close"]))
                bars[str(r["ticker"]).upper()]["volume"].append(float(r["volume"] or 0.0))
        spy = bars.get("SPY", {}).get("close") or []
        out: Dict[str, float] = {}
        for side, names in ((1, long), (-1, short)):
            for t in names:
                pct = (scores.get(t) or {}).get("percentile")
                b = bars.get(t)
                p = ml.probability(model, ml.live_features(float(pct), side, b["close"], b["volume"], spy)) \
                    if pct is not None and b else None
                if p is not None:
                    out[t] = round(max(0.0, min(1.0, 2.0 * p - 1.0)), 4)
        return out if any(v > 0 for v in out.values()) else {}


__all__ = ["PortfolioEngine"]
