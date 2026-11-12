"""
services/agents/divergence_watch.py

Raises an alert when a liquid stock moves against its peers on its own news (shared/utils/divergence.py;
reports/Divergence alert.md for the case it was built from and the tests it passed).

Scheduled, not reactive, like the signal engine: every SCAN_SEC during extended hours on weekdays it reads each
liquid stock's last price against its previous close, compares that with the same-moment move of its measured
sympathy peers and of its four-digit-SIC industry group, and publishes one correlation for each stock that
qualifies. No model is called. The daily history behind each stock's gap is rebuilt once a New York day.

Published as ALERT, or ELEVATED when news names the stock alone and the gap is 4 sigma or more. Never
INTELLIGENCE: that tier starts a seven-billion-parameter scenario on the CPU for what is a price measurement.
A stock alerts once a day, and again only if its gap grows by ELEVATED_STEP sigma, at most MAX_PER_DAY times.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from services.correlation.event_store import EventStore
from shared.kafka import SentinelProducer, Topics
from shared.models.events import AlertTier, CorrelationCluster
from shared.utils import divergence as dv
from services.agents import regime_annotation
from services.agents import peer_residual_scan
from shared.utils import intel_brief, situation
from shared.utils.quiet_failures import dropped, swallowed

logger = logging.getLogger("agent.divergence_watch")

SCAN_SEC = 600
UNIVERSE = 1000
HISTORY_SESSIONS = 62
MAX_PER_DAY = 3
ELEVATED_STEP = 1.5
FIRED_KEY = "sentinel:divergence:fired:{day}:{ticker}"
FIRED_TTL = 20 * 3600
STATE_KEY = "sentinel:situation:{ticker}"
ACTIVE_KEY = "sentinel:situation:active"
STATE_TTL = 10 * 86400
TOP_PEERS = 8
MIN_GROUP = 6
STALE_DAYS = 5          # newest daily bar older than this (a long weekend is 4): do not scan

try:
    from zoneinfo import ZoneInfo
    _NY = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    _NY = timezone(timedelta(hours=-5))

_LIQUID_SQL = """
    SELECT ticker FROM daily_bars WHERE day >= CURRENT_DATE - 30 AND close > 0
    GROUP BY ticker HAVING avg(close * volume) > 2e7 ORDER BY avg(close * volume) DESC LIMIT $1
"""
_HISTORY_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND close > 0 ORDER BY ticker, day"
_LAST_SQL = """
    SELECT DISTINCT ON (ticker) ticker, close FROM tradfi_bars
    WHERE ticker = ANY($1::text[]) AND time > now() - interval '45 minutes' AND close > 0 ORDER BY ticker, time DESC
"""
_PEERS_SQL = "SELECT ticker, peer, corr FROM sympathy_peers"
_SIC_SQL = "SELECT ticker, sic FROM company_profile WHERE sic IS NOT NULL"
_NEWS_SQL = """
    SELECT symbols[1] AS ticker, count(*) AS n, (array_agg(headline ORDER BY created_at DESC))[1] AS headline
    FROM stock_news WHERE cardinality(symbols) = 1 AND created_at > $1 GROUP BY 1
"""


def previous_close_cutoff(now: datetime) -> datetime:
    """16:00 New York on the weekday before now's session. News after it, weekend news included, belongs to this session."""
    d = now.astimezone(_NY).date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return datetime(d.year, d.month, d.day, 16, 0, tzinfo=_NY).astimezone(timezone.utc)


def in_scan_window(now: datetime) -> bool:
    """Weekdays 08:00 to 22:00 UTC: pre-market through the close, when the last price is a live one."""
    return now.weekday() < 5 and 8 <= now.hour < 22


def group_members(sic: Dict[str, int], universe: List[str]) -> Dict[str, List[str]]:
    """Each stock's industry group: the other universe stocks sharing its four-digit SIC code, if there are enough."""
    by_sic: Dict[int, List[str]] = {}
    for t in universe:
        if t in sic:
            by_sic.setdefault(sic[t], []).append(t)
    return {t: [x for x in by_sic[sic[t]] if x != t] for t in universe
            if t in sic and len(by_sic[sic[t]]) - 1 >= MIN_GROUP}


class DivergenceWatch:
    def __init__(self, db_client: Any, redis_client: Any) -> None:
        self.db, self.redis = db_client, redis_client
        self._day: Optional[date] = None
        self._last_bar: date = date.min
        self._tickers: List[str] = []
        self._prev: Dict[str, float] = {}
        self._rets: Optional[np.ndarray] = None
        self._col: Dict[str, int] = {}
        self._refs: Dict[str, Dict[str, List[str]]] = {}
        self._producer: Optional[SentinelProducer] = None

    async def run(self) -> None:
        self._producer = SentinelProducer(service_name="divergence-watch")
        await self._producer.start()
        await asyncio.sleep(90)
        try:
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    if in_scan_window(now):
                        summary = await self.scan(now)
                        logger.info("Divergence scan: %s", summary)
                except Exception as exc:
                    swallowed("agents.divergence_watch.scan", exc, logger)
                await asyncio.sleep(SCAN_SEC)
        finally:
            await self._producer.close()

    async def _build(self, today: date) -> None:
        """Daily history, peers and industry groups for the liquid universe; once per New York day."""
        tickers = [str(r["ticker"]) for r in await self.db.query(_LIQUID_SQL, UNIVERSE) or []]
        since = today - timedelta(days=HISTORY_SESSIONS * 7 // 5 + 6)
        rows = await self.db.query(_HISTORY_SQL, tickers, since) or []
        days = sorted({r["day"] for r in rows if r["day"] < today})
        di = {d: i for i, d in enumerate(days)}
        col = {t: i for i, t in enumerate(tickers)}
        close = np.full((len(days), len(tickers)), np.nan)
        for r in rows:
            if r["day"] in di and r["ticker"] in col:
                close[di[r["day"]], col[r["ticker"]]] = float(r["close"])
        with np.errstate(all="ignore"):
            rets = close[1:] / close[:-1] - 1.0
        self._rets, self._col, self._tickers = rets[-(HISTORY_SESSIONS - 2):], col, tickers
        self._prev = {t: float(close[-1, col[t]]) for t in tickers if len(days) and np.isfinite(close[-1, col[t]])}
        peers: Dict[str, List[str]] = {}
        for r in sorted(await self.db.query(_PEERS_SQL) or [], key=lambda x: -float(x["corr"])):
            if r["ticker"] in col and r["peer"] in col and len(peers.setdefault(r["ticker"], [])) < TOP_PEERS:
                peers[r["ticker"]].append(str(r["peer"]))
        sic = {str(r["ticker"]): int(r["sic"]) for r in await self.db.query(_SIC_SQL) or []}
        groups = group_members(sic, tickers)
        self._refs = {t: {k: v for k, v in (("sympathy", peers.get(t, [])), ("industry", groups.get(t, []))) if len(v) >= dv.MIN_MEMBERS}
                      for t in tickers}
        self._last_bar = days[-1] if days else date.min
        self._day = today

    def _ref_history(self, members: List[str], own: str) -> Tuple[np.ndarray, np.ndarray]:
        """(past reference means, past gaps) over the stored sessions."""
        assert self._rets is not None
        idx = [self._col[m] for m in members if m in self._col]
        block = self._rets[:, idx]
        with np.errstate(all="ignore"):
            cnt = np.isfinite(block).sum(axis=1)
            mean = np.where(cnt >= dv.MIN_MEMBERS, np.nanmean(np.where(np.isfinite(block), block, np.nan), axis=1), np.nan)
        return mean, self._rets[:, self._col[own]] - mean

    async def scan(self, now: datetime) -> Dict[str, int]:
        today = now.astimezone(_NY).date()
        if self._day != today:
            await self._build(today)
        if self._rets is None or not self._tickers:
            return {"universe": 0, "fired": 0}
        # Daily bars that stopped loading would make every "move" a multi-day one. Say so and do not scan.
        if (today - self._last_bar).days > STALE_DAYS:
            dropped("agents.divergence_watch.stale_bars", f"last daily bar is {self._last_bar}", logger)
            return {"universe": len(self._tickers), "priced": 0, "fired": 0, "stale": 1}
        last = {str(r["ticker"]): float(r["close"]) for r in await self.db.query(_LAST_SQL, self._tickers) or []}
        news = {str(r["ticker"]): r for r in await self.db.query(_NEWS_SQL, previous_close_cutoff(now)) or []}
        ret = {t: last[t] / self._prev[t] - 1.0 for t in last if t in self._prev and self._prev[t] > 0}
        found = await asyncio.to_thread(self._detect_all, ret, frozenset(news))       # numpy over ~1,000 stocks: off the loop
        p1 = await self._peer_residual(ret, frozenset(news), now)
        found = peer_residual_scan.merge(found, p1)
        fired = skipped = 0
        for d in found:
            if await self._publish(d, now, news.get(d.ticker), ret):
                fired += 1
            else:
                skipped += 1
        closed = await self._close_quiet({d.ticker for d in found}, ret, now)
        return {"universe": len(self._tickers), "priced": len(ret), "fired": fired, "repeats_held_back": skipped, "situations_closed": closed}

    async def _peer_residual(self, ret: Dict[str, float], news: frozenset, now: datetime) -> List[dv.PeerDivergence]:
        """The registered multi-horizon rule P1 for the session so far and five sessions (services/agents/peer_residual_scan.py).
        A failure is counted and the D2 detection goes on without it."""
        out: List[dv.PeerDivergence] = []
        try:
            now_ny = now.astimezone(_NY)
            if peer_residual_scan.in_session(now_ny):
                cands = await asyncio.to_thread(peer_residual_scan.stage_one, self, ret, news)
                out += await peer_residual_scan.session_hits(self.db, cands, now_ny)
            out += await asyncio.to_thread(peer_residual_scan.five_day, self, ret, news)
        except Exception as exc:
            swallowed("agents.divergence_watch.peer_residual", exc, logger)
        return out

    def _current_gap(self, ticker: str, ret: Dict[str, float]) -> Tuple[Optional[float], bool]:
        """(the gap z against the reference with the largest one, whether the stock moved with that reference) right now."""
        r = ret.get(ticker)
        best: Optional[float] = None
        same = False
        if r is None:
            return None, False
        for members in self._refs.get(ticker, {}).values():
            ref_now = dv.mean_return([ret.get(m) for m in members])
            if ref_now is None:
                continue
            _, gaps = self._ref_history(members, ticker)
            z = dv.gap_z(r - ref_now, [float(g) for g in gaps if np.isfinite(g)])
            if z is not None and (best is None or abs(z) > abs(best)):
                best, same = z, (r > 0) == (ref_now > 0)
        return best, same

    async def _close_quiet(self, flagged: set, ret: Dict[str, float], now: datetime) -> int:
        """Resolve open situations whose stock is no longer flagged and whose gap has closed."""
        raw = getattr(self.redis, "raw", self.redis)
        session = now.astimezone(_NY).date()
        closed = 0
        try:
            for t_raw in await raw.smembers(ACTIVE_KEY) or []:
                t = t_raw.decode() if isinstance(t_raw, bytes) else str(t_raw)
                if t in flagged:
                    continue
                blob = await raw.get(STATE_KEY.format(ticker=t))
                state = json.loads(blob) if blob else None
                last = date.fromisoformat(state["last_session"]) if state else session
                z, same = await asyncio.to_thread(self._current_gap, t, ret)
                new = situation.on_quiet(state, z, same, session.isoformat(), (session - last).days)
                if new is not None:
                    await raw.set(STATE_KEY.format(ticker=t), json.dumps(new), ex=STATE_TTL)
                    await raw.srem(ACTIVE_KEY, t)
                    closed += 1
                elif state is None:
                    await raw.srem(ACTIVE_KEY, t)
        except Exception as exc:
            swallowed("agents.divergence_watch.close_quiet", exc, logger)
        return closed

    async def _situation(self, d: dv.Divergence, session: str) -> Optional[Dict[str, Any]]:
        """Advance and store the stock's situation; None if the store failed (counted), and the alert goes without a stage."""
        raw = getattr(self.redis, "raw", self.redis)
        try:
            blob = await raw.get(STATE_KEY.format(ticker=d.ticker))
            state = situation.on_detection(json.loads(blob) if blob else None, d.z, d.news, session)
            await raw.set(STATE_KEY.format(ticker=d.ticker), json.dumps(state), ex=STATE_TTL)
            await raw.sadd(ACTIVE_KEY, d.ticker)
            return state
        except Exception as exc:
            swallowed("agents.divergence_watch.situation", exc, logger)
            return None

    def _detect_all(self, ret: Dict[str, float], with_news: frozenset) -> List[dv.Divergence]:
        out: List[dv.Divergence] = []
        for t, r in ret.items():
            refs: Dict[str, dv.Reference] = {}
            for name, members in self._refs.get(t, {}).items():
                now_ret = dv.mean_return([ret.get(m) for m in members])
                if now_ret is None:
                    continue
                _, gaps = self._ref_history(members, t)
                refs[name] = (now_ret, [float(g) for g in gaps if np.isfinite(g)], tuple(m for m in members if m in ret))
            d = dv.detect(t, r, refs, news=t in with_news)
            if d is not None:
                out.append(d)
        return out

    async def _publish(self, d: dv.Divergence, now: datetime, article: Optional[Dict[str, Any]], ret: Optional[Dict[str, float]] = None) -> bool:
        key = FIRED_KEY.format(day=now.astimezone(_NY).date().isoformat(), ticker=d.ticker)
        raw = getattr(self.redis, "raw", self.redis)
        prev = await raw.hgetall(key)
        count = int(prev.get(b"n", prev.get("n", 0)) or 0) if prev else 0
        last_z = float(prev.get(b"z", prev.get("z", 0)) or 0) if prev else 0.0
        if count >= MAX_PER_DAY or (count and abs(d.z) < abs(last_z) + ELEVATED_STEP):
            return False
        # Counted before it is sent, so a failed send cannot become a stream of retries.
        await raw.hset(key, mapping={"n": count + 1, "z": f"{d.z:.3f}"})
        await raw.expire(key, FIRED_TTL)
        headline_text = str((article or {}).get("headline") or "")[:200]
        state = await self._situation(d, now.astimezone(_NY).date().isoformat())
        peer_rets = [(m, ret[m]) for m in d.members if ret and m in ret]
        brief = intel_brief.build(d, article, state, peer_rets)
        # A rule-based regime label from filings, surprises, insiders and short interest; it annotates the alert and changes no order.
        regime = await regime_annotation.annotate(self.db, d.ticker, datetime.now(timezone.utc), d.z, int((article or {}).get("n") or 0))
        cluster = CorrelationCluster(
            rule_id="DIVERGENCE_NEWS" if d.news else "DIVERGENCE_UNEXPLAINED",
            rule_name="Stock Moving Against Its Peers on News" if d.news else "Stock Moving Against Its Peers, No News",
            alert_tier=AlertTier.ELEVATED if d.tier == "ELEVATED" else AlertTier.ALERT,
            primary_domain="financial",
            confidence_score=float(min(0.99, 0.5 + abs(d.z) / 20.0)),
            summary_headline=d.headline(),
            supporting_headlines=[headline_text] if headline_text else [],
            metrics_summary={"return": round(d.ret, 5), "reference_return": round(d.ref_ret, 5), "gap": round(d.gap, 5),
                             "z": round(d.z, 2), "reference": d.reference, "news_articles": int((article or {}).get("n") or 0),
                             "members": list(d.members[:8]), "situation": state, "brief": brief, "regime": regime,
                             **({"horizon": d.horizon, "breadth": round(d.breadth, 2), "beta": round(d.beta, 2),
                                 "relative_volume": round(d.rvol, 2) if d.rvol else None}
                                if isinstance(d, dv.PeerDivergence) else {})},
            trigger_event_id=str(uuid.uuid4()),
            supporting_event_ids=[],
            primary_entity_id=d.ticker,
            primary_entity_name=d.ticker,
            entity_ids=[d.ticker] + list(d.members[:5]),
            entity_names=[d.ticker] + list(d.members[:5]),
            description=d.description() + "\n\n" + intel_brief.render(brief),
            tags=["divergence", f"ticker:{d.ticker}", f"reference:{d.reference}", "news" if d.news else "no_news"]
                 + ([f"horizon:{d.horizon}"] if isinstance(d, dv.PeerDivergence) else [])
                 + ([f"situation:{state['stage'].lower()}"] if state else [])
                 + ([f"regime:{regime['label'].lower()}"] if regime else []),
        )
        try:
            await EventStore(self.redis, self.db).save_correlation(cluster)
            assert self._producer is not None
            await self._producer.send(Topics.CORRELATIONS, cluster.model_dump(), key=d.ticker)
        except Exception as exc:
            dropped("agents.divergence_watch.publish", f"{type(exc).__name__}: {exc}", logger, detail=d.ticker)
            return False
        return True


__all__ = ["DivergenceWatch", "group_members", "in_scan_window", "previous_close_cutoff"]
