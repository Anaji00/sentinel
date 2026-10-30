"""
services/agents/analyst_agent.py

An analyst for the day's strongest stock alerts (shared/utils/analyst.py holds
the arithmetic). For a correlation that names a stock the model ranks, it
gathers numbered facts -- the model's rank, the stock's recent moves, its
recent headlines with their tone, and the most similar past events from the
event memory (Qdrant, by the alert's own trigger event) -- asks the judgment
model several times, keeps only answers that cite facts they were given,
averages them with the model's rank as a prior, and records the result as a
forecast graded on prices like every other agent's.

Budgeted: at most ANALYST_DAILY_CAP alerts a day, each stock once a day, and
every model call goes through the agent's inference budget.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from services.agents.base import InferenceShed, SentinelAgent
from shared.kafka import Topics
from shared.utils import analyst as an
from shared.utils import case_file as cf
from shared.utils import gpr
from shared.utils.kalshi_consensus import KALSHI_CONSENSUS_KEY
from shared.utils import producer_grades as pg
from shared.utils.signal_store import PRODUCER_GRADES_KEY
from shared.utils.quiet_failures import dropped, swallowed

_TICKER = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")
DAILY_CAP = int(os.getenv("ANALYST_DAILY_CAP", "24"))
SAMPLES = int(os.getenv("ANALYST_SAMPLES", "3"))
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
QDRANT_COLLECTION = "sentinel_events_v2"
VIEWS_KEY = "sentinel:analyst:views"           # the latest views, newest first, for the dashboard
DAY_KEY = "sentinel:analyst:day:{day}"         # a set of tickers analysed today
CASE_FACTS_MAX = 10                            # case-file facts added to the prompt

_CLOSES_SQL = "SELECT close FROM daily_bars WHERE ticker = $1 ORDER BY day DESC LIMIT 22"
_STORED_RANK_SQL = """
    SELECT percentile FROM factor_scores_daily WHERE horizon = 'monthly' AND ticker = $1
      AND as_of > CURRENT_DATE - 14 ORDER BY as_of DESC LIMIT 1
"""
_NEWS_SQL = """
    SELECT created_at, headline, positive, negative FROM stock_news
    WHERE $1 = ANY(symbols) AND created_at > NOW() - INTERVAL '3 days' ORDER BY created_at DESC LIMIT 5
"""


class AnalystAgent(SentinelAgent):
    def __init__(self, agent_name: str = "analyst_agent", input_topics: Optional[List[str]] = None, **kwargs: Any):
        super().__init__(agent_name=agent_name, input_topics=input_topics or [Topics.CORRELATIONS], **kwargs)

    @property
    def output_topic(self) -> str:
        return Topics.FINANCIAL_ADVICE

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Nothing is published: the advice topic's readers expect a quant
        # engine's play, not this. The view lives in the predictions table
        # (graded) and in VIEWS_KEY (shown).
        if await self.analyse(message) is None:
            dropped("agents.analyst_agent.handle", "not analysed: no ranked stock, seen today, over the daily cap, "
                    "or no grounded answer", self.logger)

    async def analyse(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ticker = str(message.get("primary_entity_id") or "").upper()
        if not _TICKER.match(ticker):
            return None
        raw = self.redis.raw
        # The daily cap went to the first alerts to arrive, whatever their
        # family; most families are graded uninformative against prices, so
        # those are skipped and the budget goes to families that carry signal.
        if pg.is_uninformative(str(message.get("rule_name") or ""), await self._retired(raw)):
            return None
        rank = await self._rank(raw, ticker)
        if rank is None:
            return None                       # not a stock the model ranks: nothing to anchor on
        day = datetime.now(timezone.utc).date().isoformat()
        # The hosted shadow keeps its own day set, or it would find every ticker
        # already taken by the analyst it is being compared with.
        key = DAY_KEY.format(day=day) + ("" if self.name == "analyst_agent" else f":{self.name}")
        if await raw.sismember(key, ticker) or (await raw.scard(key) or 0) >= DAILY_CAP:
            return None
        await raw.sadd(key, ticker)
        await raw.expire(key, 3 * 86400)

        closes = [float(r["close"]) for r in await self.db.query(_CLOSES_SQL, ticker) or []]
        if len(closes) < 6:
            return None
        moves = {"last session": closes[0] / closes[1] - 1, "last 5 sessions": closes[0] / closes[5] - 1,
                 "last 21 sessions": closes[0] / closes[21] - 1 if len(closes) > 21 else None}
        news = [(str(r["created_at"])[:16], str(r["headline"])[:200],
                 (float(r["positive"]) - float(r["negative"])) if r["positive"] is not None else None)
                for r in await self.db.query(_NEWS_SQL, ticker) or []]
        similar = await self._similar(str(message.get("trigger_event_id") or ""))
        alert = str(message.get("summary_headline") or message.get("rule_name") or "an alert")[:200]
        fact_lines = an.facts(ticker, rank, "monthly", moves, news, similar, alert,
                              await self._context(raw) + await self._case_facts(raw, ticker))
        system, user = an.prompt(ticker, fact_lines)

        samples: List[Dict[str, Any]] = []
        for _ in range(SAMPLES):
            try:
                view = await self._execute_with_telemetry(message, system, user, schema=an.AnalystView,
                                                          temperature=0.7, model=self.model, num_predict=300)
            except InferenceShed:
                break
            except Exception as e:
                swallowed("agents.analyst.sample", e, self.logger)
                continue
            v = view.model_dump() if hasattr(view, "model_dump") else dict(view or {})
            if an.verified(v, len(fact_lines)):
                samples.append(v)
        got = an.combine(samples, an.prior_from_percentile(rank))
        if got is None:
            return None
        direction, conviction, p = got
        await self.record_prediction(ticker=ticker, direction=direction, conviction=conviction,
                                     entry_price=closes[0], time_horizon_hours=an.HORIZON_HOURS,
                                     strategy="analyst")
        out = {"agent": self.name, "ticker": ticker, "direction": direction, "conviction": conviction, "p_beat_market": p,
               "samples": len(samples), "prior": round(an.prior_from_percentile(rank), 4), "rank": rank,
               "rationale": samples[0].get("rationale", "")[:400] if samples else "",
               "alert": alert, "at": datetime.now(timezone.utc).isoformat()}
        try:
            await raw.lpush(VIEWS_KEY, json.dumps(out))
            await raw.ltrim(VIEWS_KEY, 0, 199)
        except Exception as e:
            swallowed("agents.analyst.views", e, self.logger)
        return out

    async def _case_facts(self, raw: Any, ticker: str) -> List[str]:
        """The stock's case file (shared/utils/case_file.py), the kinds this
        agent does not already gather: alerts with their family's grade,
        features, customers and the hosted headline read. Absent when no
        file has been built for the stock."""
        try:
            case = json.loads(await raw.get(cf.CASE_KEY.format(ticker=ticker)) or "null")
        except (TypeError, ValueError):
            return []
        if not isinstance(case, dict):
            return []
        keep = [f for f in case.get("facts") or [] if f.get("kind") in ("alert", "feature", "customer")
                or (f.get("kind") == "news" and str(f.get("text", "")).startswith("Hosted model read"))]
        return [f"{f['text']} (as of {f['as_of']})" for f in keep][:CASE_FACTS_MAX]

    async def _context(self, raw: Any) -> List[str]:
        """Market-wide facts worth the tokens: geopolitical risk, when elevated."""
        try:
            s = json.loads(await raw.get(gpr.LATEST_KEY) or "null")
        except (TypeError, ValueError):
            return []
        line = gpr.fact(s) if isinstance(s, dict) and s.get("elevated") else None
        return ([line] if line else []) + await self._market_consensus(raw)

    async def _retired(self, raw: Any) -> set:
        """Rule families graded uninformative (producer_grades), cached 10 minutes."""
        now = datetime.now(timezone.utc).timestamp()
        cached = getattr(self, "_retired_cache", None)
        if cached and now - cached[0] < 600:
            return cached[1]
        try:
            blob = await raw.get(PRODUCER_GRADES_KEY)
            retired = pg.uninformative_rules(json.loads(blob)) if blob else set()
        except Exception as e:
            swallowed("agents.analyst.retired", e, self.logger)
            retired = set()
        self._retired_cache = (now, retired)
        return retired

    async def _market_consensus(self, raw: Any) -> List[str]:
        """Prediction-market medians for macro releases due within the analyst's
        horizon. The markets collected are macro (Fed, CPI, payrolls, claims,
        oil), not per-stock, so they are context, not a prior for the stock."""
        try:
            snaps = await raw.hgetall(KALSHI_CONSENSUS_KEY) or {}
        except Exception as e:
            swallowed("agents.analyst.consensus", e, self.logger)
            return []
        now = datetime.now(timezone.utc)
        out: List[str] = []
        for v in snaps.values():
            try:
                s = json.loads(v.decode() if isinstance(v, bytes) else v)
                close = datetime.fromisoformat(str(s["close_time"]).replace("Z", "+00:00"))
            except (ValueError, KeyError, TypeError):
                continue
            if now <= close <= now + timedelta(hours=an.HORIZON_HOURS):
                out.append(f"Prediction markets price the {s['indicator'].replace('_', ' ').lower()} release due "
                           f"{close:%d %b} at a median of {s['median']}.")
        return sorted(out)[:3]

    async def _rank(self, raw: Any, ticker: str) -> Optional[float]:
        from shared.utils.signal_store import factor_scores_key

        try:
            scores = json.loads(await raw.get(factor_scores_key("monthly")) or "{}")
        except (TypeError, ValueError):
            return None
        pct = (scores.get(ticker) or {}).get("percentile")
        if pct is None and not scores:
            # The live ranks lapse when a model run is late; the last stored ones stand in.
            rows = await self.db.query(_STORED_RANK_SQL, ticker) or []
            pct = rows[0]["percentile"] if rows else None
        return float(pct) if pct is not None else None

    async def _similar(self, event_id: str, k: int = 4) -> List[Tuple[str, str, str]]:
        """The past events nearest the alert's trigger in the event memory."""
        if not event_id:
            return []
        body = {"positive": [event_id], "limit": k + 4, "with_payload": True}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{QDRANT_URL}/collections/{QDRANT_COLLECTION}/points/recommend", json=body,
                                  timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return []
                    pts = (await resp.json()).get("result") or []
        except Exception as e:
            swallowed("agents.analyst.similar", e, self.logger)
            return []
        out = []
        for p in pts:
            pl = p.get("payload") or {}
            if pl.get("headline"):
                out.append((str(pl.get("occurred_at") or "")[:10], str(pl.get("entity_name") or pl.get("entity_id") or ""),
                            str(pl["headline"])[:160]))
        return out[:k]


__all__ = ["AnalystAgent", "DAILY_CAP", "SAMPLES"]
