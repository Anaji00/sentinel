"""
services/agents/factor_engine.py

The cross-sectional ranking model (shared.utils.factor_model), run nightly.

Each night after the close: load the daily-bar panel, walk every candidate
model forward out of sample, keep the best one that clears the gate, and rank
today's cross-section with it. The report and the rankings go to Redis for the
predictions page, the ticker inspector and the quant engine's trade plans.

A model that does not clear the gate is still reported -- what it scored and
why it failed -- and its rankings are published marked unvalidated, so nothing
downstream can mistake them for evidence.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import numpy as np

from shared.utils import factor_model as fm
from shared.utils.asset_universe import load_asset_meta
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, factor_scores_key

logger = logging.getLogger("agent.factor_engine")

# Plain momentum is the benchmark every learned model has to beat out of sample.
CANDIDATES = tuple(
    ["lgbm_rank"] * fm.lightgbm_available() + ["gbm", "ridge", "momentum"]
)
LOOKBACK_YEARS = 6
FIRST_RUN_DELAY_SEC = 60     # when no report exists yet; otherwise the nightly slot
RUN_HOUR_UTC = 22            # after the close, after the day's bars land
RESULT_TTL_SEC = 3 * 86400
LOAD_BATCH = 150             # tickers per panel query

# Only names that can ever be ranked: a ticker with fewer bars than the model's
# history requirement would occupy a panel column and never be scored.
_UNIVERSE_SQL = """
    SELECT ticker FROM daily_bars
    WHERE day > $1::date AND close > 0
    GROUP BY ticker HAVING count(*) >= $2
    ORDER BY ticker
"""
_DATES_SQL = "SELECT DISTINCT day FROM daily_bars WHERE day > $1::date ORDER BY day"
_SHORT_INTEREST_SQL = """
    SELECT ticker, settlement_date, days_to_cover FROM short_interest
    WHERE settlement_date > $1::date - 60 AND days_to_cover IS NOT NULL
"""
# Earnings releases: 8-K Item 2.02, stamped at EDGAR acceptance.
_EARNINGS_SQL = """
    SELECT primary_entity_id AS ticker, occurred_at FROM signal_event_archive
    WHERE type = 'filing' AND summary LIKE '%2.02:%' AND occurred_at > $1::date
"""
_NY = ZoneInfo("America/New_York")
_CLOSE = dtime(16, 0)


def base_row(dates: List[date], accepted_at: "datetime | str") -> int:
    """Index of the last session whose 4pm close came before the release was
    accepted; -1 if none. A release at 07:00 is measured from the prior close,
    one at 16:05 from that day's. The DB client returns timestamps as ISO text."""
    if isinstance(accepted_at, str):
        accepted_at = datetime.fromisoformat(accepted_at.replace("Z", "+00:00"))
    if accepted_at.tzinfo is None:
        accepted_at = accepted_at.replace(tzinfo=timezone.utc)
    local = accepted_at.astimezone(_NY)
    cutoff = local.date() if local.time() >= _CLOSE else local.date() - timedelta(days=1)
    return bisect.bisect_right(dates, cutoff) - 1


_PROFILE_SQL = "SELECT ticker, sic FROM company_profile WHERE sic IS NOT NULL"
_PANEL_SQL = """
    SELECT ticker, day, close, high, volume FROM daily_bars
    WHERE day > $1::date AND close > 0 AND ticker = ANY($2::text[])
"""


class FactorEngine:
    def __init__(self, db: Any, redis_client: Any):
        self.db = db
        self.redis = redis_client

    async def run(self) -> None:
        raw = getattr(self.redis, "raw", self.redis)
        if await raw.exists(FACTOR_REPORT_KEY):
            await asyncio.sleep(self._seconds_to_next_run())
        else:
            await asyncio.sleep(FIRST_RUN_DELAY_SEC)
        while True:
            try:
                summary = await self.run_once()
                logger.info("Factor model: %s", summary)
            except Exception as exc:
                logger.error("Factor model run failed: %s", exc, exc_info=True)
            await asyncio.sleep(self._seconds_to_next_run())

    @staticmethod
    def _seconds_to_next_run(now: Optional[datetime] = None) -> float:
        now = now or datetime.now(timezone.utc)
        nxt = now.replace(hour=RUN_HOUR_UTC, minute=0, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        return (nxt - now).total_seconds()

    async def _panel(self) -> Optional[fm.Panel]:
        """The daily-bar panel, filled straight into arrays a batch at a time.

        Reading the whole table as rows held 2.27M asyncpg records and as many
        Python tuples at once, then pickled them to a worker; inside the agents
        container that crossed its memory limit and the kernel killed the
        whole service twenty minutes after every start."""
        since = (datetime.now(timezone.utc) - timedelta(days=365 * LOOKBACK_YEARS)).date()
        tickers: List[str] = [str(r["ticker"]).upper() for r in
                              await self.db.query(_UNIVERSE_SQL, since, fm.MIN_HISTORY) or []]
        dates: List[date] = [r["day"] for r in await self.db.query(_DATES_SQL, since) or []]
        if not tickers or not dates:
            return None
        di = {d: i for i, d in enumerate(dates)}
        ti = {t: i for i, t in enumerate(tickers)}
        shape = (len(dates), len(tickers))
        # float32: ranks need no more, and the panel and its feature matrices
        # over every liquid listed name do not fit the service's budget in float64.
        close = np.full(shape, np.nan, dtype=np.float32)
        high = np.full(shape, np.nan, dtype=np.float32)
        volume = np.full(shape, np.nan, dtype=np.float32)
        for k in range(0, len(tickers), LOAD_BATCH):
            batch = tickers[k:k + LOAD_BATCH]
            for r in await self.db.query(_PANEL_SQL, since, batch) or []:
                i, j = di.get(r["day"]), ti.get(str(r["ticker"]).upper())
                if i is None or j is None:
                    continue
                c = float(r["close"])
                close[i, j] = c
                high[i, j] = float(r["high"] or c)
                volume[i, j] = float(r["volume"] or 0.0)
        si_rows = [(str(r["ticker"]).upper(), r["settlement_date"], float(r["days_to_cover"]))
                   for r in await self.db.query(_SHORT_INTEREST_SQL, since) or []]
        extra = {"days_to_cover": fm.point_in_time(dates, tickers, si_rows)} if si_rows else {}
        panel = fm.Panel(dates, tickers, close, high, volume, extra)
        events = [(str(r["ticker"]).upper(), base_row(dates, r["occurred_at"]))
                  for r in await self.db.query(_EARNINGS_SQL, since) or [] if r["ticker"]]
        if events:
            panel.extra["earnings_reaction"] = fm.announcement_returns(panel, events)
        # Two-digit SIC: about seventy industries, the grain the literature uses.
        sic2 = {str(r["ticker"]).upper(): int(r["sic"]) // 100 for r in await self.db.query(_PROFILE_SQL) or []}
        if sic2:
            panel.extra["industry"] = np.array([sic2.get(t, -1) for t in tickers])
        meta = await load_asset_meta(getattr(self.redis, "raw", self.redis)) if self.redis is not None else {}
        if meta:
            panel.extra["not_a_stock"] = np.array([t in meta and meta[t][0] != "stock" for t in tickers])
        return panel

    async def run_once(self) -> Dict[str, Any]:
        panel = await self._panel()
        if panel is None:
            return {"skipped": "no daily bars yet"}
        # This runs in its own service (services/agents/factor_engine.py main),
        # so a thread is enough: nothing else shares the event loop here.
        result = await asyncio.to_thread(fm.run_panel, panel, CANDIDATES, fm.DEFAULT_HORIZONS)
        del panel
        if result.get("skipped"):
            return result
        scores = result.pop("scores", {}) or {}
        horizon_scores = {h: v.pop("scores", {}) or {} for h, v in (result.get("horizons") or {}).items()}
        for h, sc in horizon_scores.items():
            result["horizons"][h]["scored"] = len(sc)
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            **result,
            "features": fm.FEATURES,
            "gate": {
                "min_ic": fm.GATE_MIN_IC, "min_ic_t": fm.GATE_MIN_IC_T,
                "min_positive_folds": fm.GATE_MIN_POSITIVE_FOLDS, "min_net_sharpe": fm.GATE_MIN_NET_SHARPE,
                "min_deflated_sharpe": fm.GATE_MIN_DSR, "cost_bps_per_side": fm.COST_BPS_PER_SIDE,
                "min_price": fm.MIN_PRICE, "min_dollar_volume": fm.MIN_CONSOLIDATED_DOLLAR_VOLUME,
            },
            "caveats": fm.CAVEATS + ["funds, leveraged products and non-common securities are not ranked"],
            "scored": len(scores),
        }
        raw = getattr(self.redis, "raw", self.redis)
        await raw.set(FACTOR_REPORT_KEY, json.dumps(report, default=str), ex=RESULT_TTL_SEC)
        await raw.set(FACTOR_SCORES_KEY, json.dumps(scores), ex=RESULT_TTL_SEC)
        for h, sc in horizon_scores.items():
            await raw.set(factor_scores_key(h), json.dumps(sc), ex=RESULT_TTL_SEC)
        return {
            "universe": report["universe"], "tradable": report.get("tradable"), "days": report["trading_days"],
            "primary": report.get("primary_horizon"), "trials": report.get("trials"),
            "horizons": {
                h: {m["model"]: (m["mean_ic"], m["ic_t"], m["net_sharpe"], m["deflated_sharpe"], m["validated"])
                    for m in v["models"]}
                for h, v in (report.get("horizons") or {}).items()
            },
        }


async def main() -> None:
    """Its own service, not a task in the agents container: the walk-forward
    holds a panel and seventeen feature matrices over ~1,900 names and five
    years, which does not fit beside the agents in their memory budget."""
    from shared.db import get_redis, get_timescale
    from shared.utils.logging import setup_sentinel_logging

    from services.agents.recommendation_engine import RecommendationEngine

    setup_sentinel_logging("factor_engine")
    db, redis = await get_timescale(), await get_redis()
    # Trade ideas from the model's ranks, refreshed through the session. Here
    # because they read nothing but the model's output, and this service is
    # idle between nightly runs.
    await asyncio.gather(FactorEngine(db, redis).run(), RecommendationEngine(db, redis).run())


__all__ = ["FactorEngine"]


if __name__ == "__main__":
    asyncio.run(main())
