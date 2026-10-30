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
import math
import os
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from shared.utils import factor_model as fm
from shared.utils import fundamentals_pit as fpit
from shared.utils.asset_universe import load_asset_meta
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, factor_scores_key

logger = logging.getLogger("agent.factor_engine")

# Plain momentum is the benchmark every learned model has to beat out of sample.
CANDIDATES = tuple(
    ["lgbm_rank"] * fm.lightgbm_available() + ["gbm", "ridge", "momentum"]
)
# Years of daily bars the panel reads. Bars reach back to January 2015 since
# 22 Oct 2026. Eleven adopted 23 Oct 2026 (trial O1, MODEL_TRIALS.md): the
# monthly ranker's walk-forward went from 44 to 101 months, IC t 2.00 -> 3.12,
# net Sharpe 0.79 -> 1.01, through the 2018, 2020 and 2022 drawdowns.
LOOKBACK_YEARS = int(os.getenv("FACTOR_LOOKBACK_YEARS", "11"))
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
_FUNDAMENTALS_SQL = """
    SELECT ticker, concept, period_end, value FROM fundamentals
    WHERE ticker = ANY($1::text[]) AND period_end > $2::date
"""
FUNDAMENTAL_LAG_DAYS = 90       # a period counts once 10-Q and 10-K deadlines have passed
# Finnhub share counts for stocks the SEC frames leave out (multi-class issuers,
# foreign filers; services/collector-filings/vendor_shares.py).
_VENDOR_SHARES_SQL = "SELECT ticker, as_of, shares FROM vendor_shares WHERE ticker = ANY($1::text[])"


def vendor_share_rows(sec_rows, vendor_rows) -> List[Dict[str, Any]]:
    """Vendor share counts as fundamentals rows, for tickers with no SEC share
    count only. A snapshot is known the day it is taken, so it is dated one lag
    earlier: the point-in-time reader then admits it from that day, never before."""
    have = {str(r["ticker"]).upper() for r in sec_rows if r.get("concept") == "EntityCommonStockSharesOutstanding"}
    return [{"ticker": str(r["ticker"]).upper(), "concept": "EntityCommonStockSharesOutstanding",
             "period_end": r["as_of"] - timedelta(days=FUNDAMENTAL_LAG_DAYS), "value": float(r["shares"])}
            for r in vendor_rows if str(r["ticker"]).upper() not in have and r.get("shares")]


_CONCEPT_KEY = {
    "Assets": "assets", "StockholdersEquity": "equity", "NetIncomeLoss": "net_income",
    "NetCashProvidedByUsedInOperatingActivities": "cfo",
}
_MAX_AGE = {"assets": 200, "equity": 200, "net_income": 460, "cfo": 460, "shares_adj": 200, "asset_growth": 200}
_SPLIT_RATIOS = (2, 3, 4, 5, 10, 20, 1 / 2, 1 / 3, 1 / 4, 1 / 5, 1 / 10, 1 / 20)


def split_adjusted_shares(obs: List[tuple]) -> List[tuple]:
    """[(period_end, shares)] sorted -> same, scaled by every later split.

    A jump in reported shares close to a split ratio between consecutive
    reports is read as a split; the earlier counts are multiplied by it, so
    they match prices that are adjusted for it."""
    obs = sorted(obs)
    factors = [1.0] * len(obs)
    for i in range(len(obs) - 1, 0, -1):
        prev, cur = obs[i - 1][1], obs[i][1]
        k = 1.0
        if prev > 0:
            r = cur / prev
            near = min(_SPLIT_RATIOS, key=lambda x: abs(r / x - 1))
            if abs(r / near - 1) < 0.08 and (r >= 1.5 or r <= 0.67):
                k = near
        factors[i - 1] = factors[i] * k
    return [(d, v * f) for (d, v), f in zip(obs, factors)]


# Filing-dated facts (services/collector-filings/company_facts.py): each value
# counts from the session after its filing, restatements from theirs.
_FACTS_SQL = """
    SELECT i.ticker, f.concept, f.period_start, f.period_end, f.value, f.filed, f.accn
    FROM fundamental_facts f JOIN issuer_ticker i ON i.cik = f.cik
    WHERE i.ticker = ANY($1::text[]) AND f.filed > $2::date
"""
_FACTS_COVERAGE_SQL = """
    SELECT count(*) FILTER (WHERE fetched IS NOT NULL) AS done, count(*) AS total
    FROM (SELECT cik, max(fetched_at) AS fetched FROM issuer_ticker GROUP BY cik) x
"""
FACTS_LAG_DAYS = 1              # many filings land after the close
FACTS_MAX_AGE_DAYS = 200        # a quarter's filing, plus a missed one
FACTS_MIN_COVERAGE = 0.9        # until the first pass is this far through, frames are read
FACTS_TICKER_CHUNK = 250        # bounded memory: facts are turned into series chunk by chunk


def facts_points(rows: List[Dict[str, Any]]) -> Dict[str, List[Tuple[str, date, float]]]:
    """Fact rows -> {item: [(ticker, filed, value)]}; shares split-adjusted to match prices.

    Shares are the cover-page count summed over classes, or weighted diluted
    shares where a company reports no cover count."""
    by: Dict[str, List[fpit.Fact]] = defaultdict(list)
    for r in rows:
        t = str(r["ticker"] or "").upper()
        if t:
            by[t].append(fpit.Fact(str(r["concept"]), r["period_start"], r["period_end"], float(r["value"]),
                                   r["filed"], str(r["accn"])))
    out: Dict[str, List[Tuple[str, date, float]]] = defaultdict(list)
    for t, facts in by.items():
        s = fpit.series(facts)
        shares = s.pop("shares_out", None)
        diluted = s.pop("shares_dil", None)
        shares = shares or diluted
        if shares:
            out["shares_adj"].extend((t, d, v) for d, v in split_adjusted_shares(shares))
        for key, pts in s.items():
            out[key].extend((t, d, v) for d, v in pts)
    return out


def facts_arrays(dates: List[date], tickers: List[str], points: Dict[str, List[Tuple[str, date, float]]]) -> Dict[str, Any]:
    return {key: fm.point_in_time(dates, tickers, pts, lag_days=FACTS_LAG_DAYS,
                                  max_age_days=FACTS_MAX_AGE_DAYS).astype(np.float32)
            for key, pts in points.items() if pts}


def fundamental_arrays(dates: List[date], tickers: List[str], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Point-in-time [T, N] arrays for the model's fundamental features."""
    by: Dict[str, Dict[str, List[tuple]]] = {}
    for r in rows:
        t = str(r["ticker"] or "").upper()
        if not t:
            continue
        key = "shares" if r["concept"] == "EntityCommonStockSharesOutstanding" else _CONCEPT_KEY.get(r["concept"])
        if key:
            by.setdefault(key, {}).setdefault(t, []).append((r["period_end"], float(r["value"])))
    out: Dict[str, Any] = {}

    def pit(key: str, per_ticker: Dict[str, List[tuple]]) -> None:
        flat = [(t, d, v) for t, obs in per_ticker.items() for d, v in obs]
        if flat:
            out[key] = fm.point_in_time(dates, tickers, flat, lag_days=FUNDAMENTAL_LAG_DAYS,
                                        max_age_days=_MAX_AGE[key]).astype(np.float32)

    for key in ("assets", "equity", "net_income", "cfo"):
        pit(key, by.get(key, {}))
    pit("shares_adj", {t: split_adjusted_shares(obs) for t, obs in by.get("shares", {}).items()})
    growth: Dict[str, List[tuple]] = {}
    for t, obs in by.get("assets", {}).items():
        obs = sorted(obs)
        for i, (d, v) in enumerate(obs):
            prior = [pv for pd, pv in obs[:i] if 330 <= (d - pd).days <= 400]
            if prior and prior[-1] > 0:
                growth.setdefault(t, []).append((d, v / prior[-1] - 1.0))
    pit("asset_growth", growth)
    return out


_PANEL_SQL = """
    SELECT ticker, day, open, close, high, volume FROM daily_bars
    WHERE day > $1::date AND close > 0 AND ticker = ANY($2::text[])
"""
# FINRA's daily off-exchange short volume (shared/utils/short_data.py).
# Option-implied measures rebuilt from daily option bars (option_iv_history),
# each the close of its own session, like the stock's close in the same row.
_OPTION_IV_SQL = """
    SELECT ticker, day, atm_iv, iv_skew, cp_iv_spread, put_call_ratio FROM option_iv_history
    WHERE day > $1::date AND ticker = ANY($2::text[])
"""
_NEWS_TONE_SQL = """
    SELECT (created_at AT TIME ZONE 'America/New_York')::date AS day, avg(positive - negative) AS tone
    FROM stock_news WHERE created_at > $1::date AND scored_at IS NOT NULL GROUP BY 1
"""
_FINRA_SQL = """
    SELECT ticker, day, short_volume, total_volume FROM finra_short_volume
    WHERE day > $1::date AND ticker = ANY($2::text[])
"""


class FactorEngine:
    def __init__(self, db: Any, redis_client: Any):
        self.db = db
        self.redis = redis_client

    async def _fundamentals(self, dates: List[date], tickers: List[str], since: date) -> Dict[str, Any]:
        """Filing-dated facts once the first pass has covered the universe, frames until then."""
        cov = (await self.db.query(_FACTS_COVERAGE_SQL) or [{}])[0]
        done, total = int(cov.get("done") or 0), int(cov.get("total") or 0)
        if total and done / total >= FACTS_MIN_COVERAGE:
            points: Dict[str, List[Tuple[str, date, float]]] = defaultdict(list)
            for k in range(0, len(tickers), FACTS_TICKER_CHUNK):
                rows = await self.db.query(_FACTS_SQL, tickers[k:k + FACTS_TICKER_CHUNK],
                                           since - timedelta(days=800)) or []
                for key, pts in facts_points(rows).items():
                    points[key].extend(pts)
                del rows
            logger.info("Fundamentals from filing-dated facts (%s of %s issuers fetched).", done, total)
            return facts_arrays(dates, tickers, points)
        logger.info("Fundamentals from XBRL frames: filing-dated facts at %s of %s issuers.", done, total)
        rows = await self.db.query(_FUNDAMENTALS_SQL, tickers, since - timedelta(days=500)) or []
        rows = list(rows) + vendor_share_rows(rows, await self.db.query(_VENDOR_SHARES_SQL, tickers) or [])
        return fundamental_arrays(dates, tickers, rows)

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
        # Funds and non-common securities are never ranked (the not_a_stock mask
        # below). Consolidated history (8 Oct 2026) filled their missing days,
        # doubled the panel to ~9,900 columns and the container ran out of
        # memory; they are left out at load. Delisted names are absent from the
        # active asset list and stay, and so does the market itself.
        listing = await load_asset_meta(getattr(self.redis, "raw", self.redis)) if self.redis is not None else {}
        if listing:
            tickers = [t for t in tickers if t == "SPY" or listing.get(t, ["stock"])[0] == "stock"]
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
        opens = np.full(shape, np.nan, dtype=np.float32)
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
                if r.get("open"):
                    opens[i, j] = float(r["open"])
        # Off-exchange short and total volume, each in the row after its day:
        # the file posts by 6pm ET, after the close the row stands for.
        short_v = np.full(shape, np.nan, dtype=np.float32)
        offex_v = np.full(shape, np.nan, dtype=np.float32)
        for k in range(0, len(tickers), LOAD_BATCH):
            for r in await self.db.query(_FINRA_SQL, since, tickers[k:k + LOAD_BATCH]) or []:
                i, j = di.get(r["day"]), ti.get(str(r["ticker"]).upper())
                if i is None or j is None or i + 1 >= len(dates):
                    continue
                short_v[i + 1, j] = float(r["short_volume"])
                offex_v[i + 1, j] = float(r["total_volume"])
        opt = {k: np.full(shape, np.nan, dtype=np.float32)
               for k in ("opt_atm_iv", "opt_iv_skew", "opt_cp_iv_spread", "opt_log_pcr")}
        for k in range(0, len(tickers), LOAD_BATCH):
            for r in await self.db.query(_OPTION_IV_SQL, since, tickers[k:k + LOAD_BATCH]) or []:
                i, j = di.get(r["day"]), ti.get(str(r["ticker"]).upper())
                if i is None or j is None:
                    continue
                for key, col in (("opt_atm_iv", "atm_iv"), ("opt_iv_skew", "iv_skew"),
                                 ("opt_cp_iv_spread", "cp_iv_spread")):
                    if r[col] is not None:
                        opt[key][i, j] = float(r[col])
                if r["put_call_ratio"] is not None and r["put_call_ratio"] > 0:
                    opt["opt_log_pcr"][i, j] = math.log(float(r["put_call_ratio"]))
        si_rows = [(str(r["ticker"]).upper(), r["settlement_date"], float(r["days_to_cover"]))
                   for r in await self.db.query(_SHORT_INTEREST_SQL, since) or []]
        extra = {"days_to_cover": fm.point_in_time(dates, tickers, si_rows)} if si_rows else {}
        extra["open"] = opens
        if np.isfinite(opt["opt_atm_iv"]).any():
            extra.update(opt)
        del opt
        if np.isfinite(offex_v).any():
            extra["short_volume"], extra["offex_volume"] = short_v, offex_v
        # Market news tone per session (stock_news, FinBERT positive minus
        # negative, averaged): the sentiment the lab's short-leg tilt reads.
        tone = np.full(len(dates), np.nan, dtype=np.float32)
        for r in await self.db.query(_NEWS_TONE_SQL, since) or []:
            i = di.get(r["day"])
            if i is not None and r["tone"] is not None:
                tone[i] = float(r["tone"])
        extra["ctx_news_tone"] = tone
        # Rates and credit as context series (funds are not panel columns).
        for sym in ("TLT", "HYG"):
            series = np.full(len(dates), np.nan, dtype=np.float32)
            for r in await self.db.query(_PANEL_SQL, since, [sym]) or []:
                i = di.get(r["day"])
                if i is not None:
                    series[i] = float(r["close"])
            extra[f"ctx_{sym}"] = series
        panel = fm.Panel(dates, tickers, close, high, volume, extra)
        events = [(str(r["ticker"]).upper(), base_row(dates, r["occurred_at"]))
                  for r in await self.db.query(_EARNINGS_SQL, since) or [] if r["ticker"]]
        if events:
            panel.extra["earnings_reaction"] = fm.announcement_returns(panel, events)
        # Two-digit SIC: about seventy industries, the grain the literature uses.
        sic2 = {str(r["ticker"]).upper(): int(r["sic"]) // 100 for r in await self.db.query(_PROFILE_SQL) or []}
        if sic2:
            panel.extra["industry"] = np.array([sic2.get(t, -1) for t in tickers])
        panel.extra.update(await self._fundamentals(dates, tickers, since))
        meta = await load_asset_meta(getattr(self.redis, "raw", self.redis)) if self.redis is not None else {}
        if meta:
            panel.extra["not_a_stock"] = np.array([t in meta and meta[t][0] != "stock" for t in tickers])
            # Shortable and easy to borrow today; names gone from the list
            # (delisted) are not assumed lendable.
            panel.extra["borrowable"] = np.array([bool(t in meta and meta[t][1] and meta[t][2]) for t in tickers])
        return panel

    async def run_once(self) -> Dict[str, Any]:
        panel = await self._panel()
        if panel is None:
            return {"skipped": "no daily bars yet"}
        # This runs in its own service (services/agents/factor_engine.py main),
        # so a thread is enough: nothing else shares the event loop here.
        # The walk-forward runs on the names ever tradable, the only ones any
        # model ranks. On all 5,613 columns the feature build alone peaked at
        # 2,344 of this service's 2,432 MB (18 Oct 2026) and could not take the
        # option features; the subset reproduces the full panel's results
        # (monthly ridge IC 0.0675, t 2.34, on both).
        result = await asyncio.to_thread(fm.run_panel, fm.tradable_subset(panel), CANDIDATES, fm.DEFAULT_HORIZONS)
        if not result.get("skipped"):
            # Before the report is published, so the portfolio's next decision
            # is sized by tonight's gauge.
            try:
                await self._equity_regime(panel)
            except Exception as e:
                logger.warning("Equity regime gauge not computed: %s", e)
            try:
                await self._return_clusters(panel)
            except Exception as e:
                logger.warning("Return themes not computed: %s", e)
            try:
                hz = result.get("horizons") or {}
                await self._portfolio_lab(panel, (hz.get("monthly") or {}).get("chosen"),
                                          {h: (hz.get(h) or {}).get("chosen") for h in ("weekly", "daily")
                                           if (hz.get(h) or {}).get("chosen")})
            except Exception as e:
                logger.warning("Portfolio lab not run: %s", e)
        del panel
        if result.get("skipped"):
            return result
        scores = result.pop("scores", {}) or {}
        horizon_scores = {h: v.pop("scores", {}) or {} for h, v in (result.get("horizons") or {}).items()}
        # Shadow scores, kept and graded but never published as a horizon.
        shadow_scores = {"weekly_trial_f": ((result.get("horizons") or {}).get("weekly") or {}).pop("trial_f_scores", {}) or {}}
        for h, sc in horizon_scores.items():
            result["horizons"][h]["scored"] = len(sc)
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            **result,
            "features": fm.FEATURES,
            "context_features": fm.CONTEXT_FEATURES,
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
        # Every run kept, so drift in what the model finds can be seen.
        try:
            await self.db.execute(
                "INSERT INTO factor_model_runs (run_at, as_of, primary_horizon, universe, tradable, report) "
                "VALUES ($1, $2::date, $3, $4, $5, $6::jsonb) ON CONFLICT DO NOTHING",
                datetime.fromisoformat(report["generated_at"]),
                date.fromisoformat(str(report["as_of"])) if report.get("as_of") else None,
                report.get("primary_horizon"), report.get("universe"), report.get("tradable"),
                # Plain JSON values (dates as text) for the pool's jsonb codec to
                # encode once; json.dumps here stored a JSON string (0052).
                json.loads(json.dumps(report, default=str)),
            )
        except Exception as e:
            logger.warning("Model run not archived: %s", e)
        await raw.set(FACTOR_SCORES_KEY, json.dumps(scores), ex=RESULT_TTL_SEC)
        for h, sc in horizon_scores.items():
            await raw.set(factor_scores_key(h), json.dumps(sc), ex=RESULT_TTL_SEC)
        try:
            await self._keep_and_grade(report.get("as_of"), {**horizon_scores, **shadow_scores})
        except Exception as e:
            logger.warning("Scores not kept or graded: %s", e)
        return {
            "universe": report["universe"], "tradable": report.get("tradable"), "days": report["trading_days"],
            "primary": report.get("primary_horizon"), "trials": report.get("trials"),
            "horizons": {
                h: {m["model"]: (m["mean_ic"], m["ic_t"], m["net_sharpe"], m["deflated_sharpe"], m["validated"])
                    for m in v["models"]}
                for h, v in (report.get("horizons") or {}).items()
            },
        }

    async def _equity_regime(self, panel: Any) -> None:
        """Nightly: the stock market's own regime (shared/utils/equity_regime.py)."""
        from shared.utils import equity_regime as er

        if self.redis is None:
            return
        gauge = await asyncio.to_thread(er.gauge, panel, fm.tradable(panel))
        gauge["generated_at"] = datetime.now(timezone.utc).isoformat()
        await getattr(self.redis, "raw", self.redis).set(er.EQUITY_REGIME_KEY, json.dumps(gauge, default=str))
        logger.info("Equity regime: %s (momentum drawdown %s, vol ratio %s)", gauge.get("level"),
                    gauge.get("momentum_drawdown"), gauge.get("momentum_vol_ratio"))

    async def _return_clusters(self, panel: Any) -> int:
        """Nightly: every ranked name's statistical return theme
        (portfolio_lab.return_clusters on the last session), for the book and
        the trade ideas to rank within when the lab adopts it."""
        from shared.utils import portfolio_lab as pl

        if self.redis is None:
            return 0
        sub = fm.tradable_subset(panel)
        last = len(sub.dates) - 1
        ok = fm.tradable(sub)[last]
        cols = np.flatnonzero(ok)
        labels = await asyncio.to_thread(pl.return_clusters, sub.close, last, cols)
        themes = {sub.tickers[c]: int(lab) for c, lab in zip(cols, labels) if lab >= 0}
        await getattr(self.redis, "raw", self.redis).set(pl.RETURN_CLUSTERS_KEY, json.dumps(themes))
        return len(themes)

    async def _portfolio_lab(self, panel: Any, model_name: Optional[str],
                             horizon_models: Optional[Dict[str, str]] = None) -> None:
        """Weekly: the constructions compared on history (shared/utils/portfolio_lab.py)."""
        from shared.utils import portfolio_lab as pl

        if not model_name or self.redis is None:
            return
        raw = getattr(self.redis, "raw", self.redis)
        prior = await raw.get(pl.PORTFOLIO_LAB_KEY)
        if prior:
            try:
                ran = json.loads(prior).get("generated_at", "")
                if ran and datetime.now(timezone.utc) - datetime.fromisoformat(ran) < timedelta(days=pl.LAB_EVERY_DAYS):
                    return
            except (ValueError, TypeError) as e:
                from shared.utils.quiet_failures import swallowed
                swallowed("agents.factor_engine.portfolio_lab_age", e, logger)   # unreadable stamp: run it again
        res = await asyncio.to_thread(pl.run, panel, model_name, horizon_models)
        res["generated_at"] = datetime.now(timezone.utc).isoformat()
        # Each run's verdict on meta-sizing, kept so it goes live only when runs
        # agree (meta_label.confirmed).
        try:
            before = json.loads(prior) if prior else {}
        except (ValueError, TypeError):
            before = {}
        hist = list(before.get("meta_history") or [])
        if not hist and before.get("meta_sized"):
            # A run from before the history was kept still counts as one.
            pm = before["meta_sized"]
            hist.append({"at": before.get("generated_at"), "base": (pm.get("base") or {}).get("sharpe"),
                         "meta": (pm.get("meta") or {}).get("sharpe")})
        ms = res.get("meta_sized") or {}
        entry = {"at": res["generated_at"], "base": (ms.get("base") or {}).get("sharpe"),
                 "meta": (ms.get("meta") or {}).get("sharpe")}
        # One verdict a day: a second run that day replaces the first, so the
        # same history can never count as two confirmations.
        if hist and str(hist[-1].get("at") or "")[:10] == entry["at"][:10]:
            hist[-1] = entry
        else:
            hist.append(entry)
        res["meta_history"] = hist[-6:]
        await raw.set(pl.PORTFOLIO_LAB_KEY, json.dumps(res, default=str))
        logger.info("Portfolio lab: %s", res.get("verdict"))

    async def _keep_and_grade(self, as_of: Any, horizon_scores: Dict[str, Dict[str, Any]]) -> None:
        """Keep tonight's scores; grade every kept night whose horizon has passed
        (shared/utils/ic_monitor.py)."""
        from shared.utils import ic_monitor as icm

        day = date.fromisoformat(str(as_of)[:10]) if as_of else None
        if day is not None:
            rows = [(day, h, str(t).upper(), float(v["percentile"]))
                    for h, sc in horizon_scores.items() for t, v in sc.items()
                    if isinstance(v, dict) and v.get("percentile") is not None]
            if rows:
                await self.db.execute_many(_KEEP_SCORES_SQL, rows)
        sessions = [r["day"] for r in await self.db.query(_SPY_SESSIONS_SQL) or []]
        index = {d: i for i, d in enumerate(sessions)}
        for r in await self.db.query(_UNGRADED_SQL) or []:
            start_day, h = r["as_of"], r["horizon"]
            steps = icm.HORIZON_DAYS.get(h)
            starts = [d for d in sessions if d <= start_day]
            if not steps or not starts:
                continue
            i = index[starts[-1]]
            if i + steps >= len(sessions):
                continue                                  # not matured yet
            end_day = sessions[i + steps]
            pct = {str(x["ticker"]): float(x["percentile"])
                   for x in await self.db.query(_KEPT_SCORES_SQL, start_day, h) or []}
            closes: Dict[Any, Dict[str, float]] = {starts[-1]: {}, end_day: {}}
            for x in await self.db.query(_CLOSES_SQL, sorted(pct) + ["SPY"], [starts[-1], end_day]) or []:
                closes[x["day"]][str(x["ticker"]).upper()] = float(x["close"])
            g = icm.grade(pct, closes[starts[-1]], closes[end_day],
                          closes[starts[-1]].get("SPY", 0.0), closes[end_day].get("SPY", 0.0))
            await self.db.execute(_SAVE_IC_SQL, start_day, h, g["ic"], g["n"], end_day)


_KEEP_SCORES_SQL = """
    INSERT INTO factor_scores_daily (as_of, horizon, ticker, percentile) VALUES ($1, $2, $3, $4)
    ON CONFLICT (as_of, horizon, ticker) DO UPDATE SET percentile = EXCLUDED.percentile
"""
_SPY_SESSIONS_SQL = "SELECT day FROM daily_bars WHERE ticker = 'SPY' AND day > CURRENT_DATE - 120 ORDER BY day"
_UNGRADED_SQL = """
    SELECT DISTINCT s.as_of, s.horizon FROM factor_scores_daily s
    LEFT JOIN factor_ic i ON i.as_of = s.as_of AND i.horizon = s.horizon
    WHERE i.as_of IS NULL ORDER BY s.as_of
"""
_KEPT_SCORES_SQL = "SELECT ticker, percentile FROM factor_scores_daily WHERE as_of = $1 AND horizon = $2"
_CLOSES_SQL = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day = ANY($2::date[])"
_SAVE_IC_SQL = """
    INSERT INTO factor_ic (as_of, horizon, ic, n, end_day) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (as_of, horizon) DO UPDATE SET ic = EXCLUDED.ic, n = EXCLUDED.n, end_day = EXCLUDED.end_day,
        graded_at = NOW()
"""


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
    from services.agents.dossier_engine import DossierEngine

    from services.agents.grader import Grader

    from services.agents.earnings_reaction import EarningsReactionEngine
    from services.agents.filing_extractor import FilingExtractor

    from services.agents.portfolio_engine import PortfolioEngine
    from shared.utils import model_portfolio as mp
    from services.agents.auto_trader import AutoTrader
    from services.agents.feature_jobs import FeatureJobs
    from services.agents.researcher import Researcher
    from services.agents.customer_links_job import CustomerLinksJob
    from services.agents.llm_news_job import LlmNewsJob
    from services.agents.alert_move_job import AlertMoveJob
    from services.agents.comention_job import CoMentionJob
    from services.agents.case_file_job import CaseFileJob
    from services.agents.activist_13d_job import Activist13DJob

    from services.agents.news_model_refit import NewsModelRefit

    await asyncio.gather(FactorEngine(db, redis).run(), RecommendationEngine(db, redis).run(),
                         DossierEngine(db, redis).run(), Grader(db, redis).run(),
                         EarningsReactionEngine(db, redis).run(), PortfolioEngine(db, redis).run(),
                         FilingExtractor(db, redis).run(),
                         # The news read's monthly refit (trial M).
                         NewsModelRefit(db, redis).run(),
                         # The industry-neutral book in shadow beside the live one.
                         PortfolioEngine(db, redis, portfolio=mp.PORTFOLIO_INDUSTRY_NEUTRAL,
                                         variant="industry_neutral").run(),
                         # The headline book's positions taken in the paper account (off unless enabled).
                         AutoTrader(db, redis).run(),
                         # Filing-based stock features for the model's trials (stock_features_daily).
                         FeatureJobs(db, redis).run(),
                         # The weekly feature screen that queues candidates for trials.
                         Researcher(db, redis).run(),
                         # Major customers from 10-Ks, and the customer-momentum feature.
                         CustomerLinksJob(db, redis).run(),
                         # A hosted model's read of new headlines, and its 30-day feature.
                         LlmNewsJob(db, redis).run(),
                         # The alerts' expected-move model, walk-forward weekly (reports only).
                         AlertMoveJob(db, redis).run(),
                         # Connected-firm momentum through news co-mentions (Ali & Hirshleifer).
                         CoMentionJob(db, redis).run(),
                         # One structured file of facts per stock for the agents to share.
                         CaseFileJob(db, redis).run(),
                         # Activist intent from 13D Item 4 (Brav et al. 2008).
                         Activist13DJob(db, redis).run())


__all__ = ["FactorEngine"]


if __name__ == "__main__":
    asyncio.run(main())
