"""
services/agents/news_model_refit.py

The news read's monthly refit (shared/utils/news_embed.py, trial M), inside the
platform: per-stock news embedding sums from news_embed_daily, five-session
returns net of SPY from daily_bars, the walk-forward on monthly sufficient
statistics (shared/utils/news_embed_fit.py), each stock-session's
out-of-sample score kept in news_embed_oos, and the refit published under
news_embed.MODEL_KEY only if it still clears trial M's bar.

Two passes over the stored sums, one month of sessions at a time: the first
builds each month's statistics, the second scores each test month with the
model trained before it. Runs in the factor-model service, idle by day.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Tuple

import numpy as np

from shared.utils import news_embed as ne
from shared.utils import news_embed_fit as nf
from shared.utils import stock_news as sn
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agents.news_model_refit")

REFIT_EVERY_DAYS = 30
CHECK_EVERY_SEC = 3600             # hourly, so the run lands in RUN_HOUR_UTC
RUN_HOUR_UTC = 14                  # mid-morning New York, between the nightly runs
MIN_PRICE, MIN_ADV = 5.0, 5e6
LOAD_BATCH = 500
_TICKERS_SQL = "SELECT DISTINCT ticker FROM news_embed_daily WHERE day >= $1"
_BARS_SQL = """
    SELECT ticker, day, close, volume FROM daily_bars
    WHERE day >= $1 AND ticker = ANY($2::text[]) AND close > 0
"""
_SUMS_SQL = "SELECT day, ticker, n, v FROM news_embed_daily WHERE day > $1 AND day <= $2"
_OOS_SQL = """
    INSERT INTO news_embed_oos (day, ticker, score, fitted_at) VALUES ($1, $2, $3, NOW())
    ON CONFLICT (day, ticker) DO UPDATE SET score = EXCLUDED.score, fitted_at = NOW()
"""


def _d(x: Any) -> date:
    return x if isinstance(x, date) else date.fromisoformat(str(x)[:10])


class NewsModelRefit:
    def __init__(self, db: Any, redis: Any) -> None:
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def _panel(self) -> Tuple[List[date], List[str], np.ndarray, np.ndarray]:
        start = sn.BACKFILL_START - timedelta(days=45)
        tickers = sorted({str(r["ticker"]).upper() for r in await self.db.query(_TICKERS_SQL, sn.BACKFILL_START) or []}
                         | {"SPY"})
        # Filled a batch at a time: 7,000 names of rows held at once as Python
        # tuples took 1.5 GB, more than this service has to spare.
        days = sorted(_d(r["day"]) for r in await self.db.query(_BARS_SQL, start, ["SPY"]) or [])
        di = {d: i for i, d in enumerate(days)}
        ti = {t: j for j, t in enumerate(tickers)}
        C = np.full((len(days), len(tickers)), np.nan)
        V = np.full((len(days), len(tickers)), np.nan)
        for k in range(0, len(tickers), LOAD_BATCH):
            for r in await self.db.query(_BARS_SQL, start, tickers[k:k + LOAD_BATCH]) or []:
                i = di.get(_d(r["day"]))
                if i is not None:
                    j = ti[str(r["ticker"]).upper()]
                    C[i, j] = float(r["close"])
                    V[i, j] = float(r["close"]) * float(r["volume"] or 0.0)
        return days, tickers, C, V

    async def _months(self, days: List[date], tickers: List[str], C: np.ndarray, V: np.ndarray
                      ) -> AsyncIterator[Tuple[str, np.ndarray, List[str], np.ndarray, np.ndarray]]:
        """Each month of sessions: (month, session index, ticker, normalised
        embedding, five-session excess return demeaned across the session's
        news names), the rows trial M used."""
        h = ne.HORIZON_DAYS
        spy = C[:, tickers.index("SPY")]
        fwd = np.full(C.shape, np.nan)
        fwd[:-h] = C[h:] / C[:-h] - 1.0 - (spy[h:] / spy[:-h] - 1.0)[:, None]
        fwd = np.clip(fwd, -0.5, 0.5)
        adv = np.full(C.shape, np.nan)
        for i in range(20, len(days)):
            adv[i] = np.nanmean(V[i - 20:i], axis=0)
        ti = {t: j for j, t in enumerate(tickers)}
        first = bisect.bisect_left(days, sn.BACKFILL_START)
        by_month: Dict[str, List[int]] = {}
        for i in range(first, len(days) - h):
            by_month.setdefault(days[i].strftime("%Y-%m"), []).append(i)
        for month, idx in by_month.items():
            lo, hi = idx[0], idx[-1]
            sums: Dict[Tuple[int, str], np.ndarray] = {}
            for r in await self.db.query(_SUMS_SQL, days[lo - 1], days[hi]) or []:
                i = bisect.bisect_left(days, _d(r["day"]))
                t = str(r["ticker"]).upper()
                if i < lo or i > hi or t not in ti:
                    continue
                v = nf.unpack(bytes(r["v"]))
                key = (i, t)
                sums[key] = sums[key] + v if key in sums else v.copy()
            keep = [(i, t, v) for (i, t), v in sums.items()
                    if C[i, ti[t]] >= MIN_PRICE and adv[i, ti[t]] >= MIN_ADV and np.isfinite(fwd[i, ti[t]])]
            if not keep:
                continue
            sess = np.array([i for i, _t, _v in keep])
            X = np.stack([v for _i, _t, v in keep]).astype(np.float32)
            X /= np.linalg.norm(X, axis=1, keepdims=True) + 1e-9
            y = np.array([fwd[i, ti[t]] for i, t, _v in keep])
            for i in np.unique(sess):
                m = sess == i
                y[m] -= y[m].mean()
            yield month, sess, [t for _i, t, _v in keep], X, y

    async def run_once(self) -> Dict[str, Any]:
        days, tickers, C, V = await self._panel()
        by_month: Dict[str, nf.Stats] = {}
        async for month, _s, _t, X, y in self._months(days, tickers, C, V):
            st = nf.Stats()
            st.add(X, y)
            by_month[month] = st
        months = sorted(by_month)
        fitted = nf.folds(months, by_month)
        ics: List[float] = []
        spreads: List[float] = []
        pairs: List[Tuple[float, float]] = []
        oos_rows: List[Tuple[date, str, float]] = []
        async for month, sess, names, X, y in self._months(days, tickers, C, V):
            if month not in fitted:
                continue
            p = X @ fitted[month][0]
            for i in np.unique(sess):
                m = sess == i
                ic = nf.rank_ic(p[m], y[m])
                if ic is None:
                    continue
                ics.append(ic)
                q = max(1, int(m.sum()) // 10)
                o = np.argsort(p[m])
                spreads.append(float(y[m][o[-q:]].mean() - y[m][o[:q]].mean()))
            pairs.extend(zip(p.tolist(), y.tolist()))
            oos_rows.extend((days[int(i)], t, round(float(s), 7)) for i, t, s in zip(sess, names, p))
        summ = nf.summary(ics, spreads, ne.HORIZON_DAYS)
        for k in range(0, len(oos_rows), 5000):
            await self.db.execute_many(_OOS_SQL, oos_rows[k:k + 5000])
        P = np.array([a for a, _b in pairs])
        Y = np.array([b for _a, b in pairs])
        slope = float((P * Y).sum() / (P * P).sum()) if len(P) else 0.0
        beta, lam, pred_sd = nf.final(by_month, [lam for _b, lam in fitted.values()])
        model = {"model": "all-mpnet-base-v2", "horizon_days": ne.HORIZON_DAYS, "lambda": lam,
                 "slope": round(slope, 6), "beta": [round(float(b), 7) for b in beta],
                 "oos_mean_ic": summ["mean_ic"], "oos_ic_t": summ["ic_t"], "oos_sessions": summ["sessions"],
                 "oos_decile_spread_bps": summ["decile_spread_bps"], "pred_sd": round(pred_sd, 6),
                 "trained_through": months[-1] if months else None, "samples": int(sum(s.n for s in by_month.values())),
                 "fitted_at": datetime.now(timezone.utc).isoformat(), "trial": "M, refit monthly"}
        published = nf.passes(summ) and slope > 0
        if published:
            await self.raw.set(ne.MODEL_KEY, json.dumps(model))
        out = {**{k: v for k, v in model.items() if k != "beta"}, "published": published, "oos_rows": len(oos_rows)}
        logger.info("News model refit: %s", out)
        return out

    async def _due(self) -> bool:
        cur = await self.raw.get(ne.MODEL_KEY)
        try:
            at = json.loads(cur).get("fitted_at") if cur else None
        except ValueError:
            at = None
        if not at:
            return True
        return datetime.now(timezone.utc) - datetime.fromisoformat(at) >= timedelta(days=REFIT_EVERY_DAYS)

    async def run(self) -> None:
        while True:
            try:
                if datetime.now(timezone.utc).hour == RUN_HOUR_UTC and await self._due():
                    await self.run_once()
            except Exception as e:
                swallowed("agents.news_model_refit", e, logger)
            await asyncio.sleep(CHECK_EVERY_SEC)


__all__ = ["NewsModelRefit", "REFIT_EVERY_DAYS"]
