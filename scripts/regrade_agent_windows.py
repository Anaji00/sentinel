"""Re-grade already graded agent calls of 24 hours or more with the session-aligned window (layer audit 5.2). Dry run unless --apply.

Idempotent: a row is rewritten only when its recomputed stock, SPY or sector return differs from the stored one, so a second run changes nothing. Bounded by
--limit (default 2000, oldest first) and --days (default 90). `excess_beta` is cleared on a rewritten row, and the daily grader's beta pass refills it from the
stored 126-session beta. Reports, per agent, the mean excess over SPY per call date before and after, with the t over dates (shared/utils/excess_return.clustered_t),
so a change of verdict is visible before anything is written.
Run: docker exec -i -w /app sentinel-api_gateway python - --days 60 < scripts/regrade_agent_windows.py   (add --apply to write)
"""
import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402
from shared.utils.excess_return import aligned_window, clustered_t, sector_etf_for_sic, window_return  # noqa: E402

_ROWS = """
    SELECT p.id, p.agent_name, upper(p.ticker) AS ticker, p.occurred_at, p.horizon_hours, cp.sic, p.stock_return, p.spy_return, p.sector_return, p.excess_spy
    FROM agent_predictions p LEFT JOIN company_profile cp ON cp.ticker = upper(p.ticker)
    WHERE p.excess_graded_at IS NOT NULL AND p.horizon_hours >= 24 AND p.occurred_at > NOW() - make_interval(days => $1)
    ORDER BY p.occurred_at LIMIT $2
"""
_BARS = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND close > 0 ORDER BY ticker, day"
_UPDATE = """
    UPDATE agent_predictions SET stock_return = $2, spy_return = $3, sector_return = $4, excess_spy = $5, excess_sector = $6,
        excess_beta = NULL, excess_beta_attempts = 0 WHERE id = $1
"""
EPS = 1e-12


def _when(x) -> datetime:
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x


def _same(a, b) -> bool:
    return (a is None and b is None) or (a is not None and b is not None and abs(float(a) - float(b)) < EPS)


async def main(days: int, limit: int, apply: bool) -> None:
    db = await get_timescale()
    rows = await db.query(_ROWS, days, limit) or []
    tickers = {"SPY"} | {r["ticker"] for r in rows}
    etfs = {sector_etf_for_sic(r["sic"]) for r in rows} - {None}
    first = min((_when(r["occurred_at"]).date() for r in rows), default=date.today()) - timedelta(days=10)
    bars = defaultdict(lambda: ([], []))
    allt = sorted(tickers | etfs)
    for i in range(0, len(allt), 300):
        for b in await db.query(_BARS, allt[i:i + 300], first) or []:
            d, c = bars[b["ticker"]]
            d.append(b["day"] if isinstance(b["day"], date) else date.fromisoformat(str(b["day"])[:10]))
            c.append(float(b["close"]))
    spy_days = bars["SPY"][0]
    before, after = defaultdict(lambda: defaultdict(list)), defaultdict(lambda: defaultdict(list))
    changed = unplaced = 0
    for r in rows:
        placed = aligned_window(r["occurred_at"], int(r["horizon_hours"]), spy_days)
        if placed is None or r["ticker"] not in bars:
            unplaced += 1
            continue
        s, e = placed
        stock = window_return(*bars[r["ticker"]], s, e)
        spy = window_return(*bars["SPY"], s, e)
        etf = sector_etf_for_sic(r["sic"])
        sector = window_return(*bars[etf], s, e) if etf and etf in bars else None
        if stock is None or spy is None:
            unplaced += 1
            continue
        day = _when(r["occurred_at"]).date()
        if r["excess_spy"] is not None:
            before[r["agent_name"]][day].append(float(r["excess_spy"]))
        after[r["agent_name"]][day].append(stock - spy)
        if not (_same(stock, r["stock_return"]) and _same(spy, r["spy_return"]) and _same(sector, r["sector_return"])):
            changed += 1
            if apply:
                await db.execute(_UPDATE, r["id"], stock, spy, sector, stock - spy, None if sector is None else stock - sector)
    print(f"rows {len(rows)} changed {changed} unplaced {unplaced} apply={apply}")
    for agent in sorted(after):
        b, a = clustered_t(before[agent]), clustered_t(after[agent])
        print(f"{agent}: before {b}  after {a}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.days, a.limit, a.apply))
