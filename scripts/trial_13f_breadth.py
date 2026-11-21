"""Trial A3 (registered in reports/Upgrade pre-registrations - 12 Nov.md): ownership breadth against the next 21 sessions' excess return. Report only.

Feature: percentage change over three months in `fund_holdings_monthly.positions` per ticker, visible from its first public date (shared/utils/breadth_change.py).
Per rebalance date (every 21st session from 2020-07-01, so windows do not overlap): rank within the tradable set, statistic = mean log excess return over
SPY of the top quintile minus the bottom quintile from the close of that date to the close 21 sessions later; date-clustered bootstrap (10,000 draws).
Bar (registered, two-sided): |t| >= 2 in both halves of the dates, the same sign as the full sample.
Implementation choices fixed before the run (the registration leaves them open): tradable = close >= $5 and 20-session mean dollar volume >= $5m on the rebalance
date (point in time); minimum base of 10 positions; newest month at most 135 days old; at least 100 ranked names per date; tie-break by ticker.
Reads fund_holdings_monthly and daily_bars; writes nothing to the database. Own container, outside 22:00 to 06:00 UTC:
    docker run --rm --memory 3g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro -v <repo>/scripts:/app/scripts:ro \
        -w /app -e PYTHONPATH=/app sentinel-agents-heavy python scripts/trial_13f_breadth.py [--out reports/trial_13f_breadth.json]
"""
import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils import breadth_change as bc  # noqa: E402

START = date(2020, 7, 1)
HORIZON = 21
MIN_PRICE = 5.0
MIN_DOLLAR_VOLUME = 5e6

_BAR_SQL = """
    SELECT ticker, (array_agg(close ORDER BY day DESC))[1] AS close, avg(close * volume) AS dv
    FROM daily_bars
    WHERE day <= $1::date AND day > $1::date - 30 AND close > 0 AND volume > 0
    GROUP BY ticker
"""
_EXIT_SQL = "SELECT ticker, close FROM daily_bars WHERE day = $1::date AND close > 0"


async def run(out_path: str) -> Dict[str, Any]:
    from shared.db import get_timescale
    db = await get_timescale()
    spy = await db.query("SELECT day, close FROM daily_bars WHERE ticker = 'SPY' AND day >= $1::date ORDER BY day", START)
    sessions = [r["day"] for r in spy]
    bench = {r["day"]: float(r["close"]) for r in spy}
    dates = [d for d in bc.rebalance_dates(sessions, START, HORIZON) if sessions.index(d) + HORIZON < len(sessions)]
    holdings = await db.query("SELECT ticker, report_month, positions, first_public FROM fund_holdings_monthly")
    panel = bc.build_panel(holdings or [])
    spreads: List[Tuple[date, float]] = []
    skipped = 0
    for d in dates:
        exit_day = sessions[sessions.index(d) + HORIZON]
        entry = {r["ticker"]: (float(r["close"]), float(r["dv"] or 0)) for r in await db.query(_BAR_SQL, d)}
        exit_px = {r["ticker"]: float(r["close"]) for r in await db.query(_EXIT_SQL, exit_day)}
        feats: Dict[str, float] = {}
        rets: Dict[str, float] = {}
        for tk, (px, dv) in entry.items():
            if px < MIN_PRICE or dv < MIN_DOLLAR_VOLUME or tk not in exit_px or tk not in panel:
                continue
            f = bc.breadth_change(panel[tk], d)
            ret = bc.forward_log_excess({d: px, exit_day: exit_px[tk]}, bench, d, exit_day)
            if f is None or ret is None:
                continue
            feats[tk], rets[tk] = f, ret
        s = bc.quintile_spread(feats, rets)
        if s is None:
            skipped += 1
            continue
        spreads.append((d, s))
        print(f"{d} names {len(feats)} spread {s * 1e4:+.1f} bps")
    result = {"rebalance_dates": len(dates), "skipped_dates": skipped, **bc.clustered_bootstrap(spreads, draws=10000, seed=20261118)}
    print(json.dumps(result))
    Path(out_path).write_text(json.dumps({"result": result, "spreads": [(str(d), s) for d, s in spreads]}, indent=1), encoding="utf-8")
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/trial_13f_breadth.json")
    asyncio.run(run(ap.parse_args().out))
