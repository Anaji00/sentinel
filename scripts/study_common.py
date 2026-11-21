"""Shared loader for the free-dataset event studies: split- and dividend-adjusted daily closes from daily_bars, and the SPY benchmark map."""
import sys
from pathlib import Path
from typing import Dict, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils.event_study import Series  # noqa: E402

BENCH = "SPY"


async def load_series(db, tickers: Iterable[str], since) -> Dict[str, Series]:
    rows = await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date ORDER BY ticker, day",
                          sorted(set(tickers) | {BENCH}), since)
    out: Dict[str, Series] = {}
    for r in rows or []:
        days, closes = out.setdefault(r["ticker"], ([], []))
        days.append(r["day"])
        closes.append(float(r["close"]))
    return out


def bench_map(series: Dict[str, Series]) -> Dict:
    days, closes = series.get(BENCH, ([], []))
    return dict(zip(days, closes))
