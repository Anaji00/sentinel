"""Market-neutral grading of agent predictions (registered 13 Nov 2026, report only).

A raw direction hit rate rewards "up" in a rising market and "down" in a falling one, so it is graded here on the return
in excess of SPY (and of the stock's sector ETF where one exists). Nothing in this module feeds a gate or a score.

Returns are close-to-close on daily bars: from the last close on or before the call's date to the last close on or before
the exit date (call time plus horizon). The same bars price the stock and both benchmarks so the three are comparable.
"""
from __future__ import annotations

import bisect
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

from shared.utils.sectors import sector_of

SECTOR_ETFS: Dict[str, str] = {
    "Information Technology": "XLK", "Health Care": "XLV", "Financials": "XLF", "Consumer Discretionary": "XLY",
    "Communication Services": "XLC", "Industrials": "XLI", "Consumer Staples": "XLP", "Energy": "XLE",
    "Utilities": "XLU", "Real Estate": "XLRE", "Materials": "XLB",
}
UP_WORDS = {"up", "long", "bullish"}
DOWN_WORDS = {"down", "short", "bearish"}
MAX_STALE_DAYS = 5          # a bar older than this is a halt or a gap, not "the close on that date"
MIN_CLUSTERS = 5            # fewer distinct days than this: no t is reported


def sector_etf_for_sic(sic: object) -> Optional[str]:
    """The sector ETF for an SEC SIC code; None when the code maps to "Other" or is missing."""
    return SECTOR_ETFS.get(sector_of(sic))


def direction_sign(direction: object) -> Optional[int]:
    """+1 for an up call, -1 for a down call, None for anything else (flat and unknown are not graded)."""
    token = str(direction or "").strip().lower()
    if token in UP_WORDS:
        return 1
    if token in DOWN_WORDS:
        return -1
    return None


def close_on_or_before(days: Sequence[date], closes: Sequence[float], d: date) -> Optional[float]:
    """The last close on or before d, or None when there is none within MAX_STALE_DAYS. `days` is ascending."""
    i = bisect.bisect_right(days, d) - 1
    if i < 0 or (d - days[i]).days > MAX_STALE_DAYS or not closes[i] or closes[i] <= 0:
        return None
    return float(closes[i])


def bars_late(spy_days: Sequence[date], end: date) -> bool:
    """True when the loaded SPY bars stop before the exit date: the data is late (a top-up is behind), not missing, so a grading pass must not count an attempt."""
    return not spy_days or max(spy_days) < end


def window_return(days: Sequence[date], closes: Sequence[float], start: date, end: date) -> Optional[float]:
    """Simple return between the closes on or before start and end; None when either is missing or end <= start."""
    if end <= start:
        return None
    a, b = close_on_or_before(days, closes, start), close_on_or_before(days, closes, end)
    if a is None or b is None:
        return None
    return b / a - 1.0


def call_window(occurred_at: Union[datetime, str], horizon_hours: int) -> Tuple[date, date]:
    """(start date, exit date) of a call. Naive timestamps are read as UTC; ISO strings (as the database layer returns them) are parsed."""
    if isinstance(occurred_at, str):
        occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    t = occurred_at if occurred_at.tzinfo else occurred_at.replace(tzinfo=timezone.utc)
    return t.date(), (t + timedelta(hours=max(int(horizon_hours or 0), 0))).date()


ALIGNED_MIN_HORIZON_HOURS = 24
try:
    from zoneinfo import ZoneInfo
    _NY: Optional[object] = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    _NY = None


def aligned_window(occurred_at: Union[datetime, str], horizon_hours: int, sessions: Sequence[date]) -> Optional[Tuple[date, date]]:
    """Session-aligned (entry session, exit session) of a call of at least 24 hours, or None when it cannot be placed.

    Entry is the close of the last session completed at call time: the call's New York date if the call is at or after 16:00 there, else the day before (a
    pre-open call used to start from a close that had not happened yet). Exit is `max(1, horizon_hours // 24)` sessions after the entry session, so a Friday
    24-hour call exits on Monday, not on the Saturday that graded as exactly zero. `sessions` is the ascending trading calendar (SPY's bar dates); None when the
    calendar does not reach the exit session, so the caller parks the row instead of storing 0 (layer audit 5.2)."""
    if isinstance(occurred_at, str):
        occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    t = occurred_at if occurred_at.tzinfo else occurred_at.replace(tzinfo=timezone.utc)
    ny = t.astimezone(_NY) if _NY is not None else t - timedelta(hours=5)
    cutoff = ny.date() if ny.hour >= 16 else ny.date() - timedelta(days=1)
    i = bisect.bisect_right(sessions, cutoff) - 1
    if i < 0 or (cutoff - sessions[i]).days > MAX_STALE_DAYS:
        return None
    j = i + max(1, int(horizon_hours or 0) // 24)
    return (sessions[i], sessions[j]) if j < len(sessions) else None


def excess_returns(stock: Optional[float], spy: Optional[float], sector: Optional[float]) -> Dict[str, Optional[float]]:
    """Raw-direction-free excess returns: stock minus SPY, and stock minus its sector ETF where one was measured."""
    return {
        "stock_return": stock,
        "spy_return": spy,
        "sector_return": sector,
        "excess_spy": None if stock is None or spy is None else stock - spy,
        "excess_sector": None if stock is None or sector is None else stock - sector,
    }


BETA_SESSIONS = 126
MIN_BETA_RETURNS = 60


def beta_to_spy(days: Sequence[date], closes: Sequence[float], spy_days: Sequence[date], spy_closes: Sequence[float], before: date,
                sessions: int = BETA_SESSIONS, min_returns: int = MIN_BETA_RETURNS) -> Optional[float]:
    """The stock's beta to SPY from daily returns on the common dates strictly before `before` (the last `sessions` returns); None under `min_returns`
    returns or when SPY did not vary. The call's own day is excluded, so the beta never sees the move it is used to adjust."""
    spy = {d: float(c) for d, c in zip(spy_days, spy_closes) if c and c > 0 and d < before}
    common = [(d, float(c)) for d, c in zip(days, closes) if c and c > 0 and d < before and d in spy][-(sessions + 1):]
    if len(common) - 1 < min_returns:
        return None
    xs = [spy[b[0]] / spy[a[0]] - 1.0 for a, b in zip(common, common[1:])]
    ys = [b[1] / a[1] - 1.0 for a, b in zip(common, common[1:])]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    var = sum((x - mx) ** 2 for x in xs)
    if var <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var


def beta_excess(stock: Optional[float], spy: Optional[float], beta: Optional[float]) -> Optional[float]:
    """Stock return minus beta times SPY's return; None when any is missing or not finite."""
    if stock is None or spy is None or beta is None or not all(math.isfinite(v) for v in (stock, spy, beta)):
        return None
    return stock - beta * spy


def clustered_t(values_by_day: Dict[object, List[float]]) -> Dict[str, Optional[float]]:
    """Mean of the values with a t statistic that treats each day as one independent unit.

    Calls made on the same day share the market's move, so the standard error comes from the spread of the per-day means
    (equal weight per day), not from the pooled calls. Returns n (calls), days, mean (of the day means) and t.
    """
    day_means = [sum(v) / len(v) for v in values_by_day.values() if v]
    n_calls = sum(len(v) for v in values_by_day.values())
    k = len(day_means)
    if k == 0:
        return {"n": 0, "days": 0, "mean": None, "t": None}
    mean = sum(day_means) / k
    t: Optional[float] = None
    if k >= MIN_CLUSTERS:
        var = sum((m - mean) ** 2 for m in day_means) / (k - 1)
        se = math.sqrt(var / k)
        t = mean / se if se > 0 else None
    return {"n": n_calls, "days": k, "mean": mean, "t": t}


def hit_report(rows: Iterable[Tuple[str, date, Optional[float], Optional[bool]]]) -> List[Dict[str, object]]:
    """Hit rate on excess return by direction.

    Each row is (direction word, call date, signed excess return (positive = the call was right against the market),
    raw outcome_correct). A hit is a signed excess above zero. The t tests the hit indicator minus 0.5 and the signed
    excess against zero, both clustered by call day. Raw hit rate over the same rows is carried for comparison.
    """
    groups: Dict[str, List[Tuple[date, float, Optional[bool]]]] = defaultdict(list)
    for direction, day, signed, raw in rows:
        side = direction_sign(direction)
        if side is None or signed is None:
            continue
        groups["up" if side > 0 else "down"].append((day, signed, raw))
        groups["all"].append((day, signed, raw))
    out: List[Dict[str, object]] = []
    for name in ("up", "down", "all"):
        g = groups.get(name)
        if not g:
            continue
        hit_by_day: Dict[date, List[float]] = defaultdict(list)
        ex_by_day: Dict[date, List[float]] = defaultdict(list)
        for day, signed, _ in g:
            hit_by_day[day].append((1.0 if signed > 0 else 0.0) - 0.5)
            ex_by_day[day].append(signed)
        hits = clustered_t(hit_by_day)
        ex = clustered_t(ex_by_day)
        raws = [r for _, _, r in g if r is not None]
        out.append({
            "direction": name,
            "n": len(g),
            "days": hits["days"],
            "hit_rate": sum(1 for _, s, _ in g if s > 0) / len(g),
            "hit_t": hits["t"],
            "mean_excess_bps": None if ex["mean"] is None else ex["mean"] * 1e4,
            "excess_t": ex["t"],
            "raw_hit_rate": (sum(1 for r in raws if r) / len(raws)) if raws else None,
        })
    return out
