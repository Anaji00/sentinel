"""
shared/utils/stock_news.py

Company news for the stock system, every day: Alpaca's news feed (Benzinga,
tagged with the stocks each article is about, free with the account, history
back to 2015), each headline scored for tone by FinBERT (Araci 2019; the
ProsusAI model), and turned into what the rest of the platform reads. Pure;
collector-tradfi fetches, the correlation service scores.

  daily features  per stock and session: the tone of its articles and how
                  many there were, over the last 5 and 21 sessions, assigned
                  to the first close after publication (point in time)
  events          strongly toned articles about one or two stocks, for the
                  stock_news families the reaction study measures

The RSS headlines the platform already reads are world news; on 16 Oct 2026
only 919 in a month named a resolved company. This feed carries about 900
company-tagged articles a day.
"""
from __future__ import annotations

import bisect
from datetime import date, datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np

NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
NEWS_TABLE = "stock_news"
BACKFILL_START = date(2023, 9, 1)     # covers every test month of the walk-forward (late 2023 on) and its lookback
EVENT_SOURCE = "alpaca_news"
MAX_EVENT_SYMBOLS = 2                 # an article about one or two stocks, not a roundup
# |positive - negative| for an event, live and in history alike, so the study
# measures exactly what fires live; strict enough to keep the study's load in
# bounds: at 0.8 FinBERT passed 27% of single-stock articles, at 0.9 about 11%.
STRONG_TONE = 0.9
# An article's anomaly score: nothing here measures how unusual it is. This was
# abs(tone), so every event scored 0.90 or more by construction, and on 4 Nov
# 2026 all 5 "top ML anomalies" in the agents' world state and all 10 headlines
# in the scenario context were these. 0.5 sits with ordinary news (median 0.55)
# and clears the correlation rules' floors, none of which is above it.
EVENT_ANOMALY_SCORE = 0.5
NY = ZoneInfo("America/New_York")
FEATURE_WINDOWS = (5, 21)


def parse_item(item: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """One Alpaca news item -> a stock_news row, or None without id or headline."""
    try:
        nid = int(item.get("id"))
    except (TypeError, ValueError):
        return None
    headline = str(item.get("headline") or "").strip()
    created = item.get("created_at")
    if not headline or not created:
        return None
    at = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
    symbols = sorted({str(s).upper().replace("/", ".") for s in item.get("symbols") or [] if s})
    return {"id": nid, "created_at": at, "symbols": symbols, "headline": headline[:500],
            "summary": str(item.get("summary") or "")[:1000], "source": str(item.get("source") or ""),
            "url": str(item.get("url") or "")[:500]}


def tone(positive: Optional[float], negative: Optional[float]) -> Optional[float]:
    """FinBERT's positive minus negative probability, -1..1."""
    if positive is None or negative is None:
        return None
    return float(positive) - float(negative)


# Per stock and day, in SQL: the day is the New York date of publication plus
# eight hours, so an article after the 16:00 close counts toward the next day.
# Days that are not sessions roll forward to the next session in Python.
DAILY_SQL = f"""
    SELECT (((created_at AT TIME ZONE 'America/New_York') + INTERVAL '8 hours')::date) AS day, sym,
           sum(positive - negative) AS tone_sum, count(*) AS n
    FROM {NEWS_TABLE}, unnest(symbols) AS sym
    WHERE scored_at IS NOT NULL AND created_at >= $1 AND cardinality(symbols) BETWEEN 1 AND {MAX_EVENT_SYMBOLS + 1}
    GROUP BY 1, 2
"""


def daily_features(sessions: Sequence[date], tickers: Sequence[str],
                   rows: Iterable[Tuple[date, str, float, int]]) -> Dict[str, np.ndarray]:
    """[T, N] arrays from per-day aggregates (day, symbol, tone sum, articles):
    news_tone_5 / news_tone_21 (mean tone of the window's articles, NaN with
    none) and news_count_21 (log of 1 + articles). A day that is not a session
    counts toward the next session: news is known at the close after it."""
    T, N = len(sessions), len(tickers)
    col = {t: j for j, t in enumerate(tickers)}
    tone_sum = np.zeros((T, N))
    count = np.zeros((T, N))
    for d, sym, ts, n in rows:
        j = col.get(str(sym).upper())
        if j is None:
            continue
        i = bisect.bisect_left(sessions, d)
        if i >= T:
            continue
        tone_sum[i, j] += float(ts or 0.0)
        count[i, j] += int(n or 0)
    out: Dict[str, np.ndarray] = {}
    cs_t, cs_n = np.cumsum(tone_sum, axis=0), np.cumsum(count, axis=0)
    for w in FEATURE_WINDOWS:
        s_t, s_n = cs_t.copy(), cs_n.copy()
        s_t[w:] -= cs_t[:-w]
        s_n[w:] -= cs_n[:-w]
        with np.errstate(all="ignore"):
            out[f"news_tone_{w}"] = np.where(s_n > 0, s_t / np.maximum(s_n, 1), np.nan).astype(np.float32)
        if w == 21:
            out["news_count_21"] = np.log1p(s_n).astype(np.float32)
    return out


def event_row(r: Mapping[str, Any], positive: float, negative: float) -> Optional[Dict[str, Any]]:
    """A strongly toned article about one or two stocks as an event, else None."""
    tn = tone(positive, negative)
    syms = list(r.get("symbols") or [])
    if tn is None or abs(tn) < STRONG_TONE or not syms or len(syms) > MAX_EVENT_SYMBOLS:
        return None
    side = "positive" if tn > 0 else "negative"
    return {"tags": ["stock_news", f"tone_{side}", *[f"exposes:{s}" for s in syms]], "tone": round(tn, 3),
            "symbols": syms, "side": side}


__all__ = ["BACKFILL_START", "DAILY_SQL", "EVENT_ANOMALY_SCORE", "EVENT_SOURCE", "FEATURE_WINDOWS", "MAX_EVENT_SYMBOLS", "NEWS_TABLE",
           "NEWS_URL", "STRONG_TONE", "daily_features", "event_row", "parse_item", "tone"]
