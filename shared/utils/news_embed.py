"""
shared/utils/news_embed.py

What a stock's company news says about its next week, from the meaning of the
headlines rather than their tone. Pure; the correlation service embeds and
stores, the recommendation engine reads.

Trial M (pre-registered, 18 Oct 2026): every single-company Benzinga headline
since September 2023 (624,349 of them) embedded with all-mpnet-base-v2, the
soft correlator's model; per stock and session the normalised sum of the
embeddings of the headlines known by that session's close; a ridge walked
forward by month, target the next five sessions' return net of SPY, demeaned
across that session's news names. Out of sample over 675 sessions the mean
rank IC was 0.027 (t 4.31 on non-overlapping weeks) and the top news decile beat
the bottom by 128 bps a week before costs. FinBERT tone on the same articles
added nothing (trial J): the content carries what the tone does not (Chen,
Kelly & Xiu, Expected Returns and Large Language Models).

The forecast is relative to other stocks in the news that day -- the drift
every covered stock shares is the event families' business -- and is in
basis points over five sessions: the model's score times its out-of-sample
scale, clipped at two standard deviations of the score (the refit's scale
runs above one, and a straight line past the top decile overstates it).
"""
from __future__ import annotations

import json
import math
import pathlib
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

MODEL_KEY = "sentinel:news_embed:model"
DAY_PREFIX = "sentinel:news_embed:day:"
DAY_TTL_SEC = 6 * 86400
HORIZON_DAYS = 5
MAX_SYMBOLS = 2            # single-company news, as trained
CLIP_SD = 2.0
_NY = ZoneInfo("America/New_York")


# Trial M's final fit (ridge on all history to 9 Oct 2026, its out-of-sample
# scale), kept beside this module so the read survives a cleared Redis; the
# copy under MODEL_KEY, when present, is the one used.
PACKAGED_MODEL = pathlib.Path(__file__).with_name("news_embed_model.json")


def packaged_model() -> Optional[Dict[str, Any]]:
    try:
        return json.loads(PACKAGED_MODEL.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def news_day(created_at: datetime) -> date:
    """The day whose close an article was known by: published by 16:00 New
    York time counts that day, later the next (the session it maps to is the
    first trading day on or after this date)."""
    return (created_at.astimezone(_NY) + timedelta(hours=8)).date()


def day_key(d: date) -> str:
    return f"{DAY_PREFIX}{d.isoformat()}"


def add(total: Optional[Dict[str, Any]], vec: Sequence[float]) -> Dict[str, Any]:
    """A stock-day's running sum of unit headline embeddings, with its count."""
    if not total:
        return {"n": 1, "v": [round(float(x), 5) for x in vec]}
    return {"n": int(total["n"]) + 1, "v": [round(a + float(b), 5) for a, b in zip(total["v"], vec)]}


def merge(totals: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Several days' sums for one stock (a weekend's news rolls into Monday)."""
    out: Optional[Dict[str, Any]] = None
    for t in totals:
        if not t:
            continue
        out = dict(t) if out is None else {"n": out["n"] + t["n"], "v": [a + b for a, b in zip(out["v"], t["v"])]}
    return out


def score(total: Optional[Dict[str, Any]], beta: Sequence[float]) -> Optional[float]:
    """The ridge's prediction for the normalised sum, as trained."""
    if not total or not beta or len(total["v"]) != len(beta):
        return None
    norm = math.sqrt(sum(x * x for x in total["v"]))
    if norm <= 0:
        return None
    return sum(x * b for x, b in zip(total["v"], beta)) / norm


def expected_bps(total: Optional[Dict[str, Any]], model: Optional[Dict[str, Any]]) -> Optional[float]:
    """Expected return over HORIZON_DAYS against other news names, in bps."""
    if not model:
        return None
    s = score(total, model.get("beta") or [])
    if s is None:
        return None
    sd = float(model.get("pred_sd") or 0.0)
    if sd > 0:
        s = max(-CLIP_SD * sd, min(CLIP_SD * sd, s))
    return round(float(model.get("slope") or 0.0) * s * 1e4, 1)


def days_since(last_session: date, today_news_day: date) -> List[date]:
    """News days whose articles were not known by the last completed close."""
    out, d = [], last_session + timedelta(days=1)
    while d <= today_news_day:
        out.append(d)
        d += timedelta(days=1)
    return out


__all__ = ["PACKAGED_MODEL", "packaged_model", "DAY_PREFIX", "DAY_TTL_SEC", "HORIZON_DAYS", "MAX_SYMBOLS", "MODEL_KEY",
           "add", "day_key", "days_since", "expected_bps", "merge", "news_day", "score"]
