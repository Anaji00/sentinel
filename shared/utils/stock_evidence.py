"""
shared/utils/stock_evidence.py

The evidence behind one stock_correlation_agent call: which measured macro channel
actually moved, how the stock has traded against it, and the stored signals, each
labelled with what it has been measured to say about the next session. The model
cites these by id, and a directional call that cites none is withheld.

Measured 5 Nov 2026 over 288 sessions and up to 1,297 stocks, excess return against
SPY, one market day counting once. 15 signals and 2 horizons were tested, so t near
3 is modest evidence, and these are forecasts to grade, not trades:

    call-put IV spread       next session IC +0.014, t +2.75 over all 691 sessions from
                             Feb 2024 (+0.022 over the last 288). Mostly overnight: next
                             open to close IC +0.007, t +1.4. Nothing after: the session
                             beyond t -1.1, 5 sessions t +0.3, 21 sessions t -0.5. Stale
                             option closes inflate it: the spread against the same day's
                             return is IC -0.25, and removing that return leaves +0.010
    short volume ratio      next session IC -0.012, t -2.9
    macro beta x move        next session IC +0.012, t +0.7 (rolling beta, 38 stocks)
    earnings day             median |excess move| 243 bps against 114 bps

Own returns, skew, put/call ratio, insider flow and volatility were all near zero.

Pure: stored values in, evidence and verdicts out.
"""
from __future__ import annotations

import bisect
import math
import re
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from shared.utils.macro_exposure import YIELDS

# A graded forecast no firmer than this: 49.4% right at Brier 0.287 against 0.244 is
# overconfidence, and nothing measured here supports a firmer one-session call.
MAX_FORECAST_CONVICTION = 0.60
EVIDENCE_MIN_EDGE = 0.25       # rank spread under this, the evidence baseline makes no call
MIN_RANKED_NAMES = 50          # fewer names than this is not a cross-section
MAX_CHANNELS = 3
MAX_HEADLINES = 3
HEADLINE_CHARS = 140
MIN_INSIDER_USD = 100_000      # less than this over 90 days is not a statement

_SIGNAL = "measured next-session signal"
_CONTEXT = "context: no measured next-session signal"
_RISK = "move size, not direction"
_CITATION = re.compile(r"\bE\d+\b", re.IGNORECASE)


def macro_stats(symbol: str, closes_newest_first: Sequence[float]) -> Optional[Dict[str, float]]:
    """The instrument's last change in the units its beta was fitted in (basis points
    for a yield, else a log return), its 5-session change and its size in standard
    deviations of the sessions before it. None without two usable closes."""
    closes = [float(c) for c in reversed(closes_newest_first) if c]
    if len(closes) < 2:
        return None
    if symbol.upper() in YIELDS:
        ch = [(b - a) * 100.0 for a, b in zip(closes, closes[1:])]
    else:
        ch = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
    if not ch:
        return None
    prior = ch[:-1]
    sd = _std(prior) if len(prior) >= 20 else None
    return {"last": ch[-1], "five": sum(ch[-5:]), "z": ch[-1] / sd if sd else 0.0,
            "pct": closes[-1] / closes[-2] - 1 if closes[-2] else 0.0}


def _std(xs: Sequence[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(v) if v > 0 else None


def rank_channels(exposures: Iterable[Mapping[str, Any]], stats: Mapping[str, Optional[Mapping[str, float]]]) -> List[Dict[str, Any]]:
    """Each exposure whose instrument has a move, with the excess move it implies
    (beta x change, in bps), largest first: the channel that transmitted today, not
    the strongest exposure to an instrument that did not move."""
    out = []
    for e in exposures:
        s = stats.get(e.get("macro"))
        beta = e.get("beta")
        if not s or beta is None:
            continue
        out.append({**e, "stats": dict(s), "implied_bps": float(beta) * float(s["last"]) * 1e4})
    return sorted(out, key=lambda c: -abs(c["implied_bps"]))


def stock_tape(closes: Sequence[Tuple[Any, float]], spy: Sequence[Tuple[Any, float]]) -> Optional[Dict[str, float]]:
    """Log excess return against SPY over 1, 5 and 20 common sessions, and its daily
    standard deviation; None with too little history to say."""
    p, m = {str(d)[:10]: float(c) for d, c in closes if c}, {str(d)[:10]: float(c) for d, c in spy if c}
    days = sorted(set(p) & set(m))
    if len(days) < 2:
        return None
    ex = [math.log(p[b] / p[a]) - math.log(m[b] / m[a]) for a, b in zip(days, days[1:]) if p[a] > 0 and p[b] > 0]
    if not ex:
        return None
    return {"d1": ex[-1], "d5": sum(ex[-5:]), "d20": sum(ex[-20:]), "vol": _std(ex[-60:]) or 0.0}


def peers_last_session(rows: Iterable[Mapping[str, Any]], spy: Sequence[Tuple[Any, float]]) -> Dict[str, float]:
    """Each peer's last-session log excess return, from rows of ticker, day and close."""
    by: Dict[str, List[Tuple[Any, float]]] = {}
    for r in rows:
        by.setdefault(str(r["ticker"]).upper(), []).append((r["day"], float(r["close"])))
    out = {}
    for t, v in by.items():
        tape = stock_tape(v, spy)
        if tape:
            out[t] = tape["d1"]
    return out


def percentile_ranks(values: Mapping[str, Any]) -> Dict[str, float]:
    """Each name's rank in today's cross-section, 0 to 1. Empty when too few are ranked:
    a thin cross-section is missing data, not a ranking."""
    clean = {k: float(v) for k, v in values.items() if v is not None and math.isfinite(float(v))}
    if len(clean) < MIN_RANKED_NAMES:
        return {}
    ordered = sorted(clean.values())
    n = len(ordered) - 1
    return {k: (bisect.bisect_left(ordered, v) + bisect.bisect_right(ordered, v) - 1) / (2 * n) for k, v in clean.items()}


def evidence_direction(cp_rank: Optional[float], short_rank: Optional[float], min_edge: float = EVIDENCE_MIN_EDGE) -> Optional[str]:
    """The no-model call from the two signals with a measured next-session reading: a
    high call-put spread and a low short volume ratio lean up. Both are required, and
    a small lean makes no call. Pure."""
    if cp_rank is None or short_rank is None:
        return None
    lean = (cp_rank - 0.5) - (short_rank - 0.5)
    if abs(lean) < min_edge:
        return None
    return "up" if lean > 0 else "down"


def next_session(today: date) -> date:
    """The next weekday. Exchange holidays are not known here, so a report after one
    can read as a session late; that errs towards making the call."""
    d = today + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def reports_by_next_session(report_day: Any, today: date) -> bool:
    """Whether a scheduled report falls today or by the next session."""
    if not report_day:
        return False
    try:
        day = date.fromisoformat(str(report_day)[:10])
    except ValueError:
        return False
    return today <= day <= next_session(today)


def day_range_pct(atm_iv: Optional[float]) -> Optional[float]:
    """One standard deviation of a session implied by annualised ATM IV, in percent."""
    if atm_iv is None or not atm_iv or atm_iv <= 0:
        return None
    return float(atm_iv) / math.sqrt(252) * 100.0


def change_text(symbol: str, value: float) -> str:
    return f"{value:+.1f} bp" if symbol.upper() in YIELDS else f"{value * 100:+.2f}%"


def _clean_headline(text: Any) -> str:
    return " ".join(str(text or "").split())[:HEADLINE_CHARS]


def build_card(ticker: str, channels: Sequence[Mapping[str, Any]], tape: Optional[Mapping[str, float]] = None,
               cp_rank: Optional[float] = None, short_rank: Optional[float] = None,
               options: Optional[Mapping[str, Any]] = None, report_day: Any = None, surprise: Optional[Mapping[str, Any]] = None,
               factor_pct: Optional[float] = None, short_interest: Optional[Mapping[str, Any]] = None,
               borrow_fee: Optional[float] = None, insider_net: Optional[float] = None,
               peers: Optional[Mapping[str, float]] = None, headlines: Sequence[str] = ()) -> List[Dict[str, str]]:
    """The call's evidence as numbered items. Only what is stored appears: a missing
    input is left out rather than shown as zero."""
    items: List[Tuple[str, str]] = []
    for c in list(channels)[:MAX_CHANNELS]:
        s, m = c["stats"], c["macro"]
        items.append((_CONTEXT, f"{m} {change_text(m, s['last'])} last session ({s['z']:+.1f} sd), {change_text(m, s['five'])} over 5; "
                                f"beta {float(c['beta']):+.4f} (t {float(c.get('t') or 0):+.1f}) implies {c['implied_bps']:+.0f} bps against the market"))
    if tape:
        line = (f"{ticker} against the market: {tape['d1'] * 100:+.2f}% last session, {tape['d5'] * 100:+.2f}% over 5, "
                f"{tape['d20'] * 100:+.2f}% over 20; usual day {tape['vol'] * 100:.2f}%")
        if channels:
            gap = tape["d1"] * 1e4 - float(channels[0]["implied_bps"])
            line += f"; {gap:+.0f} bps beyond what the top channel implies"
        items.append((_CONTEXT, line))
    if cp_rank is not None:
        items.append((_SIGNAL, f"Call-put IV spread ranks {cp_rank * 100:.0f} of 100 today; a high rank has led to a better next session, mostly overnight, and to nothing after it (IC +0.014, t +2.75 over 691 sessions)"))
    if short_rank is not None:
        items.append((_SIGNAL, f"Short volume ratio ranks {short_rank * 100:.0f} of 100 today; a high rank has led to a worse next session (IC -0.012, t -2.9)"))
    day_range = day_range_pct((options or {}).get("atm_iv"))
    if day_range is not None:
        items.append((_RISK, f"Options imply about {day_range:.1f}% for one session (ATM IV {float(options['atm_iv']) * 100:.0f}%)"))
    if report_day:
        items.append((_RISK, f"Next scheduled report {str(report_day)[:10]}; report days have moved about 2x a normal day (median 243 against 114 bps)"))
    if surprise and surprise.get("surprise_pct") is not None:
        items.append((_CONTEXT, f"Last EPS surprise {float(surprise['surprise_pct']):+.1f}% ({surprise.get('period')})"))
    if factor_pct is not None:
        items.append((_CONTEXT, f"Factor model monthly rank {float(factor_pct) * 100:.0f} of 100; a one-month view, not next-session"))
    if short_interest and short_interest.get("days_to_cover") is not None:
        change = short_interest.get("change_pct")
        items.append((_CONTEXT, f"Short interest {float(short_interest['days_to_cover']):.1f} days to cover"
                                + (f", {float(change):+.1f}% since the last report" if change is not None else "")))
    if borrow_fee is not None:
        items.append((_CONTEXT, f"Borrow fee {float(borrow_fee):.2f}% a year"))
    if insider_net and abs(insider_net) >= MIN_INSIDER_USD:
        items.append((_CONTEXT, f"Insiders net {'bought' if insider_net > 0 else 'sold'} ${abs(float(insider_net)) / 1e6:.1f}M over 90 days, planned sales excluded"))
    if peers:
        mean = sum(peers.values()) / len(peers)
        shown = ", ".join(f"{t} {v * 100:+.1f}%" for t, v in list(peers.items())[:5])
        line = f"Measured peers against the market last session: {shown}; mean {mean * 100:+.2f}%"
        if tape:
            line += f"; {ticker} {(tape['d1'] - mean) * 100:+.2f}% against them"
        items.append((_CONTEXT, line))
    for h in list(headlines)[:MAX_HEADLINES]:
        text = _clean_headline(h)
        if text:
            items.append(("news, unverified", f'Headline: "{text}"'))
    return [{"id": f"E{k + 1}", "tag": tag, "text": text} for k, (tag, text) in enumerate(items)]


def card_lines(card: Sequence[Mapping[str, str]]) -> str:
    return "\n".join(f"  {e['id']} [{e['tag']}] {e['text']}" for e in card)


def citations(values: Iterable[Any]) -> List[str]:
    """Evidence ids out of whatever the model wrote around them, in order, once each."""
    seen: List[str] = []
    for v in values or []:
        for hit in _CITATION.findall(str(v)):
            cid = hit.upper()
            if cid not in seen:
                seen.append(cid)
    return seen


def known_citations(cited: Iterable[str], card: Sequence[Mapping[str, str]]) -> List[str]:
    ids: Set[str] = {e["id"] for e in card}
    return [c for c in cited if c in ids]


def capped_conviction(value: float) -> float:
    return min(float(value), MAX_FORECAST_CONVICTION)


__all__ = ["EVIDENCE_MIN_EDGE", "MAX_FORECAST_CONVICTION", "build_card", "capped_conviction", "card_lines", "change_text", "citations",
           "day_range_pct", "evidence_direction", "known_citations", "macro_stats", "next_session", "peers_last_session",
           "percentile_ranks", "rank_channels", "reports_by_next_session", "stock_tape"]
