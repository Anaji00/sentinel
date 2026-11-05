"""
shared/utils/thesis.py

The bull case and the bear case for one stock, written on request by the local model
from facts the platform already holds, and shown only if it cites them. Pure: the
route (services/api_gateway/routes/stock_thesis.py) does the I/O.

Why it is built this way. A model asked for a "thesis" will write a confident one from
nothing. So it is handed numbered facts (the stock model's rank, what stands out about
the stock, its price moves, scored headlines, unusual activity) and told to use only
those; each point must cite fact ids that exist, and a point citing nothing it was
given is dropped, as the analyst agent's answers are (shared/utils/analyst.py). Advice
and predictions are dropped too: this is two sides of the evidence, not a call.

Inference is the scarcest resource on this host, so the route runs it only when asked,
keeps the answer six hours per stock, allows each person a few an hour, and gives up
rather than queue when the model is busy.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

MAX_POINTS = 4
MIN_FACTS = 3                  # below this there is nothing to argue from
CACHE_TTL_SEC = 6 * 3600       # a ready answer is kept this long
PENDING_TTL_SEC = 20 * 60      # a running job that never reports back is forgotten after this
FAILED_TTL_SEC = 10 * 60
RATE_PER_HOUR = 6              # new generations per person per hour
DISCLAIMER = "Written by a small AI model from the facts listed under each point. It can be wrong. Not financial advice."

_FACT = re.compile(r"\bF(\d{1,2})\b")
# A point that predicts a price or tells someone what to do is not evidence for a side.
_ADVICE = re.compile(
    r"\b(you should|we recommend|i recommend|strong buy|strong sell|buy now|sell now|price target|guaranteed|"
    r"will (?:rise|fall|double|triple|crash|soar|plunge)|is going to (?:rise|fall|double|crash|soar))\b",
    re.IGNORECASE,
)


class ThesisPoint(BaseModel):
    """One point, as the model must write it."""

    point: str = Field(default="", max_length=240, description="One plain sentence, no jargon")
    cited: List[str] = Field(default_factory=list, description="Fact ids used, e.g. ['F1', 'F4']")


class ThesisView(BaseModel):
    """The model's whole answer (the schema it must fill)."""

    bull: List[ThesisPoint] = Field(default_factory=list, description="Reasons the stock could do well")
    bear: List[ThesisPoint] = Field(default_factory=list, description="Reasons the stock could do badly")
    would_change_it: str = Field(default="", max_length=300, description="What new information would shift the balance")


def _tone_words(tone: Optional[float]) -> str:
    if tone is None:
        return ""
    if tone >= 0.3:
        return " (the wording is positive)"
    if tone <= -0.3:
        return " (the wording is negative)"
    return ""


def facts(
    ticker: str,
    percentile: Optional[float],
    horizon_days: Optional[int],
    traits: Iterable[str],
    moves: Mapping[str, Optional[float]],
    headlines: Iterable[Tuple[str, str, Optional[float]]],
    events: Iterable[Tuple[str, str]],
) -> List[str]:
    """The numbered facts, F1 onward, each one a plain sentence."""
    out: List[str] = []
    if percentile is not None:
        days = f"the next {horizon_days} trading days" if horizon_days else "the coming weeks"
        out.append(
            f"Our stock model ranks {ticker} at {percentile * 100:.0f} out of 100 among about 1,000 liquid stocks "
            f"for {days} (higher means it is expected to do better than the market)."
        )
    for text in traits:
        if text:
            out.append(f"{ticker} stands out: {text}")
    for label, change in moves.items():
        if change is not None:
            out.append(f"{ticker}'s price changed {change * 100:+.1f}% over {label}.")
    for when, text, tone in headlines:
        out.append(f"News on {when}: {text}{_tone_words(tone)}")
    for when, text in events:
        out.append(f"Unusual activity on {when}: {text}")
    return [f"F{i + 1}: {line}" for i, line in enumerate(out)]


def prompt(ticker: str, fact_lines: Sequence[str]) -> Tuple[str, str]:
    system = (
        "You set out the bull case and the bear case for a stock, for someone who is not a finance professional. "
        "Use only the numbered facts. Each point is one plain sentence with no jargon and no abbreviations. "
        "Write two to four points for each side where the facts support them, and fewer where they do not: "
        "a side the facts barely support should have one point or none, never an invented one. "
        "Every point must name the fact ids it rests on, such as F2. Do not predict a price, and do not tell "
        "anyone to buy or sell. Finish with one sentence on what new information would change the balance. "
        "Answer in JSON only."
    )
    user = (
        f"Stock: {ticker}\nFacts:\n" + "\n".join(fact_lines)
        + '\n\nReturn {"bull": [{"point": "...", "cited": ["F1"]}], "bear": [{"point": "...", "cited": ["F2"]}], '
        '"would_change_it": "..."}.'
    )
    return system, user


def _tidy(text: str) -> str:
    """The sentence without fact ids, and without the brackets and spacing they leave behind."""
    text = _FACT.sub("", text)
    text = re.sub(r"[(\[]\s*[,;]*\s*[)\]]", "", text)          # "(F1)" -> "()" -> ""
    text = re.sub(r"\s+([.,;:])", r"\1", text)
    return re.sub(r"\s{2,}", " ", text).strip(" ,;")


def _ids(point: Mapping[str, Any]) -> List[int]:
    found: List[int] = []
    for c in list(point.get("cited") or []) + [str(point.get("point") or "")]:
        found += [int(x) for x in _FACT.findall(str(c))]
    return found


def _side(points: Iterable[Mapping[str, Any]], fact_lines: Sequence[str]) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    seen: set = set()
    for p in points or []:
        text = _tidy(str(p.get("point") or ""))
        ids = sorted(set(_ids(p)))
        if not text or text.lower() in seen or _ADVICE.search(text):
            continue
        if not ids or any(not 1 <= i <= len(fact_lines) for i in ids):
            continue
        seen.add(text.lower())
        kept.append({
            "point": text,
            "sources": [{"id": f"F{i}", "text": fact_lines[i - 1].split(": ", 1)[1]} for i in ids],
        })
    return kept[:MAX_POINTS]


def validate(view: Mapping[str, Any], fact_lines: Sequence[str]) -> Optional[Dict[str, Any]]:
    """The answer reduced to what may be shown, or None when it cannot stand.

    A side with no grounded point means the model was not given a case for it; showing
    one half would read as a verdict, so nothing is shown and the caller says why.
    """
    bull = _side(view.get("bull") or [], fact_lines)
    bear = _side(view.get("bear") or [], fact_lines)
    if not bull or not bear:
        return None
    change = _tidy(str(view.get("would_change_it") or ""))
    if _ADVICE.search(change):
        change = ""
    return {"bull": bull, "bear": bear, "would_change_it": change}


__all__ = ["CACHE_TTL_SEC", "DISCLAIMER", "FAILED_TTL_SEC", "MAX_POINTS", "MIN_FACTS", "PENDING_TTL_SEC",
           "RATE_PER_HOUR", "ThesisPoint", "ThesisView", "facts", "prompt", "validate"]
