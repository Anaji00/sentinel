"""
shared/utils/analyst.py

The analyst agent's arithmetic, kept pure (services/agents/analyst_agent.py
does the I/O). What the forecasting research says works, applied to one stock
alert at a time:

  facts      everything the model may use, numbered (F1, F2, ...): the stock
             model's rank, its recent moves, its recent headlines, and the
             most similar past events from the event memory
  samples    several answers at a non-zero temperature, averaged -- ensembles
             of model answers approach crowd accuracy (Halawi et al. 2024;
             Schoenegger et al., "Wisdom of the Silicon Crowd")
  prior      the stock model's own rank, combined with the samples as an equal
             partner: a weighted average of a model forecaster and a crowd beat
             both (Halawi et al.)
  verify     a claim must cite facts that exist; one citing nothing it was
             given is dropped rather than published

The claim is the probability the stock beats the S&P 500 over the next five
sessions, graded on prices like every other agent's.
"""
from __future__ import annotations

import math
import re
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

HORIZON_HOURS = 7 * 24             # five sessions, as the grader converts hours
PRIOR_SPREAD = 0.20                # a top-percentile rank alone moves the prior to 0.6
MIN_PROB, MAX_PROB = 0.05, 0.95
_FACT = re.compile(r"\bF(\d{1,2})\b")


class AnalystView(BaseModel):
    """One sample of the analyst's answer (the schema the model must fill)."""

    # Reasoning first: the local grammar and a hosted model both write fields in
    # this order, so the number comes after the argument, not before it. One
    # probability with one meaning: "direction" plus "chance the direction is
    # right" let {"down", 0.3} mean either 30% sure of down or a 30% chance of
    # beating, and p_up read it one way only.
    rationale: str = Field(default="", max_length=600, description="One or two sentences citing fact ids")
    cited: List[str] = Field(default_factory=list, description="Fact ids used, e.g. ['F1', 'F4']")
    p_beat: float = Field(ge=0.0, le=1.0, description="Chance the stock beats the S&P 500 over the next five sessions")


def prior_from_percentile(percentile: Optional[float]) -> float:
    """The stock model's rank as a chance of beating the market: 0.5 at the
    median, 0.5 +/- PRIOR_SPREAD at the extremes. No rank: 0.5."""
    if percentile is None or not math.isfinite(percentile):
        return 0.5
    p = min(1.0, max(0.0, float(percentile)))
    return 0.5 + PRIOR_SPREAD * (p - 0.5) * 2


def p_up(view: Mapping[str, object]) -> float:
    """One sample as a probability the stock beats the market. Reads p_beat;
    the older {direction, probability} shape is still understood."""
    if view.get("p_beat") is not None:
        try:
            return min(1.0, max(0.0, float(view.get("p_beat"))))
        except (TypeError, ValueError):
            return 0.5
    try:
        prob = min(1.0, max(0.0, float(view.get("probability"))))
    except (TypeError, ValueError):
        return 0.5
    d = str(view.get("direction") or "").lower()
    if d == "up":
        return prob
    if d == "down":
        return 1.0 - prob
    return 0.5


def combine(samples: Sequence[Mapping[str, object]], prior: float) -> Optional[Tuple[str, float, float]]:
    """(direction, conviction, p_up) from the samples' mean and the prior,
    averaged with equal weight; None without a sample."""
    if not samples:
        return None
    mean = sum(p_up(s) for s in samples) / len(samples)
    p = min(MAX_PROB, max(MIN_PROB, (mean + prior) / 2))
    direction = "up" if p >= 0.5 else "down"
    return direction, round(p if direction == "up" else 1 - p, 4), round(p, 4)


def verified(view: Mapping[str, object], n_facts: int) -> bool:
    """The sample cites at least one fact it was given and none it was not."""
    ids: List[int] = []
    for c in list(view.get("cited") or []) + [str(view.get("rationale") or "")]:
        ids += [int(x) for x in _FACT.findall(str(c))]
    return bool(ids) and all(1 <= i <= n_facts for i in ids)


def facts(ticker: str, percentile: Optional[float], horizon: str, moves: Mapping[str, Optional[float]],
          headlines: Iterable[Tuple[str, str, Optional[float]]], similar: Iterable[Tuple[str, str, str]],
          alert: str, context: Iterable[str] = ()) -> List[str]:
    """The numbered facts, in a fixed order; `context` (market-wide lines such
    as an elevated geopolitical risk reading) last."""
    out = [f"The alert: {alert}"]
    if percentile is not None:
        out.append(f"The stock model ranks {ticker} at percentile {percentile * 100:.0f} of 100 among about 1,000 "
                   f"liquid stocks for the {horizon} horizon (higher means expected to beat the market).")
    for k, v in moves.items():
        if v is not None:
            out.append(f"{ticker} {k}: {v * 100:+.1f}%.")
    for when, text, tone in headlines:
        out.append(f"Headline {when}: {text}" + (f" (tone {tone:+.2f})" if tone is not None else ""))
    for when, name, text in similar:
        out.append(f"Similar past event {when} ({name}): {text}")
    out += [c for c in context if c]
    return [f"F{i + 1}: {f}" for i, f in enumerate(out)]


def prompt(ticker: str, fact_lines: Sequence[str]) -> Tuple[str, str]:
    system = ("You are an equity analyst. Use only the numbered facts. Estimate the chance that "
              f"{ticker} beats the S&P 500 over the next five trading days. A separate stock model's view is "
              "combined with your answer afterwards: judge only what these facts say. Most single news "
              "items do not move a stock against the market, and half of all stocks beat it in any week: unless the "
              "facts give a clear reason, answer between 0.45 and 0.55. Write the rationale first, citing the fact "
              "ids you rely on (for example F2), then the number. Answer in JSON only.")
    user = "Facts:\n" + "\n".join(fact_lines) + (
        '\n\nReturn {"rationale": "one or two sentences citing fact ids", "cited": ["F1", ...], '
        '"p_beat": 0-1}.')
    return system, user


class BatchView(BaseModel):
    """One stock's answer in a batched prompt (AnalystView's fields, the stock
    named first); its fact ids are that stock's own."""
    ticker: str = Field(description="The stock this answer is for, exactly as its section names it")
    rationale: str = Field(default="", max_length=600, description="One or two sentences citing fact ids")
    cited: List[str] = Field(default_factory=list, description="Fact ids used, e.g. ['F1', 'F4']")
    p_beat: float = Field(ge=0.0, le=1.0, description="Chance the stock beats the S&P 500 over the next five sessions")


class AnalystBatch(BaseModel):
    views: List[BatchView] = Field(default_factory=list)


def batch_prompt(sections: Sequence[Tuple[str, Sequence[str]]]) -> Tuple[str, str]:
    """Several stocks in one call, each judged on its own facts only. Several
    items per call holds accuracy on capable models (batch prompting studies,
    10-25 items); used for the hosted shadow, not the local model."""
    system = ("You are an equity analyst. For EACH stock below, using only that stock's numbered facts, estimate the "
              "chance it beats the S&P 500 over the next five trading days. Fact ids belong to their own stock; a "
              "finding for one stock says nothing about another. A separate stock model's view is combined with your "
              "answers afterwards: judge only these facts. Most single news items do not move a stock against the "
              "market, and half of all stocks beat it in any week: unless the facts give a clear reason, answer "
              "between 0.45 and 0.55. Answer in JSON only.")
    parts = [f"STOCK {t}\nFacts:\n" + "\n".join(lines) for t, lines in sections]
    user = ("\n\n".join(parts) + '\n\nReturn {"views": [one per stock: {"ticker": "...", "rationale": "one or two '
            'sentences citing that stock\'s fact ids", "cited": ["F1", ...], "p_beat": 0-1}]}.')
    return system, user


__all__ = ["AnalystBatch", "AnalystView", "BatchView", "HORIZON_HOURS", "PRIOR_SPREAD", "batch_prompt", "combine",
           "facts", "p_up", "prior_from_percentile", "prompt", "verified"]
