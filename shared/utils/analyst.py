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
from typing import Iterable, List, Literal, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

HORIZON_HOURS = 7 * 24             # five sessions, as the grader converts hours
PRIOR_SPREAD = 0.20                # a top-percentile rank alone moves the prior to 0.6
MIN_PROB, MAX_PROB = 0.05, 0.95
_FACT = re.compile(r"\bF(\d{1,2})\b")


class AnalystView(BaseModel):
    """One sample of the analyst's answer (the schema the model must fill)."""

    direction: Literal["up", "down", "flat"] = Field(description="Beats the S&P 500 (up), trails it (down), or neither")
    probability: float = Field(ge=0.0, le=1.0, description="Chance the direction is right over five sessions")
    cited: List[str] = Field(default_factory=list, description="Fact ids used, e.g. ['F1', 'F4']")
    rationale: str = Field(default="", max_length=600)


def prior_from_percentile(percentile: Optional[float]) -> float:
    """The stock model's rank as a chance of beating the market: 0.5 at the
    median, 0.5 +/- PRIOR_SPREAD at the extremes. No rank: 0.5."""
    if percentile is None or not math.isfinite(percentile):
        return 0.5
    p = min(1.0, max(0.0, float(percentile)))
    return 0.5 + PRIOR_SPREAD * (p - 0.5) * 2


def p_up(view: Mapping[str, object]) -> float:
    """One sample as a probability the stock beats the market."""
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
    system = ("You are an equity analyst. Use only the numbered facts. Estimate whether "
              f"{ticker} will beat the S&P 500 over the next five trading days. Cite the fact ids you rely on "
              "(for example F2). Most single news items do not move a stock against the market: unless the facts "
              "give a clear reason, keep the probability near 0.5. Answer in JSON only.")
    user = "Facts:\n" + "\n".join(fact_lines) + (
        '\n\nReturn {"direction": "up"|"down"|"flat", "probability": 0-1, "cited": ["F1", ...], '
        '"rationale": "one or two sentences"}.')
    return system, user


__all__ = ["AnalystView", "HORIZON_HOURS", "PRIOR_SPREAD", "combine", "facts", "p_up", "prior_from_percentile",
           "prompt", "verified"]
