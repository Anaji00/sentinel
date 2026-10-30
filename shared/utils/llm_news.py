"""
shared/utils/llm_news.py

A language model's read of single-stock headlines, as a factor-model input.

Lopez-Lira & Tang (2023, revised 2025) found large models' answers to "is this
headline good or bad for the stock?" predict next-day returns and the drift
after, where BERT-sized models did not. Measured here on 29 Oct 2026, FinBERT
tone over 361,126 stock-days had a daily rank IC of 0.0005 (t 0.2): the same
null. So the question is put to a large hosted model instead, in their form.

Two constraints from the research shape it:
  forward only  a model can recall what prices did before its training cutoff
                (parametric look-ahead bias), so headlines are scored as they
                arrive and every score carries the model and its cutoff; only
                headlines after the cutoff can test it.
  monthly       daily news strategies lose most of their return to costs, and
                LLM-conditioned momentum did better rebalanced monthly. The
                feature is a trailing 30-day mean per stock, read by the monthly
                model, not traded on its own.

Pure: headlines in, prompt out; answers in, scores out; scores in, feature out.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

from pydantic import BaseModel, Field

FEATURE = "llm_news_30"
WINDOW_DAYS = 30
MIN_HEADLINES = 2            # a stock's mean over fewer is too noisy to publish
BATCH = 20
MAX_HEADLINE_CHARS = 240

SYSTEM = ("You are a financial expert with stock recommendation experience. For each numbered headline, answer "
          "whether it is good news (1), bad news (-1) or unclear (0) for the named company's stock price in the "
          "short term. Judge the headline alone. Answer in JSON only: "
          '{"scores": [{"i": 0, "s": 1}, ...]} with one entry per headline.')


class Scored(BaseModel):
    i: int = Field(ge=0, le=BATCH - 1)
    s: int = Field(ge=-1, le=1)


class Scores(BaseModel):
    scores: List[Scored] = Field(default_factory=list, max_length=BATCH)


def prompt(items: Sequence[Tuple[str, str]]) -> str:
    """The user prompt for up to BATCH (ticker, headline) pairs."""
    lines = [f"{k}. {t}: {h.strip()[:MAX_HEADLINE_CHARS]}" for k, (t, h) in enumerate(items[:BATCH])]
    return "Headlines:\n" + "\n".join(lines)


def parse(answer: Scores, n: int) -> Dict[int, int]:
    """{index: score} for indexes the batch had; a repeat keeps the first."""
    out: Dict[int, int] = {}
    for s in answer.scores:
        if 0 <= s.i < n and s.i not in out:
            out[s.i] = s.s
    return out


def feature_rows(scores: Iterable[Tuple[str, datetime, int]], day: date) -> List[Tuple[str, date, str, float]]:
    """(ticker, day, FEATURE, mean score) over headlines in the WINDOW_DAYS up to
    and including `day`, for stocks with at least MIN_HEADLINES of them."""
    lo = day - timedelta(days=WINDOW_DAYS)
    acc: Dict[str, List[int]] = {}
    for t, at, s in scores:
        d = at.date() if isinstance(at, datetime) else at
        if lo < d <= day:
            acc.setdefault(str(t).upper(), []).append(int(s))
    return [(t, day, FEATURE, round(sum(v) / len(v), 4)) for t, v in sorted(acc.items()) if len(v) >= MIN_HEADLINES]


def cutoff_of(model: str, cutoffs: Mapping[str, str]) -> date | None:
    """The training cutoff configured for a model, if any (ISO date)."""
    raw = cutoffs.get(model)
    try:
        return date.fromisoformat(raw) if raw else None
    except ValueError:
        return None


def parse_cutoffs(spec: str) -> Dict[str, str]:
    """'provider/model=YYYY-MM-DD;...' -> {model: date string}."""
    out: Dict[str, str] = {}
    for part in (spec or "").split(";"):
        m, _, d = part.partition("=")
        if m.strip() and d.strip():
            out[m.strip()] = d.strip()
    return out


__all__ = ["BATCH", "FEATURE", "MIN_HEADLINES", "SYSTEM", "Scored", "Scores", "WINDOW_DAYS", "cutoff_of",
           "feature_rows", "parse", "parse_cutoffs", "prompt"]
