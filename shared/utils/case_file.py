"""
shared/utils/case_file.py

One structured file of facts per stock, shared by every agent.

Agents each re-queried the database for the same stock, and what they passed
each other was model-written prose (cross-agent context: up to ~800 tokens of
unverified bulletins). A case file is the opposite: every fact carries an id,
a kind, its value, its source and when it was true, so an agent cites ids
instead of restating numbers, and a checker can confirm every citation exists.

Kinds (each optional; a missing source is simply absent, never zero):
  rank      the factor model's monthly percentile
  move      the stock's 1, 5 and 21-session returns
  news      the hosted model's 30-day headline read, and the latest scored headlines
  alert     recent correlation alerts, marked when the family is graded to
            precede bigger moves than the stock's own headline days
  feature   each stored model feature's latest value (customer, co-mention,
            commodity exposure, insider, 8-K, activist 13D)
  customer  the major customers its latest 10-K names

Pure: inputs in, file out.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

CASE_KEY = "sentinel:case:{ticker}"
CASE_INDEX_KEY = "sentinel:case:index"
CASE_TTL_SEC = 2 * 86400
MAX_FACTS = 30


def _pct(x: float) -> str:
    return f"{x * 100:+.1f}%"


def build(ticker: str, *, rank: Optional[Tuple[float, str]] = None,
          closes: Sequence[float] = (), news: Optional[Mapping[str, Any]] = None,
          headlines: Sequence[Tuple[str, str, int]] = (), alerts: Sequence[Tuple[Any, ...]] = (),
          features: Mapping[str, Tuple[float, str, str]] = {}, customers: Sequence[Tuple[str, Optional[float]]] = (),
          now: Optional[datetime] = None) -> Dict[str, Any]:
    """The case file. closes are newest first; headlines (when, text, score);
    alerts (when, family, family graded to precede bigger moves[, direction tendency]); features
    {name: (value, day, description)}; customers (ticker, % of revenue)."""
    now = now or datetime.now(timezone.utc)
    facts: List[Dict[str, Any]] = []

    def add(kind: str, text: str, value: Any, as_of: str, source: str) -> None:
        facts.append({"id": f"F{len(facts) + 1}", "kind": kind, "text": text, "value": value,
                      "as_of": as_of, "source": source})

    if rank:
        add("rank", f"Factor model ranks {ticker} at the {rank[0] * 100:.0f}th percentile (monthly)",
            round(rank[0], 4), rank[1], "factor_model")
    for k, label in ((1, "last session"), (5, "last 5 sessions"), (21, "last 21 sessions")):
        if len(closes) > k and closes[k]:
            add("move", f"{ticker} moved {_pct(closes[0] / closes[k] - 1)} over the {label}",
                round(closes[0] / closes[k] - 1, 5), "latest close", "daily_bars")
    if news and news.get("n"):
        add("news", f"Hosted model read of {news['n']} headlines over 30 days: mean {news['mean']:+.2f} (-1 bad, +1 good)",
            round(float(news["mean"]), 3), str(news.get("as_of", "")), "stock_news_llm")
    for when, text, score in list(headlines)[:3]:
        word = {1: "good", -1: "bad"}.get(int(score), "unclear")
        add("news", f"Headline ({word}): {text[:160]}", int(score), when, "stock_news_llm")
    for when, family, flags, *tendency in list(alerts)[:5]:
        tag = "graded to precede bigger moves" if flags else "not graded to add information"
        # tendency: how this family's alerts were followed (producer_grades.follow_tendencies).
        after = f"; after past alerts the stock {tendency[0]}" if tendency and tendency[0] else ""
        add("alert", f"Alert: {family} ({tag}{after})", bool(flags), when, "correlations")
    for name, (value, day, desc) in sorted(features.items()):
        add("feature", f"{name} = {value:.4g}: {desc[:120]}", round(float(value), 6), day, "stock_features_daily")
    for cust, pct in list(customers)[:3]:
        add("customer", f"Major customer {cust}" + (f" ({pct:.0f}% of revenue)" if pct else ""),
            pct, "latest 10-K", "customer_links")
    return {"ticker": ticker, "built_at": now.isoformat(), "facts": facts[:MAX_FACTS]}


def fact_lines(case: Optional[Mapping[str, Any]]) -> List[str]:
    """The facts as lines an agent can be shown: '[F3] ... (as of ...)'."""
    out = []
    for f in (case or {}).get("facts") or []:
        out.append(f"[{f['id']}] {f['text']} (as of {f['as_of']})" if f.get("as_of") else f"[{f['id']}] {f['text']}")
    return out


def cited_ids_exist(cited: Sequence[str], case: Optional[Mapping[str, Any]]) -> bool:
    """Every id an answer cites is in the case file."""
    ids = {f["id"] for f in (case or {}).get("facts") or []}
    return bool(cited) and all(c in ids for c in cited)


__all__ = ["CASE_INDEX_KEY", "CASE_KEY", "CASE_TTL_SEC", "MAX_FACTS", "build", "cited_ids_exist", "fact_lines"]
