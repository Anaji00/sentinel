"""Flags for agent calls that contradict each other (layer audit 18 Nov, finding 12). Flag only: nothing is removed, re-weighted or gated, and no consensus vote changes.

Of 2,473 ticker-days with a directional correlation-agent call in 30 days, 221 had two variants and all 221 disagreed; another 184 had one variant call both up and
down. Consensus counts each such call as a vote, so contradictory calls cancel or double-count. Pure; the caller supplies the calls."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Tuple

Call = Mapping[str, Any]      # needs id, agent_name, ticker, day (date) and direction ('up'/'down'; other values are ignored)


def _sign(direction: object) -> int:
    d = str(direction or "").lower()
    return 1 if d in ("up", "long", "bullish") else -1 if d in ("down", "short", "bearish") else 0


def family_of(agent_name: object) -> str:
    """'stock_correlation_agent.baseline' -> 'stock_correlation_agent': the variants of one agent share a family."""
    return str(agent_name or "").split(".")[0]


def flag_contradictions(calls: Iterable[Call]) -> Dict[Any, List[str]]:
    """{call id: flags} for every call that has one. 'self_contradiction': the same agent variant called both directions on that ticker-day.
    'sibling_contradiction': another variant of the same agent family called the opposite direction on that ticker-day."""
    by_key: Dict[Tuple[str, date], List[Tuple[Any, str, int]]] = defaultdict(list)
    for c in calls:
        s = _sign(c.get("direction"))
        if s:
            by_key[(str(c.get("ticker") or "").upper(), c.get("day"))].append((c.get("id"), str(c.get("agent_name") or ""), s))
    flags: Dict[Any, List[str]] = defaultdict(list)
    for group in by_key.values():
        for cid, agent, s in group:
            same = [o for o in group if o[1] == agent and o[2] != s]
            sib = [o for o in group if o[1] != agent and family_of(o[1]) == family_of(agent) and o[2] != s]
            if same:
                flags[cid].append("self_contradiction")
            if sib:
                flags[cid].append("sibling_contradiction")
    return dict(flags)


def summarize(flags: Mapping[Any, List[str]], n_calls: int) -> Dict[str, int]:
    """Counts for a report or counter: calls, flagged calls, and each flag."""
    out = {"calls": int(n_calls), "flagged": len(flags), "self_contradiction": 0, "sibling_contradiction": 0}
    for fl in flags.values():
        for f in fl:
            out[f] = out.get(f, 0) + 1
    return out


__all__ = ["family_of", "flag_contradictions", "summarize"]
