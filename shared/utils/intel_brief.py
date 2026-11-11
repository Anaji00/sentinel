"""
shared/utils/intel_brief.py

The Intelligence Brief of a divergence: seven fixed sections, each filled from a measurement or marked "not measured".

An empty section is stated as empty ("not measured") with the reason, and never filled with a plausible sentence: the
platform has no executive-link graph, no signed options flow per stock and no supplier map, so those sections say so
until a source exists. Nothing here is invented and no model is called.

Sections: what_happened, catalyst, peers, evidence, positioning, watch, limits.
Pure: measurements in, a dict of strings and lists out.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from shared.utils import divergence as dv
from shared.utils.divergence import Divergence
from shared.utils import peer_residual as pr
from shared.utils import situation as sit

NOT_MEASURED = "not measured"
SECTIONS = ("what_happened", "catalyst", "peers", "evidence", "positioning", "watch", "limits")


def _evidence(d: Divergence, refname: str) -> str:
    """What the alert rests on, with the bar of the rule that actually fired. The daily rule D2 (3 sigma over the last 60
    sessions' gaps) has a measured follow-up record; the multi-horizon rule P1 (bar by horizon, beta-adjusted) has none yet."""
    z = abs(d.z)
    if hasattr(d, "horizon"):
        bar = pr.Z_MIN_BY_HORIZON.get(getattr(d, "horizon", "session"), pr.Z_MIN)
        span = "five-session" if d.horizon == "5d" else "session-so-far"
        return (f"The unexplained move is {z:.1f} times its usual size at this horizon (the {span} rule requires {bar:g}), after removing "
                f"what this stock's beta to {refname} implies. What follows such a gap has not been measured for this rule.")
    return (f"The gap is {z:.1f} times the usual variation of this stock against {refname} over the last 60 sessions; the rule requires "
            f"{dv.Z_MIN:g}. After divergences with news the study found no continuation or reversal at 1 or 5 sessions (t 1.6, 0.2).")


def _peers_line(peer_returns: Sequence[Tuple[str, float]]) -> List[str]:
    return [f"{t} {r:+.1%}" for t, r in peer_returns[:8]]


def build(d: Divergence, article: Optional[Mapping[str, Any]], state: Optional[Mapping[str, Any]],
          peer_returns: Sequence[Tuple[str, float]] = (), positioning: Optional[str] = None) -> Dict[str, Any]:
    headline = str((article or {}).get("headline") or "").strip()
    n_art = int((article or {}).get("n") or 0)
    stage = dict(state) if state else None
    refname = "its industry group" if d.reference == "industry" else "its closest peers"
    five = getattr(d, "horizon", "session") == "5d"
    since = "the last five sessions" if five else "the previous close"
    gap_word = f"{d.gap:+.2%} unexplained after its beta of {getattr(d, 'beta', 1.0):.2f}" if hasattr(d, "beta") else f"a gap of {d.gap:+.2%}"
    return {
        "what_happened": f"{d.ticker} {d.ret:+.2%} since {since} while {refname} {d.ref_ret:+.2%}: {gap_word}.",
        "catalyst": (f"{n_art} article(s) name {d.ticker} alone this session; latest: {headline[:160]}" if d.news and headline
                     else ("News names the stock alone this session." if d.news
                           else f"No news names {d.ticker} alone this session: {NOT_MEASURED} as news-driven (the study finds such gaps close within a week)")),
        "peers": {"reference": d.reference, "members": _peers_line(peer_returns) or list(d.members[:8]),
                  "returns_known": bool(peer_returns)},
        "evidence": _evidence(d, refname),
        "positioning": positioning or f"{NOT_MEASURED}: no signed options flow, short interest or executive link is joined to this alert",
        "watch": [f"Escalating if the gap widens to {abs(d.z) + sit.ESCALATE_STEP:.1f} sigma or beyond",
                  f"Resolved if the gap falls inside {sit.RESOLVE_Z:.0f} sigma or {d.ticker} moves with {refname}",
                  "Confirmed if it is flagged again after the next close"],
        "limits": ["A price gap is a measurement, not a forecast; direction after the alert is not established",
                   f"Stage: {sit.label(stage)}" if stage else "Stage: new"],
    }


def render(brief: Mapping[str, Any]) -> str:
    """The brief as plain text, one block per section, in the fixed order."""
    parts: List[str] = []
    for k in SECTIONS:
        v = brief.get(k)
        if isinstance(v, dict):
            v = f"{v.get('reference')}: " + ", ".join(v.get("members") or [])
        elif isinstance(v, list):
            v = "; ".join(str(x) for x in v)
        parts.append(f"{k.replace('_', ' ').capitalize()}: {v}")
    return "\n".join(parts)


__all__ = ["NOT_MEASURED", "SECTIONS", "build", "render"]
