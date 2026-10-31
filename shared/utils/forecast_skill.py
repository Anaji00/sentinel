"""
shared/utils/forecast_skill.py

An agent's directional record against the forecasts that need no skill.

A hit rate set against 50% rewards an agent's lean in a one-sided week. Over
the week to 30 Oct 2026 the stock correlation agent hit 57.7% on 71 calls,
but only 31% of those stocks went down: "up" every time would have hit 69%.
The macro engine called down 93% of the time and hit 51.7%, exactly the share
that went down. Both looked like something; neither was.

Two standard measures (Wilks, Statistical Methods in the Atmospheric
Sciences), each 0 for no skill and negative for worse than none:

  - Heidke skill score: hits against the hits a forecaster would score by
    chance with the same mix of up and down calls in the same outcomes, so
    neither the agent's lean nor the week's direction can pass as skill.
  - Brier skill score: the stated probability against the base rate
    (climatology) of the same outcomes.

`majority_hit` is the hindsight bar, "always the side that won", reported
beside them. Pure.
"""
from __future__ import annotations

import math
from typing import Dict, Iterable, Optional, Tuple

MIN_N = 20          # resolved calls before a skill is stated at all

Call = Tuple[bool, bool, Optional[float]]     # (called up, went up, stated chance the call is right)


def skill(calls: Iterable[Call]) -> Dict[str, Optional[float]]:
    """n, hit rate, chance hit rate, Heidke skill, majority hit, Brier skill,
    and a verdict: skill / none / worse than chance / too few."""
    rows = list(calls)
    n = len(rows)
    out: Dict[str, Optional[float]] = {"n": n, "hit": None, "chance_hit": None, "heidke": None,
                                       "majority_hit": None, "brier": None, "brier_ref": None, "bss": None,
                                       "z": None, "verdict": "too few"}
    if n == 0:
        return out
    hits = sum(c == a for c, a, _ in rows)
    called_up = sum(c for c, _, _ in rows) / n
    went_up = sum(a for _, a, _ in rows) / n
    chance = called_up * went_up + (1 - called_up) * (1 - went_up)
    out.update(hit=round(hits / n, 4), chance_hit=round(chance, 4), majority_hit=round(max(went_up, 1 - went_up), 4))
    if chance < 1:
        out["heidke"] = round((hits / n - chance) / (1 - chance), 4)
    # The stated probability as a chance the stock went up, against the base rate.
    probs = [(p if c else 1 - p, a) for c, a, p in rows if p is not None and 0 <= p <= 1]
    if probs:
        bs = sum((p - a) ** 2 for p, a in probs) / len(probs)
        base = sum(a for _, a in probs) / len(probs)
        ref = base * (1 - base)
        out.update(brier=round(bs, 4), brier_ref=round(ref, 4), bss=round(1 - bs / ref, 4) if ref > 0 else None)
    # Hits beyond chance, as a z-score under the chance rate.
    sd = math.sqrt(chance * (1 - chance) / n) if 0 < chance < 1 else 0.0
    out["z"] = round((hits / n - chance) / sd, 2) if sd else None
    if n >= MIN_N and out["z"] is not None:
        out["verdict"] = "skill" if out["z"] >= 2 else ("worse than chance" if out["z"] <= -2 else "none")
    return out


__all__ = ["MIN_N", "Call", "skill"]
