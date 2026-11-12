"""A numbers-first evidence packet for a stock that moved against its peers, and a rule-based regime label. No model calls, no tokens.

The four labels (phase 4 of the upgrade brief; decided 12 Nov: they annotate alerts and change no order):

  UNEXPLAINED_DISLOCATION      no catalyst found with the evidence measured: a candidate for mean reversion (fade the move).
  CONFIRMED_CATALYST_DRIFT     a catalyst points the same way as the move: a candidate for drift (go with it).
  FUNDAMENTAL_IMPAIRMENT_TRAP  a drop that an adverse filing or a bad surprise explains: not a buy.
  CROWDED_NO_EDGE              conflicting evidence, a known thesis-killer, a residual under the bar, or too little measured: abstain.

An absent input is "not measured", never "none found": a no-catalyst call needs all three catalyst inputs measured, otherwise the packet abstains
and says which are missing. Pure; no I/O.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

UNEXPLAINED_DISLOCATION = "UNEXPLAINED_DISLOCATION"
CONFIRMED_CATALYST_DRIFT = "CONFIRMED_CATALYST_DRIFT"
FUNDAMENTAL_IMPAIRMENT_TRAP = "FUNDAMENTAL_IMPAIRMENT_TRAP"
CROWDED_NO_EDGE = "CROWDED_NO_EDGE"
LABELS = (UNEXPLAINED_DISLOCATION, CONFIRMED_CATALYST_DRIFT, FUNDAMENTAL_IMPAIRMENT_TRAP, CROWDED_NO_EDGE)

Z_BAR = 2.0                       # residual z below this is not a dislocation
SURPRISE_BAR = 0.05               # an earnings reaction or surprise of this size is a catalyst
ADVERSE_8K_ITEMS = frozenset({"1.03", "2.04", "2.06", "3.01", "4.01", "4.02"})   # bankruptcy, accelerated obligations, impairment, delisting, auditor, non-reliance
CATALYST_8K_ITEMS = frozenset({"1.01", "2.01", "8.01"})                          # material agreement, acquisition, other events: a catalyst of unknown sign
INSIDER_BAR = 0.5
OPTIONS_BAR = 0.5
CROWDED_DAYS_TO_COVER = 8.0
EARNINGS_IV_PERCENTILE = 95.0
EARNINGS_WITHIN_DAYS = 2


@dataclass
class Packet:
    """Everything measured about the setup. None means not measured."""
    residual_z: Optional[float] = None            # signed: positive = the stock outran its peers
    adverse_8k_items: Optional[List[str]] = None  # 8-K items seen in the window; [] = measured, none
    catalyst_8k_items: Optional[List[str]] = None
    earnings_surprise: Optional[float] = None     # signed reaction or surprise as a fraction; 0.0 = measured, none
    news_articles: Optional[int] = None
    insider_net: Optional[float] = None           # -1 to 1: net insider selling to net buying
    options_tilt: Optional[float] = None          # -1 to 1: bearish to bullish flow
    days_to_cover: Optional[float] = None
    iv_percentile: Optional[float] = None
    earnings_within_days: Optional[float] = None


@dataclass
class Verdict:
    label: str
    action: str
    reasons: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)
    basis: str = "measured"

    def as_dict(self) -> Dict[str, object]:
        return {"label": self.label, "action": self.action, "reasons": self.reasons, "missing": self.missing, "basis": self.basis}


def classify(p: Packet) -> Verdict:
    if p.residual_z is None or abs(p.residual_z) < Z_BAR:
        return Verdict(CROWDED_NO_EDGE, "abstain", [f"the residual is under the {Z_BAR:g} sigma bar" if p.residual_z is not None else "no residual measured"],
                       [] if p.residual_z is not None else ["residual_z"])
    d = 1 if p.residual_z > 0 else -1
    adverse = set(p.adverse_8k_items or [])
    reasons: List[str] = []

    # a drop that an adverse filing or a bad surprise explains
    bad_surprise = p.earnings_surprise is not None and p.earnings_surprise <= -SURPRISE_BAR
    if d < 0 and (adverse & ADVERSE_8K_ITEMS or bad_surprise):
        if adverse & ADVERSE_8K_ITEMS:
            reasons.append("adverse 8-K item " + ", ".join(sorted(adverse & ADVERSE_8K_ITEMS)))
        if bad_surprise:
            reasons.append(f"earnings reaction of {p.earnings_surprise:+.1%}")
        return Verdict(FUNDAMENTAL_IMPAIRMENT_TRAP, "veto_buy", reasons)

    # a catalyst that points the same way as the move
    surprise_aligned = p.earnings_surprise is not None and abs(p.earnings_surprise) >= SURPRISE_BAR and (p.earnings_surprise > 0) == (d > 0)
    if surprise_aligned:
        reasons.append(f"earnings reaction of {p.earnings_surprise:+.1%} in the same direction")
    if d > 0 and (p.catalyst_8k_items or []):
        reasons.append("8-K item " + ", ".join(sorted(set(p.catalyst_8k_items or []))) + " in the window")
    if reasons:
        return Verdict(CONFIRMED_CATALYST_DRIFT, "go_long" if d > 0 else "go_short", reasons)

    # known thesis-killers and conflicts: no edge
    conflicts: List[str] = []
    if p.insider_net is not None and abs(p.insider_net) >= INSIDER_BAR and (p.insider_net > 0) != (d < 0):
        conflicts.append("insiders are trading against a fade" if d > 0 else "insiders are selling into the drop")
    if p.options_tilt is not None and abs(p.options_tilt) >= OPTIONS_BAR and (p.options_tilt > 0) == (d > 0):
        conflicts.append("options flow agrees with the move")
    if p.days_to_cover is not None and p.days_to_cover >= CROWDED_DAYS_TO_COVER:
        conflicts.append(f"crowded short, {p.days_to_cover:.1f} days to cover")
    if (p.iv_percentile is not None and p.iv_percentile >= EARNINGS_IV_PERCENTILE and p.earnings_within_days is not None
            and p.earnings_within_days <= EARNINGS_WITHIN_DAYS):
        conflicts.append("earnings within two days with implied volatility in its top 5%")
    if conflicts:
        return Verdict(CROWDED_NO_EDGE, "abstain", conflicts)

    # no catalyst: only claimable when all three catalyst inputs were measured
    missing = [n for n, v in (("adverse_8k_items", p.adverse_8k_items), ("earnings_surprise", p.earnings_surprise), ("news_articles", p.news_articles)) if v is None]
    if missing:
        return Verdict(CROWDED_NO_EDGE, "abstain", ["catalyst evidence not measured, so 'no catalyst' cannot be claimed"], missing, basis="insufficient_evidence")
    if p.news_articles and p.news_articles >= 3:
        return Verdict(CROWDED_NO_EDGE, "abstain", [f"{p.news_articles} articles in the window and no filing or surprise to read them against"])
    return Verdict(UNEXPLAINED_DISLOCATION, "fade_buy" if d < 0 else "fade_sell",
                   ["no adverse filing, surprise or catalyst found", f"residual of {p.residual_z:+.1f} sigma against peers"])


__all__ = ["CROWDED_NO_EDGE", "CONFIRMED_CATALYST_DRIFT", "FUNDAMENTAL_IMPAIRMENT_TRAP", "LABELS", "Packet", "UNEXPLAINED_DISLOCATION", "Verdict", "classify"]
