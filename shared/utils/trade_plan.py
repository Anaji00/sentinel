"""Trade plans computed in code; the model writes only the thesis.

The advisory asked a 0.6B model, eight to sixteen minutes behind a queue, to
choose BUY/SELL/HOLD and a conviction score. Everything else on the play was
already overwritten by code -- entry from the current price, stop from ATR and
the measured stop multiplier, target from the stop distance and an R:R tier --
so the two numbers left to the model were exactly the two that decide whether
a trade exists and how large its target is. Nothing made them reproducible,
and the same inputs could produce a BUY one run and a SELL the next.

Here both come from measurements:

  direction   votes from the triggering event's own direction (the signal
              engine's family mapping), the moving-average alignment and the
              EMA-12/26 trend. A play needs a net of at least two votes; less
              is HOLD and no play.
  conviction  the share of votes that agree, lifted only when the trigger's
              family has passed the reaction study. An unproven play is
              labelled so and never reaches the top R:R tier.

`thesis_warranted` decides whether the model is asked for prose at all.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set

# Net votes a direction needs before a play is published.
MIN_NET_VOTES = 2
# Conviction tiers map onto the advisory's R:R tiers (<0.6 -> 1.5, <0.8 -> 2.0,
# else 3.0). An unproven play is capped below the top tier.
UNPROVEN_CONVICTION_CAP = 0.79
PROVEN_CONVICTION_FLOOR = 0.80
# An event this strong earns a thesis even without a studied family.
THESIS_MIN_SCORE = 0.80


def _num(v: Any) -> Optional[float]:
    """A finite float, or None for anything that is not one."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    elif isinstance(v, str) and v.strip().replace(".", "", 1).replace("-", "", 1).isdigit():
        f = float(v)
    else:
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


@dataclass
class DirectionCase:
    direction: int                       # +1 long, -1 short, 0 none
    votes: List[str] = field(default_factory=list)
    net: int = 0
    total: int = 0


def model_vote(model: Optional[Dict[str, Any]]) -> int:
    """+1 / -1 from the cross-sectional model's top / bottom quintile, only when
    the model has cleared its walk-forward gate; an unvalidated model abstains."""
    if not model or not model.get("validated"):
        return 0
    q = model.get("quintile")
    return 1 if q == 5 else -1 if q == 1 else 0


# Out-of-sample bars a model must clear to vote without full validation: every
# gate but the deflated Sharpe. Measured 7 October: the moving-average and EMA
# votes behind 92 calls had a 46% record; the weekly model, unvalidated only on
# the deflated test, held IC 0.05 at t 5.5 in all twelve folds.
SUPPORT_MIN_IC_T = 2.0
SUPPORT_MIN_NET_SHARPE = 0.5
SUPPORT_MIN_POSITIVE_FOLDS = 0.55
MODEL_VOTE_WEIGHT = 2


def model_support(model: Optional[Dict[str, Any]]) -> Optional[str]:
    """"validated", "supported" (every gate but the deflated Sharpe), or None."""
    if not model:
        return None
    if model.get("validated"):
        return "validated"
    try:
        if (float(model.get("ic_t") or 0) >= SUPPORT_MIN_IC_T
                and float(model.get("net_sharpe") or 0) >= SUPPORT_MIN_NET_SHARPE
                and float(model.get("positive_fold_share") or 0) >= SUPPORT_MIN_POSITIVE_FOLDS):
            return "supported"
    except (TypeError, ValueError):
        return None
    return None


def _model_side(model: Optional[Dict[str, Any]]) -> int:
    """+1 / -1 for a validated or supported model's top / bottom quintile."""
    if not model_support(model):
        return 0
    q = model.get("quintile")
    return 1 if q == 5 else -1 if q == 1 else 0


def direction_votes(
    indicators: Dict[str, Any],
    trigger_directions: Iterable[int],
    model: Optional[Dict[str, Any]] = None,
) -> DirectionCase:
    """Each measured view casts +1, -1 or abstains; the reasons travel with it."""
    votes: List[str] = []
    net = 0
    total = 0
    mv = _model_side(model)
    if mv:
        # Weighted: the one view here with an out-of-sample record.
        votes.append(f"model q{model.get('quintile')} {'+' if mv > 0 else '-'} "
                     f"({model_support(model)}, x{MODEL_VOTE_WEIGHT})")
        net += mv * MODEL_VOTE_WEIGHT
        total += MODEL_VOTE_WEIGHT

    trig = [d for d in trigger_directions if d in (1, -1)]
    if trig:
        t = 1 if sum(trig) > 0 else -1 if sum(trig) < 0 else 0
        if t:
            votes.append(f"trigger {'+' if t > 0 else '-'}")
            net += t
            total += 1

    align = str(indicators.get("ma_alignment") or "").upper()
    if "BULL" in align:
        votes.append("ma_alignment +")
        net += 1
        total += 1
    elif "BEAR" in align:
        votes.append("ma_alignment -")
        net -= 1
        total += 1

    e12, e26 = _num(indicators.get("ema_12")), _num(indicators.get("ema_26"))
    if e12 is not None and e26 is not None and e26 > 0:
        if e12 > e26:
            votes.append("ema_12>ema_26 +")
            net += 1
        elif e12 < e26:
            votes.append("ema_12<ema_26 -")
            net -= 1
        total += 1

    direction = 1 if net >= MIN_NET_VOTES else -1 if net <= -MIN_NET_VOTES else 0
    return DirectionCase(direction=direction, votes=votes, net=net, total=total)


def conviction(case: DirectionCase, proven: bool) -> float:
    """Agreement among the views that voted; a proven trigger family lifts it."""
    if case.direction == 0 or case.total == 0:
        return 0.0
    agree = abs(case.net) / case.total          # 2/3 .. 1.0 once a play exists
    c = 0.45 + 0.3 * agree                      # 0.65 .. 0.75
    if proven:
        return round(max(c, PROVEN_CONVICTION_FLOOR), 2)
    return round(min(c, UNPROVEN_CONVICTION_CAP), 2)


def deterministic_play(
    ticker: str,
    indicators: Dict[str, Any],
    trigger_directions: Iterable[int],
    trigger_families: Set[str],
    proven_families: Set[str],
    kelly_cap_pct: float,
    model: Optional[Dict[str, Any]] = None,
    require_evidence: bool = False,
) -> Optional[Dict[str, Any]]:
    """The play's decision fields, or None for HOLD. Prices are set by the caller.

    `model` is the ticker's entry from the nightly cross-sectional ranking
    ({quintile, validated, ic_t, net_sharpe, positive_fold_share}); a validated
    model agreeing with the play's side is proven evidence, as a proven trigger
    family is. A supported model agreeing is evidence, not proof.

    `require_evidence`: no play from technicals and unstudied triggers alone --
    a proven family or a validated or supported model must agree."""
    case = direction_votes(indicators, trigger_directions, model)
    if case.direction == 0:
        return None
    proven = bool(trigger_families & proven_families) or model_vote(model) == case.direction
    supported = _model_side(model) == case.direction
    if require_evidence and not (proven or supported):
        return None
    conv = conviction(case, proven)
    action = "BUY" if case.direction > 0 else "SELL"
    return {
        "ticker": ticker,
        "action": action,
        "trade_type": "Long/Buy" if action == "BUY" else "Short/Sell",
        "conviction_score": conv,
        "kelly_allocation_pct": round(max(0.0, kelly_cap_pct), 2),
        # Placeholders the caller replaces from current price and ATR.
        "entry_level": 0.0,
        "target_price": 0.0,
        "stop_loss": 0.0,
        "risk_reward_ratio": 0.0,
        "quantitative_rationale": (
            f"Rule: {', '.join(case.votes)} (net {case.net:+d} of {case.total}); "
            f"trigger families {sorted(trigger_families) or 'none'}; "
            + ("proven (reaction study or validated model)." if proven
               else "model-supported: an out-of-sample record, not yet validated." if supported
               else "UNPROVEN -- no trigger family or validated model supports it.")
        ),
        "evidence_status": "proven" if proven else "model_supported" if supported else "unproven",
    }


def thesis_warranted(
    ticker: str,
    anomaly_score: float,
    watched: Optional[Set[str]],
    trigger_families: Set[str],
) -> bool:
    """Only a real trigger earns a model call: a watched name, an event the
    signal engine studies, or a score high enough to stand on its own."""
    if watched and ticker.upper() in watched:
        return True
    if trigger_families:
        return True
    return anomaly_score >= THESIS_MIN_SCORE


def market_regime_label(rates_regime: Optional[str]) -> str:
    return str(rates_regime or "unknown")


def hedging_note(is_stress: bool) -> str:
    """What the sizing rule actually did, not advice it did not compute."""
    if is_stress:
        return "Curve flat or inverted: position size halved and capped at 5%."
    return "No regime haircut applied; size is the half-Kelly cap for this ticker."


__all__ = [
    "DirectionCase", "MIN_NET_VOTES", "THESIS_MIN_SCORE", "conviction", "deterministic_play",
    "direction_votes", "hedging_note", "market_regime_label", "thesis_warranted",
]
