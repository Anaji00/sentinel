"""Shadow measurement of retiring the generic macro fan-out rules (registered 13 Nov 2026; retirement is a separate approval).

The four rules fire one macro headline across every name that matches (US2Y Yield Drop: 2,301 events on 1,084 names at confidence
0.10). This module only decides which correlations belong to them and judges the registered bars; nothing here changes a rule.
"""
from __future__ import annotations

from typing import Optional, Sequence, Tuple

_PREFIXES: Tuple[str, ...] = ("us2y yield drop", "macro energy price headline", "cross-asset yield")


def is_generic_fanout_rule(rule_name: object) -> bool:
    """True for US2Y Yield Drop ..., Macro Energy Price Headline ..., Cross-Asset Yield ... and the US10Y-Nike rule."""
    name = " ".join(str(rule_name or "").lower().split())
    if name.startswith(_PREFIXES):
        return True
    return name.startswith("us10y") and "nike" in name


def mean_or_none(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def fanout_verdicts(tier1_total: int, tier1_retired: int, confirmed_total: int, confirmed_lost: int,
                    ratio_before: Optional[float], ratio_after: Optional[float]) -> list:
    """The three registered bars as (bar, measured value, passed).

    Tier-1 correlations fall by at least 50%; confirmed scenarios lost are under 5%; the mean 5-session move ratio of the
    remaining alerts is not lower than that of all alerts. A bar with nothing to measure fails rather than passes.
    """
    drop = tier1_retired / tier1_total if tier1_total else None
    lost = confirmed_lost / confirmed_total if confirmed_total else None
    return [
        ("tier-1 correlations fall >= 50%", "n/a" if drop is None else f"{drop:.1%} ({tier1_retired} of {tier1_total})",
         drop is not None and drop >= 0.5),
        ("confirmed scenarios lost < 5%", "n/a" if lost is None else f"{lost:.1%} ({confirmed_lost} of {confirmed_total})",
         lost is not None and lost < 0.05),
        ("remaining alerts' 5-session move ratio not lower",
         "n/a" if ratio_before is None or ratio_after is None else f"after {ratio_after:.3f} vs before {ratio_before:.3f}",
         ratio_before is not None and ratio_after is not None and ratio_after >= ratio_before),
    ]
