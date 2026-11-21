"""Record-only measures of the stored B1 book, written nightly beside it and never read by the trader (requested 21 Nov 2026 from the 19 Nov quant trials).

borrow_cost: the book's estimated daily borrow cost at a flat annual fee on short notional (T1 amendment: 2.0% a year, the fee supported by the quoted
borrow file), so the live net edge can be tracked against the trials' accounting. veto_counts: per side and per registered veto rule, the names that rule
WOULD remove from the book (divergence veto as the live one, 8-K veto on both sides as the live one, 8-K veto on longs only as the T5 cell); nothing is vetoed
here. Pure; no I/O; no sizing, cap, kill-switch or execution logic.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from shared.utils import auto_trade as at

BORROW_FEE_ANNUAL = 0.02
TRADING_DAYS = 252
LEG = 0.5            # each leg's share of the account, as the trader's target_weights(leg=0.5)
RULES = ("divergence", "eight_k_both", "eight_k_long_only")


def borrow_cost(longs: Sequence[str], shorts: Sequence[str], fee_annual: float = BORROW_FEE_ANNUAL, leg: float = LEG) -> Dict[str, Any]:
    """Estimated daily borrow cost of the equal-weight book: short gross (share of NAV) x fee / 252, in bps of NAV and in dollars on a $100,000 account."""
    weights = at.target_weights(longs, shorts, leg)
    short_gross = -sum(w for w in weights.values() if w < 0)
    daily = short_gross * fee_annual / TRADING_DAYS
    return {"fee_annual": fee_annual, "n_long": sum(1 for w in weights.values() if w > 0), "n_short": sum(1 for w in weights.values() if w < 0),
            "short_gross_nav": round(short_gross, 6), "daily_cost_bps_nav": round(daily * 1e4, 4), "daily_cost_per_100k_usd": round(daily * 1e5, 4)}


def veto_counts(longs: Iterable[str], shorts: Iterable[str], eight_k: Iterable[Mapping[str, Any]], divergences: Iterable[Mapping[str, Any]],
                z_min: float = at.VETO_DIVERGENCE_Z) -> Dict[str, Dict[str, Any]]:
    """{rule: {"long": n, "short": n, "long_names": [...], "short_names": [...]}} for the three registered rules."""
    L, S = {str(t).upper() for t in longs}, {str(t).upper() for t in shorts}
    hit_8k = {str(r.get("ticker") or "").upper() for r in eight_k if set(r.get("items") or []) & at.VETO_8K_ITEMS}
    div_long, div_short = set(), set()
    for r in divergences:
        t = str(r.get("ticker") or "").upper()
        try:
            ret, z = float(r["ret"]), float(r["z"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (math.isfinite(ret) and math.isfinite(z)) or abs(z) < z_min:
            continue
        if t in L and ret < 0:
            div_long.add(t)
        elif t in S and ret > 0:
            div_short.add(t)

    def pack(lo: set, sh: set) -> Dict[str, Any]:
        return {"long": len(lo), "short": len(sh), "long_names": sorted(lo), "short_names": sorted(sh)}
    return {"divergence": pack(div_long, div_short), "eight_k_both": pack(L & hit_8k, S & hit_8k), "eight_k_long_only": pack(L & hit_8k, set())}


__all__: List[str] = ["BORROW_FEE_ANNUAL", "RULES", "borrow_cost", "veto_counts"]
