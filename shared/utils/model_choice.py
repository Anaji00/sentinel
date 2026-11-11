"""
shared/utils/model_choice.py

Which model scores a horizon tonight: last night's, unless another beat it clearly.

The pick was the highest net Sharpe on the night. Over 26 stored runs the monthly pick flipped on 8
(31%), five times by under 0.10, between models that a shifted test calendar separates by about 0.4.
The scores that rebuild book B1 come from that pick, so a flip is churn in the book for no reason.
Replaying those runs, a margin of 0.10 halves the flips (MODEL_TRIALS.md, 6 Nov 2026). Pure.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

SWITCH_MARGIN = 0.10      # net Sharpe a challenger must beat the incumbent by; 0 restores the plain maximum


def _key(r: Any) -> Tuple[float, float]:
    return (r.net_sharpe if r.net_sharpe is not None else -9.0, r.ic_t if r.ic_t is not None else -9.0)


def choose(reports: Sequence[Any], incumbent: Optional[str] = None, margin: float = SWITCH_MARGIN) -> Tuple[Optional[Any], Optional[Any]]:
    """(the model to use, the challenger it was held over or None).

    The pool is unchanged: validated models if any, else every model that ran. An incumbent outside
    the pool (it errored, ran on too few dates, or a rival has now passed the gate) is replaced, so the
    margin never keeps a model that no longer qualifies."""
    validated = [r for r in reports if r.validated]
    pool = validated or [r for r in reports if r.dates]
    if not pool:
        return None, None
    best = max(pool, key=_key)
    if not incumbent or margin <= 0:
        return best, None
    held = next((r for r in pool if r.model == incumbent), None)
    if held is None or held is best:
        return best, None
    if _key(best)[0] - _key(held)[0] >= margin - 1e-9:      # 0.7 - 0.6 is 0.09999999999999998, and that is a margin's worth
        return best, None
    return held, best


def incumbents_from_report(report: Any) -> Dict[str, str]:
    """Each horizon's chosen model in a stored run's report; empty when it cannot be read."""
    try:
        data: Any = json.loads(report) if isinstance(report, (str, bytes)) else report
        horizons = (data or {}).get("horizons") or {}
        return {str(h): str(v["chosen"]) for h, v in horizons.items() if isinstance(v, Mapping) and v.get("chosen")}
    except (TypeError, ValueError, AttributeError):
        return {}


__all__ = ["SWITCH_MARGIN", "choose", "incumbents_from_report"]
