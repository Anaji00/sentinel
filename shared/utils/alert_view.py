"""The divergence parts of a stored alert, as the dashboard receives them.

A divergence alert carries, in `metrics_summary`, its `situation` (stage, sessions, peak and last gap, first and last session,
news) and its seven-section `brief` (shared/utils/situation.py, shared/utils/intel_brief.py). The `/correlations` route sends
those two, and the four measurements that say how to read a divergence (horizon, breadth, beta, relative volume), and nothing else from `metrics_summary`: the column holds other rules' diagnostics too, and an allowlist fails closed.
Pure: a value in, a small dict out.
"""
from __future__ import annotations

import json
from typing import Any, Dict

STAGES = ("DEVELOPING", "CONFIRMED", "ESCALATING", "RESOLVED")
HORIZONS = ("session", "5d")
# Numeric measurements carried through as they are; a value that is not a plain number is dropped, never coerced.
MEASURES = ("breadth", "beta", "relative_volume")


def divergence_view(metrics_summary: Any) -> Dict[str, Any]:
    """`{"situation": ..., "brief": ...}` for a divergence alert, `{}` for any other row or an unreadable value."""
    ms = metrics_summary
    if isinstance(ms, (str, bytes)):
        try:
            ms = json.loads(ms)
        except (ValueError, TypeError):
            return {}
    if not isinstance(ms, dict):
        return {}
    out: Dict[str, Any] = {}
    situation = ms.get("situation")
    if isinstance(situation, dict) and situation.get("stage") in STAGES:
        out["situation"] = situation
    brief = ms.get("brief")
    if isinstance(brief, dict) and brief:
        out["brief"] = brief
    if ms.get("horizon") in HORIZONS:
        out["horizon"] = ms["horizon"]
    for key in MEASURES:
        v = ms.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            out[key] = v
    return out
