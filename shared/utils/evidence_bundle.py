"""The per-ticker evidence bundle: one section per source, each reporting on its own whether it has data, so one slow or empty source never
blanks a stock page. Pure helpers only; the route does the reading.

Section status: "ok" (data present), "missing" (the source answered and there is nothing for this stock), "error" (the source could not be
read or timed out). `data` is None unless the status is "ok".
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

SECTIONS = ("quote", "snapshot", "held_back", "regime", "producer_grade", "positioning", "model")
MAX_GRADED_ALERTS = 5


def section(status: str, data: Any = None, note: Optional[str] = None) -> Dict[str, Any]:
    if status not in ("ok", "missing", "error"):
        raise ValueError(f"unknown section status {status!r}")
    return {"status": status, "data": data if status == "ok" else None, "note": note}


def regime_of(rows: Sequence[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """The newest divergence alert's regime annotation, with when it was raised; None when no alert carries one."""
    for r in rows:
        ms = r.get("metrics_summary")
        if isinstance(ms, str):
            try:
                ms = json.loads(ms)
            except ValueError:
                ms = None
        reg = ms.get("regime") if isinstance(ms, Mapping) else None
        if isinstance(reg, Mapping) and reg.get("label"):
            return {"label": reg.get("label"), "action": reg.get("action"), "reasons": reg.get("reasons") or [],
                    "missing": reg.get("missing") or [], "basis": reg.get("basis"), "alert_at": str(r.get("detected_at")),
                    "alert_rule": r.get("rule_name")}
    return None


def _finite(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def grades_for(rule_names: Sequence[str], grades: Optional[Mapping[str, Any]], family_of: Any) -> List[Dict[str, Any]]:
    """For each distinct recent rule behind this stock's alerts, the grade of that rule (5-session) or of its family: whether the stock then
    moved more than usual. Rules with no graded group are left out rather than shown as neutral."""
    if not grades:
        return []
    by_rule = {str(g.get("group")): g for g in grades.get("correlations_5d") or [] if isinstance(g, Mapping)}
    by_family = {str(g.get("group")): g for g in grades.get("correlation_families_5d") or [] if isinstance(g, Mapping)}
    out: List[Dict[str, Any]] = []
    seen = set()
    for name in rule_names:
        if name in seen:
            continue
        seen.add(name)
        fam = family_of(name)
        g, level = (by_rule.get(name), "rule") if name in by_rule else (by_family.get(fam), "family")
        if g is None:
            continue
        out.append({"rule": name, "family": fam, "graded": level, "verdict": g.get("verdict"), "n": g.get("n"),
                    "mean_ratio": _finite(g.get("mean_ratio")), "t": _finite(g.get("t")), "q": _finite(g.get("q"))})
        if len(out) >= MAX_GRADED_ALERTS:
            break
    return out


def model_row(scores: Optional[Mapping[str, Any]], ticker: str, as_of: Optional[str]) -> Optional[Dict[str, Any]]:
    row = (scores or {}).get(ticker)
    if not isinstance(row, Mapping) or _finite(row.get("percentile")) is None:
        return None
    return {"percentile": round(float(row["percentile"]), 4), "quintile": row.get("quintile"), "as_of": as_of}


__all__ = ["MAX_GRADED_ALERTS", "SECTIONS", "grades_for", "model_row", "regime_of", "section"]
