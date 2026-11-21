"""P6 Phase 0 (owner-approved 21 Nov 2026): what the generator said about a scenario at the moment it was written, before the tracker's evidence updates.

`scenarios.hypotheses` and `confidence_overall` are rewritten by the tracker on every pass, so the generator's own probabilities were never graded. This keeps the
first values, once, in `scenarios.initial_state`. Design and registered grading: reports/P6 scenario probabilities design - 21 Nov.md. Pure: no I/O.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

INITIAL_STATE_VERSION = 1


def _probability(hypothesis: Any) -> Optional[int]:
    raw = hypothesis.get("probability") if isinstance(hypothesis, dict) else getattr(hypothesis, "probability", None)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _label(hypothesis: Any) -> str:
    raw = hypothesis.get("label") if isinstance(hypothesis, dict) else getattr(hypothesis, "label", "")
    return str(raw or "")[:120]


def build_initial_state(hypotheses: Any, confidence_overall: Any, correlation_id: Any = None) -> Dict[str, Any]:
    """The generator's probabilities in hypothesis order, their labels, the overall confidence and the correlation the scenario came from (the join to its rule family
    and tier at grading time). Missing or malformed values are kept as null, never invented."""
    items: List[Any] = list(hypotheses or [])
    try:
        confidence = int(confidence_overall) if confidence_overall is not None else None
    except (TypeError, ValueError):
        confidence = None
    return {
        "version": INITIAL_STATE_VERSION,
        "probabilities": [_probability(h) for h in items],
        "labels": [_label(h) for h in items],
        "confidence_overall": confidence,
        "correlation_id": str(correlation_id) if correlation_id else None,
    }
