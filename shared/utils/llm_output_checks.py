"""Deterministic checks on a generated scenario that need no inference: does the narrative's stated confidence agree with the stored field, are the hypothesis
probabilities a round template, and does the scenario call itself an artifact. They COUNT and describe; they never reject, rewrite or gate a scenario (research, 13 Nov
2026, of 63 scenarios: 22% state an overall confidence that differs from the field, 22% call themselves artifact or noise, round triples repeat: 60/30/10 x8, 65/25/10 x7).
Pure; no I/O.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

_CONFIDENCE = re.compile(r"overall\s+confidence[^0-9%]{0,16}(\d{1,3})\s*%", re.I)
_ARTIFACT = re.compile(r"\b(data artifact|artifact|spurious|noise|statistical fluke|coincidence|no real|likely an error)\b", re.I)
_CONFIDENCE_SENTENCE = re.compile(r"(Overall confidence )\d{1,3}\s*%")
MISMATCH_POINTS = 5


def stated_confidence(text: Any) -> Optional[int]:
    """The 'Overall confidence N%' a narrative states, or None."""
    m = _CONFIDENCE.search(str(text or ""))
    if not m:
        return None
    v = int(m.group(1))
    return v if 0 <= v <= 100 else None


def with_confidence(text: Any, confidence: int) -> str:
    """The narrative with its 'Overall confidence N%' sentence set to `confidence` (0-100); text without the sentence is returned unchanged."""
    value = max(0, min(100, int(confidence)))
    return _CONFIDENCE_SENTENCE.sub(lambda m: f"{m.group(1)}{value}%", str(text or ""), count=1)


def confidence_mismatch(text: Any, field: Any) -> Optional[Tuple[int, int]]:
    """(stated, field) when the narrative's stated confidence differs from the stored field by MISMATCH_POINTS or more; None when they agree or either is missing."""
    stated = stated_confidence(text)
    try:
        value = int(field)
    except (TypeError, ValueError):
        return None
    if stated is None or abs(stated - value) < MISMATCH_POINTS:
        return None
    return stated, value


def round_triple(probabilities: Sequence[Any]) -> Optional[str]:
    """'60/30/10' when there are exactly three hypotheses whose percent probabilities are all multiples of five (a template-looking split), else None.
    Probabilities in 0-1 are read as percent."""
    vals: List[float] = []
    for p in probabilities:
        try:
            x = float(p)
        except (TypeError, ValueError):
            return None
        vals.append(x * 100.0 if 0.0 < x <= 1.0 and not float(x).is_integer() else x)
    if len(vals) != 3 or any(abs(v - round(v)) > 1e-6 or int(round(v)) % 5 for v in vals):
        return None
    return "/".join(str(int(round(v))) for v in sorted(vals, reverse=True))


def calls_itself_artifact(text: Any) -> bool:
    return bool(_ARTIFACT.search(str(text or "")))


def check_scenario(narrative: Any, field_confidence: Any, probabilities: Sequence[Any]) -> Dict[str, Any]:
    """The flags for one scenario: {"confidence_mismatch": (stated, field)|None, "round_triple": "60/30/10"|None, "artifact": bool}."""
    return {"confidence_mismatch": confidence_mismatch(narrative, field_confidence), "round_triple": round_triple(probabilities),
            "artifact": calls_itself_artifact(narrative)}


__all__ = ["MISMATCH_POINTS", "calls_itself_artifact", "check_scenario", "confidence_mismatch", "round_triple", "stated_confidence"]
