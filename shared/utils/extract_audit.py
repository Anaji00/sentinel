"""
shared/utils/extract_audit.py

How often a second model, reading the same evidence, agrees with each
extractor. Forecasts are graded on prices; extractions had no measure at all.
A disagreement rate bounds the error from above (either side may be wrong) and
moves when an extractor drifts. It sees what each extraction kept -- the
headline, or the quoted sentence a verdict rests on -- so it measures the
verdict, not what an extractor missed. Pure.
"""
from __future__ import annotations

from collections import Counter
from typing import Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

GUIDANCE_DIRECTIONS = ("raised", "lowered", "maintained", "initiated", "none")


class Verdict(BaseModel):
    i: int = Field(ge=0)
    label: str


class Verdicts(BaseModel):
    verdicts: List[Verdict] = Field(default_factory=list)


SYSTEM_13D = ("For each numbered sentence from Item 4 of a Schedule 13D, answer whether it states an intent to "
              "influence the company (activist) or only an investment purpose with the usual reservation of rights "
              "(investment). Answer in JSON only: {\"verdicts\": [{\"i\": 0, \"label\": \"activist\"|\"investment\"}]}.")
SYSTEM_GUIDANCE = ("For each numbered sentence from a company's earnings release, say what it does to the company's own "
                   "forward guidance: raised, lowered, maintained, initiated, or none. Answer in JSON only: "
                   "{\"verdicts\": [{\"i\": 0, \"label\": \"raised\"}]}.")


def numbered(texts: Sequence[str]) -> str:
    return "\n".join(f"{i}. {t.strip()[:400]}" for i, t in enumerate(texts))


def agreement(pairs: Sequence[Tuple[str, str]]) -> Dict[str, Optional[float]]:
    """n, share agreeing, and Cohen's kappa (agreement beyond chance), over
    (extractor label, auditor label) pairs."""
    n = len(pairs)
    if not n:
        return {"n": 0, "agree": None, "kappa": None}
    agree = sum(a == b for a, b in pairs) / n
    ca, cb = Counter(a for a, _ in pairs), Counter(b for _, b in pairs)
    chance = sum(ca[k] * cb.get(k, 0) for k in ca) / (n * n)
    kappa = (agree - chance) / (1 - chance) if chance < 1 else None
    return {"n": n, "agree": round(agree, 3), "kappa": round(kappa, 3) if kappa is not None else None}


def labels(answer: Optional[Verdicts], n: int) -> Dict[int, str]:
    """{index: label} for indexes the batch had; a repeat keeps the first."""
    out: Dict[int, str] = {}
    for v in (answer.verdicts if answer else []):
        if 0 <= v.i < n and v.i not in out:
            out[v.i] = str(v.label).strip().lower()
    return out


__all__ = ["GUIDANCE_DIRECTIONS", "SYSTEM_13D", "SYSTEM_GUIDANCE", "Verdict", "Verdicts", "agreement", "labels",
           "numbered"]
