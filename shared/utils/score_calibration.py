"""
shared/utils/score_calibration.py

Anomaly scores tied to what followed. Pure; the grader fits a model per
producer weekly, enrichment applies it to every event.

On 45 days to 16 Oct 2026 the enrichment score said almost nothing about the
next session: across 60,152 block trades the next move was 1.09x a normal day
at the lowest score band and 1.07x at the highest, and the radar's top band
was followed by the smallest moves. Only insider trades ranked correctly.

For each producer (event type and source) the score is fitted, monotonically,
to the probability that the stock's next session moves at least BIG_MOVE times
its usual absolute move (pool-adjacent-violators over score deciles). The
result is a probability a reader can trust and a lift over the producer's base
rate. A producer whose top decile lifts that probability by less than
MIN_LIFT is uninformative, and enrichment caps its scores below the "high"
band so it stops filling the anomaly feed and the rules keyed on it.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

ANOMALY_CALIBRATION_KEY = "sentinel:calibration:anomaly"   # the fitted models, written by the grader
BIG_MOVE = 2.0              # a session moving at least twice the stock's usual amount
MIN_SAMPLES = 300
BINS = 10
MIN_LIFT = 1.15             # the top decile must raise the big-move rate by 15%
UNINFORMATIVE_CAP = 0.5     # where an uninformative producer's scores stop


def pav(values: Sequence[float], weights: Sequence[float]) -> List[float]:
    """Pool-adjacent-violators: the non-decreasing fit closest to `values`."""
    blocks: List[List[float]] = []          # [mean, weight, count]
    for v, w in zip(values, weights):
        blocks.append([v, w, 1])
        while len(blocks) > 1 and blocks[-2][0] > blocks[-1][0]:
            m2, w2, c2 = blocks.pop()
            m1, w1, c1 = blocks.pop()
            blocks.append([(m1 * w1 + m2 * w2) / (w1 + w2), w1 + w2, c1 + c2])
    out: List[float] = []
    for m, _w, c in blocks:
        out.extend([m] * int(c))
    return out


def fit(samples: Sequence[Tuple[float, float]], big: float = BIG_MOVE) -> Optional[Dict[str, Any]]:
    """[(score, next-session move ratio)] -> a step model, or None when too few."""
    pts = sorted((float(s), 1.0 if r >= big else 0.0) for s, r in samples if s is not None and r is not None)
    n = len(pts)
    if n < MIN_SAMPLES:
        return None
    base = sum(y for _s, y in pts) / n
    size = max(1, n // BINS)
    edges: List[float] = []
    rates: List[float] = []
    counts: List[float] = []
    for i in range(0, n, size):
        chunk = pts[i:i + size]
        if len(chunk) < size // 2 and rates:          # fold a short tail into the last bin
            last = len(rates) - 1
            rates[last] = (rates[last] * counts[last] + sum(y for _s, y in chunk)) / (counts[last] + len(chunk))
            counts[last] += len(chunk)
            edges[last] = chunk[-1][0]
            continue
        edges.append(chunk[-1][0])
        rates.append(sum(y for _s, y in chunk) / len(chunk))
        counts.append(float(len(chunk)))
    fitted = pav(rates, counts)
    lift_top = fitted[-1] / base if base > 0 else None
    return {"n": n, "base_rate": round(base, 4), "edges": [round(e, 6) for e in edges],
            "p": [round(p, 4) for p in fitted], "lift_top": round(lift_top, 3) if lift_top else None,
            "informative": bool(lift_top and lift_top >= MIN_LIFT)}


def calibrate(model: Dict[str, Any], score: float) -> Tuple[float, Optional[float]]:
    """(probability of a big next-session move, lift over the base rate) for a score."""
    edges, ps = model["edges"], model["p"]
    p = ps[-1]
    for e, v in zip(edges, ps):
        if score <= e:
            p = v
            break
    base = model.get("base_rate") or 0.0
    return p, (round(p / base, 3) if base > 0 else None)


def producer_key(event_type: str, source: str) -> str:
    return f"{event_type}/{source}"


def capped(score: float, model: Optional[Dict[str, Any]]) -> float:
    """The score, held below the high band for an uninformative producer."""
    if model is None or model.get("informative", True):
        return score
    return min(score, UNINFORMATIVE_CAP)


__all__ = ["ANOMALY_CALIBRATION_KEY", "BIG_MOVE", "MIN_LIFT", "UNINFORMATIVE_CAP", "calibrate", "capped", "fit", "pav",
           "producer_key"]
