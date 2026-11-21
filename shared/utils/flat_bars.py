"""Flat or zero-volume daily bars among the names the model ranks or the book holds (layer audit 18 Nov, finding 14). A read-only measure: no bar is changed or dropped.

A flat carry-forward bar (close equal to the previous close and zero volume, or zero volume alone) is no observation; features computed over it understate volatility and
overstate staleness. Pure; the caller supplies each ticker's recent bars, oldest first, as (close, volume) pairs."""
from __future__ import annotations

from typing import Dict, Iterable, Mapping, Sequence, Tuple

Bars = Sequence[Tuple[float, float]]


def flat_share(bars: Bars) -> float:
    """Share of bars after the first that carry no information: zero volume, or the same close as the bar before with no volume above zero."""
    if len(bars) < 2:
        return 0.0
    flat = sum(1 for i in range(1, len(bars)) if not bars[i][1] or (bars[i][0] == bars[i - 1][0] and bars[i][1] <= 0))
    return flat / (len(bars) - 1)


def flat_report(series: Mapping[str, Bars], names: Iterable[str], threshold: float = 0.25) -> Dict[str, object]:
    """For `names`: how many have bars, how many have a flat share of at least `threshold`, the overall flat share, and the worst ten. Names without bars are counted `no_bars`."""
    rows = []
    no_bars = 0
    for n in names:
        b = series.get(n)
        if not b or len(b) < 2:
            no_bars += 1
            continue
        rows.append((n, flat_share(b), len(b) - 1))
    total = sum(r[2] for r in rows)
    flat_total = sum(r[1] * r[2] for r in rows)
    return {"names": len(rows), "no_bars": no_bars, "over_threshold": sum(1 for r in rows if r[1] >= threshold),
            "flat_share": round(flat_total / total, 4) if total else 0.0, "worst": [(n, round(s, 3)) for n, s, _ in sorted(rows, key=lambda r: -r[1])[:10]]}


__all__ = ["flat_report", "flat_share"]
