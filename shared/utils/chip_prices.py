"""Memory-chip and GPU prices, shaped for a screen.

`commodity_prices` already holds DRAMeXchange spot (DRAM, NAND flash, GDDR) and RunPod GPU rental
rates, one row per series per day (services/collector-tradfi/commodities.py). This turns those rows into
the sections a page draws. It reports only what is stored: a series with a single day has no change, and
the page says how long the record is, because a few weeks of prices say little about a trend.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence

from shared.utils.commodity_links import LINKS

SECTIONS = (
    ("dram", "DRAM spot", "What memory makers sell DRAM chips and modules for today, in US dollars (DRAMeXchange daily average)."),
    ("nand", "NAND flash spot", "What flash memory wafers and chips sell for today, in US dollars (DRAMeXchange daily average)."),
    ("gddr", "GDDR spot", "Graphics memory chip prices, in US dollars (DRAMeXchange daily average)."),
    ("gpu", "GPU rental", "What it costs to rent one GPU for an hour on RunPod: the secure cloud price, and the lowest price on offer."),
)
VARIANT_WORDS = {"secure": "secure cloud", "lowest": "lowest price"}
MAX_HISTORY = 120


def _split(series: str) -> tuple[str, str, Optional[str]]:
    """`gpu:H100 SXM:secure` -> (`gpu`, `H100 SXM`, `secure`); `dram:DDR4 8Gb` -> (`dram`, `DDR4 8Gb`, None)."""
    head, _, rest = series.partition(":")
    if head == "gpu":
        name, _, variant = rest.rpartition(":")
        return head, name or rest, variant or None
    return head, rest, None


def _bears_on(group: str) -> Dict[str, List[str]]:
    """Stocks that gain when this price rises, and stocks that pay for it."""
    linked: Mapping[str, int] = LINKS.get(group, {})
    return {
        "gain_when_up": sorted(t for t, s in linked.items() if s > 0),
        "pay_when_up": sorted(t for t, s in linked.items() if s < 0),
    }


def build_chip_prices(rows: Sequence[Mapping[str, Any]], today: Optional[date] = None) -> Dict[str, Any]:
    """Sections of items, each with its latest price, its change since its first stored day, and a short history."""
    by_series: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for r in rows:
        if r.get("value") is not None and r.get("day") is not None:
            by_series[str(r["series"])].append(r)

    items: Dict[str, List[Dict[str, Any]]] = {key: [] for key, _, _ in SECTIONS}
    for series, pts in by_series.items():
        group, name, variant = _split(series)
        if group not in items:
            continue
        pts.sort(key=lambda p: p["day"])
        first, last = pts[0], pts[-1]
        change = None
        if len(pts) > 1 and first["value"]:
            change = round((last["value"] / first["value"] - 1) * 100, 2)
        items[group].append({
            "name": name,
            "variant": VARIANT_WORDS.get(variant or "", variant),
            "latest": round(float(last["value"]), 3),
            "as_of": last["day"],
            "since": first["day"],
            "change_pct": change,
            "unit": last.get("unit") or "USD",
            "source": last.get("source"),
            "history": [{"day": p["day"], "value": round(float(p["value"]), 3)} for p in pts[-MAX_HISTORY:]],
        })

    sections: List[Dict[str, Any]] = []
    for key, title, note in SECTIONS:
        group_items = sorted(items[key], key=lambda i: (i["name"], i["variant"] or ""))
        if not group_items:
            continue
        sections.append({"key": key, "title": title, "note": note, "items": group_items, **_bears_on(key)})

    days = [i["since"] for s in sections for i in s["items"]]
    latest = [i["as_of"] for s in sections for i in s["items"]]
    as_of = max(latest) if latest else None
    since = min(days) if days else None
    record_days = len({h["day"] for s in sections for i in s["items"] for h in i["history"]})
    stale = bool(as_of and today and (today - as_of).days > 4)
    return {
        "as_of": as_of,
        "since": since,
        "record_days": record_days,
        "stale": stale,
        "sections": sections,
        "note": (
            f"Collected since {since}: {record_days} days of prices, too few to call a trend."
            if since and record_days < 60
            else "Prices from DRAMeXchange and RunPod."
        ),
    }
