"""Commodity futures, shaped for a screen.

`commodity_prices` holds daily continuous futures (oil, metals, grains, softs, livestock, lumber, gas) from
Yahoo, back to 2015 for most. This turns the rows into categories of series with the latest price, the change
over a day, a week, a month and a year, a short history, and the stocks each bears on (shared/utils/commodity_links.py).
A change is given only when the stored history reaches back that far, and a series that has stopped updating is flagged.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence

from shared.utils.commodity_links import FUTURES, LINKS

CATEGORIES = (
    ("energy", "Energy", ("oil", "refined", "natgas", "eu_gas")),
    ("metals", "Metals", ("gold", "silver", "copper", "aluminum", "pgm", "uranium", "lithium")),
    ("farm", "Farm and food", ("grain", "coffee", "cocoa", "sugar", "cattle", "hogs")),
    ("other", "Other", ("lumber", "bitcoin")),
)
# Calendar days back for each change: a week, a month, a year.
WINDOWS = (("day", None), ("week", 7), ("month", 30), ("year", 365))
SPARK_DAYS = 120
STALE_AFTER_DAYS = 7


def _change(pts: Sequence[Mapping[str, Any]], back_days: Optional[int]) -> Optional[float]:
    """Percent change from the last stored price at or before `back_days` ago; None when history does not reach."""
    last = pts[-1]
    if back_days is None:
        if len(pts) < 2 or not pts[-2]["value"]:
            return None
        base = pts[-2]
    else:
        cutoff = last["day"] - timedelta(days=back_days)
        earlier = [p for p in pts if p["day"] <= cutoff]
        if not earlier or not earlier[-1]["value"]:
            return None
        base = earlier[-1]
    return round((last["value"] / base["value"] - 1) * 100, 2)


def _bears_on(group: str) -> Dict[str, List[str]]:
    linked: Mapping[str, int] = LINKS.get(group, {})
    return {
        "gain_when_up": sorted(t for t, s in linked.items() if s > 0),
        "pay_when_up": sorted(t for t, s in linked.items() if s < 0),
    }


def build_commodity_board(rows: Sequence[Mapping[str, Any]], today: Optional[date] = None) -> Dict[str, Any]:
    by_series: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for r in rows:
        if r.get("value") is not None and r.get("day") is not None and r["series"] in FUTURES:
            by_series[str(r["series"])].append(r)

    newest = max((pts[-1]["day"] for pts in (sorted(v, key=lambda p: p["day"]) for v in by_series.values())), default=None)
    items_by_group: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for key, raw in by_series.items():
        pts = sorted(raw, key=lambda p: p["day"])
        meta = FUTURES[key]
        last = pts[-1]
        spark_from = last["day"] - timedelta(days=SPARK_DAYS)
        item: Dict[str, Any] = {
            "key": key,
            "name": meta.name,
            "group": meta.group,
            "latest": round(float(last["value"]), 4),
            "as_of": last["day"],
            "history_days": (last["day"] - pts[0]["day"]).days,
            "stale": bool(newest and (newest - last["day"]).days > STALE_AFTER_DAYS),
            "history": [{"day": p["day"], "value": round(float(p["value"]), 4)} for p in pts if p["day"] >= spark_from],
            **_bears_on(meta.group),
        }
        for name, back in WINDOWS:
            item[f"change_{name}"] = _change(pts, back)
        items_by_group[meta.group].append(item)

    categories: List[Dict[str, Any]] = []
    for cat_key, title, groups in CATEGORIES:
        items = [i for g in groups for i in sorted(items_by_group.get(g, []), key=lambda i: i["name"])]
        if items:
            categories.append({"key": cat_key, "title": title, "items": items})

    as_of = newest
    return {
        "as_of": as_of,
        "stale": bool(as_of and today and (today - as_of).days > 4),
        "categories": categories,
        "note": "Continuous front-month futures prices from Yahoo, once a day. A change is shown only where the stored history reaches back that far.",
    }
