"""
shared/utils/treasury.py

The US Treasury's daily par yield curve (home.treasury.gov, XML feed), parsed
into one daily series per maturity. Pure; the gateway fetches.

One source for every maturity keeps a chart honest: plotting the 2-year's two
months of daily closes beside an hour of the 10- and 30-year's intraday ticks
made two different time scales look like one.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

TREASURY_XML = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
                "?data=daily_treasury_yield_curve&field_tdr_date_value={year}")

# Our symbols -> the feed's field names.
MATURITY_FIELDS: Dict[str, str] = {"US02Y": "BC_2YEAR", "US10Y": "BC_10YEAR", "US30Y": "BC_30YEAR"}

_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}


def _num(text: Optional[str]) -> Optional[float]:
    try:
        v = float(text) if text not in (None, "") else None
    except ValueError:
        return None
    return v if v is not None and v == v else None


def parse_par_yields(xml_text: str) -> Dict[str, List[Dict[str, object]]]:
    """{symbol: [{"date": "YYYY-MM-DD", "yield": 4.82}, ...]} oldest first.

    A day missing one maturity keeps the others; malformed XML yields empty
    series rather than an exception, so one bad response cannot break a page."""
    out: Dict[str, List[Dict[str, object]]] = {s: [] for s in MATURITY_FIELDS}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out
    for entry in root.findall(".//atom:entry", _NS):
        props = entry.find("atom:content/m:properties", _NS)
        if props is None:
            continue
        d = props.find("d:NEW_DATE", _NS)
        day = (d.text or "")[:10] if d is not None else ""
        if len(day) != 10:
            continue
        for sym, field in MATURITY_FIELDS.items():
            el = props.find(f"d:{field}", _NS)
            v = _num(el.text if el is not None else None)
            if v is not None:
                out[sym].append({"date": day, "yield": round(v, 3)})
    for sym in out:
        out[sym].sort(key=lambda p: str(p["date"]))
    return out


def merge_years(*parts: Dict[str, List[Dict[str, object]]], days: int) -> Dict[str, List[Dict[str, object]]]:
    """Several years' feeds into one series per maturity, de-duplicated by
    date, the last `days` trading days kept."""
    out: Dict[str, List[Dict[str, object]]] = {}
    for sym in MATURITY_FIELDS:
        by_day: Dict[str, Dict[str, object]] = {}
        for part in parts:
            for p in part.get(sym, []):
                by_day[str(p["date"])] = p
        out[sym] = [by_day[k] for k in sorted(by_day)][-days:]
    return out


__all__ = ["MATURITY_FIELDS", "TREASURY_XML", "merge_years", "parse_par_yields"]
