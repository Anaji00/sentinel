"""
shared/utils/vessel_feeds.py

Two ways to see vessels that do not depend on shore AIS receivers, parsed here
so the collectors stay thin and the parsing is testable without credentials.

  Global Fishing Watch (GFW)  satellite-AIS derived events, worldwide: AIS
                              gaps (a transponder switched off) and encounters
                              (two vessels meeting at sea). GFW's coverage is
                              strongest for fishing vessels and carriers, and
                              its events arrive with a lag of a few days.

  NOAA VIIRS boat detection   ships seen by their lights at night from the
                              Suomi-NPP / NOAA-20 satellites -- no transponder
                              needed, so it sees a dark ship that is lit. Each
                              detection carries a quality flag; gas flares and
                              fixed lights are separated out by it.

Everything here is pure: dictionaries and text in, dictionaries out.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from shared.utils.chokepoints import SAR_AIS_CHECK_AREAS, SAR_TERMINALS
from shared.utils.conflict_zones import ZONES, zone_for_point

# Redis keys, written by collector-sar's feed tasks and read by the gateway.
VIIRS_DETECTIONS_KEY = "sentinel:viirs:detections"
VIIRS_ZONE_HISTORY_KEY = "sentinel:viirs:zone_counts:{zone_id}"
VIIRS_DETECTIONS_TTL_SEC = 3 * 86400
VIIRS_MAX_STORED = 5000

# EOG's VBD quality flags that describe a lit vessel: strong, weak and blurry
# detections, and weak-and-blurry. 4 is a gas flare, 5 a glint, 8 a recurring
# light (platforms, fixed structures); those are not vessels.
VBD_VESSEL_QF = frozenset({1, 2, 3, 10})

GFW_SOURCE = "gfw"


# -- where we look ------------------------------------------------------------

def _areas() -> List[Tuple[float, float, float, float]]:
    """(min_lat, min_lon, max_lat, max_lon) of every zone and radar area."""
    boxes = [z.bbox for z in ZONES]
    for b in {**SAR_TERMINALS, **SAR_AIS_CHECK_AREAS}.values():
        boxes.append((b["south"], b["west"], b["north"], b["east"]))
    return boxes


_WATCHED = _areas()


def in_watched_area(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    return any(a <= lat <= c and b <= lon <= d for a, b, c, d in _WATCHED)


def watched_bounds() -> Tuple[float, float, float, float]:
    """The box enclosing every watched area, for a coarse first filter."""
    return (
        min(b[0] for b in _WATCHED), min(b[1] for b in _WATCHED),
        max(b[2] for b in _WATCHED), max(b[3] for b in _WATCHED),
    )


# -- Global Fishing Watch -----------------------------------------------------

def _float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _vessel(v: Optional[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    v = v or {}
    return {
        "mmsi": str(v.get("ssvid") or "") or None,
        "name": (v.get("name") or None),
        "flag": (v.get("flag") or None),
        "type": (v.get("type") or None),
        "gfw_id": (v.get("id") or None),
    }


def gfw_event(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One GFW event as the fields of a NormalizedEvent, or None.

    Gaps become `vessel_dark` and encounters `vessel_sts`; other types and
    events outside every watched area are dropped. The caller adds region,
    exposure tags and validates the model.
    """
    etype = str(entry.get("type") or "").lower()
    pos = entry.get("position") or {}
    lat, lon = _float(pos.get("lat")), _float(pos.get("lon"))
    if not in_watched_area(lat, lon):
        return None
    vessel = _vessel(entry.get("vessel"))
    if not vessel["mmsi"] and not vessel["gfw_id"]:
        return None
    start = entry.get("start") or entry.get("end")
    occurred = start or datetime.now(timezone.utc).isoformat()
    name = vessel["name"] or (f"MMSI:{vessel['mmsi']}" if vessel["mmsi"] else vessel["gfw_id"])

    if etype == "gap":
        gap = entry.get("gap") or {}
        hours = _float(gap.get("durationHours"))
        intentional = bool(gap.get("intentionalDisabling"))
        # A short, unintentional gap is coverage, not behaviour.
        if not intentional and (hours is None or hours < 12):
            return None
        score = 0.7 if intentional else 0.55
        return {
            "type": "vessel_dark",
            "occurred_at": occurred,
            "latitude": lat, "longitude": lon,
            "entity": {"id": vessel["mmsi"] or vessel["gfw_id"], "name": name, "flag": vessel["flag"]},
            "vessel_type": vessel["type"],
            "gap_hours": hours,
            "headline": (
                f"GFW: {name} AIS off for {hours:.0f}h" if hours is not None else f"GFW: {name} AIS off"
            ) + (" (likely intentional)" if intentional else ""),
            "tags": ["dark_vessel", "ais_gap", "satellite_ais"] + (["intentional_disabling"] if intentional else []),
            "anomaly_score": score,
            "gfw_event_id": entry.get("id"),
        }

    if etype == "encounter":
        other = _vessel((entry.get("encounter") or {}).get("vessel"))
        other_name = other["name"] or other["mmsi"] or "unidentified vessel"
        hours = _float((entry.get("encounter") or {}).get("durationHours"))
        return {
            "type": "vessel_sts",
            "occurred_at": occurred,
            "latitude": lat, "longitude": lon,
            "entity": {"id": vessel["mmsi"] or vessel["gfw_id"], "name": name, "flag": vessel["flag"]},
            "vessel_type": vessel["type"],
            "partner": other,
            "headline": f"GFW encounter: {name} alongside {other_name}"
                        + (f" for {hours:.1f}h" if hours is not None else ""),
            "tags": ["vessel_sts", "encounter", "satellite_ais"],
            "anomaly_score": 0.65,
            "gfw_event_id": entry.get("id"),
        }
    return None


# -- VIIRS boat detection -----------------------------------------------------

def _col(row: Dict[str, str], *names: str) -> Optional[str]:
    lower = {k.lower(): v for k, v in row.items() if k}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def vbd_detections(lines: Iterable[str]) -> Iterator[Dict[str, Any]]:
    """Lit-vessel detections inside a watched area, from VBD CSV lines.

    Columns are matched by name, case-insensitively, because EOG has renamed
    them between versions. Rows outside every watched area, or whose quality
    flag is not a vessel, are skipped.
    """
    reader = csv.DictReader(lines)
    min_lat, min_lon, max_lat, max_lon = watched_bounds()
    for row in reader:
        lat = _float(_col(row, "Lat_DNB", "lat"))
        lon = _float(_col(row, "Lon_DNB", "lon"))
        if lat is None or lon is None:
            continue
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue
        qf = _float(_col(row, "QF_Detect", "qf"))
        if qf is None or int(qf) not in VBD_VESSEL_QF:
            continue
        if not in_watched_area(lat, lon):
            continue
        yield {
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "radiance": _float(_col(row, "Rad_DNB", "rad")),
            "qf": int(qf),
            "observed_at": _col(row, "Date_Mscan", "date"),
            "zone_id": zone_for_point(lat, lon),
        }


def vbd_csv_lines(text: str) -> Iterator[str]:
    """Lines of a decoded CSV body, for `vbd_detections`."""
    return iter(io.StringIO(text))


def zone_counts(detections: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for d in detections:
        if d.get("zone_id"):
            out[d["zone_id"]] = out.get(d["zone_id"], 0) + 1
    return out


__all__ = [
    "GFW_SOURCE", "VBD_VESSEL_QF", "VIIRS_DETECTIONS_KEY", "VIIRS_DETECTIONS_TTL_SEC",
    "VIIRS_MAX_STORED", "VIIRS_ZONE_HISTORY_KEY", "gfw_event", "in_watched_area",
    "vbd_csv_lines", "vbd_detections", "watched_bounds", "zone_counts",
]
