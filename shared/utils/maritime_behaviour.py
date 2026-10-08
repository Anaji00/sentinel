"""Two vessel behaviours the platform scored around and never detected.

`rule_maritime_chokepoint_evasion` triggers on `vessel_dark`, `vessel_sts` and
`vessel_spoof` and correlates on the same three. Two of those three had no
producer, at both ends, so the rule was a dark-gap rule wearing the name of an
evasion rule -- and ship-to-ship transfer and identity spoofing are what evasion
actually consists of.

The groundwork was already here and stopped one step short in both cases:

  * `sts_zone_risk_multiplier` scores a dark gap by its proximity to a known
    transfer zone. That is a prior about *where*, not a detection of *what*: it
    amplifies the score of a gap near a zone and says nothing about whether a
    transfer happened.
  * The Kalman residual filter's own docstring says "a vessel that jumps 50nm
    in 5 minutes has a huge residual" -- and the residual goes into a score,
    not into a claim. A sanity check that rejects an implausible position drops
    it; it does not report that something implausible was transmitted.

Both functions here are pure. Given two position reports, or a pair of vessels,
they answer yes or no and say why. Nothing in this module touches Redis, Kafka
or the clock, which is what makes the thresholds arguable against real numbers
rather than against a mock.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

EARTH_RADIUS_NM = 3440.065

# Above this, the two reports cannot both be true.
#
# The fastest merchant hulls run about 25 knots; fast ro-pax ferries reach 40;
# a naval hydrofoil is the outlier at ~50 and does not transmit commercial AIS.
# 45 is set above every civilian case so that a hit is a statement about the
# *data* rather than about the vessel -- either two transmitters share one MMSI,
# or a position is fabricated.
MAX_PLAUSIBLE_SPEED_KNOTS = 45.0

# Below this the pair is too close in time to measure: AIS position reports
# arrive every 2-10 seconds under way, and dividing a GPS jitter of tens of
# metres by three seconds produces a large implied speed from nothing.
MIN_SEPARATION_SECONDS = 60.0

# A report older than this is not a previous position, it is a different voyage.
MAX_SEPARATION_SECONDS = 6 * 3600.0

# Ship-to-ship transfer geometry.
#
# Two hulls made fast alongside are within a few hundred metres of each other by
# definition; 0.30 nm (556 m) admits the fenders-and-hawsers case plus GPS
# error, and excludes vessels merely sharing an anchorage lane.
STS_MAX_SEPARATION_NM = 0.30

# Both effectively stopped. A transfer takes hours at zero way; 0.8 knots allows
# for drift and for the AIS speed field's 0.1-knot resolution.
STS_MAX_SPEED_KNOTS = 0.8

# And stopped *together*, for long enough to move cargo. Two vessels passing
# slowly in a lane are co-located for minutes; a transfer is measured in hours.
STS_MIN_DWELL_SECONDS = 45 * 60.0


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_NM * 2 * math.asin(math.sqrt(min(1.0, a)))


def implied_speed_knots(
    lat1: float, lon1: float, t1: float, lat2: float, lon2: float, t2: float
) -> Optional[float]:
    """Speed the vessel must have made to be in both places, or None.

    None when the two reports are too close together in time to divide by, or
    far enough apart that they describe different voyages. Both are refusals to
    answer rather than a zero, because a zero here would read as "did not move".
    """
    dt = abs(float(t2) - float(t1))
    if dt < MIN_SEPARATION_SECONDS or dt > MAX_SEPARATION_SECONDS:
        return None
    distance = haversine_nm(lat1, lon1, lat2, lon2)
    return distance / (dt / 3600.0)


def impossible_transit(
    previous: Dict[str, Any], current: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """The two reports cannot both be true, or None.

    Returns the evidence rather than a boolean: the implied speed, the distance
    and the elapsed time, because "spoofed" is a conclusion a reader should be
    able to check. A transponder that reports Singapore and then Rotterdam four
    minutes later has not sailed; either the identity is being worn by two
    transmitters, or one position is fabricated.
    """
    try:
        lat1, lon1 = float(previous["lat"]), float(previous["lon"])
        lat2, lon2 = float(current["lat"]), float(current["lon"])
        t1, t2 = float(previous["ts"]), float(current["ts"])
    except (KeyError, TypeError, ValueError):
        return None

    speed = implied_speed_knots(lat1, lon1, t1, lat2, lon2, t2)
    if speed is None or speed <= MAX_PLAUSIBLE_SPEED_KNOTS:
        return None

    return {
        "implied_speed_knots": round(speed, 1),
        "distance_nm": round(haversine_nm(lat1, lon1, lat2, lon2), 2),
        "elapsed_seconds": round(abs(t2 - t1), 1),
        "from": {"lat": lat1, "lon": lon1},
        "to": {"lat": lat2, "lon": lon2},
        "threshold_knots": MAX_PLAUSIBLE_SPEED_KNOTS,
    }


def implausible_position(lat, lon) -> Optional[Dict[str, Any]]:
    """A vessel reporting from dry land, or None.

    `impossible_transit` above needs two reports and measures the speed between
    them. That catches a transponder that jumps, and passes one that simply sits
    in the wrong place: a first report, or a stationary one, is never compared
    against anything. Measured over seven days, 65 vessel positions classified
    into an airspace region -- TITAN (MMSI 304496000) at 19.13N 35.33E, roughly
    200km inland from the Red Sea coast, and a position north of the Strait of
    Hormuz, on land. Both tripped nothing.

    Sixty-five is a small number and the wrong reason to care. A hull reporting
    an inland position beside Hormuz and beside the Red Sea is the signature of
    AIS manipulation, which is one of the specific things this platform exists
    to notice.

    The test is the region classifier the platform already runs. Its polygons
    resolve to the SMALLEST containing region, so any point in water resolves to
    the water: verified against the coordinates above and against Hormuz, the
    Red Sea, Bab-el-Mandeb, Suez, Singapore and the Taiwan Strait, every one of
    which returns its maritime region. An airspace region wins only where no
    maritime polygon contains the point at all.

    `None` -- open ocean, outside every polygon -- is emphatically not a
    finding. Most of the sea is not in a named region, and flagging that would
    report the Atlantic as fraud.
    """
    try:
        latitude, longitude = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
        return None

    try:
        from shared.utils.regions import classify_region
        region = classify_region(latitude, longitude)
    except Exception:
        return None

    if not region or "airspace" not in str(region).lower():
        return None

    return {
        "reported_region": region,
        "at": {"lat": latitude, "lon": longitude},
        "reason": "vessel_position_inland",
        "basis": "smallest containing region is airspace, so no water contains it",
    }


def sts_pair(
    a: Dict[str, Any], b: Dict[str, Any], *, now: float
) -> Optional[Dict[str, Any]]:
    """Two vessels alongside and stopped together long enough, or None.

    The three conditions are separable on purpose, and all three are needed:

      close      two hulls within 0.30 nm
      stopped    both under 0.8 knots -- a transfer happens at zero way
      dwelling   both stationary for 45 minutes or more

    Dropping the dwell test is what makes a naive co-location detector useless:
    an anchorage holds dozens of vessels within a cable of each other, and a
    traffic lane puts two hulls abeam for ninety seconds every few minutes.
    """
    try:
        lat_a, lon_a, spd_a = float(a["lat"]), float(a["lon"]), float(a["speed"])
        lat_b, lon_b, spd_b = float(b["lat"]), float(b["lon"]), float(b["speed"])
    except (KeyError, TypeError, ValueError):
        return None

    if spd_a > STS_MAX_SPEED_KNOTS or spd_b > STS_MAX_SPEED_KNOTS:
        return None

    separation = haversine_nm(lat_a, lon_a, lat_b, lon_b)
    if separation > STS_MAX_SEPARATION_NM:
        return None

    # How long each has been reporting stationary, as recorded by the caller.
    #
    # Compared against None explicitly: `x or now` treats a stationary_since of
    # 0.0 as absent, and 0.0 is a valid epoch. A vessel that has been stopped
    # since the epoch is a fixture, but the same falsiness bites any caller
    # passing a relative clock.
    since_a = a.get("stationary_since")
    since_b = b.get("stationary_since")
    if since_a is None or since_b is None:
        return None
    dwell = min(float(now) - float(since_a), float(now) - float(since_b))
    if dwell < STS_MIN_DWELL_SECONDS:
        return None

    return {
        "separation_nm": round(separation, 3),
        "speed_knots": [round(spd_a, 2), round(spd_b, 2)],
        "dwell_seconds": round(dwell, 0),
        "dwell_hours": round(dwell / 3600.0, 2),
        "midpoint": {
            "lat": round((lat_a + lat_b) / 2.0, 5),
            "lon": round((lon_a + lon_b) / 2.0, 5),
        },
    }


# -- AIS coverage -------------------------------------------------------------
#
# The AISStream subscription boxes, [[min_lat, min_lon], [max_lat, max_lon]],
# in the order collector-ais names them. Positions outside these are never
# received, so the edge of a box is the edge of what this platform can see.
AIS_COVERAGE_BOXES: List[List[List[float]]] = [
    # Strait of Hormuz, drawn around where reception happens rather than around
    # the water of interest.
    #
    # The previous box, 24-27N / 56-60E, contained the strait itself and then
    # ran east into the open Gulf of Oman. It excluded every port on the
    # approach: Bandar Abbas sits at 27.18N, a fifth of a degree above the old
    # northern edge, and Jebel Ali at 55.30E, two thirds of a degree west of the
    # old western one. Terrestrial AIS is line-of-sight to a shore receiver and
    # those receivers cluster at ports, so the box was aimed at the one part of
    # the region with no chance of coverage. It returned zero messages for the
    # entire life of the system.
    #
    # Now 24-27.5N / 54-58E: Abu Dhabi, Jebel Ali, the strait, Bandar Abbas and
    # Fujairah, and less of the open water that was never going to report.
    [[24.0, 54.0], [27.5, 58.0]],   # Strait of Hormuz + Persian Gulf approaches
    [[1.0,  103.0], [6.0,  105.0]], # Strait of Malacca
    [[11.5, 43.0],  [13.5, 45.5]],  # Bab-el-Mandeb
    [[29.8, 32.2],  [31.5, 32.8]],  # Suez Canal
    [[12.0, 32.0],  [30.0, 44.0]],  # Red Sea
    [[40.5, 27.5],  [46.5, 41.5]],  # Black Sea
    [[22.0, 119.0], [26.0, 122.5]], # Taiwan Strait
    [[5.0,  109.0], [22.0, 121.0]], # South China Sea
    [[-2.0, 1.0],   [6.0,  9.0]],   # Gulf of Guinea
]


def nm_to_coverage_edge(lat: float, lon: float) -> Optional[float]:
    """Nautical miles from a position to the nearest edge of its AIS box.

    The largest such distance when boxes overlap, since a hull inside two boxes
    is still covered until it leaves both. None when the position is in none.
    """
    best: Optional[float] = None
    for (min_lat, min_lon), (max_lat, max_lon) in AIS_COVERAGE_BOXES:
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue
        cos_lat = max(0.05, math.cos(math.radians(lat)))
        d = min(
            (lat - min_lat) * 60.0,
            (max_lat - lat) * 60.0,
            (lon - min_lon) * 60.0 * cos_lat,
            (max_lon - lon) * 60.0 * cos_lat,
        )
        best = d if best is None else max(best, d)
    return best


def could_have_left_coverage(
    lat: Optional[float], lon: Optional[float], speed_knots: Optional[float], gap_hours: float,
) -> bool:
    """Whether a silent hull may simply have sailed out of AIS coverage.

    Measured on 4 October: 336 of 656 vessels marked dark with a known speed
    were making 5 knots or more at their last report, most of them in the
    Singapore and Taiwan boxes, whose edges are a few hours' steaming from the
    lanes. At its last speed, a hull that could have reached the nearest edge
    within the gap has left coverage, not gone dark. An unknown position or a
    position outside every box is treated as able to leave.
    """
    if lat is None or lon is None:
        return True
    edge = nm_to_coverage_edge(float(lat), float(lon))
    if edge is None:
        return True
    speed = float(speed_knots or 0.0)
    if speed <= STS_MAX_SPEED_KNOTS:
        return False
    return speed * gap_hours >= edge


# Hull names that mark a tanker when AIS static data gives no type code.
_TANKER_NAME_MARKERS = ("TANKER", "OIL", "CRUDE", "PETRO", "LNG", "LPG", "CHEM")


def is_tanker(vessel_type: Optional[str], name: Optional[str] = None) -> bool:
    """A tanker by AIS type, or by its name where the type is unknown."""
    if (vessel_type or "").strip().lower() == "tanker":
        return True
    upper = (name or "").upper()
    return any(marker in upper for marker in _TANKER_NAME_MARKERS)


def sts_is_reportable(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Whether a co-located pair is worth reporting as a transfer.

    The geometry in `sts_pair` cannot tell a transfer from an anchorage: 221 of
    287 STS events in 24 hours to 4 October sat in Taiwan's territorial waters
    and the Singapore approach, between fishing boats and hulls with no type at
    all. The transfers this platform cares about move oil, or involve a hull
    already on a sanctions list, so one of the two has to be a tanker or
    flagged. Each side is {"vessel_type", "name", "flags"}.
    """
    for side in (a, b):
        if side.get("flags"):
            return True
        if is_tanker(side.get("vessel_type"), side.get("name")):
            return True
    return False


__all__ = [
    "AIS_COVERAGE_BOXES",
    "MAX_PLAUSIBLE_SPEED_KNOTS",
    "STS_MAX_SEPARATION_NM",
    "STS_MAX_SPEED_KNOTS",
    "STS_MIN_DWELL_SECONDS",
    "haversine_nm",
    "implied_speed_knots",
    "implausible_position",
    "impossible_transit",
    "could_have_left_coverage",
    "is_tanker",
    "nm_to_coverage_edge",
    "sts_is_reportable",
    "sts_pair",
]
