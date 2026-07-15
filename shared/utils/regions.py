"""
shared/utils/regions.py
 
Geographic region classification.
 
Given a lat/lon, returns a named strategic region.
Used by enrichment service to tag every physical event
with a human-readable region name.
 
Why this matters:
  The correlation engine queries "give me all events in
  'Strait of Hormuz' in the last 72 hours."
  That requires every event to be pre-tagged with a region.
  We can't do bounding box queries on every correlation check —
  too slow. Tag once at enrichment, query by string forever.
 
Regions are ordered by specificity — smaller/more specific regions
first, because a position can be in both "Strait of Hormuz" AND
"Persian Gulf". We want the most specific match.
"""

import logging
import json
from typing import Optional, Tuple

logger = logging.getLogger("shared.regions")

try:
    from shapely.geometry import shape, Point, box
    from shapely.strtree import STRtree
    HAS_SHAPELY = True
except ImportError:
    HAS_SHAPELY = False



_polygons = []
_polygon_names = []
_tree = None

_fallback_boxes = []

def _init_spatial_index():
    global _tree
    
    try:
        with open("regions.geojson", "r") as f:
            data = json.load(f)
            for feature in data.get("features", []):
                name = feature["properties"].get("name", "Unknown")
                
                if HAS_SHAPELY:
                    geom = shape(feature["geometry"])
                    _polygons.append(geom)
                    _polygon_names.append(name)
                else:
                    # Manual bounding box fallback from GeoJSON
                    coords = feature["geometry"]["coordinates"][0]
                    lons = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    _fallback_boxes.append((name, min(lats), max(lats), min(lons), max(lons)))
                    
        if HAS_SHAPELY and _polygons:
            _tree = STRtree(_polygons)
            
    except Exception as e:
        logger.error(f"Failed to load spatial data from regions.geojson: {e}")

_init_spatial_index()

def classify_region(lat: float, lon: float) -> Optional[str]:
    """
    Given a lat/lon, return the most specific named region, or None.
 
    This uses an in-memory STRtree for O(1) spatial intersection checks if 
    shapely is installed. Otherwise it falls back to iterative bounding box checks.
    """
    if _tree is not None:
        p = Point(lon, lat)
        # STRtree query is highly optimized and returns indices of matching envelopes
        indices = _tree.query(p)
        for idx in indices:
            # Do the final, exact point-in-polygon math
            if _polygons[idx].contains(p):
                return _polygon_names[idx]
        return None

    # Fallback to slow Python iterative checks on the geojson boxes
    for name, min_lat, max_lat, min_lon, max_lon in _fallback_boxes:
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return name
    return None

def is_sensitive_region(region: Optional[str]) -> bool:
    """
    Returns True if this region warrants lower anomaly thresholds
    (i.e., we care more about activity here).
    """
    if region is None:
        return False

    HIGH_SENSITIVITY = {
    "Strait of Hormuz",
    "Strait of Malacca",
    "Bab-el-Mandeb",
    "Suez Canal",
    "Taiwan Strait",
    "Persian Gulf",
    "Iranian Territorial",
    "Syrian Territorial",
    "North Korean Waters",
    "Ukrainian Waters",
    "Black Sea",
    "Taiwan ADIZ",
    "Red Sea",
    "South China Sea",
    "Israeli Territorial",
    "Somali Territorial",
    "Caspian Sea",
    "Ukraine Airspace",
    "North Korea ADIZ",
    "Iran Airspace",
    "Syrian Airspace",
    "Israeli Airspace",
    "Yemeni Airspace",
    "Russian Airspace",
    }

    return region in HIGH_SENSITIVITY

def get_region_sensitivity_multiplier(region: Optional[str]) -> float:
    """
    Returns an anomaly score multiplier for a region.
    Used by anomaly scorer to amplify anomaly scores in sensitive areas.
 
    1.0 = neutral
    1.5 = elevated (major shipping lane)
    2.0 = high (conflict adjacent, sanctions zone)
    3.0 = critical (active conflict, heavy sanctions)
    """
    if region is None:
        return 1.0


    multipliers = {
        "Strait of Hormuz": 3.0,
        "Iranian Territorial": 3.0,
        "Iran Airspace": 3.0,
        "North Korean Waters": 3.0,
        "North Korea ADIZ": 3.0,
        "Ukrainian Waters": 2.5,
        "Ukraine Airspace": 2.5,
        "Taiwan Strait": 2.5,
        "Taiwan ADIZ": 2.5,
        "Bab-el-Mandeb": 3.0,  # Elevated due to active conflict
        "Israeli Territorial": 3.0,
        "Israeli Airspace": 3.0,
        "Red Sea": 2.5,        # Elevated due to active conflict (was 1.5)
        "Somali Territorial": 2.5, # Added missing multiplier for piracy risk
        "Somali Airspace": 2.5,
        "Syrian Territorial": 2.5, # Added missing multiplier
        "Syrian Airspace": 2.5,
        "Yemeni Airspace": 3.0,
        "Russian Airspace": 2.0,

        # High
        "Persian Gulf":         2.0,
        "Gulf of Aden":         2.0,
        "Black Sea":            2.0,
        "Suez Canal":           2.0,
        "Strait of Malacca":    1.8,
        "South China Sea":      1.8,
 
        # Elevated
        "Gulf of Oman":         1.5,
        "Barents Sea":          1.5,
        "East China Sea":       1.5,
    }
    return multipliers.get(region, 1.2) # 1.2 for all other watch regions, even if not explicitly listed here
    
# ── VESSEL AIS DECODE HELPERS ─────────────────────────────────────────────────
 
# AIS NavigationalStatus codes → human readable
NAVIGATIONAL_STATUS = {
    0:  "UnderWayUsingEngine",
    1:  "Anchored",
    2:  "NotUnderCommand",
    3:  "RestrictedManoeuverability",
    4:  "ConstrainedByDraught",
    5:  "Moored",
    6:  "Aground",
    7:  "EngagedInFishing",
    8:  "UnderWaySailing",
    15: "Undefined",
}

# AIS vessel/cargo type codes → category strings
def decode_vessel_type(type_code: int) -> str:
    """Convert AIS ship type code to human-readable category."""
    if 80 <= type_code <= 89:
        return "Tanker"
    elif 70 <= type_code <= 79:
        return "CargoVessel"
    elif 60 <= type_code <= 69:
        return "Passenger"
    elif 30 <= type_code <= 35:
        return "Fishing"
    elif 50 <= type_code <= 59:
        return "SpecialCraft"
    elif 35 <= type_code <= 39:
        return "Military"
    elif type_code in (1, 2, 3, 4):
        return "WIG"  # Wing in ground
    else:
        return f"Unknown({type_code})"
    

def decode_nav_status(status_code: int) -> str:
    return NAVIGATIONAL_STATUS.get(status_code, f"Unknown({status_code})")