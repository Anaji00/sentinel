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

from typing import Optional, Tuple

# ── REGION DEFINITIONS ────────────────────────────────────────────────────────
# Format: (name, min_lat, max_lat, min_lon, max_lon)
# Ordered by specificity (smaller areas first)

MARITIME_CHOKEPOINTS = [
    # ── HIGH PRIORITY: Strait chokepoints ────────────────────────────────────
    ("Strait of Hormuz",       24.0, 27.0,  56.0, 60.0),
    ("Strait of Malacca",       1.0,  6.0, 103.0, 105.0),
    ("Bab-el-Mandeb",          11.5, 13.5,  43.0, 45.5),   # Yemen/Djibouti
    ("Suez Canal",             29.8, 31.5,  32.2, 32.8),
    ("Strait of Gibraltar",    35.5, 36.5,  -5.8, -5.2),
    ("Danish Straits",         55.0, 58.0,   8.0, 13.0),   # Baltic access
    ("Turkish Straits",        40.5, 41.5,  26.0, 29.5),   # Bosphorus/Dardanelles
    ("Taiwan Strait",          22.0, 26.0, 119.0, 122.5),
    ("Strait of Lombok",       -9.0, -7.5, 115.0, 116.5),  # Indonesia
    ("Strait of Sicily",       32.0, 37.0,  12.0, 15.0),   # Italy/Sicily
    ("Strait of Dover",        50.5, 51.5,   0.0,   2.0),   # UK/France
    ("Panama Canal",           7.5, 10.0, -80.5, -77.0),
 
    # ── REGIONAL SEAS ─────────────────────────────────────────────────────────
    ("Persian Gulf",           22.0, 30.0,  47.0, 57.0),
    ("Gulf of Oman",           21.0, 26.0,  56.0, 62.0),
    ("Red Sea",                12.0, 30.0,  31.5, 44.0),
    ("Gulf of Aden",            9.0, 13.5,  42.0, 52.0),
    ("South China Sea",         1.0, 23.0, 105.0, 121.0),
    ("East China Sea",         24.0, 34.0, 119.0, 131.0),
    ("Yellow Sea",             31.0, 40.0, 119.0, 127.0),
    ("Black Sea",              40.5, 46.5,  27.5, 41.5),
    ("Caspian Sea",            36.5, 47.5,  49.0, 55.0),
    ("Baltic Sea",             53.5, 66.0,   9.5, 30.0),
    ("North Sea",              50.5, 61.0,  -4.0, 10.0),
    ("Mediterranean Sea",      30.0, 46.5,  -6.0, 37.0),
    ("Caribbean Sea",           8.0, 23.5, -86.0, -58.0),
    ("Gulf of Mexico",         18.0, 30.5, -98.0, -80.0),
    ("Gulf of Guinea",        -10.0,  8.0,  -8.0, 10.0),
    ("Mozambique Channel",    -26.0, -10.0, 34.0, 45.0),
    ("Barents Sea",            68.0, 81.0,  14.0, 60.0),   # Arctic, Russian waters
    ("Bering Sea",            52.0, 66.0, 170.0, -170.0), # Arctic, US/Russian waters
    ("Gulf of Alaska",        54.0, 60.0, -170.0, -130.0), # US waters
    ("Sea of Okhotsk",        45.0, 60.0, 135.0, 160.0),  # Russian waters
    ("Labrador Sea",          50.0, 65.0, -70.0, -50.0),   # Canada/Greenland
    ("Norwegian Sea",         62.0, 72.0,   5.0, 20.0),   # Norway/Russia
    ("Gulf of St. Lawrence",  46.5, 51.5, -66.5, -57.5),   # Canada
    ("Caspian Sea",            36.5, 47.5,  49.0, 55.0),
 
    # ── CONFLICT/WATCH ZONES ──────────────────────────────────────────────────
    ("Ukrainian Waters",       43.0, 48.0,  29.0, 37.5),
    ("North Korean Waters",    37.0, 43.0, 124.0, 132.0),
    ("Iranian Territorial",    25.0, 31.0,  48.0, 57.5),
    ("Syrian Territorial",     32.0, 37.0,  35.5, 42.0),
    ("Venezuelan Territorial",  0.0, 12.0, -75.0, -59.0),
    ("Israeli Territorial",     29.0, 34.0,  34.0, 35.5),
    ("Libyan Territorial",     30.0, 34.0,  10.0, 25.0),
    ("Yemeni Territorial",     12.0, 19.0,  42.0, 54.0),
    ("Somali Territorial",      -3.0, 12.0,  40.0, 52.0),
    ("Sudanese Territorial",     3.0, 22.0,  32.0, 38.0),
    ("Gulf of Sidra",          30.0, 35.0,  10.0, 25.0),   # Libya's claimed waters
    ("Crimean Waters",         44.0, 46.0,  32.0, 36.0),   # Disputed Russia/Ukraine
    ("Georgian Waters",        41.5, 43.5,  40.0, 42.5),   # Disputed Russia/Georgia
    ("Taiwan Territorial",     21.5, 25.5, 119.0, 122.5), # Disputed China/Taiwan
    ("Afghan Territorial",      29.0, 38.0,  60.0, 75.0),   # For maritime events near Afghanistan
    ("Malian Territorial",     10.0, 25.0, -5.0, 5.0),     # For maritime events near Mali
    ("Nigerian Territorial",    4.0, 14.0,   2.0, 15.0),   # For maritime events near Nigeria
    ("Colombian Territorial", -5.0, 15.0, -80.0, -65.0),   # For maritime events near Colombia
    # ── KEY PORT APPROACHES ───────────────────────────────────────────────────
    ("Singapore Approach",      1.0,  2.0, 103.5, 104.5),
    ("Shanghai Approach",      30.0, 32.5, 120.0, 123.0),
    ("Rotterdam Approach",     51.5, 53.0,   3.0,   6.0),
    ("Houston Ship Channel",   29.0, 30.0, -95.5, -94.5),
    ("Panama Canal Approach",  7.0, 11.0, -81.0, -77.0),
]   

AVIATION_REGIONS = [
    # These overlap maritime where relevant
    ("Taiwan ADIZ",            22.0, 26.0, 119.0, 126.0),
    ("Ukraine Airspace",       44.0, 52.5,  22.0,  40.5),
    ("Belarus Airspace",       51.0, 56.5,  23.5,  32.5),
    ("North Korea ADIZ",       37.0, 43.0, 124.0, 132.0),
    ("Iran Airspace",          25.0, 39.5,  44.0,  63.5),
    ("Syrian Airspace",       32.0, 37.0,  35.5,  42.0),
    ("Venezuelan Airspace",     0.0, 12.0, -75.0, -59.0),
    ("Israeli Airspace",       29.0, 34.0,  34.0,  35.5),
    ("Libyan Airspace",       30.0, 34.0,  10.0,  25.0),
    ("Yemeni Airspace",       12.0, 19.0,  42.0,  54.0),
    ("Somali Airspace",        -3.0, 12.0, 40.0,   52.0),
    ("Sudanese Airspace",       3.0, 22.0, 32.0,   38.0),
    ("Russian Airspace",       41.0, 82.0, 19.0,   180.0), # Covers all of Russia, including Arctic
]

# Combine all for general use
ALL_REGIONS = MARITIME_CHOKEPOINTS + AVIATION_REGIONS

def classify_region(lat: float, lon: float) -> Optional[str]:
    """
    Given a lat/lon, return the most specific named region, or None.
 
    This iterates regions in order — smaller/more specific ones are
    listed first, so the first match is the most precise.
 
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
 
    Returns:
        Region name string, or None if not in any watch region
    """

    for name, min_lat, max_lat, min_lon, max_lon, in ALL_REGIONS:
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
    "North Korean Waters",
    "Ukrainian Waters",
    "Black Sea",
    "Taiwan ADIZ",
    "Red Sea",
    "South China Sea",
    "Israeli Territorial",
    "Somali Territorial",
    "Caspian Sea",
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
        "North Korean Waters": 3.0,
        "Ukrainian Waters": 2.5,
        "Taiwan Strait": 2.5,
        "Taiwan ADIZ": 2.5,
        "Bab-el-Mandeb": 2.5,
        "Israeli Territorial": 3.0,


        # High
        "Persian Gulf":         2.0,
        "Gulf of Aden":         2.0,
        "Black Sea":            2.0,
        "Suez Canal":           2.0,
        "Strait of Malacca":    1.8,
        "South China Sea":      1.8,
 
        # Elevated
        "Red Sea":              1.5,
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