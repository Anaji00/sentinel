"""
shared/utils/conflict_zones.py

Active and potential combat zones: where they are, why they matter, and what
they move.

Each zone is stated as data. `prior` is the standing assessment written here
("active" for an ongoing war, "flashpoint" for a place where one could start);
the status a reader sees is computed from live news intensity against the
zone's own seven-day baseline (`assess_status`), so a quiet war and an
escalating flashpoint are both visible without editing this file.

Drivers are structural and long-standing -- the reasons the zone is a zone --
not reports of recent events, which arrive through the news and warning feeds.
Ripple effects name the instruments, chokepoints and sectors a zone bears on and
say why, in the same symbols the rest of the platform uses (`exposes:` tags,
futures roots), so a zone's ripple joins to price moves the platform records.

Everything here is pure: no I/O, no clock reads beyond what callers pass in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# Redis keys written by the news collector's conflict poller and read by the
# gateway. Declared once, here, so neither side spells them.
ZONE_LIVE_KEY = "sentinel:conflict:zone:{zone_id}"
NAV_WARNINGS_KEY = "sentinel:conflict:nav_warnings"
# A day: volume is refreshed every six hours and GDELT can hold a client in
# back-off for an hour at a time, so a shorter TTL would blank zones between polls.
LIVE_TTL_SEC = 24 * 3600

PRIOR_ACTIVE = "active"
PRIOR_FLASHPOINT = "flashpoint"


@dataclass(frozen=True)
class Ripple:
    """One thing a zone moves, and why."""
    target: str          # an instrument symbol, a chokepoint, or a sector
    kind: str            # "instrument" | "chokepoint" | "sector"
    why: str


@dataclass(frozen=True)
class ConflictZone:
    zone_id: str
    name: str
    prior: str                                  # PRIOR_ACTIVE | PRIOR_FLASHPOINT
    # (min_lat, min_lon, max_lat, max_lon). Used for navigational warnings and
    # for drawing; events are joined by `regions`, which classify_region emits.
    bbox: Tuple[float, float, float, float]
    center: Tuple[float, float]                 # (lat, lon)
    regions: Tuple[str, ...]
    parties: Tuple[str, ...]
    drivers: Tuple[str, ...]
    ripple: Tuple[Ripple, ...]
    # Phrases that place a headline in this zone. Matched whole-word and
    # case-insensitively; also the place half of the GDELT query.
    keywords: Tuple[str, ...]
    gdelt_places: Tuple[str, ...] = field(default=())

    @property
    def instruments(self) -> List[str]:
        return [r.target for r in self.ripple if r.kind == "instrument"]


def _r(target: str, kind: str, why: str) -> Ripple:
    return Ripple(target=target, kind=kind, why=why)


# Which way an instrument is expected to move when a zone escalates: +1 up,
# -1 down, 0 no stated expectation. A de-escalation (ceasefire, truce) is the
# opposite sign. These are priors to be tested, not assumptions to be traded:
# the reaction study measures each one and rejects a family whose measured move
# contradicts its prior (shared/utils/reaction_study.py).
ESCALATION_DIRECTION: Dict[str, int] = {
    # Supply risk lifts commodities and the price of carrying them.
    "CL=F": 1, "BZ=F": 1, "NG=F": 1, "ZW=F": 1, "ZC=F": 1, "HG=F": 1,
    "GC=F": 1,                       # haven demand
    "FRO": 1, "ZIM": 1,              # tanker and container rates
    "LMT": 1, "ESLT": 1,             # defence demand
    "MP": 1,                         # non-Chinese rare-earth supply
    # Risk to production or to the market itself.
    "TSM": -1, "SOXX": -1, "NVDA": -1, "EWY": -1, "INDA": -1,
    # Cuts both ways (licence risk against higher crude): no prior.
    "CVX": 0,
}


def escalation_direction(symbol: str) -> int:
    return ESCALATION_DIRECTION.get(symbol, 0)


ZONES: Tuple[ConflictZone, ...] = (
    ConflictZone(
        zone_id="ukraine",
        name="Russia-Ukraine war",
        prior=PRIOR_ACTIVE,
        bbox=(44.0, 22.0, 52.5, 40.5),
        center=(48.5, 34.0),
        regions=("Ukraine Airspace", "Ukrainian Waters", "Crimean Waters", "Black Sea"),
        parties=("Russia", "Ukraine", "NATO states (supply)"),
        drivers=(
            "Russia's full-scale invasion of February 2022 and a front line across eastern and southern Ukraine.",
            "Long-range drone and missile campaigns against power grids, refineries and export terminals on both sides.",
            "Control of the Black Sea coast and the grain corridor out of Odesa.",
        ),
        ripple=(
            _r("ZW=F", "instrument", "Ukraine and Russia are among the largest wheat exporters; Black Sea shipping risk moves wheat."),
            _r("ZC=F", "instrument", "Ukraine is a major corn exporter through the same ports."),
            _r("BZ=F", "instrument", "Strikes on Russian refineries and ports, and sanctions on Russian crude, move Brent."),
            _r("NG=F", "instrument", "European gas supply and storage remain sensitive to the war."),
            _r("Turkish Straits", "chokepoint", "The only sea exit for Black Sea grain and Russian crude cargoes."),
            _r("Aerospace & Defense", "sector", "Munitions and air-defence demand from Ukraine and European rearmament."),
        ),
        keywords=("Ukraine", "Ukrainian", "Kyiv", "Kharkiv", "Odesa", "Zaporizhzhia", "Donetsk", "Crimea", "Kherson", "Donbas"),
        gdelt_places=("Ukraine",),
    ),
    ConflictZone(
        zone_id="red_sea",
        name="Red Sea and Yemen",
        prior=PRIOR_ACTIVE,
        bbox=(11.0, 32.0, 30.0, 52.0),
        center=(15.5, 42.5),
        regions=("Red Sea", "Bab-el-Mandeb", "Gulf of Aden", "Yemeni Territorial", "Yemeni Airspace"),
        parties=("Houthi movement (Ansar Allah)", "US and allied navies", "Israel", "Saudi-led coalition"),
        drivers=(
            "Houthi missile and drone attacks on merchant shipping since late 2023, tied to the Gaza war.",
            "The unresolved Yemeni civil war and Iranian support for the Houthis.",
            "Bab-el-Mandeb is a 30 km strait on the Europe-Asia route through Suez.",
        ),
        ripple=(
            _r("BZ=F", "instrument", "Tankers rerouting around the Cape of Good Hope add 10-14 days and lift delivered crude costs."),
            _r("Suez Canal", "chokepoint", "Suez transits fall when Red Sea risk rises; Egypt loses canal revenue."),
            _r("Bab-el-Mandeb", "chokepoint", "The southern entrance every Suez-bound ship must pass."),
            _r("ZIM", "instrument", "Container freight rates rise with rerouting; carriers' earnings move with them."),
            _r("FRO", "instrument", "Longer tanker voyages absorb fleet capacity and raise tanker rates."),
            _r("Shipping insurance", "sector", "War-risk premiums for the southern Red Sea reprice with each attack."),
        ),
        keywords=("Houthi", "Houthis", "Yemen", "Yemeni", "Red Sea", "Bab el-Mandeb", "Bab-el-Mandeb", "Gulf of Aden", "Hodeidah", "Sanaa", "Ansar Allah"),
        gdelt_places=("Red Sea", "Yemen", "Houthi"),
    ),
    ConflictZone(
        zone_id="gulf_hormuz",
        name="Iran and the Strait of Hormuz",
        prior=PRIOR_ACTIVE,
        bbox=(23.0, 47.0, 31.0, 61.0),
        center=(26.5, 56.3),
        regions=("Strait of Hormuz", "Persian Gulf", "Gulf of Oman", "Iranian Territorial", "Iran Airspace"),
        parties=("Iran (IRGC and navy)", "United States", "Israel", "Gulf states"),
        drivers=(
            "Iran's confrontation with Israel and the United States over its nuclear and missile programmes.",
            "Iran's demonstrated ability to seize tankers and to threaten closure of the strait.",
            "About a fifth of the world's oil consumption and a large share of LNG trade pass through Hormuz.",
        ),
        ripple=(
            _r("CL=F", "instrument", "Any disruption at Hormuz removes supply no other route can replace quickly."),
            _r("BZ=F", "instrument", "Gulf crude is priced off Brent-linked benchmarks; risk premium shows here first."),
            _r("NG=F", "instrument", "Qatari LNG exits through Hormuz; global gas prices move with the strait."),
            _r("FRO", "instrument", "Tanker rates spike on Gulf risk as charterers pay war-risk premiums."),
            _r("Strait of Hormuz", "chokepoint", "The single exit for Gulf crude and LNG."),
            _r("Airlines", "sector", "Jet fuel costs and closed airspace over Iran and Iraq hit carriers."),
        ),
        keywords=("Iran", "Iranian", "Tehran", "IRGC", "Hormuz", "Persian Gulf", "Gulf of Oman", "Bandar Abbas", "Kharg"),
        gdelt_places=("Strait of Hormuz", "Iran"),
    ),
    ConflictZone(
        zone_id="israel_levant",
        name="Israel, Gaza, Lebanon and Syria",
        prior=PRIOR_ACTIVE,
        bbox=(29.0, 34.0, 37.5, 42.5),
        center=(32.5, 35.5),
        regions=("Israeli Airspace", "Israeli Territorial", "Syrian Airspace", "Syrian Territorial"),
        parties=("Israel", "Hamas", "Hezbollah", "Syrian factions", "Iran-aligned militias"),
        drivers=(
            "The war that followed the 7 October 2023 Hamas attack and its unsettled aftermath in Gaza.",
            "Israel's conflict with Hezbollah on the Lebanese border.",
            "Post-Assad instability in Syria and the reach of Iran-aligned militias.",
        ),
        ripple=(
            _r("BZ=F", "instrument", "Escalation that draws in Iran carries an oil risk premium."),
            _r("ESLT", "instrument", "Elbit Systems, Israel's largest listed defence contractor."),
            _r("LMT", "instrument", "US munitions and aircraft supply to Israel."),
            _r("Eastern Mediterranean gas", "sector", "Israel's offshore fields supply Egypt and Jordan and can be shut in."),
        ),
        keywords=("Israel", "Israeli", "Gaza", "Hamas", "Hezbollah", "Lebanon", "Lebanese", "West Bank", "Syria", "Syrian", "Damascus", "IDF"),
        gdelt_places=("Gaza", "Lebanon", "Israel"),
    ),
    ConflictZone(
        zone_id="sudan",
        name="Sudan civil war",
        prior=PRIOR_ACTIVE,
        bbox=(8.5, 21.5, 22.5, 38.6),
        center=(15.5, 30.0),
        regions=("Sudanese Territorial", "Sudanese Airspace"),
        parties=("Sudanese Armed Forces", "Rapid Support Forces"),
        drivers=(
            "War between the army and the RSF since April 2023, with famine and mass displacement.",
            "Competition for gold, Port Sudan on the Red Sea, and foreign backers on both sides.",
        ),
        ripple=(
            _r("GC=F", "instrument", "Sudan is a significant gold producer; much of it is smuggled to fund the war."),
            _r("Red Sea", "chokepoint", "Port Sudan is the government's lifeline and a Red Sea naval interest for outside powers."),
        ),
        keywords=("Sudan", "Sudanese", "Khartoum", "Darfur", "El Fasher", "Port Sudan", "RSF", "Rapid Support Forces"),
        gdelt_places=("Sudan",),
    ),
    ConflictZone(
        zone_id="myanmar",
        name="Myanmar civil war",
        prior=PRIOR_ACTIVE,
        bbox=(9.5, 92.0, 28.6, 101.2),
        center=(21.0, 96.0),
        regions=(),
        parties=("Military junta", "Ethnic armed organisations", "People's Defence Forces"),
        drivers=(
            "Armed resistance since the 2021 coup, with the junta losing ground in border states.",
            "Kachin State's rare-earth mines, a major source of heavy rare earths processed in China.",
        ),
        ripple=(
            _r("MP", "instrument", "Supply shocks in Myanmar rare earths raise the value of non-Chinese supply."),
            _r("Rare earths", "sector", "Heavy rare earths for magnets in EVs, wind turbines and missiles."),
        ),
        keywords=("Myanmar", "Burma", "Naypyidaw", "Kachin", "Rakhine", "Shan State", "Tatmadaw"),
        gdelt_places=("Myanmar",),
    ),
    ConflictZone(
        zone_id="drc_east",
        name="Eastern Congo",
        prior=PRIOR_ACTIVE,
        bbox=(-5.0, 26.0, 3.0, 31.5),
        center=(-1.5, 29.0),
        regions=(),
        parties=("DR Congo armed forces", "M23 rebels", "Rwanda (alleged backing)"),
        drivers=(
            "The M23 offensive in North and South Kivu and Rwanda's alleged role in it.",
            "Control of mines for cobalt, coltan, tin and gold.",
        ),
        ripple=(
            _r("HG=F", "instrument", "Congo's copper and cobalt belt makes it pivotal for battery and grid metals."),
            _r("Battery metals", "sector", "Cobalt and tantalum supply chains run through the region."),
        ),
        keywords=("Congo", "DRC", "Goma", "Bukavu", "North Kivu", "South Kivu", "M23"),
        gdelt_places=("Congo",),
    ),
    ConflictZone(
        zone_id="taiwan",
        name="Taiwan Strait",
        prior=PRIOR_FLASHPOINT,
        bbox=(21.5, 117.5, 26.5, 123.0),
        center=(24.0, 120.5),
        regions=("Taiwan Strait", "Taiwan Territorial", "Taiwan ADIZ"),
        parties=("China (PLA)", "Taiwan", "United States", "Japan"),
        drivers=(
            "Beijing's claim to Taiwan and large-scale PLA exercises around the island.",
            "Taiwan makes most of the world's most advanced semiconductors.",
            "US security commitments and arms sales to Taiwan.",
        ),
        ripple=(
            _r("TSM", "instrument", "TSMC fabricates most leading-edge chips; any blockade halts them."),
            _r("SOXX", "instrument", "The semiconductor sector depends on Taiwanese foundries."),
            _r("NVDA", "instrument", "Nvidia's GPUs are made by TSMC in Taiwan."),
            _r("Taiwan Strait", "chokepoint", "A main route for container traffic between Northeast and Southeast Asia."),
        ),
        keywords=("Taiwan", "Taiwanese", "Taipei", "Taiwan Strait", "PLA", "Kinmen"),
        gdelt_places=("Taiwan",),
    ),
    ConflictZone(
        zone_id="south_china_sea",
        name="South China Sea",
        prior=PRIOR_FLASHPOINT,
        bbox=(3.0, 105.0, 22.0, 121.0),
        center=(12.0, 114.0),
        regions=("South China Sea",),
        parties=("China", "Philippines", "Vietnam", "United States"),
        drivers=(
            "Overlapping claims and repeated coast-guard confrontations at Second Thomas and Scarborough Shoals.",
            "The US-Philippines mutual defence treaty covers attacks on Philippine forces there.",
        ),
        ripple=(
            _r("South China Sea", "chokepoint", "Roughly a third of global maritime trade crosses it."),
            _r("NG=F", "instrument", "LNG cargoes to Japan, Korea and China transit the sea."),
            _r("ZIM", "instrument", "Container routes between Asia and the world pass through it."),
        ),
        keywords=("South China Sea", "Scarborough", "Second Thomas", "Spratly", "Paracel", "Philippine coast guard"),
        gdelt_places=("South China Sea",),
    ),
    ConflictZone(
        zone_id="korea",
        name="Korean Peninsula",
        prior=PRIOR_FLASHPOINT,
        bbox=(33.0, 124.0, 43.0, 131.0),
        center=(38.5, 127.0),
        regions=("North Korea ADIZ", "North Korean Waters", "Yellow Sea"),
        parties=("North Korea", "South Korea", "United States", "Japan"),
        drivers=(
            "North Korea's nuclear and ballistic-missile programme and its regular test launches.",
            "North Korea's military cooperation with Russia.",
        ),
        ripple=(
            _r("EWY", "instrument", "South Korean equities, home to Samsung and SK Hynix memory production."),
            _r("Memory chips", "sector", "South Korea makes most of the world's DRAM and HBM."),
        ),
        keywords=("North Korea", "Pyongyang", "Kim Jong Un", "DPRK", "Korean Peninsula"),
        gdelt_places=("North Korea",),
    ),
    ConflictZone(
        zone_id="india_pakistan",
        name="India-Pakistan (Kashmir)",
        prior=PRIOR_FLASHPOINT,
        bbox=(23.0, 66.0, 37.0, 80.0),
        center=(33.5, 75.0),
        regions=(),
        parties=("India", "Pakistan"),
        drivers=(
            "The disputed Kashmir region and cross-border militant attacks.",
            "Two nuclear-armed states whose last exchange of strikes was in May 2025.",
        ),
        ripple=(
            _r("INDA", "instrument", "Indian equities sell off on escalation risk."),
            _r("Airlines", "sector", "Airspace closures reroute flights between Europe and Asia."),
        ),
        keywords=("Kashmir", "Pakistan", "Pakistani", "Line of Control", "Islamabad"),
        gdelt_places=("Kashmir",),
    ),
    ConflictZone(
        zone_id="venezuela",
        name="Venezuela and the Caribbean",
        prior=PRIOR_FLASHPOINT,
        bbox=(8.0, -73.5, 15.5, -59.5),
        center=(10.5, -66.0),
        regions=("Venezuelan Territorial", "Venezuelan Airspace", "Caribbean Sea"),
        parties=("Venezuela", "United States"),
        drivers=(
            "US military pressure on Venezuela, including strikes on vessels in the Caribbean since 2025.",
            "Venezuela holds the world's largest proven oil reserves under sanctions.",
        ),
        ripple=(
            _r("CL=F", "instrument", "Venezuelan heavy crude supply and US Gulf Coast refiners that run it."),
            _r("CVX", "instrument", "Chevron is the US major licensed to operate in Venezuela."),
        ),
        keywords=("Venezuela", "Venezuelan", "Caracas", "Maduro"),
        gdelt_places=("Venezuela",),
    ),
    ConflictZone(
        zone_id="baltic",
        name="Baltic Sea (Russia-NATO)",
        prior=PRIOR_FLASHPOINT,
        bbox=(53.5, 9.5, 66.0, 30.5),
        center=(58.0, 20.0),
        regions=("Baltic Sea", "Danish Straits"),
        parties=("Russia", "NATO Baltic states"),
        drivers=(
            "Damage to undersea cables and pipelines, and GPS jamming across the region.",
            "Russia's shadow fleet of tankers exporting crude from Primorsk and Ust-Luga.",
        ),
        ripple=(
            _r("BZ=F", "instrument", "Russian Baltic ports ship a large share of its seaborne crude."),
            _r("Danish Straits", "chokepoint", "Every Baltic tanker passes the Danish Straits."),
            _r("Undersea infrastructure", "sector", "Cables and gas interconnectors under the Baltic."),
        ),
        keywords=("Baltic", "Kaliningrad", "Gulf of Finland", "shadow fleet", "undersea cable"),
        gdelt_places=("Baltic Sea",),
    ),
)

ZONES_BY_ID: Dict[str, ConflictZone] = {z.zone_id: z for z in ZONES}

# Region name (as classify_region emits it) to the zones that include it.
REGION_ZONES: Dict[str, Tuple[str, ...]] = {}
for _z in ZONES:
    for _r in _z.regions:
        REGION_ZONES[_r] = REGION_ZONES.get(_r, ()) + (_z.zone_id,)


def region_zone_tags(region: Optional[str]) -> List[str]:
    """`zone:<id>` for each zone an observation's region belongs to."""
    return [f"zone:{z}" for z in REGION_ZONES.get(region or "", ())]

# What makes a news article about conflict rather than about the place.
CONFLICT_TERMS: Tuple[str, ...] = (
    "attack", "strike", "missile", "drone", "shelling", "clashes",
    "killed", "military", "troops", "warship", "offensive", "ceasefire",
)


def gdelt_query(zone: ConflictZone) -> str:
    """The GDELT DOC query for a zone: one of its places and one conflict term."""
    places = zone.gdelt_places or zone.keywords[:2]
    place_part = " OR ".join(f'"{p}"' if " " in p else p for p in places)
    if len(places) > 1:
        place_part = f"({place_part})"
    terms = " OR ".join(CONFLICT_TERMS)
    return f"{place_part} ({terms})"


def _keyword_pattern(words: Iterable[str]) -> "re.Pattern[str]":
    alternatives = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(rf"\b(?:{alternatives})\b", re.IGNORECASE)


_ZONE_PATTERNS: Dict[str, "re.Pattern[str]"] = {z.zone_id: _keyword_pattern(z.keywords) for z in ZONES}


def zones_for_text(text: str) -> List[str]:
    """The zones a headline names, by whole-word keyword."""
    if not text:
        return []
    return [zid for zid, pat in _ZONE_PATTERNS.items() if pat.search(text)]


def keyword_regex(zone: ConflictZone) -> str:
    """A Postgres case-insensitive regex (`~*`) for a zone's keywords."""
    alternatives = "|".join(re.escape(w) for w in sorted(set(zone.keywords), key=len, reverse=True))
    return rf"\m({alternatives})\M"


def zone_for_point(lat: float, lon: float) -> Optional[str]:
    """The smallest zone whose box contains the point, or None.

    Boxes overlap -- the Red Sea box takes in Port Sudan, the Gulf box the
    Arabian coast -- and the smallest box is the most specific claim.
    """
    best: Optional[Tuple[float, str]] = None
    for z in ZONES:
        min_lat, min_lon, max_lat, max_lon = z.bbox
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            area = (max_lat - min_lat) * (max_lon - min_lon)
            if best is None or area < best[0]:
                best = (area, z.zone_id)
    return best[1] if best else None


# -- navigational warnings ----------------------------------------------------

_COORD = re.compile(
    r"(\d{1,2})-(\d{2}(?:\.\d+)?)\s*([NS])\s+(\d{1,3})-(\d{2}(?:\.\d+)?)\s*([EW])"
)

MILITARY_WARNING = re.compile(
    r"MISSILE|ROCKET|GUNNERY|LIVE FIRING|FIRING EXERCISE|HAZARDOUS OPERATIONS|"
    r"GNSS|GPS|JAMMING|INTERFERENCE|MILITARY|NAVAL|MINES?\b|UNMANNED|DRONE|"
    r"ATTACK|ARMED|PIRACY|HIJACK",
    re.IGNORECASE,
)


def parse_coordinates(text: str) -> List[Tuple[float, float]]:
    """Every degree-minute position in an NGA warning, as decimal (lat, lon)."""
    points = []
    for d1, m1, ns, d2, m2, ew in _COORD.findall(text or ""):
        lat = int(d1) + float(m1) / 60.0
        lon = int(d2) + float(m2) / 60.0
        if ns == "S":
            lat = -lat
        if ew == "W":
            lon = -lon
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            points.append((round(lat, 4), round(lon, 4)))
    return points


def classify_warning(text: str) -> Dict[str, object]:
    """Where an NGA broadcast warning sits, and whether it is military.

    Returns {"points", "centroid", "zone_id", "military", "area"}. `area` is the
    warning's first line ("BLACK SEA.") which names the water it covers.
    """
    points = parse_coordinates(text)
    centroid = None
    zone_id = None
    if points:
        lat = sum(p[0] for p in points) / len(points)
        lon = sum(p[1] for p in points) / len(points)
        centroid = (round(lat, 4), round(lon, 4))
        zone_id = zone_for_point(lat, lon)
    first = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    return {
        "points": points,
        "centroid": centroid,
        "zone_id": zone_id,
        "military": bool(MILITARY_WARNING.search(text or "")),
        "area": first.rstrip(".").strip(),
    }


# -- status ------------------------------------------------------------------

def daily_volumes(hourly: Sequence[Tuple[str, float]]) -> Tuple[float, float]:
    """The last 24 hours' article volume and the mean of the days before it.

    `hourly` is GDELT timelinevolraw as (YYYYMMDDTHHMMSSZ, value) pairs, which
    sort by time as strings. Returns (last_24h, prior_daily_mean); the mean is
    0.0 when there is no earlier history.
    """
    if not hourly:
        return 0.0, 0.0
    ordered = sorted(hourly, key=lambda p: p[0])
    last = ordered[-24:]
    earlier = ordered[:-24]
    last_24h = float(sum(v for _, v in last))
    if not earlier:
        return last_24h, 0.0
    prior_daily = float(sum(v for _, v in earlier)) * 24.0 / len(earlier)
    return last_24h, round(prior_daily, 1)


def assess_status(prior: str, last_24h: float, baseline_daily: float) -> Dict[str, object]:
    """A zone's live status from its prior and its news intensity.

    The trend compares the last 24 hours with the zone's own daily mean over
    the preceding days, so a war that is always in the news is not called
    escalating merely for being reported, and a flashpoint that suddenly is
    reported is. Returns {"status", "trend", "ratio"}.
    """
    ratio = None
    if baseline_daily > 0:
        ratio = round(last_24h / baseline_daily, 2)
    if ratio is None:
        trend = "unmeasured"
    elif ratio >= 1.5:
        trend = "rising"
    elif ratio <= 0.6:
        trend = "falling"
    else:
        trend = "steady"

    if prior == PRIOR_ACTIVE:
        status = "active-escalating" if trend == "rising" else "active"
    else:
        # A flashpoint is called escalating only on a measured rise with enough
        # volume behind it to be more than a handful of articles.
        status = "escalating" if (trend == "rising" and last_24h >= 50) else "watch"
    return {"status": status, "trend": trend, "ratio": ratio}


__all__ = [
    "CONFLICT_TERMS", "ConflictZone", "LIVE_TTL_SEC", "MILITARY_WARNING",
    "NAV_WARNINGS_KEY", "PRIOR_ACTIVE", "PRIOR_FLASHPOINT", "Ripple",
    "REGION_ZONES", "ZONES", "ZONES_BY_ID", "ZONE_LIVE_KEY", "assess_status", "classify_warning",
    "daily_volumes", "gdelt_query", "keyword_regex", "parse_coordinates", "region_zone_tags",
    "zone_for_point", "zones_for_text",
]
