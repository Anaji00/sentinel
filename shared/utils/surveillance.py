"""
shared/utils/surveillance.py

Watching the watchers: public data on who surveils whom, parsed. Pure; the
collector (services/collector-civic/main.py) fetches and the API reads.

  OpenStreetMap surveillance nodes (man_made=surveillance), the data DeFlock
  and similar projects crowdsource: licence-plate readers (surveillance:type=
  ALPR, most of them Flock Safety) and acoustic gunshot detectors. Volunteer
  mapped, so coverage is uneven: an empty area is unmapped, not unwatched.

  EFF's Atlas of Surveillance (atlasofsurveillance.org/download.csv): which
  agency uses which technology (face recognition, drones, cell-site
  simulators, ALPR, camera registries, real-time crime centres...), each with
  a source link. Agencies have no coordinates; they are placed at their city
  or county's Census centroid.

Agencies and vendors are the subject. Nothing here tracks a person.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, TextIO, Tuple

ATLAS_URL = "https://atlasofsurveillance.org/download.csv"
GAZ_PLACES_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_place_national.zip"
GAZ_COUNTIES_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip"
# The main Overpass instance refused or timed out whole-country and regional
# queries on 9 Oct 2026; the mail.ru mirror answered. Tried in order.
OVERPASS_URLS = (
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass-api.de/api/interpreter",
)
DEVICE_KINDS = ("ALPR", "gunshot_detector")
# DeFlock's national ALPR file, built from the same OpenStreetMap data (ODbL):
# one weekly download of ~38 MB (144,070 readers on 9 Oct 2026) in place of
# ~1,100 Overpass tile queries, which the public servers refused for hours.
DEFLOCK_URL = "https://data.dontgetflocked.com/cameras.geojson.gz"
# Overpass is still asked for what DeFlock's file does not carry.
OVERPASS_KINDS = ("gunshot_detector",)
# 1.5 degrees keeps a dense metro's reply to a few MB (a 3-degree tile pushed the
# 160M collector over its limit on 9 Oct 2026).
TILE_DEG = 1.5
# Contiguous US, Alaska, Hawaii, Puerto Rico (south, west, north, east).
US_REGIONS: Tuple[Tuple[float, float, float, float], ...] = (
    (24.0, -125.0, 50.0, -66.0),
    (51.0, -170.0, 72.0, -129.0),
    (18.5, -161.0, 22.5, -154.0),
    (17.5, -68.0, 18.7, -65.0),
)

Box = Tuple[float, float, float, float]


def tiles(regions: Tuple[Box, ...] = US_REGIONS, step: float = TILE_DEG) -> Iterator[Box]:
    for s, w, n, e in regions:
        lat = s
        while lat < n:
            lon = w
            while lon < e:
                yield (round(lat, 4), round(lon, 4), round(min(n, lat + step), 4), round(min(e, lon + step), 4))
                lon += step
            lat += step


def split(box: Box) -> List[Box]:
    """Quarters of a box, for a tile the server would not answer whole."""
    s, w, n, e = box
    ms, mw = (s + n) / 2, (w + e) / 2
    return [(s, w, ms, mw), (s, mw, ms, e), (ms, w, n, mw), (ms, mw, n, e)]


def overpass_query(box: Box, kinds: Tuple[str, ...] = DEVICE_KINDS, timeout: int = 120) -> str:
    s, w, n, e = box
    pattern = "|".join(re.escape(k) for k in kinds)
    return (f'[out:json][timeout:{timeout}];node["man_made"="surveillance"]["surveillance:type"~"^({pattern})$"]'
            f"({s},{w},{n},{e});out body;")


# A failure counts as "the tile is too dense" only after the server worked on it
# this long; a quick error is load (the busy page also says "timeout").
SLOW_FAILURE_SEC = 90.0


def overpass_outcome(status: Optional[int], body: bytes, elapsed: float) -> str:
    """'ok' (complete JSON), 'dense' (the query ran out of time: split the tile)
    or 'busy' (the server turned it away: back off, same tile). A 200 can carry
    a partial answer with a runtime-error remark; that is not 'ok'."""
    if status == 200:
        head = body[:4096].lower()
        if b'"remark"' in head and (b"runtime error" in head or b"timed out" in head):
            return "dense"
        return "ok"
    return "dense" if elapsed >= SLOW_FAILURE_SEC else "busy"


# Tags kept with each device: what it is, who runs it, which way it looks.
KEEP_TAGS = ("manufacturer", "operator", "operator:type", "brand", "camera:direction", "direction",
             "camera:mount", "camera:type", "surveillance", "surveillance:zone", "start_date", "note")


@dataclass
class Device:
    osm_id: int
    lat: float
    lon: float
    kind: str
    manufacturer: Optional[str]
    operator: Optional[str]
    direction: Optional[str]
    tags: Dict[str, str]


def parse_overpass(body: Dict[str, Any]) -> List[Device]:
    out: List[Device] = []
    for el in body.get("elements") or []:
        if el.get("type") != "node" or "lat" not in el:
            continue
        tags = el.get("tags") or {}
        kind = tags.get("surveillance:type")
        if kind not in DEVICE_KINDS:
            continue
        out.append(Device(int(el["id"]), float(el["lat"]), float(el["lon"]), kind,
                          tags.get("manufacturer") or tags.get("brand"), tags.get("operator"),
                          tags.get("camera:direction") or tags.get("direction"),
                          {k: v for k, v in tags.items() if k in KEEP_TAGS}))
    return out


def iter_geojson_features(fh: BinaryIO, chunk: int = 1 << 16) -> Iterator[Dict[str, Any]]:
    """Features of a FeatureCollection read a piece at a time: the national
    file parsed whole would take ~10x its size in memory."""
    return iter_json_array(fh, "features", chunk)


def iter_json_array(fh: BinaryIO, key: str, chunk: int = 1 << 16) -> Iterator[Dict[str, Any]]:
    """The objects of the first array under `key` in a JSON document, one at a
    time (the array is assumed to hold objects)."""
    dec = json.JSONDecoder()
    buf = ""
    started = False
    pending = b""
    while True:
        data = fh.read(chunk)
        raw = pending + (data or b"")
        try:
            text, pending = raw.decode("utf-8"), b""
        except UnicodeDecodeError as e:          # a multi-byte character split across reads
            text, pending = raw[:e.start].decode("utf-8"), raw[e.start:]
        buf += text
        if not started:
            i = buf.find(f'"{key}"')
            if i < 0:
                if not data:
                    return
                continue
            j = buf.find("[", i)
            if j < 0:
                continue
            buf, started = buf[j + 1:], True
        while True:
            k = 0
            while k < len(buf) and buf[k] in " \t\r\n,":
                k += 1
            buf = buf[k:]
            if not buf or buf[0] == "]":
                if buf[:1] == "]":
                    return
                break
            try:
                obj, end = dec.raw_decode(buf)
            except ValueError:
                break                     # an object split across reads: read more
            yield obj
            buf = buf[end:]
        if not data:
            return


def parse_deflock(feature: Dict[str, Any]) -> Optional[Device]:
    """One DeFlock feature -> a licence-plate reader (nodes only: osm_id keys
    the table, and ways would collide with node ids)."""
    p = feature.get("properties") or {}
    g = feature.get("geometry") or {}
    if p.get("osmType") != "node" or g.get("type") != "Point":
        return None
    try:
        lon, lat = float(g["coordinates"][0]), float(g["coordinates"][1])
        osm_id = int(p["osmId"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    direction = p.get("direction") if p.get("direction") is not None else p.get("directions")
    tags = {k: str(p[k]) for k in ("surveillanceZone", "mountType", "osmTimestamp", "ref") if p.get(k) is not None}
    tags["source"] = "deflock"
    return Device(osm_id, lat, lon, "ALPR", p.get("brand"), p.get("operator"),
                  str(direction) if direction is not None else None, tags)


# ── Census gazetteer: names -> centroids ─────────────────────────────────────

_PLACE_SUFFIX = re.compile(r"\s+(city|town|village|borough|CDP|municipality|city and borough|"
                           r"consolidated government|metropolitan government|urban county|unified government)"
                           r"(\s*\(balance\))?$", re.I)
_COUNTY_SUFFIX = re.compile(r"\s+(county|parish|borough|census area|municipality|city and borough|municipio)$", re.I)


def norm_place(name: str) -> str:
    n = _PLACE_SUFFIX.sub("", (name or "").strip())
    n = re.sub(r"^(st\.?|saint)\s+", "saint ", n.lower())
    return re.sub(r"[^a-z0-9 ]", "", n).strip()


def norm_county(name: str) -> str:
    n = _COUNTY_SUFFIX.sub("", (name or "").strip())
    n = re.sub(r"^(st\.?|saint)\s+", "saint ", n.lower())
    return re.sub(r"[^a-z0-9 ]", "", n).strip()


Gazetteer = Dict[Tuple[str, str], Tuple[float, float]]


def parse_gazetteer(text: str, county: bool = False) -> Gazetteer:
    """Census gazetteer text -> {(state, normalised name): (lat, lon)}. Where two
    places share a normalised name in a state, the larger land area wins."""
    out: Gazetteer = {}
    area: Dict[Tuple[str, str], float] = {}
    reader = csv.reader(io.StringIO(text), delimiter="\t")
    header = [h.strip() for h in next(reader, [])]
    idx = {h: i for i, h in enumerate(header)}
    for row in reader:
        try:
            st, name = row[idx["USPS"]].strip(), row[idx["NAME"]].strip()
            lat, lon = float(row[idx["INTPTLAT"]]), float(row[idx["INTPTLONG"]])
            land = float(row[idx["ALAND"]])
        except (KeyError, IndexError, ValueError):
            continue
        key = (st, norm_county(name) if county else norm_place(name))
        if land > area.get(key, -1):
            out[key], area[key] = (lat, lon), land
    return out


# ── Atlas of Surveillance ─────────────────────────────────────────────────────

@dataclass
class AgencyTech:
    aos_id: str
    agency: str
    city: str
    county: str
    state: str
    lea_type: str
    juris_type: str
    technology: str
    vendor: Optional[str]
    summary: str
    link: Optional[str]
    link_date: Optional[date]
    lat: Optional[float]
    lon: Optional[float]
    geo_level: Optional[str]          # 'place', 'county' or None


# The Atlas spells face recognition two ways.
_TECH_ALIASES = {"FRT": "Face Recognition"}


def _date(x: str) -> Optional[date]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(x.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_atlas(text: str, places: Optional[Gazetteer] = None, counties: Optional[Gazetteer] = None) -> List[AgencyTech]:
    return list(iter_atlas(io.StringIO(text), places, counties))


def iter_atlas(fh: TextIO, places: Optional[Gazetteer] = None,
               counties: Optional[Gazetteer] = None) -> Iterator[AgencyTech]:
    """Rows of the Atlas CSV from an open text file, one at a time: the whole
    file as text plus every parsed row at once did not fit the collector."""
    for r in csv.DictReader(fh):
        aos = (r.get("AOSNUMBER") or "").strip()
        tech = (r.get("Technology") or "").strip()
        if not aos or not tech:
            continue
        st, city, county = (r.get("State") or "").strip(), (r.get("City") or "").strip(), (r.get("County") or "").strip()
        lat = lon = None
        level = None
        if places and city and (st, norm_place(city)) in places:
            (lat, lon), level = places[(st, norm_place(city))], "place"
        elif counties and county and (st, norm_county(county)) in counties:
            (lat, lon), level = counties[(st, norm_county(county))], "county"
        yield AgencyTech(aos, (r.get("Agency") or "").strip(), city, county, st,
                              (r.get("Type of LEA") or "").strip(), (r.get("Type of Juris") or "").strip(),
                              _TECH_ALIASES.get(tech, tech), (r.get("Vendor") or "").strip() or None,
                              (r.get("Summary") or "").strip(), (r.get("Link 1") or "").strip() or None,
                         _date(r.get("Link 1 Date") or ""), lat, lon, level)


# ── FAA drone-as-first-responder waivers (EFF FOIA release) ──────────────────

DFR_SHEET_CSV = ("https://docs.google.com/spreadsheets/d/1FpoISaAdmQLtMKelPy1sdCOLvYVNxSXBVzASo-I-djI/"
                 "export?format=csv&gid=0")
DFR_SOURCE = "https://www.eff.org/deeplinks/2026/07/hundreds-drone-first-responder-programs-could-soon-be-launched-across-country"


@dataclass
class DroneWaiver:
    applicant: str
    city: Optional[str]
    state: str
    category: Optional[str]
    status: Optional[str]
    received: Optional[date]
    completed: Optional[date]
    start_date: Optional[date]
    altitude: Optional[str]


def parse_dfr(text: str) -> List[DroneWaiver]:
    """EFF's sheet of FAA DFR waivers. A waiver permits a program; it does not
    show one is flying."""
    out: List[DroneWaiver] = []
    seen = set()
    for r in csv.DictReader(io.StringIO(text)):
        app = (r.get("Applicant") or "").strip()
        st = (r.get("State") or "").strip().upper()
        if not app or len(st) != 2 or (app, st) in seen:
            continue
        seen.add((app, st))
        out.append(DroneWaiver(app, (r.get("City") or "").strip() or None, st,
                               (r.get("Category") or "").strip() or None, (r.get("Status") or "").strip() or None,
                               _date(r.get("Date Received") or ""), _date(r.get("Completed") or ""),
                               _date(r.get("Start Date") or ""), (r.get("200ft/400ft") or "").strip() or None))
    return out


# ── Federal money (USAspending, no key) ──────────────────────────────────────

USASPENDING_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
# Vendors whose business is surveillance. Broad suppliers (Motorola's radios,
# LexisNexis, Thomson Reuters) are left out so totals are not padded.
SURVEILLANCE_VENDORS = ("Flock Group", "Axon Enterprise", "SoundThinking", "ShotSpotter", "Clearview AI",
                        "Palantir Technologies", "Babel Street", "Venntel", "Fog Data Science", "Cellebrite",
                        "Penlink", "Skydio", "Rekor Systems", "Dataminr", "ShadowDragon", "Fusus")
GRANT_KEYWORDS = ("license plate reader", "gunshot detection", "facial recognition", "real time crime center",
                  "drone as first responder")
AWARD_FIELDS = ["Award ID", "Recipient Name", "Award Amount", "Awarding Agency", "Awarding Sub Agency",
                "Start Date", "Description"]
CONTRACT_CODES = ["A", "B", "C", "D"]
GRANT_CODES = ["02", "03", "04", "05"]


def usaspending_query(kind: str, term: str, page: int, start: str = "2015-10-01", end: str = "2026-09-30",
                      limit: int = 100) -> Dict[str, Any]:
    """Request body for one page of contracts to a vendor or grants mentioning a technology."""
    f: Dict[str, Any] = {"time_period": [{"start_date": start, "end_date": end}]}
    if kind == "contract":
        f.update({"recipient_search_text": [term], "award_type_codes": CONTRACT_CODES})
    else:
        f.update({"keywords": [term], "award_type_codes": GRANT_CODES})
    return {"filters": f, "fields": AWARD_FIELDS, "page": page, "limit": limit, "sort": "Award Amount", "order": "desc"}


def parse_awards(body: Dict[str, Any], kind: str, matched: str) -> List[Tuple]:
    """(award_id, kind, matched, recipient, amount, agency, sub_agency, start_date, description)."""
    out = []
    for r in body.get("results") or []:
        aid = r.get("generated_internal_id") or r.get("Award ID")
        if not aid:
            continue
        try:
            amt = float(r.get("Award Amount")) if r.get("Award Amount") is not None else None
        except (TypeError, ValueError):
            amt = None
        out.append((str(aid), kind, matched, r.get("Recipient Name"), amt, r.get("Awarding Agency"),
                    r.get("Awarding Sub Agency"), _date(str(r.get("Start Date") or "")),
                    (r.get("Description") or "")[:500] or None))
    return out


# ── California data broker registry (CPPA, Delete Act) ───────────────────────

CA_BROKER_CSV = "https://cppa.ca.gov/data_broker_registry/registry.csv"
CA_BROKER_PAGE = "https://cppa.ca.gov/data_broker_registry/"
# What each broker declares, by a distinctive phrase of the registry's column
# heading (the headings are long sentences and change wording year to year).
BROKER_FLAGS: Dict[str, str] = {
    "minors": "personal information of minors",
    "account_logins": "account logins",
    "government_ids": "government",
    "immigration_status": "citizenship data",
    "union_membership": "union membership",
    "sexual_orientation": "sexual orientation",
    "gender_identity": "gender identity",
    "biometrics": "biometric",
    "precise_geolocation": "precise geolocation",
    "reproductive_health": "reproductive health",
    "sold_to_foreign_actor": "foreign actor",
    "sold_to_federal_government": "federal government",
    "sold_to_other_states": "other state governments",
    "sold_to_law_enforcement": "law enforcement",
    "sold_to_genai": "GenAI",
}
BROKER_FLAG_TEXT: Dict[str, str] = {
    "minors": "collects data on minors", "account_logins": "collects account logins",
    "government_ids": "collects government ID numbers", "immigration_status": "collects citizenship or immigration status",
    "union_membership": "collects union membership", "sexual_orientation": "collects sexual orientation",
    "gender_identity": "collects gender identity", "biometrics": "collects biometric data",
    "precise_geolocation": "collects precise location", "reproductive_health": "collects reproductive health data",
    "sold_to_foreign_actor": "sold data to a foreign actor", "sold_to_federal_government": "sold data to the federal government",
    "sold_to_other_states": "sold data to other state governments",
    "sold_to_law_enforcement": "sold data to law enforcement (no subpoena)", "sold_to_genai": "sold data to AI developers",
}


# ── Oregon data broker registry (DCBS, ORS 646A.593) ────────────────────────
# The licence search publishes the whole registry as a tab-separated file: a
# public form POST answers with a redirect to the generated file.
OR_BROKER_FORM = ("https://www4.cbs.state.or.us/exs/all/mylicsearch/index.cfm"
                  "?fuseaction=search.download_dfcs_file&download_file=dfcs_db.txt")
OR_BROKER_FORM_DATA = {"group_id": "20", "profession_name": "Data Broker", "profession_id": "28",
                       "profession_sub_id": "28000"}
OR_BROKER_PAGE = "https://dfr.oregon.gov/business/licensing/data-broker-registry"
REGISTRIES = ("CA", "OR")
_COMPANY_SUFFIX = re.compile(r"\b(INCORPORATED|INC|LLC|L L C|CORPORATION|CORP|CO|COMPANY|LTD|LIMITED|LP|LLP|PLLC|PBC)\b")


def broker_key(name: str) -> str:
    """A legal name reduced to compare across registries: case, punctuation and
    the company-form suffix dropped ("Above Data, Inc." matches "ABOVE DATA INC")."""
    s = re.sub(r"[^A-Z0-9 ]+", " ", (name or "").upper().replace("&", " AND "))
    s = _COMPANY_SUFFIX.sub(" ", s)
    return " ".join(s.split())


def parse_or_broker_registry(text: str) -> List[Dict[str, Any]]:
    """Oregon's file -> {licence, name, dba, city, state, issued, expires}.
    Columns: blank, licence, legal name, DBA, street, city, county, state, zip,
    phone, issued (YYYYMMDD), expires."""
    out: List[Dict[str, Any]] = []
    seen = set()
    for line in text.splitlines():
        c = [x.strip() for x in line.split("\t")]
        if len(c) < 12 or not c[1].upper().startswith("DATA-") or not c[2]:
            continue
        if c[1] in seen:
            continue
        seen.add(c[1])

        def day(v: str) -> Optional[str]:
            return f"{v[:4]}-{v[4:6]}-{v[6:8]}" if re.fullmatch(r"\d{8}", v) else None
        out.append({"licence": c[1], "name": c[2], "dba": c[3] or None, "city": c[5] or None,
                    "state": c[7] or None, "issued": day(c[10]), "expires": day(c[11])})
    return out


def add_registry(current: Optional[str], state: str) -> str:
    """The registries a broker is on, as a sorted comma list, e.g. CA,OR."""
    have = {r for r in (current or "").split(",") if r}
    have.add(state)
    return ",".join(sorted(have))


def parse_broker_registry(text: str) -> List[Dict[str, Any]]:
    """California's registry rows -> {name, dba, website, privacy_url, email,
    city, state, flags: {flag: True/False/None}}. Self-declared by each broker."""
    reader = csv.reader(io.StringIO(text.lstrip("\ufeff")))
    head = next(reader, [])
    low = [h.strip().lower() for h in head]

    def col(*needles: str) -> Optional[int]:
        for i, h in enumerate(low):
            if all(n.lower() in h for n in needles):
                return i
        return None

    flag_cols: Dict[str, Optional[int]] = {}
    for k, phrase in BROKER_FLAGS.items():
        verb = "shared or sold" if k.startswith("sold_to") else "collects"
        idx = None
        for i, h in enumerate(low):
            if verb in h and phrase.lower() in h:
                idx = i
                break
        flag_cols[k] = idx
    c_name, c_dba = col("data broker name"), col("doing business as")
    c_web, c_mail = col("primary website"), col("contact email")
    c_priv = col("exercise", "privacy rights") or col("consumer privacy rights")
    c_city, c_state = col("data broker city"), col("data broker state")
    out: List[Dict[str, Any]] = []
    seen = set()
    for r in reader:
        def g(i: Optional[int]) -> Optional[str]:
            return (r[i] or "").strip() or None if i is not None and i < len(r) else None
        name = g(c_name)
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        flags: Dict[str, Optional[bool]] = {}
        for k, i in flag_cols.items():
            v = (g(i) or "").lower()
            flags[k] = True if v.startswith("yes") else False if v.startswith("no") else None
        out.append({"name": name, "dba": g(c_dba), "website": g(c_web), "privacy_url": g(c_priv),
                    "email": g(c_mail), "city": g(c_city), "state": g(c_state), "flags": flags})
    return out


# ── Report cards ─────────────────────────────────────────────────────────────

# What each technology can do to an ordinary person, in a sentence.
TECH_TEXT: Dict[str, str] = {
    "Automated License Plate Readers": "Photographs every passing plate and logs where and when, searchable later, often shared with other agencies.",
    "Face Recognition": "Matches faces from photos or video against databases of mugshots, licences or scraped images.",
    "Drones": "Aerial cameras; drone-as-first-responder programs fly to 911 calls and film on the way.",
    "Cell-site Simulator": "Poses as a cell tower so nearby phones connect, revealing identity and location of everyone in range.",
    "Body-worn Cameras": "Officer-worn video; increasingly paired with face recognition and AI transcription.",
    "Camera Registry": "Residents and businesses register or connect private cameras so police can request or stream footage.",
    "Real-Time Crime Center": "A room that fuses live cameras, plate readers, gunshot alerts and databases.",
    "Gunshot Detection": "Street microphones that listen for gunfire; audits have found most alerts unconfirmed.",
    "Predictive Policing": "Software that forecasts where crime or who will offend, from past police data.",
    "Video Analytics": "Software that searches video for people, vehicles or behaviour automatically.",
    "Fusion Center": "Shares intelligence between local, state and federal agencies.",
    "Third-party Investigative Platforms": "Commercial data platforms: people search, social media monitoring, purchased records.",
}

# Weight for the at-a-glance intensity: how much each can see of people who are
# not suspected of anything (mass, persistent, location-revealing scores higher).
TECH_WEIGHT: Dict[str, int] = {
    "Cell-site Simulator": 3, "Face Recognition": 3, "Automated License Plate Readers": 3,
    "Real-Time Crime Center": 3, "Third-party Investigative Platforms": 2, "Drones": 2, "Camera Registry": 2,
    "Video Analytics": 2, "Predictive Policing": 2, "Fusion Center": 2, "Gunshot Detection": 1,
    "Body-worn Cameras": 1,
}


# What a person can do this week, most effective first; all free. Served by the
# API (not written into the page) so it can change without a frontend release.
PRIVACY_STEPS: List[Dict[str, Any]] = [
    {"id": "ca_drop", "states": ["CA"],
     "title": "Delete yourself from every registered data broker at once (California DROP)",
     "detail": "One free request reaches 500+ registered brokers, who must process it.",
     "link": "https://cppa.ca.gov/data_broker_registry/"},
    {"id": "badbool", "title": "Opt out of the people-search sites that matter most",
     "detail": "The Big Ass Data Broker Opt-Out List marks the crucial ones; start there.",
     "link": "https://github.com/yaelwrites/Big-Ass-Data-Broker-Opt-Out-List"},
    {"id": "ad_id", "title": "Turn off your phone's ad ID and app location access",
     "detail": "Location brokers buy app data, and police have bought it back (Fog Reveal). Delete the ad ID and "
               "give location only to apps that need it, only while in use."},
    {"id": "ring", "title": "Think before sharing doorbell video with police",
     "detail": "Sharing a clip through Ring's Community Requests also hands over your address and account email."},
    {"id": "flocked", "title": "Check whether police searched your plate",
     "detail": "Have I Been Flocked searches Flock audit logs released under public-records law. A hit means a "
               "search, not a sighting; no hit proves little.",
     "link": "https://haveibeenflocked.com"},
    {"id": "records", "title": "Ask your city what it runs",
     "detail": "File a public-records request for its Flock contract, sharing list and audit logs; MuckRock has "
               "templates and tracks replies.",
     "link": "https://www.muckrock.com"},
    {"id": "rayhunter", "title": "Detect cell-site simulators",
     "detail": "EFF's Rayhunter runs on a ~$20 hotspot and flags fake-tower behaviour.",
     "link": "https://github.com/EFForg/rayhunter"},
]


def privacy_steps(state: Optional[str] = None) -> List[Dict[str, Any]]:
    """The steps for someone in `state` (two-letter code): state-only steps
    first where they apply, then the rest, in order."""
    st = (state or "").upper()
    local = [s for s in PRIVACY_STEPS if st and st in s.get("states", [])]
    general = [s for s in PRIVACY_STEPS if not s.get("states")]
    return [{k: v for k, v in s.items() if k != "states"} for s in local + general]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def report_card(rows: List[Dict[str, Any]], nearby_devices: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """One agency's Atlas rows (dicts with technology, vendor, summary, link,
    link_date) -> what it uses, from whom, with evidence, and an intensity
    score (sum of distinct technologies' weights) to compare agencies."""
    techs: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        t = r["technology"]
        e = techs.setdefault(t, {"technology": t, "what_it_means": TECH_TEXT.get(t), "vendors": [], "evidence": []})
        if r.get("vendor") and r["vendor"] not in e["vendors"]:
            e["vendors"].append(r["vendor"])
        e["evidence"].append({"summary": r.get("summary"), "link": r.get("link"),
                              "date": str(r["link_date"]) if r.get("link_date") else None})
    items = sorted(techs.values(), key=lambda e: (-TECH_WEIGHT.get(e["technology"], 1), e["technology"]))
    return {
        "technologies": items,
        "intensity": sum(TECH_WEIGHT.get(t, 1) for t in techs),
        "max_intensity": sum(TECH_WEIGHT.values()),
        "vendors": sorted({v for e in items for v in e["vendors"]}),
        "nearby_devices": nearby_devices or {},
    }


__all__ = [
    "ATLAS_URL", "BROKER_FLAGS", "BROKER_FLAG_TEXT", "CA_BROKER_CSV", "OR_BROKER_FORM", "OR_BROKER_FORM_DATA",
    "OR_BROKER_PAGE", "REGISTRIES", "add_registry", "broker_key", "parse_or_broker_registry", "DEFLOCK_URL", "parse_broker_registry", "DFR_SHEET_CSV", "DroneWaiver", "GRANT_KEYWORDS", "SURVEILLANCE_VENDORS", "USASPENDING_URL", "parse_awards", "parse_dfr", "usaspending_query", "DEVICE_KINDS", "OVERPASS_KINDS", "iter_geojson_features", "iter_json_array", "parse_deflock", "Device", "AgencyTech", "GAZ_COUNTIES_URL", "GAZ_PLACES_URL", "OVERPASS_URLS",
    "TECH_TEXT", "TECH_WEIGHT", "US_REGIONS", "haversine_km", "norm_county", "norm_place", "overpass_query",
    "PRIVACY_STEPS", "iter_atlas", "overpass_outcome", "privacy_steps", "parse_atlas", "parse_gazetteer", "parse_overpass", "report_card", "split", "tiles",
]
