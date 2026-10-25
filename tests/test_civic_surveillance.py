"""Watching the watchers: OpenStreetMap surveillance devices, EFF's Atlas of
Surveillance placed on the map, and agency report cards."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import surveillance as sv  # noqa: E402

OVERPASS = {"elements": [
    {"type": "node", "id": 11, "lat": 33.75, "lon": -84.39,
     "tags": {"man_made": "surveillance", "surveillance:type": "ALPR", "manufacturer": "Flock Safety",
              "camera:direction": "270", "operator": "Atlanta Police Department", "name": "x"}},
    {"type": "node", "id": 12, "lat": 41.88, "lon": -87.63,
     "tags": {"man_made": "surveillance", "surveillance:type": "gunshot_detector", "brand": "ShotSpotter"}},
    {"type": "node", "id": 13, "lat": 1, "lon": 1, "tags": {"man_made": "surveillance", "surveillance:type": "camera"}},
    {"type": "way", "id": 14},
]}

PLACES = ("USPS\tGEOID\tANSICODE\tNAME\tLSAD\tFUNCSTAT\tALAND\tAWATER\tALAND_SQMI\tAWATER_SQMI\tINTPTLAT\tINTPTLONG\n"
          "GA\t1\t1\tAtlanta city\t25\tA\t350000000\t0\t1\t0\t33.76\t-84.42\n"
          "MO\t2\t2\tSt. Louis city\t25\tA\t160000000\t0\t1\t0\t38.63\t-90.24\n"
          "GA\t3\t3\tAtlanta CDP\t57\tS\t1000\t0\t1\t0\t30.0\t-80.0\n")
COUNTIES = ("USPS\tGEOID\tANSICODE\tNAME\tALAND\tAWATER\tALAND_SQMI\tAWATER_SQMI\tINTPTLAT\tINTPTLONG\n"
            "LA\t22071\t1\tOrleans Parish\t439000000\t0\t1\t0\t30.05\t-89.93\n")
ATLAS = ('"AOSNUMBER","NEWAOSNUMBER (ORI9)","City","County","State","Agency","Type of LEA","Summary","Type of Juris",'
         '"Technology","TECH ABV","Vendor","Link 1","Link 1 Snapshot","Link 1 Source","Link 1 Type","Link 1 Date"\n'
         '"AOS1","x","Atlanta","Fulton County","GA","Atlanta Police Department","Police","Uses Flock.","Municipal",'
         '"Automated License Plate Readers","","Flock Safety","https://a.example/1","","","","03/02/2024"\n'
         '"AOS2","x","Saint Louis","","MO","St. Louis Metropolitan Police","Police","Face rec.","Municipal",'
         '"FRT","","","https://a.example/2","","","",""\n'
         '"AOS3","x","","Orleans Parish","LA","Orleans Parish Sheriff","Sheriff","Drones.","County",'
         '"Drones","","DJI","","","","",""\n'
         '"AOS4","x","Nowhere","","ZZ","Ghost PD","Police","?","Municipal","Drones","","","","","","",""\n')


def test_overpass_keeps_plate_readers_and_gunshot_detectors() -> None:
    devs = sv.parse_overpass(OVERPASS)
    assert [(d.osm_id, d.kind) for d in devs] == [(11, "ALPR"), (12, "gunshot_detector")]
    assert devs[0].manufacturer == "Flock Safety" and devs[0].direction == "270" and "name" not in devs[0].tags
    assert devs[1].manufacturer == "ShotSpotter"            # brand when no manufacturer


def test_tiles_cover_the_us_and_split_into_quarters() -> None:
    t = list(sv.tiles())
    assert any(s <= 33.75 < n and w <= -84.39 < e for s, w, n, e in t)
    assert any(s <= 61.2 < n and w <= -149.9 < e for s, w, n, e in t)        # Anchorage
    q = sv.split((0.0, 0.0, 2.0, 2.0))
    assert len(q) == 4 and (1.0, 1.0, 2.0, 2.0) in q
    assert 'surveillance:type"~"^(ALPR|gunshot_detector)$"' in sv.overpass_query((1, 2, 3, 4))


def test_agencies_are_placed_at_their_city_or_county() -> None:
    places, counties = sv.parse_gazetteer(PLACES), sv.parse_gazetteer(COUNTIES, county=True)
    assert places[("GA", "atlanta")] == (33.76, -84.42)          # the city outranks the tiny CDP
    rows = {r.aos_id: r for r in sv.parse_atlas(ATLAS, places, counties)}
    assert (rows["AOS1"].lat, rows["AOS1"].geo_level) == (33.76, "place")
    assert rows["AOS1"].link_date == date(2024, 3, 2) and rows["AOS1"].vendor == "Flock Safety"
    assert rows["AOS2"].geo_level == "place" and rows["AOS2"].technology == "Face Recognition"
    assert (rows["AOS3"].lat, rows["AOS3"].geo_level) == (30.05, "county")
    assert rows["AOS4"].lat is None and rows["AOS4"].geo_level is None


def test_report_card_lists_the_most_invasive_first_with_evidence() -> None:
    rows = [{"technology": "Body-worn Cameras", "vendor": "Axon", "summary": "a", "link": "l1", "link_date": None},
            {"technology": "Face Recognition", "vendor": "Clearview AI", "summary": "b", "link": "l2",
             "link_date": date(2025, 1, 1)},
            {"technology": "Face Recognition", "vendor": "Clearview AI", "summary": "c", "link": "l3", "link_date": None}]
    card = sv.report_card(rows, {"ALPR": 12})
    assert [t["technology"] for t in card["technologies"]] == ["Face Recognition", "Body-worn Cameras"]
    fr = card["technologies"][0]
    assert fr["vendors"] == ["Clearview AI"] and len(fr["evidence"]) == 2 and fr["evidence"][0]["date"] == "2025-01-01"
    assert fr["what_it_means"].startswith("Matches faces")
    assert card["intensity"] == 4 and card["nearby_devices"] == {"ALPR": 12}
    assert card["vendors"] == ["Axon", "Clearview AI"]


def test_distance() -> None:
    assert abs(sv.haversine_km(33.75, -84.39, 33.75, -84.39)) < 1e-9
    assert 930 < sv.haversine_km(33.75, -84.39, 41.88, -87.63) < 960          # Atlanta to Chicago, ~945 km


def test_overpass_busy_is_not_mistaken_for_a_dense_tile() -> None:
    busy = b"<html>504 Gateway Time-out ... Dispatcher_Client::request_read_and_idx::timeout</html>"
    assert sv.overpass_outcome(504, busy, 1.2) == "busy"             # quick refusal: back off, same tile
    assert sv.overpass_outcome(None, b"", 3.0) == "busy"             # connection dropped
    assert sv.overpass_outcome(504, busy, 120.0) == "dense"          # worked on it, then gave up: split
    assert sv.overpass_outcome(None, b"", 150.0) == "dense"
    partial = b'{"version":0.6,"remark":"runtime error: Query timed out in \"query\" at line 1","elements":[]}'
    assert sv.overpass_outcome(200, partial, 140.0) == "dense"       # a partial answer is not complete
    assert sv.overpass_outcome(200, b'{"version":0.6,"elements":[]}', 2.0) == "ok"


def test_privacy_steps_are_state_aware_and_honest() -> None:
    ca, tx = sv.privacy_steps("ca"), sv.privacy_steps("TX")
    assert ca[0]["id"] == "ca_drop" and not any(s["id"] == "ca_drop" for s in tx)
    assert all("states" not in s for s in ca)
    ring = next(s for s in tx if s["id"] == "ring")
    assert "address" in ring["detail"]
    assert "A hit means a search, not a sighting" in next(s for s in tx if s["id"] == "flocked")["detail"]


def test_deflock_file_streams_feature_by_feature() -> None:
    import io
    import json
    feats = [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-101.92, 35.16]},
         "properties": {"osmId": 31431226, "osmType": "node", "operator": "Amarillo Police Department",
                        "brand": "Flock Safety", "direction": 270, "surveillanceZone": "traffic", "mountType": "pole"}},
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-83.15, 34.28]},
         "properties": {"osmId": 9232283, "osmType": "way", "brand": "Flock Safety"}},
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-122.15, 37.61]},
         "properties": {"osmId": 33219729, "osmType": "node", "operator": "Bay Area Toll Authority",
                        "directions": "90;270", "note": "x" * 300}},
    ]
    raw = json.dumps({"type": "FeatureCollection", "features": feats}).encode()
    got = list(sv.iter_geojson_features(io.BytesIO(raw), chunk=37))      # objects split across reads
    assert got == feats
    devs = [sv.parse_deflock(f) for f in got]
    assert devs[1] is None                                                 # ways skipped
    a, b = devs[0], devs[2]
    assert (a.osm_id, a.kind, a.manufacturer, a.direction) == (31431226, "ALPR", "Flock Safety", "270")
    assert (a.lat, a.lon) == (35.16, -101.92) and a.tags["source"] == "deflock" and a.tags["mountType"] == "pole"
    assert b.manufacturer is None and b.direction == "90;270" and "note" not in b.tags
    assert list(sv.iter_geojson_features(io.BytesIO(b'{"type":"FeatureCollection","features":[]}'))) == []


def test_drone_waivers_parse_from_effs_sheet() -> None:
    text = ("Applicant,City,State,Category,Responsible Party,Date Received,Completed,Status,200ft/400ft,Start Date\n"
            "Richmond Police Department,Richmond,CA,Law enforcement,Exemption 6,4/15/2025,5/9/2025,Approved,400ft,5/8/2025\n"
            "Richmond Police Department,Richmond,CA,Law enforcement,x,4/15/2025,,Approved,400ft,\n"
            ",Nowhere,TX,x,x,,,,,\n"
            "County Fire,Gilroy,ca,Fire,x,1/2/2026,,Pending,200ft,\n")
    w = sv.parse_dfr(text)
    assert [(x.applicant, x.state) for x in w] == [("Richmond Police Department", "CA"), ("County Fire", "CA")]
    assert w[0].received == date(2025, 4, 15) and w[0].status == "Approved" and w[0].altitude == "400ft"
    assert w[1].completed is None and w[1].category == "Fire"


def test_usaspending_queries_and_awards() -> None:
    q = sv.usaspending_query("contract", "Flock Group", 2)
    assert q["filters"]["recipient_search_text"] == ["Flock Group"] and q["filters"]["award_type_codes"] == ["A", "B", "C", "D"]
    assert q["page"] == 2 and q["sort"] == "Award Amount"
    g = sv.usaspending_query("grant", "license plate reader", 1)
    assert g["filters"]["keywords"] == ["license plate reader"] and "recipient_search_text" not in g["filters"]
    body = {"results": [{"generated_internal_id": "CONT_AWD_1", "Award ID": "140D", "Recipient Name": "FLOCK GROUP INC",
                         "Award Amount": 433600.0, "Awarding Agency": "Department of the Interior",
                         "Awarding Sub Agency": "Departmental Offices", "Start Date": "2025-09-25",
                         "Description": "USPP ALPR"},
                        {"Award ID": None}]}
    rows = sv.parse_awards(body, "contract", "Flock Group")
    assert rows == [("CONT_AWD_1", "contract", "Flock Group", "FLOCK GROUP INC", 433600.0, "Department of the Interior",
                     "Departmental Offices", date(2025, 9, 25), "USPP ALPR")]


def test_broker_registry_reads_declared_collection_and_sales() -> None:
    head = ['Data broker name:', 'Doing Business As (DBA), if applicable:', 'Data broker primary website:',
            'Data broker primary contact email address:', 'Data broker city:', 'Data broker state:',
            "Data broker's primary website that contains details on how consumers can exercise their CA Consumer Privacy rights:",
            "Data broker collects consumers' precise geolocation",
            "Data broker collects consumers' citizenship data, including immigration status",
            "Data broker collects consumers’ government‑issued identification numbers used to verify an individual’s identity",
            "Data broker shared or sold consumers' data to the federal government in the past year",
            'Data broker shared or sold consumers’ data to law enforcement in the past year, except when required by subpoena or court order']
    import csv as _csv
    import io as _io
    buf = _io.StringIO()
    w = _csv.writer(buf)
    w.writerow(head)
    w.writerow(["Fog Data science LLC", "", "https://fogdatascience.com", "privacy@x", "Leesburg", "VA",
                "https://fogdatascience.com/privacy", "Yes", "No", "No", "Yes", "Yes"])
    w.writerow(["fog data science llc", "", "", "", "", "", "", "Yes", "", "", "", ""])          # duplicate name
    w.writerow(["Quiet Co", "QC", "", "", "", "", "", "No", "No", "No", "No", ""])
    rows = sv.parse_broker_registry("﻿" + buf.getvalue())
    assert [r["name"] for r in rows] == ["Fog Data science LLC", "Quiet Co"]
    f = rows[0]["flags"]
    assert f["precise_geolocation"] and f["sold_to_law_enforcement"] and f["sold_to_federal_government"]
    assert f["immigration_status"] is False and f["government_ids"] is False
    assert rows[0]["privacy_url"] == "https://fogdatascience.com/privacy"
    assert rows[1]["flags"]["sold_to_law_enforcement"] is None and rows[1]["dba"] == "QC"   # unanswered stays unknown


def test_oregon_registry_file_parses_and_joins_california_by_legal_name() -> None:
    from shared.utils import surveillance as svx
    tab = "\t"
    line = tab.join([" ", "DATA-00447", " ABOVE DATA INC   ", "Above Data  ", "30 Oak Knoll Dr", "San Anselmo",
                     "CA", "CA", "94960", "9372433708", "20260421", "20261231", ""])
    text = line + "\r\n" + line + "\r\nfooter line\r\n"
    rows = svx.parse_or_broker_registry(text)
    assert rows == [{"licence": "DATA-00447", "name": "ABOVE DATA INC", "dba": "Above Data", "city": "San Anselmo",
                     "state": "CA", "issued": "2026-04-21", "expires": "2026-12-31"}]
    assert svx.broker_key("Above Data, Inc.") == svx.broker_key("ABOVE DATA INC") == "ABOVE DATA"
    assert svx.broker_key("Acxiom L.L.C.") == "ACXIOM"
    assert svx.add_registry("CA", "OR") == "CA,OR"
    assert svx.add_registry("CA,OR", "OR") == "CA,OR"
    assert svx.add_registry(None, "OR") == "OR"
