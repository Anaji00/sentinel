"""Is it legal here? Agencies' published Flock practice checked against state
licence-plate-reader law; ICE 287(g) agreements; the stdlib xlsx reader."""

import importlib.util
import io
import json
import pathlib
import sys
import zipfile
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import civic_law as cl  # noqa: E402
from shared.utils import surveillance as sv  # noqa: E402
from shared.utils.xlsx_lite import read_rows  # noqa: E402

_spec = importlib.util.spec_from_file_location("civic_main", ROOT / "services" / "collector-civic" / "main.py")


def _collector():
    m = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(m)
    return m


def test_organisation_names_place_themselves_in_a_state() -> None:
    assert cl.org_state("Acworth GA PD") == "GA"
    assert cl.org_state("Arlington PD (WA)") == "WA"
    assert cl.org_state("Bardstown PD KY") == "KY"
    assert cl.org_state("Arizona Department of Public Safety") == "AZ"
    assert cl.org_state("[Federal] US Postal Inspection Service") is None
    assert cl.is_federal("[Federal] Federal Bureau of Investigation (FBI)") and not cl.is_federal("Marshall TX PD")
    assert cl.org_state("Some Unnamed Task Force") is None
    # Agency-type abbreviations that collide with state codes.
    assert cl.org_state("Los Angeles County CA SD") == "CA"          # sheriff's department, not South Dakota
    assert cl.org_state("Summerset SD PD") == "SD"
    assert cl.org_state("Cameron CO TX SO") == "TX"                  # CO = county
    assert cl.org_state("RIV CO ARSON TF CA PD") == "CA"
    assert cl.org_state("Richland Township (Bucks CO) PA PD") == "PA"
    assert cl.org_state("MN - Isanti CO SO") == "MN"
    assert cl.org_state("Avon CO PD") == "CO"                        # Colorado when it stands alone


def test_california_out_of_state_sharing_is_flagged_with_the_ag_reading() -> None:
    portal = {"state": "CA", "data_retention": 30,
              "organizations_shared_with": ["Alhambra CA PD", "Acworth GA PD", "Amarillo TX PD", "[Federal] FBI"],
              "prohibited_uses": "Immigration enforcement, harassment."}
    found = {f.code: f for f in cl.check_portal(portal, cl.STATE_LAWS["CA"])}
    oos = found["out_of_state_sharing"]
    assert oos.severity == "conflict" and oos.evidence["count"] == 2 and oos.evidence["states"] == ["GA", "TX"]
    assert "Attorney General" in oos.detail and oos.source == cl.CA_AG_URL
    assert found["federal_sharing"].severity == "conflict" and found["federal_sharing"].evidence["count"] == 1
    assert "retention_over_limit" not in found                        # California sets no statewide limit


def test_retention_over_a_state_limit_is_a_conflict() -> None:
    f = cl.check_portal({"state": "CT", "data_retention": 30, "organizations_shared_with": []}, cl.STATE_LAWS["CT"])
    assert [x.code for x in f] == ["retention_over_limit"]
    assert f[0].evidence == {"retention_days": 30, "limit_days": 21} and "21 days" in f[0].detail
    assert cl.check_portal({"state": "CT", "data_retention": 21, "organizations_shared_with": []},
                           cl.STATE_LAWS["CT"]) == []
    # New Hampshire: three minutes.
    assert cl.check_portal({"state": "NH", "data_retention": 7}, cl.STATE_LAWS["NH"])[0].code == "retention_over_limit"


def test_connecticut_allows_its_neighbours_with_declarations() -> None:
    portal = {"state": "CT", "data_retention": 21, "organizations_shared_with": ["Albany NY PD", "Austin TX PD"]}
    f = {x.code: x for x in cl.check_portal(portal, cl.STATE_LAWS["CT"])}
    assert f["out_of_state_sharing"].evidence["states"] == ["TX"]      # New York is permitted
    assert f["out_of_state_sharing"].severity == "check"              # declarations may be on file


def test_illinois_asks_for_declarations_and_immigration_limits() -> None:
    portal = {"state": "IL", "data_retention": 30,
              "organizations_shared_with": ["Gary IN PD", "[Federal] Indiana HIDTA"], "prohibited_uses": "Personal use."}
    f = {x.code: x for x in cl.check_portal(portal, cl.STATE_LAWS["IL"])}
    assert f["out_of_state_declarations"].severity == "check"
    assert f["federal_sharing"].severity == "check"
    assert f["policy_omits_immigration"].severity == "check"


def test_states_without_a_statute_produce_nothing_but_287g() -> None:
    portal = {"state": "TX", "data_retention": 365, "organizations_shared_with": ["Acworth GA PD"]}
    assert cl.check_portal(portal, cl.STATE_LAWS.get("TX")) == []
    row = {"agency": "Harris County Sheriff's Office", "support_type": "Task Force Model", "signed": date(2025, 2, 27),
           "moa": "https://www.ice.gov/x.pdf"}
    f = cl.check_portal(portal, None, [row])
    assert f[0].code == "ice_287g" and "Task Force Model" in f[0].detail and "2025-02-27" in f[0].detail


def test_agency_names_match_across_sources() -> None:
    assert cl.norm_agency("Sarasota County FL SO") == cl.norm_agency("Sarasota County Sheriff's Office")
    assert cl.norm_agency("El Cajon CA PD") == cl.norm_agency("El Cajon Police Department")
    m = _collector()
    agreements = {("FL", cl.norm_agency("Sarasota County Sheriff's Office")): [{"agency": "x"}]}
    assert m.portal_agreements({"city": "Sarasota County", "type": "SO"}, "FL", agreements) == [{"agency": "x"}]
    assert m.portal_agreements({"city": "Sarasota", "type": "PD"}, "FL", agreements) == []


def _xlsx(rows):
    strings, sheet_rows = [], []
    for ri, row in enumerate(rows, 1):
        cells = []
        for ci, v in enumerate(row):
            ref = f"{chr(65 + ci)}{ri}"
            if v is None:
                continue
            if isinstance(v, str):
                strings.append(v)
                cells.append(f'<c r="{ref}" t="s"><v>{len(strings) - 1}</v></c>')
            else:
                cells.append(f'<c r="{ref}"><v>{v}</v></c>')
        sheet_rows.append(f'<row r="{ri}">{"".join(cells)}</row>')
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/sharedStrings.xml", f'<sst xmlns="{ns}">' + "".join(f"<si><t>{s}</t></si>" for s in strings) + "</sst>")
        z.writestr("xl/worksheets/sheet1.xml", f'<worksheet xmlns="{ns}"><sheetData>{"".join(sheet_rows)}</sheetData></worksheet>')
    return buf.getvalue()


def test_287g_spreadsheet_reads_with_the_standard_library() -> None:
    data = _xlsx([["STATE", "LAW ENFORCEMENT AGENCY", "TYPE", "COUNTY", "SUPPORT TYPE", "SIGNED", "MOA"],
                  ["FLORIDA", "Sarasota County Sheriff's Office", "County", "Sarasota County", "Warrant Service Officer",
                   43591, "https://www.ice.gov/doclib/287gMOA/x.pdf"],
                  ["ALABAMA", "Ashford Police Department", "Municipality", None, "Task Force Model", 46289, "link pending"],
                  ["NOWHERE", "Ghost PD", "Municipality", None, "Task Force Model", 1, None]])
    rows = read_rows(data)
    assert rows[1][1] == "Sarasota County Sheriff's Office" and rows[2][3] is None
    parsed = _collector().parse_287g(rows)
    assert parsed[0] == ("FL", "Sarasota County Sheriff's Office", "County", "Sarasota County",
                         "Warrant Service Officer", date(2019, 5, 6), "https://www.ice.gov/doclib/287gMOA/x.pdf")
    assert parsed[1][0] == "AL" and parsed[1][6] is None and len(parsed) == 2


def test_portal_stream_survives_a_character_split_across_reads() -> None:
    doc = {"summary": {"x": 1}, "portals": [{"slug": "a", "city": "Peñasco"}, {"slug": "b", "city": "Ñ"}]}
    raw = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    for chunk in (3, 5, 7, 11):
        assert list(sv.iter_json_array(io.BytesIO(raw), "portals", chunk)) == doc["portals"]


def test_records_request_asks_for_what_would_settle_each_finding() -> None:
    findings = [{"code": "out_of_state_sharing", "title": "Shares plate data with out-of-state agencies",
                 "detail": "Shares with 341 agencies in 40 other states.", "citation": "Civ. Code §1798.90.5"},
                {"code": "ice_287g", "title": "Has a 287(g) agreement", "detail": "Task Force Model.",
                 "citation": "8 U.S.C. §1357(g)"}]
    r = cl.records_request("El Cajon PD", "CA", findings, "2026-10-10")
    assert r["subject"].endswith("El Cajon PD")
    assert any("declarations" in a for a in r["asks"]) and any("287(g)" in a for a in r["asks"])
    assert not any("federal agency" in a for a in r["asks"])           # no federal finding, no federal ask
    assert "Shares with 341 agencies" in r["body"] and "[Your name]" in r["body"]
    assert "does not send" in r["note"]
    plain = cl.records_request("Somewhere PD", "TX", [], "2026-10-10")
    assert len(plain["asks"]) == 5 and "Context" not in plain["body"]


def test_a_finding_resting_on_an_ambiguous_code_is_only_a_question() -> None:
    portal = {"state": "CA", "organizations_shared_with": ["18th Judicial District DA's Office- LA PD"]}
    f = cl.check_portal(portal, cl.STATE_LAWS["CA"])[0]
    assert f.code == "out_of_state_sharing" and f.severity == "check" and "may be ambiguous" in f.detail
    portal = {"state": "MD", "organizations_shared_with": ["Delaware State Police"]}
    assert cl.check_portal(portal, cl.STATE_LAWS["MD"])[0].severity == "conflict"


def test_reason_classifier_counts_only_clear_matches() -> None:
    assert cl.classify_reason("ICE ASSIST") == {"immigration": ["ICE"]}
    assert cl.classify_reason("assist I.C.E. with locate")["immigration"] == ["ICE"]
    assert cl.classify_reason("Immigration status check")["immigration"] == ["immigration"]
    assert set(cl.classify_reason("HSI / CBP request")["immigration"]) == {"CBP", "HSI"}
    assert cl.classify_reason("287(g) task force")["immigration"] == ["287(g)"]
    # Not immigration: lower-case ice, words containing it, the month of March.
    assert cl.classify_reason("vehicle slid on black ice") == {}
    assert cl.classify_reason("PRICE CHOPPER theft; police office") == {}
    assert cl.classify_reason("March burglary series") == {}
    assert cl.classify_reason("protest at city hall") == {"first_amendment": ["protest"]}
    assert cl.classify_reason("journalist harassment report")["first_amendment"] == ["journalist"]
    assert cl.classify_reason("Flock demonstration for council") == {}       # a product demo, not a protest
    assert cl.classify_reason("demonstrators blocking road")["first_amendment"] == ["demonstrators"]
    assert cl.classify_reason(None) == {} and cl.classify_reason("") == {}


def test_audit_summary_keeps_counts_not_reasons() -> None:
    from datetime import datetime, timezone
    m = _collector()
    headers = ["id", "userId", "searchDate", "cameraCount", "reason", "networkCount"]
    rows = [["a", "***", "2026-09-21T19:03:48.787Z", None, "ICE assist", 562],
            ["b", "***", "2026-09-20 10:00:00.000", None, "Theft", 2],
            ["c", "***", "2026-09-19T10:00:00Z", None, "", 150],
            ["d", "***", "2026-09-18T10:00:00Z", None, "protest downtown", 3],
            ["e", "***", "2025-01-01T00:00:00Z", None, "ICE old", 900]]
    acc = m.summarize_audit(rows, headers, datetime(2026, 6, 1, tzinfo=timezone.utc))
    assert acc["searches"] == 4 and acc["immigration_n"] == 1 and acc["immigration"] == {"ICE": 1}
    assert acc["first_amendment_n"] == 1 and acc["wide"] == 2 and acc["no_reason"] == 1
    assert "ICE assist" not in json.dumps(acc, default=str)          # reason text is never kept
    assert acc["first"].year == 2026 and acc["last"].day == 21


def test_immigration_searches_are_flagged_where_the_law_restricts_them() -> None:
    stats = {"immigration_hits": 12, "immigration_terms": {"ICE": 10, "HSI": 2}, "days": 90}
    wa = cl.check_audit(stats, cl.STATE_LAWS["WA"])
    assert wa[0].code == "immigration_searches" and wa[0].severity == "conflict" and "ICE 10" in wa[0].detail
    assert cl.check_audit(stats, cl.STATE_LAWS["IL"])[0].severity == "check"
    assert cl.check_audit(stats, cl.STATE_LAWS["GA"]) == []          # no immigration rule
    assert cl.check_audit(stats, None) == []
    assert cl.check_audit({"immigration_hits": 0}, cl.STATE_LAWS["WA"]) == []


def test_departures_need_a_full_pass() -> None:
    assert _collector().MIN_PORTALS_FOR_DEPARTURES >= 1000
