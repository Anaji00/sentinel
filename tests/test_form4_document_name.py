"""
tests/test_form4_document_name.py

A Form 4's document is not always called primary_doc.xml.

The collector derived the document URL as `{filing dir}/primary_doc.xml`.
Probed against the live feed on 1 October, four of six current filings used
`form4.xml` or `doc4.xml` and returned 404: 70 failed fetches in the hour after
a restart, each published as the RSS stub with no ticker, insider or
transactions. The filing used to verify the parser happened to use the
default. The directory's index.json names the real file.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.form4 import document_url, index_json_url, primary_document_name

INDEX_LINK = (
    "https://www.sec.gov/Archives/edgar/data/1747846/"
    "000174784626000012/0001747846-26-000012-index.htm"
)


def _listing(*names, dirs=()):
    items = [{"name": n, "type": "file"} for n in names]
    items += [{"name": d, "type": "dir"} for d in dirs]
    return {"directory": {"item": items}}


def test_the_index_json_sits_beside_the_index_page():
    assert index_json_url(INDEX_LINK) == (
        "https://www.sec.gov/Archives/edgar/data/1747846/000174784626000012/index.json"
    )


def test_each_live_naming_is_found():
    for name in ("form4.xml", "doc4.xml", "primary_doc.xml", "wk-form4_1727800000.xml"):
        listing = _listing("0001747846-26-000012-index.htm", name,
                           "0001747846-26-000012.txt", dirs=("xslF345X05",))
        assert primary_document_name(listing) == name


def test_the_rendered_view_directory_is_not_the_document():
    listing = _listing("0001-index.htm", dirs=("xslF345X05.xml",))
    assert primary_document_name(listing) is None


def test_a_listing_with_no_xml_names_nothing():
    assert primary_document_name(_listing("a-index.htm", "a.txt")) is None
    assert primary_document_name({}) is None
    assert primary_document_name(None) is None
    assert primary_document_name({"directory": {"item": "nope"}}) is None


def test_the_document_url_uses_the_listed_name():
    assert document_url(INDEX_LINK, "form4.xml").endswith(
        "/000174784626000012/form4.xml"
    )


def test_a_name_cannot_climb_out_of_the_filing_directory():
    assert document_url(INDEX_LINK, "../other/form4.xml") is None
    assert document_url(INDEX_LINK, "") is None


def test_placeholder_symbols_are_no_symbol():
    """Three of eight live filings carried N/A or NONE as the symbol, and one
    correlation keyed an insider cluster on "n/a"."""
    from shared.utils.form4 import listed_symbol
    for raw in ("N/A", "n/a", "NONE", " none ", "", None, "-"):
        assert listed_symbol(raw) == "", raw
    assert listed_symbol(" jctc ") == "JCTC"
    assert listed_symbol("BRK.B") == "BRK.B"


def test_one_filing_listed_twice_has_one_accession():
    """The feed lists a filing under the owner's CIK and the issuer's."""
    from shared.utils.form4 import accession_of
    owner = "https://www.sec.gov/Archives/edgar/data/1747846/000091256226000041/0000912562-26-000041-index.htm"
    issuer = "https://www.sec.gov/Archives/edgar/data/912562/000091256226000041/0000912562-26-000041-index.htm"
    assert owner != issuer
    assert accession_of(owner) == accession_of(issuer) == "0000912562-26-000041"
    assert accession_of("weird") == "weird"


def test_the_collector_dedupes_by_accession_and_drops_unlisted_issuers():
    src = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    assert 'f"sentinel:seen:form4:{form4_accession_of(link)}"' in src
    assert 'if parsed and not parsed["ticker"]:' in src


def test_the_collector_reads_the_listing_before_the_document():
    src = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    fetch = src.split("async def _fetch_form4_document")[1].split("\nasync def ")[0]
    assert fetch.index("form4_index_json_url(") < fetch.index("form4_document_url(index_link, name)")


def _doc(flag_value):
    return (
        "<ownershipDocument><issuer><issuerTradingSymbol>SPG</issuerTradingSymbol>"
        "<issuerName>Simon Property Group</issuerName></issuer>"
        "<reportingOwner><reportingOwnerId><rptOwnerName>AEPPEL GLYN</rptOwnerName>"
        "</reportingOwnerId><reportingOwnerRelationship>"
        f"<isDirector>{flag_value}</isDirector><isOfficer>false</isOfficer>"
        "<isTenPercentOwner>0</isTenPercentOwner>"
        "</reportingOwnerRelationship></reportingOwner></ownershipDocument>"
    ).encode()


def test_a_director_written_as_true_is_a_director():
    """SPG writes isDirector as "true"; only "1" was accepted, so the role came
    out empty and the RSS title was published as the role."""
    from shared.utils.form4 import parse_form4
    for value in ("true", "TRUE", "1", " true "):
        owner = parse_form4(_doc(value))["owners"][0]
        assert owner["is_director"] is True, value
        assert owner["is_officer"] is False


def test_false_and_zero_are_not_a_relationship():
    from shared.utils.form4 import parse_form4
    for value in ("false", "0", ""):
        assert parse_form4(_doc(value))["owners"][0]["is_director"] is False, value
