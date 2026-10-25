"""The daily Treasury par yield curve: one source and one time scale for every maturity."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import treasury as tr  # noqa: E402


def _entry(day: str, two: str, ten: str, thirty: str) -> str:
    return (f'<entry><content type="application/xml"><m:properties>'
            f'<d:NEW_DATE m:type="Edm.DateTime">{day}T00:00:00</d:NEW_DATE>'
            f'<d:BC_2YEAR m:type="Edm.Double">{two}</d:BC_2YEAR>'
            f'<d:BC_10YEAR m:type="Edm.Double">{ten}</d:BC_10YEAR>'
            f'<d:BC_30YEAR m:type="Edm.Double">{thirty}</d:BC_30YEAR>'
            f'</m:properties></content></entry>')


def _feed(*entries: str) -> str:
    return ('<feed xmlns="http://www.w3.org/2005/Atom" '
            'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" '
            'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices">' + "".join(entries) + "</feed>")


def test_every_maturity_comes_from_the_same_days_oldest_first() -> None:
    out = tr.parse_par_yields(_feed(_entry("2026-10-14", "4.82", "5.19", "5.52"),
                                    _entry("2026-10-13", "4.79", "5.17", "5.50")))
    assert [p["date"] for p in out["US02Y"]] == ["2026-10-13", "2026-10-14"]
    assert out["US10Y"][-1] == {"date": "2026-10-14", "yield": 5.19}
    assert out["US30Y"][0]["yield"] == 5.5


def test_a_missing_maturity_drops_only_that_point_and_bad_xml_is_empty() -> None:
    out = tr.parse_par_yields(_feed(_entry("2026-10-14", "4.82", "", "5.52")))
    assert len(out["US02Y"]) == 1 and out["US10Y"] == [] and len(out["US30Y"]) == 1
    assert tr.parse_par_yields("<not xml") == {"US02Y": [], "US10Y": [], "US30Y": []}


def test_years_merge_by_date_and_keep_the_latest_days() -> None:
    last_year = tr.parse_par_yields(_feed(_entry("2025-12-31", "4.0", "4.5", "4.9")))
    this_year = tr.parse_par_yields(_feed(_entry("2026-01-02", "4.1", "4.6", "5.0"),
                                          _entry("2026-01-05", "4.2", "4.7", "5.1")))
    out = tr.merge_years(last_year, this_year, days=2)
    assert [p["date"] for p in out["US10Y"]] == ["2026-01-02", "2026-01-05"]
    assert len(tr.merge_years(last_year, this_year, days=10)["US02Y"]) == 3
