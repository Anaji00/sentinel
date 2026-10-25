"""Institutional flows rebuilt from 13F history, as the live loop computes them."""

import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "services" / "collector-filings"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _mod():
    spec = importlib.util.spec_from_file_location(
        "ownership_history", ROOT / "services" / "collector-filings" / "ownership_history.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_three_active_managers_adding_make_an_accumulation_stamped_when_the_last_filed() -> None:
    m = _mod()
    names = ["FMR LLC", "T. Rowe Price Associates", "Wellington Management Group", "Berkshire Hathaway Inc"]
    q1 = {n: ("2025-05-10", {"AAA": 100.0, "BBB": 100.0}) for n in names}
    q2 = {n: ("2025-08-1" + str(i), {"AAA": 200.0, "BBB": 100.0}) for i, n in enumerate(names)}
    out = m.flows({"2025-03-31": q1, "2025-06-30": q2})
    assert [(f["ticker"], f["flow"], f["known"]) for f in out] == [("AAA", 1, "2025-08-13")]
    assert len(out[0]["names"]) == 4


def test_a_filer_missing_the_previous_quarter_is_not_counted() -> None:
    m = _mod()
    q1 = {"FMR LLC": ("2025-05-10", {"AAA": 100.0})}
    q2 = {n: ("2025-08-10", {"AAA": 200.0}) for n in ("FMR LLC", "T. Rowe Price Associates", "Wellington Management Group")}
    assert m.flows({"2025-03-31": q1, "2025-06-30": q2}) == []


def test_issuer_kind_separates_foreign_filers_and_partnerships() -> None:
    spec = importlib.util.spec_from_file_location("history", ROOT / "services" / "collector-filings" / "history.py")
    h = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(h)

    def p(name, forms):
        return {"name": name, "filings": {"recent": {"form": forms}}}

    assert h.issuer_kind(p("Taiwan Semiconductor Manufacturing Co Ltd", ["6-K", "20-F"])) == "foreign"
    assert h.issuer_kind(p("Shopify Inc.", ["40-F", "6-K"])) == "foreign"
    assert h.issuer_kind(p("Apple Inc.", ["10-K", "10-Q", "8-K"])) == "domestic"
    assert h.issuer_kind(p("Arm Holdings plc", ["10-K", "20-F"])) == "domestic"       # files 10-K too
    assert h.issuer_kind(p("Enterprise Products Partners L.P.", ["10-K"])) == "partnership"
    assert h.issuer_kind(p("Energy Transfer LP", ["10-K"])) == "partnership"
    assert h.issuer_kind(p("Pinnacle Financial Partners, Inc.", ["10-K"])) == "domestic"
    row = h.profile_row("TSM", "1046179", {**p("TSMC", ["20-F"]), "sic": "3674", "sicDescription": "Semis",
                                           "stateOfIncorporation": "F5"})
    assert row[-2:] == ("foreign", "F5")
