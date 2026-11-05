"""The SEC bulk parsers, and the public dates that keep readers from look-ahead."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import sec_holdings as sh  # noqa: E402

FTD = """SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE
20261001|B5950S113|MDXH|630|MDXHEALTH SA|0.58
20261001|B5950S113|MDXH|10|MDXHEALTH SA|0.58
20261002|X|BAD|notanumber|X|1
20261002|Y||5|NO SYMBOL|1
20261002|Z|NOPRICE|7|Z|.
Trailer record count 51410
"""


def test_fails_parse_sums_repeats_and_skips_noise() -> None:
    rows = {r[1]: r for r in sh.parse_ftd(FTD)}
    assert set(rows) == {"MDXH", "NOPRICE"}
    assert rows["MDXH"][3] == 640 and rows["MDXH"][0] == date(2026, 10, 1)
    assert rows["NOPRICE"][4] is None


def test_fails_become_public_on_the_sec_schedule() -> None:
    assert sh.ftd_public_on(date(2026, 10, 15)) == date(2026, 10, 31)
    assert sh.ftd_public_on(date(2026, 10, 16)) == date(2026, 11, 15)
    assert sh.ftd_public_on(date(2026, 12, 20)) == date(2027, 1, 15)


def test_an_amendment_replaces_its_original() -> None:
    subs = [
        {"ACCESSION_NUMBER": "a", "REPORT_DATE": "31-AUG-2026", "FILING_DATE": "24-OCT-2026"},
        {"ACCESSION_NUMBER": "b", "REPORT_DATE": "31-AUG-2026", "FILING_DATE": "01-NOV-2026"},
        {"ACCESSION_NUMBER": "c", "REPORT_DATE": "bad", "FILING_DATE": "01-NOV-2026"},
    ]
    funds = [{"ACCESSION_NUMBER": x, "SERIES_ID": "S1"} for x in "abc"]
    assert sh.latest_filings(subs, funds) == {"b": (date(2026, 8, 1), date(2026, 11, 1))}


def test_only_long_common_stock_in_shares_counts() -> None:
    filings = {"b": (date(2026, 8, 1), date(2026, 11, 1))}
    base = {"ACCESSION_NUMBER": "b", "ISSUER_CUSIP": "c1", "BALANCE": "100", "CURRENCY_VALUE": "500",
            "ASSET_CAT": "EC", "PAYOFF_PROFILE": "Long", "UNIT": "NS", "EXCHANGE_RATE": ""}
    rows = [base, {**base, "PAYOFF_PROFILE": "Short"}, {**base, "ASSET_CAT": "DBT"}, {**base, "UNIT": "PA"},
            {**base, "ISSUER_CUSIP": "unknown"}, {**base, "ACCESSION_NUMBER": "old"},
            {**base, "CURRENCY_VALUE": "1000", "EXCHANGE_RATE": "2"}]
    out = sh.aggregate_holdings(rows, filings, {"C1": "AAA"})
    assert out == {(date(2026, 8, 1), "AAA"): [2, 200.0, 1000.0, date(2026, 11, 1)]}


def test_links_and_retries() -> None:
    page = '<a href="/files/x/cnsfails202610a.zip"><a href="/files/x/cnsfails202610a.zip"><a href="/files/x/2026q3_nport.zip">'
    assert sh.file_links(page, "cnsfails") == ["https://www.sec.gov/files/x/cnsfails202610a.zip"]
    assert sh.ftd_year("x/cnsfails202610a.zip") == 2026
    assert sh.nport_quarter("x/2026q3_nport.zip") == "2026q3"
    assert sh.retry_allowed(2) and not sh.retry_allowed(3) and sh.retry_allowed(None)
