"""Insider trades from the SEC data sets, routine or opportunistic."""

import importlib.util
import io
import pathlib
import sys
import zipfile
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import insider_history as ih  # noqa: E402
from shared.utils import signal_families as sf  # noqa: E402


def _zip() -> bytes:
    sub = ("ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\tAFF10B5ONE\n"
           "A1\t03-MAR-2025\t28-FEB-2025\t4\t0000320193\tApple\tAAPL\t0\n"
           "A2\t05-MAR-2025\t04-MAR-2025\t4\t0000001\tCo\tBRK-B\ttrue\n"
           "A3\t05-MAR-2025\t04-MAR-2025\t3\t0000001\tCo\tXYZ\t0\n")
    own = ("ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\tRPTOWNER_TITLE\n"
           "A1\t0000111\tDoe Jane\tDirector,Officer\tCEO\nA2\t0000222\tRoe Rick\tOfficer\tCFO\nA3\t0000333\tX\tDirector\t\n")
    tr = ("ACCESSION_NUMBER\tNONDERIV_TRANS_SK\tTRANS_DATE\tTRANS_CODE\tTRANS_SHARES\tTRANS_PRICEPERSHARE\t"
          "TRANS_ACQUIRED_DISP_CD\tEQUITY_SWAP_INVOLVED\n"
          "A1\t1\t28-FEB-2025\tP\t1000\t200.5\tA\t0\n"
          "A1\t2\t28-FEB-2025\tA\t5000\t0\tA\t0\n"           # a grant: not open market
          "A2\t3\t04-MAR-2025\tS\t300\t450\tD\t0\n"
          "A3\t4\t04-MAR-2025\tP\t10\t5\tA\t0\n")            # a Form 3: skipped
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("SUBMISSION.tsv", sub)
        z.writestr("REPORTINGOWNER.tsv", own)
        z.writestr("NONDERIV_TRANS.tsv", tr)
    return buf.getvalue()


def test_a_quarter_parses_to_open_market_trades_with_owner_and_plan_flag() -> None:
    trades = ih.parse_quarter(_zip())
    assert [(t.ticker, t.code, t.owner_cik, t.planned) for t in trades] == [("AAPL", "P", "111", False),
                                                                           ("BRK.B", "S", "222", True)]
    assert trades[0].filed == date(2025, 3, 3) and trades[0].price == 200.5
    assert ih.sec_date("31-OCT-2025") == date(2025, 10, 31) and ih.sec_date("") is None


def test_routine_means_the_same_month_three_years_running_and_plans_are_routine() -> None:
    d = date(2025, 3, 10)
    routine = [date(2022, 3, 2), date(2023, 3, 5), date(2024, 3, 9), date(2024, 8, 1)]
    opportunistic = [date(2022, 1, 2), date(2023, 6, 5), date(2024, 11, 9)]
    assert ih.classify(d, routine) == "routine"
    assert ih.classify(d, opportunistic) == "opportunistic"
    assert ih.classify(d, [date(2023, 3, 1), date(2024, 3, 1)]) is None        # under three years
    assert ih.classify(d, opportunistic, planned=True) == "routine"


def test_the_families_read_history_and_live_tags_alike() -> None:
    row = {"type": "insider_trade", "source": ih.ARCHIVE_SOURCE, "event_id": "f345:A1:111:P",
           "created_at": ih.known_at(date(2025, 3, 3)), "tags": ih.archive_tags("P", "opportunistic", False),
           "financial_data": {"ticker": "AAPL"}}
    fams = [(o.family, o.direction) for o in sf.observations(row)]
    assert fams == [("insider_opportunistic_buy", 1)]
    row["tags"] = ih.archive_tags("S", "routine", True)
    assert sf.observations(row) == []                                        # routine sales are not studied
    assert ih.known_at(date(2025, 3, 3)) == datetime(2025, 3, 4, 5, 0, tzinfo=timezone.utc)


def test_archive_rows_keep_opportunistic_trades_and_routine_buys() -> None:
    spec = importlib.util.spec_from_file_location("insider_loader", ROOT / "services" / "collector-filings" / "insider_history.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    trades = ih.parse_quarter(_zip())
    classes = {(trades[0].accession, trades[0].line, trades[0].owner_cik): "routine",
               (trades[1].accession, trades[1].line, trades[1].owner_cik): "routine"}
    rows = mod._archive_rows(trades, classes, date(2020, 1, 1))
    assert [r[0] for r in rows] == ["f345:A1:111:P"]                         # routine buy kept, routine sale not
    assert "insider_routine" in rows[0][5] and rows[0][6] == "AAPL"


def _loader():
    spec = importlib.util.spec_from_file_location("insider_loader", ROOT / "services" / "collector-filings" / "insider_history.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_quarter_bounds_and_the_deep_windows() -> None:
    m = _loader()
    assert m.quarter_bounds(2016, 1) == (date(2016, 1, 1), date(2016, 4, 1))
    assert m.quarter_bounds(2020, 4) == (date(2020, 10, 1), date(2021, 1, 1))
    assert m.ARCHIVE_START == date(2016, 1, 1) and m.LOAD_START == (2013, 1)
    # Three prior years of trades exist for every quarter archived.
    assert ih.quarters(m.ARCHIVE_START, m.LOAD_START)[0] == (2013, 1) and ih.quarters(m.ARCHIVE_START, m.LOAD_START)[-1] == (2015, 4)
    qs = m.archive_quarters(date(2026, 11, 7))
    assert qs[0] == (2016, 1) and qs[-1] == (2021, 4) and len(qs) == 24     # up to the quarter of 1 Nov 2021
    assert m.archive_quarters(date(2026, 2, 3))[-1] == (2021, 1)


def test_table_rows_become_trades_with_their_stored_classes() -> None:
    m = _loader()
    row = {"accession": "A1", "line": "1", "owner_cik": "111", "filed": date(2017, 3, 3), "traded": date(2017, 3, 1),
           "issuer_cik": "5", "ticker": "AAPL", "owner_name": "Cook", "relationship": "Officer", "title": "CEO",
           "code": "P", "shares": 10, "price": None, "planned": None, "insider_class": "routine"}
    trades, current = m.trades_from_rows([row])
    assert trades[0].price is None and trades[0].planned is False and trades[0].shares == 10.0
    assert current == {("A1", "1", "111"): "routine"}
    rows = m._archive_rows(trades, {("A1", "1", "111"): "routine"}, m.ARCHIVE_START)
    assert rows and rows[0][0] == "f345:A1:111:P"                           # a 2017 purchase now reaches the archive
    assert m._archive_rows(trades, {("A1", "1", "111"): "routine"}, date(2018, 1, 1)) == []
