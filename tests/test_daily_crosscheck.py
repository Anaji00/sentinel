"""The stored daily bars against a second vendor."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import daily_crosscheck as dc  # noqa: E402


def test_agreeing_vendors_pass_and_a_wrong_close_is_named() -> None:
    ours = {f"T{i}": {"close": 100.0, "volume": 1e6} for i in range(300)}
    theirs = {f"T{i}": {"close": 100.0, "volume": 1.02e6} for i in range(300)}
    ok = dc.compare(ours, theirs)
    assert ok["ok"] and ok["compared"] == 300 and ok["close_mismatches"] == 0 and ok["median_volume_ratio"] == 0.9804
    ours["T7"] = {"close": 60.18, "volume": 1e6}                       # an unadjusted split, say
    theirs["T7"] = {"close": 180.54, "volume": 1e6}
    bad = dc.compare(ours, theirs)
    assert bad["close_mismatches"] == 1 and bad["worst_closes"][0]["ticker"] == "T7"
    assert dc.compare({}, {})["ok"] is False


def test_grouped_results_parse_by_ticker() -> None:
    body = {"results": [{"T": "BRK.B", "c": 470.1, "v": 3.1e6}, {"T": "X"}, {"c": 5}]}
    assert dc.parse_grouped(body) == {"BRK.B": {"close": 470.1, "volume": 3.1e6}}


def test_yahoo_daily_bar_is_read_on_the_new_york_day_and_share_classes_use_a_dash() -> None:
    from datetime import date
    # 13:30 UTC on 15 Oct 2026 is the 09:30 New York open of that session
    body = {"chart": {"result": [{"timestamp": [1792071000, 1792157400],
                                  "indicators": {"quote": [{"close": [101.5, 102.25], "volume": [900, 1200]}]}}]}}
    assert dc.parse_yahoo_daily(body, date(2026, 10, 15)) == {"close": 101.5, "volume": 900.0}
    assert dc.parse_yahoo_daily(body, date(2026, 10, 17)) is None
    assert dc.parse_yahoo_daily({"chart": {"result": None}}, date(2026, 10, 15)) is None
    assert dc.yahoo_symbol("brk.b") == "BRK-B"


def test_yahoo_sample_keeps_the_most_traded_and_rotates_the_rest_by_day() -> None:
    from datetime import date
    names = [f"T{i}" for i in range(500)]
    a = dc.yahoo_sample(names, date(2026, 10, 15))
    b = dc.yahoo_sample(names, date(2026, 10, 16))
    assert a[:40] == names[:40] and len(a) == 100 and len(set(a)) == 100
    assert a == dc.yahoo_sample(names, date(2026, 10, 15))       # a rerun checks the same names
    assert set(a[40:]) != set(b[40:])
