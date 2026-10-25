"""The shared per-stock feature table: builders and the point-in-time reader."""

import math
import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import stock_features as sf  # noqa: E402

D = date(2026, 6, 5)          # a Friday


def test_insider_measures_over_their_windows() -> None:
    trades = {"AAA": [
        sf.Trade(D - timedelta(days=10), "o1", "P", False, False),
        sf.Trade(D - timedelta(days=20), "o2", "P", False, False),
        sf.Trade(D - timedelta(days=100), "o3", "S", False, True),     # in 182, not in 91
        sf.Trade(D - timedelta(days=5), "o4", "S", True, True),        # planned: not counted
        sf.Trade(D - timedelta(days=30), "o5", "S", False, True),
        sf.Trade(D + timedelta(days=1), "o6", "P", False, False),      # filed after the day: unknown yet
    ], "QUIET": [sf.Trade(D - timedelta(days=400), "x", "P", False, False)]}
    rows = {(t, f): v for t, d, f, v in sf.insider_rows(trades, [D])}
    assert rows[("AAA", "ins_npr_182")] == round((2 - 2) / 4, 4)
    assert rows[("AAA", "ins_buyers_91")] == 2.0
    assert rows[("AAA", "ins_opp_sells_91")] == 1.0
    assert not any(t == "QUIET" for t, _ in rows)                         # nothing in the window: missing


def test_adverse_8k_items_become_events_once_a_day() -> None:
    got = sf.eightk_rows([("AAA", D, ["3.02", "9.01", "3.02"]), ("BBB", D, ["5.02"]), ("CCC", D, ["4.01"])])
    assert got == [("AAA", D, "ev_8k_dilution", 1.0), ("CCC", D, "ev_8k_auditor_change", 1.0)]


def test_values_read_forward_only_within_their_window() -> None:
    dates = [D + timedelta(days=k) for k in range(12)]
    panel = sf.to_panel([("AAA", D + timedelta(days=1), 0.5), ("ZZZ", D, 9.0)], dates, ["AAA", "BBB"], ttl_days=7)
    assert math.isnan(panel[0, 0])                       # before it was known
    assert panel[1, 0] == 0.5 and panel[8, 0] == 0.5     # its day and seven after
    assert math.isnan(panel[9, 0])                       # lapsed
    assert all(math.isnan(x) for x in panel[:, 1])       # never had one


def test_every_feature_is_registered_with_a_window() -> None:
    for name in list(sf.EIGHT_K_FEATURES.values()) + ["ins_npr_182", "ins_buyers_91", "ins_opp_sells_91",
                                                       "cmdty_exposure_21", "cmdty_exposure_63"]:
        assert sf.ttl(name) and sf.ttl(name) > 0
    assert sf.weekly_grid(date(2026, 6, 1), date(2026, 6, 20)) == [date(2026, 6, 5), date(2026, 6, 12),
                                                                    date(2026, 6, 19)]
