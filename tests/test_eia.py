"""The EIA's weekly petroleum stocks, parsed from its history pages."""

import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import eia  # noqa: E402

PAGE = """<tr> <td class='B6'>&nbsp;&nbsp;2025-Dec</td> <td class='B5'>12/26&nbsp;</td> <td class='B3'>422,888&nbsp;&nbsp;&nbsp;</td>
 <td class='B5'>01/02&nbsp;</td> <td class='B3'>419,056&nbsp;&nbsp;&nbsp;</td> <td class='B5'>&nbsp;</td> <td class='B3'>&nbsp;</td> </tr>
<tr> <td class='B6'>&nbsp;&nbsp;2026-Jan</td> <td class='B5'>01/09&nbsp;</td> <td class='B3'>422,447&nbsp;&nbsp;&nbsp;</td> </tr>
<tr> <td height='5' colspan='13'></td> </tr>"""


def test_history_rows_become_dated_values_across_the_year_end() -> None:
    assert eia.parse_history(PAGE) == [(date(2025, 12, 26), 422888.0), (date(2026, 1, 2), 419056.0),
                                       (date(2026, 1, 9), 422447.0)]
    assert eia.parse_history("<html></html>") == []


def test_a_week_is_judged_against_its_own_record() -> None:
    start = date(2020, 1, 3)
    hist = [(start + timedelta(weeks=i), 400000.0 + (1000 if i % 2 else -1000)) for i in range(300)]
    hist.append((hist[-1][0] + timedelta(weeks=1), hist[-1][1] - 9000))       # a big draw
    rep = eia.week_report(hist)
    assert rep["change"] == -9000
    assert rep["change_z"] < -3
    assert rep["five_year_avg"] is not None and rep["week_ending"] == hist[-1][0].isoformat()
    assert eia.week_report(hist[:10]) is None
