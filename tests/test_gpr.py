"""The geopolitical risk index: rows from the published files, and the summary agents read."""

import pathlib
import sys
from datetime import date, datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import gpr  # noqa: E402


def test_daily_and_monthly_rows() -> None:
    d = gpr.daily_rows([{"date": datetime(2026, 10, 26), "GPRD": 165.2, "GPRD_ACT": 149.7, "GPRD_THREAT": float("nan")},
                        {"date": None, "GPRD": 1.0}])
    assert ("gpr_daily", date(2026, 10, 26), 165.2, gpr.SOURCE) in d and len(d) == 2
    m = gpr.monthly_rows([{"month": datetime(2026, 9, 1), "GPR": 146.2, "GPRC_USA": 0.8, "SHARE_GPR": 4.4}])
    assert {r[0] for r in m} == {"gpr_monthly", "gpr_country_usa"}


def test_summary_flags_an_elevated_week() -> None:
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(400)]
    vals = [100.0 + (i % 5) for i in range(393)] + [200.0] * 7
    s = gpr.summary(days, vals)
    assert s is not None and s["elevated"] and s["ma7"] == 200.0 and s["as_of"] == days[-1].isoformat()
    assert "elevated" in gpr.fact(s) and gpr.fact(None) is None
    assert gpr.summary(days[:100], vals[:100]) is None
