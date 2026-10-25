"""The predictions panel shows the record honestly: shrunk, bounded, calibrated."""

import asyncio
import datetime as dt
import pathlib
import sys
from typing import Any, List

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes.predictions import closes_for, interval, is_stock, summarise  # noqa: E402


def test_only_listed_stock_symbols_count() -> None:
    assert is_stock("NVDA") and is_stock("BRK.B")
    assert not is_stock("BTC") and not is_stock("CL=F") and not is_stock("ETHUSDT")


def test_a_short_record_is_shrunk_and_bounded() -> None:
    rows = [{"outcome_correct": True, "confidence": 0.7, "brier_score": 0.09}] * 5
    s = summarise(rows)
    assert s["raw_hit_rate"] == 1.0 and s["shrunk_hit_rate"] < 0.65
    lo, hi = s["interval_90"]
    assert lo < 0.75 and hi == 1.0


def test_calibration_groups_by_conviction() -> None:
    rows = (
        [{"outcome_correct": True, "confidence": 0.8}] * 3
        + [{"outcome_correct": False, "confidence": 0.8}] * 1
        + [{"outcome_correct": False, "confidence": 0.5}] * 2
    )
    bands = {b["band"]: b for b in summarise(rows)["calibration"]}
    assert bands["0.75-1.00"]["hit_rate"] == 0.75 and bands["0.75-1.00"]["calls"] == 4
    assert bands["0.00-0.55"]["hit_rate"] == 0.0


def test_no_record_is_no_rate() -> None:
    assert summarise([])["raw_hit_rate"] is None and interval(0, 0) is None


def test_calls_whose_entry_was_not_the_stocks_price_are_excluded() -> None:
    from services.api_gateway.routes.predictions import entry_check

    assert entry_check(11.02, 334.53) == "mismatch"     # AAPL recorded at another ticker's price
    assert entry_check(0.0, 50.0) == "no_entry"
    assert entry_check(100.0, 104.0) == "ok"
    assert entry_check(100.0, None) == "unverified"
    rows = [
        {"outcome_correct": True, "confidence": 0.9, "entry_check": "ok"},
        {"outcome_correct": False, "confidence": 0.9, "entry_check": "mismatch"},
        {"outcome_correct": True, "confidence": 0.9, "entry_check": "no_entry"},
    ]
    s = summarise(rows)
    assert s["resolved"] == 1 and s["excluded_invalid"] == 2


class _Closes:
    def __init__(self) -> None:
        self.args: List[Any] = []

    async def query(self, sql: str, *args: Any) -> List[dict]:
        self.args = list(args)
        return [{"day": dt.date(2026, 10, 19), "close": 19.25}, {"day": dt.date(2026, 10, 20), "close": 19.33}]


def test_the_stock_page_chart_reads_the_models_daily_closes_oldest_first() -> None:
    db = _Closes()
    out = asyncio.run(closes_for("apld", days=90, db=db, user={}))
    assert db.args == ["APLD", 90]
    assert out["closes"] == [{"day": "2026-10-19", "close": 19.25}, {"day": "2026-10-20", "close": 19.33}]
    assert asyncio.run(closes_for("../etc", days=90, db=db, user={}))["closes"] == []
