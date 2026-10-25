"""Corporate actions re-adjust stored history; the wildcard IEX stream feeds the minute universe."""

import importlib.util
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "services" / "collector-tradfi"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from shared.utils.corporate_actions import affected  # noqa: E402

spec = importlib.util.spec_from_file_location("alpaca_stream_t", ROOT / "services" / "collector-tradfi" / "alpaca_stream.py")
stream = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stream)

BODY = {"corporate_actions": {
    "forward_splits": [{"symbol": "DXJ", "ex_date": "2026-10-09", "new_rate": 3, "old_rate": 1}],
    "reverse_splits": [{"symbol": "NBIZ", "old_symbol": "NBIZ", "ex_date": "2026-09-22"}],
    "spin_offs": [{"source_symbol": "CTVA", "new_symbol": "NEWCO", "ex_date": "2026-10-01"},
                  {"source_symbol": "494ESC015", "new_symbol": "HURA", "ex_date": "2026-01-27"}],
    "cash_dividends": [{"symbol": "CTVA", "ex_date": "2026-09-01", "rate": 0.18},
                       {"symbol": "AAPL", "ex_date": "2026-11-09", "rate": 0.26}],
    "stock_dividends": [{"symbol": "09175M606", "ex_date": "2025-10-06"}],
}}


def test_every_re_adjusted_ticker_is_named_with_its_earliest_ex_date() -> None:
    a = affected(BODY, today=date(2026, 10, 15))
    assert a["DXJ"] == {"ex_date": date(2026, 10, 9), "split": True}
    assert a["CTVA"] == {"ex_date": date(2026, 9, 1), "split": True}       # dividend then spin-off
    assert a["NEWCO"]["split"] and a["HURA"]["split"]
    assert "AAPL" not in a                                                   # ex-date not reached
    assert "09175M606" not in a and "494ESC015" not in a                     # CUSIPs, not tickers


def test_dividend_only_names_skip_the_minute_re_scale() -> None:
    a = affected({"corporate_actions": {"cash_dividends": [{"symbol": "KO", "ex_date": "2026-09-12"}]}},
                 today=date(2026, 10, 15))
    assert a == {"KO": {"ex_date": date(2026, 9, 12), "split": False}}


def test_stream_bars_become_rows_labelled_by_their_own_time() -> None:
    row = stream.bar_row({"T": "b", "S": "amd", "o": 570, "h": 572.5, "l": 569, "c": 571, "v": 1200,
                          "t": "2026-10-14T13:31:00Z"})
    assert row[0] == "AMD" and row[1] == datetime(2026, 10, 14, 13, 31, tzinfo=timezone.utc)
    assert row[2:7] == (570.0, 572.5, 569.0, 571.0, 1200.0)
    assert row[7] == "REGULAR" and row[8] == stream.SOURCE
    assert stream.bar_row({"T": "t", "S": "AMD"}) is None                   # a trade, not a bar
    assert stream.bar_row({"T": "b", "S": "X", "o": 1, "h": 0.5, "l": 1, "c": 1, "t": "2026-10-14T13:31:00Z"}) is None


def test_only_the_minute_universe_is_kept_and_finnhub_names_are_left_to_finnhub() -> None:
    rows = [("AMD",), ("NVDA",), ("ZZZZ",)]
    assert stream.keep(rows, {"AMD", "NVDA"}, {"NVDA"}) == [("AMD",)]


def test_the_stream_never_overwrites_a_consolidated_bar() -> None:
    assert "WHERE tradfi_bars.source IS DISTINCT FROM 'alpaca_sip'" in stream.INSERT_SQL
