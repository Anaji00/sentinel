"""The free live slots go to the book: ranking, tick parsing, throttling, contract choice."""

import pathlib
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-tradfi"))

from shared.utils import live_focus as lf  # noqa: E402


def test_the_book_comes_before_the_radar() -> None:
    ranked = lf.rank_focus(
        trading=["msft"], changing=["NEW"], held=["AAA", "BBB", "CCC", "MSFT"],
        percentiles={"AAA": 0.55, "BBB": 0.02, "CCC": 0.97}, earnings_today=["AAA"],
        watched=["ZZZ", "BBB"])
    # Traded today, then entering/leaving, then held with earnings, then held by
    # how extreme its rank is, then the radar; each name once.
    assert ranked == ["MSFT", "NEW", "AAA", "BBB", "CCC", "ZZZ"]
    scores = lf.focus_scores(ranked)
    assert scores["MSFT"] > scores["NEW"] > scores["ZZZ"]


def test_ticks_parse_and_junk_is_dropped() -> None:
    t = lf.trade_row({"T": "t", "S": "aapl", "p": 230.5, "s": 100, "t": "2026-10-23T14:30:01.5Z",
                      "i": 7, "x": "V", "c": ["@"]}, "src")
    assert t[0] == "AAPL" and t[2] == 230.5 and t[3] == 100.0 and t[4] == 7 and t[6] == ["@"]
    assert lf.trade_row({"T": "t", "S": "AAPL", "p": 0, "t": "2026-10-23T14:30:01Z"}, "src") is None
    q = lf.quote_row({"T": "q", "S": "AAPL", "bp": 230.4, "bs": 2, "ap": 230.6, "as": 3,
                      "t": "2026-10-23T14:30:01Z"}, "src")
    assert q[2] == 230.4 and q[4] == 230.6
    assert lf.quote_row({"T": "q", "S": "AAPL", "bp": 0, "ap": 0, "t": "2026-10-23T14:30:01Z"}, "src") is None


def test_quotes_are_kept_only_when_they_change_and_not_too_often() -> None:
    th = lf.QuoteThrottle(gap=0.25)
    t0 = datetime(2026, 10, 23, 14, 30, 0, tzinfo=timezone.utc)
    row = lambda ms, bid: ("AAPL", t0.replace(microsecond=ms * 1000), bid, 1.0, 10.1, 1.0, "s")  # noqa: E731
    assert th.keep(row(0, 10.0))
    assert not th.keep(row(100, 10.05))          # changed, but within a quarter-second
    assert th.keep(row(300, 10.05))
    assert not th.keep(row(900, 10.05))          # unchanged


def test_option_ticks_carry_their_underlying() -> None:
    ts = datetime(2026, 10, 23, 14, 30, tzinfo=timezone.utc)
    q = lf.option_tick_row({"T": "q", "S": "AAPL261120C00230000", "t": ts, "bp": 5.1, "bs": 10,
                            "ap": 5.3, "as": 12}, "src")
    assert q[1] == "AAPL" and q[3] == "q" and q[4] == 5.1 and q[6] == 5.3
    tr = lf.option_tick_row({"T": "t", "S": "AAPL261120P00220000", "t": "2026-10-23T14:30:00Z",
                             "p": 2.4, "s": 3}, "src")
    assert tr[3] == "t" and tr[8] == 2.4 and tr[9] == 3.0
    assert lf.option_tick_row({"T": "q", "S": "NOTANOPTION", "t": ts, "bp": 1, "ap": 1}, "src") is None


def test_the_contracts_nearest_the_price_are_chosen() -> None:
    chain = [("C90", 90.0, "C"), ("C100", 100.0, "C"), ("C105", 105.0, "C"), ("C120", 120.0, "C"),
             ("P95", 95.0, "P"), ("P100", 100.0, "P"), ("P80", 80.0, "P")]
    assert lf.pick_contracts(chain, 101.0, per_side=2) == ["C100", "C105", "P100", "P95"]
    assert lf.pick_contracts(chain, 0.0) == []


def test_the_next_monthly_is_at_least_a_week_out() -> None:
    import option_history as oh
    import option_stream as os_

    assert os_.next_monthly(date(2026, 10, 23), oh.third_friday) == (2026, 11)   # 16 Oct passed
    assert os_.next_monthly(date(2026, 11, 16), oh.third_friday) == (2026, 12)   # 20 Nov is 4 days out


def test_the_alpaca_stream_spends_exactly_its_thirty_slots() -> None:
    import alpaca_stream as st

    focus = [f"T{k}" for k in range(45)] + ["T0"]
    assert st.tick_symbols(focus) == [f"T{k}" for k in range(30)]
