"""The model portfolio as paper-account orders: sizing, rounding, and only our own names."""

import asyncio
import pathlib
import sys
from datetime import date, datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import auto_trade as at  # noqa: E402
from services.agents import auto_trader as tr  # noqa: E402
from services.agents.auto_trader import NY  # noqa: E402


def test_weights_are_the_picks_alone_each_leg_split_evenly() -> None:
    w = at.target_weights(["AAA", "BBB", "SPY"], ["CCC", "QQQ"], leg=0.10)
    assert w == {"AAA": 0.05, "BBB": 0.05, "CCC": -0.10}            # no index fund, either side
    tilted = at.target_weights(["AAA"], ["CCC", "DDD"], leg=0.10, short_tilt=0.5,
                               weights={"CCC": 3.0, "DDD": 1.0})
    assert abs(tilted["CCC"] + 0.0375) < 1e-12 and abs(tilted["DDD"] + 0.0125) < 1e-12


def test_longs_may_be_fractional_and_shorts_are_whole() -> None:
    w = {"BIG": 0.8, "AAA": 0.001, "DDD": -0.05}
    shares = at.target_shares(w, 100_000.0, {"BIG": 500.0, "AAA": 300.0, "DDD": 40.0})
    assert shares["BIG"] == 160.0
    assert shares["AAA"] == round(100 / 300, 4)
    assert shares["DDD"] == -125.0


def test_a_small_short_leg_keeps_its_size_in_whole_shares() -> None:
    # Ten shorts of $46 each on $300 stocks: one at a time, none reaches a share.
    dollars = {f"S{k}": 46.0 for k in range(10)}
    prices = {t: 300.0 for t in dollars}
    got = at.whole_share_leg(dollars, prices)
    placed = sum(q * prices[t] for t, q in got.items())
    assert abs(placed - 460.0) <= 150.0                 # within half a share of the leg's $460
    assert all(q == 1 for q in got.values())            # spread one share each, not piled on one name
    # Cheap names fill to their weight exactly.
    assert at.whole_share_leg({"A": 100.0, "B": 50.0}, {"A": 10.0, "B": 10.0}) == {"A": 10, "B": 5}


def test_orders_touch_only_names_this_trader_manages_and_exit_first() -> None:
    held = {"BIG": 100.0, "OLD": 10.0, "MINE": -20.0, "FLIP": 5.0}
    target = {"BIG": 120.0, "NEW": 3.0, "FLIP": -4.0}
    prices = {"BIG": 500.0, "OLD": 50.0, "MINE": 20.0, "FLIP": 100.0, "NEW": 30.0}
    plans = at.plan_orders(target, held, managed={"BIG", "MINE", "FLIP"}, prices=prices)
    got = [(p.symbol, p.side, p.qty, p.reason) for p in plans]
    assert ("OLD", "sell", 10.0, "close") not in got          # someone else's position
    assert got == [
        ("FLIP", "sell", 5.0, "flip-close"),
        ("MINE", "buy", 20.0, "close"),
        ("BIG", "buy", 20.0, "add"),
        ("FLIP", "sell", 4.0, "flip-open"),
        ("NEW", "buy", 3.0, "open"),
    ]


def test_dust_is_not_traded() -> None:
    plans = at.plan_orders({"AAA": 10.01}, {"AAA": 10.0}, {"AAA"}, {"AAA": 100.0})
    assert plans == []


def test_the_b1_book_holds_only_the_extremes() -> None:
    pct = {"TOP": 0.995, "HI": 0.95, "EDGE": 0.90, "MID": 0.5, "LOW": 0.05, "NOBORROW": 0.02, "SPY": 0.93,
           "LOWEDGE": 0.10}
    longs, shorts = at.b1_book(pct, borrowable={"LOW", "LOWEDGE", "TOP"})
    assert longs == ["EDGE", "HI"]                      # the very top 1% and index funds left out
    assert shorts == ["LOW", "LOWEDGE"]                 # only what can be borrowed
    assert at.rebalance_due(None) and at.rebalance_due(21) and not at.rebalance_due(20)


class _Raw:
    def __init__(self, store):
        self.store = store

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v, **kw):
        self.store[k] = v


class _BookDb:
    def __init__(self, since):
        self.since = since

    async def query_one(self, sql, *a):
        return {"n": self.since}

    async def query(self, sql, *a):
        return []                                         # no borrow fees quoted


def test_the_book_is_kept_between_rebalances_and_rebuilt_after(monkeypatch) -> None:
    import json
    from shared.utils import asset_universe
    from shared.utils.signal_store import factor_scores_key

    async def meta(raw):
        return {"LOW": ["stock", True, True], "HI": ["stock", True, True]}

    monkeypatch.setattr(asset_universe, "load_asset_meta", meta)
    # Two scores stand for a full set here; the thin-score guard has its own test below.
    monkeypatch.setattr(tr.at, "MIN_SCORES_TO_REBUILD", 1)
    held = {"rebalanced_on": "2026-10-01", "long": ["OLD"], "short": []}
    scores = {"HI": {"percentile": 0.95}, "LOW": {"percentile": 0.03}}
    store = {tr.BOOK_KEY: json.dumps(held), factor_scores_key("monthly"): json.dumps(scores)}
    trader = tr.AutoTrader(_BookDb(since=10), None, broker=_Broker(True))
    trader.raw = _Raw(store)
    assert asyncio.run(trader._book(date(2026, 10, 15)))["long"] == ["OLD"]          # 10 sessions: kept
    trader.db = _BookDb(since=21)
    book = asyncio.run(trader._book(date(2026, 10, 30)))
    assert book["long"] == ["HI"] and book["short"] == ["LOW"] and book["rebalanced_on"] == "2026-10-30"
    assert json.loads(store[tr.BOOK_KEY])["long"] == ["HI"]


class _Broker:
    paper = True

    def __init__(self, open_: bool) -> None:
        self.open_ = open_
        self.sent: list = []

    async def is_market_open(self) -> bool:
        return self.open_


class _DB:
    async def query_one(self, *a):
        raise AssertionError("must not read the book while the market is shut")


def test_nothing_is_read_or_sent_before_the_open_or_while_the_market_is_shut() -> None:
    early = datetime(2026, 10, 22, 9, 30, tzinfo=NY)
    late = datetime(2026, 10, 22, 11, 0, tzinfo=NY)
    assert asyncio.run(tr.AutoTrader(_DB(), None, broker=_Broker(True)).run_once(early)) == {}
    assert asyncio.run(tr.AutoTrader(_DB(), None, broker=_Broker(False)).run_once(late)) == {}


def test_it_is_off_unless_switched_on(monkeypatch) -> None:
    monkeypatch.delenv("AUTO_TRADE_ENABLED", raising=False)
    assert not tr.enabled()
    monkeypatch.setenv("AUTO_TRADE_ENABLED", "true")
    assert tr.enabled()
    monkeypatch.setenv("AUTO_TRADE_CAPITAL_USD", "25000")
    assert tr.capital_cap() == 25000.0
    monkeypatch.setenv("AUTO_TRADE_CAPITAL_USD", "lots")
    assert tr.capital_cap() is None
    monkeypatch.delenv("AUTO_TRADE_LEG_FRACTION", raising=False)
    assert tr.leg_fraction() == 0.5
    monkeypatch.setenv("AUTO_TRADE_LEG_FRACTION", "0.3")
    assert tr.leg_fraction() == 0.3
    monkeypatch.setenv("AUTO_TRADE_LEG_FRACTION", "2")
    assert tr.leg_fraction() == 0.5


def test_positions_from_an_earlier_book_are_the_traders_own() -> None:
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "services" / "agents" / "auto_trader.py").read_text(
        encoding="utf-8")
    assert 'SELECT DISTINCT symbol FROM auto_trade_orders"' in src      # no filter on the book's name
    # And a managed name the new book does not hold is closed.
    plans = at.plan_orders({"NEW": 2.0}, {"OLD": 5.0, "NEW": 0.0}, managed={"OLD"}, prices={"OLD": 10.0, "NEW": 10.0})
    assert ("OLD", "sell", 5.0, "close") in [(p.symbol, p.side, p.qty, p.reason) for p in plans]


def test_a_thin_score_set_keeps_the_book(monkeypatch) -> None:
    """94 scores would have rebuilt B1 as about 9 longs and 9 shorts."""
    import json

    from shared.utils.signal_store import factor_scores_key

    held = {"rebalanced_on": "2026-10-01", "long": ["OLD"], "short": ["OLDER"]}
    scores = {f"T{i}": {"percentile": i / 94} for i in range(94)}
    store = {tr.BOOK_KEY: json.dumps(held), factor_scores_key("monthly"): json.dumps(scores)}
    trader = tr.AutoTrader(_BookDb(since=21), None, broker=_Broker(True))
    trader.raw = _Raw(store)
    assert asyncio.run(trader._book(date(2026, 10, 30))) == held
    assert json.loads(store[tr.BOOK_KEY]) == held
