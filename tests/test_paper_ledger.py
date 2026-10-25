"""Paper trades: filled at the next open, costed, stopped, net of SPY."""

import pathlib
import sys
from datetime import date, datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import paper_ledger as pl  # noqa: E402

DAYS = [date(2026, 9, 1) + timedelta(days=k) for k in range(40)]


def bars(path, vol=1e6):
    return [pl.Bar(d, o, max(o, c) * 1.01, min(o, c) * 0.99, c, vol) for d, (o, c) in zip(DAYS, path)]


FLAT = [(100.0, 100.0)] * 40
SPY = {b.day: b for b in bars(FLAT)}


def test_an_idea_logged_during_the_session_fills_at_the_next_open() -> None:
    during = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)          # 11:00 New York, on DAYS[24]
    before = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)          # 08:00 New York
    assert pl.entry_session(DAYS, during) == 25
    assert pl.entry_session(DAYS, before) == 24
    assert pl.entry_session(DAYS, datetime(2026, 12, 1, tzinfo=timezone.utc)) is None


def test_a_weekly_long_is_held_five_sessions_and_pays_costs_both_ways() -> None:
    path = FLAT[:25] + [(100.0, 101.0), (101.0, 102.0), (102.0, 103.0), (103.0, 104.0), (104.0, 110.0)] + FLAT[30:]
    f = pl.simulate(bars(path), SPY, datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc), 1, "weekly")
    assert f.entry_day == DAYS[25] and f.entry_price == 100.0
    assert f.exit_day == DAYS[29] and f.exit_reason == "horizon"
    assert abs(f.gross_excess - 0.10) < 1e-12
    assert f.net_excess < f.gross_excess and abs(f.net_excess - (0.10 - 2 * f.cost_bps / 1e4)) < 1e-12


def test_a_stop_inside_a_session_fills_at_the_stop_and_a_gap_at_the_open() -> None:
    path = FLAT[:25] + [(100.0, 99.0), (99.0, 96.0)] + FLAT[27:]
    f = pl.simulate(bars(path), SPY, datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc), 1, "weekly", stop=97.0)
    assert f.exit_reason == "stop" and f.exit_day == DAYS[26] and f.exit_price == 97.0
    gap = FLAT[:25] + [(100.0, 99.0), (94.0, 93.0)] + FLAT[27:]
    g = pl.simulate(bars(gap), SPY, datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc), 1, "weekly", stop=97.0)
    assert g.exit_price == 94.0                                             # through the stop overnight


def test_a_short_gains_when_the_stock_lags_the_market() -> None:
    spy_up = {b.day: b for b in bars(FLAT[:25] + [(100.0, 102.0)] + FLAT[26:])}
    f = pl.simulate(bars(FLAT), spy_up, datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc), -1, "daily")
    assert f.exit_day == DAYS[25] and abs(f.gross_excess - 0.02) < 1e-12


def test_thin_names_cost_more_and_the_trade_waits_for_its_session() -> None:
    assert pl.cost_bps(250_000, 2e9, 0.02) < pl.cost_bps(250_000, 2e6, 0.02)
    open_trade = pl.simulate(bars(FLAT[:27]), SPY, datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc), 1, "weekly")
    assert open_trade.exit_day is None and open_trade.entry_day == DAYS[25]


def test_the_grader_opens_one_trade_per_idea_and_closes_it() -> None:
    """A repeated idea does not double the position; the trade fills and closes from bars."""
    import asyncio
    from services.agents.grader import Grader

    trades = {}
    idea_at = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)
    ideas = [{"day": DAYS[24], "horizon": "daily", "ticker": "AAA", "direction": 1, "created_at": idea_at,
              "model": "gbm", "stop_price": None},
             {"day": DAYS[25], "horizon": "daily", "ticker": "AAA", "direction": 1,
              "created_at": idea_at + timedelta(days=1), "model": "gbm", "stop_price": None}]
    path = {"AAA": FLAT[:25] + [(100.0, 103.0)] + FLAT[26:], "SPY": FLAT}

    class DB:
        async def query(self, sql, *a):
            if "FROM recommendation_log" in sql and "created_at" in sql:
                return ideas
            if "FROM paper_trades" in sql:
                return [dict(v) for v in trades.values()]
            if "FROM daily_bars" in sql and "open" in sql:
                return [{"ticker": t, "day": d, "open": o, "high": max(o, c), "low": min(o, c), "close": c,
                         "volume": 1e6} for t, ps in path.items() for d, (o, c) in zip(DAYS, ps)]
            return []

        async def execute(self, sql, *a):
            if "INSERT INTO paper_trades" in sql:
                trades.setdefault(a[:4], {"idea_day": a[0], "horizon": a[1], "ticker": a[2], "direction": a[3],
                                          "idea_at": a[4], "stop_price": a[6], "notional_usd": a[7],
                                          "status": "pending", "exit_day": None})
            elif "UPDATE paper_trades" in sql:
                t = trades[a[:4]]
                t.update(status=a[4], exit_day=a[10], net=a[15])

    out = asyncio.run(Grader(DB()).ledger())
    assert out["paper_opened"] == 1 and out["paper_closed"] == 1     # the repeat was held, not doubled
    (t,) = trades.values()
    assert t["status"] == "closed" and t["exit_day"] == DAYS[25] and t["net"] < 0.03


def test_volatility_managed_size_shrinks_in_turbulence_and_grows_in_calm() -> None:
    import math as _m
    calm_then_wild = [100.0 * _m.exp(0.001 * k) for k in range(240)]
    for k in range(21):                                       # last month: 4% daily swings
        calm_then_wild.append(calm_then_wild[-1] * (1.04 if k % 2 else 0.96))
    assert pl.vol_scale(calm_then_wild) == pl.SCALE_BOUNDS[0]
    wild_then_calm = [100.0 * (1.03 if k % 2 else 0.97) ** 1 for k in range(240)]
    wild_then_calm += [100.0 * _m.exp(0.0005 * k) for k in range(21)]
    assert pl.vol_scale(wild_then_calm) == pl.SCALE_BOUNDS[1]
    assert pl.vol_scale([100.0] * 10) == 1.0                  # too little history: unscaled


def test_an_open_trade_is_marked_net_of_spy_and_both_ways_of_costs() -> None:
    # Long, up 10% while SPY is up 2%: +8% gross, less 2 x 10 bps.
    assert abs(pl.open_mark(1, 100.0, 400.0, 110.0, 408.0, 10.0) - 0.078) < 1e-12
    # The same move held short loses it.
    assert abs(pl.open_mark(-1, 100.0, 400.0, 110.0, 408.0, 10.0) + 0.082) < 1e-12
    assert pl.open_mark(1, 0.0, 400.0, 110.0, 408.0, 10.0) is None
    assert pl.open_mark(1, 100.0, 400.0, None, 408.0, 10.0) is None
