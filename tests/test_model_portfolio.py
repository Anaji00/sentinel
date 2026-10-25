"""The model portfolio: SPY plus the monthly model's long/short overlay, with
buy/sell/short/cover orders and their reasons, filled at the next open and
marked against SPY."""

import asyncio
import json
import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import model_portfolio as mp  # noqa: E402
from shared.utils.asset_universe import ASSET_META_KEY  # noqa: E402
from shared.utils.signal_store import FACTOR_REPORT_KEY, factor_scores_key  # noqa: E402
from services.agents import portfolio_engine as pe  # noqa: E402

# Ten names, A the model's favourite and J its least favourite.
PCT = {t: round(1.0 - k / 9, 4) for k, t in enumerate("ABCDEFGHIJ")}


def test_the_book_is_a_fifth_of_the_ranked_names_from_each_end() -> None:
    assert mp.book(PCT, [], 1) == ["A", "B"]
    assert mp.book(PCT, [], -1) == ["I", "J"]


def test_shorts_are_only_names_the_broker_will_lend() -> None:
    assert mp.book(PCT, [], -1, allowed={"G", "H", "J"}) == ["H", "J"]


def test_a_held_name_stays_while_inside_the_hold_buffer() -> None:
    # D ranks at 0.667: out of the top fifth but within the best 40%, so a
    # holder keeps it and only one slot is refilled.
    assert mp.book(PCT, ["D"], 1) == ["A", "D"]
    # F (0.444) has fallen past the buffer and is replaced.
    assert mp.book(PCT, ["F"], 1) == ["A", "B"]


def test_between_rebalances_only_forced_exits_happen() -> None:
    pct = {t: p for t, p in PCT.items() if t != "B"}
    assert mp.maintain({"A", "B"}, pct) == ["A"]
    assert mp.maintain({"I", "J"}, PCT, allowed={"J"}) == ["J"]


def test_every_order_says_why_in_words() -> None:
    ords = {(o.ticker, o.action): o.reason for o in
            mp.orders({1: {"A", "F"}, -1: {"J", "H"}}, {1: {"A", "B"}, -1: {"J", "I"}}, PCT, borrowable={"I", "J"})}
    assert set(ords) == {("B", "BUY"), ("F", "SELL"), ("I", "SHORT"), ("H", "COVER")}
    # B ranks at 0.889: in the best 12%. I at 0.111: in the worst 12%.
    assert ords[("B", "BUY")] == "Entered the model's top fifth: among its best 12% of stocks."
    assert "among its worst 12% of stocks" in ords[("I", "SHORT")]
    assert mp.orders({1: set(), -1: set()}, {1: {"A"}, -1: set()}, {"A": 0.997})[0].reason.endswith("best 1% of stocks.")
    assert "44th percentile" in ords[("F", "SELL")] and "60th" in ords[("F", "SELL")]
    assert "If you own it, this says sell" in ords[("I", "SHORT")]
    assert "borrow" in ords[("H", "COVER")]


def test_an_unranked_name_is_sold_and_a_flat_reason_overrides() -> None:
    ords = mp.orders({1: {"Z"}, -1: set()}, {1: set(), -1: set()}, PCT)
    assert ords[0].action == "SELL" and "No longer ranked" in ords[0].reason
    ords = mp.orders({1: {"A"}, -1: {"J"}}, {1: set(), -1: set()}, PCT, why_flat="gone")
    assert [(o.action, o.reason) for o in ords] == [("SELL", "gone"), ("COVER", "gone")]


def test_rebalance_cadence_follows_the_monthly_model() -> None:
    assert mp.REBALANCE_SESSIONS == 21
    assert mp.due(None) and mp.due(21) and not mp.due(20)


def test_a_changing_leg_earns_overnight_on_what_leaves_and_intraday_on_what_arrives() -> None:
    prev = {"A": 100.0, "B": 50.0}
    today = {"A": (102.0, 104.0), "B": (51.0, 49.0), "C": (20.0, 22.0)}
    r, miss = mp.leg_return({"A", "B"}, {"A", "B"}, prev, today)
    assert abs(r - ((104 / 100 - 1) + (49 / 50 - 1)) / 2) < 1e-12 and miss == []
    # B sold at the open (+2% overnight), C bought there (+10% to the close).
    r, miss = mp.leg_return({"A", "B"}, {"A", "C"}, prev, today)
    expect = ((102 / 100 - 1) + (51 / 50 - 1)) / 2 + ((104 / 102 - 1) + (22 / 20 - 1)) / 2
    assert abs(r - expect) < 1e-12 and miss == []


def test_a_missing_bar_counts_flat_and_is_reported() -> None:
    r, miss = mp.leg_return({"A", "X"}, {"A", "X"}, {"A": 100.0, "X": 10.0}, {"A": (100.0, 110.0)})
    assert abs(r - 0.05) < 1e-12 and miss == ["X"]


def test_the_day_is_spy_plus_the_overlay_less_cost_and_borrow() -> None:
    s = mp.day_step(1.0, 1.0, 0.01, 0.02, -0.01, traded_long=2.0, traded_short=2.0, gross=0.3)
    cost = 0.3 * 4.0 * mp.COST_BPS_PER_SIDE / 1e4
    borrow = 0.3 * mp.BORROW_FEE_ANNUAL / 252
    assert abs(s["ret"] - (0.01 + 0.3 * 0.03 - cost - borrow)) < 1e-12
    assert abs(s["spy_nav"] - 1.01) < 1e-12 and s["nav"] > s["spy_nav"]


def test_traded_fraction_counts_both_sides_of_a_leg() -> None:
    assert mp.traded_fraction(set(), {"A", "B"}) == 1.0
    assert mp.traded_fraction({"A", "B"}, {"A", "C"}) == 1.0
    assert mp.traded_fraction({"A", "B"}, {"A", "B"}) == 0.0


def test_the_summary_reads_against_spy() -> None:
    rows = [{"ret": 0.02, "spy_ret": 0.01, "nav": 1.02, "spy_nav": 1.01},
            {"ret": -0.02, "spy_ret": 0.0, "nav": 0.9996, "spy_nav": 1.01},
            {"ret": 0.01, "spy_ret": 0.0, "nav": 1.009596, "spy_nav": 1.01}]
    s = mp.summary(rows)
    assert s["sessions"] == 3 and s["return_pct"] == 0.96 and s["spy_return_pct"] == 1.0
    assert s["excess_pct"] == -0.04 and s["ahead_share"] == round(2 / 3, 3)
    # Ahead by 0.99% after day one, then 1.03% behind: the worst stretch.
    assert abs(s["worst_vs_spy_pct"] - round((0.9996 / 1.01) / (1.02 / 1.01) * 100 - 100, 2)) < 1e-9
    assert mp.summary([]) == {"sessions": 0}


def test_the_borrow_charge_uses_quoted_fees() -> None:
    assert mp.book_fee([], {}) == mp.BORROW_FEE_ANNUAL
    assert abs(mp.book_fee(["X", "Y"], {"X": 0.3}) - (0.003 + mp.BORROW_FEE_ANNUAL) / 2) < 1e-12
    cheap = mp.day_step(1.0, 1.0, 0.0, 0.0, 0.0, borrow_fee_annual=0.003)
    dear = mp.day_step(1.0, 1.0, 0.0, 0.0, 0.0, borrow_fee_annual=0.25)
    assert dear["borrow"] > 80 * cheap["borrow"]


# ── the engine, against an in-memory database ───────────────────────────────

DAYS = [date(2026, 9, 1) + timedelta(days=k) for k in range(10)]


class FakeRedis:
    def __init__(self) -> None:
        self.kv = {}

    async def get(self, k):
        return self.kv.get(k)


class FakeDB:
    def __init__(self, bars):
        self.state = None
        self.orders = {}
        self.nav = []
        self.bars = bars                                     # {(ticker, day): (open, close)}
        self.fees = {}                                       # {ticker: annual fee %}

    async def query_one(self, sql, *a):
        if sql is pe._STATE_SQL:
            return dict(self.state) if self.state else None
        if sql is pe._SESSIONS_BETWEEN_SQL:
            return {"n": sum(1 for d in DAYS if a[0] < d <= a[1])}
        raise AssertionError(sql)

    async def query(self, sql, *a):
        if sql is pe._SESSIONS_SQL:
            return [{"day": d} for d in DAYS if d >= a[0]]
        if sql is pe._FEES_SQL:
            return [{"ticker": t, "fee_rate": f} for t, f in self.fees.items()]
        if sql is pe._BARS_SQL:
            return [{"ticker": t, "day": d, "open": o, "close": c} for (t, d), (o, c) in self.bars.items()
                    if t in a[0] and a[1] <= d <= a[2]]
        raise AssertionError(sql)

    async def execute(self, sql, *a):
        if sql is pe._SAVE_SQL:
            keys = ["portfolio", "long_book", "short_book", "pending", "rebalanced_on", "scores_as_of",
                    "marked_through", "started_on", "nav", "spy_nav"]
            self.state = dict(zip(keys, a))
        elif sql is pe._NAV_SQL:
            self.nav.append(a)
        else:
            raise AssertionError(sql)

    async def execute_many(self, sql, rows):
        for r in rows:
            if sql is pe._ORDER_SQL:
                self.orders[(r[1], r[2], r[3])] = {"kind": r[4], "reason": r[5], "fill": None}
            elif sql is pe._FILL_SQL:
                for (dec, t, _a), o in self.orders.items():
                    if dec == r[1] and t == r[3] and o["fill"] is None:
                        o["fill"] = (r[2], r[4])
            else:
                raise AssertionError(sql)


def _setup(support="supported", pct=PCT):
    bars = {}
    for t in list("ABCDEFGHIJ") + ["SPY"]:
        for k, d in enumerate(DAYS):
            bars[(t, d)] = (100.0 + k, 100.0 + k + (1.0 if t in "AB" else -1.0 if t in "IJ" else 0.5))
    db, r = FakeDB(bars), FakeRedis()
    r.kv[ASSET_META_KEY] = json.dumps({t: ["stock", True, True] for t in "ABCDEFGHIJ"})
    r.kv[FACTOR_REPORT_KEY] = json.dumps({"as_of": str(DAYS[2]), "horizons": {"monthly": {"support": support}}})
    r.kv[factor_scores_key("monthly")] = json.dumps({t: {"percentile": p} for t, p in pct.items()})
    return db, r, pe.PortfolioEngine(db, r)


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def test_orders_are_decided_after_the_run_and_filled_at_the_next_open() -> None:
    db, r, eng = _setup()

    async def go():
        st = await eng._state()
        out = await eng._decide(st)
        assert out["orders"] == 4 and out["kind"] == "rebalance"
        assert st["pending"]["long"] == ["A", "B"] and st["pending"]["short"] == ["I", "J"]
        await eng._save(st)
        st = await eng._state()
        assert await eng._mark(st, today=DAYS[5]) == 2          # DAYS[3] (the fill) and DAYS[4]
        await eng._save(st)
        return st

    st = _run(go())
    assert st["long"] == {"A", "B"} and st["short"] == {"I", "J"} and st["pending"] is None
    assert st["started_on"] == DAYS[3] and st["marked_through"] == DAYS[4]
    assert all(o["fill"] == (DAYS[3], 103.0) for o in db.orders.values())
    # Longs rose and shorts fell: the portfolio beats SPY on both days.
    assert st["nav"] > st["spy_nav"] and len(db.nav) == 2


def test_a_name_that_leaves_the_rankings_is_sold_before_the_next_rebalance() -> None:
    db, r, eng = _setup()

    async def go():
        st = await eng._state()
        await eng._decide(st)
        await eng._mark(st, today=DAYS[5])
        r.kv[FACTOR_REPORT_KEY] = json.dumps({"as_of": str(DAYS[4]), "horizons": {"monthly": {"support": "supported"}}})
        r.kv[factor_scores_key("monthly")] = json.dumps({t: {"percentile": p} for t, p in PCT.items() if t != "B"})
        out = await eng._decide(st)
        return st, out

    st, out = _run(go())
    assert out == {"orders": 1, "kind": "maintenance", "as_of": str(DAYS[4])}
    assert st["pending"]["long"] == ["A"] and st["pending"]["short"] == ["I", "J"]
    assert "No longer ranked" in db.orders[(DAYS[4], "B", "SELL")]["reason"]


def test_without_model_evidence_the_overlay_is_closed() -> None:
    db, r, eng = _setup()

    async def go():
        st = await eng._state()
        await eng._decide(st)
        await eng._mark(st, today=DAYS[5])
        r.kv[FACTOR_REPORT_KEY] = json.dumps({"as_of": str(DAYS[4]), "horizons": {"monthly": {"support": None}}})
        return st, await eng._decide(st)

    st, out = _run(go())
    assert out["orders"] == 4
    assert st["pending"]["long"] == [] and st["pending"]["short"] == []
    assert all(o["reason"] == pe.FLAT_REASON for (d, _t, _a), o in db.orders.items() if d == DAYS[4])


def test_a_session_waits_for_its_bars() -> None:
    db, r, eng = _setup()
    for t in "ABIJ":
        del db.bars[(t, DAYS[3])]

    async def go():
        st = await eng._state()
        await eng._decide(st)
        return st, await eng._mark(st, today=DAYS[5])

    st, marked = _run(go())
    assert marked == 0 and st["pending"] is not None and st["marked_through"] is None


def test_a_name_too_dear_to_borrow_is_not_shorted() -> None:
    db, r, eng = _setup()
    db.fees = {"J": 25.2, "I": 0.3}

    async def go():
        st = await eng._state()
        await eng._decide(st)
        return st

    st = _run(go())
    assert st["pending"]["short"] == ["H", "I"]


def test_weights_and_a_short_tilt_size_the_legs_apart() -> None:
    prev = {"A": 100.0, "B": 100.0}
    today = {"A": (100.0, 110.0), "B": (100.0, 100.0)}
    eq, _ = mp.leg_return({"A", "B"}, {"A", "B"}, prev, today)
    tilted, _ = mp.leg_return({"A", "B"}, {"A", "B"}, prev, today, {"A": 3.0, "B": 1.0})
    assert abs(eq - 0.05) < 1e-12 and abs(tilted - 0.075) < 1e-12
    base = mp.day_step(1.0, 1.0, 0.0, 0.0, 0.02, gross=0.3, borrow_fee_annual=0.0)
    leaned = mp.day_step(1.0, 1.0, 0.0, 0.0, 0.02, gross=0.3, borrow_fee_annual=0.0, short_gross=0.45)
    assert abs(base["overlay"] + 0.006) < 1e-12 and abs(leaned["overlay"] + 0.009) < 1e-12
    hist = [{"decided_on": "2026-10-14", "gross": 0.3, "short_tilt": 1.2, "weights": {"A": 0.5}}]
    from datetime import date
    rec = mp.sizing_record(hist, date(2026, 10, 15))
    assert rec["short_tilt"] == 1.2 and mp.sizing_record(hist, date(2026, 10, 14)) == {}
