import asyncio
import json

from shared.utils import auto_trade as at
from shared.utils import shadow_ledger as sl

LONGS = ["AAA", "BBB", "CCC", "DDD"]
SHORTS = ["XXX", "YYY", "ZZZ"]


def test_borrow_cost_is_short_gross_times_fee_over_252():
    b = sl.borrow_cost(LONGS, SHORTS)
    assert b["n_long"] == 4 and b["n_short"] == 3 and b["short_gross_nav"] == 0.5
    assert abs(b["daily_cost_bps_nav"] - 0.5 * 0.02 / 252 * 1e4) < 1e-3
    assert abs(b["daily_cost_per_100k_usd"] - 0.5 * 0.02 / 252 * 1e5) < 1e-3
    assert sl.borrow_cost(LONGS, [])["daily_cost_bps_nav"] == 0.0              # no short book, no cost
    assert sl.borrow_cost(LONGS, SHORTS, 0.053)["daily_cost_bps_nav"] > b["daily_cost_bps_nav"]


def test_veto_counts_by_side_and_rule_remove_nothing():
    eight = [{"ticker": "AAA", "items": ["4.02", "9.01"]}, {"ticker": "XXX", "items": ["3.01"]}, {"ticker": "BBB", "items": ["2.02"]}, {"ticker": "NOPE", "items": ["1.03"]}]
    divs = [{"ticker": "CCC", "ret": -0.1, "z": -3.5}, {"ticker": "DDD", "ret": 0.1, "z": 3.5},          # a long that rose: not adverse
            {"ticker": "YYY", "ret": 0.1, "z": 3.2}, {"ticker": "ZZZ", "ret": 0.1, "z": 1.0}, {"ticker": "AAA", "ret": "x", "z": 9}]
    out = sl.veto_counts(LONGS, SHORTS, eight, divs)
    assert out["divergence"]["long_names"] == ["CCC"] and out["divergence"]["short_names"] == ["YYY"]
    assert out["eight_k_both"]["long_names"] == ["AAA"] and out["eight_k_both"]["short_names"] == ["XXX"]
    assert out["eight_k_long_only"]["long"] == 1 and out["eight_k_long_only"]["short"] == 0
    assert set(out) == set(sl.RULES)
    live = at.veto_names(LONGS, SHORTS, eight, divs)                                                     # the live rule agrees with the both-sides counts
    assert set(live) == {"AAA", "XXX", "CCC", "YYY"}


def test_recorder_writes_once_a_day_counts_failures_and_never_reaches_the_broker():
    from services.agents import shadow_ledger as job

    class Raw:
        async def get(self, k):
            return json.dumps({"rebalanced_on": "2026-11-12", "long": LONGS, "short": SHORTS})

    class Db:
        def __init__(self, fail=False):
            self.rows, self.fail = {}, fail

        async def query(self, sql, *a):
            return []

        async def query_one(self, sql, day, kind):
            return {"n": int((day, kind) in self.rows)}

        async def execute(self, sql, day, kind, payload):
            assert isinstance(payload, dict)                                                                 # the pool's jsonb codec encodes; a pre-dumped string would be stored double-encoded
            if self.fail:
                raise RuntimeError("db down")
            self.rows[(day, kind)] = payload

    db = Db()
    led = job.ShadowLedger(db, Raw())
    first = asyncio.run(led.run_once())
    assert first["recorded"] == 2 and sorted(k for _, k in db.rows) == ["borrow_2pct", "veto_counts"]
    assert asyncio.run(led.run_once())["recorded"] == 0
    bad = asyncio.run(job.ShadowLedger(Db(fail=True), Raw()).run_once())
    assert bad["recorded"] == 0 and bad["failed"] == 2                                                   # counted, not reported as success
    src = open(job.__file__, encoding="utf-8").read().lower()
    assert "submit_order" not in src and "broker" not in src
