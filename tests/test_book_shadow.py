import asyncio
import json
from datetime import date, timedelta

from shared.utils import auto_trade as at
from shared.utils import book_shadow as bs

LONGS = ["AAA", "BBB", "CCC", "DDD"]
SHORTS = ["XXX", "YYY", "ZZZ"]
PCT = {"AAA": 0.985, "BBB": 0.95, "CCC": 0.92, "DDD": 0.905, "XXX": 0.005, "YYY": 0.05, "ZZZ": 0.09}


def test_s0_is_exactly_the_traders_equal_weights_and_s5_keeps_each_leg_at_half():
    w = bs.shadow_weights(LONGS, SHORTS, PCT)
    assert w["S0"] == at.target_weights(LONGS, SHORTS, 0.5)
    assert abs(sum(v for v in w["S5"].values() if v > 0) - 0.5) < 1e-9 and abs(sum(v for v in w["S5"].values() if v < 0) + 0.5) < 1e-9
    assert set(w["S5"]) == set(w["S0"])                               # the same names, only the weights differ


def test_s5_leans_on_conviction_within_bounds_and_never_leaves_a_name_out():
    s5 = bs.shadow_weights(LONGS, SHORTS, PCT)["S5"]
    assert s5["AAA"] > s5["BBB"] > s5["CCC"] > s5["DDD"] > 0                  # a longer-ranked long gets more
    assert s5["XXX"] < s5["YYY"] < s5["ZZZ"] < 0                                # a more extreme short gets more (more negative)
    assert max(abs(v) for v in s5.values()) < 0.5                                # no single name is a whole leg
    missing = bs.shadow_weights(LONGS, SHORTS, {k: v for k, v in PCT.items() if k != "BBB"})["S5"]
    assert "BBB" in missing and missing["BBB"] > 0                              # a missing score keeps the name, at the leg's average conviction


def test_hold_return_signs_drops_unpriced_names_and_is_none_when_nothing_prices():
    d0, d1 = date(2026, 11, 2), date(2026, 12, 1)
    closes = {"AAA": [(d0, 100.0), (d1, 110.0)], "XXX": [(d0, 50.0), (d1, 40.0)]}
    r = bs.hold_return({"AAA": 0.25, "XXX": -0.25, "NOPE": 0.5}, closes, d0, d1)
    assert abs(r - (0.25 * 0.10 + (-0.25) * (-0.20))) < 1e-12                   # long up 10%, short down 20%: both help; NOPE is dropped, not zero-return
    assert bs.hold_return({"NOPE": 1.0}, closes, d0, d1) is None
    assert bs.paired_blocks([0.01, 0.02], [0.02, 0.01, 0.5]) == [0.01, -0.01]


def test_the_recorder_writes_both_sets_once_per_rebalance_and_places_no_orders():
    from services.agents import book_shadow as job

    class Raw:
        def __init__(self):
            self.kv = {job.BOOK_KEY: json.dumps({"rebalanced_on": "2026-11-12", "long": LONGS, "short": SHORTS, "scores_as_of": "2026-11-12"}),
                       "sentinel:factor_model:scores:monthly": json.dumps({t: {"percentile": p} for t, p in PCT.items()}),
                       "sentinel:factor_model:report": json.dumps({"as_of": "2026-11-12"})}

        async def get(self, k):
            return self.kv.get(k)

    class Db:
        def __init__(self):
            self.rows = {}

        async def query_one(self, sql, day, variants):
            return {"n": sum((day, v) in self.rows for v in variants)}

        async def execute(self, sql, day, variants, weights, as_of):
            for v, w in zip(variants, weights):
                self.rows[(day, v)] = json.loads(w)

    db = Db()
    shadow = job.BookShadow(db, Raw())
    first = asyncio.run(shadow.run_once())
    assert first["recorded"] == 2 and sorted(v for _, v in db.rows) == ["S0", "S5"]
    assert asyncio.run(shadow.run_once())["recorded"] == 0                        # the same rebalance is not recorded twice
    src = open(job.__file__, encoding="utf-8").read()
    assert "broker" not in src.lower() and "submit_order" not in src, "the shadow must never reach the broker"


def _shadow(book_as_of, report_as_of, fail_after_first=False):
    from services.agents import book_shadow as job

    class Raw:
        kv = {job.BOOK_KEY: json.dumps({"rebalanced_on": "2026-11-12", "long": LONGS, "short": SHORTS, "scores_as_of": book_as_of}),
              "sentinel:factor_model:scores:monthly": json.dumps({t: {"percentile": p} for t, p in PCT.items()}),
              "sentinel:factor_model:report": json.dumps({"as_of": report_as_of})}

        async def get(self, k):
            return self.kv.get(k)

    class Db:
        def __init__(self):
            self.rows, self.calls = {}, 0

        async def query_one(self, sql, day, variants):
            return {"n": sum((day, v) in self.rows for v in variants)}

        async def execute(self, sql, day, variants, weights, as_of):
            self.calls += 1
            if fail_after_first and self.calls == 1:
                raise RuntimeError("insert failed")                   # one statement: nothing of the rebalance is written
            for v, w in zip(variants, weights):
                self.rows.setdefault((day, v), json.loads(w))

    db = Db()
    return job.BookShadow(db, Raw()), db


def test_a_failed_insert_leaves_nothing_half_recorded_and_the_next_pass_records_both():
    shadow, db = _shadow("2026-11-12", "2026-11-12", fail_after_first=True)
    try:
        asyncio.run(shadow.run_once())
    except RuntimeError:
        pass
    assert db.rows == {}
    assert asyncio.run(shadow.run_once())["recorded"] == 2 and len(db.rows) == 2


def test_a_day_with_only_one_variant_is_completed_not_reported_as_recorded():
    shadow, db = _shadow("2026-11-12", "2026-11-12")
    db.rows[(date(2026, 11, 12), "S0")] = {"AAA": 0.1}
    assert asyncio.run(shadow.run_once())["recorded"] == 2 and (date(2026, 11, 12), "S5") in db.rows


def test_weights_from_newer_scores_than_the_books_are_not_recorded():
    for book_as_of in ("2026-11-12", None):
        shadow, db = _shadow(book_as_of, "2026-11-16")
        assert asyncio.run(shadow.run_once())["recorded"] == 0 and db.rows == {}
