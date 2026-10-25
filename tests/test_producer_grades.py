"""Correlations, scenarios and agent claims graded against prices, not themselves."""

import math
import pathlib
import random
import sys
from datetime import date, datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import producer_grades as pg  # noqa: E402

DAYS = [date(2026, 1, 1) + timedelta(days=k) for k in range(300)]


def walk(seed, jump_at=None, jump=0.0, vol=0.01):
    rng = random.Random(seed)
    px, opens, closes = 100.0, [], []
    for k in range(len(DAYS)):
        o = px
        px *= math.exp(rng.gauss(0, vol) + (jump if k == jump_at else 0.0))
        opens.append(o)
        closes.append(px)
    return opens, closes


def at(k):
    return datetime.combine(DAYS[k], datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=12)   # pre-open


def test_noise_averages_about_one_and_a_real_move_stands_out() -> None:
    so, sc = walk(1, vol=0.008)
    spy = {d: (o, c) for d, o, c in zip(DAYS, so, sc)}
    noise = []
    for s in range(60):
        o, c = walk(100 + s)
        r = pg.move_ratio(DAYS, o, c, spy, at(150 + s), 1)
        noise.append(("noise", r[0]))
    big = []
    for s in range(30):
        o, c = walk(500 + s, jump_at=200, jump=0.06)
        big.append(("big", pg.move_ratio(DAYS, o, c, spy, at(200), 1)[0]))
    table = {r["group"]: r for r in pg.ratio_table(noise + big)}
    assert 0.7 < table["noise"]["mean_ratio"] < 1.3 and table["noise"]["verdict"] == "no difference"
    assert table["big"]["mean_ratio"] > 2 and table["big"]["verdict"] == "moves more"


def test_no_history_no_grade_and_small_groups_are_not_tested() -> None:
    o, c = walk(3)
    spy = {d: (100.0, 100.0) for d in DAYS}
    assert pg.move_ratio(DAYS, o, c, spy, at(10), 1) is None             # 60 sessions needed before
    assert pg.ratio_table([("tiny", 1.4)] * 3)[0]["verdict"] == "too few"


def test_a_claim_no_better_than_its_placebo_has_no_skill() -> None:
    r = pg.placebo_rate([True] * 63 + [False] * 37, [True] * 61 + [False] * 39)
    assert r["hit_rate"] == 0.63 and abs(r["skill"] - 0.02) < 1e-9


def test_overnight_gaps_and_fat_tails_do_not_bias_the_null() -> None:
    """The first version compared an open-to-close move with close-to-close
    volatility and read every rule as "moves less" (0.6-0.9)."""
    def gappy(seed):
        rng = random.Random(seed)
        px, opens, closes = 100.0, [], []
        for _ in DAYS:
            o = px * math.exp(rng.gauss(0, 0.012))                      # overnight
            jump = rng.gauss(0, 0.05) if rng.random() < 0.03 else 0.0      # fat tail
            px = o * math.exp(rng.gauss(0, 0.008) + jump)
            opens.append(o)
            closes.append(px)
        return opens, closes

    so, sc = gappy(9)
    spy = {d: (o, c) for d, o, c in zip(DAYS, so, sc)}
    for k in (1, 5):
        rows = []
        for s in range(80):
            o, c = gappy(1000 + s)
            got = pg.move_ratio(DAYS, o, c, spy, at(120 + s), k)
            rows.append(("noise", got[0]))
        r = pg.ratio_table(rows)[0]
        assert 0.8 < r["mean_ratio"] < 1.25 and r["verdict"] == "no difference", (k, r)


def test_only_well_graded_rules_with_no_edge_are_retired() -> None:
    grades = {
        "correlations_1d": [{"group": "Busy", "n": 700, "verdict": "moves less"},
                            {"group": "Useful", "n": 90, "verdict": "moves more"},
                            {"group": "Thin", "n": 12, "verdict": "no difference"}],
        "correlations_5d": [{"group": "Busy", "n": 400, "verdict": "no difference"},
                            {"group": "Useful", "n": 60, "verdict": "no difference"}],
    }
    assert pg.uninformative_rules(grades) == {"Busy"}
    assert pg.uninformative_rules(None) == set()


def test_reasoning_skips_scenarios_for_retired_rules() -> None:
    import asyncio
    import importlib
    import json

    from shared.utils.signal_store import PRODUCER_GRADES_KEY

    reasoning = importlib.import_module("services.reasoning.main")
    reasoning._RETIRED_CACHE.update(at=0.0, rules=set())
    grades = {"correlations_5d": [{"group": "Busy", "n": 400, "verdict": "no difference"}]}

    class Raw:
        async def get(self, key):
            return json.dumps(grades) if key == PRODUCER_GRADES_KEY else None

    class Redis:
        raw = Raw()

    assert asyncio.run(reasoning.retired_rules(Redis())) == {"Busy"}
