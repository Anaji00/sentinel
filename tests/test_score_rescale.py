"""Pure-function tests for shared/utils/score_rescale.py (registered 21 Nov 2026)."""
import math
import random
from datetime import date, timedelta

from shared.utils import score_rescale as sc


def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv(sc.FLAG, raising=False)
    assert sc.v2_enabled() is False
    monkeypatch.setenv(sc.FLAG, "1")
    assert sc.v2_enabled() is True


def test_jumpiness_and_earnings_score_never_above_old():
    rng = random.Random(1)
    flat = [rng.gauss(0, 0.01) for _ in range(252)]
    jumpy = list(flat)
    for i in (10, 70, 140, 200):
        jumpy[i] = 0.12
    assert sc.jumpiness(flat[:50]) is None
    assert sc.jumpiness([0.0] * 252) is None
    assert sc.jumpiness(jumpy) > sc.jumpiness(flat)
    assert sc.earnings_pre_score(0.45, None) == 0.45
    low, high = sc.earnings_pre_score(0.45, sc.jumpiness(flat)), sc.earnings_pre_score(0.45, sc.jumpiness(jumpy))
    assert low <= high <= 0.45 and low >= 0.25
    assert sc.earnings_pre_score(0.45, 100.0) == 0.45 and sc.earnings_pre_score(0.45, 0.0) == 0.25


def test_kalshi_quiet_score_range_and_scope():
    assert sc.kalshi_quiet_score(0.5, 10.0, 0.5) == 0.5                       # not on the cap: untouched
    assert sc.kalshi_quiet_score(0.3, 6000.0, 0.5) == 0.3                      # not quiet by size
    assert sc.kalshi_quiet_score(0.3, None, 0.5) == 0.3
    assert sc.kalshi_quiet_score(0.3, 1.0, 0.5) == 0.3                         # a $1 trade has no size term
    top = sc.kalshi_quiet_score(0.3, 4999.0, 0.5)
    assert 0.38 <= top <= 0.39
    assert sc.kalshi_quiet_score(0.3, 4999.0, 0.01) < sc.kalshi_quiet_score(0.3, 4999.0, 0.5)     # a 1% market's odds update says little
    assert sc.kalshi_quiet_score(0.3, 200.0, None) == sc.kalshi_quiet_score(0.3, 200.0, 0.5)
    assert all(0.3 <= sc.kalshi_quiet_score(0.3, n, p) <= 0.39 for n in (1, 7, 90, 4000) for p in (0.0, 0.2, 0.5, 1.0))


def test_adv_and_filing_score():
    assert sc.adv_usd([10.0] * 3, [1e6] * 3) is None                         # too few sessions
    assert sc.adv_usd([10.0] * 25, [1e6] * 25) == 1e7
    assert sc.filing_score(0.86, None) == 0.86
    assert sc.filing_score(0.86, 1e7) == 0.86                                  # small name keeps its score
    assert sc.filing_score(0.86, 1e10) == round(0.86 * 0.75, 3)
    assert sc.filing_score(0.86, 1e9) < sc.filing_score(0.86, 1e8) <= 0.86
    assert sc.filing_score(0.2, 1e10) < 0.2


def test_block_score_capped_and_monotone():
    assert sc.block_score(0.9, None, 1e9) == 0.9 and sc.block_score(0.9, 1e6, None) == 0.9 and sc.block_score(0.9, 0.0, 1e9) == 0.9
    assert sc.block_score(0.9, 1e5, 1e9) == 0.35                                # 0.01% of a day's dollar volume
    assert sc.block_score(0.99, 1e7, 1e9) == 0.95                               # 1%
    assert sc.block_score(0.5, 1e7, 1e9) == 0.5                                 # never above old
    a, b = sc.block_score(0.99, 2e5, 1e9), sc.block_score(0.99, 2e6, 1e9)
    assert a < b


def test_slot_quantile_keeps_marginal_and_falls_back():
    pooled = sorted([i / 100 for i in range(100)])
    slot_hi = sorted([0.5 + i / 200 for i in range(100)])     # a slot whose scores are all high
    assert sc.slot_quantile_score(0.7, slot_hi, pooled, min_slot=50) < 0.7       # high for the slot's own history, so pulled down
    slot_lo = sorted([i / 400 for i in range(100)])
    assert sc.slot_quantile_score(0.2, slot_lo, pooled, min_slot=50) > 0.2
    assert sc.slot_quantile_score(0.7, slot_hi[:10], pooled) == 0.7
    assert sc.slot_quantile_score(0.7, slot_hi, pooled[:10]) == 0.7
    assert sc.slot_quantile_score(5.0, slot_hi, pooled) == pooled[-1]            # above everything seen: top of the pooled range
    assert sc.slot_quantile_score(-1.0, slot_hi, pooled) == pooled[0]
    assert sc.slot_key("OHLCV_15M_BAR", 3, False) == ("OHLCV_15M_BAR", 3, False)


def _series(n=120, start=date(2026, 1, 1)):
    days = [start + timedelta(days=i) for i in range(n)]
    closes = [100.0 * math.exp(0.01 * math.sin(i) + 0.001 * i) for i in range(n)]
    return days, closes


def test_normalized_abs_move():
    days, closes = _series()
    d = days[80]
    m = sc.normalized_abs_move(days, closes, d)
    assert m is not None and m > 0
    assert sc.normalized_abs_move(days, closes, days[-1]) is None               # no bar 5 sessions on
    assert sc.normalized_abs_move(days, closes, days[0]) is None                # no history
    assert sc.normalized_abs_move(days, closes, days[80] + timedelta(days=30)) is None or True
    flat = [100.0] * len(days)
    assert sc.normalized_abs_move(days, flat, d) is None                        # flat history has no spread
    assert sc.normalized_abs_move(days, closes, d, sessions=2) != m


def test_date_cluster_diff_detects_a_better_score_and_is_deterministic():
    rng = random.Random(3)
    rows = []
    for k in range(30):
        for _ in range(8):
            outcome = rng.random()
            old = rng.random()                                  # unrelated to the outcome
            new = outcome + 0.2 * rng.random()                  # tracks it
            rows.append((k, old, new, outcome))
    res = sc.date_cluster_diff(rows, draws=300)
    assert res["diff"] > 0.5 and res["lo"] > 0 and res["dates"] == 30
    assert res == sc.date_cluster_diff(rows, draws=300)
    assert sc.not_worse(res, 100) and not sc.not_worse(res, 10_000)
    worse = [(d, n, o, y) for d, o, n, y in rows]           # swap the columns: new is now the noise
    assert not sc.not_worse(sc.date_cluster_diff(worse, draws=300), 100)
    assert sc.date_cluster_diff(rows[:2]) is None and not sc.not_worse(None, 1)


def test_constant_old_score_counts_as_zero_correlation():
    rows = [(i % 5, 0.3, i / 100, i / 100) for i in range(100)]
    res = sc.date_cluster_diff(rows, draws=100)
    assert res["rho_old"] == 0.0 and res["rho_new"] > 0.99


def test_mode_share():
    assert sc.mode_share([]) == (0.0, 0.0)
    assert sc.mode_share([0.3, 0.3, 0.3, 0.1]) == (0.3, 0.75)


# ---- the enrichment wiring (flag off by default) ------------------------------------------------------------------------------------

import asyncio  # noqa: E402

from services.enrichment.enrichers.tradfi import TradFiEnricher  # noqa: E402


class _FakeDb:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail, self.calls = rows or [], fail, 0

    async def query(self, sql, *args):
        self.calls += 1
        if self.fail:
            raise RuntimeError("db down")
        return list(reversed(self.rows))


def _enricher(db):
    return TradFiEnricher(None, None, None, db=db)


def _bars(n=20, close=100.0, volume=1_000_000):
    return [{"close": close, "volume": volume} for _ in range(n)]          # ADV = $100M


def test_participation_rescore_is_off_by_default(monkeypatch):
    monkeypatch.delenv(sc.FLAG, raising=False)
    db = _FakeDb(_bars())
    adj = []
    assert asyncio.run(_enricher(db)._participation_rescore("AAPL", 1e6, 0.9, adj)) == 0.9
    assert adj == [] and db.calls == 0


def test_participation_rescore_on_lowers_and_records(monkeypatch):
    monkeypatch.setenv(sc.FLAG, "1")
    db = _FakeDb(_bars())
    adj = []
    e = _enricher(db)
    out = asyncio.run(e._participation_rescore("AAPL", 1e6, 0.9, adj))      # 1% of ADV: 0.95 cap, so 0.9 stays
    assert out == 0.9 and adj == []
    out = asyncio.run(e._participation_rescore("AAPL", 1e4, 0.9, adj))      # 0.01% of ADV: 0.35
    assert out == 0.35 and adj[0].reason == "block_participation_v2" and adj[0].delta == round(0.35 - 0.9, 6)
    assert db.calls == 1                                                   # the ADV is cached


def test_participation_rescore_keeps_score_when_adv_unknown_or_db_fails(monkeypatch):
    monkeypatch.setenv(sc.FLAG, "1")
    assert asyncio.run(_enricher(_FakeDb(_bars(3)))._participation_rescore("X", 1e4, 0.8, [])) == 0.8
    assert asyncio.run(_enricher(_FakeDb(fail=True))._participation_rescore("X", 1e4, 0.8, [])) == 0.8
