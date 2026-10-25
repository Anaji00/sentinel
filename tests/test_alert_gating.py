"""Rules and edges earn the top of the board by their track record."""

import asyncio
import json
import pathlib
import sys
from typing import Any, Dict

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import producer_grades as pg  # noqa: E402


def test_entity_variants_are_judged_as_one_family() -> None:
    assert pg.rule_family("Geopolitical Cascade (Msft)") == "Geopolitical Cascade"
    assert pg.rule_family("Equity Block & Options Convergence") == "Equity Block & Options Convergence"
    grades = {"correlations_5d": [{"group": "Geopolitical Cascade (Msft)", "n": 9, "verdict": "too few"}],
              "correlation_families_5d": [{"group": "Geopolitical Cascade", "n": 220, "verdict": "no difference"},
                                          {"group": "Cross-Domain Hawkes Excitation Forecast", "n": 766,
                                           "verdict": "moves more"}]}
    retired = pg.uninformative_rules(grades)
    assert pg.is_uninformative("Geopolitical Cascade (Nvda)", retired)
    assert not pg.is_uninformative("Cross-Domain Hawkes Excitation Forecast", retired)


def test_an_uninformative_rule_is_capped_at_alert() -> None:
    from services.correlation import main as cm
    from shared.models.events import AlertTier
    retired = {"Geopolitical Cascade"}
    assert cm._tier_after_grades("Geopolitical Cascade (Aapl)", AlertTier.CRITICAL, retired) == AlertTier.ALERT
    assert cm._tier_after_grades("Geopolitical Cascade (Aapl)", AlertTier.WATCH, retired) == AlertTier.WATCH
    assert cm._tier_after_grades("Hawkes", AlertTier.CRITICAL, retired) == AlertTier.CRITICAL


class _Raw:
    def __init__(self) -> None:
        self.h: Dict[str, Dict[str, str]] = {}

    async def hset(self, k: str, f: str, v: str) -> None:
        self.h.setdefault(k, {})[f] = v

    async def hget(self, k: str, f: str) -> Any:
        return self.h.get(k, {}).get(f)

    async def hdel(self, k: str, f: str) -> None:
        self.h.get(k, {}).pop(f, None)

    async def expire(self, k: str, s: int) -> None:
        return None


class _Redis:
    def __init__(self) -> None:
        self.raw = _Raw()


def test_a_new_edge_is_held_until_it_replicates() -> None:
    from services.correlation import edge_survival as es

    sent = []

    class _Producer:
        async def send(self, topic: Any, payload: Any, key: Any = None) -> None:
            sent.append((payload, key))

    class _Disc:
        _gate_edge = __import__("services.correlation.statistical_discovery",
                                fromlist=["x"]).StatisticalDiscoveryEngine._gate_edge

    eng = _Disc()
    eng.survival = es.EdgeSurvivalTracker(_Redis())
    eng.producer = _Producer()
    edge = "pearson:AAA:BBB"
    assert asyncio.run(eng._gate_edge(edge, "topic", {"x": 1}, "AAA")) is False and sent == []
    assert asyncio.run(eng.survival.release(edge)) == {"proposal": {"x": 1}, "key": "AAA"}
    eng.survival.redis.raw.h.setdefault(es.REPLICATED_KEY, {})[edge] = json.dumps({"at": __import__("time").time()})
    assert asyncio.run(eng._gate_edge(edge, "topic", {"x": 2}, "AAA")) is True and sent == [({"x": 2}, "AAA")]


def test_a_granger_edge_is_retested_as_granger_not_as_a_correlation() -> None:
    import numpy as np
    from services.correlation.statistical_discovery import retest_statistic

    rng = np.random.default_rng(5)
    shocks = rng.normal(0, 0.02, 200)
    a = 100 * np.cumprod(1 + shocks)
    lagged = np.concatenate([[0.0], shocks[:-1]])            # b follows a's previous move
    b = 50 * np.cumprod(1 + 0.8 * lagged + rng.normal(0, 0.003, 200))
    f_ab, p_ab = retest_statistic("granger", list(a), list(b))
    f_ba, p_ba = retest_statistic("granger", list(b), list(a))
    assert p_ab < 0.01 and f_ab > f_ba
    r, p = retest_statistic("pearson", list(a), list(b))
    assert r is not None and p is not None
