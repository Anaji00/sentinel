"""Peer decoupling generalises beyond NVDA, and the generic discovery alert cannot flood again (reports/Divergence alert.md)."""

import asyncio
import pathlib
import random
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import divergence_watch as dw  # noqa: E402
from services.agents import quant_trading_engine as qte  # noqa: E402
from shared.utils import divergence as dv  # noqa: E402


def history(sd: float = 0.02, seed: int = 4) -> list:
    rnd = random.Random(seed)
    return [rnd.gauss(0, sd) for _ in range(60)]


class _Raw:
    def __init__(self, fail: bool = False) -> None:
        self.keys: dict = {}
        self.fail = fail

    async def set(self, key, value, nx=False, ex=None):
        if self.fail:
            raise ConnectionError("redis down")
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True


class _Redis:
    def __init__(self, fail: bool = False) -> None:
        self.raw = _Raw(fail)


def _engine(fail: bool = False):
    engine = qte.QuantTradingEngine.__new__(qte.QuantTradingEngine)
    engine.redis = _Redis(fail)
    return engine


def test_an_upside_divergence_in_another_industry_fires_with_its_own_numbers() -> None:
    """A bank rising 6% while its group falls 4% on its own news: the sign is symmetric and the text names the move."""
    banks = ("JPM", "BAC", "WFC", "C", "GS")
    d = dv.detect("USB", 0.06, {"industry": (-0.04, history(), banks)}, news=True)
    assert d is not None and d.ticker == "USB" and d.z > 3 and d.kind == "news"
    text = d.headline() + " " + d.description()
    assert "USB" in text and "+6.0%" in text and "-4.0%" in text and "JPM" in d.description()


def test_a_universe_where_everything_moves_together_fires_nothing() -> None:
    watch = dw.DivergenceWatch(None, _Redis())
    tickers = [f"T{i}" for i in range(40)]
    rng = np.random.default_rng(7)
    watch._rets = rng.normal(0, 0.01, (60, 1)) + rng.normal(0, 0.004, (60, len(tickers)))
    watch._col = {t: i for i, t in enumerate(tickers)}
    watch._refs = {t: {"industry": [x for x in tickers if x != t][:8]} for t in tickers}
    ret = {t: 0.03 + float(rng.normal(0, 0.003)) for t in tickers}      # a broad rally: nobody decouples
    assert watch._detect_all(ret, frozenset(tickers)) == []


def test_two_stocks_in_different_groups_are_each_found_and_only_those() -> None:
    watch = dw.DivergenceWatch(None, _Redis())
    semis = ["NVDA", "AMD", "AVGO", "MRVL", "TSM", "MU"]
    banks = ["USB", "JPM", "BAC", "WFC", "C", "GS"]
    tickers = semis + banks
    rng = np.random.default_rng(3)
    watch._rets = rng.normal(0, 0.01, (60, 1)) + rng.normal(0, 0.005, (60, len(tickers)))
    watch._col = {t: i for i, t in enumerate(tickers)}
    watch._refs = {t: {"industry": [x for x in (semis if t in semis else banks) if x != t]} for t in tickers}
    ret = {"NVDA": -0.043, "AMD": 0.133, "AVGO": 0.088, "MRVL": 0.095, "TSM": 0.035, "MU": 0.0625,
           "USB": 0.06, "JPM": -0.04, "BAC": -0.045, "WFC": -0.035, "C": -0.04, "GS": -0.05}
    found = watch._detect_all(ret, frozenset({"NVDA", "USB"}))
    assert sorted(d.ticker for d in found) == ["NVDA", "USB"]


def test_the_generic_peer_alert_is_once_per_ticker_per_cooldown_and_names_no_decoupling() -> None:
    engine = _engine()
    assert asyncio.run(engine._peer_alert_due("NVDA")) is True
    assert asyncio.run(engine._peer_alert_due("NVDA")) is False      # the 56-in-48-hours flood
    assert asyncio.run(engine._peer_alert_due("AMD")) is True
    assert qte.PEER_ALERT_COOLDOWN_SEC >= 6 * 3600
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert "Peer link found" in src and "Peer Decoupling Detected" not in src


def test_a_cooldown_store_failure_lets_the_alert_through() -> None:
    assert asyncio.run(_engine(fail=True)._peer_alert_due("NVDA")) is True
