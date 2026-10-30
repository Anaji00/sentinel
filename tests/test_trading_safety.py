"""The auto-trader's safety layer: per-name caps, liquidity, marketable limits,
the kill switch and its automatic trips (day loss, drawdown, drift, broken
execution). FIA/CFTC practice: stop new orders, never force-sell to "make safe"."""
import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import auto_trade as at  # noqa: E402


def test_names_are_capped_and_the_excess_stays_cash():
    w = at.cap_weights({"A": 0.10, "B": -0.08, "C": 0.01}, 0.03)
    assert w == {"A": 0.03, "B": -0.03, "C": 0.01}


def test_opening_orders_are_shrunk_to_liquidity_and_exits_never_are():
    p = at.Plan("A", 1000.0, "buy", "open")
    assert at.liquidity_capped(p, 10.0, 500_000.0, 0.01).qty == 500.0
    exit_ = at.Plan("A", 1000.0, "sell", "close")
    assert at.liquidity_capped(exit_, 10.0, 500_000.0, 0.01).qty == 1000.0
    assert at.liquidity_capped(p, 10.0, None, 0.01).qty == 1000.0, "no volume data, no cap"


def test_limits_are_marketable_but_bounded_and_on_the_tick():
    assert at.limit_price("buy", 100.0, 0.005) == 100.5
    assert at.limit_price("sell", 100.0, 0.005) == 99.5
    assert at.limit_price("buy", 0.5, 0.005) == 0.5025


def test_a_bad_day_a_deep_drawdown_or_drift_halts():
    flat = [(100.0, 500.0)] * 25
    assert at.safety_halts(flat, 99.0, 500.0, 0.05, 0.15, 0.08) == []
    assert "day loss" in at.safety_halts(flat, 93.0, 500.0, 0.05, 0.15, 0.08)[0]
    peaked = [(100.0, 500.0)] * 20 + [(130.0, 500.0), (120.0, 500.0)]
    assert any("drawdown" in r for r in at.safety_halts(peaked, 108.0, 500.0, 0.20, 0.15, 0.08))
    assert any("drift" in r for r in at.safety_halts(flat, 99.0, 550.0, 0.05, 0.15, 0.08)), "SPY +10%, book -1%"
    assert at.safety_halts([], 100.0, 500.0, 0.05, 0.15, 0.08) == [], "a first day cannot trip"


def test_broken_execution_halts_only_on_enough_orders():
    assert at.execution_halt(2, 8, 0.25) == "8 of 10 orders failed"
    assert at.execution_halt(1, 2, 0.25) is None
    assert at.execution_halt(90, 10, 0.25) is None


class _Raw:
    def __init__(self, halt=None):
        self.v = {}
        if halt:
            self.v["sentinel:auto_trade:halt:b1_extremes"] = halt

    async def get(self, k):
        return self.v.get(k)

    async def set(self, k, v, **_):
        self.v[k] = v


def test_the_kill_switch_reads_the_environment_and_the_flag(monkeypatch):
    from services.agents import auto_trader as tr
    t = tr.AutoTrader.__new__(tr.AutoTrader)
    t.portfolio, t.raw = "b1_extremes", _Raw()
    monkeypatch.delenv("AUTO_TRADE_HALT", raising=False)
    assert asyncio.run(t._halted()) is None
    monkeypatch.setenv("AUTO_TRADE_HALT", "1")
    assert asyncio.run(t._halted()) == "AUTO_TRADE_HALT is set"
    monkeypatch.delenv("AUTO_TRADE_HALT")
    asyncio.run(t._halt("day loss -7%"))
    assert asyncio.run(t._halted()) == "day loss -7%"


def test_the_halt_is_checked_before_any_order_and_flips_close_at_market():
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    body = src.split("async def run_once")[1].split("async def _halted")[0]
    assert body.index("await self._halted()") < body.index("await self._send(")
    assert body.index("_check_and_record") < body.index("await self._send(")
    assert 'plan.reason != "flip-close"' in src and "OrderType.LIMIT" in src


# ── review fixes ─────────────────────────────────────────────────────────────

def test_a_neutral_book_is_not_measured_against_a_rally():
    flat = [(100.0, 500.0)] * 25
    assert at.safety_halts(flat, 100.5, 545.0, 0.05, 0.15, 0.08, beta=0.0) == [], "SPY +9%, neutral book +0.5%"
    assert any("drift" in r for r in at.safety_halts(flat, 91.0, 500.0, 0.10, 0.15, 0.08, beta=0.0))
    assert any("drift" in r for r in at.safety_halts(flat, 100.5, 545.0, 0.05, 0.15, 0.08, beta=1.0))


def test_equity_is_recorded_before_the_halt_check_and_limits_use_a_live_price():
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    body = src.split("async def run_once")[1].split("async def _halted")[0]
    assert body.index("_check_and_record") < body.index("await self._halted()")
    assert "beta=sum(weights.values())" in body
    assert "self.broker.latest_prices(" in body and "live.get(plan.symbol) or marks.get(plan.symbol)" in body


def test_latest_prices_reads_the_data_api_in_batches():
    from shared.broker import alpaca
    src = (ROOT / "shared/broker/alpaca.py").read_text(encoding="utf-8")
    assert 'f"{DATA_URL}/v2/stocks/trades/latest"' in src and "syms[k:k + 100]" in src
    assert alpaca.DATA_URL == "https://data.alpaca.markets"


def test_funds_never_enter_the_book_even_from_a_stored_one():
    stocks = {"AAPL", "PBR", "BRK.B"}
    assert at.picks_only(["AAPL", "EWZ", "SPDN", "pbr", "SPY"], stocks) == ["AAPL", "PBR"]
    assert at.picks_only(["AAPL", "EWZ", "SPY"], None) == ["AAPL", "EWZ"], "without the asset list only INDEX_FUNDS go"
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    body = src.split("async def run_once")[1].split("async def _halted")[0]
    assert body.index("at.picks_only(") < body.index("at.target_weights(")
