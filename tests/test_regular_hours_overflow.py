"""Watched names the 50-symbol socket cannot carry are still priced, by snapshot."""

import importlib.util
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
TRADFI = ROOT / "services/collector-tradfi/main.py"


def _module():
    spec = importlib.util.spec_from_file_location("tradfi_main_for_overflow", TRADFI)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


def test_overflow_is_exactly_what_the_socket_leaves_out() -> None:
    m = _module()
    import itertools
    watched = ["".join(p) for p in itertools.product("BCDFG", "HJKLM", "NPRST")][:100]   # 100 names
    watched += ["MU", "BRK.B", "MU", "CL=F"]           # oldest last; a futures code is never polled
    streamed = set(m.select_subscription_symbols(watched))
    out = m.overflow_symbols(watched, limit=500)
    assert not streamed & set(out)
    assert "MU" in out and out.count("MU") == 1
    assert all(m._ALPACA_EQUITY_SYMBOL.match(t) for t in out) and "CL=F" not in out
    assert len(m.overflow_symbols(watched, limit=3)) == 3


def test_regular_hours_minute_bars_are_never_called_blocks() -> None:
    src = TRADFI.read_text(encoding="utf-8")
    assert "if is_extended and _is_block_trade(ticker, notional):" in src


def test_event_names_are_polled_even_when_the_watchlist_is_short() -> None:
    m = _module()
    watched = ["AAA", "BBB"]                           # everything fits the socket
    out = m.overflow_symbols(watched, extra=["MU", "AAA", "NVDA"])
    assert out == ["MU"]                               # AAA streams; NVDA is a core anchor


def test_the_timescale_singleton_is_never_seen_half_connected() -> None:
    import asyncio
    import shared.db as dbm

    async def run():
        dbm._timescale = None
        seen = []

        class Slow(dbm.TimescaleClient):
            async def _connect(self, retries: int = 12):
                await asyncio.sleep(0.05)
                self._pool = object()

        orig = dbm.TimescaleClient
        dbm.TimescaleClient = Slow
        try:
            async def grab():
                c = await dbm.get_timescale()
                seen.append(c._pool)
            await asyncio.gather(grab(), grab(), grab())
        finally:
            dbm.TimescaleClient = orig
            dbm._timescale = None
        return seen

    assert all(p is not None for p in asyncio.run(run()))


def test_only_names_over_the_floors_go_deep() -> None:
    m = _module()
    rows = [{"ticker": "aapl", "px": 250.0, "dv": 3e8}, {"ticker": "PENY", "px": 1.2, "dv": 5e6},
            {"ticker": "THIN", "px": 40.0, "dv": 1e3}, {"ticker": "BAD", "px": None, "dv": 1e9}]
    assert m.liquid_names(rows, 3.0, 75_000) == ["AAPL"]
    assert m._clean_equity("BRK.B") and not m._clean_equity("CL=F") and not m._clean_equity("BTCUSDT")
