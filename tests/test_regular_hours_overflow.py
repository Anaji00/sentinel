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


def test_trade_ideas_are_priced_first_whatever_the_watchlist_holds() -> None:
    import asyncio
    import json as _json
    m = _module()

    class Raw:
        async def get(self, key):
            if key.endswith("recommendations:latest"):
                return _json.dumps({"horizons": {"weekly": {"ideas": [{"ticker": "MU"}, {"ticker": "crm"}]}}})
            return _json.dumps({"board": [{"ticker": "AAA", "net": 1}, {"ticker": "BBB", "net": -5}]})

    class Redis:
        raw = Raw()

    async def no_db():
        raise RuntimeError("no database in this test")

    import shared.db as dbm
    orig = dbm.get_timescale
    dbm.get_timescale = no_db
    try:
        out = asyncio.run(m._attention_tickers(Redis()))
    finally:
        dbm.get_timescale = orig
    assert out[:2] == ["MU", "CRM"] and out[2:] == ["BBB", "AAA"]


def test_snapshots_are_requested_a_hundred_at_a_time() -> None:
    import asyncio
    m = _module()
    calls = []

    class Resp:
        def __init__(self, url):
            self.url, self.status = url, 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            syms = self.url.split("symbols=")[1].split("&")[0].split(",")
            return {s: {"minuteBar": {"c": 1}} for s in syms}

    class Session:
        def get(self, url):
            calls.append(url)
            return Resp(url)

    async def run():
        async with m._snapshot_batches(Session(), [f"S{i}" for i in range(250)], "iex") as resp:
            return resp.status, await resp.json()

    status, data = asyncio.run(run())
    assert status == 200 and len(calls) == 3 and len(data) == 250


def test_a_block_at_the_last_price_takes_the_last_price_changes_side() -> None:
    """Zero-tick rule: 53% of blocks were left without a side (8 Oct 2026)."""
    mod = _load_collector() if "_load_collector" in globals() else None
    import importlib.util
    import pathlib
    import sys
    import types

    root = pathlib.Path(__file__).resolve().parents[1]
    for name in ("websockets",):
        sys.modules.setdefault(name, types.ModuleType(name))
    spec = importlib.util.spec_from_file_location("tradfi_tick", root / "services" / "collector-tradfi" / "main.py")
    mod = mod or importlib.util.module_from_spec(spec)
    if not hasattr(mod, "classify_tick"):
        spec.loader.exec_module(mod)
    last = {}
    mod._LAST_TICK.clear()
    assert mod.classify_tick("X", 10.0, last) == "ZeroTick"     # first print: unknown
    assert mod.classify_tick("X", 10.1, last) == "UpTick"
    assert mod.classify_tick("X", 10.1, last) == "UpTick"       # unchanged: inherits
    assert mod.classify_tick("X", 10.0, last) == "DownTick"
    assert mod.classify_tick("X", 10.0, last) == "DownTick"
