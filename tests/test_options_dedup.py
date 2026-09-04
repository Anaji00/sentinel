"""One option fill must be published once.

The collector reads snapshot.latestTrade, which is the contract's last trade and
does not change between polls unless the contract trades again -- and the loop
republished it every cycle regardless. The macro collector had the identical
defect against the identical Alpaca field and was fixed with a fingerprint; this
path was missed.

Measured over 24 hours: 15,970 options events carrying 1,392 distinct contracts,
91.3% duplicates, one contract published 200 times. Every copy was scored,
correlated and counted, so a single $5.8m TEX sweep became 200 separate
anomalies at a score of 1.000 -- and those 201 ceiling events were most of the
top decile of the entire options distribution.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-tradfi"))

_spec = importlib.util.spec_from_file_location(
    "tradfi_main", ROOT / "services" / "collector-tradfi" / "main.py"
)


def _mod():
    if "tradfi_main" in sys.modules:
        return sys.modules["tradfi_main"]
    m = importlib.util.module_from_spec(_spec)
    sys.modules["tradfi_main"] = m
    _spec.loader.exec_module(m)
    return m


def _fp(contract, ts, price, size):
    return (contract, ts, price, size)


def test_an_unchanged_trade_is_recognised():
    m = _mod()
    m._last_option_trade.clear()
    a = _fp("TEX260918C00045000", "2026-09-02T15:30:00Z", 36.7, 1575.0)
    m._last_option_trade["TEX260918C00045000"] = a
    assert m._last_option_trade.get("TEX260918C00045000") == a


def test_a_new_fill_differs_by_timestamp():
    a = _fp("TEX260918C00045000", "2026-09-02T15:30:00Z", 36.7, 1575.0)
    b = _fp("TEX260918C00045000", "2026-09-02T15:31:00Z", 36.7, 1575.0)
    assert a != b, "a genuine later fill must not be suppressed"


def test_a_repriced_fill_differs_even_at_the_same_timestamp():
    """A feed that omits or repeats the timestamp still deduplicates on the
    trade itself rather than swallowing a different fill."""
    a = _fp("X", None, 36.7, 1575.0)
    assert a != _fp("X", None, 40.0, 1575.0)
    assert a != _fp("X", None, 36.7, 2000.0)


def test_different_contracts_never_collide():
    assert _fp("A", "t", 1.0, 1.0) != _fp("B", "t", 1.0, 1.0)


def test_the_fingerprint_map_is_pruned():
    """Contracts expire and the chain rolls, so this would grow for the life of
    the process."""
    m = _mod()
    m._last_option_trade.clear()
    for i in range(m._MAX_OPTION_FINGERPRINTS + 100):
        m._last_option_trade[f"C{i}"] = (f"C{i}", "t", 1.0, 1.0)
    m._prune_option_fingerprints()
    assert len(m._last_option_trade) <= m._MAX_OPTION_FINGERPRINTS
    assert len(m._last_option_trade) > 0, "pruning emptied the map entirely"


def test_pruning_keeps_the_most_recent_entries():
    """Insertion order means the oldest are the contracts that stopped trading."""
    m = _mod()
    m._last_option_trade.clear()
    for i in range(m._MAX_OPTION_FINGERPRINTS + 100):
        m._last_option_trade[f"C{i}"] = (f"C{i}", "t", 1.0, 1.0)
    m._prune_option_fingerprints()
    newest = f"C{m._MAX_OPTION_FINGERPRINTS + 99}"
    assert newest in m._last_option_trade


def test_pruning_is_a_no_op_below_the_cap():
    m = _mod()
    m._last_option_trade.clear()
    m._last_option_trade["A"] = ("A", "t", 1.0, 1.0)
    m._prune_option_fingerprints()
    assert len(m._last_option_trade) == 1


def test_the_prune_is_actually_called():
    """A bounded structure nothing prunes is unbounded."""
    source = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    calls = [
        line for line in source.splitlines()
        if "_prune_option_fingerprints()" in line and not line.strip().startswith(("#", "def "))
    ]
    assert calls, "the prune helper is defined and never invoked"


# ── Survives a restart ────────────────────────────────────────────────────────
#
# The fingerprint map was process memory, so every deploy republished the whole
# chain once. The aviation gap detector had the identical shape earlier in this
# audit -- 462 of 464 events turned out to be redeploys re-emitting a backlog --
# and the fix there was this same move to Redis.


class _Raw:
    def __init__(self):
        self.store = {}
    async def get(self, k):
        return self.store.get(k)
    async def set(self, k, v, ex=None):
        self.store[k] = v


class _Redis:
    def __init__(self):
        self.raw = _Raw()


class _BrokenRaw:
    async def get(self, k):
        raise RuntimeError('connection reset')
    async def set(self, k, v, ex=None):
        raise RuntimeError('connection reset')


class _BrokenRedis:
    def __init__(self):
        self.raw = _BrokenRaw()


def test_a_repeat_is_suppressed_across_a_restart():
    import asyncio
    m = _mod()
    r = _Redis()
    first = asyncio.run(m._option_trade_is_new(r, 'TEX260918C00045000', 't1|36.7|1575'))
    # A restart clears process memory; Redis keeps the fingerprint.
    m._last_option_trade.clear()
    second = asyncio.run(m._option_trade_is_new(r, 'TEX260918C00045000', 't1|36.7|1575'))
    assert first is True and second is False


def test_a_genuine_new_fill_still_publishes():
    import asyncio
    m = _mod()
    r = _Redis()
    asyncio.run(m._option_trade_is_new(r, 'X', 't1|1|1'))
    assert asyncio.run(m._option_trade_is_new(r, 'X', 't2|1|1')) is True


def test_redis_failure_falls_back_rather_than_raising():
    """A dedup outage must not stop the collector publishing."""
    import asyncio
    m = _mod()
    m._last_option_trade.clear()
    b = _BrokenRedis()
    assert asyncio.run(m._option_trade_is_new(b, 'Y', 'f1')) is True
    assert asyncio.run(m._option_trade_is_new(b, 'Y', 'f1')) is False


def test_no_redis_at_all_uses_the_process_map():
    import asyncio
    m = _mod()
    m._last_option_trade.clear()
    assert asyncio.run(m._option_trade_is_new(None, 'Z', 'f1')) is True
    assert asyncio.run(m._option_trade_is_new(None, 'Z', 'f1')) is False


def test_the_fingerprint_expires():
    """Contracts expire and the chain rolls."""
    m = _mod()
    assert 0 < m._OPTION_FINGERPRINT_TTL_SEC <= 7 * 86400
