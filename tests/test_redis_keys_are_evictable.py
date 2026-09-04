"""
tests/test_redis_keys_are_evictable.py

Switching Redis to volatile-lru stopped the silent destruction of predictions
and started refusing writes instead:

    command not allowed when used memory > 'maxmemory'
    SET aircraft:last_seen:84b7e5 {...} EX 86400

volatile-lru may only evict keys that carry a TTL. The candle lists carry none:
3,436 of them, up to 1,440 JSON entries each, which is on the order of 700MB and
matches the 744MB peak recorded before the policy change. Redis could not
reclaim any of it, filled, and began rejecting every write -- including the
aircraft:last_seen keys the dark-flight detector scans, which is why that
detector reported "No aircraft tracked in Redis" while 2,454 flight events
arrived in twenty minutes.

The previous policy, allkeys-lru, hid this by evicting whatever it liked -- 83,289
keys, taking every recorded prediction with them. Neither silent loss nor
refused writes is acceptable, and the fix is neither policy: it is that a
cache should say how long it is worth keeping.

Anything written to Redis as a cache needs an expiry. Durable state has no TTL
by design and is exactly what volatile-lru is meant to protect; the mistake was
leaving bulk time-series in that category.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CANDLES = (ROOT / "shared" / "utils" / "candles.py").read_text(encoding="utf-8")
TRADFI = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")


def test_the_policy_still_protects_durable_state():
    """allkeys-lru ate 83,289 keys including every prediction. Going back is
    not the fix for the write refusals."""
    command = [ln for ln in COMPOSE.splitlines()
               if "redis-server" in ln and not ln.lstrip().startswith("#")]
    assert command, "the redis command line disappeared"
    assert "volatile-lru" in command[0]
    assert "allkeys-lru" not in command[0]


def test_the_lua_aggregator_expires_what_it_writes():
    """It builds one list per ticker per timeframe and had no EXPIRE at all."""
    assert 'redis.call("EXPIRE", key' in TRADFI


def test_the_minute_candle_list_expires():
    block = TRADFI.split("pipe.ltrim(redis_list_key, 0, 1439)")[1][:300]
    assert "pipe.expire(redis_list_key" in block


def test_the_crypto_candle_lists_expire():
    block = CANDLES.split("ltrim(candles_tf_key, 0, 199)")[1][:400]
    assert "expire(candles_tf_key" in block


def test_every_trimmed_list_in_candles_also_expires():
    """LTRIM bounds a list's length, not its lifetime. A bounded list with no
    TTL is still permanently resident."""
    trimmed = set(re.findall(r"ltrim\((\w+),", CANDLES))
    expired = set(re.findall(r"expire\((\w+),", CANDLES))
    missing = trimmed - expired
    assert not missing, f"trimmed but never expired, so not evictable: {sorted(missing)}"


def test_the_ttl_outlives_any_consumer_window():
    """The longest window anything reads is a few hundred bars. A week is
    generous and still lets an abandoned key go."""
    for source in (CANDLES, TRADFI):
        assert "604800" in source


def test_the_radar_baselines_expire():
    """3,548 permanent keys, two per evaluated ticker across 11,631 symbols --
    the largest non-evictable family in the instance, and what refused writes
    again after the candle lists were fixed."""
    radar = (ROOT / "services" / "collector-radar" / "main.py").read_text(encoding="utf-8")
    assert "ex=BASELINE_TTL_SEC" in radar
    # Writes only. A read site naming the same key is not a missing expiry.
    writes = [
        ln for ln in radar.splitlines()
        if ".set(" in ln and "sentinel:radar:" in ln and not ln.lstrip().startswith("#")
    ]
    assert writes, "the radar baseline writes disappeared"
    missing = [ln for ln in writes if "ex=" not in ln]
    assert not missing, f"radar baseline written without a TTL: {missing}"


def test_the_radar_ttl_outlives_an_active_ticker():
    """An active ticker rewrites its key every scan, so the expiry must only
    reach the long tail that has stopped trading."""
    # Read from source: this module connects on import, so it cannot be
    # loaded in a test process.
    import re

    radar = (ROOT / "services" / "collector-radar" / "main.py").read_text(encoding="utf-8")
    m = re.search(r"BASELINE_TTL_SEC\s*=\s*(\d+)\s*\*\s*(\d+)", radar)
    assert m, "BASELINE_TTL_SEC not found"
    assert int(m.group(1)) * int(m.group(2)) >= 7 * 86400


def test_last_seen_keys_still_carry_their_own_ttl():
    """These are the keys the gap detectors scan, and the ones the refusals
    were landing on."""
    aviation = (ROOT / "services" / "enrichment" / "enrichers" / "aviation.py").read_text(encoding="utf-8")
    assert "aircraft:last_seen" in aviation
    # The write, not the docstring that mentions it.
    write = aviation.split('f"aircraft:last_seen:{icao24}"')[1][:400]
    assert "86400" in write
