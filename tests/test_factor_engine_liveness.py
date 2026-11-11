"""The factor engine's heartbeat, the stale watchdog and the trader's halt (shared/utils/engine_liveness.py)."""

import asyncio
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import auto_trader as atr  # noqa: E402
from services.agents import factor_watchdog as fw  # noqa: E402
from shared.utils import engine_liveness as el  # noqa: E402

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)      # a Wednesday, inside market hours


def beat(seconds_ago: float) -> str:
    return json.dumps({"component": el.COMPONENT, "ts": (NOW - timedelta(seconds=seconds_ago)).isoformat()})


class Raw:
    def __init__(self) -> None:
        self.d: dict = {}

    async def get(self, k):
        return self.d.get(k)

    async def set(self, k, v, ex=None, nx=False):
        if nx and k in self.d:
            return None
        self.d[k] = v
        return True

    async def delete(self, k):
        self.d.pop(k, None)


class Redis:
    def __init__(self) -> None:
        self.raw = Raw()


def test_verdict_uses_the_180_second_limit_and_tells_old_from_gone() -> None:
    assert el.verdict(60)[0] == "ok" and el.verdict(180)[0] == "ok"
    assert el.verdict(181)[0] == "stale" and "181s" in el.verdict(181)[1]
    assert el.verdict(None)[0] == "missing"
    assert el.heartbeat_age(beat(90), NOW) == 90.0
    assert el.heartbeat_age("not json", NOW) is None and el.heartbeat_age(None, NOW) is None
    assert el.HEARTBEAT_SEC == 60 and el.HEARTBEAT_TTL_SEC > el.STALE_AFTER_SEC


def test_market_hours_are_weekdays_between_the_open_and_the_close() -> None:
    assert el.in_market_hours(NOW) is True
    assert el.in_market_hours(datetime(2026, 11, 14, 15, 0, tzinfo=timezone.utc)) is False      # Saturday
    assert el.in_market_hours(datetime(2026, 11, 11, 3, 0, tzinfo=timezone.utc)) is False


def _watchdog(redis):
    w = fw.FactorWatchdog(None, redis)
    sent = []
    w._alert = lambda why, state, now: _record(sent, why, state, now)       # type: ignore[assignment]
    return w, sent


async def _record(sent, why, state, now):
    sent.append((state, why))
    return True


def test_a_fresh_beat_clears_the_halt_and_a_silent_one_sets_it_and_alerts_once() -> None:
    r = Redis()
    w, sent = _watchdog(r)
    r.raw.d[f"sentinel:heartbeat:{el.COMPONENT}"] = beat(30)
    assert asyncio.run(w.check(NOW)) == "ok" and el.HALT_KEY not in r.raw.d and el.SEEN_KEY in r.raw.d
    r.raw.d[f"sentinel:heartbeat:{el.COMPONENT}"] = beat(400)
    assert asyncio.run(w.check(NOW)) == "stale" and el.HALT_KEY in r.raw.d and len(sent) == 1
    assert asyncio.run(w.check(NOW)) == "stale" and len(sent) == 1               # one alert per outage, the halt is renewed
    r.raw.d[f"sentinel:heartbeat:{el.COMPONENT}"] = beat(20)
    assert asyncio.run(w.check(NOW)) == "ok" and el.HALT_KEY not in r.raw.d


def test_a_heartbeat_never_seen_is_not_an_outage_but_one_that_vanishes_after_being_seen_is() -> None:
    r = Redis()
    w, sent = _watchdog(r)
    assert asyncio.run(w.check(NOW)) == "unseen" and not sent and el.HALT_KEY not in r.raw.d
    r.raw.d[el.SEEN_KEY] = "1"
    assert asyncio.run(w.check(NOW)) == "missing" and len(sent) == 1 and el.HALT_KEY in r.raw.d


def test_the_alert_is_elevated_in_market_hours_alert_outside_and_never_intelligence() -> None:
    src = (ROOT / "services/agents/factor_watchdog.py").read_text(encoding="utf-8")
    assert "AlertTier.ELEVATED if market else AlertTier.ALERT" in src and "INTELLIGENCE" not in src.replace("never INTELLIGENCE", "")
    assert '"STALE_FACTOR_ENGINE"' in src


def test_the_trader_skips_orders_while_the_flag_is_set_or_the_beat_is_stale_and_resumes_after() -> None:
    t = atr.AutoTrader.__new__(atr.AutoTrader)
    t.raw = Raw()
    assert asyncio.run(t._engine_stale()) is None                                # nothing known: trading is not blocked
    t.raw.d[el.HALT_KEY] = "silent for 400s"
    assert "silent" in asyncio.run(t._engine_stale())
    del t.raw.d[el.HALT_KEY]
    t.raw.d[el.SEEN_KEY] = "1"
    t.raw.d[f"sentinel:heartbeat:{el.COMPONENT}"] = json.dumps(
        {"ts": (datetime.now(timezone.utc) - timedelta(seconds=400)).isoformat()})
    assert asyncio.run(t._engine_stale()) is not None                            # the beat task died while the loop lives
    t.raw.d[f"sentinel:heartbeat:{el.COMPONENT}"] = json.dumps({"ts": datetime.now(timezone.utc).isoformat()})
    assert asyncio.run(t._engine_stale()) is None


def test_the_trader_check_sits_after_the_kill_switch_and_before_any_order() -> None:
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    run_once = src[src.index("async def run_once"):]
    assert run_once.index("await self._halted()") < run_once.index("await self._engine_stale()") < run_once.index("await self._send(")
    # transient: the stale path must not write a persistent halt
    stale_block = run_once[run_once.index("await self._engine_stale()"):run_once.index("reasons:")]
    assert "_halt(" not in stale_block


def test_it_is_wired_into_both_services() -> None:
    fe = (ROOT / "services/agents/factor_engine.py").read_text(encoding="utf-8")
    assert "start_heartbeat_task(redis, el.COMPONENT, interval=el.HEARTBEAT_SEC" in fe
    mn = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
    assert "FactorWatchdog(" in mn and 'tier_filter in ("all", "fast")' in mn
    hb = (ROOT / "shared/utils/heartbeat.py").read_text(encoding="utf-8")
    assert '"factor-engine"' in hb
