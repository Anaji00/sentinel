"""19 Nov 2026 had no orders and the cause was unrecoverable: the trader left no record of a skipped day (layer audit Addendum 3,
finding 1). One Redis hash a day, written after every attempt, recording only: no sizing, cap, kill-switch or planning logic."""
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from services.agents import auto_trader as tr

NY = ZoneInfo("America/New_York")
AFTER = datetime(2026, 11, 19, 10, 5, tzinfo=NY)
KEY = tr.RUN_RECORD_KEY.format(portfolio=tr.PORTFOLIO, day="2026-11-19")


class _Raw:
    def __init__(self, fail=False):
        self.h, self.kv, self.fail = {}, {}, fail

    async def hset(self, k, mapping):
        if self.fail:
            raise ConnectionError("redis down")
        self.h.setdefault(k, {}).update(mapping)

    async def exists(self, k):
        return int(k in self.h)

    async def expire(self, k, s):
        return True

    async def get(self, k):
        return self.kv.get(k)


def _trader(out, behind=4, fail=False):
    t = tr.AutoTrader(None, None, broker=None)
    t.raw = _Raw(fail)

    async def run_once(now=None):
        return out

    async def sessions():
        return behind

    t.run_once, t._scores_sessions_behind = run_once, sessions
    return t


def test_a_run_skipped_for_stale_scores_leaves_a_record_with_the_reason():
    reason = "scores are 4 completed sessions behind"
    t = _trader({"day": "2026-11-19", "halted": reason})
    assert asyncio.run(t._run_once_recorded(AFTER)) == {"day": "2026-11-19", "halted": reason}
    rec = t.raw.h[KEY]
    assert rec["skip_reason"] == reason and rec["halted"] == "1" and rec["orders_submitted"] == "0"
    assert rec["scores_sessions_behind"] == "4" and rec["day"] == "2026-11-19"


def test_a_traded_day_records_its_order_count_and_no_reason():
    t = _trader({"day": "2026-11-19", "orders": 93, "failed": 1}, behind=0)
    asyncio.run(t._run_once_recorded(AFTER))
    rec = t.raw.h[KEY]
    assert rec["orders_submitted"] == "93" and rec["skip_reason"] == "" and rec["halted"] == "0"


def test_a_silent_return_is_recorded_once_and_never_overwrites_a_real_record():
    t = _trader({})
    asyncio.run(t._run_once_recorded(AFTER))
    assert t.raw.h[KEY]["skip_reason"].startswith("no_run")
    t.raw.h[KEY]["skip_reason"] = "kept"
    asyncio.run(t._run_once_recorded(AFTER))
    assert t.raw.h[KEY]["skip_reason"] == "kept"


def test_before_the_trade_time_nothing_is_recorded():
    t = _trader({})
    asyncio.run(t._run_once_recorded(datetime(2026, 11, 19, 8, 0, tzinfo=NY)))
    assert not t.raw.h


def test_a_failed_write_is_counted_and_never_changes_the_run_result():
    t = _trader({"day": "2026-11-19", "orders": 5}, fail=True)
    assert asyncio.run(t._run_once_recorded(AFTER))["orders"] == 5
