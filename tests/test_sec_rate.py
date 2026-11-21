"""Every bulk SEC loop in a process shares one pace (8 Oct 2026: ~13/s combined)."""
import asyncio
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import sec_rate  # noqa: E402


def test_concurrent_loops_share_one_rate(monkeypatch) -> None:
    monkeypatch.setattr(sec_rate, "BULK_REQUESTS_PER_SEC", 50.0)
    monkeypatch.setattr(sec_rate, "_lock", None)
    monkeypatch.setattr(sec_rate, "_next_at", 0.0)
    monkeypatch.setenv("SEC_SHARED_SLOT", "0")                    # this test is about the per-process pace; the shared clock has its own test below

    async def loop(n):
        for _ in range(n):
            await sec_rate.sec_slot()

    async def main():
        t = time.monotonic()
        await asyncio.gather(loop(10), loop(10), loop(10))      # three loops, 30 requests
        return time.monotonic() - t

    elapsed = asyncio.run(main())
    assert elapsed >= 29 / 50.0 * 0.95                            # one shared pace, not three


def test_two_processes_share_one_host_wide_budget(monkeypatch) -> None:
    """Review F9: two processes against one Redis clock never exceed GLOBAL_REQUESTS_PER_SEC in total (a fake Redis runs the same reservation as the Lua script)."""
    monkeypatch.setattr(sec_rate, "GLOBAL_REQUESTS_PER_SEC", 100.0)
    monkeypatch.setattr(sec_rate, "BULK_REQUESTS_PER_SEC", 1000.0)

    class FakeRaw:
        nxt = 0.0

        async def eval(self, lua, n, key, step_ms):
            now = time.monotonic() * 1000
            self.nxt = max(self.nxt, now)
            wait = self.nxt - now
            self.nxt += float(step_ms)
            return wait

    raw = FakeRaw()

    async def provider():
        return raw

    monkeypatch.setattr(sec_rate, "_raw_provider", provider)
    monkeypatch.delenv("SEC_SHARED_SLOT", raising=False)

    async def process(n):
        monkeypatch.setattr(sec_rate, "_lock", None)              # each "process" has its own local lock and clock
        for _ in range(n):
            await sec_rate.sec_slot()

    async def main():
        t = time.monotonic()
        await asyncio.gather(process(20), process(20))            # 40 requests from two processes
        return time.monotonic() - t

    assert asyncio.run(main()) >= 39 / 100.0 * 0.9                # one 100/s budget for both, not 100/s each


def test_a_redis_failure_falls_back_to_the_local_pace(monkeypatch) -> None:
    async def broken():
        raise ConnectionError("redis down")

    monkeypatch.setattr(sec_rate, "_raw_provider", broken)
    monkeypatch.delenv("SEC_SHARED_SLOT", raising=False)
    asyncio.run(sec_rate.sec_slot())                              # returns, does not raise
