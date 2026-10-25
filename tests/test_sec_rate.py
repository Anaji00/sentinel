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

    async def loop(n):
        for _ in range(n):
            await sec_rate.sec_slot()

    async def main():
        t = time.monotonic()
        await asyncio.gather(loop(10), loop(10), loop(10))      # three loops, 30 requests
        return time.monotonic() - t

    elapsed = asyncio.run(main())
    assert elapsed >= 29 / 50.0 * 0.95                            # one shared pace, not three
