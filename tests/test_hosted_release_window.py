"""Unused tokens go back to the minute they were reserved in, not the minute the call returned in (shared/utils/hosted_llm.py)."""

import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import hosted_llm as h  # noqa: E402

STEP = "groq/x"
Q = h.Quota(30, 0, 8000, 0)
RESERVED_AT = 1_779_999_960.0 + 59.0            # 1_779_999_960 is a whole minute: this is :59, one second before the next
RETURNED_AT = RESERVED_AT + 3.0                  # the call comes back in the next minute


class Pipe:
    def __init__(self, store):
        self.store, self.ops = store, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def incrby(self, k, n):
        self.ops.append(("incr", k, n))

    def decrby(self, k, n):
        self.ops.append(("incr", k, -n))

    def expire(self, k, s):
        self.ops.append(("expire", k, 0))

    async def execute(self):
        out = []
        for op, k, n in self.ops:
            if op == "expire":
                out.append(True)                    # the real pipeline returns a result for every queued command
                continue
            self.store[k] = self.store.get(k, 0) + n
            out.append(self.store[k])
        return out


class FakeRaw:
    def __init__(self):
        self.d = {}

    def pipeline(self):
        return Pipe(self.d)

    async def mget(self, keys):
        return [self.d.get(k) for k in keys]


def test_a_release_after_the_minute_turned_goes_back_to_the_reserved_minute(monkeypatch) -> None:
    raw = FakeRaw()
    monkeypatch.setattr(h.time, "time", lambda: RETURNED_AT)           # the call returns in the next minute
    assert asyncio.run(h._reserve(raw, STEP, Q, 2300, now=RESERVED_AT)) is None
    asyncio.run(h._release(raw, STEP, Q, 1000, at=RESERVED_AT))
    reserved_minute = h.BUCKET_KEY.format(step=STEP, unit="t", window=h.windows(RESERVED_AT)["m"])
    returned_minute = h.BUCKET_KEY.format(step=STEP, unit="t", window=h.windows(RETURNED_AT)["m"])
    assert raw.d[reserved_minute] == 1300                              # reserved 2,300, gave back 1,000
    assert raw.d.get(returned_minute, 0) == 0                          # the next minute is untouched, never negative


def test_the_old_behaviour_would_have_gone_negative(monkeypatch) -> None:
    raw = FakeRaw()
    monkeypatch.setattr(h.time, "time", lambda: RETURNED_AT)
    asyncio.run(h._reserve(raw, STEP, Q, 2300, now=RESERVED_AT))
    asyncio.run(h._release(raw, STEP, Q, 1000))                         # no `at`: counted where the call returned
    returned_minute = h.BUCKET_KEY.format(step=STEP, unit="t", window=h.windows(RETURNED_AT)["m"])
    assert raw.d[returned_minute] == -1000                              # the defect, kept as a test of the default


def test_complete_passes_the_reservation_time_to_the_release() -> None:
    src = (ROOT / "shared/utils/hosted_llm.py").read_text(encoding="utf-8")
    assert "now=reserved_at" in src and "at=reserved_at" in src
