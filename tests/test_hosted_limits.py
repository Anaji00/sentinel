"""Free-tier limits in shared/utils/hosted_llm.py: sliding minute, cooldown on every kind of refusal, Retry-After forms."""

import asyncio
import pathlib
import sys
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, List

import aiohttp

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import hosted_llm as h  # noqa: E402

STEP = "groq/openai/gpt-oss-20b"


def test_retry_after_in_seconds_as_a_date_or_in_googles_body() -> None:
    assert h.retry_seconds("30") == 30 and h.retry_seconds("1.5") == 1
    when = datetime(2026, 11, 11, 12, 0, 45, tzinfo=timezone.utc)
    assert h.retry_seconds(format_datetime(when, usegmt=True), now=when.timestamp() - 40) == 40
    assert h.retry_seconds(format_datetime(when, usegmt=True), now=when.timestamp() + 99) == 0         # a date in the past
    body = '{"error": {"details": [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "17s"}]}}'
    assert h.retry_seconds(None, body) == 18
    assert h.retry_seconds("soon", "no delay here") is None and h.retry_seconds(None) is None


def test_a_burst_across_a_minute_boundary_counts_against_the_sliding_minute() -> None:
    """8,000 tokens used at :59 and 8,000 more at :01 is 16,000 inside 60 seconds; fixed calendar minutes allowed it."""
    t0 = 1_800_000_000 - (1_800_000_000 % 60)                 # a minute boundary
    used = {"rm": 0, "rd": 0, "tm": 0, "td": 8000}
    prev = {"rm": 4, "tm": 8000}
    near_start = h.sliding(used, prev, t0 + 1)
    near_end = h.sliding(used, prev, t0 + 59)
    assert near_start["tm"] > 7800 and near_start["rm"] == 4
    assert near_end["tm"] < 300 and near_end["rm"] == 0
    q = h.Quota(rpm=30, rpd=0, tpm=8000, tpd=0)
    assert h.over_quota(near_start, q, 2000) == "tpm"
    assert h.over_quota(near_end, q, 2000) is None


def test_reserve_uses_the_previous_minute_in_process(monkeypatch) -> None:
    h._local_counts.clear()
    t0 = 1_800_000_000 - (1_800_000_000 % 60)
    monkeypatch.setattr(h.time, "time", lambda: t0 + 2.0)
    pm = h.prev_minute(t0 + 2.0)
    h._local_counts[h.BUCKET_KEY.format(step=STEP, unit="t", window=pm)] = 7900
    q = h.Quota(rpm=0, rpd=0, tpm=8000, tpd=0)
    assert asyncio.run(h._reserve(None, STEP, q, 1500)) == "tpm"
    monkeypatch.setattr(h.time, "time", lambda: t0 + 58.0)
    assert asyncio.run(h._reserve(None, STEP, q, 1500)) is None
    assert any(k.endswith(pm) for k in h._local_counts), "the previous minute was swept as stale while the sliding window needed it"


class _Resp:
    def __init__(self, status: int, body: str = "", headers: Dict[str, str] = None) -> None:     # type: ignore[assignment]
        self.status, self._body, self.headers = status, body, headers or {}

    async def __aenter__(self) -> "_Resp":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    async def text(self) -> str:
        return self._body

    async def json(self, **_: Any) -> Dict[str, Any]:
        return {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}}


class _Session:
    def __init__(self, outcome: Any) -> None:
        self.outcome, self.calls = outcome, 0

    def post(self, *_: Any, **__: Any) -> _Resp:
        self.calls += 1
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def _go(session: _Session) -> Any:
    return asyncio.run(h.complete(session, None, STEP, "p", 0.0, None, 50, site="t"))


def _setup(monkeypatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "k")
    monkeypatch.delenv("LLM_QUOTAS", raising=False)
    monkeypatch.delenv("LLM_FALLBACK_SITES", raising=False)
    h._local_cooldown.clear()
    h._local_counts.clear()


def test_a_503_a_refusal_and_a_timeout_each_cool_the_step_so_the_next_call_does_not_resend(monkeypatch) -> None:
    for outcome, lo, hi in ((_Resp(503), 25, 31), (_Resp(503, headers={"retry-after": "90"}), 85, 91),
                            (_Resp(404, "model removed"), 295, 301), (asyncio.TimeoutError(), 25, 31),
                            (aiohttp.ClientConnectionError("down"), 25, 31)):
        _setup(monkeypatch)
        s = _Session(outcome)
        assert _go(s) is None and s.calls == 1
        remaining = h._local_cooldown[STEP] - h.time.time()
        assert lo <= remaining <= hi, (outcome, remaining)
        assert _go(s) is None and s.calls == 1, "a cooling step was sent to again"


def test_a_429_honours_a_retry_after_given_as_a_date(monkeypatch) -> None:
    _setup(monkeypatch)
    at = datetime.fromtimestamp(h.time.time() + 120, timezone.utc)
    s = _Session(_Resp(429, headers={"retry-after": format_datetime(at, usegmt=True)}))
    assert _go(s) is None
    assert 110 <= h._local_cooldown[STEP] - h.time.time() <= 121


def test_a_good_answer_leaves_no_cooldown(monkeypatch) -> None:
    _setup(monkeypatch)
    got = _go(_Session(_Resp(200)))
    assert got is not None and got[1] == STEP and STEP not in h._local_cooldown


class _Raw:
    def __init__(self) -> None:
        self.seen: set = set()
        self.hashes: Dict[str, Dict[str, int]] = {}
        self.ttls: List[str] = []

    async def set(self, key: str, value: str, nx: bool = False, ex: int = 0) -> Any:
        if nx and key in self.seen:
            return None
        self.seen.add(key)
        return True

    def pipeline(self) -> "_Raw":
        self._ops: list = []
        return self

    async def __aenter__(self) -> "_Raw":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    def hincrby(self, key: str, field: str, n: int) -> None:
        self._ops.append(("h", key, field, n))

    def expire(self, key: str, s: int) -> None:
        self._ops.append(("e", key, s, 0))

    async def execute(self) -> None:
        for op, key, a, n in self._ops:
            if op == "h":
                self.hashes.setdefault(key, {})[a] = self.hashes.get(key, {}).get(a, 0) + n
            else:
                self.ttls.append(key)


def test_repeated_prompts_are_counted_by_caller_before_any_cache_is_built() -> None:
    raw = _Raw()
    for text in ("a", "a", "b", "a"):
        asyncio.run(h._note_prompt(raw, "analyst_shadow", text))
    asyncio.run(h._note_prompt(raw, "llm_news", "a"))
    (key,) = raw.hashes
    assert raw.hashes[key]["analyst_shadow|calls"] == 4 and raw.hashes[key]["analyst_shadow|repeats"] == 2
    assert raw.hashes[key]["llm_news|calls"] == 1 and "llm_news|repeats" not in raw.hashes[key] or raw.hashes[key]["llm_news|repeats"] == 1
    assert key in raw.ttls, "the counter hash must expire"
    asyncio.run(h._note_prompt(None, "x", "y"))                  # no Redis: nothing happens, nothing raises
