"""Every model call's tokens are metered by caller and model, for the API cost estimate."""

import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import ollama as ol  # noqa: E402


def test_one_response_adds_its_tokens() -> None:
    f = ol.usage_fields("quant_trading_engine.py:_write_thesis", "qwen2.5:7b",
                        {"prompt_eval_count": 1800, "eval_count": 240, "total_duration": 95_000_000_000})
    assert f == {"quant_trading_engine.py:_write_thesis|qwen2.5:7b|calls": 1,
                 "quant_trading_engine.py:_write_thesis|qwen2.5:7b|in": 1800,
                 "quant_trading_engine.py:_write_thesis|qwen2.5:7b|out": 240,
                 "quant_trading_engine.py:_write_thesis|qwen2.5:7b|ms": 95_000}


class _Pipe:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def hincrby(self, k, f, v):
        self.store.setdefault(k, {})
        self.store[k][f] = self.store[k].get(f, 0) + v

    def expire(self, k, ttl):
        pass

    async def execute(self):
        return []


class _Redis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return _Pipe(self.store)


def test_usage_accumulates_per_day_and_never_raises() -> None:
    r = _Redis()
    ol.bind_rate_store(r)
    try:
        for _ in range(3):
            asyncio.run(ol.record_usage("site", "m", {"prompt_eval_count": 10, "eval_count": 2}))
        (day, fields), = r.store.items()
        assert day.startswith("sentinel:llm_usage:") and fields["site|m|calls"] == 3 and fields["site|m|in"] == 30
        asyncio.run(ol.record_usage("site", "m", "not a dict"))       # ignored, no error
    finally:
        ol.bind_rate_store(None)


def test_each_resident_model_gets_its_share_of_the_cores(monkeypatch) -> None:
    assert ol.model_threads("qwen2.5:7b=4, qwen2.5:3b=2,bad,=3,x=0") == {"qwen2.5:7b": 4, "qwen2.5:3b": 2}
    monkeypatch.setenv("OLLAMA_MODEL_THREADS", "qwen2.5:7b=4,qwen2.5:3b=2")
    assert ol.threads_for("qwen2.5:7b") == 4 and ol.threads_for("qwen2.5:3b") == 2


def test_usage_is_charged_to_the_agent_not_the_shared_base_frame() -> None:
    from shared.utils import ollama as ol

    class _Agent:
        name = "analyst_agent"

        def call(self) -> str:
            return ol._usage_site()

    # Outside services/agents/ the label is file:function, as before.
    assert ol._usage_site().startswith("test_llm_usage_meter.py:")
    _Agent.call.__code__ = _Agent.call.__code__.replace(co_filename="/app/services/agents/analyst_agent.py")
    assert _Agent().call() == "analyst_agent"
