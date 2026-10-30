"""Free hosted inference in front of Ollama (shared/utils/hosted_llm.py).

The free tiers are quota-gated, so the client counts before it sends, and a full
bucket or a 429 moves to the next step without a retry. Off by default; a model
named "provider/model" is hosted-only and never answered by a local stand-in,
because the analyst's shadow is graded as that hosted model.
"""
import asyncio
import pathlib
import sys
from typing import Any, List, Optional

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pydantic import BaseModel  # noqa: E402

from shared.utils import hosted_llm as h  # noqa: E402
from shared.utils import ollama as ol  # noqa: E402

ROUTES = "qwen2.5:7b=groq/openai/gpt-oss-120b,gemini/gemini-3.5-flash-lite;qwen2.5:3b=groq/llama-3.1-8b-instant"


def test_routes_parse_in_order_and_drop_unknown_providers():
    r = h.parse_routes(ROUTES + ";qwen2.5:1.5b=nowhere/x")
    assert r["qwen2.5:7b"] == ["groq/openai/gpt-oss-120b", "gemini/gemini-3.5-flash-lite"]
    assert r["qwen2.5:3b"] == ["groq/llama-3.1-8b-instant"]
    assert "qwen2.5:1.5b" not in r


def test_quotas_parse_and_skip_malformed():
    q = h.parse_quotas("groq/a=30,1000,8000,200000;groq/b=1,2,3")
    assert q == {"groq/a": h.Quota(30, 1000, 8000, 200000)}


def test_over_quota_names_the_limit_broken():
    q = h.Quota(rpm=2, rpd=10, tpm=1000, tpd=0)
    assert h.over_quota({"rm": 1, "rd": 1, "tm": 0, "td": 0}, q, 500) is None
    assert h.over_quota({"rm": 2}, q, 10) == "rpm"
    assert h.over_quota({"tm": 600}, q, 500) == "tpm"
    assert h.over_quota({"td": 10 ** 9}, q, 500) is None, "0 means no limit"


def test_route_is_empty_when_off_but_a_hosted_name_always_routes(monkeypatch):
    monkeypatch.setenv("LLM_ROUTES", ROUTES)
    monkeypatch.setenv("LLM_HOSTED_MODE", "off")
    assert h.route_for("qwen2.5:7b") == []
    assert h.route_for("groq/openai/gpt-oss-120b") == ["groq/openai/gpt-oss-120b"]
    monkeypatch.setenv("LLM_HOSTED_MODE", "on")
    assert h.route_for("qwen2.5:7b")[0] == "groq/openai/gpt-oss-120b"
    assert h.route_for("qwen2.5:14b") == []


def test_a_json_schema_is_stated_in_the_prompt_and_json_mode_set():
    body = h.request_body("openai/gpt-oss-120b", "Q", 0.2, {"type": "object"}, 300)
    assert body["response_format"] == {"type": "json_object"}
    assert "JSON SCHEMA" in body["messages"][0]["content"]
    assert body["reasoning_effort"] == "low" and body["max_tokens"] == 300
    plain = h.request_body("llama-3.1-8b-instant", "Q", 0.2, None, 300)
    assert "response_format" not in plain and "reasoning_effort" not in plain


def test_usage_maps_onto_the_meter_fields():
    u = h.usage_as_ollama({"usage": {"prompt_tokens": 120, "completion_tokens": 30}}, 1.5)
    assert u == {"prompt_eval_count": 120, "eval_count": 30, "total_duration": 1_500_000_000, "done_reason": "stop"}
    cut = h.usage_as_ollama({"choices": [{"finish_reason": "length"}]}, 0.1)
    assert cut["done_reason"] == "length", "a cut-off answer must reach the truncation retry"


def test_no_key_means_the_step_is_skipped(monkeypatch):
    monkeypatch.setenv("LLM_HOSTED_MODE", "on")
    monkeypatch.setenv("LLM_ROUTES", ROUTES)
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    class NoSession:
        def post(self, *_: Any, **__: Any) -> Any:
            raise AssertionError("sent a request without a key")

    assert asyncio.run(h.complete(NoSession(), None, "qwen2.5:7b", "p", 0.1, None, 50)) is None


def test_an_in_process_bucket_refuses_once_full(monkeypatch):
    h._local_counts.clear()
    q = h.Quota(rpm=1, rpd=0, tpm=0, tpd=0)
    assert asyncio.run(h._reserve(None, "groq/x", q, 10)) is None
    assert asyncio.run(h._reserve(None, "groq/x", q, 10)) == "rpm"


class _Out(BaseModel):
    x: int


def _client(hosted: Optional[tuple], local: List[str]) -> ol.OllamaClient:
    c = ol.OllamaClient(None, model="qwen2.5:7b")

    async def fake_complete(*_: Any, **__: Any) -> Optional[tuple]:
        return hosted

    async def no_fallback(*_: Any, **__: Any) -> Optional[str]:
        return None

    async def resolve(model: str, **__: Any) -> str:
        local.append(model)
        raise AssertionError("reached the local model")

    c._get_fallback_model = no_fallback  # type: ignore[method-assign]
    c._resolve_model = resolve  # type: ignore[method-assign]
    return c, fake_complete


def test_a_hosted_answer_is_used_and_metered(monkeypatch):
    monkeypatch.setenv("LLM_HOSTED_MODE", "on")
    monkeypatch.setenv("LLM_ROUTES", ROUTES)
    local: List[str] = []
    c, fake = _client(('{"x": 3}', "groq/openai/gpt-oss-120b", {"eval_count": 1}), local)
    monkeypatch.setattr(h, "complete", fake)
    out = asyncio.run(c.infer("s", "u", _Out))
    assert out.x == 3 and local == []


def test_a_hosted_only_model_never_falls_back_to_a_local_one(monkeypatch):
    local: List[str] = []
    c, fake = _client(None, local)
    monkeypatch.setattr(h, "complete", fake)
    with pytest.raises(ol.InferenceError):
        asyncio.run(c.infer("s", "u", _Out, model="gemini/gemini-3.5-flash-lite", fallback_model="qwen2.5:3b"))
    assert local == [], "a hosted-only call was answered locally"


def test_the_shadow_analyst_is_built_only_for_a_hosted_model():
    src = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
    block = src.split('agent_name="analyst_shadow"')[1].split("agents_by_name")[0]
    assert "if is_hosted(shadow_model) else None" in block and "fallback_model=None" in block


class _PipeRaw:
    """A Redis stand-in with pipelines, for the reservation's increment-first path."""

    def __init__(self):
        self.kv = {}

    def pipeline(self):
        raw = self

        class _P:
            def __init__(self):
                self.ops = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            def incrby(self, k, n):
                self.ops.append(("incr", k, n))

            def decrby(self, k, n):
                self.ops.append(("incr", k, -n))

            def expire(self, k, s):
                self.ops.append(("expire", k, s))

            async def execute(self):
                out = []
                for op, k, n in self.ops:
                    if op == "incr":
                        raw.kv[k] = raw.kv.get(k, 0) + n
                        out.append(raw.kv[k])
                    else:
                        out.append(True)
                return out

        return _P()


def test_reservation_increments_first_and_gives_back_on_overflow():
    raw = _PipeRaw()
    q = h.Quota(rpm=2, rpd=0, tpm=0, tpd=0)
    assert asyncio.run(h._reserve(raw, "groq/x", q, 10)) is None
    assert asyncio.run(h._reserve(raw, "groq/x", q, 10)) is None
    assert asyncio.run(h._reserve(raw, "groq/x", q, 10)) == "rpm"
    assert max(v for k, v in raw.kv.items() if ":r:" in k and len(k.split(":")[-1]) == 12) == 2, "the overflow was rolled back"


def test_in_process_buckets_drop_passed_windows():
    h._local_counts.clear()
    h._local_counts["sentinel:hosted_llm:groq/x:r:200001010000"] = 5
    asyncio.run(h._reserve(None, "groq/x", h.Quota(1, 0, 0, 0), 1))
    assert "sentinel:hosted_llm:groq/x:r:200001010000" not in h._local_counts


def test_capacity_needs_a_key_and_room(monkeypatch):
    monkeypatch.setenv("LLM_HOSTED_MODE", "on")
    monkeypatch.setenv("LLM_ROUTES", "qwen2.5:3b=groq/openai/gpt-oss-20b")
    monkeypatch.setenv("LLM_QUOTAS", "groq/openai/gpt-oss-20b=1,0,0,0")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    h._local_counts.clear()
    assert not asyncio.run(h.has_capacity(None, "qwen2.5:3b"))
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    assert asyncio.run(h.has_capacity(None, "qwen2.5:3b"))
    asyncio.run(h._reserve(None, "groq/openai/gpt-oss-20b", h.Quota(1, 0, 0, 0), 1))
    assert not asyncio.run(h.has_capacity(None, "qwen2.5:3b")), "the minute's one request is spent"
    assert not asyncio.run(h.has_capacity(None, "qwen2.5:7b")), "no route, no capacity"


class _CountRaw:
    def __init__(self):
        self.kv = {}

    async def incr(self, k):
        self.kv[k] = self.kv.get(k, 0) + 1
        return self.kv[k]

    async def expire(self, *_):
        return True


def test_each_agent_has_an_hourly_share_of_hosted_calls():
    from services.agents import base
    from services.agents.analyst_agent import AnalystAgent
    a = AnalystAgent.__new__(AnalystAgent)
    a.name, a.logger = "stock_correlation_agent", __import__("logging").getLogger("t")
    raw = _CountRaw()
    share = base.hosted_share("stock_correlation_agent")
    turns = [asyncio.run(a._hosted_turn(raw)) for _ in range(share + 3)]
    assert turns.count(True) == share and turns[-1] is False
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert "await self._hosted_turn(_raw)" in src and "self._inference_budget.passes_bar(" in src


def test_shares_follow_importance_and_the_reserve_spares_priority_callers(monkeypatch):
    from services.agents import base
    monkeypatch.delenv("HOSTED_SHARES", raising=False)
    assert base.hosted_share("analyst_shadow") > base.hosted_share("stock_correlation_agent") > base.hosted_share("radar_agent")
    monkeypatch.setenv("HOSTED_SHARES", "radar_agent=9,bad")
    assert base.hosted_share("radar_agent") == 9
    monkeypatch.delenv("HOSTED_PRIORITY", raising=False)
    assert "analyst_shadow" in base.hosted_priority() and "stock_correlation_agent" not in base.hosted_priority()
    monkeypatch.setenv("LLM_HOSTED_MODE", "on")
    monkeypatch.setenv("LLM_ROUTES", "qwen2.5:3b=groq/openai/gpt-oss-20b")
    monkeypatch.setenv("LLM_QUOTAS", "groq/openai/gpt-oss-20b=10,0,0,0")
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    h._local_counts.clear()
    for _ in range(7):
        asyncio.run(h._reserve(None, "groq/openai/gpt-oss-20b", h.Quota(10, 0, 0, 0), 1))
    assert asyncio.run(h.has_capacity(None, "qwen2.5:3b", reserve=0.0)), "3 of 10 left for a priority caller"
    assert not asyncio.run(h.has_capacity(None, "qwen2.5:3b", reserve=0.3)), "a reserve of 3 is not a lower caller's"
