"""A hosted-only model may fall to other hosted models, but only for the callers that opt in (shared/utils/hosted_llm.py)."""

import asyncio
import pathlib
import sys
from typing import Any, Dict, List

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import hosted_llm as h  # noqa: E402

MAIN = "groq/openai/gpt-oss-120b"
CHAIN = "gemini/gemini-3.6-flash,groq/qwen/qwen3.8-27b"


def _env(monkeypatch, sites: str = "scenario_generator") -> None:
    monkeypatch.setenv("LLM_FALLBACKS", f"{MAIN}={CHAIN}")
    monkeypatch.setenv("LLM_FALLBACK_SITES", sites)
    monkeypatch.setenv("GROQ_API_KEY", "test")
    monkeypatch.setenv("GEMINI_API_KEY", "test")
    monkeypatch.delenv("LLM_QUOTAS", raising=False)
    h._local_cooldown.clear()


def test_a_listed_caller_gets_the_chain_after_its_own_model(monkeypatch):
    _env(monkeypatch)
    assert h.route_for(MAIN, "scenario_generator.py:_synthesize") == [MAIN, "gemini/gemini-3.6-flash", "groq/qwen/qwen3.8-27b"]


def test_callers_whose_series_is_per_model_never_get_a_fallback(monkeypatch):
    _env(monkeypatch)
    for site in ("llm_news_job.py:run_once", "analyst_shadow", "activist_13d_job.py:_read", "", "unknown"):
        assert h.route_for(MAIN, site) == [MAIN], site


def test_nothing_changes_until_sites_are_listed(monkeypatch):
    _env(monkeypatch, sites="")
    assert h.route_for(MAIN, "scenario_generator.py:_synthesize") == [MAIN]


def test_the_chain_drops_itself_and_non_hosted_names(monkeypatch):
    monkeypatch.setenv("LLM_FALLBACK_SITES", "x")
    monkeypatch.setenv("LLM_FALLBACKS", f"{MAIN}={MAIN},qwen2.5:7b,nowhere/model,gemini/gemini-3.6-flash")
    assert h.fallback_chain(MAIN, "x.py") == ["gemini/gemini-3.6-flash"]


class _Resp:
    def __init__(self, status: int, body: Dict[str, Any], headers: Dict[str, str]) -> None:
        self.status, self._body, self.headers = status, body, headers

    async def __aenter__(self) -> "_Resp":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    async def json(self, **_: Any) -> Dict[str, Any]:
        return self._body

    async def text(self) -> str:
        return str(self._body)


class _Session:
    """Groq answers 429; Gemini answers."""

    def __init__(self) -> None:
        self.urls: List[str] = []

    def post(self, url: str, **kw: Any) -> _Resp:
        self.urls.append(url)
        if "groq" in url:
            return _Resp(429, {}, {"retry-after": "30"})
        ok = {"choices": [{"message": {"content": '{"x": 1}'}, "finish_reason": "stop"}],
              "usage": {"prompt_tokens": 5, "completion_tokens": 3}}
        return _Resp(200, ok, {})


def test_a_rate_limited_model_falls_to_the_next_provider_and_the_answerer_is_named(monkeypatch):
    _env(monkeypatch)
    s = _Session()
    got = asyncio.run(h.complete(s, None, MAIN, "p", 0.0, None, 50, site="scenario_generator.py:_synthesize"))
    assert got is not None and got[1] == "gemini/gemini-3.6-flash"
    assert len(s.urls) == 2 and "groq" in s.urls[0] and "generativelanguage" in s.urls[1]
    assert h._local_cooldown.get(MAIN, 0) > 0, "the 429 still cools the step down"


def test_a_caller_off_the_list_gets_no_answer_rather_than_another_model(monkeypatch):
    _env(monkeypatch)
    s = _Session()
    assert asyncio.run(h.complete(s, None, MAIN, "p", 0.0, None, 50, site="llm_news_job.py:run_once")) is None
    assert len(s.urls) == 1


def test_the_client_passes_its_call_site_to_the_route():
    src = (ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8")
    assert "site = _usage_site() if hosted_llm.is_hosted(active_model)" in src and "site=site)" in src
