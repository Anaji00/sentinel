"""
tests/test_ollama_metrics.py

Unit tests for Task B8: Ollama call instrumentation and MetricsCollector integration.
"""

import asyncio
import json
from pydantic import BaseModel
from shared.utils.ollama import OllamaClient
from shared.utils.metrics import MetricsCollector


class SampleSchema(BaseModel):
    headline: str
    confidence: int


class MockResponse:
    def __init__(self, json_data, status=200):
        self._json_data = json_data
        self.status = status

    async def json(self):
        return self._json_data

    async def text(self):
        return json.dumps(self._json_data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


class MockSession:
    def __init__(self, target_data):
        self.target_data = target_data

    def post(self, url, **kwargs):
        return MockResponse({"response": json.dumps(self.target_data)})

    def get(self, url, **kwargs):
        return MockResponse({"models": [{"name": "qwen2.5:7b"}]})


async def _async_test_ollama_metrics_instrumentation():
    session = MockSession({"headline": "Test Headline", "confidence": 85})
    client = OllamaClient(session=session, service_name="reasoning_test_service")

    result = await client.infer(
        system_prompt="System instructions",
        user_prompt="User input",
        schema=SampleSchema,
    )

    assert result.headline == "Test Headline"
    assert result.confidence == 85

    summary = MetricsCollector.get_summary()
    counters = summary.get("counters", {})
    latencies = summary.get("latencies", {})

    assert counters.get("ollama_calls_total", 0) > 0
    assert counters.get("ollama_calls_reasoning_test_service", 0) > 0

    assert "ollama_latency_reasoning_test_service" in latencies
    assert "ollama_semaphore_wait_seconds_reasoning_test_service" in latencies

    prom_text = MetricsCollector.to_prometheus_format()
    assert "sentinel_ollama_calls_total" in prom_text
    assert "sentinel_ollama_calls_reasoning_test_service" in prom_text


def test_ollama_metrics_instrumentation():
    asyncio.run(_async_test_ollama_metrics_instrumentation())
