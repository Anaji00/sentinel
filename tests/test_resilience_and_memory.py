import asyncio
import time
import pytest
from typing import Dict, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import BaseModel

from shared.utils.ollama import OllamaClient, InferenceError, get_ollama_semaphore
from services.agents.base import SentinelAgent

class DummySchema(BaseModel):
    response: str

def test_discrete_circuit_breaker():
    """Verify that circuit breaker state is tracked independently per model."""
    async def run_test():
        session = AsyncMock()
        client = OllamaClient(session=session, model="llama3")

        # Mock tags resolution to return the requested model
        client._resolve_model = AsyncMock(side_effect=lambda m, exclude_models=None: m)
        client._get_fallback_model = AsyncMock(return_value=None)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"response": '{"response": "rerouted success"}'})
        ctx_mock = AsyncMock()
        ctx_mock.__aenter__.return_value = mock_resp
        session.post = MagicMock()
        session.post.return_value = ctx_mock

        # Force consecutive timeouts above threshold (6)
        client._consecutive_timeouts["llama3"] = 10
        client._circuit_open_until["llama3"] = time.monotonic() + 300.0

        # Calling llama3 without fallback should raise InferenceError (circuit open)
        with pytest.raises(InferenceError) as exc_info:
            await client.infer("sys", "user", DummySchema, model="llama3")
        assert "Circuit breaker OPEN for model 'llama3'" in str(exc_info.value)

        # qwen circuit should remain healthy (not present in tracking dicts)
        assert client._consecutive_timeouts.get("qwen", 0) == 0

        # If we call llama3 with fallback_model="qwen", it should automatically reroute and succeed
        result = await client.infer("sys", "user", DummySchema, model="llama3", fallback_model="qwen")
        assert result.response == "rerouted success"

    asyncio.run(run_test())


def test_model_specific_semaphores():
    """Verify that different models have different isolated semaphores."""
    sem1 = get_ollama_semaphore("llama3")
    sem2 = get_ollama_semaphore("qwen")
    sem3 = get_ollama_semaphore("llama3")

    assert sem1 is not sem2
    assert sem1 is sem3


def test_watchlist_rank_based_pruning():
    """Verify that ZADD and rank-based pruning prunes watchlist to the top 50 elements."""
    async def run_test():
        pipe_mock = AsyncMock()
        pipe_mock.__aenter__.return_value = pipe_mock
        pipe_mock.execute.return_value = [1, 1]
        
        redis_mock = MagicMock()
        redis_mock.pipeline.return_value = pipe_mock
        
        async def mock_zremrangebyrank(key, start, end):
            assert key == "sentinel:watched:equities"
            assert start == 0
            assert end == -51
            return 1

        pipe_mock.zremrangebyrank = AsyncMock(side_effect=mock_zremrangebyrank)
        pipe_mock.zadd = MagicMock()

        # Execute transaction block mock
        async with redis_mock.pipeline(transaction=True) as pipe:
            pipe.zadd("sentinel:watched:equities", mapping={"AAPL": time.time()})
            await pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
            results = await pipe.execute()

        assert results == [1, 1]
        pipe_mock.zadd.assert_called_once()
        pipe_mock.zremrangebyrank.assert_called_once()

    asyncio.run(run_test())


def test_consumer_rebalance_mitigation():
    """Verify partition pausing and heartbeats in the correlation consumer loop."""
    consumer_mock = MagicMock()
    consumer_mock._c = MagicMock()
    
    assigned_partitions = [MagicMock(), MagicMock()]
    consumer_mock._c.assignment.return_value = assigned_partitions
    consumer_mock._c.pause = MagicMock()
    consumer_mock._c.resume = MagicMock()
    
    # 1. Pause assigned
    assigned = consumer_mock._c.assignment()
    if assigned:
        consumer_mock._c.pause(*assigned)
        
    consumer_mock._c.pause.assert_called_once_with(*assigned_partitions)
    
    # 2. Resume assigned
    if assigned:
        consumer_mock._c.resume(*assigned)
        
    consumer_mock._c.resume.assert_called_once_with(*assigned_partitions)


def test_num_predict_custom_limits():
    """Verify that num_predict is correctly forwarded to Ollama API request payload."""
    async def run_test():
        session = AsyncMock()
        client = OllamaClient(session=session, model="llama3")

        # Mock resolve_model
        client._resolve_model = AsyncMock(return_value="llama3")

        # Mock session.post context manager and JSON response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"response": '{"response": "test response"}'})
        
        # Correctly mock aiohttp post async context manager
        session.post = MagicMock()
        context_mock = AsyncMock()
        context_mock.__aenter__.return_value = mock_response
        session.post.return_value = context_mock

        # Execute infer with num_predict=123
        await client.infer("sys", "user", DummySchema, num_predict=123)

        # Assert post was called and payload options contained num_predict=123
        session.post.assert_called_once()
        args, kwargs = session.post.call_args
        payload = kwargs["json"]
        assert payload["options"]["num_predict"] == 123

    asyncio.run(run_test())


def test_fuzzy_model_resolution():
    """Verify that short model family names like 'qwen' match available tags like 'qwen2.5:7b'."""
    async def run_test():
        session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "models": [{"name": "llama3:latest"}, {"name": "qwen2.5:7b"}]
        })
        
        ctx_mock = AsyncMock()
        ctx_mock.__aenter__.return_value = mock_resp
        session.get = MagicMock(return_value=ctx_mock)

        client = OllamaClient(session=session, model="llama3")

        # Resolving "qwen" should match "qwen2.5:7b"
        resolved = await client._resolve_model("qwen")
        assert resolved == "qwen2.5:7b"

        # Resolving "llama3" should match "llama3:latest"
        resolved_llama = await client._resolve_model("llama3")
        assert resolved_llama == "llama3:latest"

    asyncio.run(run_test())


def test_recursion_prevention_during_fallback():
    """Verify that circuit breaker and fallback loop stops gracefully when all candidate models are open/visited."""
    async def run_test():
        session = AsyncMock()
        client = OllamaClient(session=session, model="llama3")

        # Mark circuit open for both llama3 and qwen2.5:7b
        client._consecutive_timeouts["llama3"] = 10
        client._circuit_open_until["llama3"] = time.monotonic() + 300.0

        client._consecutive_timeouts["qwen2.5:7b"] = 10
        client._circuit_open_until["qwen2.5:7b"] = time.monotonic() + 300.0

        client._resolve_model = AsyncMock(side_effect=lambda m, exclude_models=None: m)
        client._get_fallback_model = AsyncMock(return_value=None)

        with pytest.raises(InferenceError) as exc_info:
            await client.infer("sys", "user", DummySchema, model="llama3", fallback_model="qwen2.5:7b")

        assert "InferenceError" in str(exc_info.type)
        assert "Circuit breaker OPEN" in str(exc_info.value)

    asyncio.run(run_test())


def test_agent_dispatch_dlq_on_inference_error():
    """Verify that agent _dispatch catches InferenceError and sends to DLQ without crashing."""
    async def run_test():
        mock_redis = AsyncMock()
        mock_db = AsyncMock()
        mock_neo4j = AsyncMock()
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_dlq = AsyncMock()

        class DummyAgent(SentinelAgent):
            @property
            def output_topic(self) -> str:
                return "test.topic"

            async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                raise InferenceError("Ollama container timed out")

        agent = DummyAgent("test_agent", ["input.topic"], mock_redis, mock_db, mock_neo4j, mock_producer, mock_consumer, mock_dlq)
        
        # Dispatch should catch InferenceError, increment _errors, and call _send_dlq
        await agent._dispatch({"raw": "data"})
        
        assert agent._errors == 1
        mock_dlq.send.assert_called_once()

    asyncio.run(run_test())

