"""The two loops that spent most of the host's inference on 28 October.

1. The quant engine published peer-decoupling clusters at INTELLIGENCE tier
   without a correlations row. Reasoning wrote a 7B scenario for each (about
   eight minutes), the insert failed scenarios_correlation_id_fkey, and the
   scenario was still broadcast and counted: 27 written, 0 saved.
2. One 8-K timed out on every pass. The exception left the run before the filing
   was marked done, so it was retried every fifty minutes, and the Ollama client
   turned each timeout into a fallback plus three more 600-second attempts.

Guarded here: a timeout is never resent to the same model and hops at most once,
to a smaller one; reasoning spends nothing on a cluster without a row and
announces nothing it failed to save; one filing's failure is parked after
MAX_ATTEMPTS and never stops the run; a missing live ranking is not "no filter".
"""
import asyncio
import pathlib
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pydantic import BaseModel  # noqa: E402

from shared.utils import ollama as ol  # noqa: E402


class _Out(BaseModel):
    x: int


def _client(replies: List[Any], seen: List[str]) -> ol.OllamaClient:
    """A client whose model call returns or raises the next reply in turn."""
    c = ol.OllamaClient(None, model="qwen2.5:7b")

    async def call(prompt: str, temperature: float, model: str, **_: Any) -> str:
        seen.append(model)
        r = replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    async def no_fallback(*_: Any, **__: Any) -> Optional[str]:
        return None

    c._call_ollama = call  # type: ignore[method-assign]
    c._get_fallback_model = no_fallback  # type: ignore[method-assign]
    return c


def test_smaller_model_reads_declared_sizes():
    assert ol.smaller_model("qwen2.5:3b", "qwen2.5:7b")
    assert not ol.smaller_model("qwen2.5:7b", "qwen2.5:3b")
    assert not ol.smaller_model("qwen2.5:7b", "qwen2.5:7b")
    assert not ol.smaller_model(None, "qwen2.5:7b")
    assert not ol.smaller_model("qwen2.5:latest", "qwen2.5:7b")


def test_a_timeout_hops_once_to_a_smaller_model_and_stops():
    seen: List[str] = []
    c = _client([ol.InferenceTimeout("t"), ol.InferenceTimeout("t")], seen)
    with pytest.raises(ol.InferenceTimeout):
        asyncio.run(c.infer("s", "u", _Out, fallback_model="qwen2.5:3b"))
    assert seen == ["qwen2.5:7b", "qwen2.5:3b"], seen


def test_a_timeout_is_not_resent_to_the_same_model():
    seen: List[str] = []
    c = _client([ol.InferenceTimeout("t")], seen)
    with pytest.raises(ol.InferenceTimeout):
        asyncio.run(c.infer("s", "u", _Out, model="qwen2.5:3b"))
    assert seen == ["qwen2.5:3b"], seen


def test_a_timeout_does_not_fall_back_to_a_larger_model():
    seen: List[str] = []
    c = _client([ol.InferenceTimeout("t")], seen)
    with pytest.raises(ol.InferenceTimeout):
        asyncio.run(c.infer("s", "u", _Out, model="qwen2.5:3b", fallback_model="qwen2.5:7b"))
    assert seen == ["qwen2.5:3b"], seen


def test_malformed_output_keeps_its_retries():
    seen: List[str] = []
    c = _client(["not json", '{"x": 1}'], seen)
    out = asyncio.run(c.infer("s", "u", _Out, model="qwen2.5:3b"))
    assert out.x == 1 and seen == ["qwen2.5:3b", "qwen2.5:3b"]


def test_a_raw_timeout_is_not_retried():
    seen: List[str] = []
    c = _client([ol.InferenceTimeout("t")], seen)
    with pytest.raises(ol.InferenceTimeout):
        asyncio.run(c.infer_raw("s", "u", model="qwen2.5:3b"))
    assert seen == ["qwen2.5:3b"], seen


# ── reasoning ────────────────────────────────────────────────────────────────

class _Redis:
    def __init__(self) -> None:
        self.raw = self

    async def get(self, _: str) -> None:
        return None


class _Producer:
    def __init__(self) -> None:
        self.sent: List[str] = []

    async def send(self, topic: str, *_: Any, **__: Any) -> None:
        self.sent.append(topic)


class _Db:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self.rows = rows

    async def query(self, *_: Any) -> List[Dict[str, Any]]:
        return self.rows


class _Generator:
    def __init__(self, scenario: Any) -> None:
        self.scenario, self.calls = scenario, 0

    async def generate(self, *_: Any) -> Any:
        self.calls += 1
        return self.scenario


class _Ctx:
    async def build(self, _: Any) -> Dict[str, Any]:
        return {}


class _Library:
    async def find_similar(self, *_: Any) -> List[Any]:
        return []

    async def outcome_base_rate(self, _: str) -> float:
        return 0.5


def _cluster() -> Any:
    from shared.models import AlertTier
    return SimpleNamespace(alert_tier=AlertTier.INTELLIGENCE, rule_name="Some Rule", rule_id="R1",
                           correlation_id="00000000-0000-0000-0000-000000000001", tags=[])


def test_reasoning_spends_nothing_on_a_cluster_without_a_row():
    import services.reasoning.main as rm
    gen, prod = _Generator(None), _Producer()
    out = asyncio.run(rm.process_cluster(_cluster(), _Db([]), _Redis(), prod, _Ctx(), gen, _Library()))
    assert out is None and gen.calls == 0 and prod.sent == []


def test_reasoning_announces_nothing_it_failed_to_save(monkeypatch):
    import services.reasoning.main as rm

    async def not_saved(*_: Any) -> bool:
        return False

    monkeypatch.setattr(rm, "_save_scenario", not_saved)
    gen, prod = _Generator(SimpleNamespace(scenario_id="s1")), _Producer()
    out = asyncio.run(rm.process_cluster(_cluster(), _Db([{"?column?": 1}]), _Redis(), prod, _Ctx(), gen, _Library()))
    assert out is None and gen.calls == 1 and prod.sent == []


def test_quant_clusters_are_persisted_before_publish_and_not_intelligence():
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    block = src.split('rule_id="QUANT_PEER_DECOUPLING"')[1].split("Publish structured AgentBulletin")[0]
    assert "AlertTier.INTELLIGENCE" not in block
    save, send = block.find("save_correlation(corr_cluster)"), block.find("Topics.CORRELATIONS")
    assert 0 <= save < send, "the cluster must be saved before it is published"


# ── filing extractor ─────────────────────────────────────────────────────────

class _Raw:
    def __init__(self) -> None:
        self.sets: Dict[str, set] = {}
        self.hashes: Dict[str, Dict[str, int]] = {}

    async def get(self, _: str) -> None:
        return None

    async def sismember(self, key: str, v: str) -> bool:
        return v in self.sets.get(key, set())

    async def sadd(self, key: str, v: str) -> None:
        self.sets.setdefault(key, set()).add(v)

    async def expire(self, *_: Any) -> None:
        return None

    async def hincrby(self, key: str, field: str, n: int) -> int:
        h = self.hashes.setdefault(key, {})
        h[field] = h.get(field, 0) + n
        return h[field]


class _FilingDb:
    def __init__(self, filings: List[Dict[str, Any]], ranked: List[str]) -> None:
        self.filings, self.ranked = filings, ranked

    async def query(self, sql: str, *_: Any) -> List[Dict[str, Any]]:
        return [{"ticker": t} for t in self.ranked] if "factor_scores_daily" in sql else self.filings


def _filing(acc: str, ticker: str = "AAA") -> Dict[str, Any]:
    return {"filing_data": {"ticker": ticker, "accession_number": acc, "items": ["2.02"]}, "fd_ticker": ticker}


def _extractor(filings: List[Dict[str, Any]], ranked: List[str]) -> Any:
    from services.agents.filing_extractor import FilingExtractor
    fe = FilingExtractor(_FilingDb(filings, ranked), _Raw())
    return fe


def test_one_failing_filing_does_not_stop_the_run_and_is_parked():
    from services.agents import filing_extractor as fxm
    fe = _extractor([_filing("bad"), _filing("good")], ["AAA"])
    read: List[str] = []

    async def fake_read(_s: Any, _l: Any, fd: Dict[str, Any], _t: str, acc: str) -> Any:
        read.append(acc)
        if acc == "bad":
            raise ol.InferenceTimeout("t")
        return True, None

    fe._read = fake_read
    for _ in range(fxm.MAX_ATTEMPTS):
        asyncio.run(fe.run_once(None, None))
    assert read.count("good") == 1, "the filing after the failure was not read"
    assert read.count("bad") == fxm.MAX_ATTEMPTS
    assert "bad" in fe.raw.sets[fxm.DONE_KEY], "a filing that keeps failing must be parked"
    asyncio.run(fe.run_once(None, None))
    assert read.count("bad") == fxm.MAX_ATTEMPTS, "a parked filing was read again"


def test_a_missing_live_ranking_falls_back_to_the_stored_one():
    fe = _extractor([_filing("a1", "AAA"), _filing("z1", "ZZZ")], ["AAA"])
    read: List[str] = []

    async def fake_read(_s: Any, _l: Any, fd: Dict[str, Any], _t: str, acc: str) -> Any:
        read.append(acc)
        return True, None

    fe._read = fake_read
    asyncio.run(fe.run_once(None, None))
    assert read == ["a1"], "an unranked ticker passed because the filter was empty"
