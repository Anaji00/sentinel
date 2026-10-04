"""
tests/test_kg_extraction_batching.py

Headline extraction is batched, three to an inference.

The knowledge graph engine offered ~790 candidates in thirteen minutes and
drained one. Each extraction paid a ~1,100-token shared prompt -- global
context, cross-agent block, predicate rules -- around a ~80-token headline, on
a host where prompt evaluation is 44-67% of every call. radar_agent, which
already batches, measured 10-12 answers per inference with zero singletons.

Two things a batch can get wrong that a single call cannot: one malformed brief
failing everyone's answer, and a brief filed under the wrong item. Both are
tested here, because the second is invisible downstream -- it publishes a
correct-looking brief about the wrong report.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.knowledge_graph_engine import (
    IntelBrief,
    IntelBriefBatch,
    KnowledgeGraphEngine,
    KG_EXTRACTION_BATCH_SIZE,
    _describes,
)


def _brief(ref, headline, entity="Maersk"):
    return {
        "ref": ref,
        "headline": headline,
        "summary": headline,
        "primary_entity": entity,
    }


def _item(headline, score=0.7):
    return {
        "message": {"anomaly_score": score, "headline": headline},
        "headline": headline,
        "summary": "",
        "anomaly_score": score,
        "source": "test",
    }


def _engine(batch_payload):
    eng = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    eng.fetch_global_context = AsyncMock(return_value="ctx")
    eng.get_cross_agent_context = AsyncMock(return_value="")
    eng._execute_with_telemetry = AsyncMock(
        return_value=IntelBriefBatch.model_validate(batch_payload)
    )
    return eng


def test_a_malformed_brief_does_not_fail_the_batch():
    batch = IntelBriefBatch.model_validate({"briefs": [
        _brief("H1", "Tanker seized in the Strait of Hormuz"),
        {"ref": "H2", "headline": "", "summary": "", "primary_entity": "x"},
    ]})
    assert [b.ref for b in batch.briefs] == ["H1"]


def test_briefs_are_returned_under_their_own_items():
    items = [
        ("k1", _item("Tanker seized in the Strait of Hormuz")),
        ("k2", _item("Chipmaker halts Taiwan fabrication after earthquake")),
    ]
    eng = _engine({"briefs": [
        _brief("H2", "Taiwan earthquake halts chip fabrication", "TSMC"),
        _brief("H1", "Hormuz tanker seizure", "Iran"),
    ]})
    out = asyncio.run(eng._extract_batch(items))
    assert out["k1"].primary_entity == "Iran"
    assert out["k2"].primary_entity == "TSMC"


def test_the_ref_does_not_leak_into_the_published_brief():
    items = [("k1", _item("Tanker seized in the Strait of Hormuz"))]
    eng = _engine({"briefs": [_brief("H1", "Hormuz tanker seizure")]})
    out = asyncio.run(eng._extract_batch(items))
    assert type(out["k1"]) is IntelBrief
    assert "ref" not in out["k1"].model_dump()


def test_a_misfiled_brief_is_no_brief():
    """Tagged H1, written about H2's report: dropped, not published."""
    items = [
        ("k1", _item("Tanker seized in the Strait of Hormuz")),
        ("k2", _item("Chipmaker halts Taiwan fabrication after earthquake")),
    ]
    eng = _engine({"briefs": [_brief("H1", "Taiwan earthquake halts chip fabrication")]})
    out = asyncio.run(eng._extract_batch(items))
    assert out == {}


def test_an_invented_ref_and_a_duplicate_are_ignored():
    items = [("k1", _item("Tanker seized in the Strait of Hormuz"))]
    eng = _engine({"briefs": [
        _brief("H9", "Tanker seized near Hormuz"),
        _brief("H1", "Tanker seized near Hormuz", "Iran"),
        _brief("H1", "Tanker seized near Hormuz", "Oman"),
    ]})
    out = asyncio.run(eng._extract_batch(items))
    assert list(out) == ["k1"]
    assert out["k1"].primary_entity == "Iran"


def test_the_batch_competes_at_its_best_score():
    items = [
        ("k1", _item("Tanker seized in the Strait of Hormuz", score=0.3)),
        ("k2", _item("Chipmaker halts Taiwan fabrication", score=0.95)),
    ]
    eng = _engine({"briefs": []})
    asyncio.run(eng._extract_batch(items))
    lead = eng._execute_with_telemetry.await_args.kwargs["message"]
    assert lead["anomaly_score"] == 0.95


def test_the_token_budget_scales_with_the_batch_and_is_capped():
    items = [(f"k{i}", _item(f"Headline number {i} about shipping")) for i in range(3)]
    eng = _engine({"briefs": []})
    asyncio.run(eng._extract_batch(items))
    budget = eng._execute_with_telemetry.await_args.kwargs["num_predict"]
    assert 1000 <= budget <= 1536


def test_paraphrase_is_not_misfiling():
    b = IntelBrief.model_validate(_brief("H1", "Iran seizes oil tanker near Hormuz"))
    assert _describes("Tanker seized in the Strait of Hormuz", b)


def test_handle_goes_through_the_batcher():
    src = (ROOT / "services" / "agents" / "knowledge_graph_engine.py").read_text(encoding="utf-8")
    handle = src.split("    async def handle(")[1].split("\n    async def ")[0]
    assert "self._extraction_batcher.submit(" in handle
    assert "schema=IntelBrief," not in handle


def test_batch_size_leaves_room_for_a_truncation_retry():
    """A truncated call is retried at twice its budget; three briefs at that
    ceiling must still fit the model timeout on a ~6.5 tok/s host."""
    assert 1 <= KG_EXTRACTION_BATCH_SIZE <= 4
