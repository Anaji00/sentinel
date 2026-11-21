"""Every Kafka topic constant has a producer in the code, so a new topic cannot be declared and forgotten (research, 13 Nov 2026: `alerts.outbound` has 143,745 lifetime
offsets, no producer and no consumer; `agents.ontology.updates` has no consumer group; `agents.intel.briefs` has no live producer). The check is a static one: a
`Topics.NAME` that is sent to (send, publish, produce, an output_topic, a topic= argument or a returned topic), searched across services/ and shared/. A topic that is
declared and never sent to is listed in EXEMPT with the reason; an exemption that starts being used must be dropped.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]

# Declared, never sent to by code. Each entry is a claim: delete the constant, or give it a producer, and then drop the entry.
EXEMPT = {
    "ONTOLOGY_UPDATES": "no producer or consumer group in code; knowledge_graph_engine is disabled (agents.ontology.updates)",
    "SYSTEM_HEARTBEAT": "declared for a heartbeat stream that is written to Redis, not Kafka",
    "ALERTS": "alerts.outbound: no producer and no consumer in code (143,745 lifetime offsets, none retained)",
}


def _topics():
    src = (ROOT / "shared" / "kafka" / "__init__.py").read_text(encoding="utf-8")
    body = src[src.index("class Topics:"):]
    return re.findall(r"^    ([A-Z][A-Z0-9_]*)\s*=\s*[\"']([^\"']+)[\"']", body, re.M)


def _code() -> str:
    parts = []
    for root in ("services", "shared"):
        for p in (ROOT / root).rglob("*.py"):
            if "__pycache__" in p.parts or p == ROOT / "shared" / "kafka" / "__init__.py":
                continue
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts)


def _has_producer(name: str, code: str) -> bool:
    patterns = (
        rf"(send|produce|publish|send_and_wait)\w*\(\s*(topic\s*=\s*)?(f?[\"'].*)?Topics\.{name}\b",
        rf"topic\s*=\s*Topics\.{name}\b",
        rf"return\s+Topics\.{name}\b",
        rf"output_topic\w*\s*[:=]\s*Topics\.{name}\b",
    )
    return any(re.search(p, code) for p in patterns)


def test_every_topic_has_a_producer_or_an_exemption_with_a_reason():
    code = _code()
    names = [n for n, _ in _topics()]
    assert len(names) >= 30, "the Topics class was not read"
    unproduced = sorted(n for n in names if not _has_producer(n, code))
    new = [n for n in unproduced if n not in EXEMPT]
    assert not new, f"topics declared but never sent to: {new}. Give each a producer, delete the constant, or add it to EXEMPT with the reason."


def test_the_exemptions_still_name_unproduced_topics():
    code = _code()
    names = {n for n, _ in _topics()}
    for name in EXEMPT:
        assert name in names, f"{name} is exempted and no longer exists; drop the entry"
        assert not _has_producer(name, code), f"{name} now has a producer; drop it from EXEMPT"
