"""A prediction nothing can trace to an author cannot be credited or debited.

Measured on the running deployment 2026-09-20:

    agent_name                |  n | resolved | newest
    --------------------------+----+----------+---------------------
    (null)                    | 50 |        0 | 2026-09-20 21:18
    quant_trading_engine      | 19 |       19 | 2026-09-19 19:03
    macro_intelligence_engine |  5 |        5 | 2026-09-18 17:03

The 50 unattributed rows are the wargamer's, arriving every ~10 minutes with
properly named targets -- RAGNAR, THY164, SOLUSDT, Turkish Straits -- so the
producer is working. They land through the telemetry worker rather than the
agents' own `record_prediction`, and that INSERT names seven columns, none of
them `agent_name`. The message carries `agent`; the column was simply never
written, which is a default standing in for a value that was already present.

The 24 rows that do carry an author are 100% resolved. The 50 that do not are
0% resolved, and that half is a separate gap: a next-target prediction has no
ticker, entry price or horizon, so there is no deadline at which it could be
graded. Attribution is what this file fixes. Resolution is still open.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

WORKER = (ROOT / "services/telemetry-worker/main.py").read_text(encoding="utf-8")


def _insert_block() -> str:
    start = WORKER.index("INSERT INTO agent_predictions")
    return WORKER[start : WORKER.index("MetricsCollector.increment", start)]


def test_the_insert_stores_the_author():
    block = _insert_block()
    assert "agent_name" in block, "every row this worker writes is unattributed"


def test_the_author_comes_from_the_message_not_a_hardcoded_name():
    """AGENTS_PREDICTIONS may carry more than one producer."""
    block = _insert_block()
    assert 'data.get("agent")' in block
    assert "adversarial_wargamer" not in block, (
        "hardcoding the author would mislabel any other producer on this topic"
    )


def test_the_placeholder_count_matches_the_columns():
    """An added column with no added $n silently shifts every later binding."""
    block = _insert_block()
    columns = block[block.index("(") + 1 : block.index("VALUES")]
    columns = columns[: columns.rindex(")")]
    n_columns = len([c for c in columns.split(",") if c.strip()])
    placeholders = block[block.index("VALUES") : block.index('"""', block.index("VALUES"))]
    n_placeholders = len(set(re.findall(r"\$\d+", placeholders)))
    assert n_columns == n_placeholders, (
        f"{n_columns} columns against {n_placeholders} placeholders"
    )


def test_an_empty_agent_is_stored_as_null_not_as_an_empty_string():
    """Otherwise '' becomes a distinct author that groups on its own."""
    block = _insert_block()
    assert 'str(data.get("agent") or "") or None' in block


def test_the_wargamer_names_itself_on_what_it_publishes():
    """The fix reads a field; this is the code that has to put it there."""
    src = (ROOT / "services/agents/adversarial_wargamer.py").read_text(encoding="utf-8")
    assert 'output["agent"] = self.name' in src


# ── a constant whose docstring described a writer that does not exist ────────


def test_the_concept_collection_does_not_claim_a_writer():
    """"Written by the ontology path" -- there is no such path.

    Measured 2026-09-20: sentinel_concepts holds 0 points against 664,969 in
    the event collection, and reports `grey` status, which is Qdrant saying no
    shard was ever brought up for it. The constant is referenced only by
    __all__.
    """
    src = (ROOT / "shared/utils/vector_index.py").read_text(encoding="utf-8")
    # The original line, not the phrase -- the correction quotes the phrase.
    assert "#: Concept vectors, written by the ontology path." not in src
    assert "never built" in src


def test_nothing_writes_the_concept_collection():
    """If a writer ever appears, the comment above stops being true."""
    import re

    hits = []
    for folder in ("services", "shared"):
        for path in (ROOT / folder).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            code = "\n".join(
                line for line in text.splitlines()
                if not line.lstrip().startswith("#") and not line.lstrip().startswith("#:")
            )
            if re.search(r"CONCEPT_COLLECTION|sentinel_concepts", code):
                hits.append(path.name)
    assert hits == ["vector_index.py"], (
        f"something now references the empty concept collection: {hits}"
    )
