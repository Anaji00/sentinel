"""A triple's endpoints carry the type the model already gave them.

`_merge_graph_triples` read `getattr(t, 'subject_type', 'Entity')` at four
sites against a `GraphTriple` that has no such field, so every read resolved to
the default and every node this engine proposed was labelled `Entity`,
unconditionally. A default standing in for a lookup -- and the lookup existed:
the same brief carries `entities`, each with the `entity_type` the model chose.

Measured 2026-09-20 while this was live: `graph.label_fallback_to_entity` was
climbing steadily (112 -> 135 inside forty minutes) on the heavy tier.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/agents/knowledge_graph_engine.py").read_text(encoding="utf-8")


def _code_only(text: str) -> str:
    """Comments name the old form in order to record it; code must not use it."""
    return "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))


def test_the_absent_field_is_no_longer_read():
    assert "getattr(t, 'subject_type'" not in _code_only(SRC)
    assert "getattr(t, 'object_type'" not in _code_only(SRC)


def test_the_label_comes_from_the_briefs_own_entities():
    assert "entity_types.get(t.subject.strip().lower()" in SRC
    assert "entity_types.get(t.object.strip().lower()" in SRC


def test_the_map_is_built_from_the_brief_and_passed_through():
    assert "for e in (brief.entities or [])" in SRC
    assert "self._merge_graph_triples(valid_triples, entity_types)" in SRC


def test_entity_remains_the_fallback_when_the_model_named_no_type():
    """Unknown must stay Entity rather than becoming a guess."""
    assert 'entity_types.get(t.subject.strip().lower(), "Entity")' in SRC


def test_the_model_types_are_labels_the_supervisor_accepts():
    """A type the model may emit has to survive resolve_node_label."""
    from shared.models.ontology import ALLOWED_NODE_LABELS

    # The vocabulary IntelEntity documents for the model.
    for label in ("Company", "Vessel", "Aircraft", "Organization", "Location", "Person"):
        assert label in ALLOWED_NODE_LABELS, label


def test_the_duplicate_centrality_helper_is_gone():
    """It had no caller, and the severity weighting it described is applied by
    the correlation tiering's own copy."""
    assert "async def get_entity_centrality" not in SRC
