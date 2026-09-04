"""The rule agent must be told the vocabulary it is writing in.

The prompt named nine event types in prose, out of the forty-three the platform
defines, and constrained only trigger_event_type -- it said nothing about what
may appear inside a correlation clause, which is the field that matters most.
That is where "location" and "vessel" came from: three synthesised rules sitting
in production naming event types that do not exist, incapable of firing, dead
since 2026-08-31 with nothing logged.

The example it was given shipped "correlations": [] and "conditions": {}, so the
model had no shape for the clause and no reason to set an anomaly floor. A rule
with neither triggers on every matching event.
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "rule_agent_mod", ROOT / "services" / "agents" / "rule_agent.py"
)


def _mod():
    if "rule_agent_mod" in sys.modules:
        return sys.modules["rule_agent_mod"]
    m = importlib.util.module_from_spec(_spec)
    sys.modules["rule_agent_mod"] = m
    _spec.loader.exec_module(m)
    return m


class _Rule:
    def __init__(self, trigger, correlations):
        self.rule_id = "syn_1"
        self.trigger_event_type = trigger
        self.correlations = correlations


def test_the_invented_vocabulary_is_rejected():
    """The exact rules sitting in production: syn_3 and syn_1."""
    m = _mod()
    assert m._unknown_event_types(_Rule("location", [{"event_types": ["vessel"]}])) == {"location", "vessel"}
    assert m._unknown_event_types(_Rule("vessel_dark", [{"event_types": ["location", "vessel"]}])) == {"location", "vessel"}


def test_a_real_rule_passes():
    m = _mod()
    rule = _Rule("insider_trade", [{"event_types": ["options_flow", "equity_block"]}])
    assert m._unknown_event_types(rule) == set()


def test_a_list_trigger_is_checked_too():
    m = _mod()
    assert m._unknown_event_types(_Rule(["equity_block", "nonsense"], [])) == {"nonsense"}


def test_a_rule_naming_nothing_is_not_rejected_here():
    """An over-broad rule is a different problem from one that cannot fire."""
    m = _mod()
    assert m._unknown_event_types(_Rule(None, [])) == set()


def test_the_rejection_is_wired_into_the_write_path():
    source = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    write = source[source.index("hset(\"sentinel:correlation:dynamic_rules\", rule.rule_id"):]
    before = source[:source.index("hset(\"sentinel:correlation:dynamic_rules\", rule.rule_id")]
    assert "_unknown_event_types(rule)" in before, "validation does not run before the write"
    assert "continue" in before


# ── The prompt ────────────────────────────────────────────────────────────────

PROMPT = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
BLOCK = PROMPT[PROMPT.index("=== SYNTHETIC RULE GENERATION ==="):]
BLOCK = BLOCK[:BLOCK.index('"""', 10)]


def test_the_vocabulary_is_injected_rather_than_hardcoded():
    """Nine of forty-three types, written in prose, is how this drifted."""
    assert "{vocabulary}" in BLOCK
    assert "vessel_dark, options_flow, futures_cot" not in BLOCK


def test_correlation_event_types_are_constrained_too():
    """The field the old prompt never mentioned."""
    assert "correlation's event_types" in BLOCK


def test_the_worked_example_populates_the_clause():
    assert '"correlations": []' not in BLOCK
    assert '"event_types"' in BLOCK and '"hours"' in BLOCK


def test_an_anomaly_floor_is_required():
    assert "min_anomaly is required" in BLOCK
    assert '"conditions": {{}}' not in BLOCK


def test_same_entity_is_explained():
    """The rule semantics fix, taught rather than left to be inferred."""
    assert "same_entity" in BLOCK
    assert "SAME name" in BLOCK


def test_existing_rules_are_shown_so_they_are_not_restated():
    assert "{already}" in BLOCK
