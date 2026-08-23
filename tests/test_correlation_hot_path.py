"""Keeps the most expensive call on the correlation hot path out of the way of
events that will never use its result.

Embedding an event with all-mpnet-base-v2 costs ~945ms on this host (~454ms
batched). The engine called it unconditionally and *before* deciding whether to
skip soft correlation, so a skipped event paid the full encoder cost and the
result was discarded. Routine telemetry -- already capped at 0.15 anomaly by the
enricher that produced it, and roughly two thirds of the stream -- was being
asked whether one position fix is semantically like another.
"""
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")


def test_embedding_is_not_computed_before_the_skip_decision():
    """The exact ordering bug: the encoder ran, then the code decided to skip.

    Asserted on the parsed function rather than on source text, so moving the
    code does not quietly retire the check.
    """
    import ast
    tree = ast.parse(SRC)
    target = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_process_correlation_event"
    )
    decided_at = embedded_at = None
    for node in ast.walk(target):
        if isinstance(node, ast.Name) and node.id == "skip_soft_correlation" and decided_at is None:
            decided_at = node.lineno
        if isinstance(node, ast.Attribute) and node.attr == "embed_event":
            embedded_at = node.lineno if embedded_at is None else min(embedded_at, node.lineno)
    assert decided_at is not None, "the skip decision has disappeared"
    if embedded_at is not None:
        assert decided_at < embedded_at, "the encoder is reached before the skip decision"


def test_the_batch_encoder_is_used_on_the_hot_path():
    """Per-event encoding left the engine at 475 events/min against 623/min of
    production; batched it is roughly twice as fast for identical vectors."""
    assert "embed_events(" in SRC, "the loop still encodes one event at a time"


def test_routine_telemetry_is_excluded_from_embedding():
    """0.15 is the enricher's own cap for routine fixes -- the same number it
    uses to say 'this is not interesting'."""
    assert "anomaly_score <= 0.15" in SRC, "low-anomaly events still reach the encoder"


def test_news_story_deduplication_survives():
    """The pre-existing novelty skip must not have been lost in the change."""
    assert "score_novelty" in SRC
    assert "novelty < 0.30" in SRC


def test_correlation_has_cpu_headroom():
    """It is CPU-bound on encoding and was pinned at its ceiling while lag grew."""
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    limits = ((compose["services"]["correlation"].get("deploy") or {})
              .get("resources", {}).get("limits", {}))
    assert float(limits.get("cpus", 0)) >= 2.0, (
        f"correlation capped at {limits.get('cpus')} CPUs remains saturated"
    )
