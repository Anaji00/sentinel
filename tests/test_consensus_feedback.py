"""
tests/test_consensus_feedback.py

The swarm's consensus never reached the swarm.

Every agent builds its prompt context in SentinelAgent.get_swarm_context(),
which reads the persisted report and does:

    summary = cons.get("summary") or cons.get("consensus_summary")
    if summary:
        lines.append(f"Swarm Consensus: {summary[:200]}")

ConsensusReport defined neither field. Confirmed against the live report in
Redis: its keys were report_id, generated_at, contradictions, consensus_signals,
ach_reports, stale_agents, total_active_bulletins, agents_reporting -- and the
expression above evaluated to None on every read. The engine computed Subjective
Logic fusion, contradiction detection and ACH matrices, persisted all of it, and
no agent ever saw one word.

The second defect is what that line would have said. A ticker with one bulletin
was published with agreement_ratio=1.0 -- one agent agreeing with itself,
reported as unanimity, fed back to the agent that said it.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.consensus_engine import (  # noqa: E402
    ConsensusEngine,
    ConsensusReport,
    ConsensusSignal,
)


def _signal(ticker, agents, agreement, score=0.5, direction="bullish"):
    return ConsensusSignal(
        ticker=ticker, direction=direction, consensus_score=score,
        contributing_agents=agents, agreement_ratio=agreement,
    )


def test_the_report_carries_the_field_agents_read():
    """The exact expression from get_swarm_context() must resolve."""
    report = ConsensusReport(summary="AAPL bullish (3 agents, 80% agreement)")
    blob = report.model_dump()

    summary = blob.get("summary") or blob.get("consensus_summary")
    assert summary, "agents would silently get nothing, as they did in production"


def test_a_report_without_signals_still_says_something_true():
    text = ConsensusEngine._build_summary([], [], [])
    assert text == "No corroborated swarm signals."


def test_corroborated_signals_state_their_agent_count():
    """The reader is a model that treats this as corroboration."""
    text = ConsensusEngine._build_summary(
        [_signal("AAPL", 4, 0.75), _signal("MSFT", 3, 1.0, score=0.9)], [], []
    )
    assert "4 agents" in text and "MSFT" in text and "AAPL" in text


def test_single_agent_signals_are_named_as_such():
    """A one-agent call must not be indistinguishable from a corroborated one."""
    text = ConsensusEngine._build_summary(
        [_signal("NVDA", 1, 0.0)], [], []
    )
    assert "single-agent only" in text
    assert "NVDA" in text
    assert "agents," not in text, "a solo call was dressed up as a multi-agent one"


def test_strongest_signals_lead():
    """The line is truncated to 200 chars downstream, so ordering matters."""
    text = ConsensusEngine._build_summary(
        [_signal("WEAK", 2, 0.5, score=0.05), _signal("STRONG", 2, 0.5, score=0.95)],
        [], [],
    )
    assert text.index("STRONG") < text.index("WEAK")


def test_summary_survives_the_downstream_truncation():
    """get_swarm_context() slices to 200 characters."""
    text = ConsensusEngine._build_summary(
        [_signal(f"TICK{i}", 3, 0.8) for i in range(12)], [], []
    )
    assert len(text[:200]) > 0 and "agents" in text[:200]


def test_contradictions_and_stale_agents_are_surfaced():
    class _C:
        pass
    text = ConsensusEngine._build_summary([], [_C(), _C()], ["radar_agent"])
    assert "2 contradiction(s)" in text
    assert "radar_agent" in text


def test_one_agent_is_not_unanimity():
    """The single-bulletin branch used to hard-code agreement_ratio=1.0."""
    import inspect
    src = inspect.getsource(ConsensusEngine.analyze)
    single_branch = src[src.index("if len(group) < 2"):src.index("# Multiple agents")]
    assert "agreement_ratio=1.0" not in single_branch, (
        "a lone agent is again being published as full agreement"
    )
    assert "agreement_ratio=0.0" in single_branch
