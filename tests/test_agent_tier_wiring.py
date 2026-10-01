"""Three wiring faults in the agent tier, each measured on the running system.

1. The heartbeat roster counted aliases. `agents_by_name` holds 21 entries for
   10 agents -- eleven are back-compat aliases for a task queue that is not
   started -- and `_tier_metadata` iterated all of them, unfiltered by tier.
   Live on 2026-09-20, `sentinel:heartbeat:agents-fast` published
   `agent_detail` with 21 rows for the 5 agents that tier runs, including
   5 agents belonging to the other tier, each reporting processed=0 because
   the object exists in this process and the work happens in the other one.

2. The supervisor announced commits it had refused. It subscribed to five
   topics, treated every dict as a proposal, and returned a receipt either way
   -- and the base class publishes any non-None return. `agents.ontology.updates`
   held 3,120,653 messages, every sampled one a supervisor receipt, a quarter
   of them `"summary": "Refused, no entity_id: 1"`. knowledge_graph_engine
   subscribes to that topic expecting ontology decisions and drops all of them.

3. The edge validator's query matched nothing at all. Measured: 0 edges
   returned, and no relationship in the graph has ever carried a
   `validation_samples` property. Three independent causes, each alone fatal --
   see the test below.
"""

import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MAIN = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
SUPERVISOR_SRC = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")

from services.agents.edge_validator import (  # noqa: E402
    EXPOSURE_PREDICATES,
    _PREDICATE_ALTERNATION,
)
from services.agents.supervisor import GraphSupervisor  # noqa: E402


# ── 1. the roster ────────────────────────────────────────────────────────────


def test_the_published_roster_is_the_agents_this_process_runs():
    """`agents_by_name` carries aliases and both tiers; `active_agents` does not."""
    block = MAIN[MAIN.index('"agent_detail"'):MAIN.index('"agent_detail"') + 700]
    assert "for ag in active_agents.values()" in block, block[:400]
    assert "for ag in agents_by_name.values()" not in block


def test_the_alias_map_still_has_more_entries_than_agents():
    """The premise of the fix. If this ever stops being true the fix is moot."""
    import re
    block = MAIN[MAIN.index("agents_by_name = {"):MAIN.index("_distinct_engines")]
    pairs = re.findall(r'"([a-z_]+)":\s+([a-z_]+),', block)
    assert len(pairs) > len({v for _, v in pairs}), "aliases have gone; simplify this"


def test_shutdown_closes_each_agent_once():
    assert "for ag in {id(a): a for a in agents_by_name.values()}.values():" in MAIN


def test_the_swarm_size_is_counted_not_asserted():
    assert "8 core engines live" not in MAIN
    assert "len(_distinct_engines)" in MAIN


# ── 2. the supervisor ────────────────────────────────────────────────────────


class _Raw:
    def __init__(self):
        self.store = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def eval(self, script, numkeys, key, token):
        if self.store.get(key) == token:
            del self.store[key]
            return 1
        return 0


class _Redis:
    def __init__(self):
        self.raw = _Raw()


class _Neo4j:
    def __init__(self):
        self.calls = []

    async def execute(self, cypher, params=None):
        self.calls.append((cypher, params))
        return None


def _supervisor():
    a = object.__new__(GraphSupervisor)
    a.name = "supervisor"
    a.redis = _Redis()
    a.neo4j = _Neo4j()
    return a


@pytest.mark.asyncio
async def test_a_proposal_with_no_entity_is_refused_not_reported():
    sup = _supervisor()
    assert await sup.execute_proposal({"action": "MERGE_ONTOLOGY_NODE"}) is False
    assert await sup.handle({"action": "MERGE_ONTOLOGY_NODE"}) is None
    assert sup.neo4j.calls == []


@pytest.mark.asyncio
async def test_a_message_that_is_not_a_proposal_produces_no_receipt():
    """A correlation or a headline reaching this agent must publish nothing."""
    sup = _supervisor()
    assert await sup.handle({"headline": "Oil prices surge", "anomaly_score": 0.8}) is None
    assert await sup.handle({"correlation_id": "abc", "entity_ids": ["X"]}) is None


@pytest.mark.asyncio
async def test_a_real_commit_still_reports(caplog):
    """A commit must stay distinguishable from a refusal.

    It used to be distinguishable by the receipt this returned, which the base
    class published to ONTOLOGY_UPDATES. That topic's only consumer was removed
    once it was established no branch had ever applied an update, which left it
    written by two producers and read by nobody -- so the receipt is gone and
    the report is a log line. What the test protects is unchanged: a commit
    says so, and it says so only when something was actually written.
    """
    sup = _supervisor()
    with caplog.at_level(logging.INFO):
        out = await sup.handle({
            "entity_id": "AAPL",
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company"},
        })
    assert out is None, "a receipt published to a topic nobody reads"
    assert sup.neo4j.calls, "nothing was written but a commit was reported"
    assert any("AAPL" in r.getMessage() for r in caplog.records), (
        "the commit reached the graph and nothing said so"
    )


@pytest.mark.asyncio
async def test_a_refusal_does_not_report_a_commit(caplog):
    """The other half: silence on the write path must not read as success."""
    sup = _supervisor()
    with caplog.at_level(logging.INFO):
        await sup.handle({"action": "MERGE_ONTOLOGY_NODE"})
    assert not any("committed" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_an_unknown_action_is_not_a_commit():
    sup = _supervisor()
    assert await sup.execute_proposal({"entity_id": "X", "action": "DROP_DATABASE"}) is False


@pytest.mark.asyncio
async def test_a_batch_of_refusals_produces_no_receipt():
    sup = _supervisor()
    assert await sup.handle([{"action": "LINK_ENTITY"}, {"entity_id": "Y"}]) is None


def test_the_supervisor_reads_only_the_proposals_topic():
    assert "input_topics=[Topics.ONTOLOGY_PROPOSALS]," in MAIN


# ── 3. the edge validator ────────────────────────────────────────────────────


def test_the_query_grades_every_predicate_the_list_names():
    """The list was widened to nine and the query kept matching four.

    713 of 1,090 edges were unreachable, including all 242 GRANGER_CAUSES.
    """
    for predicate in EXPOSURE_PREDICATES:
        assert predicate in _PREDICATE_ALTERNATION, predicate
    assert _PREDICATE_ALTERNATION.count("|") == len(EXPOSURE_PREDICATES) - 1


def test_the_alternation_is_derived_rather_than_written_out():
    """What stops the two drifting apart again."""
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    assert '"|".join(EXPOSURE_PREDICATES)' in src
    # The hand-written four must not come back as a literal.
    assert "SUPPLIES|COMMODITY_EXPOSURE|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO" not in src


def test_the_dead_label_and_type_constraints_are_gone():
    """`(a:Entity)` and `type: 'instrument'` each matched zero rows.

    The sources are :Company, :Commodity, :MacroFactor and :Region; the targets
    carry b.type of Commodity, Company, MacroFactor, Index and CryptoAsset.
    The value 'instrument' does not occur in this graph.
    """
    # Code only. The comment above the query names both constraints in order
    # to record why they went, and a check that cannot tell an explanation
    # from an instruction would forbid saying so.
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in src.splitlines() if not line.lstrip().startswith("#")
    )
    assert "type: 'instrument'" not in code
    assert "MATCH (a:Entity)-[r:" not in code


def test_the_sweep_is_bounded():
    """1,090 edges x ~51 queries every five minutes is not a free repair."""
    from services.agents.edge_validator import EDGES_PER_SWEEP
    assert 0 < EDGES_PER_SWEEP <= 200
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    assert "ORDER BY last_validated ASC" in src, "the sweep must rotate, not re-grade the same head"
    assert "LIMIT $batch" in src


def test_the_write_back_targets_the_edge_that_was_read():
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    assert "elementId(r) AS rid" in src
    assert "WHERE elementId(r) = $rid" in src


def test_a_sweep_that_grades_nothing_still_says_so():
    """The silence is why this went unnoticed for the life of the deployment."""
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    assert "self._sweeps % 100 == 1" in src


# ── the describer and the writer disagreed about every relationship ──────────


def test_the_description_names_the_predicate_that_is_written():
    """Two functions resolved the predicate and neither read the other's key.

    `execute_proposal` writes `data["relation_type"]`. `_describe_proposals`
    read `predicate` or `relationship` and fell back to "RELATED_TO" -- so a
    proposal carrying `relation_type: "TRANSACTED_WITH"` was written correctly
    and announced as RELATED_TO.

    Measured on the running graph 2026-09-20: the supervisor logged
    `0xbbbb... -[RELATED_TO]-> 0xbeef...`, and the edge between those two
    wallets is TRANSACTED_WITH, updated two minutes earlier. Independently, no
    RELATED_TO edge has been created or updated in 4.4 days, which is what made
    the claim checkable at all.
    """
    from services.agents.supervisor import _describe_proposals

    out = _describe_proposals([{
        "entity_id": "0xbbbb", "action": "LINK_ENTITY",
        "data": {"target_id": "0xbeef", "relation_type": "TRANSACTED_WITH"},
    }])
    assert "TRANSACTED_WITH" in out
    assert "RELATED_TO" not in out


def test_a_sympathy_edge_is_described_as_the_edge_it_writes():
    from services.agents.supervisor import _describe_proposals

    out = _describe_proposals([{
        "entity_id": "AAPL", "action": "ADD_SYMPATHY_EDGE",
        "data": {"sympathy_ticker": "MSFT"},
    }])
    assert "SYMPATHY_MOVER" in out


def test_both_writers_resolve_the_predicate_through_one_function():
    """What stops them drifting apart again -- they already had, twice over."""
    assert SUPERVISOR_SRC.count("_proposed_predicate(action, data)") >= 3, (
        "the batch writer, the single writer and the describer must share it"
    )
    assert 'data.get("relation_type", "RELATED_TO")' not in SUPERVISOR_SRC, (
        "a second, independent resolution is how the two came to disagree"
    )
