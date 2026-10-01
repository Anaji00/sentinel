"""An agent should not be handed messages it discards in full.

Every message on a subscribed topic is deserialised, counted against the
dispatch semaphore and then rejected on the first lines of `handle()`. That
rejection is correct and it happens too late: the cost was already paid.

Measured on the running broker, 2026-09-20:

    knowledge_graph_engine   112,661 messages processed in one hour
                                   1 inference produced

    of its subscriptions:
      enriched.events                 6,369,013 messages
      agents.ontology.updates         3,138,845   dropped in full
      sentinel.ontology.proposals     2,815,243   dropped in full, its own output
      sentinel.correlations             432,523
      agents.ontology.unknown_entities         1   the topic its classifier serves

Both of the dropped topics were verified rather than assumed: 30 consecutive
proposals sampled from the broker were 26 LINK_ENTITY and 4 MERGE_ONTOLOGY_NODE
with no headline on any of them, which is the exact shape `handle()` refuses on
its second branch. The updates topic is supervisor commit receipts, which carry
no headline either.

`edge_validator` is the other shape: `handle()` returns None for every message
unconditionally -- all its work is in the five-minute sweep -- and it was
subscribed to RAW_NEWS, CORRELATIONS and INTEL_BRIEFS.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MAIN = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")

AGENT_FILES = {
    "macro_intelligence_engine": "macro_intelligence_engine",
    "quant_trading_engine": "quant_trading_engine",
    "knowledge_graph_engine": "knowledge_graph_engine",
    "radar_agent": "radar_agent",
    "rule_synthesizer": "rule_agent",
    "supervisor": "supervisor",
    "consensus_engine": "consensus_engine",
    "adversarial_wargamer": "adversarial_wargamer",
    "edge_validator": "edge_validator",
    "stock_correlation_agent": "stock_correlation_agent",
}


def _subscriptions() -> dict:
    """{agent_name: {TOPIC, ...}} as main.py wires them."""
    out = {}
    for name, body in re.findall(
        r'agent_name="([a-z_]+)",\s*(?:#[^\n]*\n\s*)*input_topics=\[(.*?)\]', MAIN, re.S
    ):
        out[name] = set(re.findall(r"Topics\.([A-Z_]+)", body))
    return out


def _publications(agent: str) -> set:
    src = (ROOT / f"services/agents/{AGENT_FILES[agent]}.py").read_text(encoding="utf-8")
    return set(re.findall(r"_producer\.send\(\s*Topics\.([A-Z_]+)", src)) | set(
        re.findall(r"return Topics\.([A-Z_]+)", src)
    )


def test_every_agent_is_wired():
    subs = _subscriptions()
    assert set(subs) == set(AGENT_FILES), sorted(set(AGENT_FILES) ^ set(subs))


# ── the two that were dropped in full ────────────────────────────────────────


def test_the_graph_engine_does_not_read_the_proposals_it_writes():
    subs = _subscriptions()["knowledge_graph_engine"]
    assert "ONTOLOGY_PROPOSALS" not in subs
    assert "ONTOLOGY_PROPOSALS" in _publications("knowledge_graph_engine"), (
        "it is still the producer; only the subscription was the mistake"
    )


def test_the_graph_engine_does_not_read_commit_receipts():
    """Re-add this only with a branch in handle() that acts on one."""
    assert "ONTOLOGY_UPDATES" not in _subscriptions()["knowledge_graph_engine"]


def test_a_scheduled_agent_does_not_drink_from_the_firehose():
    """edge_validator.handle() returns None for everything, unconditionally."""
    src = (ROOT / "services/agents/edge_validator.py").read_text(encoding="utf-8")
    body = src[src.index("async def handle("):]
    assert "return None" in body.split("\n\n")[0], "handle() is no longer a no-op; revisit this"

    subs = _subscriptions()["edge_validator"]
    for firehose in ("RAW_NEWS", "CORRELATIONS", "INTEL_BRIEFS", "ENRICHED_EVENTS"):
        assert firehose not in subs, f"{firehose} is deserialised only to be discarded"


# ── the ratchet ──────────────────────────────────────────────────────────────

# Subscriptions where an agent reads a topic it also writes.
#
# Not always wrong: the consensus engine reads CONSENSUS_REPORTS so a
# disagreement it published reaches an arbiter, and edge_validator keeps
# QUANT_DISCOVERIES because it is small and it is that sweep's own output
# channel. What must not happen is the number growing unnoticed -- a self-loop
# on a busy topic is an agent paying a dispatch slot to be handed its own work
# back, which is what cost the graph engine 5.9M messages.
MAX_SELF_CONSUMED_SUBSCRIPTIONS = 4


def test_self_consumption_does_not_grow():
    subs = _subscriptions()
    loops = {a: sorted(t & _publications(a)) for a, t in subs.items()}
    loops = {a: t for a, t in loops.items() if t}
    total = sum(len(t) for t in loops.values())
    assert total <= MAX_SELF_CONSUMED_SUBSCRIPTIONS, (
        f"{total} self-consumed subscriptions, up from "
        f"{MAX_SELF_CONSUMED_SUBSCRIPTIONS}: {loops}"
    )


# Total fan-out across the tier. Every subscription is a deserialisation cost
# on a topic, paid per agent; RAW_NEWS and CORRELATIONS reached all ten.
MAX_TOTAL_SUBSCRIPTIONS = 62


def test_total_fan_out_does_not_grow():
    subs = _subscriptions()
    total = sum(len(t) for t in subs.values())
    assert total <= MAX_TOTAL_SUBSCRIPTIONS, (
        f"{total} subscriptions across {len(subs)} agents, up from "
        f"{MAX_TOTAL_SUBSCRIPTIONS}. Each one is the whole topic, filtered "
        "afterwards: "
        + str({a: len(t) for a, t in sorted(subs.items(), key=lambda kv: -len(kv[1]))})
    )


# ── a subscribed topic whose shape no branch matched ─────────────────────────


def test_the_rule_synthesiser_routes_macro_decoupling():
    """stock_correlation_agent's findings reached this agent and were dropped.

    It publishes {agent, created_at, assessment} to MACRO_DECOUPLING, the rule
    synthesiser subscribes to that topic, and no branch matched the shape -- so
    the counter recorded `keys=['agent', 'assessment', 'created_at']` as
    unroutable. That payload is the one input here that is already a measured,
    named relationship between two instruments, which is what a rule is made of.
    """
    subs = _subscriptions()["rule_synthesizer"]
    assert "MACRO_DECOUPLING" in subs, "the subscription went; so should this branch"

    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    assert 'message.get("assessment", {}).get("macro_asset")' in src, (
        "no branch reads the macro-decoupling shape this agent is sent"
    )
    assert "MACRO ASSET:" in src, "the branch does not reach the prompt"


# ── a topic left written by two producers and read by nobody ─────────────────


def test_the_supervisor_publishes_no_commit_receipts():
    """Removing the consumer left the producers running.

    ONTOLOGY_UPDATES carried 3.1M supervisor receipts and a duplicate copy of
    triples the graph engine had already merged into Neo4j. Its only consumer
    was removed once it was established that no branch had ever applied an
    update -- which turned the topic into a write with no reader rather than a
    write with a wasteful one. The base class publishes any non-None `handle()`
    return, so the fix is that there is nothing to publish.
    """
    import ast

    src = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    assert "return Topics.ONTOLOGY_UPDATES" not in src, (
        "declaring it as output_topic is what makes the base class publish there"
    )
    # Read the returned dicts, not the prose: the docstring explaining this fix
    # quotes the very shape being asserted against.
    tree = ast.parse(src)
    returned = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            for k, v in zip(node.value.keys, node.value.values):
                if isinstance(k, ast.Constant) and k.value == "action":
                    if isinstance(v, ast.Constant):
                        returned.add(v.value)
    assert not returned & {"single_commit", "batch_commit"}, (
        f"receipts nothing reads are still returned: {sorted(returned)}"
    )


def test_the_graph_engine_does_not_echo_triples_it_has_already_merged():
    src = (ROOT / "services/agents/knowledge_graph_engine.py").read_text(encoding="utf-8")
    assert "Topics.ONTOLOGY_UPDATES" not in src, (
        "the emit was kept 'for backwards compatibility' with this agent's own "
        "subscription, which no longer exists"
    )


def test_nothing_produces_the_orphaned_topic_any_more():
    """The constant stays; a producer would make it an orphan again."""
    agents = ROOT / "services" / "agents"
    producers = [
        p.name for p in agents.glob("*.py")
        if "Topics.ONTOLOGY_UPDATES" in p.read_text(encoding="utf-8")
        and p.name != "main.py"
    ]
    assert not producers, f"ONTOLOGY_UPDATES has no consumer, but: {producers}"


# ── found by splitting the counter that was burying it ───────────────────────


def test_the_rule_synthesiser_does_not_read_raw_news():
    """A raw news item has no `brief`, so the default branch discarded it.

    Measured lifetime volumes for this agent's inputs:

        sentinel.correlations     435,021   refused on purpose (rule firings)
        events.raw.news            49,464   no branch -- every one dropped
        agents.macro.decoupling    35,365   branch added by the pass before
        scenarios.generated         1,697
        agents.intel.briefs         1,148   the analysed form of raw news
        agents.rules.feedback         381
        agents.quant.discoveries      123
        agents.macro.assessment        42
        agents.rules.candidates        24   the topic this agent exists for
        agents.insider.clusters         0

    RAW_NEWS was 43x the volume of INTEL_BRIEFS, which carries the same
    material after analysis and does have a branch. It only became visible once
    the deliberate refusal of rule firings stopped sharing its counter.
    """
    subs = _subscriptions()["rule_synthesizer"]
    assert "RAW_NEWS" not in subs, "an input with no branch, at 49,464 messages"
    assert "INTEL_BRIEFS" in subs, "the analysed form is what the else branch reads"


def test_the_rule_synthesiser_routes_quant_discoveries():
    """The branch read three keys the producer does not publish.

    `quant_trading_engine` sends {agent, agent_run_id, trigger, discovery,
    quality_metrics, created_at} with the finding nested under `discovery`. The
    branch tested `type == "quant_discovery"` and read `description` and
    `correlated_assets` -- none of the three exist on the wire, so every quant
    discovery fell to the default branch and was counted unroutable.

    Found nine minutes after the unrouted counter stopped sharing a ladder with
    the deliberate refusal of rule firings, which had been burying it at 134:1.
    """
    subs = _subscriptions()["rule_synthesizer"]
    assert "QUANT_DISCOVERIES" in subs

    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    assert 'isinstance(message.get("discovery"), dict)' in src, (
        "no branch reads the shape this topic actually carries"
    )
    for field in ("primary_ticker", "peer_tickers", "catalyst_category",
                  "structural_decoupling", "macro_instruments"):
        assert field in src, f"{field} never reaches the prompt"


def test_an_unverified_peer_is_not_offered_as_evidence():
    """`verification` defaults to "untested" so the two cannot be confused."""
    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    assert '"untested"' in src
    assert "statistically verified" in src, (
        "the prompt must say how many peers the statistics actually supported"
    )
