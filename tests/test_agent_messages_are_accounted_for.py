"""A message an agent decides not to act on should leave a trace.

The enrichment tier ends its dispatch chains with `dropped(...)`, which counts
the discard and escalates when the same site keeps firing -- one unmatched
shape is a new producer being wired up, the same shape unmatched ten thousand
times is a feed being thrown away, and only the count tells them apart. That
mechanism found 8,081 crypto events discarded in 28 minutes.

The agent tier has the same shape and none of the instrumentation, on the tier
that costs the most per message. Thirty-six bare returns across eleven agents,
and the one this file was written for: `/feedback` publishes an
analyst's verdict to RULES_FEEDBACK, the rule synthesiser subscribes to
RULES_FEEDBACK, and the topic-contract check reports the pairing as healthy --
a producer and a consumer, correctly wired.

What that check cannot see is that the message reached the agent's `else`
branch, found no `brief.headline_summary`, and returned on the next line.
**A consumer that discards every message of a type still counts as a
consumer.** Every analyst verdict the platform has collected arrived at an
agent and was dropped by it.
"""
import ast
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

AGENTS = ROOT / "services" / "agents"
HANDLER_NAMES = {"process", "process_message", "handle", "handle_message", "on_message"}

# Today's count of bare returns in agent handlers with no recorder before them.
#
# A ratchet, like the debug-only handler count beside it: it may fall and must
# not rise. Deliberately blunt. Some of these are decisions rather than routing
# gaps -- an anomaly below threshold, a ticker outside the supported universe --
# and a few are a handled message completing normally. Classifying all of them
# correctly today would be churn in the tier this audit has most often broken.
#
# What the number does is stop the population growing silently: new code either
# records why it discarded a message, or moves this figure and says so in a
# commit. Each dispatch fall-through that gets a `dropped(...)` takes it down.
MAX_SILENT_AGENT_DROPS = 36

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


RECORDERS = {"dropped", "swallowed"}


def _records_a_drop(stmt) -> bool:
    """Is this statement a call to the quiet-failure recorder?"""
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
        return False
    func = stmt.value.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
    return name in RECORDERS


class _Handlers(ast.NodeVisitor):
    """Bare returns inside an agent's message handler, and whether each says so.

    Read from the syntax tree rather than from a window of nearby lines. A
    `swallowed(...)` inside an unrelated `except` block a few lines above is not
    instrumentation of this return, and a line-window detector counts it as one
    -- the same class of mistake as the regex that once read four live event
    types as dead, and as the topic scan that missed `input_topics=` while
    writing this file.
    """

    def __init__(self, source):
        self.silent = []
        self.accounted = []

    def visit_AsyncFunctionDef(self, node):
        self._check(node)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        self._check(node)
        self.generic_visit(node)

    def _check(self, node):
        if node.name not in HANDLER_NAMES:
            return
        for parent in ast.walk(node):
            for field in ("body", "orelse", "finalbody"):
                block = getattr(parent, field, None)
                if not isinstance(block, list):
                    continue
                for index, stmt in enumerate(block):
                    if not isinstance(stmt, ast.Return):
                        continue
                    if stmt.value is not None and not (
                        isinstance(stmt.value, ast.Constant) and stmt.value.value is None
                    ):
                        continue
                    # Accounted for only when a recorder runs in the same block
                    # before this return.
                    target = (
                        self.accounted
                        if any(_records_a_drop(s) for s in block[:index])
                        else self.silent
                    )
                    target.append(f"{node.name}:{stmt.lineno}")


def _scan():
    silent, accounted = {}, {}
    for path in sorted(AGENTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        source = path.read_text(encoding="utf-8", errors="replace")
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        visitor = _Handlers(source)
        visitor.visit(tree)
        if visitor.silent:
            silent[path.name] = visitor.silent
        if visitor.accounted:
            accounted[path.name] = visitor.accounted
    return silent, accounted


def test_silent_agent_drops_do_not_grow():
    silent, _ = _scan()
    total = sum(len(v) for v in silent.values())
    assert total <= MAX_SILENT_AGENT_DROPS, (
        f"{total} early returns in agent message handlers record nothing, up "
        f"from {MAX_SILENT_AGENT_DROPS}. A message an agent declines to act on "
        f"should be counted, or a routing gap is indistinguishable from a "
        f"deliberate decision: {silent}"
    )


def test_the_rule_synthesizer_accounts_for_what_it_cannot_route():
    """The first of the thirty-seven to be instrumented, and the reason why."""
    _, accounted = _scan()
    assert any(
        "handle" in entry for entry in accounted.get("rule_agent.py", [])
    ), "the dispatch fall-through that ate every analyst verdict is uncounted again"

    source = (AGENTS / "rule_agent.py").read_text(encoding="utf-8")
    assert "agents.rule_synthesizer.unrouted_message" in source


# -- and the verdict now reaches the thing that decides a rule's fate --------


class _Raw:
    def __init__(self, values):
        self.values = values

    async def hgetall(self, key):
        return self.values.get(key, {})


class _Redis:
    def __init__(self, **values):
        self.raw = _Raw(values)


def _agent(redis):
    import logging

    from services.agents.rule_agent import RuleSynthesizerAgent

    agent = RuleSynthesizerAgent.__new__(RuleSynthesizerAgent)
    agent.logger = logging.getLogger("test.rule_agent")
    agent.redis = redis
    return agent


async def test_an_analyst_verdict_reaches_the_curator():
    """It used to reach the `else` branch and return on the next line."""
    from shared.utils.rule_feedback import RULE_FEEDBACK_KEY

    redis = _Redis(**{f"{RULE_FEEDBACK_KEY}:rule_x": {"total": "9", "negative": "9"}})
    agent = _agent(redis)
    asked = []

    async def _prune(context):
        asked.append(context)

    agent._maybe_prune_rules = _prune

    await agent.handle({"source": "analyst", "rule_id": "rule_x", "verdict": "wrong"})

    assert asked, "a rule nine analysts called wrong did not reach the prune pass"
    assert "rule_x" in asked[0]
    assert "9 of 9" in asked[0]


async def test_one_complaint_does_not_spend_an_inference():
    """A single analyst on a single bad morning must not drive the curator.

    The route's own docstring says feedback is evidence rather than a command;
    the review threshold is where that is enforced.
    """
    from shared.utils.rule_feedback import RULE_FEEDBACK_KEY

    redis = _Redis(**{f"{RULE_FEEDBACK_KEY}:rule_y": {"total": "1", "negative": "1"}})
    agent = _agent(redis)
    asked = []

    async def _prune(context):
        asked.append(context)

    agent._maybe_prune_rules = _prune

    await agent.handle({"source": "analyst", "rule_id": "rule_y", "verdict": "wrong"})
    assert asked == []


async def test_an_unroutable_message_is_counted_rather_than_dropped():
    from shared.utils import quiet_failures

    agent = _agent(_Redis())
    before = quiet_failures.snapshot()

    await agent.handle({"nothing": "the agent recognises"})

    after = quiet_failures.snapshot()
    site = "agents.rule_synthesizer.unrouted_message"
    assert site in after, "the fall-through recorded nothing"
    assert after[site]["count"] > before.get(site, {}).get("count", 0)
