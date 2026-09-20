"""What the swarm writes for itself has to fit in what it costs to read.

Measured on the running deployment 2026-09-19, the global context block was
5,711 characters -- about 1,427 tokens -- paid for by three agents on every
call, and 73% of it was the swarm quoting itself:

    SHARED SWARM MEMORIES    2,518 ch   (~629 tok)   44%
    ACTIVE AGENT BULLETINS   1,634 ch   (~409 tok)   29%
    LATEST GLOBAL NEWS         998 ch   (~249 tok)   17%
    TOP ML ANOMALIES           393 ch   (~ 98 tok)    7%

Four of the five memories were `stock_correlation_agent` restating the same two
ticker pairs, and two of the four bulletins carried conviction 0%.

The counts were always capped -- five memories, five bulletins. The *length* of
each was not, and the entries are written by a model, so as the rationales grew
the block grew with them: average agent prompt went 2,325 -> 3,386 characters
over twenty-four hours and the maximum went 2,918 -> 11,001. A cap on how many
and none on how long is not a budget.

These tests pin the three filters that close it.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.base import (  # noqa: E402
    PROMPT_ENTRY_MAX_CHARS,
    AgentBulletin,
    SentinelAgent,
    _prompt_entry_topic,
)


class _Probe(SentinelAgent):
    """Concrete only so the class can be instantiated; neither method is used."""

    async def handle(self, message):  # pragma: no cover - never called
        raise NotImplementedError

    @property
    def output_topic(self):  # pragma: no cover - never called
        raise NotImplementedError


def _agent(bulletins):
    a = object.__new__(_Probe)

    async def _subscribe(types, limit=10):
        return list(bulletins)[:limit]

    a.subscribe_bulletins = _subscribe
    return a


def _bulletin(summary, conviction=0.8, agent="stock_correlation_agent",
              ticker="BTCUSDT", btype="alert"):
    return AgentBulletin(
        agent_name=agent,
        bulletin_type=btype,
        ticker=ticker,
        conviction=conviction,
        summary=summary,
    )


# ── the de-duplication key ────────────────────────────────────────────────────


def test_the_topic_is_the_claim_not_the_reasoning():
    """Two rewordings of one finding share a topic; two findings do not."""
    a = _prompt_entry_topic(
        "Dynamic Correlation Discovery (ZW=F / BTCUSDT): supply chain dependence"
    )
    b = _prompt_entry_topic(
        "Dynamic Correlation Discovery (ZW=F / BTCUSDT): a shared customer base"
    )
    c = _prompt_entry_topic(
        "Dynamic Correlation Discovery (ZW=F / GOOGL): input cost pressure"
    )
    assert a == b
    assert a != c


def test_the_topic_survives_whitespace_and_case():
    assert _prompt_entry_topic("Regime  Shift:  x") == _prompt_entry_topic(
        "regime shift: y"
    )


def test_a_missing_topic_is_not_a_shared_one():
    """Empty must not collapse unrelated entries onto one another."""
    assert _prompt_entry_topic(None) == ""
    assert _prompt_entry_topic("") == ""


# ── (3) a bulletin at zero conviction asserts nothing ────────────────────────


@pytest.mark.asyncio
async def test_zero_conviction_bulletins_are_dropped():
    agent = _agent([
        _bulletin("No opinion here", conviction=0.0, ticker="AVAXUSDT"),
        _bulletin("A real call", conviction=0.82, ticker="BTCUSDT"),
    ])
    out = await agent.get_bulletins_for_prompt()
    assert "A real call" in out
    assert "No opinion here" not in out


@pytest.mark.asyncio
async def test_the_default_conviction_is_not_dropped():
    """0.5 is the unset default. Only a deliberate zero is a non-assertion."""
    agent = _agent([_bulletin("Default conviction call", conviction=0.5)])
    out = await agent.get_bulletins_for_prompt()
    assert "Default conviction call" in out


@pytest.mark.asyncio
async def test_nothing_to_show_yields_no_header():
    """An empty section is still a header and two newlines of prompt."""
    agent = _agent([_bulletin("dropped", conviction=0.0)])
    assert await agent.get_bulletins_for_prompt() == ""


# ── (2) one finding restated is one finding ──────────────────────────────────


@pytest.mark.asyncio
async def test_restatements_of_one_claim_collapse():
    agent = _agent([
        _bulletin("Dynamic Correlation Discovery (ZW=F / BTCUSDT): reason one"),
        _bulletin("Dynamic Correlation Discovery (ZW=F / BTCUSDT): reason two"),
        _bulletin("Dynamic Correlation Discovery (ZW=F / BTCUSDT): reason three"),
    ])
    out = await agent.get_bulletins_for_prompt()
    assert out.count("Dynamic Correlation Discovery") == 1


@pytest.mark.asyncio
async def test_distinct_claims_are_all_kept():
    agent = _agent([
        _bulletin("Correlation (ZW=F / BTCUSDT): x", ticker="BTCUSDT"),
        _bulletin("Correlation (ZW=F / GOOGL): y", ticker="GOOGL"),
    ])
    out = await agent.get_bulletins_for_prompt()
    assert "BTCUSDT" in out and "GOOGL" in out


@pytest.mark.asyncio
async def test_the_same_claim_from_two_agents_is_two_claims():
    """Agreement between agents is a signal, not a duplicate."""
    agent = _agent([
        _bulletin("Regime shift: risk off", agent="macro_intelligence_engine"),
        _bulletin("Regime shift: risk off", agent="quant_trading_engine"),
    ])
    out = await agent.get_bulletins_for_prompt()
    assert out.count("Regime shift") == 2


# ── (1) a claim needs its subject, not its argument ──────────────────────────


@pytest.mark.asyncio
async def test_a_long_summary_is_capped():
    agent = _agent([_bulletin("Real claim: " + ("padding " * 200))])
    out = await agent.get_bulletins_for_prompt()
    body = out.split("): ", 1)[1]
    assert len(body) <= PROMPT_ENTRY_MAX_CHARS + 8
    assert "Real claim" in out, "the cap must keep the claim, not just clip it"


@pytest.mark.asyncio
async def test_the_section_is_bounded_by_the_limit():
    """Over-reading to refill de-duplicated slots must not overfill them."""
    agent = _agent([
        _bulletin(f"Claim {i}: reasoning", ticker=f"T{i}") for i in range(40)
    ])
    out = await agent.get_bulletins_for_prompt(limit=5)
    assert out.count("\n- ") + out.startswith("- ") <= 5
    assert len([ln for ln in out.splitlines() if ln.startswith("- ")]) == 5


@pytest.mark.asyncio
async def test_the_block_is_smaller_than_what_it_replaced():
    """The whole point, stated as an assertion rather than a hope.

    The measured section was 1,634 characters for four bulletins, two of which
    asserted nothing. The same input must now cost meaningfully less.
    """
    verbose = (
        "Dynamic Correlation Discovery (ZW=F / {t}): The observed relationship "
        "suggests a causal mechanism rooted in supply chain dependency, where "
        "macroeconomic factors influence supply and demand dynamics, creating a "
        "symbiotic relationship grounded in empirical statistics indicating a "
        "positive correlation coefficient of unmeasured strength."
    )
    agent = _agent([
        _bulletin(verbose.format(t="AVAXUSDT"), conviction=0.0, ticker="AVAXUSDT"),
        _bulletin(verbose.format(t="GOOGL"), conviction=0.0, ticker="GOOGL"),
        _bulletin(verbose.format(t="BTCUSDT"), conviction=0.82, ticker="BTCUSDT"),
        _bulletin(verbose.format(t="BTCUSDT"), conviction=0.82, ticker="BTCUSDT"),
    ])
    out = await agent.get_bulletins_for_prompt()
    assert len(out) < 700, f"section is still {len(out)} chars"
