"""The swarm has to be more than one agent saying one thing about one asset.

Measured on the running deployment 2026-09-19, before any of this:

    consensus signals   : 4
    contributing_agents : {1: 4}      every signal fused exactly one opinion
    agreement_ratio     : {0.0: 4}
    bulletin authors    : {'stock_correlation_agent': 4}
    macro_asset         : {'ZW=F': 4}

Eighteen macro assets were available and seventeen -- VXX, HYG, the whole
yield curve, oil, gold, the index futures -- had never once been examined,
because the selection was `macro_assets[0]` over a deterministic Redis SCAN.
Three consecutive runs returned the identical ordering and therefore the
identical choice.

These tests pin the four mechanisms that close that: rotation over the macro
axis, round-robin over bulletin authors, a cap and de-duplication on the
cross-agent block, and a self-fallback for the one agent the peer filter
leaves with nothing.
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
)
from services.agents.stock_correlation_agent import _next_macro_asset  # noqa: E402


class _Raw:
    """Only the four calls these paths make."""

    def __init__(self, hash_=None, mems=None, consensus=None, fail=False):
        self.h = dict(hash_ or {})
        self.mems = list(mems or [])
        self.consensus = consensus
        self.fail = fail

    async def hgetall(self, key):
        if self.fail:
            raise RuntimeError("redis down")
        return dict(self.h)

    async def hset(self, key, field, value):
        self.h[field] = value

    async def get(self, key):
        return self.consensus

    async def zrevrange(self, key, lo, hi):
        return self.mems[lo:hi + 1]


class _Redis:
    def __init__(self, raw):
        self.raw = raw


MACRO = ["ZW=F", "VXX", "HYG", "US10Y", "NQ=F"]


# ── B: the macro axis rotates ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_never_examined_asset_is_chosen_first():
    """A symbol absent from the hash must sort ahead of one already seen."""
    raw = _Raw(hash_={"ZW=F": "9999999999"})
    assert await _next_macro_asset(_Redis(raw), MACRO) == "VXX"


@pytest.mark.asyncio
async def test_rotation_reaches_every_macro_asset():
    """The bug in one assertion: seventeen of eighteen were unreachable."""
    raw = _Raw()
    redis = _Redis(raw)
    picked = [await _next_macro_asset(redis, MACRO) for _ in range(len(MACRO))]
    assert sorted(picked) == sorted(MACRO), picked
    assert len(set(picked)) == len(MACRO), "an asset was visited twice before all were"


@pytest.mark.asyncio
async def test_the_least_recently_examined_wins():
    raw = _Raw(hash_={"ZW=F": "100", "VXX": "500", "HYG": "50",
                      "US10Y": "400", "NQ=F": "300"})
    assert await _next_macro_asset(_Redis(raw), MACRO) == "HYG"


@pytest.mark.asyncio
async def test_the_choice_is_recorded_so_it_is_not_repeated():
    raw = _Raw()
    redis = _Redis(raw)
    first = await _next_macro_asset(redis, MACRO)
    assert first in raw.h
    assert await _next_macro_asset(redis, MACRO) != first


@pytest.mark.asyncio
async def test_redis_failure_falls_back_to_previous_behaviour():
    """A rotation that fails closed would stop the agent entirely."""
    raw = _Raw(fail=True)
    assert await _next_macro_asset(_Redis(raw), MACRO) == MACRO[0]


@pytest.mark.asyncio
async def test_no_macro_assets_is_not_a_crash():
    assert await _next_macro_asset(_Redis(_Raw()), []) == ""


# ── A: one agent cannot own the bulletin section ─────────────────────────────


class _Probe(SentinelAgent):
    async def handle(self, message):  # pragma: no cover - never called
        raise NotImplementedError

    @property
    def output_topic(self):  # pragma: no cover - never called
        raise NotImplementedError


def _bulletin(summary, agent="stock_correlation_agent", ticker="BTCUSDT",
              conviction=0.8, btype="alert"):
    return AgentBulletin(agent_name=agent, bulletin_type=btype, ticker=ticker,
                         conviction=conviction, summary=summary)


def _agent_with(bulletins):
    a = object.__new__(_Probe)

    async def _subscribe(types, limit=10):
        return list(bulletins)[:limit]

    a.subscribe_bulletins = _subscribe
    return a


@pytest.mark.asyncio
async def test_a_quiet_agent_is_not_buried_by_a_prolific_one():
    """Recency order gave every row to whichever agent runs most often."""
    noisy = [_bulletin(f"Correlation {i}: reasoning", ticker=f"T{i}")
             for i in range(10)]
    quiet = [_bulletin("Regime shift: risk off", agent="macro_intelligence_engine",
                       ticker="SPY", btype="regime_change")]
    out = await _agent_with(noisy + quiet).get_bulletins_for_prompt(limit=5)
    assert "macro_intelligence_engine" in out, out


@pytest.mark.asyncio
async def test_round_robin_alternates_rather_than_blocks():
    a = [_bulletin(f"A{i}: r", agent="agent_a", ticker=f"A{i}") for i in range(5)]
    b = [_bulletin(f"B{i}: r", agent="agent_b", ticker=f"B{i}") for i in range(5)]
    out = await _agent_with(a + b).get_bulletins_for_prompt(limit=4)
    rows = [ln for ln in out.splitlines() if ln.startswith("- ")]
    assert len(rows) == 4
    assert sum("agent_b" in r for r in rows) >= 2, rows


@pytest.mark.asyncio
async def test_a_single_agent_still_fills_the_section():
    """Fairness must not starve the section when there is only one publisher."""
    only = [_bulletin(f"Claim {i}: r", ticker=f"T{i}") for i in range(8)]
    out = await _agent_with(only).get_bulletins_for_prompt(limit=5)
    assert len([ln for ln in out.splitlines() if ln.startswith("- ")]) == 5


# ── C / D / F: the cross-agent block ─────────────────────────────────────────


def _cross_agent(name, bulletins, mems, consensus=None):
    a = object.__new__(_Probe)
    a.name = name
    a.redis = _Redis(_Raw(mems=mems, consensus=consensus))
    import logging
    a.logger = logging.getLogger("test")

    async def _read(ticker=None, **kw):
        return list(bulletins)

    a.read_bulletins = _read
    return a


def _mem(agent, text):
    import json
    return json.dumps({"agent": agent, "text": text}).encode()


CLAIM = "Dynamic Correlation Discovery (ZW=F / BTCUSDT)"


@pytest.mark.asyncio
async def test_a_claim_is_not_printed_twice():
    """publish_bulletin mirrors into memory, so both stores held the same text."""
    agent = _cross_agent(
        "knowledge_graph_engine",
        [_bulletin(f"{CLAIM}: because supply chains")],
        [_mem("stock_correlation_agent", f"[alert] {CLAIM}: because supply chains")],
    )
    out = await agent.get_cross_agent_context()
    assert out.count("ZW=F / BTCUSDT") == 1, out


@pytest.mark.asyncio
async def test_a_claim_with_no_colon_is_not_printed_twice():
    """The case the first version of this file missed.

    `_prompt_entry_topic` splits on ":" to separate claim from reasoning, so a
    summary containing no colon is compared whole -- and the memory mirror
    appends " (conviction 0.60)" to it. Live after the first fix:

        Active Bulletins:   SELL GOOGL @ $7.16 -> $6.89 (Kelly 2.0%)
        Cross-Agent Memories: [signal] SELL GOOGL @ ... (conviction 0.60)

    Same claim, two rows, because the tail differed.
    """
    signal = "SELL GOOGL @ $7.16 -> $6.89 (Kelly 2.0%)"
    agent = _cross_agent(
        "radar_agent",
        [_bulletin(signal, agent="quant_trading_engine", ticker="GOOGL",
                   btype="signal")],
        [_mem("quant_trading_engine", f"[signal] {signal} (conviction 0.60)")],
    )
    out = await agent.get_cross_agent_context()
    assert out.count("SELL GOOGL") == 1, out


@pytest.mark.asyncio
async def test_a_genuinely_different_memory_still_appears():
    agent = _cross_agent(
        "knowledge_graph_engine",
        [_bulletin(f"{CLAIM}: reasoning")],
        [_mem("radar_agent", "NVDA escalated to primary surveillance: volume")],
    )
    out = await agent.get_cross_agent_context()
    assert "NVDA escalated" in out


@pytest.mark.asyncio
async def test_cross_agent_entries_are_capped():
    agent = _cross_agent(
        "knowledge_graph_engine",
        [_bulletin("Real claim: " + ("padding " * 200))],
        [_mem("radar_agent", "Other claim: " + ("padding " * 200))],
    )
    out = await agent.get_cross_agent_context()
    for line in out.splitlines():
        if line.startswith("- ") or line.startswith("["):
            assert len(line) < PROMPT_ENTRY_MAX_CHARS + 80, line


@pytest.mark.asyncio
async def test_the_sole_producer_gets_its_own_conclusions_back():
    """It received 338 characters where its peers received 3,264, and with
    nothing to read it re-derived the same correlation every ten minutes."""
    agent = _cross_agent(
        "stock_correlation_agent",
        [_bulletin(f"{CLAIM}: reasoning")],                      # its own
        [_mem("stock_correlation_agent", f"[alert] {CLAIM}: r")],  # its own
    )
    out = await agent.get_cross_agent_context()
    assert "Your Own Recent Conclusions" in out
    assert "do not restate" in out


@pytest.mark.asyncio
async def test_own_conclusions_yield_to_real_peer_content():
    """The fallback must not fire when there is something better to say."""
    agent = _cross_agent(
        "stock_correlation_agent",
        [_bulletin("Regime shift: risk off", agent="macro_intelligence_engine")],
        [_mem("stock_correlation_agent", f"[alert] {CLAIM}: r")],
    )
    out = await agent.get_cross_agent_context()
    assert "Your Own Recent Conclusions" not in out
    assert "macro_intelligence_engine" in out


# ── E: the dead channel is gone ──────────────────────────────────────────────


def test_no_publish_to_a_channel_nobody_subscribes_to():
    """`sentinel:bulletins:stream` had one reference in the tree: the publish.

    PUBSUB NUMSUB answered 0 on the running deployment, and Redis discards a
    PUBLISH with no subscriber, so every bulletin was serialised in order to be
    dropped.
    """
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    # Scoped to this one channel on purpose. `sentinel:events:live` is also
    # published from this file and *is* subscribed -- by the API gateway's
    # /events stream -- so a blanket ban on publishing would be wrong.
    assert '"sentinel:bulletins:stream"' not in src, (
        "the bulletin PubSub publish is back; nothing subscribes to that channel"
    )
