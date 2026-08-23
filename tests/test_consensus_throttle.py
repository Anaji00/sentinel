"""Stops a whole-swarm computation from being re-run per message.

Consensus is a property of every agent's current opinions, not of any single
message: analyze() re-reads all bulletins, all scorecards and every state digest,
then runs subjective-logic fusion. handle() ignored its message entirely and ran
that global computation for each one, re-deriving an identical answer thousands
of times an hour and holding the consumer at a few hundred messages an hour
against 61,000 of backlog.

Messages still trigger a review, so a burst is reflected promptly; they simply
cannot trigger one faster than the picture can meaningfully change.
"""
import pathlib
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.consensus_engine import ConsensusEngine  # noqa: E402


def _engine():
    e = ConsensusEngine.__new__(ConsensusEngine)
    e.name = "consensus_engine"
    e.logger = MagicMock()
    e.analyze = AsyncMock(return_value=MagicMock(contradictions=[], consensus_signals=[]))
    return e


@pytest.mark.anyio
async def test_a_burst_of_messages_triggers_one_review():
    engine = _engine()
    for _ in range(200):
        await engine.handle({"ticker": "NVDA"})
    assert engine.analyze.await_count == 1, (
        f"{engine.analyze.await_count} global analyses for 200 messages"
    )


@pytest.mark.anyio
async def test_the_first_message_is_not_delayed():
    """Throttling must not mean the swarm waits two minutes for its first read."""
    engine = _engine()
    await engine.handle({"ticker": "NVDA"})
    assert engine.analyze.await_count == 1


@pytest.mark.anyio
async def test_a_later_message_reviews_again_once_the_window_passes():
    import time
    engine = _engine()
    await engine.handle({})
    engine._last_review_at = time.monotonic() - (engine._REVIEW_INTERVAL_SEC + 1)
    await engine.handle({})
    assert engine.analyze.await_count == 2, "the review never runs again"


def test_the_interval_is_short_enough_to_stay_responsive():
    """Consensus that lags the swarm by too long stops describing it."""
    assert 30 <= ConsensusEngine._REVIEW_INTERVAL_SEC <= 600


@pytest.mark.anyio
async def test_a_throttled_message_is_not_reported_as_a_result():
    """Returning a stale report would publish the same consensus repeatedly."""
    engine = _engine()
    await engine.handle({})
    assert await engine.handle({}) is None
