"""
tests/test_inference_admission.py

Which twenty of a hundred and fifty thousand, and why.

This host affords roughly twenty model inferences an hour against an input of
about a hundred and fifty thousand events. `try_acquire` had taken a `score`
parameter since it was written and never read it, so admission was decided by
one thing: which caller happened to arrive while the slot was free.

That makes timing the selection criterion for everything the system chooses to
think about. Asked which twenty events were analysed and why, the honest answer
was "whichever arrived at the right moment" -- which is not a defensible answer
for a surveillance platform, however well every individual component works.

A candidate now has to beat what this process has lately been seeing. The bar
is a percentile of recent scores rather than a fixed threshold, for the same
reason the detectors are: only the deployment knows what ordinary looks like.
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.utils.inference_budget import (  # noqa: E402
    ADMISSION_MIN_HISTORY,
    ADMISSION_PERCENTILE,
    MAX_HOLDBACK_SEC,
    InferenceBudget,
)


async def _warmed(ordinary=0.5, n=200):
    """A budget whose bar has seen a steady stream of ordinary traffic.

    `redis=None` on purpose: the score window is now shared through Redis, and
    passing None exercises the per-process fallback, which is the path a Redis
    outage takes and the one these tests are about.
    """
    budget = InferenceBudget(None, "test-model")
    for i in range(n):
        await budget._passes_admission_bar(ordinary + (i % 7) * 0.01)
    return budget


# -- the selection ------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_weak_candidate_is_held_back():
    assert await (await _warmed())._passes_admission_bar(0.05) is False


@pytest.mark.asyncio
async def test_a_strong_candidate_is_admitted():
    assert await (await _warmed())._passes_admission_bar(0.99) is True


@pytest.mark.asyncio
async def test_the_bar_tracks_the_stream_rather_than_a_constant():
    """0.6 is unremarkable in a busy stream and exceptional in a quiet one.

    A fixed threshold would call it the same thing in both.
    """
    busy = await _warmed(ordinary=0.85)
    quiet = await _warmed(ordinary=0.15)

    assert await busy._passes_admission_bar(0.60) is False
    assert await quiet._passes_admission_bar(0.60) is True


# -- the rails that stop it starving -----------------------------------------

@pytest.mark.asyncio
async def test_an_unscored_caller_is_never_refused():
    """A caller that does not score its work must not be silently starved by a
    selection rule it never participated in."""
    assert await (await _warmed())._passes_admission_bar(None) is True


@pytest.mark.asyncio
async def test_a_malformed_score_is_not_treated_as_low():
    assert await (await _warmed())._passes_admission_bar("not a number") is True


@pytest.mark.asyncio
async def test_admission_is_open_until_there_is_history_to_judge_against():
    """A bar computed from a handful of samples mostly encodes their order."""
    budget = InferenceBudget(None, "test-model")
    for _ in range(ADMISSION_MIN_HISTORY - 1):
        assert await budget._passes_admission_bar(0.01) is True


@pytest.mark.asyncio
async def test_a_long_holdback_eventually_admits_anything():
    """An idle slot helps nobody, and a rule that can refuse forever is worse
    than no rule."""
    budget = await _warmed()
    assert await budget._passes_admission_bar(0.05) is False

    budget._last_admit = time.monotonic() - (MAX_HOLDBACK_SEC + 1)
    assert await budget._passes_admission_bar(0.05) is True


# -- the parameter is actually supplied --------------------------------------

@pytest.mark.asyncio
async def test_the_agent_passes_a_score_when_claiming_a_slot():
    """The bar is inert unless callers fill the parameter -- which none did for
    the entire life of the method."""
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert "score=_message_score(message)" in source


@pytest.mark.parametrize(
    "message,expected",
    [
        ({"anomaly_score": 0.93}, 0.93),
        ({"severity": 4}, 0.8),                      # authored 1-5, normalised
        ({"trigger": {"anomaly_score": 0.71}}, 0.71),
        ({"headline": "no score here"}, None),
        ({"anomaly_score": "bad"}, None),
    ],
)
def test_the_score_is_read_from_whatever_the_producer_supplied(message, expected):
    from services.agents.base import _message_score

    assert _message_score(message) == expected


@pytest.mark.asyncio
async def test_the_percentile_is_configurable():
    source = (ROOT / "shared/utils/inference_budget.py").read_text(encoding="utf-8")
    assert "INFERENCE_ADMISSION_PERCENTILE" in source
    assert 0.0 < ADMISSION_PERCENTILE < 1.0


# -- the window is shared, and that is the whole point -------------------------

class _FakeRedis:
    """A list, a trim and an expire. Enough for the score window."""

    def __init__(self):
        self.lists = {}
        self.raw = self

    def pipeline(self):
        return _FakePipe(self)

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, stop):
        self.lists[key] = self.lists.get(key, [])[start: stop + 1]

    async def expire(self, *_a, **_k):
        return True

    async def lrange(self, key, start, stop):
        return self.lists.get(key, [])[start: (None if stop == -1 else stop + 1)]


class _FakePipe:
    def __init__(self, redis):
        self._r = redis
        self._q = []

    def lpush(self, *a):
        self._q.append(("lpush", a)); return self

    def ltrim(self, *a):
        self._q.append(("ltrim", a)); return self

    def expire(self, *a):
        self._q.append(("expire", a)); return self

    async def execute(self):
        for name, args in self._q:
            await getattr(self._r, name)(*args)
        self._q.clear()


@pytest.mark.asyncio
async def test_two_agents_contending_for_one_model_share_one_bar():
    """One model slot is one resource, so it needs one standard.

    The window was a per-process deque, which gave thirteen agents thirteen
    private bars -- none of which could see what the others had declined. An
    agent that only ever sees weak candidates would judge a weak one "better
    than usual" and spend the shared slot on it.
    """
    redis = _FakeRedis()
    busy = InferenceBudget(redis, "shared-model")
    quiet = InferenceBudget(redis, "shared-model")

    # One agent sees a stream of strong candidates.
    for i in range(ADMISSION_MIN_HISTORY * 2):
        await busy._passes_admission_bar(0.90 + (i % 5) * 0.01)

    # The other, which has seen nothing itself, judges against that stream.
    assert await quiet._passes_admission_bar(0.10) is False, (
        "an agent with no history of its own should still meet the shared bar"
    )
    assert len(quiet._recent_scores) <= 2, "it should not need its own history"


@pytest.mark.asyncio
async def test_the_window_outlives_a_process():
    """The bar needs 32 samples and no process lived long enough to collect them.

    Measured: ~20 inferences an hour across thirteen agents, containers
    restarting on every deploy, and the "held back N below-bar candidate(s)"
    line -- which logs on the very first holdback -- never appearing in any
    container's logs. The selection was correct, deployed, and had never once
    applied.
    """
    redis = _FakeRedis()
    first = InferenceBudget(redis, "persisted-model")
    for i in range(ADMISSION_MIN_HISTORY * 2):
        await first._passes_admission_bar(0.80 + (i % 5) * 0.01)

    # A fresh process, as after a deploy.
    reborn = InferenceBudget(redis, "persisted-model")
    assert await reborn._passes_admission_bar(0.05) is False, (
        "a restarted agent should inherit the bar rather than start open"
    )


@pytest.mark.asyncio
async def test_a_redis_outage_degrades_to_the_old_behaviour():
    """Losing the shared window must not disable admission control.

    The per-process deque is kept for exactly this: a Redis failure returns the
    bar to what it was before this change rather than admitting everything.
    """
    class _Broken:
        raw = None

        def pipeline(self):
            raise RuntimeError("redis down")

    budget = InferenceBudget(_Broken(), "outage-model")
    for i in range(ADMISSION_MIN_HISTORY * 2):
        await budget._passes_admission_bar(0.50 + (i % 5) * 0.01)

    assert await budget._passes_admission_bar(0.01) is False
    assert len(budget._recent_scores) >= ADMISSION_MIN_HISTORY


@pytest.mark.asyncio
async def test_an_unreadable_window_does_not_read_as_no_history():
    """No history admits everything, so an empty reply must not look like one."""
    class _EmptyReplies(_FakeRedis):
        async def lrange(self, *_a, **_k):
            return []

    redis = _EmptyReplies()
    budget = InferenceBudget(redis, "empty-model")
    for i in range(ADMISSION_MIN_HISTORY * 2):
        await budget._passes_admission_bar(0.70 + (i % 5) * 0.01)

    assert await budget._passes_admission_bar(0.01) is False, (
        "an empty Redis reply fell through to 'no history', which admits all"
    )
