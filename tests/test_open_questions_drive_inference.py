"""Gap 5: the scarcest resource, allocated by importance but not by informativeness.

Inference is the binding constraint here -- dispatches measured at 440 seconds
against a 420-second timeout, with 71% of candidates receiving no verdict at
all. Admission used to be arrival order, and that was fixed: `try_acquire` now
requires a candidate to beat a percentile of what this process has lately seen.

Importance is not informativeness. Two events at the same anomaly score are not
equally worth an inference if one of them would resolve a scenario the platform
is currently holding open and the other answers nothing. "Which of these
hundred candidates would change what we believe" is answerable, and the data to
answer it already existed: every unresolved scenario carries watch and deny
signals that name their entities, and the focus set already reorders candidates.
Nothing connected the two, so the platform could not prefer an event that would
settle a question it had asked itself.

Deliberately weighted below an agent's own escalation. A scenario waiting on an
entity is a reason to prefer it between equals, not a reason to outrank
something an agent has judged urgent.
"""
import inspect

import pytest

import services.reasoning.scenario_tracker as tracker


def test_the_sweep_publishes_what_it_is_waiting_on():
    assert hasattr(tracker.ScenarioTracker, "_offer_open_questions")
    assert "_offer_open_questions" in inspect.getsource(tracker.ScenarioTracker.check_all), (
        "the mechanism exists and nothing calls it, which is the shape this "
        "audit has found ten times"
    )


def test_open_questions_are_bounded():
    """A sweep over five hundred scenarios must not fill the focus set."""
    assert 0 < tracker.OPEN_QUESTION_LIMIT <= 100


def test_they_rank_below_an_agent_s_own_escalation():
    from shared.utils.focus import FOCUS_MIN_CONVICTION

    assert tracker.OPEN_QUESTION_CONVICTION >= FOCUS_MIN_CONVICTION, (
        "below the floor the offer is silently dropped and the wiring does nothing"
    )
    assert tracker.OPEN_QUESTION_CONVICTION < 1.0


@pytest.mark.anyio
async def test_it_offers_the_entities_the_signals_name():
    offered = []

    class _Redis:
        pass

    async def _fake_offer(redis_client, entity, conviction=1.0, offered_by=""):
        offered.append((entity, conviction, offered_by))
        return True

    t = tracker.ScenarioTracker.__new__(tracker.ScenarioTracker)
    t._redis = _Redis()

    active = [
        {
            "scenario_id": "s1",
            "hypotheses": [
                {
                    "watch_signals": [{"entity": "AAPL", "observable": "block trades"}],
                    "deny_signals": [{"entity": "TSM", "observable": "shipments"}],
                }
            ],
        },
        {
            "scenario_id": "s2",
            "hypotheses": [
                {"watch_signals": [{"entity": "AAPL", "observable": "options flow"}]}
            ],
        },
    ]

    original = tracker.offer_focus
    tracker.offer_focus = _fake_offer
    try:
        await t._offer_open_questions(active)
    finally:
        tracker.offer_focus = original

    names = {e for e, _c, _b in offered}
    assert names == {"AAPL", "TSM"}, names
    assert all(b == "scenario_tracker" for _e, _c, b in offered)


@pytest.mark.anyio
async def test_a_signal_naming_nothing_offers_nothing():
    """A sentence with no entity is not a question anything can answer."""
    offered = []

    async def _fake_offer(redis_client, entity, conviction=1.0, offered_by=""):
        offered.append(entity)
        return True

    t = tracker.ScenarioTracker.__new__(tracker.ScenarioTracker)
    t._redis = object()

    active = [{
        "scenario_id": "s",
        "hypotheses": [{"watch_signals": ["Significant price movements on exchanges"]}],
    }]

    original = tracker.offer_focus
    tracker.offer_focus = _fake_offer
    try:
        await t._offer_open_questions(active)
    finally:
        tracker.offer_focus = original

    assert offered == []


@pytest.mark.anyio
async def test_no_redis_is_not_an_error():
    t = tracker.ScenarioTracker.__new__(tracker.ScenarioTracker)
    t._redis = None
    await t._offer_open_questions([{"scenario_id": "s", "hypotheses": []}])
