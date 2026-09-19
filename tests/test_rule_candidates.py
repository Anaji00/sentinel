"""Closing the rule-synthesis loop with co-occurrences no rule covers.

The synthesizer received ~150 correlations an hour and dropped every one. A
correlation carries a `rule_id` -- it is a rule *firing* -- so synthesising from
one re-derives the rule that produced it, and two generic rules account for 60%
of the volume. The loop would have manufactured rules out of its own echo.

What it needed was the inverse: event types that keep occurring together and
that no rule connects. Nothing emitted those. These tests pin the thing that
now does.
"""

import pytest

from shared.utils.cooccurrence import (
    MIN_PAIR_COUNT,
    NEAR_MISS_MIN_ANOMALY,
    PAIR_KEY,
    PROPOSED_KEY,
    RECENT_KEY,
    covered_type_pairs,
    mark_proposed,
    pair_key,
    record_notable_event,
    rule_candidates,
)


class FakeRedis:
    """Enough of Redis to exercise the real code paths, and no more."""

    def __init__(self):
        self.z = {}      # key -> {member: score}
        self.sets = {}   # key -> set
        self.raw = self

    # ── sorted sets ──────────────────────────────────────────────────────
    async def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    async def zincrby(self, key, amount, member):
        d = self.z.setdefault(key, {})
        d[member] = d.get(member, 0) + amount

    async def zrange(self, key, start, stop):
        items = sorted(self.z.get(key, {}).items(), key=lambda kv: kv[1])
        return [m for m, _ in items[start: (None if stop == -1 else stop + 1)]]

    async def zrevrange(self, key, start, stop, withscores=False):
        items = sorted(self.z.get(key, {}).items(), key=lambda kv: -kv[1])
        sliced = items[start: (None if stop == -1 else stop + 1)]
        return sliced if withscores else [m for m, _ in sliced]

    async def zscore(self, key, member):
        return self.z.get(key, {}).get(member)

    async def zremrangebyscore(self, key, lo, hi):
        d = self.z.get(key, {})
        hi = float("inf") if hi == "+inf" else float(hi)
        lo = float("-inf") if lo == "-inf" else float(lo)
        for m in [m for m, s in d.items() if lo <= s <= hi]:
            del d[m]

    # ── sets ─────────────────────────────────────────────────────────────
    async def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def expire(self, *_a, **_k):
        return True

    # ── pipeline ─────────────────────────────────────────────────────────
    #
    # redis-py queues pipeline commands *synchronously* and runs them on
    # `await execute()`. A double that returns coroutines from the queuing
    # calls never runs them, and the first version of this fake did exactly
    # that -- so the tests failed while the production code was correct.
    def pipeline(self):
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._queued = []

    def _queue(self, name, *args, **kwargs):
        self._queued.append((name, args, kwargs))
        return self

    def zadd(self, *a, **k):
        return self._queue("zadd", *a, **k)

    def zincrby(self, *a, **k):
        return self._queue("zincrby", *a, **k)

    def sadd(self, *a, **k):
        return self._queue("sadd", *a, **k)

    def expire(self, *a, **k):
        return self._queue("expire", *a, **k)

    async def execute(self):
        for name, args, kwargs in self._queued:
            await getattr(self._redis, name)(*args, **kwargs)
        self._queued.clear()
        return []


def seed_pair(r, a, b, pair_count, count_a=100.0, count_b=100.0,
              background=5000.0):
    """A pair plus the type frequencies lift needs to judge it.

    Lift is undefined without both sides' frequencies, and an unmeasurable pair
    is skipped rather than ranked against measured ones -- so a test that seeds
    only the pair is testing the skip, not the ranking.

    `background` exists because lift divides by the total event count. A fixture
    holding only the two types under test makes that total tiny and depresses
    every lift toward 1, which is an artefact of the fixture rather than of the
    data. The platform ingests ~65,000 events an hour across a few dozen types;
    this keeps the denominator in that shape.
    """
    from shared.utils.cooccurrence import TYPE_KEY

    r.z.setdefault(TYPE_KEY, {}).update({a: count_a, b: count_b})
    r.z[TYPE_KEY].setdefault("background|other", background)
    r.z.setdefault(PAIR_KEY, {})[pair_key(a, b)] = float(pair_count)
    return pair_key(a, b)


@pytest.mark.asyncio
async def test_a_quiet_event_is_not_recorded():
    """Recording the whole firehose teaches that telemetry is telemetry.

    One hour of live traffic: 64,750 events, 1,035 at or above the gate. The
    other 63,715 would be ~47 Redis operations a second spent learning that
    crypto transfers co-occur with crypto transfers.
    """
    r = FakeRedis()
    written = await record_notable_event(r, "crypto_transfer", "crypto", 0.1)
    assert written == 0
    assert not r.z.get(PAIR_KEY)


@pytest.mark.asyncio
async def test_two_notable_events_in_the_window_become_a_pair():
    r = FakeRedis()
    await record_notable_event(r, "vessel_dark", "maritime", 0.9, now=1000.0)
    await record_notable_event(r, "options_flow", "tradfi", 0.9, now=1010.0)

    key = pair_key("vessel_dark|maritime", "options_flow|tradfi")
    assert r.z[PAIR_KEY][key] == 1


@pytest.mark.asyncio
async def test_an_event_does_not_pair_with_its_own_type():
    """A type recurring is a burst, not a pattern.

    The correlation engine already has recurrence handling for that shape, and
    a self-pair would dominate every ranking -- the highest-volume type would
    always be the top candidate.
    """
    r = FakeRedis()
    await record_notable_event(r, "market_anomaly", "tradfi", 0.9, now=1000.0)
    await record_notable_event(r, "market_anomaly", "tradfi", 0.9, now=1010.0)
    assert not r.z.get(PAIR_KEY)


@pytest.mark.asyncio
async def test_events_outside_the_window_do_not_pair():
    """`Together` has to mean something, and it means the correlation window."""
    r = FakeRedis()
    await record_notable_event(r, "vessel_dark", "maritime", 0.9, now=1000.0)
    await record_notable_event(r, "options_flow", "tradfi", 0.9, now=1000.0 + 5000)
    assert not r.z.get(PAIR_KEY)


def test_coverage_is_read_from_the_rule_definitions():
    """An unfired rule still covers its pattern.

    Coverage deliberately does not depend on firing: proposing a duplicate of a
    rule that exists but has never matched would be noise, and the reason it
    has not matched is a separate question from whether it exists.
    """
    rules = [{
        "rule_id": "r1",
        "trigger_event_type": "vessel_dark",
        "correlations": [{"event_types": ["options_flow", "market_anomaly"]}],
    }]
    covered = covered_type_pairs(rules)
    assert ("options_flow", "vessel_dark") in covered
    assert ("market_anomaly", "vessel_dark") in covered
    # Two pieces of evidence for one trigger do not cover each other.
    assert ("market_anomaly", "options_flow") not in covered


def test_a_list_trigger_covers_every_type_it_names():
    rules = [{
        "rule_id": "r2",
        "trigger_event_type": ["headline", "filing"],
        "correlations": [{"event_types": ["equity_block"]}],
    }]
    covered = covered_type_pairs(rules)
    assert ("equity_block", "headline") in covered
    assert ("equity_block", "filing") in covered


@pytest.mark.asyncio
async def test_a_covered_pair_is_never_proposed():
    r = FakeRedis()
    key = seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi",
                    MIN_PAIR_COUNT + 50)

    rules = [{
        "rule_id": "r1",
        "trigger_event_type": "vessel_dark",
        "correlations": [{"event_types": ["options_flow"]}],
    }]
    assert await rule_candidates(r, rules) == []
    # And with no rule covering it, the same pair is a candidate.
    assert await rule_candidates(r, []) != []


@pytest.mark.asyncio
async def test_a_pair_must_recur_before_it_is_worth_an_inference():
    """One co-occurrence is a coincidence.

    The platform affords roughly thirty-five inferences an hour across every
    agent, so the bar for spending one on "should this be a rule" sits well
    above noise.
    """
    r = FakeRedis()
    seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi", MIN_PAIR_COUNT - 1)
    assert await rule_candidates(r, []) == []

    seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi", MIN_PAIR_COUNT)
    assert len(await rule_candidates(r, [])) == 1


@pytest.mark.asyncio
async def test_a_proposed_pattern_is_not_proposed_again():
    """The patterns that clear the threshold are the ones that keep happening.

    Without this the loop spends every inference it has re-proposing its own
    favourite pattern, for as long as that pattern continues.
    """
    r = FakeRedis()
    key = seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi",
                    MIN_PAIR_COUNT + 100)

    first = await rule_candidates(r, [])
    assert len(first) == 1

    await mark_proposed(r, [c["pair_key"] for c in first])
    assert key in r.sets[PROPOSED_KEY]
    assert await rule_candidates(r, []) == []


@pytest.mark.asyncio
async def test_cross_domain_candidates_rank_above_single_domain_ones():
    """Joining two domains is the thing a single-domain rule set cannot learn."""
    r = FakeRedis()
    # The single-domain pair has 16x the raw count. Lift says otherwise:
    # 2.28 against 2.73, because one side of it is a busy type.
    same = seed_pair(r, "market_anomaly|tradfi", "options_flow|tradfi",
                     1000, count_a=2000.0, count_b=2000.0)
    cross = seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi",
                      60, count_a=100.0, count_b=2000.0)

    candidates = await rule_candidates(r, [], limit=2)
    assert candidates[0]["pair_key"] == cross, (
        "a cross-domain pattern should outrank a busier single-domain one"
    )
    assert candidates[0]["cross_domain"] is True
    assert candidates[1]["cross_domain"] is False


@pytest.mark.asyncio
async def test_a_candidate_carries_what_the_prompt_needs_to_judge_it():
    """A pattern with no counts is an assertion, not evidence."""
    r = FakeRedis()
    seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi", 77)

    candidate = (await rule_candidates(r, []))[0]
    assert candidate["times_seen"] == 77
    assert candidate["window_sec"] > 0
    assert candidate["min_anomaly"] == NEAR_MISS_MIN_ANOMALY
    assert {candidate["event_type_a"], candidate["event_type_b"]} == {
        "vessel_dark", "options_flow"
    }
    assert {candidate["domain_a"], candidate["domain_b"]} == {"maritime", "tradfi"}


@pytest.mark.asyncio
async def test_a_redis_failure_costs_nothing_but_the_co_occurrence():
    """This is learned structure, not the finding.

    A failure here must not take down the correlation that was being computed,
    and must be counted rather than swallowed.
    """
    class Broken:
        raw = None

        def __getattr__(self, _name):
            raise RuntimeError("redis down")

    from shared.utils.quiet_failures import reset, snapshot

    reset()
    assert await record_notable_event(Broken(), "vessel_dark", "maritime", 0.9) == 0
    assert any("cooccurrence" in site for site in snapshot())


@pytest.mark.asyncio
async def test_no_redis_at_all_is_not_an_error():
    assert await record_notable_event(None, "vessel_dark", "maritime", 0.9) == 0
    assert await rule_candidates(None, []) == []
    await mark_proposed(None, ["x"])  # must not raise


def test_the_synthesizer_routes_a_rule_candidate():
    """The branch that makes this agent able to learn.

    Scanned over source rather than executed, because handle() needs a live
    agent. Comment lines are stripped, so this cannot pass by matching the
    explanation beside the branch.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "services" / "agents" / "rule_agent.py"
    body = "\n".join(
        ln for ln in src.read_text(encoding="utf-8").splitlines()
        if not ln.lstrip().startswith("#")
    )
    assert 'message.get("type") == "rule_candidate"' in body, (
        "the synthesizer has no branch for the one input it exists to act on"
    )


def test_the_synthesizer_subscribes_to_the_candidate_topic():
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "services" / "agents" / "main.py"
    body = "\n".join(
        ln for ln in src.read_text(encoding="utf-8").splitlines()
        if not ln.lstrip().startswith("#")
    )
    block = body[body.index("rule_synthesizer_agent = build_agent"):][:900]
    assert "Topics.RULE_CANDIDATES" in block


def test_the_correlation_service_records_and_proposes():
    """Both halves, in the service that has the events.

    A recorder with no proposer accumulates a Redis key nobody reads -- which is
    exactly what `_record_cooccurrence` had been doing since it was written.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "services" / "correlation" / "main.py"
    body = "\n".join(
        ln for ln in src.read_text(encoding="utf-8").splitlines()
        if not ln.lstrip().startswith("#")
    )
    assert "record_notable_event(" in body, "nothing records co-occurrences"
    assert "rule_candidates(" in body, "nothing proposes them"
    assert "Topics.RULE_CANDIDATES" in body, "candidates reach no topic"
    assert "mark_proposed(" in body, "the same pattern would be proposed forever"


# ── lift, not volume ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_a_busy_type_does_not_lead_every_pairing_it_appears_in():
    """Raw count measures traffic, not association.

    Four minutes of live data made the point: crypto_transfer led the top three
    pairs at 117, 102 and 99, against 16 for the busiest pair that did not
    involve it. crypto_transfer is 83% of everything the platform ingests, so it
    co-occurs with everything -- and ranking by count would have made the same
    three candidates the top proposal forever.
    """
    from shared.utils.cooccurrence import TYPE_KEY

    r = FakeRedis()
    busy = "crypto_transfer|crypto"
    rare_a = "vessel_dark|maritime"
    rare_b = "options_flow|tradfi"

    # The busy type occurs 100x more than either rare one.
    r.z[TYPE_KEY] = {busy: 100000.0, rare_a: 1000.0, rare_b: 1000.0}
    r.z[PAIR_KEY] = {
        pair_key(busy, rare_a): 117.0,   # high count, explained by base rate
        pair_key(rare_a, rare_b): 40.0,  # lower count, genuinely associated
    }

    candidates = await rule_candidates(r, [], limit=5)
    assert candidates, "the associated pair should still be proposed"
    top = candidates[0]
    assert {top["event_type_a"], top["event_type_b"]} == {"vessel_dark", "options_flow"}, (
        f"ranked by volume rather than association: {candidates}"
    )


@pytest.mark.asyncio
async def test_an_independent_pair_is_not_a_candidate():
    """Two unrelated types of any volume sit at a lift of 1.0.

    The threshold is therefore a statement about association, not about how much
    traffic a pair happens to carry.
    """
    from shared.utils.cooccurrence import MIN_LIFT, TYPE_KEY

    r = FakeRedis()
    a, b = "crypto_transfer|crypto", "vessel_position|maritime"
    # Counts chosen so P(A,B) is exactly P(A)P(B): independent.
    r.z[TYPE_KEY] = {a: 500.0, b: 500.0}
    r.z[PAIR_KEY] = {pair_key(a, b): 250.0}

    assert MIN_LIFT > 1.0, "a threshold at or below 1.0 admits independence"
    assert await rule_candidates(r, []) == []


@pytest.mark.asyncio
async def test_a_candidate_reports_its_lift():
    """A pattern proposed without its strength is an assertion."""
    from shared.utils.cooccurrence import TYPE_KEY

    r = FakeRedis()
    seed_pair(r, "vessel_dark|maritime", "options_flow|tradfi", 80)

    candidate = (await rule_candidates(r, []))[0]
    assert candidate["lift"] > 1.0
    assert candidate["times_seen"] == 80


def test_the_counter_keys_are_versioned_together():
    """Lift is a ratio between two counters, so they must cover one window.

    TYPE_KEY was added after PAIR_KEY had been accumulating, and the first live
    read showed pair counts of 153 and 126 against type counts of 26 and 10 --
    lifts of 21 and 179 for pairs counted over a longer history than their own
    denominators. Any change to what is recorded has to restart all of it.
    """
    from shared.utils import cooccurrence as co

    keys = [co.RECENT_KEY, co.PAIR_KEY, co.PROPOSED_KEY, co.TYPE_KEY]
    versions = {k.split(":")[2] for k in keys}
    assert len(versions) == 1, f"keys carry different versions: {keys}"
    assert all(k.startswith("sentinel:nearmiss:") for k in keys)


@pytest.mark.asyncio
async def test_a_type_already_in_the_window_is_refreshed_not_recounted():
    """Pair counts and type counts must measure the same thing.

    Pairing on every arrival made them measure different things. Live:
    `vessel_sts` had occurred once and `crypto_transfer` thirty-four times, and
    their pair stood at 23 -- every crypto transfer arriving during that one
    vessel_sts's fifteen minutes of window residency counted again. The pair
    count was really "how often the busy type arrived", which is the base rate
    lift exists to divide out, smuggled into the numerator.
    """
    from shared.utils.cooccurrence import TYPE_KEY

    r = FakeRedis()
    await record_notable_event(r, "vessel_sts", "maritime", 0.9, now=1000.0)
    # A busy type arriving repeatedly inside one window.
    for i in range(10):
        await record_notable_event(r, "crypto_transfer", "crypto", 0.9,
                                   now=1010.0 + i)

    key = pair_key("vessel_sts|maritime", "crypto_transfer|crypto")
    assert r.z[PAIR_KEY][key] == 1, (
        "ten arrivals of one type inside a window are one entry, not ten"
    )
    assert r.z[TYPE_KEY]["crypto_transfer|crypto"] == 1
    assert r.z[TYPE_KEY]["vessel_sts|maritime"] == 1


@pytest.mark.asyncio
async def test_re_entering_after_the_window_counts_again():
    """A pattern that recurs across windows is exactly what should accumulate."""
    from shared.utils.cooccurrence import NEAR_MISS_WINDOW_SEC, TYPE_KEY

    r = FakeRedis()
    far = NEAR_MISS_WINDOW_SEC + 100
    await record_notable_event(r, "vessel_dark", "maritime", 0.9, now=1000.0)
    await record_notable_event(r, "options_flow", "tradfi", 0.9, now=1001.0)
    # A window later, both happen again.
    await record_notable_event(r, "vessel_dark", "maritime", 0.9, now=1000.0 + far)
    await record_notable_event(r, "options_flow", "tradfi", 0.9, now=1001.0 + far)

    key = pair_key("vessel_dark|maritime", "options_flow|tradfi")
    assert r.z[PAIR_KEY][key] == 2
    assert r.z[TYPE_KEY]["vessel_dark|maritime"] == 2


# ── the focus floor: 539 ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_a_quiet_domain_keeps_one_subject_past_the_ordinary_window():
    """The domains do not offer at the same rate, and the TTL assumed they did.

    Measured on the live focus set: maritime offered 98 times and present with
    4 subjects; tradfi offered 39 times and present with 0. A tradfi subject
    arrives roughly every eight hours and the window is forty-five minutes, so
    `stock_correlation_agent` -- whose domains are tradfi, market, equity and
    macro -- consulted an empty set about ninety per cent of the time.

    The quiet domains got the coordination least, which is backwards: they are
    where two agents landing on one subject by chance is least likely, and so
    where a focus set is worth most.
    """
    import time as _time

    from shared.utils import focus as F

    class _Redis:
        def __init__(self):
            self.z = {}
            self.h = {}
            self.raw = self

        async def zadd(self, key, mapping, xx=False, gt=False):
            d = self.z.setdefault(key, {})
            for m, score in mapping.items():
                if xx and m not in d:
                    continue
                if gt and m in d and score <= d[m]:
                    continue
                d[m] = score

        async def hset(self, key, field, value):
            self.h.setdefault(key, {})[field] = value

        async def hmget(self, key, fields):
            d = self.h.get(key, {})
            return [d.get(f) for f in fields]

        async def hdel(self, key, *fields):
            for f in fields:
                self.h.get(key, {}).pop(f, None)

        async def zrange(self, key, start, stop):
            items = sorted(self.z.get(key, {}).items(), key=lambda kv: kv[1])
            return [m for m, _ in items[start: (None if stop == -1 else stop + 1)]]

        async def zrem(self, key, *members):
            for m in members:
                self.z.get(key, {}).pop(m, None)

        async def zremrangebyscore(self, key, lo, hi):
            d = self.z.get(key, {})
            hi = float("inf") if hi == "+inf" else float(hi)
            lo = float("-inf") if lo == "-inf" else float(lo)
            for m in [m for m, s in d.items() if lo <= s <= hi]:
                del d[m]

        async def zremrangebyrank(self, *_a, **_k):
            return 0

        async def expire(self, *_a, **_k):
            return True

    r = _Redis()
    await F.offer_focus(r, "NVDA", conviction=0.9, offered_by="test", domain="tradfi")

    scored = r.z[F.FOCUS_KEY]["NVDA"]
    # Scored into the future relative to the ordinary window, so the next
    # eviction sweep does not remove it.
    assert scored > _time.time(), (
        "the newest subject in a domain should outlive the ordinary window"
    )
    assert F.FOCUS_FLOOR_TTL_SEC > F.FOCUS_TTL_SEC


def test_the_floor_is_one_subject_not_the_whole_domain():
    """The point is to give a quiet domain something, not to pin the swarm.

    `prioritise` only reorders, so a slightly old suggestion costs an ordering
    rather than a slot -- but four stale subjects per domain would be a stale
    list, which is what the short TTL exists to prevent.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "shared" / "utils" / "focus.py"
    body = "\n".join(
        ln for ln in src.read_text(encoding="utf-8").splitlines()
        if not ln.lstrip().startswith("#")
    )
    assert "newest = names[-1]" in body, "the floor should keep one, not a slice"
