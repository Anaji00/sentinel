"""Event-type pairs that keep happening together and that no rule connects.

WHY THIS EXISTS
---------------
The rule synthesizer subscribes to nine topics and receives, in practice, one:
`CORRELATIONS`, at about 150 an hour. It has no branch for them and drops all
of them -- `processed` and `unrouted_message` were the same number.

Adding a branch would be worse than the gap. A correlation carries a `rule_id`;
it *is* a rule firing, not an observed co-occurrence. Synthesising a rule from
one re-derives the rule that produced it, and two generic rules account for 60%
of the volume (SEMANTIC_001 at 163,198 firings in thirty days, HAWKES_EXCITATION
at 102,341). The loop would manufacture rules out of its own echo.

What a rule synthesizer actually needs is the opposite: things that co-occur and
that **no rule matched**. Those are the candidates. Nothing in the platform
emitted them, which is why the synthesizer had nothing real to eat.

WHAT IS RECORDED, AND WHAT IS NOT
---------------------------------
Not every event. Measured over one hour: 64,750 events, of which 3,419 scored
at or above 0.5 and 1,035 at or above 0.6. Recording pairs for the whole
firehose would be ~47 Redis operations a second to learn that crypto transfers
co-occur with crypto transfers.

The gate is 0.6, which is also the quant engine's admission threshold, so
"notable enough to record a co-occurrence" and "notable enough to reason about"
are the same line rather than two numbers that drift apart.

The window is the correlation engine's own 15 minutes. Using a different one
would mean this module's "co-occurred" and the correlation engine's
"co-occurred" were different claims wearing one word.

COVERAGE IS COMPUTED, NOT ASSUMED
---------------------------------
A pair is *covered* when some rule already names one type as its trigger and the
other in a correlation clause. That is read out of the live rule definitions, so
a rule the synthesizer writes today stops its own pattern being proposed again
tomorrow. Nothing here depends on whether a rule has ever fired: an unfired rule
still covers its pair, and proposing a duplicate of it would be noise.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("sentinel.cooccurrence")

# Events below this are not worth remembering as a co-occurrence.
#
# 0.6 because that is the quant engine's admission gate. One hour of live
# traffic: 64,750 events, 1,035 at or above this. A threshold of 0 would record
# 47 pairs a second and mostly learn that routine telemetry is routine.
NEAR_MISS_MIN_ANOMALY = float(os.getenv("NEAR_MISS_MIN_ANOMALY", "0.6"))

# What "together" means. The correlation engine's window, deliberately.
NEAR_MISS_WINDOW_SEC = int(os.getenv("NEAR_MISS_WINDOW_SEC", "900"))

# Versioned together, because lift is a ratio between them.
#
# `TYPE_KEY` was added after `PAIR_KEY` had already been accumulating, and the
# result was immediate nonsense: pair counts of 153 and 126 against type counts
# of 26 and 10, giving lifts of 21 and 179 for pairs that had simply been
# counted over a longer history than their own denominators.
#
# Any change to what is recorded has to start a fresh window for all of it, or
# the ratio compares two different periods. The same rule the SEC registry cache
# needed: a derived structure is only as valid as the agreement between its
# parts.
#
# v3 because the counting rule changed again -- from "per arrival" to "per
# window entry" -- and counts accumulated under the old rule are not comparable
# with ones under the new.
_V = "v3"

# Recently-seen notable events, as a sorted set scored by timestamp.
RECENT_KEY = f"sentinel:nearmiss:{_V}:recent"

# How often each uncovered pair has been seen.
PAIR_KEY = f"sentinel:nearmiss:{_V}:pairs"

# Pairs already proposed, so one pattern is not proposed every cycle.
PROPOSED_KEY = f"sentinel:nearmiss:{_V}:proposed"

# How often each type has been seen at all, which is what turns a raw
# co-occurrence count into an association.
TYPE_KEY = f"sentinel:nearmiss:{_V}:types"

# How much more often a pair occurs than two independent types of its
# frequencies would.
#
# Ranking by raw count was wrong and running it showed why within four minutes:
#
#     117  crypto_transfer + vessel_position
#     102  crypto_transfer + flight_anomaly
#      99  crypto_transfer + vessel_sts
#      16  flight_anomaly  + vessel_position
#
# `crypto_transfer` is 83% of everything the platform ingests, so it leads every
# pairing it appears in -- not because it is related to vessel positions, but
# because it is always happening. That is measuring a base rate and reporting it
# as a relationship, and it would have made the same three candidates the top
# proposal forever.
#
# Lift divides that out: P(A,B) / (P(A)P(B)). A pair of independent types sits
# at 1.0 whatever their volumes, so the threshold is a statement about
# association rather than about traffic.
MIN_LIFT = float(os.getenv("NEAR_MISS_MIN_LIFT", "1.5"))

# Long enough that a daily pattern accumulates, short enough that a pattern
# which stops happening stops being proposed.
PAIR_TTL_SEC = int(os.getenv("NEAR_MISS_PAIR_TTL_SEC", str(7 * 24 * 3600)))

# How long a proposed pattern stays proposed.
PROPOSED_TTL_SEC = int(os.getenv("NEAR_MISS_PROPOSED_TTL_SEC", str(7 * 24 * 3600)))

# How many times a pair must recur before it is worth an inference.
#
# One co-occurrence is a coincidence. The platform affords roughly thirty-five
# inferences an hour across every agent, so the bar for spending one on "should
# this be a rule" has to sit well above noise.
MIN_PAIR_COUNT = int(os.getenv("NEAR_MISS_MIN_PAIR_COUNT", "25"))

# Cap on how many distinct recent events are paired against.
#
# Bounded work per event: the window holds one entry per (type, domain), not one
# per event, so this is the number of distinct kinds of thing happening at once.
MAX_WINDOW_MEMBERS = 40


def _member(event_type: str, domain: str) -> str:
    return f"{event_type}|{domain}"


def _split(member: Any) -> Tuple[str, str]:
    text = member.decode("utf-8") if isinstance(member, bytes) else str(member)
    event_type, _, domain = text.partition("|")
    return event_type, domain


def pair_key(a: str, b: str) -> str:
    """One key per unordered pair, so (A,B) and (B,A) are the same pattern."""
    return "::".join(sorted((a, b)))


async def record_notable_event(
    redis: Any,
    event_type: str,
    domain: str,
    anomaly_score: float,
    now: Optional[float] = None,
) -> int:
    """Remember this event and count what it co-occurred with.

    Returns the number of pairs incremented, so a caller can report coverage
    rather than assume it. Never raises: this is learned structure, and losing a
    co-occurrence must not cost the correlation that was being computed.
    """
    if redis is None or not event_type:
        return 0
    if float(anomaly_score or 0.0) < NEAR_MISS_MIN_ANOMALY:
        return 0

    stamp = float(now if now is not None else time.time())
    cutoff = stamp - NEAR_MISS_WINDOW_SEC
    me = _member(str(event_type), str(domain or "unknown"))

    try:
        raw = getattr(redis, "raw", redis)
        # Drop what has fallen out of the window before reading it, so the
        # window is a window rather than everything ever seen.
        await raw.zremrangebyscore(RECENT_KEY, "-inf", cutoff)

        # Only a type *entering* the window counts.
        #
        # Pairing on every arrival made the two counters measure different
        # things. Live: `vessel_sts` had occurred once and `crypto_transfer`
        # thirty-four times, and their pair stood at 23 -- because every crypto
        # transfer arriving during that single vessel_sts's fifteen minutes of
        # window residency counted again. The pair count was really "how often
        # the busy type arrived", which is the base rate the lift is supposed to
        # divide out, smuggled back in through the numerator.
        #
        # Counting entries makes pair and type counts commensurate: both are
        # "how many times this became true", so their ratio is an association.
        # It also collapses the write volume for exactly the types that
        # generated most of it.
        already_present = await raw.zscore(RECENT_KEY, me)
        if already_present is not None:
            # Still here: refresh its place and record nothing.
            pipe = raw.pipeline()
            pipe.zadd(RECENT_KEY, {me: stamp})
            pipe.expire(RECENT_KEY, NEAR_MISS_WINDOW_SEC * 2)
            await pipe.execute()
            return 0

        members = await raw.zrange(RECENT_KEY, 0, MAX_WINDOW_MEMBERS - 1)

        pipe = raw.pipeline()
        written = 0
        for other in members or []:
            other_type, _other_domain = _split(other)
            if not other_type or other_type == str(event_type):
                # A type co-occurring with itself is not a pattern, it is a
                # burst. The correlation engine's recurrence handling already
                # covers that shape.
                continue
            pipe.zincrby(PAIR_KEY, 1, pair_key(me, _member(other_type, _other_domain)))
            written += 1

        # The type's own frequency, counted the same way the pairs are: once
        # per entry into the window. Counting events here and entries above
        # would put the two sides of the ratio on different scales.
        pipe.zincrby(TYPE_KEY, 1, me)
        pipe.expire(TYPE_KEY, PAIR_TTL_SEC)

        # Scored by time, so the same kind of event arriving again refreshes its
        # place in the window instead of adding a second entry.
        pipe.zadd(RECENT_KEY, {me: stamp})
        pipe.expire(RECENT_KEY, NEAR_MISS_WINDOW_SEC * 2)
        pipe.expire(PAIR_KEY, PAIR_TTL_SEC)
        await pipe.execute()
        return written
    except Exception as exc:
        swallowed("shared.cooccurrence.record", exc, logger, detail=str(event_type))
        return 0


def _types_of(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.lower()]
    return [str(v).lower() for v in value if v]


def covered_type_pairs(rules: Iterable[dict]) -> Set[Tuple[str, str]]:
    """Every (trigger, evidence) type pair some rule already connects.

    Read from the rule definitions rather than from what has fired, because an
    unfired rule still covers its pattern and proposing a duplicate of it would
    be noise. Pairs are sorted, so a rule is found however the candidate
    happens to be ordered.
    """
    covered: Set[Tuple[str, str]] = set()
    for rule in rules or []:
        if not isinstance(rule, dict):
            continue
        triggers = _types_of(rule.get("trigger_event_type"))
        evidence: List[str] = []
        for clause in rule.get("correlations") or []:
            if isinstance(clause, dict):
                evidence.extend(_types_of(clause.get("event_types")))
        for t in triggers:
            for e in evidence:
                if t and e and t != e:
                    covered.add(tuple(sorted((t, e))))
    return covered


async def rule_candidates(
    redis: Any,
    rules: Iterable[dict],
    min_count: int = MIN_PAIR_COUNT,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Frequent co-occurring type pairs that no rule connects.

    Ordered by how often the pair has been seen. Cross-domain pairs first among
    equals: joining two domains is the platform's stated purpose and the thing
    a single-domain rule set cannot learn by itself.
    """
    if redis is None:
        return []

    covered = covered_type_pairs(rules)
    try:
        raw = getattr(redis, "raw", redis)
        rows = await raw.zrevrange(PAIR_KEY, 0, 200, withscores=True)
        already = await raw.smembers(PROPOSED_KEY)
        type_rows = await raw.zrevrange(TYPE_KEY, 0, -1, withscores=True)
    except Exception as exc:
        swallowed("shared.cooccurrence.candidates", exc, logger)
        return []

    proposed = {
        (m.decode("utf-8") if isinstance(m, bytes) else str(m)) for m in (already or [])
    }

    type_counts: Dict[str, float] = {}
    for member, score in type_rows or []:
        key = member.decode("utf-8") if isinstance(member, bytes) else str(member)
        type_counts[key] = float(score)
    total_events = sum(type_counts.values()) or 1.0

    out: List[Dict[str, Any]] = []
    for member, score in rows or []:
        if float(score) < min_count:
            # zrevrange is descending, so nothing after this clears the bar.
            break
        key = member.decode("utf-8") if isinstance(member, bytes) else str(member)
        if key in proposed:
            continue
        left, _, right = key.partition("::")
        type_a, domain_a = _split(left)
        type_b, domain_b = _split(right)
        if not type_a or not type_b:
            continue
        if tuple(sorted((type_a.lower(), type_b.lower()))) in covered:
            continue

        # How much more often this pair occurs than two independent types of
        # these frequencies would.
        #
        #     lift = P(B|A) / P(B) = pair * total / (count_a * count_b)
        #
        # Both sides in event units. The first version divided a pair count by
        # the sum of pair counts and a type count by the sum of type counts --
        # two different denominators -- and put a genuinely independent pair at
        # 4.0 instead of 1.0.
        #
        # Symmetric in a and b, so a pair has one lift rather than one per
        # reading direction.
        count_a = type_counts.get(left, 0.0)
        count_b = type_counts.get(right, 0.0)
        if count_a <= 0 or count_b <= 0:
            # No frequency for one side means lift is not defined. Proposing it
            # anyway would rank an unmeasurable pair against measured ones.
            continue
        lift = (float(score) * total_events) / (count_a * count_b)
        if lift < MIN_LIFT:
            # Frequent because both sides are frequent, which is a fact about
            # traffic and not about either of them.
            continue

        out.append({
            "pair_key": key,
            "event_type_a": type_a,
            "event_type_b": type_b,
            "domain_a": domain_a,
            "domain_b": domain_b,
            "cross_domain": bool(domain_a and domain_b and domain_a != domain_b),
            "times_seen": int(score),
            "lift": round(lift, 2),
            "window_sec": NEAR_MISS_WINDOW_SEC,
            "min_anomaly": NEAR_MISS_MIN_ANOMALY,
        })

    # Lift, not volume. Cross-domain first among comparable lifts, because
    # joining two domains is the thing a single-domain rule set cannot learn by
    # itself.
    out.sort(key=lambda c: (c["cross_domain"], c["lift"]), reverse=True)
    return out[:limit]


async def mark_proposed(redis: Any, pair_keys: Sequence[str]) -> None:
    """Remember that these patterns have been put to the synthesizer.

    Without this the same pattern is proposed every cycle for as long as it
    keeps happening -- and the ones that keep happening are exactly the ones
    that clear the threshold, so the loop would spend every inference it has on
    its own favourite pattern.
    """
    if redis is None or not pair_keys:
        return
    try:
        raw = getattr(redis, "raw", redis)
        pipe = raw.pipeline()
        for key in pair_keys:
            pipe.sadd(PROPOSED_KEY, key)
        pipe.expire(PROPOSED_KEY, PROPOSED_TTL_SEC)
        await pipe.execute()
    except Exception as exc:
        swallowed("shared.cooccurrence.mark_proposed", exc, logger)
