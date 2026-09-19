"""What the platform knows about how a correlation rule is doing.

Two things exist and neither reached the one place that decides a rule's fate.

`/feedback` lets an analyst mark a fired rule useful or wrong. The counters are
kept per rule, aggregated by `/feedback/rules`, and flagged `needs_review` when
a rule has both enough feedback to judge and a negative share above the
threshold. Nothing read them: the key was written and read only inside the
gateway route that owns it, no component fetched the endpoint, and the counters
aged out on a TTL. An analyst could mark the same rule wrong every day for a
week and the platform's own rule curator would never hear about it.

`correlation_rule_fired:{rule_id}` is incremented by the correlation engine on
every firing and published into the metrics hash. The rule agent's prune pass
documents itself as evaluating rules "against current market context and hit
rates", and the summaries it builds carry a rule name, a trigger type and an
expiry -- no firing count of any kind.

This module is the one definition of both key conventions, so the writer and
the readers cannot drift apart the way `sentinel:watched:equities` did.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("shared.rule_feedback")

# Per-rule verdict counters, written by the gateway's /feedback route.
RULE_FEEDBACK_KEY = "sentinel:feedback:rule"

# The metric the correlation engine increments when a rule fires.
RULE_FIRED_METRIC_PREFIX = "correlation_rule_fired:"

# Enough feedback to judge, and a negative share above which a rule is worth
# a second look. Defined here rather than in the route so the curator and the
# endpoint agree about what "needs review" means.
MIN_FEEDBACK_FOR_REVIEW = 5
NEGATIVE_SHARE_FOR_REVIEW = 0.6

# How long a rule's record survives without new verdicts.
RULE_FEEDBACK_TTL_SEC = 30 * 24 * 3600

# How long a rule's record survives without new verdicts.
RULE_FEEDBACK_TTL_SEC = 30 * 24 * 3600

# When a rule is bad enough to delete, as opposed to bad enough to look at.
#
# needs_review above is the bar for spending an inference on a prune pass. It is
# the wrong bar for deletion, because this platform's own base rate is worse
# than it: 536 of 818 resolved scenarios are denied, 65.5%, across every rule
# there is. A rule sitting at a 0.6 negative share is beating the stream it
# belongs to.
#
# Set above the base rate with enough verdicts to mean something. At 0.85 and
# eight verdicts, rule_financial_block_volume_spike -- 37.3% confirmed over 77
# scenarios, the second-best financial rule here -- survives, and a rule that is
# denied seven times out of eight does not.
MIN_VERDICTS_FOR_DEPRECATION = 8
NEGATIVE_SHARE_FOR_DEPRECATION = 0.85


def should_deprecate(total: int, negative: int) -> bool:
    """Whether a rule's record is bad enough to remove it outright."""
    if total < MIN_VERDICTS_FOR_DEPRECATION:
        return False
    return (negative / total) >= NEGATIVE_SHARE_FOR_DEPRECATION


def _as_int(value: Any) -> int:
    try:
        return int(value.decode() if isinstance(value, bytes) else value)
    except (TypeError, ValueError, AttributeError):
        return 0


def needs_review(total: int, negative: int) -> bool:
    """Both conditions, not either.

    Three complaints out of four firings is a signal; three out of three
    hundred is one analyst's morning.
    """
    if total < MIN_FEEDBACK_FOR_REVIEW:
        return False
    return (negative / total) >= NEGATIVE_SHARE_FOR_REVIEW if total else False


async def feedback_for_rule(redis: Any, rule_id: str) -> Dict[str, Any]:
    """Analyst verdicts recorded against one rule. Zeros when there are none.

    Zeros rather than None: a rule nobody has judged and a rule judged well are
    different, and the caller is told which by `total`.
    """
    out: Dict[str, Any] = {"total": 0, "negative": 0, "negative_share": 0.0,
                           "needs_review": False}
    if redis is None:
        return out
    try:
        raw = getattr(redis, "raw", redis)
        counts = await raw.hgetall(f"{RULE_FEEDBACK_KEY}:{rule_id}")
        if not counts:
            return out
        decoded = {
            (k.decode() if isinstance(k, bytes) else str(k)): _as_int(v)
            for k, v in counts.items()
        }
        total = decoded.get("total", 0)
        negative = decoded.get("negative", 0)
        out["total"] = total
        out["negative"] = negative
        out["negative_share"] = round((negative / total) if total else 0.0, 4)
        out["needs_review"] = needs_review(total, negative)
        return out
    except Exception as _exc:
        swallowed("shared.rule_feedback.feedback_for_rule", _exc, logger)
        return out


async def record_rule_verdict(
    redis: Any, rule_id: str, negative: bool, source: str = "auto"
) -> Dict[str, Any]:
    """Add one verdict to a rule's record and return the record.

    The gateway's /feedback route has always written these counters for analyst
    verdicts. The automated path -- a scenario the tracker resolved as denied --
    did not write them at all; it went straight to deprecating the rule, so a
    rule's entire record was the single verdict that killed it.

    Same keys as the route, because a counter written under two conventions is
    how this platform came to read `sentinel:watched:equities` under a name
    nothing wrote.
    """
    out = {"total": 0, "negative": 0, "negative_share": 0.0, "needs_review": False}
    if redis is None or not rule_id:
        return out
    try:
        raw = getattr(redis, "raw", redis)
        key = f"{RULE_FEEDBACK_KEY}:{rule_id}"
        pipe = raw.pipeline()
        pipe.hincrby(key, "total", 1)
        pipe.hincrby(key, "negative" if negative else "positive", 1)
        # Same TTL the route applies, so an automated verdict ages out the way
        # an analyst's does rather than accumulating forever.
        pipe.expire(key, RULE_FEEDBACK_TTL_SEC)
        await pipe.execute()
    except Exception as exc:
        swallowed("shared.rule_feedback.record_rule_verdict", exc, logger, detail=rule_id)
        return out
    return await feedback_for_rule(redis, rule_id)


async def record_rule_verdict(
    redis: Any, rule_id: str, negative: bool, source: str = "auto"
) -> Dict[str, Any]:
    """Add one verdict to a rule's record and return the record.

    The gateway's /feedback route has always written these counters for analyst
    verdicts. The automated path -- a scenario the tracker resolved as denied --
    did not write them at all; it went straight to deprecating the rule, so a
    rule's entire record was the single verdict that killed it.

    Same keys as the route, because a counter written under two conventions is
    how this platform came to read `sentinel:watched:equities` under a name
    nothing wrote.
    """
    out = {"total": 0, "negative": 0, "negative_share": 0.0, "needs_review": False}
    if redis is None or not rule_id:
        return out
    try:
        raw = getattr(redis, "raw", redis)
        key = f"{RULE_FEEDBACK_KEY}:{rule_id}"
        pipe = raw.pipeline()
        pipe.hincrby(key, "total", 1)
        pipe.hincrby(key, "negative" if negative else "positive", 1)
        # Same TTL the route applies, so an automated verdict ages out the way
        # an analyst's does rather than accumulating forever.
        pipe.expire(key, RULE_FEEDBACK_TTL_SEC)
        await pipe.execute()
    except Exception as exc:
        swallowed("shared.rule_feedback.record_rule_verdict", exc, logger, detail=rule_id)
        return out
    return await feedback_for_rule(redis, rule_id)


async def firing_counts(redis: Any) -> Dict[str, int]:
    """How many times each rule has fired, by rule id.

    Read through `collect_all` rather than by naming the publishing service, so
    a change to SENTINEL_SERVICE does not silently return an empty mapping.
    """
    if redis is None:
        return {}
    try:
        from shared.utils.metrics import collect_all

        everything = await collect_all(redis)
    except Exception as _exc:
        swallowed("shared.rule_feedback.firing_counts", _exc, logger)
        return {}

    counts: Dict[str, int] = {}
    for _service, metrics in (everything or {}).items():
        for name, value in (metrics or {}).items():
            if not name.startswith(RULE_FIRED_METRIC_PREFIX):
                continue
            rule_id = name[len(RULE_FIRED_METRIC_PREFIX):]
            # `collect_all` returns each counter twice: once summed across
            # services under the bare name, and once per service under a
            # labelled variant like `rule_x{service="correlation"}`. Summing
            # both would double every count, and keeping the labelled ones puts
            # a second entry per rule in a mapping whose keys are supposed to be
            # rule ids.
            if "{" in rule_id:
                continue
            try:
                counts[rule_id] = counts.get(rule_id, 0) + int(float(value))
            except (TypeError, ValueError):
                continue
    return counts


async def firing_counts_from_history(db: Any, days: int = 30) -> Dict[str, int]:
    """How many times each rule has fired, from the durable record.

    `firing_counts` reads `MetricsCollector`, whose counters live in a
    process-local defaultdict. They reset whenever the correlation service
    restarts -- which is every deploy -- so the number reaching the pruning
    curator is "firings since the last restart" wearing the name "times_fired".

    Measured: the correlation service had been up 53 seconds and reported 3
    firings for `rule_aviation_activity_surge` and 2 for
    `rule_maritime_chokepoint_evasion`. Over seven days the `correlations`
    table holds 1,606 and 1,816 for the same two rules, and the two
    highest-firing rules on the platform -- SEMANTIC_001 at 12,134 and
    HAWKES_EXCITATION at 7,303 -- did not appear in the metric at all.

    A curator deciding which rules to retire was being handed a number three
    orders of magnitude low, and blind to the rules that fire most.
    """
    if db is None:
        return {}
    try:
        rows = await db.query(
            """
            SELECT rule_id, count(*) AS times_fired
              FROM correlations
             WHERE detected_at > NOW() - make_interval(days => $1)
               AND rule_id IS NOT NULL
             GROUP BY rule_id
            """,
            int(days),
        )
    except Exception as _exc:
        swallowed("shared.rule_feedback.firing_counts_from_history", _exc, logger)
        return {}

    counts: Dict[str, int] = {}
    for row in rows or []:
        rule_id = row.get("rule_id") if isinstance(row, dict) else row["rule_id"]
        value = row.get("times_fired") if isinstance(row, dict) else row["times_fired"]
        if rule_id:
            counts[str(rule_id)] = int(value or 0)
    return counts


async def rule_performance(
    redis: Any, rule_id: str, fired: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """Everything known about one rule's record, for a curator to read."""
    record = await feedback_for_rule(redis, rule_id)
    record["times_fired"] = (fired or {}).get(rule_id, 0)
    return record


# How much of a rule's priority can rest on its conversion record.
#
# A floor rather than a gate. A rule that has never produced a scenario is not
# worthless -- it may be new, or it may fire on something rare and important --
# so the worst a bad record can do is scale a cluster's priority to this, never
# to zero. A rule with no history at all is scored as though it were average,
# because "unmeasured" and "measured and poor" are different claims.
CONVERSION_FLOOR = 0.45

# Firings before a rule's conversion rate is believed.
#
# Below this the rate is mostly an artefact of which few clusters happened to
# reach the reasoning tier, and the tier admits roughly thirty-six an hour.
MIN_FIRINGS_FOR_CONVERSION = 50

CONVERSION_KEY = "sentinel:feedback:rule:conversion"


async def conversion_rates(db: Any, days: int = 2) -> Dict[str, float]:
    """How often each rule's correlations became a scenario.

    The reasoning tier ranks clusters on tier, confidence, breadth and whether
    they cross domains -- all properties of the cluster in hand, and none of
    them a memory of what that rule has produced before. Measured over 48 hours:

        SEMANTIC_001                       7,354 fired    35 scenarios   0.48%
        HAWKES_EXCITATION                  5,786 fired     3 scenarios   0.05%
        rule_maritime_chokepoint_evasion   1,596 fired    84 scenarios   5.26%
        rule_aviation_activity_surge       1,144 fired    67 scenarios   5.86%

    Two rules are 77% of everything the tier is offered and convert worst; the
    hand-written domain rules are a small minority and convert a hundred times
    better. The scarcest resource on this platform was being allocated by
    arrival order into a queue dominated by its least productive supplier.

    This is deliberately not a kill switch. HAWKES fires across thousands of
    entities and confirms at a third when it does reach a scenario, so the
    answer is not to turn it off -- it is to stop it outranking a chokepoint
    evasion cluster purely by arriving first.
    """
    if db is None:
        return {}
    try:
        rows = await db.query(
            """
            SELECT c.rule_id AS rule_id,
                   count(*) AS fired,
                   count(DISTINCT s.scenario_id) AS scenarios
            FROM correlations c
            LEFT JOIN scenarios s ON s.correlation_id = c.correlation_id
            WHERE c.detected_at > NOW() - ($1 || ' days')::INTERVAL
            GROUP BY 1
            """,
            str(int(days)),
        )
    except Exception:
        return {}

    rates: Dict[str, float] = {}
    for row in rows or []:
        try:
            rule_id = str(row["rule_id"] or "").strip()
            fired = int(row["fired"] or 0)
            scenarios = int(row["scenarios"] or 0)
        except (TypeError, ValueError, KeyError):
            continue
        if not rule_id or fired < MIN_FIRINGS_FOR_CONVERSION:
            continue
        rates[rule_id] = scenarios / float(fired)
    return rates


def conversion_weight(rule_id: str, rates: Optional[Dict[str, float]]) -> float:
    """A bounded multiplier for a rule's priority, from its conversion record.

    Relative to the best performer in the same window rather than to an
    absolute rate, because the absolute numbers are small everywhere -- 5.9% is
    the *good* case here -- and a fixed scale would compress every rule onto the
    floor and rank nothing.

    Returns 1.0 when there is nothing to say: no map, no entry, or a rule below
    the firing threshold. An unmeasured rule competes on the cluster's own
    merits, exactly as it did before.
    """
    if not rates or not rule_id:
        return 1.0
    mine = rates.get(str(rule_id).strip())
    if mine is None:
        return 1.0
    best = max(rates.values()) if rates else 0.0
    if best <= 0.0:
        return 1.0
    share = max(0.0, min(1.0, mine / best))
    return CONVERSION_FLOOR + (1.0 - CONVERSION_FLOOR) * share
