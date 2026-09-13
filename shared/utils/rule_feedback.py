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
            try:
                counts[rule_id] = counts.get(rule_id, 0) + int(float(value))
            except (TypeError, ValueError):
                continue
    return counts


async def rule_performance(
    redis: Any, rule_id: str, fired: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """Everything known about one rule's record, for a curator to read."""
    record = await feedback_for_rule(redis, rule_id)
    record["times_fired"] = (fired or {}).get(rule_id, 0)
    return record
