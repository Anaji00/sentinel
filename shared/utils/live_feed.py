"""
shared/utils/live_feed.py

What is worth putting in front of a person.

The enrichment layer already decides this and says so. A transfer below the
notional floor is tagged `baseline_data` and scored 0.000 — recorded because the
history is worth having, not because anyone needs to see it. Both live-feed
publishers ignored that entirely and broadcast every enriched event, so a $6
USDC transfer arrived in the feed rendered identically to a $16.44M whale
movement:

    EVENT HEADLINE:  Transfer: $6 USDC
    Anomaly score: 0.00. Provenance tags: crypto, transfer, baseline_data

Nothing was wrong with the scoring. The label was correct, the score was
correct, and the consumer read neither.

The events still reach Kafka and the database, so nothing is lost to anyone
querying. What changes is the push feed, which is a person's attention and the
scarcest thing this system spends.
"""

import logging
from typing import Optional

logger = logging.getLogger("shared.live_feed")

# Tags the enricher applies to say "kept, not notable".
BASELINE_TAGS = frozenset({"baseline_data", "routine_telemetry", "infrastructure_flow"})

# Score below which an event is noise regardless of tags.
#
# Deliberately low. This is a floor against events the enricher scored at or
# near zero, not a second opinion on its ranking -- a detector that says 0.02
# has already said this is nothing.
LIVE_FEED_MIN_SCORE = 0.05


def worth_broadcasting(event: dict) -> bool:
    """Whether an enriched event belongs in the live feed.

    Reads the enricher's own verdict rather than forming a new one. Anything
    kept as baseline is excluded whatever its score, and anything the scorer
    put on the floor is excluded whatever its tags.
    """
    if not isinstance(event, dict):
        return False

    tags = event.get("tags") or []
    if isinstance(tags, (list, tuple, set)):
        if any(str(t).strip().lower() in BASELINE_TAGS for t in tags):
            return False

    try:
        score = float(event.get("anomaly_score") or 0.0)
    except (TypeError, ValueError):
        # An unscored event is not evidence of importance.
        return False

    return score >= LIVE_FEED_MIN_SCORE


def agent_narrative(result: dict) -> Optional[str]:
    """The human-readable part of an agent result, or None.

    The publisher fell back to `str(res_dict)[:200]`, so an agent whose result
    carried no prose had its internal bookkeeping presented as an executive
    summary:

        {'agent': 'supervisor', 'action': 'single_commit', 'entity_id': '8881EF'}

    That is a graph-write confirmation. Returning None instead lets the caller
    decline to publish, which is the honest outcome: an agent that produced no
    brief has no brief to show.
    """
    if not isinstance(result, dict):
        return None
    for field in ("summary", "rationale", "narrative", "brief", "assessment", "headline"):
        value = result.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
