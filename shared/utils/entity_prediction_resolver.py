"""Grading a prediction about an entity rather than a price.

The wargamer predicts which entity a cascade reaches next. Measured on the
running deployment 2026-09-21:

    agent_name                |  n | resolved
    adversarial_wargamer      | 50 |        0
    quant_trading_engine      | 19 |       19
    macro_intelligence_engine |  5 |        5

Production works and attribution works; grading never did. The agents' own
resolution loop scores a directional price call -- it needs a ticker, an entry
price and a horizon, and a next-target prediction carries none of them, so
those fifty rows were never offered to it and no deadline existed at which they
could be judged.

The claim is checkable, just not by that loop. "The next entity affected will
be RAGNAR" is true if an event names RAGNAR inside the horizon and false if
none does. Both halves matter: a prediction that cannot be wrong is not a
prediction, and this file exists so the wargamer can be wrong.

Scored with the Brier score, the same measure the directional path uses, so the
two are comparable on one scorecard: (confidence - outcome)^2, lower is better.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sentinel.entity_predictions")

# How long a next-target claim has to come true.
#
# The wargamer reasons about cascades over days, not minutes, and a horizon
# shorter than the thing being predicted grades the clock rather than the
# claim. Twenty-four hours is long enough for a maritime or aviation cascade to
# reach a second entity and short enough that the scorecard moves.
DEFAULT_HORIZON_HOURS = 24

# Rows graded per sweep. Bounded so a backlog is worked through steadily
# rather than in one transaction that holds the table.
DEFAULT_BATCH = 200

# Only rows this old are graded: a prediction whose horizon has not elapsed is
# not wrong, it is pending, and marking it either way would be inventing.
_DUE = """
    SELECT id, predicted_target, confidence, occurred_at, agent_name
      FROM agent_predictions
     WHERE resolved_at IS NULL
       AND predicted_target IS NOT NULL
       AND predicted_target <> ''
       AND predicted_target <> 'unknown'
       AND ticker IS NULL
       AND occurred_at < NOW() - ($1 || ' hours')::INTERVAL
     ORDER BY occurred_at
     LIMIT $2
"""

# Did anything name this entity after the prediction and inside the horizon?
#
# Matched on id and name because the two halves of the platform key on
# different ones: the wargamer predicts whatever `entity_ids` carried, which is
# an MMSI for a vessel and a callsign for a flight.
_HIT = """
    SELECT 1
      FROM events
     WHERE occurred_at > $2
       AND occurred_at <= $2 + ($3 || ' hours')::INTERVAL
       AND (upper(primary_entity_id) = upper($1) OR upper(primary_entity_name) = upper($1))
     LIMIT 1
"""

_RESOLVE = """
    UPDATE agent_predictions
       SET resolved_at = NOW(), outcome_correct = $2, brier_score = $3
     WHERE id = $1
"""


def _as_datetime(value: Any) -> Optional[datetime]:
    """A timestamptz parameter asyncpg will accept.

    This client hands back `occurred_at` as an ISO string, and asyncpg refuses
    a string for a timestamptz argument:

        invalid input for query argument $2: '2026-09-02T15:21:17.080732+00:00'
        (expected a datetime.date or datetime.datetime instance, got 'str')

    Every one of the first thirty rows failed on it. They were left unresolved
    rather than marked wrong, which is the one part of that sweep that behaved
    -- a grading failure is not evidence against a forecast. Same coercion as
    the edge validator's, for the same reason.
    """
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def brier(confidence: Optional[float], hit: bool) -> float:
    """(confidence - outcome)^2, with an unstated confidence treated as 0.5.

    0.5 rather than 0.0: a prediction that stated no confidence is not a
    confident claim of nothing, it is an unquantified one, and scoring it as
    certain-and-wrong would punish a missing field as though it were a bad
    forecast.
    """
    try:
        p = float(confidence)
    except (TypeError, ValueError):
        p = 0.5
    if p != p or p < 0.0 or p > 1.0:          # NaN or out of range
        p = 0.5
    return round((p - (1.0 if hit else 0.0)) ** 2, 6)


async def resolve_entity_predictions(
    db,
    horizon_hours: int = DEFAULT_HORIZON_HOURS,
    batch: int = DEFAULT_BATCH,
) -> Dict[str, int]:
    """Grade every next-target prediction whose horizon has elapsed.

    Returns counts rather than logging a total nobody reads: {"graded", "hit",
    "missed"}. A sweep that grades nothing returns zeros, which is a different
    statement from a sweep that did not run.
    """
    if db is None:
        return {"graded": 0, "hit": 0, "missed": 0}

    try:
        due: List[Dict[str, Any]] = await db.query(_DUE, str(horizon_hours), int(batch))
    except Exception as e:
        logger.warning("Entity prediction sweep could not read due rows: %s", e)
        return {"graded": 0, "hit": 0, "missed": 0}

    graded = hit_count = 0
    for row in due or []:
        target = str(row.get("predicted_target") or "").strip()
        # The SQL excludes these too. Both, deliberately: the placeholder the
        # telemetry worker writes when a message names no target is the literal
        # "unknown", and grading it would mark a prediction that was never made
        # as one the wargamer got wrong. A function whose correctness depends
        # on its caller's WHERE clause is one refactor from silently scoring
        # placeholders.
        if not target or target.lower() in ("unknown", "none", "null"):
            continue
        made_at = _as_datetime(row.get("occurred_at"))
        if made_at is None:
            # No usable timestamp means no window to check, which is not the
            # same as a missed prediction.
            logger.warning(
                "Prediction %s has an unreadable occurred_at; left unresolved.",
                row.get("id"),
            )
            continue
        try:
            hits = await db.query(_HIT, target, made_at, str(horizon_hours))
            hit = bool(hits)
            await db.execute(
                _RESOLVE, row.get("id"), hit, brier(row.get("confidence"), hit)
            )
        except Exception as e:
            # Left unresolved rather than marked wrong. A grading failure is
            # not evidence against the forecast.
            logger.warning("Could not grade prediction %s: %s", row.get("id"), e)
            continue
        graded += 1
        hit_count += int(hit)

    if graded:
        logger.info(
            "Graded %s entity prediction(s): %s hit, %s missed, horizon %sh.",
            graded, hit_count, graded - hit_count, horizon_hours,
        )
    return {"graded": graded, "hit": hit_count, "missed": graded - hit_count}
