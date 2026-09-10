"""Turning heuristic confidence scores into empirical probabilities.

A correlation is published with a `confidence_score` built from three
hand-chosen weights -- 0.45 on the trigger's own anomaly, 0.30 on how much
supporting evidence was gathered, 0.25 on whether it spans domains. Those
weights are reasonable and they are not probabilities. Nothing had ever
compared a published 0.8 against how often a cluster published at 0.8 turned
out to be worth acting on, so the number carried a precision it had not earned.

This closes that loop. Outcomes are recorded as they resolve, an isotonic fit
maps the heuristic score onto the observed rate, and the mapping is applied to
what gets published.

Isotonic rather than Platt scaling, for two reasons. It assumes only that the
ranking is right -- that a higher heuristic score really does mean a better
claim -- which is exactly what a weighted blend can be trusted to get right and
nothing more. And it is free to be non-linear, which matters because the
failure this is meant to catch is usually a bunching: many scores crowded near
0.8 that behave nothing like 80%.

Three properties it deliberately keeps:

  * Below MIN_CALIBRATION_SAMPLES it returns the raw score unchanged and says
    it is uncalibrated. A mapping fitted on nine outcomes is worse than none,
    because it looks like evidence.
  * It never inverts the ranking. Isotonic regression is monotone by
    construction, so a cluster the engine ranked higher is never published
    lower than one it ranked below.
  * The raw score is preserved alongside the calibrated one, so a reader can
    always see what the heuristic said before the correction.
"""
from __future__ import annotations

import json
import logging

from shared.utils.quiet_failures import swallowed
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("sentinel.calibration")

# Where resolved (score, outcome) pairs accumulate.
OUTCOMES_KEY = "sentinel:calibration:correlation_outcomes"

# Pairs retained. Enough for a stable fit, bounded so the list cannot grow
# without limit on a busy deployment.
MAX_OUTCOMES = int(os.getenv("CALIBRATION_MAX_OUTCOMES", "5000"))

# Below this the mapping is not fitted at all.
#
# Isotonic regression will happily interpolate a handful of points into a
# confident-looking curve. Two hundred resolved outcomes is a modest floor and
# still means every bucket rests on more than a couple of observations.
MIN_CALIBRATION_SAMPLES = int(os.getenv("CALIBRATION_MIN_SAMPLES", "200"))

# How long a fitted mapping is reused before it is refitted from the store.
REFIT_INTERVAL_SEC = float(os.getenv("CALIBRATION_REFIT_SEC", "900"))

_cached_model: Optional[Any] = None
_cached_at: float = 0.0
_cached_n: int = 0


async def record_outcome(redis_client, raw_confidence: float, was_correct: bool) -> None:
    """Store one resolved (published confidence, outcome) pair.

    Called when a correlation's downstream scenario resolves. Failures here are
    logged rather than raised: losing a calibration sample degrades the fit
    slowly and must not disturb the path that resolved the scenario.
    """
    if redis_client is None:
        return
    try:
        score = float(raw_confidence)
    except (TypeError, ValueError):
        return
    if not (0.0 <= score <= 1.0):
        return

    try:
        raw = getattr(redis_client, "raw", redis_client)
        pipe = raw.pipeline()
        pipe.lpush(OUTCOMES_KEY, json.dumps([round(score, 6), 1 if was_correct else 0]))
        pipe.ltrim(OUTCOMES_KEY, 0, MAX_OUTCOMES - 1)
        await pipe.execute()
    except Exception as e:
        logger.warning("Could not record a calibration outcome: %s", e)


# Scenarios already fed to the calibrator, so a sweep can run repeatedly
# without counting the same resolution twice.
BACKFILLED_KEY = "sentinel:calibration:backfilled_scenarios"


async def backfill_from_resolved(db, redis_client, limit: int = 5000) -> dict:
    """Feed the calibrator every resolved scenario it has not already seen.

    `record_outcome` fires when a scenario resolves, and only then -- so the
    fit depends on the tracker being up at the moment of each resolution, and a
    restart loses every sample that landed while it was down. Measured on this
    deployment: **562 resolved scenarios in the database and 0 calibration
    samples in Redis.** The loop was closed and starting from cold.

    Most of those 562 cannot help: 554 of them point at correlations whose
    `confidence_score` is null, because they were published before the engine
    recorded one. That is a fact about the history rather than something to
    repair -- there is no published confidence to calibrate against, and
    inventing one would be worse than the gap. Recent clusters all carry it, so
    the sweep becomes useful as those resolve.

    Deduplicated by scenario id: this is idempotent and safe to run on a
    schedule.
    """
    out = {"considered": 0, "recorded": 0, "already_seen": 0, "no_confidence": 0}
    if db is None or redis_client is None:
        return out
    try:
        rows = await db.query(
            """
            SELECT s.scenario_id,
                   s.status,
                   COALESCE(
                       (c.metrics_summary->>'raw_confidence')::float8,
                       c.confidence_score
                   ) AS raw_confidence
            FROM scenarios s
            JOIN correlations c ON s.correlation_id = c.correlation_id
            WHERE s.status IN ('confirmed', 'denied')
            ORDER BY s.updated_at DESC NULLS LAST
            LIMIT $1
            """,
            limit,
        )
    except Exception as e:
        logger.warning("Calibration backfill query failed: %s", e)
        return out

    raw = getattr(redis_client, "raw", redis_client)
    for r in rows or []:
        out["considered"] += 1
        sid = str(r.get("scenario_id") or "")
        conf = r.get("raw_confidence")
        if conf is None:
            out["no_confidence"] += 1
            continue
        try:
            if await raw.sismember(BACKFILLED_KEY, sid):
                out["already_seen"] += 1
                continue
        except Exception as e:
            swallowed("utils.confidence_calibration.backfill_dedup", e, logger, detail=sid)
            continue
        await record_outcome(redis_client, float(conf), str(r.get("status")) == "confirmed")
        try:
            await raw.sadd(BACKFILLED_KEY, sid)
        except Exception as e:
            swallowed("utils.confidence_calibration.backfill_mark", e, logger, detail=sid)
        out["recorded"] += 1

    if out["recorded"] or out["no_confidence"]:
        logger.info(
            "Calibration backfill: %s recorded, %s already seen, %s resolved "
            "scenarios whose correlation carried no confidence to calibrate "
            "against, of %s considered.",
            out["recorded"], out["already_seen"], out["no_confidence"], out["considered"],
        )
    return out


async def load_outcomes(redis_client) -> List[Tuple[float, int]]:
    """Every stored (score, outcome) pair."""
    if redis_client is None:
        return []
    try:
        raw = getattr(redis_client, "raw", redis_client)
        rows = await raw.lrange(OUTCOMES_KEY, 0, MAX_OUTCOMES - 1)
    except Exception as e:
        logger.warning("Could not read calibration outcomes: %s", e)
        return []

    pairs: List[Tuple[float, int]] = []
    for row in rows or []:
        try:
            item = json.loads(row if isinstance(row, str) else row.decode("utf-8"))
            pairs.append((float(item[0]), int(item[1])))
        except (ValueError, TypeError, IndexError, AttributeError):
            continue
    return pairs


def fit(pairs: Sequence[Tuple[float, int]]):
    """An isotonic mapping from heuristic score to observed rate, or None.

    None means "not enough evidence to correct anything", which the caller
    treats as "publish the raw score and say it is uncalibrated" rather than as
    an error.
    """
    if len(pairs) < MIN_CALIBRATION_SAMPLES:
        return None

    outcomes = {int(o) for _, o in pairs}
    if len(outcomes) < 2:
        # Everything resolved the same way. A mapping fitted here would encode
        # "always 1.0" or "always 0.0", which describes the sample rather than
        # the relationship.
        logger.info(
            "Calibration skipped: all %s outcomes are identical, so the mapping "
            "would describe the sample rather than the score.", len(pairs),
        )
        return None

    try:
        import numpy as np
        from sklearn.isotonic import IsotonicRegression
    except ImportError as e:  # pragma: no cover - dependency is declared
        logger.warning("Calibration unavailable (%s); publishing raw scores.", e)
        return None

    x = np.asarray([p[0] for p in pairs], dtype=float)
    y = np.asarray([p[1] for p in pairs], dtype=float)

    model = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
    model.fit(x, y)
    return model


async def get_model(redis_client, now: Optional[float] = None):
    """The current mapping, refitted at most every REFIT_INTERVAL_SEC."""
    global _cached_model, _cached_at, _cached_n
    import time as _time

    now = _time.monotonic() if now is None else now
    if _cached_model is not None and (now - _cached_at) < REFIT_INTERVAL_SEC:
        return _cached_model

    pairs = await load_outcomes(redis_client)
    model = fit(pairs)
    _cached_model, _cached_at, _cached_n = model, now, len(pairs)
    if model is not None:
        logger.info("Correlation confidence calibration refitted on %s outcomes.", len(pairs))
    return model


def apply(model, raw_confidence: float) -> float:
    """Map a heuristic score onto its observed rate.

    Returns the raw score unchanged when there is no mapping, so an
    uncalibrated deployment behaves exactly as it did before.
    """
    try:
        score = float(raw_confidence)
    except (TypeError, ValueError):
        return raw_confidence
    if model is None:
        return score
    try:
        return float(round(model.predict([score])[0], 4))
    except Exception as e:
        logger.warning("Calibration lookup failed for %.3f: %s", score, e)
        return score


async def calibrate(redis_client, raw_confidence: float) -> Dict[str, Any]:
    """The published confidence, with the raw score kept beside it.

    Both are returned deliberately. A reader comparing them can see how far the
    heuristic was off, and a reader who only ever sees the corrected number has
    no way to notice the correction is doing nothing.
    """
    model = await get_model(redis_client)
    calibrated = apply(model, raw_confidence)
    return {
        "confidence": calibrated,
        "raw_confidence": float(raw_confidence),
        "calibrated": model is not None,
        "calibration_samples": _cached_n,
    }
