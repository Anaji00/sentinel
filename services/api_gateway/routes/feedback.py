"""
services/api_gateway/routes/feedback.py

The judgement the system could not hear.

Everything this platform learns, it learns from the market: a prediction
resolves, a scenario is confirmed or denied, a Brier score moves. That is a real
feedback loop and it is the only one there was. It cannot capture the most
common way an alert is wrong here -- technically valid and operationally
useless. A correlation that fires correctly on a rule nobody wants, on an entity
nobody watches, resolves as "confirmed" and is reinforced.

There was no route, no control and no store through which an analyst could say
so. This adds one, and routes it to the same place the machine feedback goes
(RULES_FEEDBACK), so the rule synthesiser and the consensus engine see human
judgement in the shape they already consume.

Feedback is recorded as evidence, not as a command: marking an alert useless
does not delete a rule or suppress an entity. It accumulates against the rule,
and a rule whose feedback is consistently negative is surfaced for review. A
single analyst on a single bad morning should not be able to blind the platform.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from services.api_gateway.dependencies import get_redis_optional
from shared.utils.rbac import require_role, Role

logger = logging.getLogger("api-gateway.feedback")
router = APIRouter(prefix="/api/v1/feedback", tags=["Analyst Feedback"])

# Where feedback accumulates. Per-rule counters drive the review list; the log
# keeps the reasons, which are the part worth reading.
RULE_FEEDBACK_KEY = "sentinel:feedback:rule"
FEEDBACK_LOG_KEY = "sentinel:feedback:log"
FEEDBACK_LOG_MAX = 1000
FEEDBACK_TTL_SEC = 90 * 86400

# How much negative feedback, and how consistently, before a rule is surfaced
# for review. Both bars exist: three complaints out of four firings is a signal,
# three out of three hundred is an opinion.
MIN_FEEDBACK_FOR_REVIEW = 5
NEGATIVE_SHARE_FOR_REVIEW = 0.6

VERDICTS = ("useful", "not_useful", "wrong", "duplicate")


class FeedbackRequest(BaseModel):
    correlation_id: Optional[str] = Field(None, description="The correlation being judged")
    rule_id: Optional[str] = Field(None, description="The rule that produced it")
    verdict: str = Field(..., description=f"One of {VERDICTS}")
    reason: Optional[str] = Field(None, max_length=2000)

    @field_validator("verdict")
    @classmethod
    def _known_verdict(cls, v: str) -> str:
        value = str(v or "").strip().lower()
        if value not in VERDICTS:
            raise ValueError(f"verdict must be one of {VERDICTS}")
        return value

    @field_validator("correlation_id", "rule_id")
    @classmethod
    def _strip(cls, v):
        return str(v).strip() if v else None


@router.post("", dependencies=[Depends(require_role(Role.ANALYST))])
async def submit_feedback(req: FeedbackRequest, redis=Depends(get_redis_optional)):
    """Records an analyst's judgement of an alert.

    Requires a rule_id or a correlation_id: feedback that names nothing cannot
    be attributed to anything and would only inflate a denominator.
    """
    if not req.rule_id and not req.correlation_id:
        raise HTTPException(
            status_code=400,
            detail="feedback must name a rule_id or a correlation_id",
        )
    if not redis:
        raise HTTPException(status_code=503, detail="feedback store unavailable")

    rule_id = req.rule_id or "unattributed"
    positive = req.verdict == "useful"

    entry = {
        "correlation_id": req.correlation_id,
        "rule_id": rule_id,
        "verdict": req.verdict,
        "reason": req.reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        raw = getattr(redis, "raw", redis)
        pipe = raw.pipeline()
        pipe.hincrby(f"{RULE_FEEDBACK_KEY}:{rule_id}", "total", 1)
        pipe.hincrby(f"{RULE_FEEDBACK_KEY}:{rule_id}", req.verdict, 1)
        if not positive:
            pipe.hincrby(f"{RULE_FEEDBACK_KEY}:{rule_id}", "negative", 1)
        pipe.expire(f"{RULE_FEEDBACK_KEY}:{rule_id}", FEEDBACK_TTL_SEC)
        pipe.lpush(FEEDBACK_LOG_KEY, json.dumps(entry))
        pipe.ltrim(FEEDBACK_LOG_KEY, 0, FEEDBACK_LOG_MAX - 1)
        pipe.expire(FEEDBACK_LOG_KEY, FEEDBACK_TTL_SEC)
        pipe.sadd(f"{RULE_FEEDBACK_KEY}:index", rule_id)
        pipe.expire(f"{RULE_FEEDBACK_KEY}:index", FEEDBACK_TTL_SEC)
        await pipe.execute()
    except Exception as e:
        logger.error("Could not record feedback: %s", e)
        raise HTTPException(status_code=503, detail="feedback not recorded")

    # Onto the same topic the machine feedback uses, so the rule synthesiser and
    # the consensus engine receive human judgement in a shape they already read.
    try:
        from shared.kafka import SentinelProducer, Topics
        producer = SentinelProducer()
        await producer.start()
        try:
            await producer.send(
                Topics.RULES_FEEDBACK,
                {"source": "analyst", **entry},
                key=rule_id,
            )
        finally:
            await producer.stop()
    except Exception as e:
        # The record is already durable; publishing is the optional half.
        logger.debug("Feedback published to Redis but not to Kafka: %s", e)

    return {"recorded": True, **entry}


@router.get("/rules")
async def get_rule_feedback(
    limit: int = Query(50, ge=1, le=200),
    redis=Depends(get_redis_optional),
):
    """Feedback accumulated per rule, worst first.

    `needs_review` is set when a rule has both enough feedback to judge and a
    negative share above the threshold. Both conditions are required: three
    complaints out of four firings is a signal, three out of three hundred is
    one analyst's morning.
    """
    if not redis:
        return {"rules": []}
    try:
        raw = getattr(redis, "raw", redis)
        members = await raw.smembers(f"{RULE_FEEDBACK_KEY}:index")
        rule_ids = [m.decode() if isinstance(m, bytes) else str(m) for m in (members or [])]

        out: List[Dict[str, Any]] = []
        for rid in rule_ids:
            counts = await raw.hgetall(f"{RULE_FEEDBACK_KEY}:{rid}")
            if not counts:
                continue
            decoded = {
                (k.decode() if isinstance(k, bytes) else str(k)):
                int(v.decode() if isinstance(v, bytes) else v)
                for k, v in counts.items()
            }
            total = decoded.get("total", 0)
            negative = decoded.get("negative", 0)
            share = (negative / total) if total else 0.0
            out.append({
                "rule_id": rid,
                "total": total,
                "negative": negative,
                "negative_share": round(share, 4),
                "verdicts": {k: v for k, v in decoded.items()
                             if k not in ("total", "negative")},
                "needs_review": bool(
                    total >= MIN_FEEDBACK_FOR_REVIEW and share >= NEGATIVE_SHARE_FOR_REVIEW
                ),
            })

        out.sort(key=lambda r: (-r["negative_share"], -r["total"]))
        return {"rules": out[:limit]}
    except Exception as e:
        logger.debug("Rule feedback read failed: %s", e)
        return {"rules": []}


@router.get("/log")
async def get_feedback_log(
    limit: int = Query(50, ge=1, le=200),
    redis=Depends(get_redis_optional),
):
    """The reasons analysts gave, most recent first.

    The counters say a rule is disliked; only the reasons say why, and why is
    what a rule change has to be built on.
    """
    if not redis:
        return {"entries": []}
    try:
        raw = getattr(redis, "raw", redis)
        rows = await raw.lrange(FEEDBACK_LOG_KEY, 0, limit - 1)
        entries = []
        for r in rows or []:
            try:
                entries.append(json.loads(r.decode() if isinstance(r, bytes) else r))
            except Exception:
                continue
        return {"entries": entries}
    except Exception as e:
        logger.debug("Feedback log read failed: %s", e)
        return {"entries": []}


# ── ANALYST INTERACTION RECORD ────────────────────────────────────────────────
#
# The consequence gap, and the one thing in this audit that could not be closed
# by repairing code: "which score bands have preceded something a person acted
# on". Nothing anywhere recorded an analyst action, so the nearest available
# proxy was whether a scenario confirmed -- which measures the platform agreeing
# with itself.
#
# The explicit-judgement route above is not that record. It captures the alerts
# an analyst felt strongly enough about to grade, which is a small and
# self-selected sample. What calibration needs is the ordinary traffic: what was
# opened, what was scrolled past, what preceded a position.
#
# Recorded per score band rather than per alert, because the question is about
# bands: an alert at 0.9 that nobody opened is evidence about 0.9, and it stays
# evidence when the alert itself has expired out of every store. Bands are the
# unit the calibration loop already works in.
INTERACTION_KEY = "sentinel:feedback:interaction"
INTERACTION_LOG_KEY = "sentinel:feedback:interaction:log"
INTERACTION_LOG_MAX = 5000
INTERACTION_TTL_SEC = 180 * 86400

# Ordered weakest to strongest. `surfaced` is the denominator -- an alert the
# reader could have acted on -- and without it "opened 40 times" says nothing,
# because 40 of 50 and 40 of 40,000 are opposite findings.
INTERACTIONS = ("surfaced", "opened", "dismissed", "acted_on")

# Ten bands. Finer than that and each holds too little to say anything; coarser
# and the thing being calibrated disappears into the bucket.
INTERACTION_BANDS = 10


def _score_band(score: float) -> str:
    """The band a score falls in, as a stable string key."""
    try:
        s = max(0.0, min(1.0, float(score)))
    except (TypeError, ValueError):
        return "unknown"
    idx = min(INTERACTION_BANDS - 1, int(s * INTERACTION_BANDS))
    return f"{idx / INTERACTION_BANDS:.1f}-{(idx + 1) / INTERACTION_BANDS:.1f}"


class InteractionRequest(BaseModel):
    """One thing a reader did, or did not do, with one alert."""

    action: str = Field(..., description=f"One of {INTERACTIONS}")
    score: float = Field(..., ge=0.0, le=1.0, description="The alert's own score")
    correlation_id: Optional[str] = None
    rule_id: Optional[str] = None

    @field_validator("action")
    @classmethod
    def _known_action(cls, v: str) -> str:
        value = str(v or "").strip().lower()
        if value not in INTERACTIONS:
            raise ValueError(f"action must be one of {INTERACTIONS}")
        return value


@router.post("/interaction", dependencies=[Depends(require_role(Role.VIEWER))])
async def record_interaction(req: InteractionRequest, redis=Depends(get_redis_optional)):
    """Records that a reader saw, opened, dismissed or acted on an alert.

    VIEWER rather than ANALYST: this is the ordinary traffic of reading the
    platform, and restricting it to the role that grades alerts would reproduce
    the self-selection the explicit-feedback route already has.
    """
    if not redis:
        raise HTTPException(status_code=503, detail="interaction store unavailable")

    band = _score_band(req.score)
    entry = {
        "action": req.action,
        "score": round(float(req.score), 4),
        "band": band,
        "correlation_id": req.correlation_id,
        "rule_id": req.rule_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        raw = getattr(redis, "raw", redis)
        pipe = raw.pipeline()
        pipe.hincrby(f"{INTERACTION_KEY}:{band}", req.action, 1)
        pipe.expire(f"{INTERACTION_KEY}:{band}", INTERACTION_TTL_SEC)
        pipe.sadd(f"{INTERACTION_KEY}:index", band)
        pipe.expire(f"{INTERACTION_KEY}:index", INTERACTION_TTL_SEC)
        pipe.lpush(INTERACTION_LOG_KEY, json.dumps(entry))
        pipe.ltrim(INTERACTION_LOG_KEY, 0, INTERACTION_LOG_MAX - 1)
        pipe.expire(INTERACTION_LOG_KEY, INTERACTION_TTL_SEC)
        await pipe.execute()
    except Exception as e:
        logger.error("Could not record interaction: %s", e)
        raise HTTPException(status_code=503, detail="interaction not recorded")

    return {"recorded": True, **entry}


@router.get("/interaction/bands")
async def get_interaction_bands(redis=Depends(get_redis_optional)):
    """What readers did with each score band.

    `engagement_rate` is opened over surfaced and `action_rate` is acted_on over
    surfaced. A platform whose 0.9 band is opened less often than its 0.5 band
    is miscalibrated in the way that matters -- not against the market, against
    the person reading it -- and that is a statement no confirm/deny loop can
    make, because it is not about whether the alert was right.

    Reported with counts as well as rates, so a rate computed from four
    observations is visibly a rate computed from four observations.
    """
    if not redis:
        return {"bands": [], "total_surfaced": 0}
    try:
        raw = getattr(redis, "raw", redis)
        members = await raw.smembers(f"{INTERACTION_KEY}:index")
        bands = sorted(m.decode() if isinstance(m, bytes) else str(m) for m in (members or []))

        out: List[Dict[str, Any]] = []
        total_surfaced = 0
        for band in bands:
            counts = await raw.hgetall(f"{INTERACTION_KEY}:{band}")
            if not counts:
                continue
            decoded = {
                (k.decode() if isinstance(k, bytes) else str(k)):
                int(v.decode() if isinstance(v, bytes) else v)
                for k, v in counts.items()
            }
            surfaced = decoded.get("surfaced", 0)
            total_surfaced += surfaced
            out.append({
                "band": band,
                "surfaced": surfaced,
                "opened": decoded.get("opened", 0),
                "dismissed": decoded.get("dismissed", 0),
                "acted_on": decoded.get("acted_on", 0),
                # None, not 0.0, when nothing was surfaced: a rate with no
                # denominator is not a rate, and reporting it as zero would say
                # "nobody opened these" about a band nobody was ever shown.
                "engagement_rate": (
                    round(decoded.get("opened", 0) / surfaced, 4) if surfaced else None
                ),
                "action_rate": (
                    round(decoded.get("acted_on", 0) / surfaced, 4) if surfaced else None
                ),
            })
        return {"bands": out, "total_surfaced": total_surfaced}
    except Exception as e:
        logger.debug("Interaction band read failed: %s", e)
        return {"bands": [], "total_surfaced": 0}
