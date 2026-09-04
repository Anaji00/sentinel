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
