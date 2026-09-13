"""
services/api_gateway/routes/dlq.py

Dead-letter inspection and replay.

`failed_events` had 11,539 rows, a `resolved` column no statement ever wrote,
an index maintained on that column, and no way for anyone to act on a row. A
dead-letter table with no replay path is a log file with a schema: the reason
the full payload is kept is so it can be put back.

Replay is queued rather than performed here. The gateway has no Kafka producer
and should not grow one for this -- the dlq-worker already holds the producer,
already owns the table, and is the process that must survive the republish. The
queue is a Redis list so a request an operator made outlives a worker restart.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from shared.utils.rbac import require_role, Role
from services.api_gateway.dependencies import get_redis_client, get_db_optional

logger = logging.getLogger("api-gateway.dlq")

router = APIRouter(prefix="/api/v1/dlq", tags=["Operations"])

# Must match REPLAY_QUEUE_KEY in services/dlq-worker/main.py.
REPLAY_QUEUE_KEY = "sentinel:dlq:replay_queue"

# One request may not queue an unbounded replay: putting ten thousand poison
# pills back on a live topic is the failure this table exists to stop.
MAX_REPLAY_BATCH = 100


class ReplayRequest(BaseModel):
    """Which dead letters to put back."""
    ids: List[int] = Field(..., min_length=1, max_length=MAX_REPLAY_BATCH)


def _requester(request: Request) -> str:
    identity = getattr(request.state, "identity", None)
    if isinstance(identity, str) and identity:
        return identity[:120]
    return "unknown"


@router.get("/summary", dependencies=[Depends(require_role(Role.ANALYST))])
async def dlq_summary(db=Depends(get_db_optional)) -> Dict[str, Any]:
    """What is outstanding, by topic."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    rows = await db.query(
        """
        SELECT original_topic,
               COUNT(*)                                   AS total,
               COUNT(*) FILTER (WHERE resolved)           AS resolved,
               COUNT(*) FILTER (WHERE NOT resolved)       AS outstanding,
               COUNT(*) FILTER (WHERE permanently_failed) AS permanently_failed,
               MIN(failed_at)                             AS oldest,
               MAX(failed_at)                             AS newest
        FROM failed_events
        GROUP BY original_topic
        ORDER BY outstanding DESC, total DESC
        """
    )
    topics = [
        {
            "topic": r["original_topic"],
            "total": int(r["total"] or 0),
            "resolved": int(r["resolved"] or 0),
            "outstanding": int(r["outstanding"] or 0),
            "permanently_failed": int(r["permanently_failed"] or 0),
            "oldest": r["oldest"].isoformat() if r["oldest"] else None,
            "newest": r["newest"].isoformat() if r["newest"] else None,
        }
        for r in rows
    ]
    return {
        "topics": topics,
        "total": sum(t["total"] for t in topics),
        "outstanding": sum(t["outstanding"] for t in topics),
    }


@router.get("/events", dependencies=[Depends(require_role(Role.ANALYST))])
async def list_failed_events(
    topic: Optional[str] = Query(None, description="Restrict to one original topic"),
    outstanding_only: bool = Query(True),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db=Depends(get_db_optional),
) -> Dict[str, Any]:
    """The rows themselves, newest first."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    clauses: List[str] = []
    params: List[Any] = []
    if topic:
        params.append(topic)
        clauses.append("original_topic = $" + str(len(params)))
    if outstanding_only:
        clauses.append("resolved = FALSE")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    params.extend([limit, offset])
    limit_ph = "$" + str(len(params) - 1)
    offset_ph = "$" + str(len(params))
    rows = await db.query(
        f"""
        SELECT id, failed_at, original_topic, error_message, retry_count,
               permanently_failed, resolved, resolved_at, resolved_by,
               replay_count, last_replay_error
        FROM failed_events
        {where}
        ORDER BY failed_at DESC
        LIMIT {limit_ph} OFFSET {offset_ph}
        """,
        *params,
    )
    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "events": [
            {
                "id": int(r["id"]),
                "failed_at": r["failed_at"].isoformat() if r["failed_at"] else None,
                "topic": r["original_topic"],
                # Truncated: these are stack-trace tails and the list view is a
                # triage surface, not a debugger.
                "error": (r["error_message"] or "")[:500],
                "retry_count": int(r["retry_count"] or 0),
                "permanently_failed": bool(r["permanently_failed"]),
                "resolved": bool(r["resolved"]),
                "resolved_at": r["resolved_at"].isoformat() if r["resolved_at"] else None,
                "resolved_by": r["resolved_by"],
                "replay_count": int(r["replay_count"] or 0),
                "last_replay_error": r["last_replay_error"],
            }
            for r in rows
        ],
    }


@router.post("/replay", dependencies=[Depends(require_role(Role.ADMIN))])
async def replay_failed_events(
    body: ReplayRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_client),
) -> Dict[str, Any]:
    """Queue dead letters to be republished on the topic they failed on.

    Admin-only, and bounded. Most of what reaches this table was classified as a
    poison pill before the row was written, and replaying those in bulk
    reproduces exactly the unbounded retry the dead-letter path exists to end.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # Only rows that exist and are still outstanding are queued, so the reply
    # says what will actually happen rather than echoing the request back.
    rows = await db.query(
        """
        SELECT id, original_topic
        FROM failed_events
        WHERE id = ANY($1::bigint[]) AND resolved = FALSE
        """,
        body.ids,
    )
    eligible = [int(r["id"]) for r in rows]
    skipped = sorted(set(body.ids) - set(eligible))

    requester = _requester(request)
    queued = 0
    for row_id in eligible:
        try:
            await redis.raw.rpush(
                REPLAY_QUEUE_KEY,
                json.dumps({"id": row_id, "requested_by": requester}),
            )
            queued += 1
        except Exception as e:
            logger.error(f"Could not queue DLQ replay for {row_id}: {e}")
            break

    logger.info("DLQ replay queued: %d row(s) by %s", queued, requester)
    return {
        "queued": queued,
        "skipped": skipped,
        "detail": (
            "Queued for the dlq-worker to republish. Rows already resolved or "
            "not found are reported under 'skipped'."
        ),
    }
