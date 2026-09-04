"""
services/api_gateway/routes/attribution.py

Which signals earn their weight, and which entities are the same subject.

Two capabilities the platform lacked, exposed where a person can act on them:

  - Signal attribution. There are 57 hand-set weights in the tree and nothing
    ever checked one against an outcome. This reports each signal's lift over
    the base rate so a reader can see which are carrying the system, which are
    indifferent, and which point the wrong way.

  - Entity merge review. Structural name folding is automatic and safe; fuzzy
    matches are not, so they are proposed here for a person to confirm or
    refuse. A wrong merge propagates into the graph, the consensus fusion and
    every stored correlation, and is not recoverable; a missed merge only costs
    a smaller graph.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils.entity_resolution import (
    canonical_key,
    record_alias,
    reject_merge,
    resolve_entity,
    suggest_merges,
)

logger = logging.getLogger("api-gateway.attribution")
router = APIRouter(prefix="/api/v1/attribution", tags=["Signal Attribution & Entity Resolution"])


@router.get("/signals")
async def get_signal_attribution(
    lookback_days: int = Query(90, ge=7, le=365),
    db=Depends(get_db_optional),
):
    """Each signal's measured lift over the base rate.

    `lift` is the difference in confirmation rate between outcomes where the
    signal was present and where it was absent. Near zero means the signal is
    not distinguishing anything, whatever weight it carries in the code.
    Negative means it points the wrong way.

    `support` is reported beside every figure because a lift of 0.4 on nine
    samples and 0.04 on nine thousand are opposite findings that look alike in
    a table. When there is too little history the response says so rather than
    returning zeros, which would be indistinguishable from a measured null.
    """
    from services.reasoning.signal_attribution import attribute_signals
    return await attribute_signals(db, lookback_days=lookback_days)


class AliasRequest(BaseModel):
    alias: str = Field(..., min_length=1, description="The spelling to record")
    canonical: str = Field(..., min_length=1, description="The subject it names")


@router.get("/entities/resolve")
async def resolve_one(
    name: str = Query(..., min_length=1),
    redis=Depends(get_redis_optional),
):
    """What a given spelling resolves to, and what it folds to without the store."""
    return {
        "input": name,
        "canonical": await resolve_entity(redis, name),
        "structural_fold": canonical_key(name),
    }


@router.get("/entities/merge-candidates")
async def get_merge_candidates(
    limit: int = Query(25, ge=1, le=100),
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Pairs that look like one subject, for a person to confirm.

    Never applied automatically. "Delta Air Lines" and "Delta Apparel" are four
    characters apart, and the platform would rather hold two nodes for one
    company than one node for two.
    """
    if not db:
        return {"candidates": [], "reason": "no database client"}
    try:
        rows = await db.query(
            """
            SELECT DISTINCT primary_entity_name AS name
            FROM events
            WHERE primary_entity_name IS NOT NULL
              AND primary_entity_name <> ''
              AND occurred_at > NOW() - INTERVAL '30 days'
            LIMIT 2000
            """
        )
    except Exception as e:
        logger.debug("Merge candidate scan failed: %s", e)
        raise HTTPException(status_code=503, detail="entity scan unavailable")

    names = [r["name"] for r in (rows or []) if r.get("name")]
    return {
        "scanned": len(names),
        "candidates": await suggest_merges(redis, names, limit=limit),
    }


@router.post("/entities/alias")
async def post_alias(req: AliasRequest, redis=Depends(get_redis_optional)):
    """Records that two spellings name the same subject.

    Explicit and permanent, and the top of the resolution order: being told
    beats being inferred.
    """
    ok = await record_alias(redis, req.alias, req.canonical)
    if not ok:
        raise HTTPException(
            status_code=400,
            detail="alias not recorded: empty, unavailable, or identical to the canonical form",
        )
    return {"alias": req.alias, "canonical": canonical_key(req.canonical), "recorded": True}


class RejectRequest(BaseModel):
    pair_id: str = Field(..., min_length=1, description="pair_id from merge-candidates")


@router.post("/entities/reject-merge")
async def post_reject_merge(req: RejectRequest, redis=Depends(get_redis_optional)):
    """Records that a person looked at a candidate pair and said no.

    A refusal is as much a fact as a confirmation. Without storing it the same
    pair is proposed on every sweep, which trains the reader to ignore the list.
    """
    ok = await reject_merge(redis, req.pair_id)
    if not ok:
        raise HTTPException(status_code=400, detail="rejection not recorded")
    return {"pair_id": req.pair_id, "rejected": True}
