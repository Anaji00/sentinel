"""
services/api_gateway/routes/search.py

Semantic retrieval over the event corpus (INN-03).

Embeddings have been computed and written to Qdrant since the platform was
built, but nothing ever read them: there was no query surface anywhere in the
API. The index was write-only.

"Show me historically similar situations to this one" is a different analytical
question from "show me correlated tickers" -- correlation finds what moves
together, similarity finds precedent. It is also the natural retrieval layer for
grounding an agent prompt in what actually happened before, rather than in a
fixed graph hop.

Retrieval works from the vector already stored with each event, so the gateway
never loads an embedding model. That keeps a ~420 MB model out of the API
container, which matters on a CPU-only host where memory is the binding
constraint.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils.serialization import score_dto

logger = logging.getLogger("api-gateway.search")

router = APIRouter(prefix="/api/v1/search", tags=["Semantic Retrieval"])

QDRANT_COLLECTION = "sentinel_events"

# Cosine similarity below this is noise: with 768-dimensional embeddings almost
# any pair scores weakly positive, so an unfiltered nearest-neighbour list
# always returns `limit` results regardless of whether anything is actually
# similar. Requiring a floor lets the endpoint return nothing, honestly.
MIN_SIMILARITY = 0.55


async def _qdrant():
    """Async Qdrant client, or None when the dependency is unavailable.

    Imported lazily so the gateway starts without qdrant-client installed --
    semantic search then reports itself unavailable rather than breaking
    every other route in the process.
    """
    try:
        from qdrant_client import AsyncQdrantClient
    except ImportError:
        logger.warning("qdrant-client not installed; semantic search disabled.")
        return None
    try:
        host = os.getenv("QDRANT_HOST", "qdrant")
        client = AsyncQdrantClient(host=host, port=int(os.getenv("QDRANT_PORT", "6333")))
        if not await client.collection_exists(QDRANT_COLLECTION):
            logger.warning("Qdrant collection '%s' does not exist yet.", QDRANT_COLLECTION)
            return None
        return client
    except Exception as e:
        logger.warning("Qdrant unreachable: %s", e)
        return None


def _hit_to_dto(hit: Any) -> Dict[str, Any]:
    """Normalizes a Qdrant hit into the platform's response shape."""
    payload = getattr(hit, "payload", None) or {}
    return score_dto({
        "event_id": payload.get("event_id") or str(getattr(hit, "id", "")),
        "type": payload.get("type"),
        "domain": payload.get("domain"),
        "region": payload.get("region"),
        "occurred_at": payload.get("occurred_at"),
        "anomaly_score": payload.get("anomaly"),
        "similarity": float(getattr(hit, "score", 0.0)),
    })


@router.get("/similar/{event_id}")
async def find_similar_events(
    event_id: str,
    limit: int = Query(10, ge=1, le=50),
    min_similarity: float = Query(MIN_SIMILARITY, ge=0.0, le=1.0),
    exclude_domain: Optional[str] = Query(
        None,
        description="Omit results from this domain. Set it to the source event's "
                    "own domain to surface cross-domain precedent only.",
    ),
    redis=Depends(get_redis_optional),
):
    """Events semantically nearest to *event_id*, by stored embedding.

    Uses the vector already indexed for this event, so no embedding model is
    loaded here. Returns an empty list -- not an error -- when the corpus holds
    nothing above the similarity floor.
    """
    client = await _qdrant()
    if client is None:
        raise HTTPException(
            status_code=503,
            detail="Semantic search unavailable: the vector index is not reachable.",
        )

    try:
        stored = await client.retrieve(
            collection_name=QDRANT_COLLECTION,
            ids=[event_id],
            with_vectors=True,
        )
        if not stored:
            raise HTTPException(
                status_code=404,
                detail=f"Event '{event_id}' has no embedding indexed. "
                       f"Only enriched events are retrievable.",
            )

        vector = getattr(stored[0], "vector", None)
        if not vector:
            raise HTTPException(status_code=404, detail="Indexed event carries no vector.")

        # limit + 1: the query event is its own nearest neighbour and is
        # filtered out below, so ask for one extra to still return `limit`.
        hits = await client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=vector,
            limit=limit + 1,
            score_threshold=min_similarity,
        )

        results: List[Dict[str, Any]] = []
        for hit in hits:
            payload = getattr(hit, "payload", None) or {}
            if payload.get("event_id") == event_id:
                continue
            if exclude_domain and payload.get("domain") == exclude_domain:
                continue
            results.append(_hit_to_dto(hit))
            if len(results) >= limit:
                break

        return {
            "query_event_id": event_id,
            "min_similarity": min_similarity,
            "count": len(results),
            "results": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Semantic search failed for %s: %s", event_id, e, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Vector search failed: {e}")


@router.get("/status")
async def search_status():
    """Whether semantic retrieval is available, and how much is indexed.

    Exposed because "no results" and "index empty" look identical from a
    dashboard, and they call for completely different responses.
    """
    client = await _qdrant()
    if client is None:
        return {
            "available": False,
            "reason": "Qdrant unreachable or collection absent",
            "indexed_events": None,
        }
    try:
        info = await client.count(collection_name=QDRANT_COLLECTION, exact=False)
        return {
            "available": True,
            "collection": QDRANT_COLLECTION,
            "indexed_events": int(getattr(info, "count", 0)),
            "min_similarity_default": MIN_SIMILARITY,
        }
    except Exception as e:
        return {"available": False, "reason": str(e), "indexed_events": None}
