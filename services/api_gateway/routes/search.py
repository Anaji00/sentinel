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
from shared.utils.vector_index import EVENT_COLLECTION

logger = logging.getLogger("api-gateway.search")

router = APIRouter(prefix="/api/v1/search", tags=["Semantic Retrieval"])

# One definition, shared with the correlator that writes these vectors.
# This read `sentinel_events`, which Qdrant 404s, while the writer had long
# since moved to the versioned name -- so every semantic query reported the
# index unavailable against half a million points.
QDRANT_COLLECTION = EVENT_COLLECTION

# Cosine similarity below this is noise: with 768-dimensional embeddings almost
# any pair scores weakly positive, so an unfiltered nearest-neighbour list
# always returns `limit` results regardless of whether anything is actually
# similar. Requiring a floor lets the endpoint return nothing, honestly.
MIN_SIMILARITY = 0.55


def _qdrant_base() -> str:
    host = os.getenv("QDRANT_HOST", "qdrant")
    port = os.getenv("QDRANT_PORT") or "6333"
    return f"http://{host}:{port}"


class VectorIndexUnavailable(RuntimeError):
    """The index cannot be queried, and which of the reasons it is.

    `reason` is deliberately specific. The endpoint used to answer every
    failure with "Qdrant unreachable or collection absent", which is two
    guesses and covered neither of the two things that were actually wrong --
    an uninstalled client library and a collection name that had never
    existed. An operator reading "unavailable" needs to know whether to
    restart a container, fix a network, or run an indexer.
    """

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


async def _qdrant_post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """One Qdrant REST call.

    `aiohttp` rather than `qdrant_client`: the client package lives in
    `requirements-ml.txt` and this container is built from the base image, so
    importing it has never succeeded here. aiohttp is already a base
    dependency and Qdrant's four operations are four HTTP requests.
    """
    import aiohttp

    url = f"{_qdrant_base()}{path}"
    # Thirty seconds, matching the platform's own client timeout. Ten was
    # too tight: a cold search over half a million vectors returned 502
    # 'Qdrant unreachable' when Qdrant was answering perfectly well, which
    # is the same conflation this endpoint was just fixed for.
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=body) as resp:
                if resp.status == 404:
                    raise VectorIndexUnavailable(
                        f"collection '{QDRANT_COLLECTION}' does not exist"
                    )
                if resp.status >= 400:
                    raise VectorIndexUnavailable(
                        f"Qdrant returned HTTP {resp.status} for {path}"
                    )
                payload = await resp.json()
    except VectorIndexUnavailable:
        raise
    except Exception as e:
        raise VectorIndexUnavailable(f"Qdrant unreachable at {_qdrant_base()}: {e}") from e

    return payload.get("result") or {}


async def _assert_index_ready() -> None:
    """Raises with the specific reason, or returns having confirmed the index."""
    import aiohttp

    url = f"{_qdrant_base()}/collections/{QDRANT_COLLECTION}"
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status == 404:
                    raise VectorIndexUnavailable(
                        f"collection '{QDRANT_COLLECTION}' does not exist"
                    )
                if resp.status >= 400:
                    raise VectorIndexUnavailable(f"Qdrant returned HTTP {resp.status}")
    except VectorIndexUnavailable:
        raise
    except Exception as e:
        raise VectorIndexUnavailable(f"Qdrant unreachable at {_qdrant_base()}: {e}") from e


def _hit_to_dto(hit: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizes a Qdrant hit into the platform's response shape.

    A plain dict now, because the REST API returns JSON rather than the
    client's objects. The field names are Qdrant's own.
    """
    payload = hit.get("payload") or {}
    return score_dto({
        "event_id": payload.get("event_id") or str(hit.get("id", "")),
        "type": payload.get("type"),
        "domain": payload.get("domain"),
        "region": payload.get("region"),
        "occurred_at": payload.get("occurred_at"),
        "anomaly_score": payload.get("anomaly"),
        "similarity": float(hit.get("score") or 0.0),
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
    try:
        await _assert_index_ready()
    except VectorIndexUnavailable as e:
        raise HTTPException(
            status_code=503,
            detail=f"Semantic search unavailable: {e.reason}",
        )

    try:
        stored = await _qdrant_post(
            f"/collections/{QDRANT_COLLECTION}/points",
            {"ids": [event_id], "with_vector": True, "with_payload": False},
        )
        points = stored if isinstance(stored, list) else stored.get("points") or []
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"Event '{event_id}' has no embedding indexed. "
                       f"Only enriched events are retrievable.",
            )

        vector = points[0].get("vector")
        if not vector:
            raise HTTPException(status_code=404, detail="Indexed event carries no vector.")

        # limit + 1: the query event is its own nearest neighbour and is
        # filtered out below, so ask for one extra to still return `limit`.
        hits = await _qdrant_post(
            f"/collections/{QDRANT_COLLECTION}/points/search",
            {
                "vector": vector,
                "limit": limit + 1,
                "score_threshold": min_similarity,
                "with_payload": True,
            },
        )
        hits = hits if isinstance(hits, list) else hits.get("points") or []

        results: List[Dict[str, Any]] = []
        for hit in hits:
            payload = hit.get("payload") or {}
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
    try:
        await _assert_index_ready()
        info = await _qdrant_post(
            f"/collections/{QDRANT_COLLECTION}/points/count", {"exact": False}
        )
        return {
            "available": True,
            "collection": QDRANT_COLLECTION,
            "indexed_events": int(info.get("count") or 0),
            "min_similarity_default": MIN_SIMILARITY,
        }
    except VectorIndexUnavailable as e:
        # The specific reason, not a guess covering three of them.
        return {
            "available": False,
            "reason": e.reason,
            "collection": QDRANT_COLLECTION,
            "indexed_events": None,
        }
    except Exception as e:
        return {
            "available": False,
            "reason": f"unexpected error: {e}",
            "collection": QDRANT_COLLECTION,
            "indexed_events": None,
        }
