"""
services/correlation/soft_correlator.py
 
Phase 2 — Embedding-based soft correlation.
 
The rule engine (rules/*.py) catches known patterns.
This catches unknown ones by embedding every event and finding
semantically similar events across domains using cosine similarity.
 
How it works:
  1. Every NormalizedEvent gets embedded (sentence-transformers)
  2. Embeddings stored in Qdrant vector DB (or pgvector)
  3. On each new event, query nearest neighbours across domains
  4. If similarity > threshold and domains differ → soft correlation
 
This is what catches "tanker named SUNRISE GLORY goes dark" correlating with
"Russia crude export decline" even though no hard rule connects them —
the embedding space captures the semantic connection.
 
Not running yet — wired in but disabled until Phase 2 infra is ready.
"""
import asyncio
import uuid
import os
import logging
from functools import partial
# Import datetime for handling time-based logic.
from datetime import datetime, timezone
# Import type hints for better code readability and IDE support.
from typing import List, Optional, Dict
 
# Import the NormalizedEvent model, which is the standard data format we use across Sentinel.
from shared.models import NormalizedEvent
from shared.utils.ollama import OllamaClient
# Initialize the logger specific to this soft correlation module.
logger = logging.getLogger("correlation.soft")
 
# Similarity threshold — events with cosine similarity above this
# are considered semantically related
# (Cosine similarity ranges from -1.0 to 1.0. 0.72 means "fairly similar meaning").
SIMILARITY_THRESHOLD = 0.72
 
# Minimum time window to search for correlations (hours)
LOOKBACK_HOURS = 48

class SoftCorrelator:
    """
    Embedding-based correlator.
    Finds semantically similar events across domains without hard-coded rules.
 
    Requires:
      sentence-transformers    (pip install sentence-transformers)
      qdrant-client            (pip install qdrant-client)
      Running Qdrant instance  (add to docker-compose in Phase 2)
    """
    def __init__(self, ollama_client: OllamaClient):
        self._model = None  # Lazy load the embedding model
        self._client = None  # Lazy load the Qdrant client
        self._enabled = False  # Set to True when ready to activate'
        self._llm = ollama_client
        self._embed_semaphore = asyncio.Semaphore(5)  # Limit concurrent embeddings to avoid overload
        self._load_lock = asyncio.Lock()  # Ensure only one load happens if multiple events come in at startup
    async def _load(self):
        """Lazy-load heavy dependencies. Called on first use."""
        if self._enabled: return
        async with self._load_lock:
            if self._enabled: return  # Double-check locking
            try:
                import sentence_transformers
                from qdrant_client import AsyncQdrantClient
                from qdrant_client.http import models
                loop = asyncio.get_running_loop()
                self._model = await loop.run_in_executor(
                        None, 
                        lambda: sentence_transformers.SentenceTransformer("all-mpnet-base-v2")
                    )
                
                logger.info("SentenceTransformer model loaded")
                qdrant_host = os.getenv("QDRANT_HOST", "qdrant")
                # Connect to the local Qdrant instance on its default port.
                self._client = AsyncQdrantClient(host=qdrant_host, port=6333)
                # Mark the correlator as fully active and ready to process events.
                for collection in ["sentinel_events", "sentinel_concepts"]:
                    exists = await self._client.collection_exists(collection)
                    if not exists:
                        try:
                            await self._client.create_collection(
                                collection_name=collection,
                                vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE)
                            )
                        except Exception as ce:
                            if "already exists" in str(ce).lower() or "409" in str(ce):
                                logger.info(f"Collection '{collection}' was created concurrently.")
                            else:
                                raise
                self._enabled = True
                logger.info(f"Async Qdrant client connected to {qdrant_host} and SoftCorrelator enabled.")
            except Exception as e:
                # Catch any other errors (like Qdrant being unreachable) so the main app doesn't crash.
                logger.warning(f"Qdrant unreachable (Phase 2 feature): {e}. Soft correlation disabled.")
        
    async def embed_event(self, event: NormalizedEvent) -> Optional[List[float]]:
        """Convert event to embedding vector for similarity search."""
        """Convert event to embedding vector without freezing the event loop."""
        if not self._enabled: return None
        
        async with self._embed_semaphore:
            # 1. Native Semantic Formatting
            # LLM translation is too slow for real-time telemetry streaming and causes Ollama timeouts.
            # We use native f-strings to build a dense semantic representation for the embedding model.
            natural_language_desc = f"Event of type {event.type.value} involving {event.primary_entity.name} in {event.region}. Flags: {event.primary_entity.flags}. Description: {event.headline}"
            loop = asyncio.get_running_loop()
            try:
                encode_func = partial(self._model.encode, show_progress_bar=False)
                embedding_array = await loop.run_in_executor(None, encode_func, natural_language_desc)
                return embedding_array.tolist()  
            except Exception as e:
                logger.error(f"Error embedding event {event.event_id}: {e}")
                return None
        
    async def store(self, event: NormalizedEvent, embedding: List[float]):
        """Store event embedding in Qdrant with metadata for later retrieval."""
        # Safety check: Do nothing if the system isn't enabled or connected.
        if not self._enabled or not self._client:
            return
        try:
            # 'Upsert' means "Insert if it doesn't exist, Update if it does".
            await self._client.upsert( 
                collection_name="sentinel_events",
                points = [{
                    # Qdrant requires IDs to be specific formats (like a 16-byte UUID or integer).
                    # We strip the hyphens from our event UUID and take the first 16 characters.
                    "id": event.event_id,  # Qdrant expects a 16-byte hex string for ID
                    "vector": embedding, # The embedding vector for similarity search
                    "payload": { # Metadata to help us understand what this event is about when we retrieve it
                        "event_id": event.event_id, # JSON-serializable ID for reference
                        "type": event.type.value, 
                        "occurred_at": event.occurred_at.isoformat(),
                        "region": event.region,
                        # Extract the high-level domain (e.g. "maritime" from "maritime_vessel_dark").
                        "domain": event.type.value.split("_")[0],  # Extract domain from type (e.g., "Maritime" from "Maritime_Anomaly")
                        "anomaly": event.anomaly_score,
                    },
                }],
            )
        except Exception as e:
            # Log the error. (Note: 'debug44' appears to be a typo for 'debug' or 'error' in the original code).
            logger.debug(f"Qdrant store failed for event{event.event_id}: {e}", exc_info=True)
    
    async def find_similar(
            self, 
            embedding: List[float],
            exclude_domain: str,
            limit: int = 10,
    ) -> List[Dict]:
        """
        Find similar events from OTHER domains.
        Returns list of payload dicts from Qdrant.
        """

        # Safety check to ensure the vector database client is ready.
        if not self._enabled or not self._client:
            return []
        try:
            from qdrant_client.http import models
            # Ask the vector database for points that are closest in semantic meaning.
            results = await self._client.search(
                collection_name="sentinel_events",
                query_vector=embedding,
                # We fetch extra results because the filter step (must_not) might remove some.
                limit=limit + 20,   # fetch extra to filter by domain
                query_filter=models.Filter(
                    must_not=[
                        models.FieldCondition(
                            key="domain", 
                            match=models.MatchValue(value=exclude_domain)
                        )
                    ]
                ),
                score_threshold=SIMILARITY_THRESHOLD, # Only return results above the similarity threshold to reduce noise
            )
            # Map the raw Qdrant results back into a list of our custom 'payload' dictionaries.
            return [r.payload for r in results[:limit]] # Return only the top 'limit' results after filtering
        except Exception as e:
            logger.debug(f"Qdrant search failed: {e}")
            return []
        
    # ─── AGENTIC ONTOLOGY METHODS ──────────────────────────────────────────

    async def embed_text(self, text: str) -> List[float]:
        """Embeds raw concept strings efficiently on the background thread."""
        if not self._enabled: return []
        
        loop = asyncio.get_running_loop()
        try:
            embedding_array = await loop.run_in_executor(None, self._model.encode, text)
            return embedding_array.tolist()
        except Exception as e:
            logger.error(f"Text embedding failed for '{text}': {e}")
            return []

    async def register_concept(self, concept_name: str, embedding: List[float]):
        """Stores a newly discovered ontology concept asynchronously."""
        if not self._enabled or not self._client: return
        
        try:
            concept_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, concept_name))
            
            await self._client.upsert(
                collection_name="sentinel_concepts",
                points=[{
                    "id": concept_uuid, 
                    "vector": embedding, 
                    "payload": {
                        "concept_name": concept_name,
                        "created_at": datetime.now(timezone.utc).isoformat()
                    },
                }]
            )
        except Exception as e:
            logger.error(f"Qdrant async concept registration failed: {e}")

    async def find_similar_concepts(self, embedding: List[float], limit: int = 1) -> List[Dict]:
        """Checks if a proposed concept is semantically identical to an existing one."""
        if not self._enabled or not self._client: return []
        
        try:
            results = await self._client.search(
                collection_name="sentinel_concepts",
                query_vector=embedding,
                limit=limit,
                score_threshold=0.85, 
            )
            return [{"concept_name": r.payload["concept_name"], "score": r.score} for r in results]
        except Exception as e:
            logger.debug(f"Qdrant concept search empty or failed: {e}")
            return []
        
    
 