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
# Import standard logging for debugging and information output.
import logging
# Import datetime for handling time-based logic.
from datetime import datetime, timezone
# Import type hints for better code readability and IDE support.
from typing import List, Optional, Dict
 
# Import the NormalizedEvent model, which is the standard data format we use across Sentinel.
from shared.models import NormalizedEvent
 
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
    def __init__(self):
        # BEST PRACTICE: Lazy Loading. 
        # We don't load the heavy ML model or DB client right away. 
        # This keeps the app lightweight if Soft Correlation is turned off.
        self._model = None  # Lazy load the embedding model
        self._client = None  # Lazy load the Qdrant client
        self._enabled = False  # Set to True when ready to activate'
    
    def _load(self):
        """Lazy-load heavy dependencies. Called on first use."""
        try:
            # Import the ML library INSIDE the function so it only loads into memory when called.
            from sentence_transformers import SentenceTransformer
            # Load a pre-trained model capable of turning text into high-quality vector arrays.
            self._model = SentenceTransformer("all-mpnet-base-v2")
            logger.info("SentenceTransformer model loaded")

            # Import the client for Qdrant (our Vector Database).
            from qdrant_client import QdrantClient
            # Connect to the local Qdrant instance on its default port.
            self._client = QdrantClient(host="localhost", port=6333)
            # Mark the correlator as fully active and ready to process events.
            self._enabled = True
            logger.info(r"Qdrant client initialized and SoftCorrelator enabled")
            
        except ImportError as e:
            # If the required pip packages aren't installed, fail gracefully.
            logger.warning(f"Failed to load SoftCorrelator dependencies: {e}. Soft correlation disabled.")
        except Exception as e:
            # Catch any other errors (like Qdrant being unreachable) so the main app doesn't crash.
            logger.error(f"Error initializing SoftCorrelator: {e}. Soft correlation disabled.", exc_info=True)
        
    def embed_event(self, event: NormalizedEvent) -> Optional[List[float]]:
        """Convert event to embedding vector for similarity search."""
        # If the ML model isn't loaded or active, skip processing.
        if not self._enabled: 
            return None
        
        # We code similarity search by embedding the headline + tags + primary entity name
        # Start building a list of text strings that describe this event.
        parts = [f"type:{event.type.value}"]
        
        # If the event has a physical location, add it to our descriptive text.
        if event.region:
            parts.append(f"region:{event.region}")
            
        if event.primary_entity.name:
            parts.append(f"entity:{event.primary_entity.name}") # Primary entity (e.g., a company or country) is often the most important signal, so we keep it as a separate section
            
        if event.primary_entity.flags:
            parts.append(f"flags:{' '.join(event.primary_entity.flags)}") # Include flags in embedding to capture important context (e.g., "sanctioned", "critical_infrastructure")
            
        if event.headline:
            parts.append(f"headline:{event.headline}") # Headline is often the most semantically rich part, so we keep it as a separate section
            
        if event.named_entities:
            parts.append(f"entities:{" ".join(event.named_entities[:50])}")  # Limit to first 50 for embedding size
        
        # Combine all the descriptive parts into one single sentence/string separated by ' | '.
        text = " | ".join(parts) # Simple concatenation — can experiment with more sophisticated templates
        
        try:
            # Pass the text to the ML model, which returns an array of floats (the "embedding").
            # We convert it to a standard Python list so it can be saved in the database.
            return self._model.encode(text).tolist()  # Convert numpy array to list for storage
        except Exception as e:
            logger.error(f"Error embedding event {event.event_id}: {e}", exc_info=True)
            return None
        
    def store(self, event: NormalizedEvent, embedding: List[float]):
        """Store event embedding in Qdrant with metadata for later retrieval."""
        # Safety check: Do nothing if the system isn't enabled or connected.
        if not self._enabled or not self._client:
            return
        try:
            # 'Upsert' means "Insert if it doesn't exist, Update if it does".
            self._client.upsert( 
                collection_name="sentinel_events",
                points = [{
                    # Qdrant requires IDs to be specific formats (like a 16-byte UUID or integer).
                    # We strip the hyphens from our event UUID and take the first 16 characters.
                    "id": event.event_id.replace("-", "")[:16],  # Qdrant expects a 16-byte hex string for ID
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
    
    def find_similar(
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
            # Ask the vector database for points that are closest in semantic meaning.
            results = self._client.search(
                collection_name="sentinel_events",
                query_vector=embedding,
                # We fetch extra results because the filter step (must_not) might remove some.
                limit=limit + 20,   # fetch extra to filter by domain
                query_filter={
                    "must_not": [
                        # Core Logic: Only return events that belong to a DIFFERENT domain.
                        {"key": "domain", "match": {"value": exclude_domain}} # Exclude events from the same domain as the trigger event to focus on cross-domain correlations
                    ]
                },
                score_threshold=SIMILARITY_THRESHOLD, # Only return results above the similarity threshold to reduce noise
            )
            # Map the raw Qdrant results back into a list of our custom 'payload' dictionaries.
            return [r.payload for r in results[:limit]] # Return only the top 'limit' results after filtering
        except Exception as e:
            logger.debug(f"Qdrant search failed: {e}")
            return []
 