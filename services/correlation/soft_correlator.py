"""
services/correlation/soft_correlator.py

Embedding-based soft correlation — ACTIVE (wired in correlation/main.py).

The rule engine (cascade.py) catches known hard-rule patterns.
This catches unknown ones by embedding every event and finding
semantically similar events across domains using cosine similarity.

How it works:
  1. Every NormalizedEvent gets embedded (sentence-transformers, all-mpnet-base-v2)
  2. Embeddings stored in Qdrant vector DB
  3. On each new event, query nearest neighbours across domains
  4. If similarity > threshold and domains differ → soft correlation

This is what catches "tanker named SUNRISE GLORY goes dark" correlating with
"Russia crude export decline" even though no hard rule connects them —
the embedding space captures the semantic connection.

The similarity threshold is conformally calibrated (ConformalSimilarityCalibrator)
rather than fixed, targeting a specific false cross-domain link rate.
"""
import asyncio
import logging
import os
import uuid
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("correlation.soft")
from functools import partial
# Import datetime for handling time-based logic.
from datetime import datetime, timedelta, timezone
# Import type hints for better code readability and IDE support.
from typing import List, Optional, Dict

# Import the NormalizedEvent model, which is the standard data format we use across Sentinel.
from shared.models import NormalizedEvent
from shared.models.events import event_domain as canonical_domain
from shared.models.events import resolve_event_domain
from shared.utils.ollama import OllamaClient
from shared.utils.vector_index import EVENT_COLLECTION as _EVENT_COLLECTION
from shared.utils.quiet_failures import swallowed
from shared.utils.liveness import declare as _declare, fired as _fired
# Initialize the logger specific to this soft correlation module.
logger = logging.getLogger("correlation.soft")

# Default similarity threshold — used as initial prior before conformal calibration.
# Once calibrated, the ConformalSimilarityCalibrator's threshold replaces this.
# Versioned deliberately. Vectors written before the embedding text was
# corrected carry ~0.27 of similarity contributed purely by a shared sentence
# frame; comparing them against corrected ones would be worse than either
# alone. A new collection separates them without deleting anything -- the old
# one can be dropped once nothing needs it.
# Imported rather than declared: the API that reads these vectors had its
# own copy of this name and the two had drifted apart.
EVENT_COLLECTION = _EVENT_COLLECTION

# How long a vector outlives nothing.
#
# Set to the `events` hypertable's own retention, because a point whose event
# has been dropped can still be returned by a similarity search and then cannot
# be fetched. Migration 0020 sets `drop_after: 90 days`; if that changes, this
# is the other half and has to change with it. Nothing pruned this index before:
# 596,154 points had accumulated against a table that keeps ninety days.
VECTOR_RETENTION_SEC = int(os.getenv("VECTOR_RETENTION_SEC", str(90 * 86400)))

# Finding 545: nothing had ever deleted a vector.
_declare("correlation.vectors.pruned",
         "vector points whose events aged out were removed from the index")

# Ran, as distinct from had an effect.
#
# The count above fires only when the sweep removes something, so a sweep that
# runs daily and correctly finds nothing is indistinguishable from one that
# never runs -- which is the exact absence-vs-zero confusion the registry
# exists to end, reproduced inside it. "Swept 30, pruned 0" is a healthy
# mechanism; "swept 0" is the finding.
_declare("correlation.vectors.swept",
         "the vector retention sweep ran, whether or not it removed anything")

# Event types that say only "this thing is here now".
#
# A position fix makes no claim, so there is nothing for it to converge with,
# and its headline is a filled-in template: "Flight TGW405 position in Strait of
# Malacca" against "Vessel MSC MARA position in Strait of Malacca" share almost
# every token. A sentence encoder scores that pair very highly and it means
# nothing -- a ship and an aircraft near the same strait is geography, not a
# relationship. Observed: the container ship MSC MARA was published as
# "semantically converged" with an aviation emergency alert on exactly that
# resemblance, and the two share no connection beyond a place name.
#
# Excluded from the index entirely rather than filtered at query time: something
# that cannot be evidence should not be stored as evidence. Dark, spoofed and
# anomalous position events are deliberately NOT listed -- those are findings
# about a vessel or aircraft, and a finding is the kind of thing worth
# correlating.
# Defined in shared/models/events.py so the agent layer can apply the same
# rule without importing this service. Re-exported here because callers across
# the correlation package import it from this module by name.
from shared.models.events import POSITION_TELEMETRY_TYPES
from shared.models.events import Domain

# The canonical domain names, and the mapping that repairs a stored pseudo-one.
#
# A payload written before `store_event` was corrected holds a `.split("_")[0]`
# prefix rather than a domain: `flight` for aviation, `vessel` for maritime,
# `market`/`equity`/`options`/`filing`/`earnings` for tradfi, `bgp`/`ransomware`
# for cyber, `headline` for news. Those are not domains and never were, and the
# points carrying them cannot be rewritten because store_event only upserts on
# ingest.
_CANONICAL_DOMAINS = frozenset(d.value for d in Domain)

# Written out, not derived. Deriving a domain from a prefix is the defect being
# repaired here; a table of the prefixes actually observed in the live store is
# the honest inverse of it.
_LEGACY_DOMAIN_ALIASES = {
    "flight": Domain.AVIATION.value,
    "aircraft": Domain.AVIATION.value,
    "adsb": Domain.AVIATION.value,
    "vessel": Domain.MARITIME.value,
    "ais": Domain.MARITIME.value,
    # "market" is deliberately absent.
    #
    # It is the prefix of `market_anomaly`, which the crypto candle path and the
    # equity candle path both emit -- the one type this platform already records
    # as unresolvable from a type alone (AMBIGUOUS_EVENT_TYPES names it, and
    # resolve_event_domain exists because of it). A stale payload carrying
    # "market" does not say which domain produced it, so mapping it to either
    # would invent the answer. Unresolved means it is not counted as a domain at
    # all, which is the honest treatment and is what the consumer's `if d`
    # filter already does.
    "equity": Domain.TRADFI.value,
    "options": Domain.TRADFI.value,
    "filing": Domain.TRADFI.value,
    "earnings": Domain.TRADFI.value,
    "insider": Domain.TRADFI.value,
    "dark": Domain.TRADFI.value,
    "bgp": Domain.CYBER.value,
    "ransomware": Domain.CYBER.value,
    "vulnerability": Domain.CYBER.value,
    "breach": Domain.CYBER.value,
    "headline": Domain.NEWS.value,
    "social": Domain.NEWS.value,
    "osint": Domain.NEWS.value,
    "macro": Domain.MACRO.value,
    "supply": Domain.MACRO.value,
    "prediction": Domain.PREDICTION.value,
    "crypto": Domain.CRYPTO.value,
}


def _canonical_domain_name(stored) -> Optional[str]:
    """A stored payload domain, as a canonical domain name.

    Returns the value unchanged when it already is one, maps the known legacy
    prefixes when it is not, and falls back to the event-type table for anything
    else. `None` stays `None`: an absent domain is not evidence of a domain, and
    the consumer filters those out rather than counting them.
    """
    if not stored:
        return None
    value = str(stored).strip().lower()
    if value in _CANONICAL_DOMAINS:
        return value
    mapped = _LEGACY_DOMAIN_ALIASES.get(value)
    if mapped:
        return mapped
    # Some legacy payloads stored a whole event type rather than a prefix.
    resolved = canonical_domain(value)
    return resolved if resolved != Domain.OTHER.value else None


SIMILARITY_THRESHOLD_DEFAULT = 0.65


class ConformalSimilarityCalibrator:
    """
    Conformal calibration for embedding-similarity thresholds.

    Instead of a fixed cosine cutoff (0.65), we calibrate against a target
    false cross-domain link rate (FAR).  The approach:

    1. Maintain a rolling buffer of observed similarity scores from
       same-domain pairs (which should NOT be linked cross-domain).
    2. These form the "null distribution" — similarity scores under the
       null hypothesis that two events are unrelated.
    3. Set the threshold at the (1 - target_FAR) quantile of this
       null distribution, so only target_FAR fraction of truly unrelated
       pairs would exceed the threshold.

    This mirrors the conformal-calibration idea applied to anomaly z-scores:
    calibrate against an actual target false-positive rate rather than a
    fixed global cutoff.
    """

    def __init__(
        self,
        target_far: float = 0.05,
        buffer_size: int = 2000,
        min_samples: int = 50,
        default_threshold: float = SIMILARITY_THRESHOLD_DEFAULT,
    ):
        self.target_far = target_far
        self.buffer_size = buffer_size
        self.min_samples = min_samples
        self.default_threshold = default_threshold

        # Rolling buffer of null-distribution similarity scores
        # (scores from same-domain pairs that should not be cross-linked)
        self._null_scores: list = []
        self._calibrated_threshold: Optional[float] = None

    @property
    def threshold(self) -> float:
        """Return the current calibrated threshold, or default if not yet calibrated."""
        if self._calibrated_threshold is not None:
            return self._calibrated_threshold
        return self.default_threshold

    def observe_null_score(self, similarity: float):
        """
        Record a similarity score from a same-domain pair (null hypothesis:
        these events should NOT produce a cross-domain link).
        """
        self._null_scores.append(similarity)
        if len(self._null_scores) > self.buffer_size:
            self._null_scores = self._null_scores[-self.buffer_size:]

        # Recalibrate when we have enough samples
        if len(self._null_scores) >= self.min_samples:
            self._recalibrate()

    def _recalibrate(self):
        """Recompute the threshold from the null distribution."""
        sorted_scores = sorted(self._null_scores)
        # Threshold = (1 - FAR) quantile of null distribution
        idx = int(len(sorted_scores) * (1.0 - self.target_far))
        idx = min(idx, len(sorted_scores) - 1)
        new_threshold = sorted_scores[idx]

        # Clamp to reasonable range [0.40, 0.90] to prevent degenerate thresholds
        new_threshold = max(0.40, min(0.90, new_threshold))

        if self._calibrated_threshold is None or abs(new_threshold - self._calibrated_threshold) > 0.01:
            logger.info(
                f"Conformal similarity threshold recalibrated: "
                f"{self._calibrated_threshold or self.default_threshold:.3f} → {new_threshold:.3f} "
                f"(null samples: {len(self._null_scores)}, target FAR: {self.target_far})"
            )
        self._calibrated_threshold = new_threshold

    def get_status(self) -> Dict:
        """Return calibration status for diagnostics."""
        return {
            "threshold": self.threshold,
            "calibrated": self._calibrated_threshold is not None,
            "null_samples": len(self._null_scores),
            "target_far": self.target_far,
        }
 
# Minimum time window to search for correlations (hours)
LOOKBACK_HOURS = 48

class SoftCorrelator:
    """
    Embedding-based correlator — ACTIVE.
    Finds semantically similar events across domains without hard-coded rules.

    Requires:
      sentence-transformers    (pip install sentence-transformers)
      qdrant-client            (pip install qdrant-client)
      Running Qdrant instance  (docker-compose service)
    """
    def __init__(self, ollama_client: OllamaClient):
        self._model = None  # Lazy load the embedding model
        # Recently-embedded claim digests, for suppressing identical repeats.
        self._claim_window = {}
        self._client = None  # Lazy load the Qdrant client
        self._enabled = False  # Set to True when ready to activate
        self._llm = ollama_client
        self._embed_semaphore = asyncio.Semaphore(5)  # Limit concurrent embeddings to avoid overload
        self._retry_task = None
        self._load_lock = asyncio.Lock()
        self._similarity_calibrator = ConformalSimilarityCalibrator()

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def get_status(self) -> Dict:
        return {
            "enabled": self._enabled,
            "model_loaded": self._model is not None,
            "client_connected": self._client is not None,
            "calibrator": self._similarity_calibrator.get_status(),
        }

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
                if not self._model:
                    self._model = await loop.run_in_executor(
                        None, 
                        lambda: sentence_transformers.SentenceTransformer("all-mpnet-base-v2")
                    )
                    logger.info("SentenceTransformer model loaded")

                qdrant_host = os.getenv("QDRANT_HOST", "qdrant")
                self._client = AsyncQdrantClient(host=qdrant_host, port=6333)
                # Only the collection this correlator actually uses.
                #
                # "sentinel_concepts" was created on every connect and appears
                # nowhere else in the tree -- no writer, no query. It has held
                # zero points for the life of the deployment with a `grey`
                # status, which is Qdrant reporting that no shard was ever
                # brought up for it: infrastructure provisioned for a capability
                # that was never built.
                for collection in [EVENT_COLLECTION]:
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
                logger.warning(f"Qdrant connection failed: {e}. Scheduling background retry loop.")
                if self._retry_task is None or self._retry_task.done():
                    self._retry_task = safe_create_task(self._connect_retry_loop(), name="soft-correlator-retry")

    async def _connect_retry_loop(self):
        """Background retry loop to connect to Qdrant if initial load fails."""
        backoff = 5
        while not self._enabled:
            await asyncio.sleep(backoff)
            logger.info(f"Retrying Qdrant connection (backoff={backoff}s)...")
            try:
                await self._load()
                if self._enabled:
                    logger.info("Qdrant connection established via background retry loop!")
                    break
            except Exception as e:
                logger.debug(f"Retry connect failed: {e}")
            backoff = min(backoff * 2, 60)
        
    def _describe(self, event: NormalizedEvent) -> str:
        """The text an event is embedded as.

        Shared by the single and batch paths on purpose: two spellings of this
        would produce vectors that are not comparable with each other, which is
        the same defect as the sentence frame this replaced.
        """
        parts = [
            event.headline or "",
            event.primary_entity.name if event.primary_entity else "",
            event.region or "",
        ]
        if event.primary_entity and event.primary_entity.flags:
            parts.append(" ".join(str(f) for f in event.primary_entity.flags))
        return ". ".join(p for p in parts if p).strip() or str(event.type.value)

    async def embed_event(self, event: NormalizedEvent) -> Optional[List[float]]:
        """Convert event to embedding vector for similarity search."""
        """Convert event to embedding vector without freezing the event loop."""
        if not self._enabled: return None
        
        async with self._embed_semaphore:
            # 1. Native Semantic Formatting
            # LLM translation is too slow for real-time telemetry streaming and causes Ollama timeouts.
            # We use native f-strings to build a dense semantic representation for the embedding model.
            # Content only. Every event previously carried the same sentence
            # frame -- "Event of type X involving Y in Z. Flags: [...].
            # Description: ..." -- and that shared scaffolding dominated the
            # embedding. Measured on this model, four deliberately unrelated
            # cross-domain events scored a mean cosine similarity of 0.453 with
            # the frame and 0.186 without it: the wrapper alone contributed
            # ~0.27 of apparent similarity between events that have nothing in
            # common. Against a 0.65 threshold and a corpus this large, every
            # event could find some neighbour above the bar, which is how 93.5%
            # of events came to fire a "correlation".
            natural_language_desc = self._describe(event)
            loop = asyncio.get_running_loop()
            try:
                encode_func = partial(self._model.encode, show_progress_bar=False)
                embedding_array = await loop.run_in_executor(None, encode_func, natural_language_desc)
                return embedding_array.tolist()  
            except Exception as e:
                logger.error(f"Error embedding event {event.event_id}: {e}")
                return None
        
    async def embed_events(self, events: List[NormalizedEvent]) -> Dict[str, List[float]]:
        """Encodes many events in one pass, keyed by event_id.

        The encoder is the dominant cost on the correlation hot path. Measured on
        this host with all-mpnet-base-v2: ~945ms per event called one at a time,
        ~454ms when batched -- a 2.08x difference for the same model and the same
        vectors. Per-event calls left the engine consuming 475 events/min against
        623/min of production.

        Returns only what encoded successfully; callers fall back to the
        single-event path for anything missing.
        """
        if not self._enabled or not events:
            return {}

        texts, ids = [], []
        for event in events:
            try:
                texts.append(self._describe(event))
                ids.append(str(event.event_id))
            except Exception:
                continue
        if not texts:
            return {}

        async with self._embed_semaphore:
            loop = asyncio.get_running_loop()
            try:
                encode = partial(self._model.encode, batch_size=32, show_progress_bar=False)
                vectors = await loop.run_in_executor(None, encode, texts)
                return {eid: vec.tolist() for eid, vec in zip(ids, vectors)}
            except Exception as e:
                logger.error(f"Batch embedding failed for {len(texts)} events: {e}")
                return {}

    # How long one claim suppresses re-embedding of an identical one.
    #
    # Matched to the semantic window: two identical sentences an hour apart are
    # the same claim for retrieval purposes, and beyond that the second is worth
    # re-stating because the situation around it has moved.
    _CLAIM_DEDUP_TTL_SEC = 3600
    # Digests are small; the cap bounds memory at a few megabytes.
    _CLAIM_WINDOW_MAX = 50_000

    async def _is_duplicate_claim(self, event) -> bool:
        """True when an identical claim was embedded recently.

        In-process rather than in Redis: this correlator holds no Redis client,
        and there is one correlation service, so a bounded local window is both
        sufficient and one less dependency on the embedding path.

        Keyed on the event type, the entity and the normalised headline, so two
        different assets producing coincidentally identical text remain distinct
        claims. Fails open -- a duplicate vector is a smaller harm than a
        missing one.
        """
        try:
            import hashlib, time as _t
            etype = event.type.value if hasattr(event.type, "value") else str(event.type)
            entity = str(event.primary_entity.id or "") if getattr(event, "primary_entity", None) else ""
            text = " ".join(str(x or "").strip().lower()
                            for x in (etype, entity, getattr(event, "headline", "")))
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]

            now = _t.monotonic()
            seen = self._claim_window.get(digest)
            if seen is not None and (now - seen) < self._CLAIM_DEDUP_TTL_SEC:
                return True
            self._claim_window[digest] = now

            # Bounded: evict the oldest whenever the window outgrows its cap, so
            # a long-running process cannot accumulate a digest per event.
            if len(self._claim_window) > self._CLAIM_WINDOW_MAX:
                cutoff = now - self._CLAIM_DEDUP_TTL_SEC
                self._claim_window = {
                    k: v for k, v in self._claim_window.items() if v > cutoff
                }
                if len(self._claim_window) > self._CLAIM_WINDOW_MAX:
                    for k in sorted(self._claim_window, key=self._claim_window.get)[
                        : len(self._claim_window) - self._CLAIM_WINDOW_MAX
                    ]:
                        self._claim_window.pop(k, None)
            return False
        except Exception:
            return False

    async def store(self, event: NormalizedEvent, embedding: List[float]):
        """Store event embedding in Qdrant with metadata for later retrieval.

        Routine position telemetry is never stored, so it can neither trigger a
        semantic match nor be offered as evidence for one -- see
        POSITION_TELEMETRY_TYPES.
        """
        # Safety check: Do nothing if the system isn't enabled or connected.
        if not self._enabled or not self._client:
            return
        event_type = event.type.value if hasattr(event.type, "value") else str(event.type)
        if event_type in POSITION_TELEMETRY_TYPES:
            return

        # One vector per distinct claim, not one per occurrence.
        #
        # Measured live on 4 September: 83 byte-identical "Transfer: $15 USDT"
        # headlines in thirty minutes, 65 of "$30 USDT", 62 of "$100 USDC" --
        # each stored as its own point among 413,702. So the nearest neighbours
        # of any crypto event were hundreds of near-copies of itself, which is
        # the mechanical reason "Cross-Domain Semantic Convergence" averaged
        # 1.01 distinct evidence types across 657 firings: the space had nothing
        # else nearby to find.
        #
        # The duplicate is not lost -- it is in Timescale, in Kafka and on the
        # wire. What it stops being is a separate point in a space whose whole
        # purpose is measuring distance between different things.
        if await self._is_duplicate_claim(event):
            return
        try:
            # 'Upsert' means "Insert if it doesn't exist, Update if it does".
            await self._client.upsert( 
                collection_name=EVENT_COLLECTION,
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
                        # Canonical. The cross-domain semantic check compares
                        # this field, so a prefix here meant "vessel" and
                        # "maritime" read as different domains.
                        "domain": resolve_event_domain(event),
                        "anomaly": event.anomaly_score,
                        # The five fields the semantic correlation path reads
                        # back and this payload has never written.
                        #
                        # `find_similar` returns these points, and the consumer
                        # asks each for headline, summary, entity_name,
                        # entity_id and source. None of the five was here, so
                        # every one fell back silently:
                        #
                        #   supp_headlines -> f"{type}: Unknown", so every
                        #     cluster's evidence read "flight_dark: Unknown"
                        #     three times -- which is what the scenario
                        #     generator was handed as the thing to reason about.
                        #   distinct_subjects -> {entity_name or entity_id or
                        #     idx}, which falls to enumerate's index and is
                        #     therefore ALWAYS len(kept), capped at 3. That is
                        #     why 1,201 of 1,243 semantic clusters carried the
                        #     identical confidence 0.7999999999999999: it is
                        #     0.35 + 0.15x3 and the 3 was never a measurement.
                        #   _independent_support -> reads `source`, absent, so
                        #     every cluster took the all-unknown branch.
                        #
                        # Truncated at write for the same reason the correlation
                        # window truncates: the consumer cuts them anyway, and
                        # storing them in full multiplies a large structure.
                        "headline": (event.headline or "")[:160] or None,
                        "summary": (getattr(event, "summary", None) or "")[:200] or None,
                        "entity_name": (
                            event.primary_entity.name
                            if event.primary_entity and event.primary_entity.name
                            else (event.primary_entity.id if event.primary_entity else None)
                        ),
                        "entity_id": event.primary_entity.id if event.primary_entity else None,
                        "source": getattr(event, "source", None),
                    },
                }],
            )
        except Exception as e:
            # Log the error. (Note: 'debug44' appears to be a typo for 'debug' or 'error' in the original code).
            logger.debug(f"Qdrant store failed for event{event.event_id}: {e}", exc_info=True)
    
    async def prune_expired_vectors(self) -> int:
        """Drop vector points whose event no longer exists. Returns the count.

        Nothing in this platform has ever deleted a vector. `events` drops after
        90 days and the index grew to 596,154 points against it, so every point
        older than the retention window describes an event that cannot be
        fetched: `/search/similar/{event_id}` returns a neighbour whose row is
        gone, and the semantic path cites evidence nobody can open.

        It also quietly falsifies a comment one method below, which excludes
        position telemetry from results "for as long as they take to age out".
        Nothing was ageing out.

        Pruning by the payload's own `occurred_at` rather than by comparing
        against Timescale: the two systems already agree on that field, one
        filtered delete replaces 596,154 existence checks, and an event deleted
        early for any other reason is not this sweep's business.
        """
        if not self._client:
            return 0
        try:
            from qdrant_client.http import models

            cutoff = datetime.now(timezone.utc) - timedelta(seconds=VECTOR_RETENTION_SEC)
            stale = models.Filter(
                must=[
                    models.FieldCondition(
                        key="occurred_at",
                        range=models.DatetimeRange(lt=cutoff),
                    )
                ]
            )
            # Counted before the delete so the log says what was removed. Qdrant
            # reports an operation status, not a row count.
            #
            # exact=True, and the difference is not a nicety. `occurred_at` is an
            # unindexed payload string, and the estimating counter answered
            # 298,097 -- almost exactly half the collection -- for every cutoff
            # it was given: 30 days, 60, 80 and 90 all returned the identical
            # number. The true figures are 731, 202, 167 and 167. The delete
            # itself would still have been right, because it selects on the
            # filter rather than on this number, but the line below would have
            # reported three orders of magnitude more than it removed, and the
            # zero-guard would have been reading noise.
            try:
                measured = await self._client.count(
                    collection_name=EVENT_COLLECTION, count_filter=stale, exact=True
                )
                n = int(getattr(measured, "count", 0) or 0)
            except Exception as _exc:
                swallowed("correlation.soft_correlator.prune_count", _exc, logger)
                n = 0

            _fired("correlation.vectors.swept")
            if n <= 0:
                return 0

            await self._client.delete(
                collection_name=EVENT_COLLECTION,
                points_selector=models.FilterSelector(filter=stale),
            )
            _fired("correlation.vectors.pruned", n)
            logger.info(
                "Pruned %d vector point(s) older than %d days; their events had "
                "already aged out of retention.",
                n, VECTOR_RETENTION_SEC // 86400,
            )
            return n
        except Exception as exc:
            # A failed sweep leaves orphans, which is the previous state rather
            # than a new fault.
            swallowed("correlation.soft_correlator.prune_expired_vectors", exc, logger)
            return 0

    async def find_similar(
            self, 
            embedding: List[float],
            exclude_domain: str,
            limit: int = 10,
            for_calibration: bool = False,
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
                collection_name=EVENT_COLLECTION,
                query_vector=embedding,
                # We fetch extra results because the filter step (must_not) might remove some.
                limit=limit + 20,   # fetch extra to filter by domain
                # Telemetry is excluded here as well as at write time. The
                # write gate stops new position fixes entering the index; this
                # stops the ones already in it from being returned as evidence
                # for as long as they take to age out.
                query_filter=models.Filter(
                    must_not=[
                        models.FieldCondition(
                            key="domain",
                            match=models.MatchValue(value=exclude_domain)
                        ),
                        models.FieldCondition(
                            key="type",
                            match=models.MatchAny(any=sorted(POSITION_TELEMETRY_TYPES)),
                        ),
                    ]
                ),
                # Unfiltered when the caller is building the null distribution:
                # sampling only pairs that already clear the cutoff censors the
                # sample from below by the very quantity being estimated.
                score_threshold=(None if for_calibration else self._similarity_calibrator.threshold),
            )
            # The score travels with the payload.
            #
            # This returned r.payload alone, discarding the cosine similarity
            # Qdrant puts on every ScoredPoint -- which is why the calibration
            # caller had no score to observe and passed the calibrator its own
            # threshold instead, making it a fixed point that never moved off
            # its 0.65 default while reporting itself calibrated.
            out = []
            for r in results[:limit]:
                payload = dict(r.payload or {})
                # The stored domain, normalised on the way out.
                #
                # `store_event` writes a canonical domain and says why: "the
                # cross-domain semantic check compares this field, so a prefix
                # here meant 'vessel' and 'maritime' read as different domains".
                # The writer was fixed and the points already in the collection
                # were never migrated -- and store_event only upserts on ingest,
                # so a third of them keep the old `.split("_")[0]` pseudo-domain
                # for good. Measured: 3,900 of 12,000 sampled points, 32.5%,
                # carrying `market`, `flight`, `vessel`, `bgp`, `headline`,
                # `earnings`, `equity`, `options`, `ransomware`.
                #
                # The consumer unions this with the trigger's canonical domain,
                # so a cluster of aircraft events resolved to
                # {"aviation", "flight"} and counted two. Over 24 hours, 812 of
                # 812 semantic clusters claimed to be cross-domain and every one
                # was a single domain under two names -- and that count gates the
                # tier competing for inference slots, feeds _rule_confidence's
                # cross-domain term, and reaches the model as fact, which is why
                # it wrote up dark aircraft as "Suspicion of Maritime Security
                # Breaches".
                #
                # Normalised here rather than by a migration because this is the
                # one place every reader passes through, and it corrects history
                # as well as anything written from now on.
                payload["domain"] = _canonical_domain_name(payload.get("domain"))
                payload["_similarity"] = float(getattr(r, "score", 0.0) or 0.0)
                out.append(payload)
            return out
        except Exception as e:
            logger.debug(f"Qdrant search failed: {e}")
            return []
        
    # ─── AGENTIC ONTOLOGY METHODS ──────────────────────────────────────────



        
    
 