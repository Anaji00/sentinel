"""Which Qdrant collection holds the event vectors, said once.

The correlator wrote `sentinel_events_v2` and the search API read
`sentinel_events`. The second name has never existed: Qdrant returns 404 for
it, while `sentinel_events_v2` held 493,715 points at 768 dimensions when this
was measured. So `/search/status` reported

    {"available": false, "reason": "Qdrant unreachable or collection absent"}

against a populated index, and `/search/similar/{event_id}` raised a 503 for
every event on the platform. Semantic retrieval has been off for as long as the
versioned collection has existed, and said so in words that point at
infrastructure rather than at a name.

There was a test guarding exactly this -- `test_correlation_selectivity` asserts
the collection is not `sentinel_events` and that no hardcoded reference to the
old name survives -- and it scanned the writer only. The reader is in a
different file, so the one place the mismatch could occur was the one place
nobody looked.

Versioning is deliberate and stays. Vectors written before the embedding text
was corrected carry roughly 0.27 of similarity from a shared sentence frame;
comparing them against corrected ones is worse than either alone. A new
collection separates them without deleting anything.
"""

from __future__ import annotations

#: The collection every reader and writer of event vectors must use.
EVENT_COLLECTION = "sentinel_events_v2"

#: Concept vectors, written by the ontology path. Separate space, separate name.
CONCEPT_COLLECTION = "sentinel_concepts"

#: Dimensions of the vectors actually stored.
#:
#: Measured from Qdrant rather than taken from a docstring: the sovereignty
#: manifest describes "384-dimensional dense semantic embeddings", and the live
#: collection reports 768. A reader sizing a query vector from the manifest
#: would build one the index rejects.
EVENT_VECTOR_DIMS = 768

#: The name that never existed. Named so a check can assert against it.
RETIRED_EVENT_COLLECTION = "sentinel_events"

__all__ = [
    "EVENT_COLLECTION",
    "CONCEPT_COLLECTION",
    "EVENT_VECTOR_DIMS",
    "RETIRED_EVENT_COLLECTION",
]
