"""Keeps cross-domain correlation selective enough to mean something.

The engine fired on 93.5% of events -- 4,967 correlations from 5,310 events --
which is not a signal, it is a second copy of the event stream. Every downstream
consumer drowned in it: the wargamer, the consensus engine and the reasoning
service were all growing lag while the correlations they were reading carried
almost no information.

The cause was in what got embedded. Every event was wrapped in the same sentence
frame, and that shared scaffolding dominated the vector. Measured on this model
with four deliberately unrelated cross-domain events: mean cosine similarity
0.453 with the frame, 0.186 without it. The wrapper alone contributed ~0.27 of
apparent similarity between events with nothing in common, so against a 0.65
threshold nearly anything could find a partner.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/correlation/soft_correlator.py").read_text(encoding="utf-8")


def test_the_embedding_text_carries_no_shared_sentence_frame():
    """Boilerplate identical across every event inflates all similarities."""
    for phrase in ("Event of type", "involving", "Flags:", "Description:"):
        assert phrase not in SRC or "natural_language_desc" not in SRC.split(phrase)[0][-400:], (
            f"the embedded text still contains the shared frame fragment {phrase!r}"
        )


def test_embedding_is_built_from_event_content():
    """The text fed to the model must be the event's own words.

    Exercised through the function rather than by reading the source, so it
    keeps holding when the code moves.
    """
    from datetime import datetime, timezone
    from shared.models.events import Entity, EntityType, EventType, NormalizedEvent
    from services.correlation.soft_correlator import SoftCorrelator

    event = NormalizedEvent(
        event_id="e1", trace_id="t1", type=EventType.MARKET_ANOMALY,
        occurred_at=datetime.now(timezone.utc), source="test",
        primary_entity=Entity(id="NVDA", type=EntityType.COMPANY, name="NVIDIA"),
        region="GLOBAL", headline="Semiconductor export controls tighten",
    )
    text = SoftCorrelator._describe(SoftCorrelator.__new__(SoftCorrelator), event)
    assert "Semiconductor export controls tighten" in text
    assert "NVIDIA" in text
    for frame in ("Event of type", "Flags:", "Description:"):
        assert frame not in text, f"the shared sentence frame {frame!r} is back"


def test_single_and_batch_paths_embed_identical_text():
    """Two spellings would produce vectors that cannot be compared with each
    other -- the same defect as the sentence frame this replaced."""
    single = SRC[SRC.index("async def embed_event("):SRC.index("async def embed_events(")]
    batch = SRC[SRC.index("async def embed_events("):]
    assert "self._describe(event)" in single
    assert "self._describe(event)" in batch


def test_vectors_from_the_old_and_new_schemes_are_kept_apart():
    """Old vectors carry the frame's inflation; mixing them is worse than either.

    This scanned the writer only, and the name it guards is one both sides of
    the index have to agree on. The search API kept its own copy, reading
    `sentinel_events` -- a collection Qdrant 404s -- while the correlator wrote
    `sentinel_events_v2`, which held 493,715 points when this was measured. So
    `/search/status` reported the index unavailable and `/search/similar`
    raised 503 for every event, and this test passed throughout, because the
    one place the two could disagree was the one place it did not look.

    The name now lives in `shared.utils.vector_index` and both sides import it.
    """
    from shared.utils.vector_index import EVENT_COLLECTION, RETIRED_EVENT_COLLECTION

    assert "EVENT_COLLECTION" in SRC
    assert EVENT_COLLECTION != RETIRED_EVENT_COLLECTION, (
        "corrected vectors would land among inflated ones"
    )
    # Every module that names a collection, not just this one.
    readers = [
        ROOT / "services" / "correlation" / "soft_correlator.py",
        ROOT / "services" / "api_gateway" / "routes" / "search.py",
    ]
    for path in readers:
        text = path.read_text(encoding="utf-8")
        text = re.sub(r"^\s*#.*$", "", text, flags=re.M)
        text = re.sub(r'""".*?"""', "", text, flags=re.S)
        assert f'"{RETIRED_EVENT_COLLECTION}"' not in text, (
            f"{path.name} still names the retired collection; the reader and the "
            f"writer must agree, and a hardcoded copy is how they stopped agreeing"
        )


def test_a_similarity_threshold_is_actually_applied_at_query_time():
    """Without it, 'similar' means only 'nearest', and in a large corpus the
    nearest neighbour is always close."""
    assert "score_threshold=" in SRC, "the vector search returns neighbours regardless of distance"


def test_matches_are_restricted_to_other_domains():
    """A cross-domain correlation that matches within a domain is just a
    duplicate detector: thousands of near-identical position fixes would all
    'correlate' with each other."""
    assert "exclude_domain" in SRC


def test_the_gateway_can_actually_reach_the_vector_index():
    """The collection name was only half of why semantic search was dead.

    `services/api_gateway/routes/search.py` imported `qdrant_client`, which is
    declared in `requirements-ml.txt` and installed into `ml.Dockerfile`. The
    gateway builds from the root `Dockerfile` and `requirements-base.txt`, which
    has never carried it -- so the lazy import raised ImportError on every
    request, the helper returned None, and the endpoint answered

        {"available": false, "reason": "Qdrant unreachable or collection absent"}

    naming neither of the two things that were wrong. Qdrant speaks HTTP and
    `aiohttp` is already a base dependency, so the four operations are four
    requests rather than a vector client in twenty service images.
    """
    search = (ROOT / "services" / "api_gateway" / "routes" / "search.py").read_text(
        encoding="utf-8"
    )
    base_reqs = (ROOT / "requirements-base.txt").read_text(encoding="utf-8")

    # Comments *and* docstrings: the module explains why it stopped importing
    # `qdrant_client`, and naming the package in that explanation is not the
    # same as importing it. A check that cannot tell those apart is the trap
    # this audit has now walked into seven times.
    code = re.sub(r'"""(?:.|\n)*?"""', "", search)
    code = re.sub(r"^\s*#.*$", "", code, flags=re.M)

    if "qdrant_client" in code:
        assert "qdrant-client" in base_reqs, (
            "search.py imports qdrant_client and the gateway's own requirements "
            "do not install it; the import fails at runtime and every semantic "
            "query reports the index unavailable"
        )


def test_an_unavailable_index_says_which_of_the_reasons_it_is():
    """Three causes shared one sentence.

    No client library, no network route and no such collection are different
    problems with different remedies -- restart a container, fix a network, run
    an indexer -- and the endpoint answered all three with the same guess.
    """
    search = (ROOT / "services" / "api_gateway" / "routes" / "search.py").read_text(
        encoding="utf-8"
    )
    assert "VectorIndexUnavailable" in search, "failures must carry their specific reason"
    assert "does not exist" in search, "a missing collection must say so"
    assert "unreachable at" in search, "an unreachable index must say where it looked"
