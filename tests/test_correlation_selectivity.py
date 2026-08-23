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
    """Old vectors carry the frame's inflation; mixing them is worse than either."""
    assert "EVENT_COLLECTION" in SRC
    name = re.search(r'EVENT_COLLECTION\s*=\s*"([^"]+)"', SRC).group(1)
    assert name != "sentinel_events", "corrected vectors would land among inflated ones"
    assert '"sentinel_events"' not in SRC, "a hardcoded reference to the old collection remains"


def test_a_similarity_threshold_is_actually_applied_at_query_time():
    """Without it, 'similar' means only 'nearest', and in a large corpus the
    nearest neighbour is always close."""
    assert "score_threshold=" in SRC, "the vector search returns neighbours regardless of distance"


def test_matches_are_restricted_to_other_domains():
    """A cross-domain correlation that matches within a domain is just a
    duplicate detector: thousands of near-identical position fixes would all
    'correlate' with each other."""
    assert "exclude_domain" in SRC
