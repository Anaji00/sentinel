"""
tests/test_semantic_tier_grading.py

The correlation layer was filing "QQQ appeared in three events" at the highest
severity it has.

Measured live over three hours during market hours:

    Cross-Domain Semantic Convergence  tier 4   1,367 clusters   1.0 entities
    Cross-Domain Semantic Convergence  tier 5   1,344 clusters   1.0 entities
    Equity Block & Options Convergence tier 3   2,464 clusters   5.9 entities

    tier 5 | {QQQ} | Embedding resemblance: 'QQQ' matches 3 retained
                     event(s) across 3 distinct subject(s)

2,711 clusters at the two top tiers, each citing a single entity, against a rule
that cites ten supporting events and six entities and grades itself lower. QQQ
is one of fifty subscribed symbols and appearing in three events is the most
ordinary thing in the system.

The rule's own description already said the right thing -- "Textual similarity
only -- shared wording or a shared place name is not itself a relationship" --
while `effective_score >= 4.0` graded it CRITICAL anyway. Three distinct
subjects and a centrality of 1.33 was enough.

This is not only mislabelling. Every cluster is a candidate for an inference
slot on a host that affords a few dozen an hour, so an over-graded rule crowds
out the correlations that earned their tier.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _tier(distinct_subjects: float, centrality: float) -> str:
    from services.correlation.main import SEMANTIC_INTELLIGENCE_SCORE

    return "INTELLIGENCE" if distinct_subjects * centrality >= SEMANTIC_INTELLIGENCE_SCORE else "ALERT"


def test_a_resemblance_can_never_be_critical():
    """A sentence encoder reporting that two headlines are worded alike cannot
    establish the relationship CRITICAL is supposed to mean."""
    for subjects in (1, 3, 10, 50):
        for centrality in (1.0, 1.67, 3.0):
            assert _tier(subjects, centrality) != "CRITICAL"


def test_the_live_case_no_longer_reaches_the_top_tiers():
    """'QQQ matches 3 retained events across 3 distinct subjects', which was
    filed at tier 5."""
    assert _tier(3, 1.33) == "ALERT"
    assert _tier(3, 1.0) == "ALERT"


def test_genuine_breadth_still_earns_intelligence():
    """The rule must not be neutered -- a resemblance spanning many subjects on
    a central entity is still worth a look."""
    assert _tier(6, 1.0) == "INTELLIGENCE"
    assert _tier(4, 2.0) == "INTELLIGENCE"


def test_the_bar_is_above_what_used_to_reach_critical():
    from services.correlation.main import SEMANTIC_INTELLIGENCE_SCORE

    assert SEMANTIC_INTELLIGENCE_SCORE > 4.0, "4.0 is what graded QQQ critical"


def test_the_tier_and_the_description_now_agree():
    """The description has said 'not itself a relationship' for some time while
    the tier said CRITICAL. A reader believes whichever they read first."""
    source = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    block = source.split("effective_score = distinct_subjects * centrality_mult")[1][:2000]
    assert "AlertTier.CRITICAL" not in block
    assert "itself a relationship" in source, "the honest description is the half that was already right"
