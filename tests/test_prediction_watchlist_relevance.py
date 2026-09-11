"""The relevance filter ran on every sweep and had no effect on what was polled.

`collector-prediction` filters Polymarket's volume-ranked top 100 by subject --
"kept 11 of 100 markets; 89 were off-domain (sports, entertainment, novelty)" --
and then polls sixty slugs read from a Redis *set* that nothing filtered. The
verdict was computed, logged, and discarded.

Measured against the live set with the collector's own filter: **501 of 609
stored slugs, 82%, were rejected by it** and were being polled anyway.

Two mechanisms produced that. The enricher `sadd`ed every slug it saw with no
filter at all, so the collector's decision was undone from a different service.
And the cap took `watched_slugs[-60:]` of a set, which has no order, while
logging that it was "syncing the most recent".

**Then the repair introduced a third.** The enricher was given a
reject-known-bad rule -- a regex of league prefixes -- while the collector kept
pruning under a require-positive-match rule. A slug that passes one and fails
the other is added on every enriched event and removed on every sweep, forever.
Measured on both filters as they stood: `will-wti-dip-to-90-in-september-2026`
and a French leadership market were kept by the enricher and pruned by the
collector, so a crude-oil market this platform exists to read oscillated in and
out of the set and was never reliably polled.

One predicate now owns the set. These tests drive it directly rather than
slicing it out of two source files, and assert that neither service has grown a
second one.
"""
import ast
import pathlib

import pytest

from shared.utils.prediction_markets import is_relevant_market, slug_is_relevant

ROOT = pathlib.Path(__file__).resolve().parents[1]
COLLECTOR = (ROOT / "services" / "collector-prediction" / "main.py").read_text(encoding="utf-8")
ENRICHER = (ROOT / "services" / "enrichment" / "enrichers" / "prediction.py").read_text(encoding="utf-8")


SPORT_SLUGS = [
    "lol-hmble-cg-2026-09-03",
    "mlb-phi-ari-2026-09-02-nrfi",
    "cs2-mgc-k271-2026-09-02",
    "epl-new-bou-2026-09-05-halftime-result-draw",
    "christian-mccaffrey-1374pt5-rushing-yards-2026-27",
    "highest-temperature-in-manila-on-september-4-2026-26c",
    "where-will-it-rain-on-september-3-2026-seattle-wa",
    "lol-gal-tlnpir-2026-09-02-game4",
    "highest-temperature-in-paris-on-september-4-2026-34corhigher",
]

INTEL_SLUGS = [
    "iran-charges-hormuz-fees-by-september-30",
    "will-wti-dip-to-90-in-september-2026-from-september-3",
    "will-emmanuel-macron-be-the-next-leader-out-before-2027-20260630194627364",
]


@pytest.mark.parametrize("slug", SPORT_SLUGS)
def test_sports_and_weather_stay_out(slug):
    assert not slug_is_relevant(slug)


@pytest.mark.parametrize("slug", INTEL_SLUGS)
def test_the_platform_keeps_what_it_exists_to_read(slug):
    assert slug_is_relevant(slug), (
        f"{slug} is the kind of market this platform exists to read; dropping it "
        "would trade one failure for its opposite"
    )


@pytest.mark.parametrize("slug", INTEL_SLUGS + SPORT_SLUGS)
def test_the_two_services_cannot_disagree(slug):
    """The whole defect, as a property.

    The enricher adds with `slug_is_relevant(slug)`; the collector prunes with
    `is_relevant_market({"slug": x})`. They must be the same verdict for the
    same slug, or the set oscillates.
    """
    assert slug_is_relevant(slug) is is_relevant_market({"slug": slug})


def test_neither_service_carries_its_own_vocabulary():
    """A second filter over this set is the defect, not an implementation detail."""
    for name, src in (("collector", COLLECTOR), ("enricher", ENRICHER)):
        assert "RELEVANT_MARKET_TERMS = frozenset" not in src, name
        assert "_OFF_DOMAIN_SLUG = re.compile" not in src, name


def test_the_enricher_gates_its_sadd_on_the_shared_filter():
    i = ENRICHER.index('sadd("sentinel:polymarket:watched_slugs"')
    assert "slug_is_relevant(slug)" in ENRICHER[max(0, i - 1200):i], (
        "the enricher writes the set the collector prunes; it has to write it "
        "under the collector's rule"
    )


def test_the_collector_filters_what_it_reads_not_only_what_it_sweeps():
    assert "_is_relevant_market({\"slug\"" in COLLECTOR, (
        "the stored watch list is polled unfiltered, so the sweep's verdict is "
        "computed and discarded"
    )


def test_off_domain_slugs_are_pruned_rather_than_re_filtered_forever():
    """`sadd` never removes, so skipping them means re-filtering 501 every cycle."""
    assert "srem(redis_key" in COLLECTOR


def test_the_cap_no_longer_claims_a_recency_a_set_cannot_have():
    assert "syncing the most recent" not in COLLECTOR, (
        "watched_slugs comes from smembers, and a Redis set has no order -- "
        "[-60:] took an arbitrary tail and the log called it recency"
    )
    assert "watched_slugs[-MAX_WATCHED_SLUGS:]" not in COLLECTOR


def test_dynamic_slugs_is_bound_before_the_cap_reads_it():
    """The ordering now reads the sweep's output; an exception must not unbind it."""
    tree = ast.parse(COLLECTOR)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and n.name == "update_subscriptions"
    )
    src = ast.unparse(fn)
    assert src.index("dynamic_slugs = []") < src.index("for x in dynamic_slugs")
