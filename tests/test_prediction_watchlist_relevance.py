"""The relevance filter ran on every sweep and had no effect on what was polled.

`collector-prediction` filters Polymarket's volume-ranked top 100 by subject --
"kept 11 of 100 markets; 89 were off-domain (sports, entertainment, novelty)" --
and then polls sixty slugs read from a Redis *set* that nothing filtered. The
verdict was computed, logged, and discarded.

Measured against the live set with the collector's own filter: **501 of 609
stored slugs, 82%, were rejected by it** and were being polled anyway. What the
platform actually tracked was week-old football and esports fixtures and daily
temperature questions, while `iran-charges-hormuz-fees-by-september-30` -- a
market about the chokepoint this platform watches by name -- sat in the same set
and was polled only by luck.

Two mechanisms produced that. The enricher `sadd`ed every slug it saw with no
filter at all, so the collector's decision was undone from a different service.
And the cap took `watched_slugs[-60:]` of a set, which has no order, while
logging that it was "syncing the most recent" -- a property the data structure
cannot have.
"""
import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
COLLECTOR = (ROOT / "services" / "collector-prediction" / "main.py").read_text(encoding="utf-8")
ENRICHER = (ROOT / "services" / "enrichment" / "enrichers" / "prediction.py").read_text(encoding="utf-8")


def _relevance():
    """The collector's own filter, loaded without importing the service."""
    lines = COLLECTOR.splitlines(keepends=True)
    blk = "".join(lines[186:])
    ns = {"re": re}
    exec(compile(blk[: blk.index("async def stream_polymarket")], "pm", "exec"), ns)
    return ns["_is_relevant_market"]


def _worth_watching():
    i = ENRICHER.index("_OFF_DOMAIN_SLUG")
    j = ENRICHER.index("class ", i) if "class " in ENRICHER[i:] else len(ENRICHER)
    ns = {"re": re}
    exec(compile(ENRICHER[i:j], "en", "exec"), ns)
    return ns["_slug_is_worth_watching"]


SPORT_SLUGS = [
    "lol-hmble-cg-2026-09-03",
    "mlb-phi-ari-2026-09-02-nrfi",
    "cs2-mgc-k271-2026-09-02",
    "epl-new-bou-2026-09-05-halftime-result-draw",
    "christian-mccaffrey-1374pt5-rushing-yards-2026-27",
    "highest-temperature-in-manila-on-september-4-2026-26c",
    "where-will-it-rain-on-september-3-2026-seattle-wa",
]

INTEL_SLUGS = [
    "iran-charges-hormuz-fees-by-september-30",
    "will-wti-dip-to-90-in-september-2026-from-september-3",
    "will-emmanuel-macron-be-the-next-leader-out-before-2027-20260630194627364",
]


@pytest.mark.parametrize("slug", SPORT_SLUGS)
def test_the_enricher_stops_putting_sports_back(slug):
    assert not _worth_watching()(slug)


@pytest.mark.parametrize("slug", INTEL_SLUGS)
def test_the_enricher_keeps_what_the_platform_watches(slug):
    assert _worth_watching()(slug), (
        f"{slug} is the kind of market this platform exists to read; dropping it "
        "would trade one failure for its opposite"
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


def test_the_live_slugs_that_motivated_this_are_still_rejected():
    """The filter is loaded from source, so a vocabulary edit is caught here."""
    f = _relevance()
    assert not f({"slug": "lol-gal-tlnpir-2026-09-02-game4"})
    assert not f({"slug": "highest-temperature-in-paris-on-september-4-2026-34corhigher"})
    assert f({"slug": "iran-charges-hormuz-fees-by-september-30"})


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
