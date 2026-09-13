"""Fields a collector fetches, against the readers that ask for them.

The same question as the order book and the discarded feeds, asked of the four
collectors that had not been looked at: ADS-B, social, prediction and filings --
and then of the frontend, which is the last reader in the chain and the only one
a person actually sees.

Most of what this found was fine, and saying so matters as much as the rest:
the ADS-B parser keeps fifteen of seventeen OpenSky fields and skips two it
names; the prediction enricher reads almost everything its collector emits; the
filings collector's `is_synthetic` flag is honoured by giving a seeded report a
source reliability of zero. Three things were not fine, and each is the same
shape -- a value known on one side of a boundary and asked for by a different
name on the other.
"""
import pathlib
import re

import pytest

from shared.models.events import SecurityData

ROOT = pathlib.Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── The cyber panel's KEV count ─────────────────────────────────────────────


def test_the_model_carries_the_three_fields_the_cyber_panel_reads():
    """`CyberIntelligencePanel` reads all three; the server sent none of them.

    The count is the one that matters:

        (cyberEvents || []).filter(e => e.security_data?.cisa_kev).length

    `SecurityData` had no `cisa_kev`, so that headline number was structurally
    always zero on a platform that ingests the CISA known-exploited-
    vulnerability catalogue -- and always zero reads as "nothing is being
    actively exploited right now", which is the most reassuring possible wrong
    answer.
    """
    fields = set(SecurityData.model_fields)
    for name in ("cisa_kev", "asn", "ransomware_group"):
        assert name in fields, f"the frontend reads security_data.{name}"


def test_the_panel_still_reads_them():
    """If the panel stops, the fields above are cargo rather than a repair."""
    panel = (ROOT / "frontend" / "src" / "components" / "CyberIntelligencePanel.tsx")
    text = panel.read_text(encoding="utf-8")
    assert "security_data?.cisa_kev" in text
    assert "sd.asn" in text


def test_a_kev_entry_says_it_is_one():
    source = (ROOT / "services" / "enrichment" / "enrichers" / "cyber.py").read_text(
        encoding="utf-8"
    )
    kev = source[source.index("async def _process_kev"):]
    kev = kev[: kev.index("async def _process_ransomware")]
    assert "cisa_kev=True" in kev, (
        "the KEV handler exists because the entry is in CISA's catalogue, and "
        "the event does not say so"
    )


def test_a_bgp_event_carries_the_as_number_it_names_in_its_headline():
    """The headline has always said "via AS{origin}"; the structured row did not.

    The panel renders `selectedEvent.security_data.ip_address ||
    selectedEvent.security_data.asn || 'N/A'` under "IP / ASN". The row was not
    blank, which is worse: `_process_bgp` passed the announced prefix as
    `ip_address`, so "IP / ASN" showed a CIDR block under a label naming
    neither, while the AS number -- the event's actual subject -- had no field
    at all and appeared only inside the prose headline.

    Both now go where they belong. `route_leak_prefix` is a field the frontend
    has always declared and the server never sent, and the panel's own tab is
    called "BGP ROUTE LEAKS".
    """
    source = (ROOT / "services" / "enrichment" / "enrichers" / "cyber.py").read_text(
        encoding="utf-8"
    )
    bgp = source[source.index("async def _process_bgp"):]
    bgp = bgp[: bgp.index("async def _process_exposure")]
    assert 'asn=f"AS{origin}"' in bgp
    assert "route_leak_prefix=prefix" in bgp
    assert "ip_address=prefix" not in bgp, (
        "a route prefix is not an IP address; passing it as one is what put a "
        "CIDR block in the panel's IP row"
    )


# ── The prediction market's resolution date ─────────────────────────────────


def test_the_kalshi_poller_keeps_the_expiry_it_already_reads():
    """Fetched, used to reject expired markets, and then dropped.

    `PredictionMarketData.resolution_date` is declared, the enricher reads
    `p.get("resolution_date")` at two call sites, and the frontend type
    declares it -- so it was null on every prediction event the platform has
    ever produced, by three readers asking for a value the collector had in
    hand and did not pass on.

    A contract at 67% resolving in three weeks is a tradeable claim. The same
    67% resolving in 2028 is noise. Nothing downstream could tell them apart.
    """
    collector = (ROOT / "services" / "collector-prediction" / "main.py").read_text(
        encoding="utf-8"
    )
    assert 'market.get("expiration_ts")' in collector, "the poller stopped reading it"
    assert '"resolution_date": market.get("expiration_ts")' in collector, (
        "the expiry is read and still not carried into the payload"
    )


def test_the_enricher_asks_for_it_by_that_name():
    enricher = (ROOT / "services" / "enrichment" / "enrichers" / "prediction.py").read_text(
        encoding="utf-8"
    )
    assert 'resolution_date=p.get("resolution_date")' in enricher


# ── The social venue ────────────────────────────────────────────────────────


async def test_a_social_post_carries_the_venue_it_came_from():
    """One subreddit shouting and three discussing are different findings.

    The collector emits `subreddit` or `channel` and `author` on every
    primary_social item; the enricher read neither, so the platform could not
    distinguish a single loud thread from independent interest in three places
    -- which is the entire content of a coordinated-push signal.
    """
    enricher = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(
        encoding="utf-8"
    )
    assert 'p.get("subreddit") or p.get("channel")' in enricher
    assert 'tags.append(f"venue:' in enricher
    assert 'tags.append(f"author:' in enricher


def test_independent_support_still_counts_feeds_rather_than_venues():
    """Recorded, not changed. The measurement, so the decision can be taken.

    `_independent_support` counts distinct `source` values, and every Reddit
    post carries `source="reddit"`. Ten posts across ten subreddits therefore
    count as one source plus a sub-linear tail -- about 3.3 units of evidence,
    not ten.

    Whether a venue should count as an independent source is a scoring
    decision, not a bug fix: a subreddit is not a collector, and `source` is
    what the scorecards, the freshness monitor and the unrouted-source counter
    all key on. The venue now reaches the event either way, so the decision can
    be made later on data rather than blocked by its absence.
    """
    from services.correlation.main import _independent_support

    one_venue = [{"source": "reddit"} for _ in range(10)]
    ten_feeds = [{"source": f"feed_{i}"} for i in range(10)]

    assert _independent_support(one_venue) < 4.0
    assert _independent_support(ten_feeds) == pytest.approx(10.0)
    # And this is the behaviour being recorded, not asserted as correct.
    assert _independent_support(one_venue) < _independent_support(ten_feeds)


# ── What was checked and found sound ───────────────────────────────────────


def test_the_adsb_parser_names_every_field_it_skips():
    """Fifteen of seventeen OpenSky fields, and the two omissions.

    Index 12 is `sensors`, a list of receiver ids, and the parser says so.
    Index 15 is `spi` -- the special-purpose/ident flag -- and it is skipped
    silently. Neither is a loss worth a change; the asymmetry in how they are
    documented is worth one line.
    """
    collector = (ROOT / "services" / "collector-adsb" / "main.py").read_text(encoding="utf-8")
    parser = collector[collector.index("def parse_state_vector"):]
    parser = parser[: parser.index("# ── ZONE POLLER")]
    assert "index 12 = sensors, skipped" in parser
    # Any field whose value comes from the state vector, however it is wrapped
    # -- `callsign` is `(state[1] or "").strip()`, not a bare index.
    kept = set(re.findall(r'"([a-z_0-9]+)":\s+[^\n]*state\[', parser))
    assert len(kept) == 15, f"the parser now keeps {len(kept)} fields; the note above is stale"


def test_position_source_is_collected_and_read_by_nothing():
    """ADS-B, ASTERIX or MLAT -- and nothing asks which.

    MLAT positions are derived from time-difference-of-arrival across ground
    receivers, with patchy coverage. An aircraft leaving MLAT coverage looks
    exactly like one switching its transponder off, and `flight_dark` is built
    on that distinction.

    Mitigated rather than blind: the gap detector scores a gap against its own
    region's empirical gap distribution, so a region with poor MLAT coverage
    has a longer expected gap. Within a region the two kinds are still pooled.
    Recorded, because partitioning by position source is a change to a detector
    that should be made on measured data rather than on this reasoning.
    """
    readers = []
    for path in list((ROOT / "services").rglob("*.py")) + list((ROOT / "shared").rglob("*.py")):
        if "__pycache__" in path.parts or path.name == "main.py" and "collector-adsb" in str(path):
            continue
        if "position_source" in path.read_text(encoding="utf-8", errors="replace"):
            readers.append(str(path.relative_to(ROOT)))
    assert not readers, (
        f"{readers} now read position_source. If a detector partitions on it, "
        f"replace this test with one asserting what it does."
    )


def test_epistemic_confidence_is_a_copy_of_reliability():
    """A field with a grander name than its contents.

    `"epistemic_confidence": reliability` -- the same variable, twice, on every
    social item. Not a lost signal: a duplicate carrying no information beyond
    the field beside it, which `reliability` already supplies and the enricher
    already reads. Recorded so it is not mistaken for a second measurement.
    """
    collector = (ROOT / "services" / "collector-social" / "main.py").read_text(encoding="utf-8")
    for line in collector.splitlines():
        if '"epistemic_confidence"' in line:
            assert line.strip().rstrip(",").endswith("reliability"), (
                f"epistemic_confidence now carries something of its own: {line.strip()!r}"
            )


def test_a_synthetic_filing_is_given_no_source_reliability():
    """Checked and sound. The flag is honoured where it counts."""
    collector = (ROOT / "services" / "collector-filings" / "main.py").read_text(encoding="utf-8")
    assert '"reliability": 0.99 if not report.is_synthetic else 0.0' in collector
