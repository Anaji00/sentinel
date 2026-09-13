"""Feeds the platform pays for, collects, scores, and throws away on arrival.

The FIX order book was the first of these: a collector asking a venue for
everything and a parser keeping one field. Looking for the same shape elsewhere
found four more, each with the same signature -- a working producer, a log line
saying so, and a consumer with no branch for it.

The shape is hard to see from either end. From the collector the feed looks
alive: it polls, it thresholds, it logs "Cross-Venue Divergence Detected" at
INFO. From the rules the type looks unproduced, which reads as "not built yet"
rather than "built and discarded". Only putting the two lists side by side
shows it.

Each test below is the use case the feed exists for, expressed as the event a
desk would expect to see.
"""
import importlib.util
import pathlib
import sys
from datetime import datetime, timezone

import pytest

from shared.models import EventType, RawEvent

ROOT = pathlib.Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── Cross-venue funding divergence ──────────────────────────────────────────


def _divergence_payload(spread_bps=10.4):
    """Binance and Bybit disagreeing about the cost of holding BTC perp.

    Funding converges across venues because arbitrage makes it converge. A
    persistent spread says the arbitrage is not happening -- inventory,
    withdrawal friction, someone unable to move collateral -- which is a
    statement about the venues and leads both venue stress and liquidation
    cascades. It is also the one crypto signal that needs two exchanges to see
    at all, which is why the collector polls both.
    """
    return {
        "asset": "BTC",
        "trade_type": "CROSS_EXCHANGE_FUNDING_DIVERGENCE",
        "binance_funding_rate": 0.0009,
        "bybit_funding_rate": -0.00011,
        "funding_spread_bps": spread_bps,
        "price_spread_pct": 0.41,
        "binance_price": 64210.0,
        "bybit_price": 63947.0,
        "divergence_score": min(1.0, 0.40 + (spread_bps / 20.0)),
    }


async def test_a_cross_venue_funding_divergence_becomes_an_event():
    from services.enrichment.enrichers.crypto import CryptoEnricher

    enricher = CryptoEnricher.__new__(CryptoEnricher)
    raw = RawEvent(
        source="crypto_divergence",
        occurred_at=datetime.now(timezone.utc),
        raw_payload=_divergence_payload(),
    )
    event = await enricher._enrich_funding_divergence(raw, raw.raw_payload)

    assert event is not None, (
        "A 10bp funding spread between Binance and Bybit produced nothing. The "
        "collector polls both venues, thresholds at 3bp and logs the finding; "
        "until this branch existed every one of those reached the unrouted "
        "counter and stopped."
    )
    assert event.type is EventType.CRYPTO_PERP_FUNDING
    assert event.primary_entity.id == "BTC"
    assert 0.0 < event.anomaly_score <= 1.0
    assert "cross_venue_divergence" in event.tags


async def test_a_wider_divergence_scores_higher():
    """The score has to move with the spread, or it is a constant with a name."""
    from services.enrichment.enrichers.crypto import CryptoEnricher

    enricher = CryptoEnricher.__new__(CryptoEnricher)

    async def score(bps):
        raw = RawEvent(
            source="crypto_divergence",
            occurred_at=datetime.now(timezone.utc),
            raw_payload=_divergence_payload(bps),
        )
        return (await enricher._enrich_funding_divergence(raw, raw.raw_payload)).anomaly_score

    assert await score(4.0) < await score(12.0) < await score(24.0)


async def test_the_score_is_not_the_collectors_own_grade():
    """A detector grading its own output is the shape this audit keeps removing.

    The collector attaches `divergence_score = 0.40 + bps/20`, which starts at
    0.40 for a reading exactly on its own publishing threshold. Carrying it
    through as a field is useful; using it as the score would mean nothing
    downstream could ever disagree with the collector.
    """
    from services.enrichment.enrichers.crypto import CryptoEnricher

    enricher = CryptoEnricher.__new__(CryptoEnricher)
    payload = _divergence_payload(3.0)  # exactly on the threshold
    raw = RawEvent(source="crypto_divergence",
                   occurred_at=datetime.now(timezone.utc), raw_payload=payload)
    event = await enricher._enrich_funding_divergence(raw, payload)

    assert event.anomaly_score != pytest.approx(payload["divergence_score"]), (
        "the enricher is publishing the collector's own grade"
    )
    assert event.anomaly_score == pytest.approx(0.0, abs=1e-9), (
        "a reading exactly on the publishing threshold is the least remarkable "
        "one that can arrive, and scored 0.55 under the collector's formula"
    )


async def test_the_crypto_dispatch_has_a_branch_for_it():
    """The routing, not just the builder.

    A builder nothing calls is the defect this repair was for, one level up.
    """
    source = (ROOT / "services" / "enrichment" / "enrichers" / "crypto.py").read_text(
        encoding="utf-8"
    )
    dispatch = source[source.index("async def enrich_batch"):]
    dispatch = dispatch[: dispatch.index("results = await asyncio.gather")]
    assert "CROSS_EXCHANGE_FUNDING_DIVERGENCE" in dispatch
    assert "_enrich_funding_divergence" in dispatch


# ── Sentinel-1 radar over a chokepoint ──────────────────────────────────────


def _sar_payload(z_score=-3.2, with_baseline=True):
    """Radar over Bab el-Mandeb, showing far fewer metal returns than usual.

    The Red Sea in 2024: traffic diverted around the Cape weeks before
    container rates moved. Radar is the measurement AIS cannot be -- a vessel
    that has switched its transponder off still returns like metal -- so a
    chokepoint emptying on SAR while AIS looks normal is the difference between
    "ships are gone" and "ships stopped reporting".
    """
    payload = {
        "chokepoint": "Bab-el-Mandeb",
        "observed_on": "2026-09-09",
        "target_pixels": 118,
        "water_pixels": 410_000,
        "target_density": 0.00028780,
        "bbox": {"west": 43.20, "south": 12.40, "east": 43.60, "north": 12.80},
        "instrument": "sentinel-1-sar",
        "method": "VV backscatter > 0.0 dB",
        "is_vessel_count": False,
    }
    payload["traffic_assessment"] = {
        "chokepoint": "Bab-el-Mandeb", "source": "sar", "value": 0.00028780,
        "baseline_mean": 0.00071, "baseline_std": 0.00013, "observations": 41,
        "z_score": z_score, "direction": "quieter_than_usual",
        "observed_at": "2026-09-09T04:11:00Z",
    } if with_baseline else None
    return payload


def _maritime_enricher():
    from services.enrichment.enrichers.maritime import MaritimeEnricher

    return MaritimeEnricher.__new__(MaritimeEnricher)


async def test_a_chokepoint_emptying_on_radar_becomes_a_supply_chain_metric():
    enricher = _maritime_enricher()
    raw = RawEvent(source="copernicus_sentinel1",
                   occurred_at=datetime.now(timezone.utc),
                   raw_payload=_sar_payload())
    event = await enricher._chokepoint_reading(raw, raw.raw_payload)

    assert event is not None, "the SAR reading produced nothing"
    assert event.type is EventType.SUPPLY_CHAIN_METRIC, (
        "SUPPLY_CHAIN_METRIC was declared, named by "
        "rule_physical_disruption_repricing as its evidence, and constructed "
        "nowhere -- so a rule about a strait emptying could not learn that one had"
    )
    assert event.anomaly_score > 0.5, f"3.2 sigma scored {event.anomaly_score}"
    assert "quieter_than_usual" in event.tags


async def test_the_region_matches_what_a_vessel_in_the_same_water_gets():
    """The region join is the whole basis of the chokepoint rules.

    `rule_maritime_chokepoint_evasion` joins on `region: True`, and a vessel's
    region comes from `classify_region` on its position. The SAR collector
    images four chokepoints under its own labels. Three agree by luck; "Gulf of
    Guinea" resolves to "Nigerian Territorial", so a radar reading tagged with
    the collector's label could never join the vessels inside it.

    Asserted against every chokepoint the collector actually images, so adding
    a fifth cannot reintroduce it.
    """
    import importlib.util

    from shared.utils.regions import classify_region

    spec = importlib.util.spec_from_file_location(
        "sar_detection_under_test",
        ROOT / "services" / "collector-sar" / "sar_detection.py",
    )
    sar = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = sar
    spec.loader.exec_module(sar)

    enricher = _maritime_enricher()
    assert sar.BLIND_CHOKEPOINTS, "the collector images nothing"

    for name, bbox in sar.BLIND_CHOKEPOINTS.items():
        payload = _sar_payload()
        payload["chokepoint"] = name
        payload["bbox"] = bbox
        payload["traffic_assessment"]["chokepoint"] = name
        raw = RawEvent(source="copernicus_sentinel1",
                       occurred_at=datetime.now(timezone.utc), raw_payload=payload)
        event = await enricher._chokepoint_reading(raw, payload)

        lat = (float(bbox["south"]) + float(bbox["north"])) / 2.0
        lon = (float(bbox["west"]) + float(bbox["east"])) / 2.0
        expected = classify_region(lat, lon)

        assert event.region == expected, (
            f"{name}: radar reading tagged region {event.region!r}, while a "
            f"vessel at the same coordinates is classified {expected!r}. The "
            f"region join between them cannot match."
        )
        # And the identity stays the chokepoint, which is what a reader needs.
        assert event.primary_entity.name == name
        # Coordinates too, so a proximity join can reach it.
        assert event.latitude == pytest.approx(lat)
        assert event.longitude == pytest.approx(lon)


async def test_a_reading_with_no_bounding_box_still_names_its_chokepoint():
    """The honest fallback: nothing to classify, so say what the collector said."""
    enricher = _maritime_enricher()
    payload = _sar_payload()
    payload.pop("bbox")
    raw = RawEvent(source="copernicus_sentinel1",
                   occurred_at=datetime.now(timezone.utc), raw_payload=payload)
    event = await enricher._chokepoint_reading(raw, payload)
    assert event.region == payload["chokepoint"]
    assert event.latitude is None


async def test_a_chokepoint_with_no_baseline_is_not_reported_as_calm():
    """"Not measured" and "quiet" must not read alike.

    The collector is explicit about this -- it logs "recorded, no baseline yet
    to judge it against" -- and the same distinction has to survive into the
    event, or the first weeks of a new chokepoint look like the calmest weeks
    it has ever had.
    """
    enricher = _maritime_enricher()
    raw = RawEvent(source="copernicus_sentinel1",
                   occurred_at=datetime.now(timezone.utc),
                   raw_payload=_sar_payload(with_baseline=False))
    event = await enricher._chokepoint_reading(raw, raw.raw_payload)

    assert event.anomaly_score == 0.0
    assert "no_baseline" in event.tags
    assert "no baseline yet" in event.headline


async def test_a_busier_than_usual_strait_is_as_reportable_as_an_emptier_one():
    """A strait filling is not a strait emptying, and both are findings."""
    enricher = _maritime_enricher()

    async def score(z):
        payload = _sar_payload(z_score=z)
        payload["traffic_assessment"]["direction"] = (
            "busier_than_usual" if z > 0 else "quieter_than_usual"
        )
        raw = RawEvent(source="copernicus_sentinel1",
                       occurred_at=datetime.now(timezone.utc), raw_payload=payload)
        return (await enricher._chokepoint_reading(raw, payload)).anomaly_score

    assert await score(3.2) == await score(-3.2)


async def test_the_reading_does_not_claim_to_be_a_vessel_count():
    """The collector says so on every reading; the event must not upgrade it."""
    enricher = _maritime_enricher()
    raw = RawEvent(source="copernicus_sentinel1",
                   occurred_at=datetime.now(timezone.utc),
                   raw_payload=_sar_payload())
    event = await enricher._chokepoint_reading(raw, raw.raw_payload)
    assert "not a vessel count" in (event.summary or "")


async def test_a_maritime_message_matching_no_branch_is_counted():
    """It used to leave the loop with no counter, no log and no dead letter.

    That is the one failure mode this platform is least able to see, and it is
    how every Sentinel-1 reading was lost: the payload has no `MessageType`, so
    it matched neither AIS branch and simply was not appended to anything.
    """
    source = (ROOT / "services" / "enrichment" / "enrichers" / "maritime.py").read_text(
        encoding="utf-8"
    )
    dispatch = source[source.index("async def enrich_batch"):]
    dispatch = dispatch[: dispatch.index("results = []")]
    assert "enrichment.maritime.unrouted_message" in dispatch, (
        "a maritime message matching no branch still vanishes silently"
    )


# ── The tickers the regulatory collector already resolved ───────────────────


def test_the_regulatory_collector_resolves_tickers_the_enricher_now_reads():
    """An export control names equipment; the collector names the companies.

    `collector-macro/regulatory.py` reads the Federal Register and attaches
    `affected_tickers` to each rule it finds. Nothing read it, so a BIS rule on
    semiconductor equipment arrived with AMAT, LRCX and KLAC already resolved
    and the enricher discarded them to extract "Bureau of Industry and
    Security" from the headline with spaCy instead.
    """
    collector = (ROOT / "services" / "collector-macro" / "regulatory.py").read_text(
        encoding="utf-8"
    )
    assert '"affected_tickers"' in collector, "the collector stopped resolving tickers"

    enricher = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(
        encoding="utf-8"
    )
    assert 'p.get("affected_tickers")' in enricher, (
        "the field the collector resolves is read by nothing again"
    )
    # Ahead of the extracted ones: a resolved ticker is the strongest identity
    # a news event can carry, and the subject join compares identity tokens.
    assert "resolved + named_entities" in enricher


# ── The macro release published to a topic that discards it ─────────────────


def test_a_macro_release_is_published_once_to_the_topic_that_keeps_it():
    """`build_macro_release_event` returns a NormalizedEvent, not a raw payload.

    Sending it to RAW_TRADFI as well did not enrich it twice. The raw-topic
    consumer parses with `RawEvent(**raw_data)`, a NormalizedEvent has no
    `raw_payload`, so the duplicate arrived with an empty one, matched no source
    branch, and incremented `enrichment.tradfi.unrouted_source` -- the counter
    that exists to find feeds being thrown away, being fed by a deliberate
    duplicate at the rate of every macro release the platform sees.
    """
    calendar = (ROOT / "services" / "collector-macro" / "economic_calendar.py").read_text(
        encoding="utf-8"
    )
    publish = calendar[calendar.index("if self.producer:"):]
    publish = publish[: publish.index("events_published.append")]
    assert "Topics.ENRICHED_EVENTS" in publish
    assert "Topics.RAW_TRADFI" not in publish, (
        "the macro release is still being sent to a raw topic that drops it"
    )


def test_a_normalized_event_on_a_raw_topic_really_does_arrive_empty():
    """The mechanism, rather than the assertion that it happens.

    If RawEvent ever rejected this outright the duplicate would have been
    visible as an error instead of a silent drop, so the shape is worth pinning.
    """
    from shared.models import NormalizedEvent
    from shared.models.events import Entity, EntityType

    event = NormalizedEvent(
        type=EventType.MACRO_RELEASE,
        occurred_at=datetime.now(timezone.utc),
        source="finnhub_calendar",
        primary_entity=Entity(id="US-CPI", type=EntityType.INSTRUMENT),
        anomaly_score=0.8,
    )
    parsed = RawEvent(**event.model_dump())
    assert parsed.raw_payload == {}, (
        "a NormalizedEvent on a raw topic no longer parses to an empty payload; "
        "the drop this test describes would now look different"
    )
