"""One domain taxonomy, checked exhaustively.

Four components derived an event's domain independently and disagreed:

  * the correlation engine took `event.type.value.split("_")[0]`, minting a
    pseudo-domain per prefix -- 29 of them against 8 real domains -- so the
    Hawkes tracker accumulated excitation history under names nothing else
    used, and five of the eight canonical domains never registered any;
  * the anomaly scorer ran an ordered substring scan whose tests were not
    mutually exclusive, so `dark_pool` and `flight_dark` both matched "dark"
    and became maritime, `market_candle` matched "candle" and became crypto,
    and every cyber event fell through to a `return "tradfi"` default;
  * the enrichment service split the *collector name* -- yielding "alpaca",
    "ripe", "finnhub" -- and compared two of those to decide whether a
    correlation crossed a domain boundary;
  * the event store, the vector payload and the Hawkes history loader each
    reconstructed it from a prefix again on the way back out.

The mapping is now a single table and this test is what keeps it that way. The
exhaustiveness check is the important one: a new EventType added without a
domain fails here rather than landing silently in whatever the default was.
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.events import (  # noqa: E402
    AMBIGUOUS_EVENT_TYPES,
    CROSS_DOMAIN_MEMBERS,
    EVENT_TYPE_TO_DOMAIN,
    Domain,
    EventType,
    event_domain,
)


def test_every_event_type_has_a_domain():
    """The check that makes a missing mapping a build failure."""
    missing = set(EventType) - set(EVENT_TYPE_TO_DOMAIN)
    assert not missing, (
        "EventType members with no domain: "
        + ", ".join(sorted(m.name for m in missing))
        + ". Add them to EVENT_TYPE_TO_DOMAIN rather than relying on a default."
    )


def test_the_mapping_invents_no_event_types():
    assert not set(EVENT_TYPE_TO_DOMAIN) - set(EventType)


def test_every_mapped_value_is_a_real_domain():
    for member, domain in EVENT_TYPE_TO_DOMAIN.items():
        assert isinstance(domain, Domain), f"{member.name} maps to {domain!r}"


@pytest.mark.parametrize(
    "event_type,expected",
    [
        # Each of these was measured going somewhere else before the fix.
        ("dark_pool", "tradfi"),        # matched "dark" -> maritime
        ("flight_dark", "aviation"),    # matched "dark" -> maritime
        ("market_candle", "tradfi"),    # matched "candle" -> crypto
        ("ransomware", "cyber"),        # matched nothing -> tradfi default
        ("breach_detected", "cyber"),   # matched nothing -> tradfi default
        # And these were already right, so the fix must not move them.
        ("vessel_dark", "maritime"),
        ("bgp_anomaly", "cyber"),
        ("crypto_transfer", "crypto"),
        ("macro_release", "macro"),
        ("headline", "news"),
        ("prediction_market_trade", "prediction"),
    ],
)
def test_historically_misrouted_types_land_in_the_right_domain(event_type, expected):
    assert event_domain(event_type) == expected


def test_platform_self_reports_are_not_a_domain_of_the_world():
    """A collector outage must not enter the cross-excitation matrix.

    INFRASTRUCTURE_DEGRADED is the platform saying its own AIS feed went quiet.
    Recorded as a maritime event it would teach the Hawkes model that our
    outages cause maritime activity.
    """
    assert event_domain("infrastructure_degraded") == Domain.OTHER.value
    assert event_domain("custom") == Domain.OTHER.value
    assert Domain.OTHER.value not in CROSS_DOMAIN_MEMBERS


def test_unknown_input_is_other_rather_than_the_busiest_domain():
    """Defaulting an unrecognised type into tradfi is how ransomware
    disclosures came to be counted as financial events."""
    for junk in ("", None, "not_an_event_type", "freight_rate"):
        assert event_domain(junk) == Domain.OTHER.value


def test_cross_domain_members_are_the_eight_canonical_domains():
    assert len(CROSS_DOMAIN_MEMBERS) == 8
    assert set(CROSS_DOMAIN_MEMBERS) == {d.value for d in Domain if d is not Domain.OTHER}


def test_no_component_derives_a_domain_by_splitting_an_event_type():
    """The regression that would reintroduce the pseudo-domains.

    Scoped to the assignment shape that caused it -- a `domain` being set from
    a split -- so that unrelated uses of split() are not swept up.
    """
    offenders = []
    pattern = re.compile(r'domain[^\n=]*=\s*[^\n]*\.split\("_"\)\[0\]')
    for path in list((ROOT / "services").rglob("*.py")) + list((ROOT / "shared").rglob("*.py")):
        if "__pycache__" in str(path):
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if pattern.search(line):
                offenders.append(f"{path.relative_to(ROOT)}:{n}")
    assert not offenders, (
        "domain derived by splitting an event type at: " + ", ".join(offenders)
        + ". Use shared.models.events.event_domain instead."
    )

# ── the one type two domains legitimately emit ────────────────────────────

def test_market_anomaly_is_the_only_ambiguous_type():
    """Established by asking which enricher emits which type, not by inspection.

    If a second enricher starts emitting an existing type, this fails and the
    payload resolver needs to learn about it -- which is the whole point, since
    a type-level table silently misfiles the newcomer otherwise.
    """
    import collections
    import re as _re

    owner = {"crypto.py": "crypto", "tradfi.py": "tradfi", "maritime.py": "maritime",
             "aviation.py": "aviation", "cyber.py": "cyber", "news.py": "news",
             "macro.py": "macro", "prediction.py": "prediction"}
    emit = collections.defaultdict(set)
    for f in (ROOT / "services" / "enrichment" / "enrichers").glob("*.py"):
        dom = owner.get(f.name)
        if not dom:
            continue
        for m in _re.finditer(r"type=EventType\.([A-Z_]+)", f.read_text(encoding="utf-8")):
            emit[m.group(1)].add(dom)

    multi = {t for t, d in emit.items() if len(d) > 1}
    assert multi == {"MARKET_ANOMALY"}, (
        f"event types emitted by more than one domain: {sorted(multi)}. "
        "Add them to AMBIGUOUS_EVENT_TYPES and give the resolver a payload to "
        "tell them apart."
    )
    assert {t.name for t in AMBIGUOUS_EVENT_TYPES} == multi


def test_a_crypto_candle_is_crypto_and_an_equity_candle_is_tradfi():
    """Both emit MARKET_ANOMALY, so only the payload separates them."""
    from datetime import datetime, timezone

    from shared.models.events import (
        CryptoData, Entity, EntityType, FinancialData, NormalizedEvent,
        resolve_event_domain,
    )

    def _event(**payload):
        return NormalizedEvent(
            type=EventType.MARKET_ANOMALY,
            occurred_at=datetime.now(timezone.utc),
            source="test",
            primary_entity=Entity(id="X", type=EntityType.INSTRUMENT, name="X"),
            headline="h",
            **payload,
        )

    crypto = _event(crypto_data=CryptoData(
        pair="BTCUSDT", trade_type="OHLCV_5M_BAR", side="buy", price=1.0, size_tokens=1.0))
    equity = _event(financial_data=FinancialData(ticker="AAPL", instrument_type="equity"))

    assert resolve_event_domain(crypto) == "crypto"
    assert resolve_event_domain(equity) == "tradfi"
    # No payload falls back to the type-level answer rather than guessing.
    assert resolve_event_domain(_event()) == event_domain("market_anomaly")


def test_market_candle_is_declared_but_never_emitted():
    """Kept as a note, not an assertion about correctness.

    Both candle paths emit MARKET_ANOMALY; MARKET_CANDLE is declared, named in
    one list, and produced by no enricher. Recorded so that a future reader
    looking for where candles are classified is not misled by the name.
    """
    import re as _re
    emitted = set()
    for f in (ROOT / "services").rglob("*.py"):
        if "__pycache__" in str(f):
            continue
        emitted |= set(_re.findall(r"type=EventType\.([A-Z_]+)", f.read_text(encoding="utf-8")))
    assert "MARKET_CANDLE" not in emitted
