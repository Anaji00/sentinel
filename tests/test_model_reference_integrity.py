"""Guards the failure mode that took enrichment down in production.

`SecurityData.severity`, `.vector`, `.product` and `FlightData.altitude_ft`,
`.ground_speed_kts` were referenced when building headline/summary strings, but
none of those fields exist on those models. Nothing caught it: the code is
inside an `elif` that only runs for one domain, so every test stayed green while
the AttributeError escaped the batch loop and killed the enrichment service.
Cyber events were produced by the tens of thousands and never reached the
database, and the aviation branch was dead only because `flight_data` was never
populated -- populating it would have moved the outage rather than fixed it.

Two gates: the references must resolve statically, and each domain branch must
actually render.
"""
import importlib.util
import pathlib
import re
import sys
from datetime import datetime, timezone

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.models import (  # noqa: E402
    Entity, EntityType, EventType, NormalizedEvent,
    CryptoData, FinancialData, FlightData, PredictionMarketData,
    SecurityData, VesselData,
)


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


check_model_refs = _load("check_model_refs", "scripts/check_model_refs.py")


def test_no_references_to_nonexistent_model_fields_or_enum_members():
    """Static sweep: every enum member and payload field referenced must exist."""
    findings = []
    for d in ("services", "shared"):
        for f in (ROOT / d).rglob("*.py"):
            for ln, msg in sorted(set(check_model_refs.check(f, f.read_text(encoding="utf-8")))):
                findings.append(f"{f.relative_to(ROOT)}:{ln}: {msg}")
    assert not findings, "Unresolvable model/enum references:\n" + "\n".join(findings)


def _event(**payload) -> NormalizedEvent:
    return NormalizedEvent(
        event_id="evt-1", trace_id="trc-1", type=EventType.MARKET_ANOMALY,
        occurred_at=datetime.now(timezone.utc), source="test",
        primary_entity=Entity(id="E1", type=EntityType.UNKNOWN, name="Subject"),
        anomaly_score=0.5, **payload,
    )


DOMAIN_PAYLOADS = {
    "security_data": SecurityData(cve_id="CVE-2026-1234", cvss_score=9.8,
                                  exposure_type="Network", affected_org="Acme"),
    "flight_data": FlightData(icao24="a1b2c3", callsign="UAL42",
                              baro_altitude_m=10668.0, velocity_ms=250.0),
    "vessel_data": VesselData(mmsi="123456789", vessel_type="Tanker",
                              speed_knots=12.5, heading=90.0),
    "crypto_data": CryptoData(pair="BTC-USD", trade_type="spot", side="buy",
                              price=64000.0, size_tokens=1.5, funding_rate=0.0001),
    "financial_data": FinancialData(ticker="AAPL", underlying_price=195.0,
                                    premium_usd=2_500_000.0, volume=1000),
    "prediction_market_data": PredictionMarketData(
        market_id="MKT1", ticker="PRES24", question="Who wins?",
        outcome="YES", shares_traded=1000, price_usd=0.62),
}


@pytest.mark.parametrize("field,payload", sorted(DOMAIN_PAYLOADS.items()))
def test_display_text_renders_for_every_domain(field, payload):
    """Each domain branch must produce real text, not raise and not stay blank."""
    enrichment = _load("enrichment_main", "services/enrichment/main.py")
    e = _event(**{field: payload})
    e.headline, e.summary = "", ""
    enrichment.apply_display_text(e)          # must not raise
    assert len(e.headline) > 5, f"{field}: empty headline"
    assert len(e.summary) > 15, f"{field}: empty summary"
    assert "UNKNOWN" not in e.headline


def test_display_text_failure_cannot_stop_ingestion():
    """A formatting error must degrade cosmetics, never halt the batch."""
    enrichment = _load("enrichment_main", "services/enrichment/main.py")

    class Exploding:
        def __getattr__(self, _):
            raise AttributeError("simulated renamed field")

    e = _event()
    e.headline, e.summary = "", ""
    object.__setattr__(e, "security_data", Exploding())
    with pytest.raises(AttributeError):
        enrichment.apply_display_text(e)      # the raw formatter still raises...

    # ...but the call site swallows it, which is what keeps the pipeline alive.
    src = (ROOT / "services/enrichment/main.py").read_text(encoding="utf-8")
    guarded = re.search(
        r"try:\s+apply_display_text\(e\)\s+except Exception", src)
    assert guarded, "display formatting is not wrapped in try/except at the call site"


def test_cvss_severity_bands():
    """Severity is derived from the score because SecurityData has no label."""
    enrichment = _load("enrichment_main", "services/enrichment/main.py")
    f = enrichment._cvss_severity
    assert f(None) == "UNSCORED"
    assert f(9.8) == "CRITICAL"
    assert f(7.0) == "HIGH"
    assert f(4.0) == "MEDIUM"
    assert f(0.1) == "LOW"


def test_si_units_converted_for_aviation_display():
    """Models store SI (`*_m`, `*_ms`); aviation reads feet and knots."""
    enrichment = _load("enrichment_main", "services/enrichment/main.py")
    assert round(enrichment._m_to_ft(10668.0)) == 35000      # FL350
    assert round(enrichment._ms_to_kts(250.0)) == 486
    assert enrichment._m_to_ft(None) == 0.0
