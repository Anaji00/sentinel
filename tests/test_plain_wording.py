"""Words a person reads: alerts, ship types, track records. Each replaced something that read as machine output."""
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from services.alert_manager.formatters.telegram import format_correlation, format_intel_brief
from shared.utils.agent_conclusions import scorecard_scope
from shared.utils.regions import decode_vessel_type, vessel_type_label

ROOT = Path(__file__).resolve().parents[1]
EMOJI = re.compile("[\U0001F300-\U0001FAFF☀-➿₿]")


def test_ship_types_read_as_words() -> None:
    assert vessel_type_label("CargoVessel") == "Cargo ship"
    assert vessel_type_label("SpecialCraft") == "Special craft"
    assert vessel_type_label("Tanker") == "Tanker"
    assert vessel_type_label("Unknown(16)") == "Ship"
    assert vessel_type_label(None) == "Ship"


def test_the_stored_category_is_unchanged_because_other_logic_reads_it() -> None:
    assert decode_vessel_type(75) == "CargoVessel"
    assert decode_vessel_type(16) == "Unknown(16)"


def test_a_track_record_says_what_it_covers() -> None:
    assert scorecard_scope("sentinel:agents:scorecard:radar_agent") == ""
    assert scorecard_scope("sentinel:agents:scorecard:radar_agent:momentum:bull") == "momentum / bull"
    assert scorecard_scope("sentinel:agents:scorecard:radar_agent:momentum:any") == "momentum"
    assert scorecard_scope("sentinel:agents:scorecard:radar_agent:any:bull") == "bull"


def test_telegram_alerts_carry_no_emoji() -> None:
    cluster = SimpleNamespace(
        alert_tier=SimpleNamespace(name="ALERT"), tags=["maritime", "trigger_anomaly_0.9"], rule_name="Rule",
        description="Words.", detected_at=datetime(2026, 11, 4, tzinfo=timezone.utc), correlation_id="abcdef123456",
        entity_ids=["X"],
    )
    message = format_correlation(cluster)
    assert not EMOJI.search(message), message
    assert "ALERT" in message, "the tier is what the emoji stood for"
    brief = format_intel_brief({"agent": "a", "computed_severity": 4, "brief": {
        "headline_summary": "H", "geographic_hotspots": ["Y"], "financial_instruments_affected": ["Z"],
    }})
    assert not EMOJI.search(brief), brief
    assert "Severity: 4/5" in brief


def test_no_enricher_writes_a_shouting_label_again() -> None:
    for rel in ("services/enrichment/enrichers/crypto.py", "services/enrichment/enrichers/tradfi.py"):
        src = (ROOT / rel).read_text(encoding="utf-8")
        for label in ("FUNDING RATE EXTREME", "OPEN INTEREST {", "EARNINGS UPCOMING", "SEC FILING:", "13F REPORT:"):
            assert label not in src, f"{rel}: {label}"
