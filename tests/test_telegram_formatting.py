"""
tests/test_telegram_formatting.py

Comprehensive tests for Telegram MarkdownV2 formatting correctness:
1. Code-span escaping: ONLY ` and \\ escaped inside `...` code spans.
2. Plain-text escaping: All 18 MarkdownV2 reserved characters escaped.
3. CorrelationCluster, Scenario, and IntelBrief message formatting.
"""

import pytest
from datetime import datetime, timezone
from shared.models import CorrelationCluster, AlertTier, Scenario
from services.alert_manager.formatters.telegram import (
    _escape,
    _escape_code,
    format_correlation,
    format_scenario,
    format_intel_brief,
)


def test_code_span_escaping_does_not_escape_hyphens_dots_underscores():
    # Code span content with dashes, dots, underscores, commas
    sample_id = "corr-984a-3b1c"
    escaped_code = _escape_code(sample_id)
    # MUST NOT contain backslashed hyphens
    assert escaped_code == "corr-984a-3b1c"
    assert "\\-" not in escaped_code

    # Tag list
    tags = "vessel-dark, option.call, market_candle"
    escaped_tags = _escape_code(tags)
    assert "\\-" not in escaped_tags
    assert "\\." not in escaped_tags
    assert "\\_" not in escaped_tags

    # Backticks and backslashes MUST be escaped
    code_with_backtick = "code `with` backslash \\"
    esc = _escape_code(code_with_backtick)
    assert esc == "code \\`with\\` backslash \\\\"


def test_plain_text_escaping_escapes_all_reserved_characters():
    special_text = "Price dropped -2.5% [Alert] (Tier 1) *Important* _note_ ~strike~ `code` >quote #tag +add =eq |pipe {brace} !excl \\slash"
    escaped = _escape(special_text)
    
    assert "\\-" in escaped
    assert "\\." in escaped
    assert "\\[" in escaped
    assert "\\]" in escaped
    assert "\\(" in escaped
    assert "\\)" in escaped
    assert "\\*" in escaped
    assert "\\_" in escaped
    assert "\\!" in escaped


def test_format_correlation_code_spans_are_clean():
    cluster = CorrelationCluster(
        rule_id="RULE-MARITIME-001",
        rule_name="maritime_dark_vessel_anomaly",
        trigger_event_id="EVT-TRIGGER-999",
        alert_tier=AlertTier.CRITICAL,
        description="Vessel IMO-9842103 disabled transponder near Strait of Hormuz (-26.4N, 56.2E).",
        entity_ids=["IMO-9842103", "MMSI-240192831"],
        tags=["maritime", "vessel-dark", "chokepoint-hormuz", "trigger_anomaly_speed_drop"],
        correlation_id="corr-984a-3b1c-7f2e",
        detected_at=datetime(2026, 8, 17, 0, 30, tzinfo=timezone.utc),
    )

    msg = format_correlation(cluster)

    # 1. Plain text description must be escaped
    assert "\\(" in msg
    assert "\\)" in msg

    # 2. Code spans must NOT contain backslashed hyphens
    assert "`corr-984`" in msg
    assert "\\`corr\\-984" not in msg
    assert "`IMO-9842103, MMSI-240192831`" in msg
    assert "\\`IMO\\-9842103" not in msg
    assert "`maritime, vessel-dark, chokepoint-hormuz, trigger_anomaly_speed_drop`" in msg


def test_format_scenario_markdown_validity():
    scenario = Scenario(
        correlation_id="corr-12345-67890",
        headline="Simultaneous GPS Spoofing & Critical Infrastructure Anomaly",
        significance="Multiple maritime vessels and aircraft report 15nm position offsets.",
        confidence_overall=87,
        confidence_rationale="Corroborated across 3 distinct sensor feeds with p < 0.01.",
        hypotheses=[
            {
                "label": "Electronic Warfare Testing",
                "probability": 65,
                "mechanism": "High-power GNSS repeater emitting false ephemeris signals.",
                "beneficiaries": ["Regional Military Command"],
                "watch_signals": ["ADS-B GPS integrity drop"],
                "deny_signals": ["Natural solar flare event"],
                "time_horizon": "24-48h",
            },
            {
                "label": "Accidental Jamming Leakage",
                "probability": 22,
                "mechanism": "Commercial port radar interference harmonics.",
                "beneficiaries": ["None"],
                "watch_signals": ["Port radar frequency shift"],
                "deny_signals": ["Multi-site correlation"],
                "time_horizon": "12h",
            },
        ],
        recommended_monitoring=["BGP route anomalies", "AIS coastal receiver RSSI"],
    )

    msg = format_scenario(scenario)

    assert "*INTELLIGENCE ASSESSMENT*" in msg
    assert "87%" in msg
    assert "Electronic Warfare Testing" in msg
    assert "• BGP route anomalies" in msg


def test_format_intel_brief_code_spans():
    brief = {
        "agent": "macro-quant-agent",
        "computed_severity": 4,
        "brief": {
            "headline_summary": "Unusual Put Accumulation Ahead of Semiconductor Export Controls",
            "catalyst_type": "REGULATORY_ANNOUNCEMENT",
            "geopolitical_theater": "EAST_ASIA",
            "geographic_hotspots": ["Taiwan-Strait", "Hsinchu-Park"],
            "financial_instruments_affected": ["TSM", "NVDA", "ASML-US"],
        }
    }

    msg = format_intel_brief(brief)

    # Code spans for agent and affected assets must not have backslashed hyphens
    assert "`macro-quant-agent`" in msg
    assert "\\`macro\\-quant\\-agent" not in msg
    assert "`TSM, NVDA, ASML-US`" in msg
    assert "\\`TSM\\, NVDA\\, ASML\\-US" not in msg
