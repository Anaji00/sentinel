"""
services/alert-manager/formatters/telegram.py

Formats a CorrelationCluster, Scenario, or IntelBrief into a Telegram message.
Uses MarkdownV2 formatting.
"""

import re
from typing import Optional
from shared.models import CorrelationCluster, Scenario, AlertTier

TIER_EMOJI = {
    AlertTier.WATCH:        "👁",
    AlertTier.ALERT:        "⚠️",
    AlertTier.INTELLIGENCE: "🔴",
    AlertTier.CRITICAL:     "🔥",
}

def _escape(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!\\"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))

def format_correlation(cluster: CorrelationCluster) -> str:
    """Format a CorrelationCluster as a Telegram alert message."""
    emoji = TIER_EMOJI.get(cluster.alert_tier, "❓")
    tier = cluster.alert_tier.name

    domain_emoji = ""
    tags_lower = [t.lower() for t in cluster.tags]
    if any(t in tags_lower for t in ["maritime", "vessel", "vessel_dark", "vessel_sts", "vessel_spoof"]):
        domain_emoji = "🚢 "
    elif any(t in tags_lower for t in ["aviation", "flight", "aircraft", "flight_dark", "flight_anomaly"]):
        domain_emoji = "✈️ "
    elif any(t in tags_lower for t in ["tradfi", "equity", "options", "financial", "equity_block", "market_candle"]):
        domain_emoji = "💰 "
    elif any(t in tags_lower for t in ["cyber", "ransomware", "bgp", "bgp_anomaly", "breach_detected"]):
        domain_emoji = "🔐 "
    elif any(t in tags_lower for t in ["crypto", "wallet", "crypto_trade", "crypto_liquidation"]):
        domain_emoji = "₿ "

    anomaly_str = ""
    for tag in cluster.tags:
        if tag.startswith("trigger_anomaly_"):
            anomaly_str = tag.replace("trigger_anomaly_", "")
            break

    anomaly_suffix = f" \\(Anomaly: {anomaly_str}\\)" if anomaly_str else ""

    lines = [
        f"{emoji} {domain_emoji}*\\[{_escape(tier)}\\] {_escape(cluster.rule_name)}*{anomaly_suffix}",
        "",
        _escape(cluster.description),
        "",
        f"🏷 Tags: `{_escape(', '.join(cluster.tags[:6]))}`",
        f"📅 {_escape(cluster.detected_at.strftime('%Y-%m-%d %H:%M UTC'))}",
        f"🆔 `{_escape(cluster.correlation_id[:8])}`",
    ]

    if cluster.entity_ids:
        lines.append(f"👥 Entities: `{_escape(', '.join(cluster.entity_ids[:3]))}`")

    return "\n".join(lines)

def format_scenario(scenario: Scenario) -> str:
    """Format a Scenario as a Telegram intelligence briefing."""
    lines = [
        f"💡 *INTELLIGENCE ASSESSMENT*",
        "",
        f"*{_escape(scenario.headline)}*",
        "",
        _escape(scenario.significance),
        "",
        f"*Confidence: {scenario.confidence_overall}%*",
        _escape(scenario.confidence_rationale),
        "",
    ]

    for i, h in enumerate(scenario.hypotheses[:3], 1):
        label       = h.get("label", f"Scenario {i}")
        probability = h.get("probability", "?")
        mechanism   = h.get("mechanism", "")
        # Truncate mechanism to 150 chars to avoid 4096-char Telegram limit overflow
        truncated_mechanism = mechanism[:150] + "..." if len(mechanism) > 150 else mechanism
        lines += [
            f"*{i}\\. {_escape(label)} \\({probability}%\\)*",
            _escape(truncated_mechanism),
            "",
        ]

    if scenario.recommended_monitoring:
        lines.append("*Monitor:*")
        for item in scenario.recommended_monitoring[:5]:
            lines.append(f"• {_escape(item)}")

    return "\n".join(lines)

def format_intel_brief(brief: dict) -> str:
    """Format an IntelBrief payload as a Telegram alert message."""
    b = brief.get("brief", {})
    headline = b.get("headline_summary", "Intel Brief")
    severity = brief.get("computed_severity", b.get("severity", 3))
    sev_emoji = "🚨" if severity >= 3 else "ℹ️"
    agent_name = brief.get("agent", "news_intel")
    
    lines = [
        f"{sev_emoji} *INTELLIGENCE BRIEF \\(Severity: {severity}/5\\)*",
        f"🤖 Agent: `{_escape(agent_name)}`",
        "",
        f"*{_escape(headline)}*",
        "",
        f"📂 *Catalyst:* {_escape(b.get('catalyst_type', 'unknown'))}",
        f"📍 *Theater:* {_escape(b.get('geopolitical_theater', 'unknown'))}",
        "",
    ]
    
    if b.get("geographic_hotspots"):
        lines.append(f"🌍 *Hotspots:* {_escape(', '.join(b['geographic_hotspots']))}")
    if b.get("financial_instruments_affected"):
        lines.append(f"💰 *Affected Assets:* `{_escape(', '.join(b['financial_instruments_affected']))}`")
        
    entities = b.get("entities", [])
    if entities:
        lines.append("")
        lines.append("*Key Entities:*")
        for ent in entities[:4]:
            threat_str = " \\(Threat Actor\\)" if ent.get("is_threat_actor") else ""
            lines.append(f"• {_escape(ent.get('name'))} \\({_escape(ent.get('type'))}\\){threat_str}")
            
    if b.get("recommended_monitoring"):
        lines.append("")
        lines.append("*Recommended Monitoring:*")
        for item in b["recommended_monitoring"][:3]:
            lines.append(f"• {_escape(item)}")
            
    return "\n".join(lines)
