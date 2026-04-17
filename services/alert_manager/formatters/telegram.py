"""
services/alert-manager/formatters/telegram.py
 
Formats a CorrelationCluster or Scenario into a Telegram message.
Uses MarkdownV2 formatting (Telegram's preferred format).
"""
 
import re
from typing import Optional
from shared.models import CorrelationCluster, Scenario, AlertTier
 
 
TIER_EMOJI = {
    AlertTier.WATCH:        "👁",
    AlertTier.ALERT:        "⚠️",
    AlertTier.INTELLIGENCE: "🔴",
}

def _escape(text: str) -> str:
    """
    Escape special chars for Telegram MarkdownV2.
    
    TEACHING MOMENT: Telegram's API is very strict. If you try to send a message 
    with a character like '-' or '.' without "escaping" it (putting a backslash 
    in front of it, like '\.'), Telegram will reject the message and crash the alert.
    This function uses Regular Expressions (re) to find all special characters 
    and automatically add that backslash.
    """
    special = r"_*[]()~`>#+-=|{}.!\\"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))

def format_correlation(cluster: CorrelationCluster) -> str:
    """Format a CorrelationCluster as a Telegram alert message."""
    # What's fed in? The 'cluster' object, which was created in main.py 
    # right after the Kafka Consumer grabbed the raw JSON from the event stream.
    emoji = TIER_EMOJI.get(cluster.alert_tier, "❓")
    tier = cluster.alert_tier.name

    # BEST PRACTICE: We build the message as a list of strings first. 
    # Appending to lists is much faster and cleaner in Python than gluing 
    # strings together with the '+' operator.
    lines = [
        f"{emoji} *\\[{_escape(tier)}\\] {_escape(cluster.rule_name)}*",
        "",
        _escape(cluster.description),
        "",
        f"🏷 Tags: `{_escape(', '.join(cluster.tags[:6]))}`",
        f"📅 {_escape(cluster.detected_at.strftime('%Y-%m-%d %H:%M UTC'))}",
        f"🆔 `{_escape(cluster.correlation_id[:8])}`",
    ]
 
    if cluster.entity_ids:
        lines.append(f"🚢 Entities: `{_escape(', '.join(cluster.entity_ids[:3]))}`")
 
    # Finally, we join all those list items together with a newline character (\n).
    return "\n".join(lines)
 
 
def format_scenario(scenario: Scenario) -> str:
    """Format a Scenario as a Telegram intelligence briefing."""
    # What's fed in? The 'scenario' object, containing AI-generated hypotheses 
    # fetched from the database in main.py.
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
 
    # --- THE HYPOTHESIS LOOP ---
    # scenario.hypotheses is a list of dictionaries.
    # We slice it `[:3]` to only loop through the top 3, preventing massive wall-of-text messages.
    # `enumerate(..., 1)` loops through the list BUT also gives us a counter 'i' 
    # that starts at 1. So 'i' is the number, 'h' is the hypothesis dictionary.
    for i, h in enumerate(scenario.hypotheses[:3], 1):
        # DEFENSIVE PROGRAMMING: Using .get() ensures that if the AI forgot to 
        # include a "label" in the dictionary, the code won't crash. It safely defaults to "Scenario X".
        label       = h.get("label", f"Scenario {i}")
        probability = h.get("probability", "?")
        mechanism   = h.get("mechanism", "")
        lines += [
            f"*{i}\\. {_escape(label)} \\({probability}%\\)*",
            _escape(mechanism[:200]),
            "",
        ]
 
    if scenario.recommended_monitoring:
        lines.append("*Monitor:*")
        # --- THE MONITORING LOOP ---
        # scenario.recommended_monitoring is just a simple list of strings.
        # We loop through up to 5 items and append them as bullet points.
        for item in scenario.recommended_monitoring[:5]:
            lines.append(f"• {_escape(item)}")
 
    return "\n".join(lines)
 
