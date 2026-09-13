"""
services/reporting/report_generator.py

Executive Intelligence Report Generator for Sentinel.

Synthesizes anomaly telemetry, correlation clusters, generated scenarios and
feed health into a brief.

It said "correlation graphs, and portfolio risk", and the method below said
"across events, correlations, scenarios, and health metrics" -- while the only
occurrences of the words "correlation" and "scenario" anywhere in this file
were those two sentences. Neither table was queried and neither section
existed. Portfolio risk still does not appear here: it is a property of a
book, not of an observation window, and /portfolio/risk is where it lives.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Dict, List, Optional, Any

from shared.models.events import event_domain

logger = logging.getLogger("reporting.generator")

# How many anomalies the brief's cross-domain table shows.
BRIEF_TOP_N = 10

# How many rows any one event type may contribute to the candidate pool. The
# pool is then diversified across domains, so this only has to be large enough
# that a domain is not starved when it genuinely dominates.
BRIEF_PER_TYPE_CAP = 5

# How many correlation clusters and scenarios a brief lists.
BRIEF_CORRELATIONS = 8
BRIEF_SCENARIOS = 5

# The report catalogue, and the one place it is defined.
#
# /reports/templates advertised three of these with ids and prose descriptions
# -- one promising "parametric VaR, CVaR tail risk, and sector concentration",
# another "active AI-generated scenarios" -- and /reports/generate had no
# parameter to receive a choice. Picking one could not change anything, and the
# generator had no notion of a template at all.
#
# `sections` is what a template actually controls, and every name in it is a
# section this file can produce.
REPORT_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "DAILY_EXECUTIVE_BRIEF": {
        "id": "DAILY_EXECUTIVE_BRIEF",
        "name": "Daily Executive Threat & Market Brief",
        "default_timeframe_hours": 24,
        "sections": ["anomalies", "correlations", "scenarios", "health"],
        "description": (
            "Cross-domain anomalies, the correlation clusters raised over the "
            "window, the scenarios written from them, and feed health."
        ),
    },
    "WEEKLY_INTELLIGENCE_REVIEW": {
        "id": "WEEKLY_INTELLIGENCE_REVIEW",
        "name": "Weekly Intelligence Review",
        "default_timeframe_hours": 168,
        "sections": ["anomalies", "correlations", "scenarios", "health"],
        "description": (
            "The same sections over a seven-day window. Replaces "
            "WEEKLY_PORTFOLIO_RISK, which promised parametric VaR, CVaR tail "
            "risk and sector concentration that this generator has never "
            "produced -- those live on /portfolio/risk, which is about a book "
            "rather than a window."
        ),
    },
    "INCIDENT_FLASH_REPORT": {
        "id": "INCIDENT_FLASH_REPORT",
        "name": "Rapid Incident Flash Report",
        "default_timeframe_hours": 4,
        "sections": ["anomalies", "scenarios"],
        "description": (
            "Recent high-scoring anomalies and the scenarios raised from them, "
            "over a short window. No health section: an incident brief is about "
            "the incident."
        ),
    },
}

DEFAULT_TEMPLATE_ID = "DAILY_EXECUTIVE_BRIEF"


def _domain_drivers(events: List[Dict[str, Any]]) -> str:
    """Which domains actually drove the window.

    This line read "Energy / Crude Oil Disruption, Geopolitical Shipping Lanes,
    Tech Sector Volume Spikes" on every brief the platform has ever produced,
    regardless of what was in the data -- a fabricated finding in a document
    presented to a reader as an analysis of the preceding window.
    """
    if not events:
        return "None - no anomalies above threshold in this window"
    counts: Dict[str, int] = {}
    for ev in events:
        domain = event_domain(ev.get("type"))
        counts[domain] = counts.get(domain, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{domain} ({count})" for domain, count in ranked[:4])


def _diversify_by_domain(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """Pick the strongest anomalies without letting one domain take the table.

    A global `ORDER BY anomaly_score DESC LIMIT 10` returned nine BGP rows on the
    live deployment, under a heading that says "Top Cross-Domain Anomalies". That
    is not a ranking artefact to be tolerated -- it is what a global sort does
    when one domain's scorer saturates: the domain with the highest *scale* wins
    every slot regardless of what happened in the others, and the brief then
    reports a quiet day in five domains as no day at all.

    Scores are not comparable across domains anyway. Each domain's scorer is
    calibrated against its own history, so 0.95 in BGP and 0.95 in maritime are
    two different statements. Ranking within a domain is meaningful; ranking
    across them is not, which is exactly why the selection is round-robin over
    domains and the ordering within each domain is by score.
    """
    by_domain: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_domain.setdefault(event_domain(row.get("type")), []).append(row)
    for bucket in by_domain.values():
        bucket.sort(key=lambda r: float(r.get("anomaly_score") or 0.0), reverse=True)

    # Domains enter the rotation ordered by their own strongest event, so a
    # domain with something genuinely extreme is seen first.
    order = sorted(
        by_domain,
        key=lambda d: float(by_domain[d][0].get("anomaly_score") or 0.0),
        reverse=True,
    )

    picked: List[Dict[str, Any]] = []
    depth = 0
    while len(picked) < limit and any(len(by_domain[d]) > depth for d in order):
        for domain in order:
            if len(picked) >= limit:
                break
            bucket = by_domain[domain]
            if depth < len(bucket):
                picked.append(bucket[depth])
        depth += 1
    return picked


class ReportGenerator:
    def __init__(self, db_client: Any = None, redis_client: Any = None):
        self.db = db_client
        self.redis = redis_client

    async def _recent_correlations(self, timeframe_hours: int) -> List[Dict[str, Any]]:
        """Clusters raised in the window, highest tier first."""
        if not self.db:
            return []
        try:
            rows = await self.db.query(
                """
                SELECT correlation_id, rule_name, alert_tier, detected_at, description
                FROM correlations
                WHERE detected_at >= NOW() - INTERVAL '1 hour' * $1
                ORDER BY alert_tier DESC, detected_at DESC
                LIMIT $2;
                """,
                timeframe_hours, BRIEF_CORRELATIONS,
            )
            return [dict(r) for r in (rows or [])]
        except Exception as e:
            logger.warning("Error fetching report correlations: %s", e)
            return []

    async def _recent_scenarios(self, timeframe_hours: int) -> List[Dict[str, Any]]:
        """Scenarios written in the window, most confident first."""
        if not self.db:
            return []
        try:
            rows = await self.db.query(
                """
                SELECT scenario_id, headline, significance, status,
                       confidence_overall, primary_entity_name, created_at
                FROM scenarios
                WHERE created_at >= NOW() - INTERVAL '1 hour' * $1
                ORDER BY confidence_overall DESC NULLS LAST, created_at DESC
                LIMIT $2;
                """,
                timeframe_hours, BRIEF_SCENARIOS,
            )
            return [dict(r) for r in (rows or [])]
        except Exception as e:
            logger.warning("Error fetching report scenarios: %s", e)
            return []

    async def generate_brief(
        self,
        timeframe_hours: Optional[int] = None,
        title: Optional[str] = None,
        author: str = "Sentinel AI Surveillance System",
        template_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generates an intelligence brief across events, correlations, scenarios
        and feed health, over the requested window.

        `template_id` selects which of those sections appear. It is new: the
        catalogue existed, was served to the client with three ids, and there
        was no parameter here to receive one.
        """
        template = REPORT_TEMPLATES.get(
            (template_id or DEFAULT_TEMPLATE_ID).upper(), REPORT_TEMPLATES[DEFAULT_TEMPLATE_ID]
        )
        sections = set(template["sections"])
        # The template's own window unless the caller named one. Resolved here
        # rather than only in the route, so the generator cannot be called
        # directly and quietly produce a four-hour flash report over 24 hours.
        if timeframe_hours is None:
            timeframe_hours = int(template["default_timeframe_hours"])
        now = datetime.now(timezone.utc)
        report_id = f"RPT-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}"
        report_title = title or f"Sentinel Strategic Intelligence & Market Risk Brief ({timeframe_hours}h Window)"

        # Fetch recent anomalies
        events_summary = []
        if self.db:
            try:
                # Capped per event type in SQL so the candidate pool cannot be
                # one saturated type, then diversified across domains below.
                rows = await self.db.query(
                    """
                    SELECT event_id, type, source, primary_entity_name, anomaly_score, occurred_at
                    FROM (
                        SELECT event_id, type, source, primary_entity_name,
                               anomaly_score, occurred_at,
                               ROW_NUMBER() OVER (
                                   PARTITION BY type ORDER BY anomaly_score DESC, occurred_at DESC
                               ) AS rn
                        FROM events
                        WHERE occurred_at >= NOW() - INTERVAL '1 hour' * $1
                    ) ranked
                    WHERE rn <= $2
                    ORDER BY anomaly_score DESC;
                    """,
                    timeframe_hours, BRIEF_PER_TYPE_CAP,
                )
                candidates = [
                    {
                        "event_id": r.get("event_id"),
                        "type": r.get("type"),
                        "source": r.get("source"),
                        "entity": r.get("primary_entity_name"),
                        "anomaly_score": float(r.get("anomaly_score") or 0.0),
                    }
                    for r in rows
                ]
                events_summary = _diversify_by_domain(candidates, BRIEF_TOP_N)
            except Exception as e:
                logger.warning(f"Error fetching report events: {e}")

        correlations = await self._recent_correlations(timeframe_hours) if "correlations" in sections else []
        scenarios = await self._recent_scenarios(timeframe_hours) if "scenarios" in sections else []

        # Fetch health overview
        from shared.utils.heartbeat import get_all_heartbeats_status
        health_status = await get_all_heartbeats_status(self.redis)

        # Build Markdown content
        markdown_body = f"""# {report_title}
**Report ID:** `{report_id}`  
**Generated At:** {now.strftime('%Y-%m-%d %H:%M:%S UTC')}  
**Author:** {author}  
**Classification:** TLP:AMBER+STRICT  

---

## 1. Executive Summary
During the preceding {timeframe_hours}-hour observation window, Sentinel surveillance engines evaluated cross-domain signals across US TradFi Equities, Prediction Markets, Global Crypto Flows, Cyber Assets, and Geopolitical Vectors.

- **System Health:** `{health_status.get('system_status', 'UNKNOWN')}` ({int(health_status.get('healthy_ratio', 0) * 100)}% active components)
- **High-Confidence Anomalies Detected:** {len(events_summary)}
- **Primary Domain Drivers:** {_domain_drivers(events_summary)}

---

## 2. Top Cross-Domain Anomalies
| Entity | Domain | Event Type | Source | Anomaly Score | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- |
"""
        for ev in events_summary:
            score = ev.get("anomaly_score", 0.0)
            sev = "CRITICAL" if score >= 0.8 else ("HIGH" if score >= 0.5 else "MEDIUM")
            # The column is headed "Domain" and was printing the event type.
            domain = event_domain(ev.get("type"))
            markdown_body += (
                f"| **{ev.get('entity', 'Unknown')}** | `{domain}` | `{ev.get('type')}` "
                f"| `{ev.get('source')}` | {score:.2f} | `{sev}` |" + chr(10)
            )

        if not events_summary:
            markdown_body += "| *No critical anomalies recorded during this interval* | - | - | - | - | - |\n"

        if "correlations" in sections:
            markdown_body += chr(10) + "---" + chr(10) + chr(10) + "## 3. Correlation Clusters Raised" + chr(10) + chr(10)
            if correlations:
                markdown_body += "| Tier | Rule | Detected | Description |" + chr(10)
                markdown_body += "| :--- | :--- | :--- | :--- |" + chr(10)
                for c in correlations:
                    detected = c.get("detected_at")
                    when = detected.strftime("%Y-%m-%d %H:%M") if hasattr(detected, "strftime") else str(detected)
                    desc = (c.get("description") or "")[:160]
                    markdown_body += (
                        f"| `{c.get('alert_tier')}` | **{c.get('rule_name')}** "
                        f"| {when} | {desc} |" + chr(10)
                    )
            else:
                markdown_body += "*No correlation clusters were raised during this interval.*" + chr(10)

        if "scenarios" in sections:
            markdown_body += chr(10) + "---" + chr(10) + chr(10) + "## 4. Generated Scenarios" + chr(10) + chr(10)
            if scenarios:
                for sc in scenarios:
                    markdown_body += (
                        f"**{sc.get('headline')}**  " + chr(10)
                        + f"Subject: `{sc.get('primary_entity_name') or 'multi-entity'}` · "
                        + f"Status: `{sc.get('status')}` · "
                        + f"Confidence: {sc.get('confidence_overall')}%" + chr(10) + chr(10)
                        + f"{(sc.get('significance') or '').strip()}" + chr(10) + chr(10)
                    )
            else:
                markdown_body += "*No scenarios were generated during this interval.*" + chr(10)

        if "health" in sections:
            markdown_body += f"""
---

## 5. Infrastructure & Feed Health
- **Total Registered Collectors/Engines:** {health_status.get('components_count', 0)}
- **Healthy Feeds:** {health_status.get('healthy_count', 0)}
- **Degraded/Offline Feeds:** {health_status.get('degraded_count', 0) + health_status.get('offline_count', 0)}
"""

        markdown_body += """
---
*End of Report — Confidential & Proprietary to Sentinel Surveillance System.*
"""

        return {
            "report_id": report_id,
            "title": report_title,
            "generated_at": now.isoformat(),
            "timeframe_hours": timeframe_hours,
            "template_id": template["id"],
            "sections": sorted(sections),
            "author": author,
            "markdown": markdown_body,
            "events_count": len(events_summary),
            "correlations_count": len(correlations),
            "scenarios_count": len(scenarios),
            "system_status": health_status.get("system_status"),
        }
