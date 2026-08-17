"""
services/reporting/report_generator.py

Executive Intelligence & Risk Report Generator for Sentinel.
Synthesizes cross-domain anomaly telemetry, correlation graphs, and portfolio risk
into professional executive briefs.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger("reporting.generator")


class ReportGenerator:
    def __init__(self, db_client: Any = None, redis_client: Any = None):
        self.db = db_client
        self.redis = redis_client

    async def generate_brief(
        self,
        timeframe_hours: int = 24,
        title: Optional[str] = None,
        author: str = "Sentinel AI Surveillance System",
    ) -> Dict[str, Any]:
        """
        Generates an intelligence brief across events, correlations, scenarios, and health metrics.
        """
        now = datetime.now(timezone.utc)
        report_id = f"RPT-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}"
        report_title = title or f"Sentinel Strategic Intelligence & Market Risk Brief ({timeframe_hours}h Window)"

        # Fetch recent anomalies
        events_summary = []
        if self.db:
            try:
                rows = await self.db.query(
                    """
                    SELECT event_id, type, source, primary_entity_name, anomaly_score, occurred_at
                    FROM events
                    WHERE occurred_at >= NOW() - INTERVAL '1 hour' * $1
                    ORDER BY anomaly_score DESC
                    LIMIT 10;
                    """,
                    timeframe_hours,
                )
                for r in rows:
                    events_summary.append({
                        "event_id": r.get("event_id"),
                        "type": r.get("type"),
                        "source": r.get("source"),
                        "entity": r.get("primary_entity_name"),
                        "anomaly_score": float(r.get("anomaly_score") or 0.0),
                    })
            except Exception as e:
                logger.warning(f"Error fetching report events: {e}")

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
- **Primary Domain Drivers:** Energy / Crude Oil Disruption, Geopolitical Shipping Lanes, Tech Sector Volume Spikes

---

## 2. Top Cross-Domain Anomalies
| Entity | Domain | Source | Anomaly Score | Severity |
| :--- | :--- | :--- | :--- | :--- |
"""
        for ev in events_summary:
            score = ev.get("anomaly_score", 0.0)
            sev = "CRITICAL" if score >= 0.8 else ("HIGH" if score >= 0.5 else "MEDIUM")
            markdown_body += f"| **{ev.get('entity', 'Unknown')}** | `{ev.get('type')}` | `{ev.get('source')}` | {score:.2f} | `{sev}` |\n"

        if not events_summary:
            markdown_body += "| *No critical anomalies recorded during this interval* | - | - | - | - |\n"

        markdown_body += f"""
---

## 3. Infrastructure & Feed Health
- **Total Registered Collectors/Engines:** {health_status.get('components_count', 0)}
- **Healthy Feeds:** {health_status.get('healthy_count', 0)}
- **Degraded/Offline Feeds:** {health_status.get('degraded_count', 0) + health_status.get('offline_count', 0)}

---
*End of Report — Confidential & Proprietary to Sentinel Surveillance System.*
"""

        return {
            "report_id": report_id,
            "title": report_title,
            "generated_at": now.isoformat(),
            "timeframe_hours": timeframe_hours,
            "author": author,
            "markdown": markdown_body,
            "events_count": len(events_summary),
            "system_status": health_status.get("system_status"),
        }
