"""
services/reasoning/scenario_generator.py

Uses Google Gemini to synthesize cross-domain correlation clusters into
structured intelligence scenarios.
"""

import os
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
import asyncio
from google import genai
from google.genai import types

from shared.models import CorrelationCluster, Scenario, ScenarioStatus

logger = logging.getLogger("reasoning.generator")

class ScenarioGenerator:
    # Accept the TimescaleDB client via dependency injection to hydrate events
    def __init__(self, db_client):
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model_name = "gemini-2.5-flash"
        self.db = db_client
        # RATE LIMIT DEFENSE: Ensure we only ever have 1 concurrent request hitting Gemini
        self.concurrency_limiter = asyncio.Semaphore(1)
    async def generate(self, cluster: CorrelationCluster, context: dict, patterns: list) -> Optional[Scenario]:
        """
        Builds the prompt and calls Gemini to analyze the cluster.
        """
        # 1. EVENT HYDRATION: Fetch the full raw events from TimescaleDB using the IDs
        event_ids = [cluster.trigger_event_id] + cluster.supporting_event_ids
        raw_events = []
        
        try:
            # Query the database for the exact events that triggered this cluster
            format_strings = ','.join(['%s'] * len(event_ids))
            sql = f"""
                SELECT type, source, tags, anomaly_score, occurred_at, 
                financial_data, prediction_market_data, crypto_data, cyber_data,
                vessel_data, flight_data, headline
                FROM events 
                WHERE event_id IN ({format_strings})
            """
            rows = self.db.query(sql, tuple(event_ids))
            
            # Clean up datetime objects for JSON serialization
            for row in rows:
                if 'occurred_at' in row and isinstance(row['occurred_at'], datetime):
                    row['occurred_at'] = row['occurred_at'].isoformat()
                raw_events.append({k: v for k, v in row.items() if v is not None})
                
        except Exception as e:
            logger.error(f"Failed to hydrate events for cluster {cluster.correlation_id}: {e}")

        # 2. Construct the Intelligence Briefing Prompt
        prompt = f"""
        You are 'Sentinel', an elite multi-domain intelligence analyst system.
        You have detected a complex anomaly spanning multiple data streams.

        === ANOMALY TRIGGER ===
        Rule Fired: {cluster.rule_name}
        Description: {cluster.description}
        Tags: {', '.join(cluster.tags)}

        === RAW EVENT DATA ===
        The following deeply-analyzed events triggered this alert:
        {json.dumps(raw_events, indent=2)}

        === GRAPH DATABASE CONTEXT (NEO4J) ===
        Known background intelligence on the entities involved:
        {json.dumps(context, indent=2)}

        === HISTORICAL PATTERNS ===
        Similar events from the past 90 days:
        {json.dumps(patterns, indent=2)}

        TASK:
        Analyze the raw events, graph context, and historical patterns.
        Provide a structured intelligence brief detailing what is likely unfolding.
        Maintain a highly objective, analytical, and professional tone.
        """

        # 3. Define the exact JSON schema
        scenario_schema = types.Schema(
            type=types.Type.OBJECT,
            properties={
                "headline": types.Schema(
                    type=types.Type.STRING,
                    description="A concise, 1-sentence intelligence summary."
                ),
                "significance": types.Schema(
                    type=types.Type.STRING,
                    description="Why does this matter? What is the strategic or financial impact?"
                ),
                "hypotheses": types.Schema(
                    type=types.Type.ARRAY,
                    description="List exactly 3 likely hypotheses for what is happening.",
                    items=types.Schema(type=types.Type.STRING)
                ),
                "recommended_monitoring": types.Schema(
                    type=types.Type.STRING,
                    description="Specific data feeds or entities analysts should monitor next to confirm the hypotheses."
                ),
                "confidence_overall": types.Schema(
                    type=types.Type.INTEGER,
                    description="An integer from 0 to 100 representing confidence in the assessment."
                ),
                "confidence_rationale": types.Schema(
                    type=types.Type.STRING,
                    description="Justification for the confidence score based on the available data."
                )
            },
            required=["headline", "significance", "hypotheses", "recommended_monitoring", "confidence_overall", "confidence_rationale"]
        )

        async with self.concurrency_limiter:
            max_retries = 3
            retry_delay = 10

            for attempt in range(max_retries):
                try:
                    logger.info(f"Invoking Gemini for cluster {cluster.correlation_id} (Attempt {attempt+1}/{max_retries})...")
                    response = await asyncio.to_thread(
                        self.client.models.generate_content,
                        model=self.model_name,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            response_mime_type="application/json",
                            response_schema=scenario_schema,
                            temperature=0.2, 
                        )
                    )

                    ai_data = json.loads(response.text)

                    scenario = Scenario(
                        scenario_id=f"scn_{uuid.uuid4().hex[:8]}",
                        correlation_id=cluster.correlation_id,
                        status=ScenarioStatus.ACTIVE,
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                        headline=ai_data["headline"],
                        significance=ai_data["significance"],
                        hypotheses=ai_data["hypotheses"],
                        recommended_monitoring=ai_data["recommended_monitoring"],
                        confidence_overall=ai_data["confidence_overall"],
                        confidence_rationale=ai_data["confidence_rationale"]
                    )
                    await asyncio.sleep(2)  # Brief pause to respect rate limits
                    return scenario
                except Exception as e:
                    error = str(e)
                    if "429" in error or "RESOURCE_EXHAUSTED" in error:
                        logger.warning(f"Rate limit hit for cluster {cluster.correlation_id}: {error} — retrying in {retry_delay}s")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(f"Gemini API Error for cluster {cluster.correlation_id}: {error}", exc_info=True)
                        return None
