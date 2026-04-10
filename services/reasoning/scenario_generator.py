"""
services/reasoning/scenario_generator.py
 
Calls Gemini to generate structured intelligence scenarios
from a CorrelationCluster + its assembled context.
 
Output schema (parsed from Gemini's JSON response):
  {
    "headline": str,
    "significance": str,
    "hypotheses": [
      {
        "label": str,
        "probability": int,      # 0-100
        "mechanism": str,
        "beneficiaries": [str],
        "watch_signals": [str],
        "deny_signals": [str],
        "time_horizon": str
      }
    ],
    "recommended_monitoring": [str],
    "confidence_overall": int,
    "confidence_rationale": str
  }
 
Gemini is told: every claim must reference an event_id from the context.
This prevents hallucination — if it can't cite evidence, it can't assert.
"""

# Import standard libraries for JSON manipulation, logging, and environment variables.
import json
import logging
import os
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import google.api_core.exceptions

 
# Import the official Google Generative AI SDK for calling Gemini.
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError

# Import our shared data models to ensure we output standardized objects.
from shared.models import CorrelationCluster, Scenario, ScenarioStatus

logger = logging.getLogger("reasoning.scenario_generator")

# ── MODEL CONFIGURATION ───────────────────────────────────────────────────────
# We use the 'Flash' model for speed and cost-efficiency on standard alerts.
# For Tier 3 (Critical Intelligence), we bump up to the smarter 'Pro' model.
MODEL_TIER2 = "gemini-2.5-flash"
MODEL_TIER3 = "gemini-2.5-pro"

# ── PROMPT ENGINEERING ────────────────────────────────────────────────────────
# This is the "System Prompt" — it acts as the instructions and persona for Gemini.
SYSTEM_PROMPT = """You are a senior multi-domain intelligence analyst at an advanced geopolitical risk firm.
You receive structured event data from an automated monitoring system that tracks real-time anomalies across Physical, Cyber, Financial, and Information domains, and you produce falsifiable intelligence assessments.

EXPECTED EVENT DOMAINS & SIGNAL TYPES:
- Maritime: Vessel dark (AIS gaps), ship-to-ship transfers (STS), GPS spoofing, speeding in chokepoints.
- Aviation: Flight anomalies, emergency squawks (7500 hijack, 7600 comm fail, 7700 general), military/dark flights.
- Financial: Large options sweeps on geopolitical tickers (oil, defense, shipping), abnormal futures (COT) positions, insider trades.
- Cyber: BGP routing hijacks, critical infrastructure exposed ports (SCADA/ICS), ransomware victim leaks, zero-day (KEV) exploits.
- Information (OSINT): Geopolitical news headlines, sanctions announcements, military strike reports.

Rules:
1. Every factual claim must cite a specific event_id from the provided context.
2. Cross-domain reasoning: Explicitly connect physical events (e.g., vessel dark) with non-kinetic events (e.g., cyber anomalies or financial sweeps) if present in the data.
3. Probabilities must sum to 100 across all hypotheses.
4. Watch signals must be specific, observable, measurable future events that would CONFIRM a hypothesis.
5. Deny signals must be specific, observable future events that would REFUTE a hypothesis.
6. Do not assert anything you cannot support with the provided data. Do not hallucinate external events.
7. Respond ONLY with valid JSON matching the schema — no preamble, no markdown.

JSON schema:
{
  "headline": "one sentence, <15 words",
  "significance": "2-3 sentences on why this matters and how the domains intersect",
  "hypotheses": [
    {
      "label": "short name for this scenario",
      "probability": 0-100,
      "mechanism": "how this would work mechanically based on the data",
      "beneficiaries": ["who gains if this hypothesis is true"],
      "watch_signals": ["specific observable event that would confirm"],
      "deny_signals": ["specific observable event that would refute"],
      "time_horizon": "when we'd expect resolution (e.g., 24-48 hours)"
    }
  ],
  "recommended_monitoring": ["specific domains or entities to watch closely"],
  "confidence_overall": 0-100,
  "confidence_rationale": "why you are or aren't confident based on the available evidence"
}"""

class ScenarioGenerator:
    """
    Takes a cluster of correlated events (CorrelationCluster) and asks Gemini to
    generate a human-readable intelligence scenario explaining what might be happening.
    """
    def __init__(self):
        # Load the API key from the environment. Fail immediately if it's missing.
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY not set")
        # Configure the global genai client with our API key.
        genai.configure(api_key=api_key)

    @retry(
            wait=wait_exponential(multiplier=1, min=2, max=10),
            stop=stop_after_attempt(5),
            retry=retry_if_exception_type((
                google.api_core.exceptions.ServiceUnavailable,
                google.api_core.exceptions.DeadlineExceeded,
            ))
    )
    def generate(
            self, 
            cluster: CorrelationCluster,
            context: Dict[str, Any],
            similar_patterns: list, 
    ) -> Optional[Scenario]:
        """
        Main entry point. Takes the cluster data, calls Gemini, cleans the output,
        and returns a structured Scenario object.
        """
        # Cost Optimization: Use Pro for high-tier alerts, Flash for medium-tier.
        model = MODEL_TIER3 if cluster.alert_tier >= 3 else MODEL_TIER2
        # Combine the system prompt with the specific event data.
        prompt = self._build_prompt(cluster, context, similar_patterns)

        logger.info(f"Generating scenario for cluster {cluster.rule_id} with model {model} :: TIER {cluster.alert_tier}")

        try:
            # Call the Gemini API.
            llm = genai.Generative(
                model=model,
                system_instruction = SYSTEM_PROMPT,
            )
            response = llm.generate_content(
                prompt, 
                generation_config = genai.GenerationConfig(
                    max_output_tokens=2000,                  # Cap the response length
                    response_mime_type="application/json",   # Request strict JSON formatting
                )
            )
            # Extract the raw text from the AI's response.
            raw_text = response.text.strip()

            # DEFENSIVE PARSING: 
            # Sometimes, even in JSON mode, LLMs wrap the output in markdown code blocks 
            # (e.g., ```json { ... } ```). This breaks Python's json.loads().
            # We strip out the markdown backticks and 'json' tag if they exist.
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]  # Extract JSON from markdown code block if present
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]  # Remove "json" prefix if present
            raw_text = raw_text.strip('` \n')  # Final cleanup

            # Convert the cleaned string into a Python dictionary.
            parsed = json.loads(raw_text)
            # Map the dictionary to our strict Pydantic 'Scenario' model.
            return self._to_scenario(cluster, parsed)
            
        # Handle specific errors gracefully so one bad generation doesn't crash the pipeline.
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error for cluster {cluster.rule_id}: {e} :: Raw response: {response.text}")
            return None
        except GoogleAPIError as e:
            logger.error(f"Google API error for cluster {cluster.rule_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for cluster {cluster.rule_id}: {e}", exc_info=True)
            return None
        
    def _build_prompt(
            self, 
            cluster: CorrelationCluster,
            context: Dict[str, Any],
            similar_patterns: list, 
    
    ) -> str:
        """
        Assembles the final text prompt sent to the LLM.
        Combines the base instructions with the specific data for this alert.
        """
        # Start by describing the rule that triggered this alert.
        parts = [
            "## Correlation Alert",
            f"Rule: {cluster.rule_name}",
            f"Tier: {cluster.alert_tier.name}",
            f"Description: {cluster.description}",
            "",
            "## Event Data",
            json.dumps(context, indent=2, default=str),
        ]
 
        # If we found matching historical patterns (e.g., via Soft Correlation), add them!
        if similar_patterns:
            parts += [
                "",
                "## Historical Precedents",
                json.dumps(similar_patterns, indent=2, default=str),
            ]
 
        parts += [
            # Final reinforcement of the rules.
            "",
            "Generate a structured intelligence assessment following the JSON schema.",
            "Produce 2-3 hypotheses. Cite event_ids as evidence.",
        ]
 
        return "\n".join(parts)
 
    def _to_scenario(self, cluster: CorrelationCluster, parsed: Dict) -> Scenario:
        """
        Maps the raw dictionary from Gemini into our strongly-typed Scenario model.
        """
        return Scenario(
            correlation_id=cluster.correlation_id,
            status=ScenarioStatus.HYPOTHESIS,
            headline=parsed.get("headline", ""),
            significance=parsed.get("significance", ""),
            hypotheses=parsed.get("hypotheses", []),
            recommended_monitoring=parsed.get("recommended_monitoring", []),
            confidence_overall=int(parsed.get("confidence_overall", 50)),
            confidence_rationale=parsed.get("confidence_rationale", ""),
        )