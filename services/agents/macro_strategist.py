import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from services.agents.base import SentinelAgent
from services.agents.news_intel import IntelBrief

logger = logging.getLogger("agent.macro_strategist")

class MacroStrategistAgent(SentinelAgent):
    """
    Runs periodically (e.g., every 4 hours) rather than reacting to live events.
    Analyzes historical TimeScale aggregates and Redis Ontology matrices to detect trends.
    """
    @property
    def output_topic(self) -> str:
        return "agents.intel.briefs"

    async def handle(self, message: dict) -> None:
        pass

    async def run(self):
        review_task = asyncio.create_task(self.run_scheduled_review())
        try:
            await super().run()
        finally:
            review_task.cancel()

    async def run_scheduled_review(self):
        while True:
            try:
                self.logger.info("Initiating Macro Trend Review...")
                
                # 1. Fetch Top Co-occurring concepts from Redis
                cooccurrence = await self.redis.raw.zrevrange("sentinel:ontology:cooccurrence", 0, 10, withscores=True)
                
                # 2. Fetch sector aggregate data from TimescaleDB (Pseudo-query)
                query = """
                    SELECT sector_tags, AVG(forward_pe) as avg_fwd_pe, SUM(notional_volume) 
                    FROM market_aggregates 
                    WHERE time > NOW() - INTERVAL '24 hours'
                    GROUP BY sector_tags
                """
                # (Execute via your asyncpg / connection)
                
                prompt = f"""
                You are a Quantitative Macro Strategist.
                Review the following systemic shifts over the last 24 hours:
                Ontology Co-occurrences: {cooccurrence}
                Sector Data: [Insert DB Data]
                
                Identify any subtle aggregate shifts (e.g., accumulation in defense, 
                valuation multiples compressing in semiconductors). 
                Generate a JSON strategic brief.
                """
                
                run_id = f"macro_review_{int(time.time())}"
                msg = {"event_id": run_id}
                
                response = await self._execute_with_telemetry(
                    message=msg,
                    system_prompt=prompt,
                    user_prompt="Generate the strategic brief.",
                    schema=IntelBrief,
                    temperature=0.1
                )
                
                brief_payload = {
                    "agent":            self.name,
                    "agent_run_id":     run_id,
                    "created_at":       datetime.now(timezone.utc).isoformat(),
                    "brief":            response.model_dump() if hasattr(response, "model_dump") else response.dict(),
                    "computed_severity": response.severity if hasattr(response, "severity") else 3,
                }
                
                # Publish to a new topic for the UI to consume
                await self._producer.send("agents.intel.briefs", brief_payload, key="macro_review")
                
                # Expose to other agents via fast Redis cache
                await self.redis.raw.set("sentinel:macro:latest_brief", json.dumps(brief_payload), ex=86400)
                
                self.logger.info("Macro Trend Review completed and published successfully.")
                
                # Sleep for 30 minutes
                await asyncio.sleep(1800)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in Macro Trend Review loop: {e}", exc_info=True)
                # Backoff and retry in 1 minute on unexpected error
                await asyncio.sleep(60)