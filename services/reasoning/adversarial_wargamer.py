import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
from dotenv import load_dotenv
from networkx import subgraph
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.db import get_neo4j, get_redis
from shared.utils.ollama import OllamaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("reasoning.wargamer")

class SimulationMove(BaseModel):
    persona_name: str
    proposed_counter_action: str
    target_entity_id: str
    disruption_potential_percent: int = Field(default=10, ge=0, le=100)
    strategic_rationale: str

class WargameSimulationOutput(BaseModel):
    simulation_run_id: str
    primary_vulnerability_isolated: str
    cascade_failure_probability: int
    predicted_next_target_entity_id: str
    remediation_recommendation: str

class AdversarialSimulationEngine:
    def __init__(self, neo4j_client, redis_client, ollama_client, producer):
        self.neo4j = neo4j_client
        self.redis = redis_client
        self.llm = ollama_client
        self.producer = producer

    async def _fetch_subgraph_context(self, primary_entity_ids: List[str]) -> List[str]:
        loop = asyncio.get_running_loop()
        extracted_edges = []
        for entity_id in primary_entity_ids:
            try:
                rows = await loop.run_in_executor(None, lambda eid=entity_id: self.neo4j.query("""
                    MATCH (a:Entity {id: $id})-[r*1..3]-(b:Entity)
                    RETURN a.id as src, type(r[-1]) as rel, b.id as tgt LIMIT 15
                """, {"id": eid}))
                for r in rows: extracted_edges.append(f"({r['src']})-[:{r['rel']}]->({r['tgt']})")
            except Exception as e:
                logger.error(f"Graph context extraction failed for {entity_id}: {e}")
        return list(set(extracted_edges))
    
    async def _execute_persona_turn(self, persona: str, system_prompt: str, scenario: str, subgraph: List[str]) -> SimulationMove:
        user_prompt = f"SCENARIO:\n{scenario}\n\nGRAPH CONSTRAINTS:\n{json.dumps(subgraph)}\n\nPropose counter-maneuver."
        try:
            return await self.llm.infer(system_prompt=system_prompt, user_prompt=user_prompt, schema=SimulationMove, temperature=0.40)
        except Exception as e:
            logger.error(f"Simulation failed for {persona}: {e}")
            return SimulationMove(persona_name=persona, proposed_counter_action="PASS", target_entity_id="NONE", strategic_rationale="Failure fallback.")
        
    
    async def run_predictive_wargame(self, cluster_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        entity_ids = cluster_data.get("entity_ids", [])
        description = cluster_data.get("description", "Generic threat cluster.")
        if not entity_ids: return None

        subgraph = await self._fetch_subgraph_context(entity_ids)
        personas = {
            "State_Saboteur": "Aggressive geopolitical state saboteur.",
            "Financial_Short_Seller": "Predatory hedge-fund operator exploiting physical chaos.",
            "Asymmetric_Defender": "Advanced intelligence defense grid."
        }

        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(self._execute_persona_turn(name, prompt, description, subgraph)) for name, prompt in personas.items()]
            moves = [t.result() for t in tasks]
        except Exception as e:
            logger.error(f"TaskGroup batch execution broken: {e}")
            return None
        
        try:
            arbitration_prompt = f"Analyze maneuvers:\n{json.dumps([m.model_dump() for m in moves])}\nIsolate failure point."
            synthesis: WargameSimulationOutput = await self.llm.infer(
                system_prompt="Principal game theory analyst.", user_prompt=arbitration_prompt, schema=WargameSimulationOutput, temperature=0.05
            )
            output = synthesis.model_dump()
            output["source_correlation_id"] = cluster_data.get("correlation_id", "unknown")
            return output
        except Exception as e:
            logger.error(f"Synthesis phase failed: {e}")
            return None

async def main_pipeline_pump():
    import aiohttp
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5)) as session:
        engine = AdversarialSimulationEngine(get_neo4j(), get_redis(), OllamaClient(session), SentinelProducer())
        consumer = SentinelConsumer(topics = [Topics.CORRELATIONS], group_id="simulation-wargamer", auto_offset_reset="latest")
        loop = asyncio.get_running_loop()

        try:
            while True:
                messages = await loop.run_in_executor(None, consumer.raw.poll, 1.0)
                if not messages: continue
                
                for _, msg_list in messages.items():
                    for msg in msg_list:
                        payload = json.loads(msg.value.decode('utf-8')) if isinstance(msg.value, bytes) else msg.value
                        sim_result = await engine.run_predictive_wargame(payload)
                        if sim_result:
                            engine.producer.send("agents.predictions.output", sim_result, key=sim_result.get("predicted_next_target_entity_id"))
        finally:
            consumer.close()

if __name__ == "__main__":
    asyncio.run(main_pipeline_pump())