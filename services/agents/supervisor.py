import asyncio
import json
import logging
import time
from services.agents.base import SentinelAgent
from shared.kafka import Topics

logger = logging.getLogger("agent.supervisor")

ALLOWED_RELATIONS = {
    "RELATED_TO", "CONTROLS", "ALLIED_WITH", "OWNS", "COMPETES_WITH", 
    "HAS_EXPOSURE_IN", "CORRELATED_WITH", "SUPPLIES", "PURCHASES_FROM", 
    "COMMODITY_EXPOSURE", "MACRO_CORRELATED", "SANCTIONS_TARGET", 
    "FLAGGED_BY", "SUBSIDIARY_OF", "ADJACENT_TO", "ATTACKED", 
    "TARGETED_BY", "REGISTERED_IN", "EMPLOYS", "POSITIVE_EXPOSURE_TO", 
    "INVERSE_EXPOSURE_TO"
}


class GraphSupervisor(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def output_topic(self) -> str:
        return "sentinel.ontology.supervisor.noop"

    async def handle(self, message: dict) -> None:
        await self.execute_proposal(message)

    async def acquire_lock(self, entity_id: str, timeout: int = 5) -> bool:
        lock_key = f"sentinel:lock:neo4j:{entity_id}"
        end_time = time.time() + timeout
        while time.time() < end_time:
            if await self.redis.raw.set(lock_key, "locked", nx=True, ex=15):
                return True
            await asyncio.sleep(0.1)
        return False

    async def release_lock(self, entity_id: str):
        await self.redis.raw.delete(f"sentinel:lock:neo4j:{entity_id}")

    async def execute_proposal(self, payload: dict):
        """Safely maps trusted JSON structs to parameterized Cypher queries."""
        entity_id = payload.get("entity_id")
        action = payload.get("action") 
        data = payload.get("data", {})
        
        if not entity_id or not action: return

        if not await self.acquire_lock(entity_id):
            logger.error(f"Lock timeout for entity {entity_id}. Dropping proposal.")
            return

        try:
            if action == "MERGE_ONTOLOGY_NODE":
                label = data.get("label", "UnknownEntity")
                import re
                if not re.match(r"^[A-Za-z0-9]+$", label): label = "UnknownEntity"

                cypher = f"""
                MERGE (e:{label} {{name: $name}})
                SET e.primary_domain = $domain,
                    e.macro_concepts = $concepts,
                    e.sanctions_risk = $sanctions,
                    e.confidence = $confidence,
                    e.updated_at = datetime()
                """
                await self.neo4j.execute(cypher, {
                    "name": entity_id, "domain": data.get("primary_domain"),
                    "concepts": data.get("macro_concepts"), "sanctions": data.get("sanctions_risk"),
                    "confidence": data.get("confidence")
                })
                logger.debug(f"✅ Created/Updated Node: {entity_id}")

            elif action == "LINK_ENTITY":
                target_id = data.get("target_id")
                source_label = data.get("source_label", "Entity")
                target_label = data.get("target_label", "Entity")
                relation = data.get("relation_type", "RELATED_TO").upper()
                
                if relation not in ALLOWED_RELATIONS:
                    logger.warning(f"Rejected invalid LLM graph relation type: {relation}")
                    return

                cypher = f"""
                MERGE (a:{source_label} {{name: $id}})
                MERGE (b:{target_label} {{name: $target_id}})
                MERGE (a)-[r:{relation}]->(b)
                SET r.weight = $weight, r.updated_at = datetime()
                """
                await self.neo4j.execute(cypher, {"id": entity_id, "target_id": target_id, "weight": data.get("weight", 1.0)})
                logger.debug(f"✅ Created Edge: {entity_id} -[{relation}]-> {target_id}")

            elif action == "ADD_TAGS":
                tags = data.get("tags", [])
                label = data.get("label", "Entity")
                if not tags: return

                # Pure Cypher array deduplication: combines existing tags with new tags, unrolls them, and collects only unique ones.
                cypher = f"""
                MERGE (e:{label} {{name: $id}})
                WITH e, coalesce(e.tags, []) + $new_tags AS all_tags
                UNWIND all_tags AS tag
                WITH e, collect(distinct tag) AS unique_tags
                SET e.tags = unique_tags, e.updated_at = datetime()
                """
                await self.neo4j.execute(cypher, {"id": entity_id, "new_tags": tags})
                logger.debug(f"✅ Added {len(tags)} tags to {entity_id}")

            else:
                logger.warning(f"Unknown proposal action: {action}")

        except Exception as e:
            logger.error(f"Neo4j commit failed for {entity_id}: {e}")
        finally:
            await self.release_lock(entity_id)

async def start_supervisor():
    logger.info("🛡️ Graph Supervisor Online. Protecting Neo4j state.")
    from shared.db import get_redis, get_neo4j, get_timescale
    from shared.kafka import SentinelProducer, SentinelConsumer

    redis_client = await get_redis()
    db_client = await get_timescale()
    neo4j_client = await get_neo4j()

    producer = SentinelProducer()
    dlq = SentinelProducer()
    consumer = SentinelConsumer(
        topics=["sentinel.ontology.proposals"],
        group_id="supervisor-group",
        auto_offset_reset="latest",
    )

    supervisor = GraphSupervisor(
        agent_name="supervisor",
        input_topics=["sentinel.ontology.proposals"],
        redis_client=redis_client,
        db_client=db_client,
        neo4j_client=neo4j_client,
        producer=producer,
        consumer=consumer,
        dlq=dlq,
    )
    await supervisor.run()

if __name__ == "__main__":
    asyncio.run(start_supervisor())