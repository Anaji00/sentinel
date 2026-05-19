import asyncio
import json
import logging
import time
from aiokafka import AIOKafkaConsumer
from shared.db import get_redis, get_neo4j
from shared.kafka import Topics

logger = logging.getLogger("agent.supervisor")

class GraphSupervisor:
    """
    A single-threaded, deterministic gatekeeper for the Knowledge Graph.
    LLM Agents propose changes to Kafka; this service safely executes them.
    """
    def __init__(self):
        self.redis = get_redis()
        self.neo4j = get_neo4j()
        
    async def acquire_lock(self, entity_id: str, timeout: int = 5) -> bool:
        """Distributed Redlock to prevent Neo4j Race Conditions."""
        lock_key = f"sentinel:lock:neo4j:{entity_id}"
        end_time = time.time() + timeout
        while time.time() < end_time:
            if self.redis.raw.set(lock_key, "locked", nx=True, ex=5):
                return True
            await asyncio.sleep(0.1)
        return False

    def release_lock(self, entity_id: str):
        self.redis.raw.delete(f"sentinel:lock:neo4j:{entity_id}")

    async def execute_proposal(self, payload: dict):
        entity_id = payload.get("entity_id")
        action = payload.get("action") # "ADD_TAGS" or "LINK_ENTITY"
        data = payload.get("data", {})
        
        if not entity_id or not action:
            return

        # 1. Acquire Distributed Lock for this specific entity
        if not await self.acquire_lock(entity_id):
            logger.error(f"Lock timeout for entity {entity_id}. Dropping proposal.")
            return

        try:
            # 2. Safely execute the exact graph structural change
            if action == "ADD_TAGS":
                tags = data.get("tags", [])
                query = """
                MERGE (e:Entity {id: $id})
                SET e.tags = array_distinct(coalesce(e.tags, []) + $new_tags),
                    e.last_updated = timestamp()
                """
                self.neo4j.execute(query, {"id": entity_id, "new_tags": tags})
                
            elif action == "LINK_ENTITY":
                target_id = data.get("target_id")
                relation = data.get("relation_type", "RELATED_TO")
                # Parameterized Cypher to prevent LLM injection attacks
                query = f"""
                MERGE (a:Entity {{id: $id}})
                MERGE (b:Entity {{id: $target_id}})
                MERGE (a)-[r:{relation}]->(b)
                SET r.weight = $weight, r.updated_at = timestamp()
                """
                self.neo4j.execute(query, {"id": entity_id, "target_id": target_id, "weight": data.get("weight", 1.0)})

            logger.info(f"✅ Supervisor safely committed {action} for {entity_id}")

        except Exception as e:
            logger.error(f"Neo4j commit failed for {entity_id}: {e}")
        finally:
            # 3. Always release the lock
            self.release_lock(entity_id)

async def start_supervisor():
    logger.info("Graph Supervisor Online. Protecting Neo4j state.")
    supervisor = GraphSupervisor()
    
    # Listen to the new proposal topic
    consumer = AIOKafkaConsumer(
        "sentinel.ontology.proposals",
        bootstrap_servers="kafka:9092",
        group_id="supervisor-group"
    )
    await consumer.start()
    
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode('utf-8'))
            await supervisor.execute_proposal(payload)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(start_supervisor())