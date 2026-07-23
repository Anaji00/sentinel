"""
services/enrichment/graph_writer.py

Writes entity nodes and relationships to Neo4j.
All Neo4j writes go through here — enrichers never touch the graph DB directly.

Node types: Vessel, Aircraft
Relationship types: FLAGGED_AS

Phase 2 additions:
  OWNED_BY   (vessel → company ownership chain)
  CONNECTED_TO (company → company, company → country)
  REGISTERED_TO (vessel → flag state)
"""

import logging
from shared.kafka import SentinelProducer, Topics

logger = logging.getLogger("enrichment.graph")


class GraphWriter:

    def __init__(self, producer: SentinelProducer):
        self.producer = producer

    async def upsert_vessel(self, mmsi: str, data: dict):
        """Routes structural creation to the GraphSupervisor via Kafka."""
        try:
            # 1. Create the primary Node Proposal
            proposal = {
                "entity_id": mmsi,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Vessel",
                    "primary_domain": "maritime",
                    "name": data.get("name") or mmsi,
                    "macro_concepts": [],
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=mmsi)

            # 2. Append Flags as Tags dynamically
            flags = data.get("flags", [])
            if flags:
                tag_proposal = {
                    "entity_id": mmsi,
                    "action": "ADD_TAGS",
                    "data": {"tags": flags}
                }
                await self.producer.send(Topics.ONTOLOGY_PROPOSALS, tag_proposal, key=mmsi)

        except Exception as e:
            logger.error(f"Failed to route vessel {mmsi} to Supervisor: {e}")

    async def upsert_aircraft(self, icao24: str, data: dict):
        try:
            proposal = {
                "entity_id": icao24,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Aircraft",
                    "primary_domain": "aviation",
                    "name": data.get("callsign") or icao24,
                    "macro_concepts": [],
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=icao24)
        except Exception as e:
            logger.error(f"Failed to route aircraft {icao24} to Supervisor: {e}")