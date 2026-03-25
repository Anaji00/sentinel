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

logger = logging.getLogger("enrichment.graph")


class GraphWriter:

    def __init__(self, neo4j_client):
        self.neo4j = neo4j_client

    def upsert_vessel(self, mmsi: str, data: dict):
        """
        Create or update a Vessel node.
        MERGE on mmsi so re-running on the same vessel updates in place.
        Adds FLAGGED_AS edges for each flag string.
        """
        try:
            self.neo4j.execute("""
                MERGE (v:Vessel {mmsi: $mmsi})
                SET v.name        = $name,
                    v.vessel_type = $vessel_type,
                    v.flag_state  = $flag_state,
                    v.updated_at  = datetime()
            """, {
                "mmsi":        mmsi,
                "name":        data.get("name", ""),
                "vessel_type": data.get("vessel_type", ""),
                "flag_state":  data.get("flag_state", ""),
            })

            for flag in data.get("flags", []):
                self.neo4j.execute("""
                    MERGE (v:Vessel {mmsi: $mmsi})
                    MERGE (f:Flag {type: $flag})
                    MERGE (v)-[:FLAGGED_AS]->(f)
                """, {"mmsi": mmsi, "flag": flag})

        except Exception as e:
            logger.error(f"Neo4j vessel upsert failed ({mmsi}): {e}")

    def upsert_aircraft(self, icao24: str, data: dict):
        """Create or update an Aircraft node."""
        try:
            self.neo4j.execute("""
                MERGE (a:Aircraft {icao24: $icao24})
                SET a.callsign       = $callsign,
                    a.origin_country = $origin_country,
                    a.updated_at     = datetime()
            """, {
                "icao24":         icao24,
                "callsign":       data.get("callsign"),
                "origin_country": data.get("origin_country"),
            })
        except Exception as e:
            logger.error(f"Neo4j aircraft upsert failed ({icao24}): {e}")