"""
services/enrichment/graph_writer.py

Universal Graph Governance Choke Point:
Writes entity nodes and relationships to Neo4j via Kafka Topics.ONTOLOGY_PROPOSALS.
All Neo4j writes go through here — enrichers and agents never touch the graph DB directly.

Supported Node Types:
  - Vessel, Aircraft
  - Company (Equity), Index, Sector, Industry, MacroFactor, Commodity, Location, Organization

Supported Relationship Families:
  - Corporate & Narrative (OWNED_BY, MEMBER_OF, CUSTOMER_OF, SUPPLIER_TO, COMPETES_WITH, SYMPATHY_MOVER)
  - Macro & Regulatory (COMMODITY_EXPOSURE, FX_EXPOSURE_TO, REGULATORY_EXPOSURE, SANCTIONS_TARGET)
  - Statistical & Empirical (STATISTICALLY_CORRELATED_WITH, GRANGER_CAUSES, HAWKES_EXCITES)
"""

import logging
from typing import Optional, Dict, Any, List
from shared.kafka import SentinelProducer, Topics
from shared.models.ontology import is_valid_predicate, normalize_predicate, is_valid_node_label

logger = logging.getLogger("enrichment.graph")


class GraphWriter:

    def __init__(self, producer: SentinelProducer):
        self.producer = producer

    # ── PHYSICAL ASSETS ────────────────────────────────────────────────────────

    async def upsert_vessel(self, mmsi: str, data: Optional[Dict[str, Any]] = None):
        """Routes vessel node and flag tags to GraphSupervisor via Kafka."""
        if not mmsi:
            return
        data = data or {}
        try:
            proposal = {
                "entity_id": str(mmsi).upper(),
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Vessel",
                    "primary_domain": "maritime",
                    "name": data.get("name") or str(mmsi),
                    "macro_concepts": data.get("macro_concepts", []),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=str(mmsi))

            flags = data.get("flags", [])
            if flags:
                tag_proposal = {
                    "entity_id": str(mmsi).upper(),
                    "action": "ADD_TAGS",
                    "data": {"tags": flags, "label": "Vessel"}
                }
                await self.producer.send(Topics.ONTOLOGY_PROPOSALS, tag_proposal, key=str(mmsi))
        except Exception as e:
            logger.error(f"Failed to route vessel {mmsi} to Supervisor: {e}")

    async def upsert_aircraft(self, icao24: str, data: Optional[Dict[str, Any]] = None):
        """Routes aircraft node creation to GraphSupervisor via Kafka."""
        if not icao24:
            return
        data = data or {}
        try:
            proposal = {
                "entity_id": str(icao24).upper(),
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Aircraft",
                    "primary_domain": "aviation",
                    "name": data.get("callsign") or str(icao24),
                    "macro_concepts": data.get("macro_concepts", []),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=str(icao24))
        except Exception as e:
            logger.error(f"Failed to route aircraft {icao24} to Supervisor: {e}")

    # ── FINANCIAL & CORPORATE ENTITIES (§3.2) ──────────────────────────────────

    async def upsert_equity(self, ticker: str, data: Optional[Dict[str, Any]] = None):
        """Routes equity (Company) node creation and structural relationships to GraphSupervisor."""
        if not ticker:
            return
        ticker_sym = str(ticker).upper()
        data = data or {}
        try:
            proposal = {
                "entity_id": ticker_sym,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Company",
                    "primary_domain": "financial",
                    "name": data.get("name") or data.get("company_name") or ticker_sym,
                    "macro_concepts": data.get("macro_concepts", []),
                    "sector": data.get("sector"),
                    "industry": data.get("industry"),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=ticker_sym)

            # Link to Sector if supplied
            sector = data.get("sector")
            if sector:
                await self.upsert_sector(sector)
                await self.link_entities(
                    source_id=ticker_sym,
                    relation_type="OPERATES_IN",
                    target_id=str(sector).upper(),
                    properties={"weight": 1.0},
                    source_label="Company",
                    target_label="Sector"
                )

            # Link to Index / Benchmark if supplied (MEMBER_OF §3.5)
            indices = data.get("indices") or ([data.get("index")] if data.get("index") else [])
            for idx in indices:
                if idx:
                    await self.upsert_index(idx)
                    await self.link_entities(
                        source_id=ticker_sym,
                        relation_type="MEMBER_OF",
                        target_id=str(idx).upper(),
                        properties={"weight": 1.0},
                        source_label="Company",
                        target_label="Index"
                    )

            # Add tags if present
            tags = data.get("tags", [])
            if tags:
                await self.add_tags(ticker_sym, tags, label="Company")

        except Exception as e:
            logger.error(f"Failed to route equity {ticker} to Supervisor: {e}")

    async def upsert_index(self, index_name: str, data: Optional[Dict[str, Any]] = None):
        """Routes index/benchmark node creation to GraphSupervisor."""
        if not index_name:
            return
        idx_sym = str(index_name).upper()
        data = data or {}
        try:
            proposal = {
                "entity_id": idx_sym,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Index",
                    "primary_domain": "financial",
                    "name": data.get("name") or idx_sym,
                    "macro_concepts": data.get("macro_concepts", []),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=idx_sym)
        except Exception as e:
            logger.error(f"Failed to route index {index_name} to Supervisor: {e}")

    async def upsert_sector(self, sector_name: str, data: Optional[Dict[str, Any]] = None):
        """Routes sector node creation to GraphSupervisor."""
        if not sector_name:
            return
        sec_sym = str(sector_name).upper()
        data = data or {}
        try:
            proposal = {
                "entity_id": sec_sym,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Sector",
                    "primary_domain": "financial",
                    "name": data.get("name") or sec_sym,
                    "macro_concepts": data.get("macro_concepts", []),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=sec_sym)
        except Exception as e:
            logger.error(f"Failed to route sector {sector_name} to Supervisor: {e}")

    async def upsert_macro_factor(self, factor_name: str, data: Optional[Dict[str, Any]] = None):
        """Routes macro factor node creation (rates, commodities, FX, inflation) to GraphSupervisor."""
        if not factor_name:
            return
        fac_sym = str(factor_name).upper()
        data = data or {}
        try:
            proposal = {
                "entity_id": fac_sym,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "MacroFactor",
                    "primary_domain": "macro",
                    "name": data.get("name") or fac_sym,
                    "macro_concepts": data.get("macro_concepts", []),
                    "confidence": data.get("confidence", 1.0),
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=fac_sym)
        except Exception as e:
            logger.error(f"Failed to route macro factor {factor_name} to Supervisor: {e}")

    # ── UNIVERSAL RELATIONSHIP & TAG ROUTERS (§3.2, §3.5) ──────────────────────

    async def link_entities(
        self,
        source_id: str,
        relation_type: str,
        target_id: str,
        properties: Optional[Dict[str, Any]] = None,
        source_label: str = "Entity",
        target_label: str = "Entity"
    ):
        """
        Emits a governed relationship proposal to Topics.ONTOLOGY_PROPOSALS.
        Validates predicate against VALID_PREDICATES allowlist before emission.
        """
        if not source_id or not target_id or not relation_type:
            return
        
        normalized_rel = normalize_predicate(relation_type, default="RELATED_TO")
        properties = properties or {}
        
        try:
            link_data = {
                "target_id": str(target_id).upper(),
                "source_label": source_label if is_valid_node_label(source_label) else "Entity",
                "target_label": target_label if is_valid_node_label(target_label) else "Entity",
                "relation_type": normalized_rel,
                "weight": float(properties.get("weight", 1.0)),
                "confidence": float(properties.get("confidence", 1.0)),
                "properties": properties,
            }
            proposal = {
                "entity_id": str(source_id).upper(),
                "action": "LINK_ENTITY",
                "data": link_data
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=str(source_id).upper())
            logger.debug(f"Emitted governed link proposal: ({source_id}) -[:{normalized_rel}]-> ({target_id})")
        except Exception as e:
            logger.error(f"Failed to route link {source_id} -[{relation_type}]-> {target_id}: {e}")

    async def add_tags(self, entity_id: str, tags: List[str], label: str = "Entity"):
        """Emits an ADD_TAGS proposal to Topics.ONTOLOGY_PROPOSALS."""
        if not entity_id or not tags:
            return
        try:
            tag_proposal = {
                "entity_id": str(entity_id).upper(),
                "action": "ADD_TAGS",
                "data": {
                    "tags": tags,
                    "label": label if is_valid_node_label(label) else "Entity"
                }
            }
            await self.producer.send(Topics.ONTOLOGY_PROPOSALS, tag_proposal, key=str(entity_id).upper())
        except Exception as e:
            logger.error(f"Failed to route tags for {entity_id}: {e}")