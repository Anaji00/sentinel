"""
services/reasoning/context_builder.py
 
Assembles the full context package sent to Claude for scenario generation.
 
Takes a CorrelationCluster and fetches everything needed:
  - The trigger event and all supporting events (from TimescaleDB)
  - Entity ownership chains (from Neo4j)
  - Historical pattern matches (from pattern_library)
  - Recent relevant headlines
 
Output is a structured dict that scenario_generator.py passes to Claude.
"""

import json
import time
import math
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
 
from shared.db import get_timescale, get_neo4j
from shared.models import CorrelationCluster
from shared.models.events import UNRATED_EDGE_CONFIDENCE
 
logger = logging.getLogger("reasoning.context")

def _money(value) -> str:
    """A dollar figure at a unit that shows it."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.0f}k"
    return f"${v:.0f}"


def _financial_facts(evt: dict) -> str:
    """The structured facts a headline does not carry.

    Deliberately terse. This is appended to every financial row in a prompt
    already cut to a character budget, so it earns its space by carrying only
    what the prose cannot: sector, because contagion runs along it; notional,
    because premium is what a position cost and notional is what it controls,
    and on a live sweep the two differed by 34x; and strike and expiry, because
    a directional bet is not readable without them.
    """
    fin = evt.get("financial_data") or {}
    if not isinstance(fin, dict):
        return ""

    parts = []
    sector = fin.get("sector")
    if sector:
        parts.append(str(sector)[:18])

    # Appended only if it formats. A truthy-but-unparseable value such as the
    # string "abc" otherwise yields a bare "notional " that tells the model
    # less than nothing.
    notional = _money(fin.get("notional_usd"))
    if notional:
        parts.append(f"notional {notional}")

    strike, expiry = fin.get("strike"), fin.get("expiry")
    try:
        if strike is not None and expiry:
            parts.append(f"K{float(strike):g}@{str(expiry)[:10]}")
    except (TypeError, ValueError):
        pass

    iv = fin.get("implied_volatility")
    if iv:
        try:
            parts.append(f"IV{float(iv) * 100:.0f}%")
        except (TypeError, ValueError):
            pass

    return f" [{'; '.join(parts)}]" if parts else ""


class ContextBuilder:
    def __init__(self, db_client):
        # Timescale is a thread-pooled async client now
        self._db = db_client

    # Converted core build orchestrator to async
    async def build(self, cluster: CorrelationCluster) -> Dict[str, Any]:
        """Build the full context dict for a correlation cluster."""
        # Await async database calls directly
        trigger, supporting, recent_news = await asyncio.gather(
            self._fetch_event(cluster.trigger_event_id),
            self._fetch_events(cluster.supporting_event_ids),
            self._fetch_recent_news(cluster)
        )
        
        # Natively await asynchronous Redis/Neo4j graph calls
        entity_graph = await self._fetch_entity_graph(cluster.entity_ids)
        agent_intel = await self._fetch_agent_intel(cluster)
        active_bulletins = await self._fetch_active_bulletins(cluster.entity_ids)
        consensus_report = await self._fetch_consensus_report()
        
        pattern_matches = self._fetch_pattern_matches(cluster) 
        
        all_events = ([trigger] if trigger else []) + (supporting or [])
        compressed_events_table = self._compress_event_sequence(all_events)
        
        return {
            "correlation": {
                "id": cluster.correlation_id,
                "rule": cluster.rule_name,
                "tier": cluster.alert_tier.value,
                "detected_at": cluster.detected_at.isoformat(),
                "description": cluster.description,
                "tags": cluster.tags,
            },
            "trigger_event": trigger,
            "supporting_events": supporting,
            "compressed_events_table": compressed_events_table,
            "entity_graph": entity_graph,
            "historical_patterns": pattern_matches, 
            "recent_headlines": recent_news,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "agent_intel_briefs": agent_intel,
            "active_bulletins": active_bulletins,
            "consensus_analysis": consensus_report,
        }

    def _compress_event_sequence(self, events: List[Dict]) -> str:
        """
        Hierarchical Context Compression:
        Summarizes prior event states into structured tables before prompt assembly
        to reduce context consumption while preserving situational facts.
        """
        if not events:
            return "No event sequence available."

        rows = [
            "| Timestamp (UTC) | Type | Source | Entity | Region | Core Detail | Anomaly Score |",
            "| --- | --- | --- | --- | --- | --- | --- |"
        ]

        for evt in events:
            if not isinstance(evt, dict):
                continue
            ts = str(evt.get("occurred_at") or evt.get("collected_at") or "Unknown")[:19]
            etype = str(evt.get("type") or "event")
            source = str(evt.get("source") or "unknown")
            entity = str(evt.get("primary_entity_name") or evt.get("primary_entity_id") or "N/A")
            region = str(evt.get("region") or "GLOBAL")
            score = f"{float(evt.get('anomaly_score', 0.0)):.2f}"

            headline = evt.get("headline") or evt.get("summary")
            if headline:
                detail = str(headline)[:60]
                # The headline is prose and carries none of the structured
                # fields the enrichment layer works to attach. A financial
                # event always has one, so this branch made the financial
                # detail below unreachable and the model never saw a sector or
                # a notional -- the two facts that let it reason about which
                # other issuers a move should touch, and how large the position
                # actually is as opposed to what it cost.
                detail += _financial_facts(evt)
            elif evt.get("financial_data"):
                fin = evt["financial_data"]
                ticker = fin.get("ticker", "")
                prem = fin.get("premium_usd")
                detail = f"{ticker} Prem: ${prem:,.0f}" if prem else f"Financial {ticker}"
                detail += _financial_facts(evt)
            elif evt.get("vessel_data"):
                vessel = evt["vessel_data"]
                mmsi = vessel.get("mmsi", "")
                spd = vessel.get("speed_knots")
                detail = f"Vessel MMSI {mmsi} Spd: {spd}kts" if spd is not None else f"Vessel {mmsi}"
            elif evt.get("flight_data"):
                flight = evt["flight_data"]
                callsign = flight.get("callsign") or flight.get("icao24", "")
                detail = f"Flight {callsign}"
            elif evt.get("cyber_data"):
                cyb = evt["cyber_data"]
                cve = cyb.get("cve_id") or cyb.get("breach_type", "Cyber incident")
                detail = f"Cyber {cve}"
            else:
                detail = f"{etype} signal"

            detail = detail.replace("|", "/")
            entity = entity.replace("|", "/")
            
            rows.append(f"| {ts} | {etype} | {source} | {entity} | {region} | {detail} | {score} |")

        return "\n".join(rows)


    async def _fetch_event(self, event_id: str) -> Optional[Dict]:
        try:
            # CRITICAL THINKING: Parameterized Queries.
            # We use `$1` for asyncpg parameterized SQL queries to prevent SQL Injection 
            # attacks, even though these event_ids are generated internally.
            row = await self._db.query_one(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, prediction_market_data, crypto_data, cyber_data, tags
                   FROM events WHERE event_id = $1""",
                event_id
            )
            return self._serialize_row(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching event {event_id}: {e}")
            return None
        
    async def _fetch_events(self, event_ids: List[str]) -> List[Dict]:
        if not event_ids:
            return []
        try:
            # CRITICAL THINKING: Batch DB Queries.
            # Instead of looping and running `_fetch_event` N times (which creates an N+1 
            # query bottleneck), we use PostgreSQL's `ANY($1)` array operator. 
            # This fetches all supporting events in a single, fast database round-trip.
            rows = await self._db.query(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, prediction_market_data, crypto_data, cyber_data, tags
                   FROM events
                   WHERE event_id::text = ANY($1::text[])
                   ORDER BY occurred_at DESC""",
                event_ids
            )
            return [self._serialize_row(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching events {event_ids}: {e}")
            return []

    async def _fetch_entity_graph(self, entity_ids: List[str]) -> List[Dict]:
        """
        Batches and parallelizes Neo4j graph lookup for entities.
        Fetches up to 3 hops of relationships and active flags in just 2 parallel database round-trips.
        Applies edge staleness decay weighting (§3.6) to prioritize recent statistical and verified edges.
        """
        if not entity_ids:
            return []
        
        targets = list(dict.fromkeys(entity_ids))[:5]
        now_ts = time.time()
        
        try:
            neo4j_client = await get_neo4j()
            rel_task = neo4j_client.query("""
                MATCH (v) WHERE v.name IN $ids OR v.mmsi IN $ids OR v.id IN $ids
                MATCH (v)-[r*1..3]-(n)
                RETURN coalesce(v.name, v.mmsi, v.id) as entity_id,
                       type(r[0]) as rel,
                       coalesce(n.name, n.mmsi, n.id) as connected,
                       labels(n) as labels,
                       coalesce(r[0].weight, 1.0) as weight,
                       coalesce(r[0].confidence, $unrated) as confidence,
                       coalesce(r[0].last_updated, r[0].updated_at, 0) as last_updated
                LIMIT 100
            """, {"ids": targets, "unrated": UNRATED_EDGE_CONFIDENCE})
            
            flag_task = neo4j_client.query("""
                MATCH (v) WHERE v.name IN $ids OR v.mmsi IN $ids OR v.id IN $ids
                MATCH (v)-[:FLAGGED_AS]->(f:Flag)
                RETURN coalesce(v.name, v.mmsi, v.id) as entity_id, f.type as flag
                LIMIT 50
            """, {"ids": targets})
            
            rel_rows, flag_rows = await asyncio.gather(rel_task, flag_task)
            
            results = []
            
            rel_by_entity = {}
            for r in (rel_rows or []):
                ent = r["entity_id"]
                raw_updated = r.get("last_updated") or 0
                # If updated_at is epoch ms vs epoch s
                updated_s = (raw_updated / 1000.0) if raw_updated > 1e11 else float(raw_updated)
                
                # Exponential decay: 30-day half-life decay factor
                age_days = max(0.0, (now_ts - updated_s) / 86400.0) if updated_s > 0 else 60.0
                decay_factor = math.exp(-0.693 * age_days / 30.0) if age_days < 365.0 else 0.05
                base_weight = float(r.get("weight", 1.0)) * float(r.get("confidence", UNRATED_EDGE_CONFIDENCE))
                decayed_weight = round(base_weight * decay_factor, 4)
                
                r_enriched = dict(r)
                r_enriched["decayed_weight"] = decayed_weight
                rel_by_entity.setdefault(ent, []).append(r_enriched)
                
            for ent, relationships in rel_by_entity.items():
                # Sort relationships by decayed weight descending
                sorted_rels = sorted(relationships, key=lambda x: x.get("decayed_weight", 0.0), reverse=True)
                results.append({"entity_id": ent, "relationships": sorted_rels})
                
            flags_by_entity = {}
            for r in (flag_rows or []):
                ent = r["entity_id"]
                flags_by_entity.setdefault(ent, []).append(r["flag"])
                
            for ent, flags in flags_by_entity.items():
                results.append({"entity_id": ent, "flags": flags})
                
            return results
        except Exception as e:
            logger.debug(f"Error fetching batch graph for entities {targets}: {e}")
            return []
    
    async def _fetch_recent_news(self, cluster: CorrelationCluster) -> List[str]:
        """Fetch recent high-anomaly headlines related to the correlation."""
        try:
            # CRITICAL THINKING: Time-Bounding.
            # We only want news from 72 hours *before* the cluster was detected. 
            # Anything older is likely irrelevant noise to the LLM.
            cutoff = cluster.detected_at - timedelta(hours=72)
            rows = await self._db.query(
                """SELECT headline FROM events
                   WHERE type = 'headline'
                     AND anomaly_score >= 0.4
                     AND occurred_at BETWEEN $1 AND $2
                   ORDER BY anomaly_score DESC
                   LIMIT 10""",
                cutoff, cluster.detected_at
            )
            return [r["headline"] for r in rows if r.get("headline")]
        except Exception as e:
            logger.error(f"Error fetching recent news: {e}")
            return []
        

    def _fetch_pattern_matches(self, cluster: CorrelationCluster) -> List[Dict]:
        """
        Stub — returns empty list.
 
        Pattern matching is handled by PatternLibrary.find_similar() in
        reasoning/main.py, which passes results directly to
        scenario_generator.generate(). If you ever want the context
        builder to bundle patterns itself (e.g. for a standalone call),
        inject a PatternLibrary instance and call it here.
 
        TODO Phase 2: wire in PatternLibrary if the calling pattern changes.
        """
        return []
 
        
    def _serialize_row(self, row: Dict) -> Dict:
        """
        Make a DB row JSON-safe (convert datetime, parse JSONB).
        
        CODING CONVICTION: Data Normalization at the Edge.
        Python standard `json.dumps()` cannot handle raw `datetime` objects. 
        Since this data is destined for an LLM API (which requires JSON), we must 
        proactively stringify dates using ISO-8601 format to avoid runtime crashes 
        in the `scenario_generator.py` API call.
        """
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, (dict, list)):
                out[k] = v
            else:
                out[k] = v
        return out

    async def _fetch_agent_intel(self, cluster: CorrelationCluster) -> list:
        """
        Fetch recent agent-generated intel briefs from Redis.
        These are richer than raw news — already structured by the Intel Agent.
        """
        try:
            from shared.db import get_redis
            redis = await get_redis()
            
            # Fetch recent high-severity briefs
            raw = await redis.raw.get("sentinel:intel:briefs:latest")
            if raw:
                brief = json.loads(raw)
                # Check if thematically related to this cluster
                cluster_tags = set(cluster.tags)
                brief_hotspots = set(brief.get("geographic_hotspots", []))
                if not cluster_tags or cluster_tags.intersection(brief_hotspots):
                    return [brief]
        except Exception as e:
            logger.debug(f"Agent intel fetch failed: {e}")
        return []

    async def _fetch_active_bulletins(self, entity_ids: List[str]) -> List[Dict]:
        """Fetch active agent bulletins relevant to cluster entities from Redis."""
        try:
            from shared.db import get_redis
            redis = await get_redis()
            bulletins = []
            cursor = 0
            search_ids = set(e.upper() for e in (entity_ids or []) if e)
            while True:
                cursor, keys = await redis.raw.scan(cursor=cursor, match="sentinel:bulletins:*", count=50)
                if keys:
                    values = await redis.raw.mget(keys)
                    for val in values:
                        if val:
                            try:
                                data = json.loads(val if isinstance(val, str) else val.decode("utf-8"))
                                ticker = (data.get("ticker") or "").upper()
                                ent_id = (data.get("primary_entity_id") or "").upper()
                                ent_name = (data.get("primary_entity_name") or "").upper()
                                if not search_ids or ticker in search_ids or ent_id in search_ids or any(s in ent_name for s in search_ids):
                                    bulletins.append(data)
                            except Exception as e:
                                logger.debug(f"Bulletin parse warning: {e}")
                if cursor == 0:
                    break
            return bulletins[:10]
        except Exception as e:
            logger.debug(f"Bulletin fetch failed: {e}")
            return []

    async def _fetch_consensus_report(self) -> Optional[Dict]:
        """Fetch the latest consensus and contradiction analysis from Redis."""
        try:
            from shared.db import get_redis
            redis = await get_redis()
            raw = await redis.raw.get("sentinel:consensus:latest")
            if raw:
                return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception as e:
            logger.debug(f"Consensus report fetch failed: {e}")
        return None