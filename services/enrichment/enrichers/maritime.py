"""
services/enrichment/enrichers/maritime.py
 
THE TRANSLATOR
==============
Converts raw AIS (Automatic Identification System) messages into rich,
structured events that the rest of the system can understand.
"""
 
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, List
 
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
from shared.utils.regions import classify_region, decode_nav_status, decode_vessel_type
from shared.utils.sanctions import check_sanctions
 
logger = logging.getLogger("enrichment.maritime")
 
class MaritimeEnricher:
    # ── STRICT DI ALIGNMENT ──
    def __init__(self, scorer, redis_client, graph_writer, resolver=None):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer
        self.resolver = resolver

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        # Backward compatibility for direct calls
        res = await self.enrich_batch([raw])
        return res[0] if res else None
        
    async def enrich_batch(self, events: list) -> list:
        if not events: return []
        
        positions = []
        statics = []
        for raw in events:
            payload = raw.raw_payload
            msg_type = payload.get("MessageType", "")
            if msg_type == "PositionReport":
                positions.append(raw)
            elif msg_type == "ShipStaticData":
                statics.append(raw)
                
        results = []
        if statics:
            tasks = [self._static(e, e.raw_payload, e.raw_payload.get("MetaData", {}), str(e.raw_payload.get("MetaData", {}).get("MMSI", "")).strip()) for e in statics]
            s_res = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend([r for r in s_res if isinstance(r, NormalizedEvent)])
            
        if positions:
            p_res = await self._position_batch(positions)
            results.extend(p_res)
            
        return results
    
    # ── Position ──────────────────────────────────────────────────────────────

    async def _position_batch(self, events: list) -> list:
        parsed = []
        mmsi_list = []
        meta_list = []
        for raw in events:
            payload = raw.raw_payload
            meta = payload.get("MetaData", {})
            mmsi = str(meta.get("MMSI", "")).strip()
            
            if not mmsi or mmsi == "0": continue
            
            pos = payload.get("Message", {}).get("PositionReport", {})
            lat = pos.get("Latitude")
            lon = pos.get("Longitude")
            if lat is None or lon is None: continue
            
            speed = float(pos.get("Sog") or 0)
            heading = int(pos.get("TrueHeading") or 0)
            nav_status = decode_nav_status(pos.get("NavigationalStatus") or 0)
            region = classify_region(lat, lon)
            
            parsed.append((raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, region))
            mmsi_list.append(mmsi)
            meta_list.append(meta)
            
        if not parsed: return []
        
        # Batch resolve vessels
        if self.resolver and hasattr(self.resolver, "resolve_vessel_batch"):
            vessels = await self.resolver.resolve_vessel_batch(mmsi_list, meta_list)
        else:
            vessels = []
            for m, mt in zip(mmsi_list, meta_list):
                vessels.append(await self._get_vessel(m, mt))
                
        features_list = []
        entities = []
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, region), vessel in zip(parsed, vessels):
            flags = vessel.get("flags", [])
            is_sanctioned = 1.0 if any("sanctioned" in f.lower() for f in flags) else 0.0
            features_list.append([speed, is_sanctioned, 0.5, 0.0, 0.0])
            entities.append(mmsi)
            
        scores = await self.scorer.score_event_batch("vessel_position", entities, features_list)
        
        results = []
        pipe = self.redis.raw.pipeline()
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, region), vessel, score_dict in zip(parsed, vessels, scores):
            anomaly = score_dict.get("score", 0.0)
            flags = vessel.get("flags", [])
            vtype = vessel.get("vessel_type", "Unknown")
            
            pipe.set(
                f"vessel:last_seen:{mmsi}",
                json.dumps({
                    "lat": lat, "lon": lon, "heading": heading,
                    "region": region, "speed": speed, "ts": (raw.occurred_at or datetime.now(timezone.utc)).isoformat(),
                }),
                ex = 172800 
            )
            
            results.append((raw, meta, mmsi, lat, lon, speed, heading, nav_status, region, vessel, flags, vtype, anomaly))
            
        await pipe.execute()
        
        # Batch graph updates
        graph_tasks = []
        for (_, _, mmsi, _, _, _, _, _, _, vessel, flags, vtype, _) in results:
            graph_tasks.append(self.graph.upsert_vessel(mmsi, {
                "name": vessel.get("name", ""),
                "vessel_type": vtype,
                "flag_state": vessel.get("flag_state", ""),
                "flags": flags,
            }))
            
        if graph_tasks:
            await asyncio.gather(*graph_tasks, return_exceptions=True)
            
        final_events = []
        for (raw, meta, mmsi, lat, lon, speed, heading, nav_status, region, vessel, flags, vtype, anomaly) in results:
            final_events.append(NormalizedEvent(
                event_id = raw.event_id,
                type = EventType.VESSEL_POSITION,
                occurred_at = raw.occurred_at or datetime.now(timezone.utc),
                source = raw.source,
                primary_entity = Entity(
                    id = mmsi, type=EntityType.VESSEL, 
                    name=vessel.get("name") or meta.get("ShipName", ""),
                    flags=flags,
                ),
                latitude = lat,
                longitude = lon,
                region = region,
                vessel_data = VesselData(
                    mmsi=mmsi, 
                    speed_knots = speed,
                    heading=heading,
                    nav_status=nav_status,
                    vessel_type = vtype,
                    flag_state = vessel.get("flag_state"),
                    destination = vessel.get("destination"),
                ),
                tags = self._tags(region, vtype, flags),
                anomaly_score = anomaly,
            ))
            
        return final_events
    
    # ── Static ────────────────────────────────────────────────────────────────
    async def _static(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        if not mmsi or mmsi == "0": return None
        s = payload.get("Message", {}).get("ShipStaticData", {})
        name = str(s.get("Name", meta.get("ShipName", ""))).strip()
        dest = str(s.get("Destination", "")).strip()
        code = int(s.get("Type") or 0)
        vtype = decode_vessel_type(code)
        
        flags = check_sanctions(name, mmsi)

        await self.redis.raw.set(
            f"vessel:info:{mmsi}",
            json.dumps({ "name": name, "destination": dest,
                        "vessel_type": vtype, "flags": flags }),
            ex = 864000 
        )
        await self.graph.upsert_vessel(mmsi, {"name": name, "vessel_type": vtype, "flags": flags})

        return NormalizedEvent(
            event_id = raw.event_id,
            type = EventType.VESSEL_STATIC,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            primary_entity = Entity(id=mmsi, type=EntityType.VESSEL, name=name, flags=flags),
            vessel_data = VesselData(mmsi=mmsi, vessel_type=vtype, destination=dest, cargo_type=code),
            tags = [vtype.lower(), "static_data"],
            anomaly_score = 0.0,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def _get_vessel(self, mmsi: str, meta: dict) -> dict:
        if self.resolver:
            try:
                return await self.resolver.resolve_vessel(mmsi, ais_meta=meta)
            except Exception as e:
                logger.debug(f"Resolver failed for {mmsi}: {e}")
        
        cached = await self.redis.raw.get(f"vessel:info:{mmsi}")
        if cached:
            return json.loads(cached)
        name = meta.get("ShipName", "")
        return {
            "name":        name,
            "vessel_type": "Unknown",
            "flags":       check_sanctions(name, mmsi),
            "flag_state":  "",
        }
 
    def _tags(self, region, vtype, flags) -> list:
        tags = []
        if region:
            tags.append(region.lower().replace(" ", "_"))
        if vtype and vtype != "Unknown":
            tags.append(vtype.lower())
        if any("sanctioned" in f for f in flags):
            tags.append("sanctions_risk")
        return tags