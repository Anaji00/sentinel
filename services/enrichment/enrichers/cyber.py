"""
services/enrichment/enrichers/cyber.py
 
Phase 3 — Cyber / Infrastructure enricher.
Handles breach disclosures, exposed infrastructure, BGP anomalies,
ransomware group victim posts.
"""
import logging
import json
from datetime import datetime, timezone
from typing import Optional, List

from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, SecurityData
from shared.models.events import RawEvent
from shared.utils.sanctions import check_sanctions

logger = logging.getLogger("enrichment.cyber")

HIGH_VALUE_ORGS = {
    "port", "terminal", "pipeline", "refinery", "grid",
    "power", "energy", "water", "rail", "airport",
    "nato", "government", "ministry", "defense",
}

ICS_VENDORS = {
    "siemens", "schneider", "rockwell", "honeywell",
    "ge", "abb", "moxa", "emerson", "yokogawa", "mitsubishi",
}

APT_GROUPS = {
    "sandworm", "lazarus", "apt41", "apt28", "apt29", "apt40",
    "volt typhoon", "salt typhoon", "cl0p", "lockbit", "blackcat",
    "alphv", "hive", "royal", "play", "akira", "black basta",
}

class CyberThreatScorer:
    """Deterministic, rule-based engine that fetches thresholds from Redis."""
    def __init__(self, redis_client):
        self.redis = redis_client
        self.cfg = {}

    async def load_thresholds(self):
        try:
            if hasattr(self.redis, "raw"):
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    loaded = json.loads(raw_cfg)
                    # Support both nested cyber_* or direct definitions
                    self.cfg = loaded.get("cyber", loaded)
        except Exception as e:
            logger.debug(f"Could not load custom cyber thresholds: {e}")

    def score_exposure(self, is_critical: bool, is_ics: bool) -> float:
        c = self.cfg.get("cyber_exposure", {"ics_score": 0.80, "critical_score": 0.60, "base_score": 0.20})
        if is_ics: return c.get("ics_score", 0.80)
        if is_critical: return c.get("critical_score", 0.60)
        return c.get("base_score", 0.20)

    def score_cve(self, is_critical: bool, is_ics: bool, is_ransomware: bool) -> float:
        c = self.cfg.get("cyber_cve", {"ics_ransomware": 0.95, "ics_score": 0.88, "ransomware": 0.85, "critical": 0.80, "base": 0.70})
        if is_ics and is_ransomware: return c.get("ics_ransomware", 0.95)
        if is_ics: return c.get("ics_score", 0.88)
        if is_ransomware: return c.get("ransomware", 0.85)
        if is_critical: return c.get("critical", 0.80)
        return c.get("base", 0.70)

    def score_bgp(self, is_hijack: bool, velocity_score: float) -> float:
        c = self.cfg.get("cyber_bgp", {"hijack": 0.90})
        if is_hijack: return c.get("hijack", 0.90)
        return velocity_score

    def score_ransomware(self, is_critical: bool, is_apt: bool) -> float:
        c = self.cfg.get("cyber_ransomware", {"apt": 0.95, "critical": 0.75, "base": 0.45})
        if is_apt: return c.get("apt", 0.95)
        if is_critical: return c.get("critical", 0.75)
        return c.get("base", 0.45)

    def score_breach(self, records: int, is_critical: bool) -> float:
        c = self.cfg.get("cyber_breach", {"base": 0.3, "record_multiplier": 0.4, "record_divisor": 10000000.0, "critical_multiplier": 1.5})
        anomaly = min(1.0, c.get("base", 0.3) + (records / c.get("record_divisor", 1e7)) * c.get("record_multiplier", 0.4))
        if is_critical:
            anomaly = min(1.0, anomaly * c.get("critical_multiplier", 1.5))
        return round(anomaly, 3)

class CyberEnricher:
    """
    Translates raw cyber security alerts into standardized NormalizedEvents.
    Provides batch processing interfaces to prevent downstream floods.
    """
    def __init__(self, scorer, redis_client, graph_writer, resolver=None):
        # We ignore the ML 'scorer' passed from main.py and use our specialized threat scorer.
        self.graph = graph_writer
        self.redis = redis_client
        self.cyber_scorer = CyberThreatScorer(redis_client)
    
    async def _calculate_velocity_batch(self, category: str, entity_ids: list, threshold: int = 100) -> dict:
        """Processes Redis velocity hits in a single bulk pipeline."""
        if not entity_ids:
            return {}
        
        try:
            pipe = self.redis.raw.pipeline()
            for e_id in entity_ids:
                key = f"sentinel:velocity:{category}:{e_id}"
                pipe.incr(key)
            
            counts = await pipe.execute()
            
            # We also need to set expiry on newly created keys
            expire_pipe = self.redis.raw.pipeline()
            for i, count in enumerate(counts):
                if count == 1:
                    key = f"sentinel:velocity:{category}:{entity_ids[i]}"
                    expire_pipe.expire(key, 60)
            
            if len(expire_pipe.command_stack) > 0:
                await expire_pipe.execute()

            results = {}
            for i, count in enumerate(counts):
                if count < threshold:
                    results[entity_ids[i]] = 0.0
                else:
                    base_score = 0.5
                    scale_factor = (count - threshold) / 1000.0
                    results[entity_ids[i]] = min(1.0, base_score + scale_factor)
            
            return results
        except Exception as e:
            logger.error(f"Redis velocity check failed for batch: {e}")
            return {eid: 0.0 for eid in entity_ids}

    async def _propose_ontology_batch(self, proposals: list):
        """Sends ontology proposals to Kafka asynchronously."""
        for p in proposals:
            try:
                await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                    "entity_id": p["entity_id"],
                    "action": "MERGE_ONTOLOGY_NODE",
                    "data": {"label": p["label"], "primary_domain": "cyber", "confidence": p["confidence"]}
                }, key=p["entity_id"])
            except Exception as e:
                logger.error(f"Failed to propose ontology for {p['entity_id']}: {e}")

    async def enrich(self, raw: RawEvent) -> Optional[NormalizedEvent]:
        """Legacy single-event enrichment. Useful for fallback."""
        res = await self.enrich_batch([raw])
        return res[0] if res else None

    async def enrich_batch(self, events: List[RawEvent]) -> List[NormalizedEvent]:
        """
        Main bulk entry point. Groups events by type, performs pipelined 
        external queries (Redis/Velocity), and yields normalized events.
        """
        if not events: return []
        
        await self.cyber_scorer.load_thresholds()
        
        # Categorize events
        exposures, kevs, bgps, ransomwares, breaches = [], [], [], [], []
        
        for raw in events:
            src = raw.source
            if src in ("censys", "censys_monitor", "shadowserver_feed"): exposures.append(raw)
            elif src in ("cisa_kev", "cisa_kev_feed"): kevs.append(raw)
            elif src in ("bgp_monitor", "ripe_ris"): bgps.append(raw)
            elif src in ("ransomware_feed", "ransomware_live", "darkfeed"): ransomwares.append(raw)
            elif src == "breach_monitor": breaches.append(raw)
            
        normalized = []
        ontology_proposals = []
        
        # ── Process BGP ──────────────────────────────────────────────────────────
        bgp_entities = [f"AS{e.raw_payload.get('origin_as', '')}" for e in bgps if e.raw_payload.get('origin_as')]
        bgp_velocities = await self._calculate_velocity_batch("bgp", bgp_entities, threshold=50)

        for raw in bgps:
            p = raw.raw_payload
            prefix = p.get("prefix", "")
            origin = p.get("origin_as", "")
            hijack = p.get("is_hijack", False)
            country = (p.get("country_code") or "")[:2].upper()
            as_name = p.get("as_name", f"AS{origin}")
            
            if not prefix: continue
            
            is_critical = any(kw in as_name.lower() for kw in HIGH_VALUE_ORGS)
            if not hijack and not is_critical: continue
            
            entity_id = f"AS{origin}"
            vel = bgp_velocities.get(entity_id, 0.0) if not hijack else 0.0
            anomaly = self.cyber_scorer.score_bgp(hijack, vel)
            if anomaly == 0.0: continue

            tags = ["bgp_anomaly", "routing", "bgp_hijack"] if hijack else ["bgp_anomaly", "routing"]
            
            entity = Entity(
                id=entity_id, type=EntityType.INFRASTRUCTURE,
                name=as_name, country_code=country or None,
                flags=check_sanctions(as_name, "")
            )
            ontology_proposals.append({"entity_id": entity_id, "label": "Infrastructure", "confidence": anomaly})
            
            normalized.append(NormalizedEvent(
                event_id=raw.event_id, type=EventType.BGP_ANOMALY,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source, primary_entity=entity,
                headline=f"BGP {'hijack' if hijack else 'anomaly'}: {prefix} via AS{origin}",
                security_data=SecurityData(breach_type="bgp_hijack" if hijack else "bgp_anomaly", affected_org=as_name, ip_address=prefix),
                tags=tags, country_code=country or None, anomaly_score=anomaly
            ))
            
        # ── Process Exposures ────────────────────────────────────────────────────
        # Group exposures by IP address
        grouped_exposures = {}
        for raw in exposures:
            p = raw.raw_payload
            ip = p.get("ip_address") or p.get("ip") or ""
            if not ip: continue
            if ip not in grouped_exposures:
                grouped_exposures[ip] = []
            grouped_exposures[ip].append(raw)
            
        for ip, exp_list in grouped_exposures.items():
            # Use the first event for base details
            base_raw = exp_list[0]
            p = base_raw.raw_payload
            
            org = (p.get("org") or p.get("organization") or p.get("asn_name") or "").lower()
            country = (p.get("country_code") or "")[:2].upper()
            
            ports = [str(e.raw_payload.get("port")) for e in exp_list if e.raw_payload.get("port")]
            products = [e.raw_payload.get("product") or e.raw_payload.get("service") or "Unknown" for e in exp_list]
            
            # Unique lists
            ports = list(set(ports))
            products = list(set(products))
            
            is_critical = any(kw in org for kw in HIGH_VALUE_ORGS)
            is_ics_vendor = any(v in org for v in ICS_VENDORS)
            
            # Guard against scoring massive feeds of useless endpoints
            if is_critical or is_ics_vendor:
                anomaly = self.cyber_scorer.score_exposure(is_critical, is_ics_vendor)
            else:
                anomaly = 0.20
                
            tags = ["infra_exposed", base_raw.source]
            if is_critical: tags.append("critical_infrastructure")
            if is_ics_vendor: tags.append("ics_vendor")
            for raw in exp_list:
                proto = raw.raw_payload.get("protocol")
                if proto and proto not in tags: tags.append(proto)
            
            entity = Entity(
                id=ip, type=EntityType.INFRASTRUCTURE,
                name=org or ip, country_code=country or None,
                flags=check_sanctions(org or ip, "")
            )
            ontology_proposals.append({"entity_id": ip, "label": "Infrastructure", "confidence": anomaly})
            
            ports_str = ",".join(ports)
            products_str = " / ".join(products)
            
            normalized.append(NormalizedEvent(
                event_id=base_raw.event_id, type=EventType.INFRA_EXPOSED,
                occurred_at=base_raw.occurred_at or datetime.now(timezone.utc),
                source=base_raw.source, primary_entity=entity,
                headline=f"Exposed {products_str} on {ip} (Ports: {ports_str}) ({org})",
                security_data=SecurityData(exposure_type="open_port", ip_address=ip, affected_org=org, port=ports[0] if ports else None),
                tags=tags, anomaly_score=anomaly
            ))
            
        # ── Process KEVs ─────────────────────────────────────────────────────────
        for raw in kevs:
            p = raw.raw_payload
            cve_id = p.get("cve_id") or p.get("cveID") or p.get("cve") or ""
            product = (p.get("product") or "").lower()
            vendor = (p.get("vendor") or p.get("vendorProject") or "").lower()
            vuln_name = p.get("vulnerabilityName", "Unknown Vulnerability")
            ransomware_use = (p.get("ransomware_use") or "Unknown")
            if not cve_id: continue
            
            is_ics_vendor = any(v in vendor for v in ICS_VENDORS)
            is_critical_org = any(kw in vendor or kw in product for kw in HIGH_VALUE_ORGS)
            is_ransomware = str(ransomware_use).lower() == "known"
            
            anomaly = self.cyber_scorer.score_cve(is_critical_org, is_ics_vendor, is_ransomware)
            
            tags = ["cve_kev", "exploited_vuln", "cve"]
            if is_critical_org: tags.append("critical_infrastructure")
            if is_ics_vendor: tags.append("ics_vendor")
            if is_ransomware: tags.append("ransomware_linked")
            
            entity = Entity(
                id=cve_id or vendor, type=EntityType.COMPANY,
                name=vendor.title() or "Unknown Vendor",
                country_code="US" if vendor else None,
                flags=check_sanctions(vendor, "")
            )
            ontology_proposals.append({"entity_id": cve_id or vendor, "label": "Vulnerability", "confidence": anomaly})
            
            event_type = getattr(EventType, "VULNERABILITY", EventType.INFRA_EXPOSED)
            normalized.append(NormalizedEvent(
                event_id=raw.event_id, type=event_type,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source, primary_entity=entity,
                headline=f"KEV Added: {cve_id} — {vendor} {product} (ransomware:{ransomware_use})",
                security_data=SecurityData(breach_type="known_exploited_vulnerability", affected_org=vendor, cve_id=cve_id, data_types=[cve_id, vuln_name]),
                tags=tags, anomaly_score=anomaly
            ))
            
        # ── Process Ransomwares ──────────────────────────────────────────────────
        for raw in ransomwares:
            p = raw.raw_payload
            victim = (p.get("victim") or p.get("company") or "").lower()
            group = p.get("group", "Unknown")
            country = (p.get("country") or p.get("country_code") or "")[:2].upper()
            sector = (p.get("sector") or p.get("activity") or "").lower()
            if not victim: continue
            
            is_critical = any(kw in victim or kw in sector for kw in HIGH_VALUE_ORGS)
            is_apt = any(apt in group.lower() for apt in APT_GROUPS)
            
            anomaly = self.cyber_scorer.score_ransomware(is_critical, is_apt)
            
            tags = ["ransomware", group.lower().replace(" ", "_")]
            if is_critical: tags.append("critical_infrastructure")
            if is_apt: tags.append("apt_group")
            
            entity = Entity(
                id=victim, type=EntityType.COMPANY,
                name=victim, country_code=country or None,
                flags=check_sanctions(victim, "")
            )
            ontology_proposals.append({"entity_id": victim, "label": "Company", "confidence": anomaly})
            
            normalized.append(NormalizedEvent(
                event_id=raw.event_id, type=EventType.RANSOMWARE,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source, primary_entity=entity,
                headline=f"{group} ransomware: {victim} ({sector})",
                security_data=SecurityData(breach_type="ransomware", affected_org=victim, data_types=p.get("data_types", [])),
                tags=tags, country_code=country or None, anomaly_score=anomaly
            ))
            
        # ── Process Breaches ─────────────────────────────────────────────────────
        for raw in breaches:
            p = raw.raw_payload
            org = (p.get("org") or p.get("name") or "").lower()
            records = int(p.get("record_count") or 0)
            country = (p.get("country_code") or "")[:2].upper()
            if not org: continue
            
            is_critical = any(kw in org for kw in HIGH_VALUE_ORGS)
            anomaly = self.cyber_scorer.score_breach(records, is_critical)
            
            entity = Entity(
                id=org, type=EntityType.COMPANY,
                name=org, country_code=country or None,
                flags=check_sanctions(org, "")
            )
            ontology_proposals.append({"entity_id": org, "label": "Company", "confidence": anomaly})
            
            normalized.append(NormalizedEvent(
                event_id=raw.event_id, type=EventType.BREACH_DETECTED,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source, primary_entity=entity,
                headline=f"Data breach: {org} — {records:,} records",
                security_data=SecurityData(breach_type="data_breach", affected_org=org, record_count=records, data_types=p.get("data_types", [])),
                tags=["breach_detected", "data_breach"], country_code=country or None, anomaly_score=anomaly
            ))
            
        # ── Bulk dispatch ────────────────────────────────────────────────────────
        if ontology_proposals:
            await self._propose_ontology_batch(ontology_proposals)
            
        return normalized