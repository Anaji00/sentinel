"""
services/enrichment/enrichers/cyber.py
 
Phase 3 — Cyber / Infrastructure enricher.
Handles breach disclosures, exposed infrastructure, BGP anomalies,
ransomware group victim posts.
 
Data sources:
  Shodan          — exposed industrial control systems, critical infra
  BGP monitoring  — route hijacks, unusual announcements
  Ransomware feeds — .onion mirror blogs, darkfeed aggregators
  HaveIBeenPwned  — credential breaches for watched organisations
"""

# Import the logging module to output debug and error messages.
import logging
# Import datetime tools to accurately timestamp our events in UTC.
from datetime import datetime, timezone
# Import Optional to indicate that a function might return a valid object OR None.
from typing import Optional
from shared.db import get_redis
# Import the standard models (Data Schemas) shared across the SENTINEL platform.
from shared.models import NormalizedEvent, EventType, Entity, EntityType, SecurityData

# Initialize the logger specific to this cyber module.
logger = logging.getLogger("enrichment.cyber")

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Keywords that indicate an organization is part of Critical Infrastructure.
# A breach at a local bakery is low priority; a breach at a "water pipeline" is high priority.
HIGH_VALUE_ORGS = {
    "port", "terminal", "pipeline", "refinery", "grid",
    "power", "energy", "water", "rail", "airport",
    "nato", "government", "ministry", "defense",
}

ICS_VENDORS = {
    "siemens", "schneider", "rockwell", "honeywell",
    "ge", "abb", "moxa", "emerson", "yokogawa", "mitsubishi",
}

# Nation-state / APT groups tracked on ransomware blogs
APT_GROUPS = {
    "sandworm", "lazarus", "apt41", "apt28", "apt29", "apt40",
    "volt typhoon", "salt typhoon", "cl0p", "lockbit", "blackcat",
    "alphv", "hive", "royal", "play", "akira", "black basta",
}

class CyberEnricher:
    """
    Translates raw cyber security alerts into standardized NormalizedEvents.
    Routes on raw.source to the correct private handler.
    """
    def __init__(self, scorer):
        # Save the anomaly scorer instance passed into this class so we can use it later.
        self.scorer = scorer
        self.redis = get_redis()
    
    def _calculate_velocity_score(self, entity_id: str, event_category: str, threshold: int = 100) -> float:
        key = f"sentinel:velocity:{event_category}:{entity_id}"

        try:
            count = self.redis.incr(key)
            if count == 1:
                self.redis.expire(key, 60)
            
            if count < threshold:
                return 0.0
            base_score = 0.5
            scale_factor = (count - threshold) / 1000.0
            return min(1.0, base_score + scale_factor)
        except Exception as e:
            logger.error(f"Redis velocity check failed for {key}: {e}")
            return 0.0


    def enrich(self, raw) -> Optional[NormalizedEvent]:
        """
        Main entry point. Routes the raw event to the correct parser method based on
        the source of the data.
        """
        # Extract the raw JSON dictionary payload and the source identifier.
        p = raw.raw_payload
        source = raw.source

        # Route to specific handlers.
        if source in ("censys", "censys_monitor", "shadowserver_feed"):
            return self._enrich_exposure(raw, p)
        elif source in ("cisa_kev", "cisa_kev_feed"):
            return self._enrich_cisa_kev(raw, p)
        elif source in ("bgp_monitor", "ripe_ris"):
            return self._enrich_bgp(raw, p)
        elif source in ("ransomware_feed", "ransomware_live", "darkfeed"):
            return self._enrich_ransomware(raw, p)
        elif source == "breach_monitor":
            return self._enrich_breach(raw, p)
            
        return None
    
    # ── Exposed Infrastructure ────────────────────────────────────────────────

    def _enrich_exposure(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses events from Censys Search API or Shadowserver daily feeds.
        Looks for critical industrial control systems (e.g., ports 502, 102) 
        left exposed to the public internet.
        """
        # Extract standard connection details. Safely default to empty strings.
        ip = p.get("ip_address") or p.get("ip") or ""
        port = p.get("port")
        # Extract organization, lowercase it for keyword matching later.
        org = (p.get("org") or p.get("organization") or p.get("asn_name") or "").lower()
        # The specific software running (e.g., "Apache" or "SCADA Controller").
        product = p.get("product") or p.get("service") or "Unknown Service"
        # Ensure country code is a standard 2-letter uppercase string.
        country = (p.get("country_code") or "")[:2].upper()

        # If there's no IP address, this alert is useless to us, so discard it.
        if not ip:
            return None
        
        # Check if ANY of our high-value keywords are found inside the organization's name.
        # (FIX: Corrected a typo here where 'jw' was used instead of 'kw')
        is_critical = any(kw in org for kw in HIGH_VALUE_ORGS)
        is_ics_vendor = any(v in org for v in ICS_VENDORS)
        # Baseline anomaly is 0.2 (low). If it's a critical org, jump immediately to 0.6 (high).
        if is_ics_vendor:
            anomaly = 0.80
        elif is_critical:
            anomaly = 0.60
        else:
            anomaly = 0.20
        # Build the tags list for downstream correlation rules.
        tags = ["infra_exposed", raw.source]
        if is_critical:
            tags.append("critical_infrastructure")
        
        if is_ics_vendor:
            tags.append("ics_vendor")
        if p.get("protocol"):
            tags.append(p["protocol"])
        # Build the primary Entity representation of this exposed server.
        # (FIX: Corrected a typo here where 'nme' was used instead of 'name')
        entity = Entity(
            id = ip, type = EntityType.INFRASTRUCTURE,
            name = org or ip, country_code = country or None,
        )
        
        # Build and return the standardized event.
        return NormalizedEvent(
            event_id = raw.event_id,
            type = EventType.INFRA_EXPOSED,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            primary_entity = entity,
            headline=f"Exposed {product} on {ip}:{port} ({org})",
            # Put domain-specific cyber fields into the SecurityData model.
            security_data = SecurityData(
                exposure_type = "open_port",
                ip_address = ip,
                affected_org = org,
                port = port,
            ), 
            tags = tags,
            anomaly_score = anomaly,
        )
        
    # ── BGP Anomaly ───────────────────────────────────────────────────────────

    def _enrich_cisa_kev(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses events from CISA KEV (Known Exploited Vulnerabilities) feed.
        These are critical vulnerabilities that have been observed being actively exploited in the wild.
        """
        # Extract the CVE identifier (e.g., CVE-2021-44228).
        cve_id = p.get("cve_id") or p.get("cveID") or p.get("cve") or ""
        # Extract the name of the affected software or system.
        product = (p.get("product") or "").lower()
        # Extract the organization responsible for the vulnerable product.
        vendor = (p.get("vendor") or p.get("vendorProject") or "").lower()
        # Extract the CVSS score, which indicates severity (0.0 to 10.0).
        vuln_name = p.get("vulnerabilityName", "Unknown Vulnerability")
        ransomware_use = (p.get("ransomware_use") or "Unknown")
        if not cve_id:
            return None

        is_ics_vendor  = any(v in vendor for v in ICS_VENDORS)
        is_critical_org = any(kw in vendor or kw in product for kw in HIGH_VALUE_ORGS)
        is_ransomware  = str(ransomware_use).lower() == "known"
        
        if is_ics_vendor and is_ransomware:
            anomaly = 0.95
        elif is_ics_vendor:
            anomaly = 0.88
        elif is_ransomware:
            anomaly = 0.85
        elif is_critical_org:
            anomaly = 0.80
        else:
            anomaly = 0.70      # Floor: confirmed exploitation warrants ALERT tier
        tags = ["cve_kev", "exploited_vuln", "cve"]
        if is_critical_org:
            tags.append("critical_infrastructure")
        if is_ics_vendor:
            tags.append("ics_vendor")
        if is_ransomware:
            tags.append("ransomware_linked")
        entity = Entity(
            id=cve_id or vendor, type=EntityType.COMPANY,
            name=vendor.title() or "Unknown Vendor",
            country_code= "US" if vendor else None,  # Assume US-based if vendor is known, else None
        )
        
        event_type = getattr(EventType, "VULNERABILITY", EventType.INFRA_EXPOSED)
        return NormalizedEvent(
            event_id=raw.event_id,
            type=event_type,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            headline=f"KEV Added: {cve_id} — {vendor} {product} (ransomware:{ransomware_use})",
            security_data=SecurityData(
                breach_type="known_exploited_vulnerability",
                affected_org=vendor,
                cve_id=cve_id,
                data_types = [cve_id, vuln_name]
            ),
            tags=tags,
            anomaly_score=anomaly,
        )
    def _enrich_bgp(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses Border Gateway Protocol (BGP) alerts. BGP is how internet traffic is routed.
        A "Hijack" means an unauthorized entity is stealing or rerouting internet traffic.
        """
        # Extract the IP address block affected (e.g., 192.168.0.0/24).
        prefix   = p.get("prefix", "")
        # Extract the Autonomous System Number (ASN) claiming to own the IP block.
        origin   = p.get("origin_as", "")
        # Boolean flag indicating if the system suspects this is a malicious hijack.
        hijack   = p.get("is_hijack", False)
        country  = (p.get("country_code") or "")[:2].upper()

        if not prefix:
            return None
        
        if hijack:
            anomaly = 0.8
        else:
            anomaly = self._calculate_velocity_score(f"AS{origin}", "bgp", threshold=100)

            if anomaly == 0.0:
                return None
        tags = ["bgp_anomaly", "routing"]
        if hijack:
            tags.append("bgp_hijack")

        # Represent the Autonomous System as the primary entity.
        # (FIX: Corrected a typo here where 'INFASTRUCTURE' was used instead of 'INFRASTRUCTURE')
        entity = Entity(
            id=f"AS{origin}", type = EntityType.INFRASTRUCTURE,
            name=p.get("as_name", f"AS{origin}"), country_code=country or None,
        )

        # Build and return the standardized event.
        return NormalizedEvent(
            event_id = raw.event_id,
            type = EventType.BGP_ANOMALY,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            primary_entity = entity,
            headline=f"BGP {'hijack' if hijack else 'anomaly'}: {prefix} via AS{origin}",
            security_data = SecurityData(
                breach_type = "bgp_hijack" if hijack else "bgp_anomaly",
                affected_org = p.get("as_name", ""),
                ip_address=prefix
            ),
            tags = tags,
            country_code = country or None,
            anomaly_score = anomaly
        )
        
    # ── Ransomware Victim Post ───────────────────────────────────────────────
    def _enrich_ransomware(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses events from dark web monitors that scrape ransomware leak sites 
        (e.g., LockBit or LockBit posting a new victim).
        """
        # Safely extract the target company.
        victim = (p.get("victim") or p.get("company") or "").lower()
        # Extract the name of the hacking group.
        group = p.get("group", "Unknown")
        country = (p.get("country") or p.get("country_code") or "")[:2].upper()
        sector = (p.get("sector") or p.get("activity") or "").lower()

        if not victim:
            return None
        
        # Check if the victim's name or industry sector matches our critical list.
        is_critical = any(kw in victim or kw in sector for kw in HIGH_VALUE_ORGS)
        # Standard ransomware is 0.45. Critical infrastructure ransomware is 0.75.
        is_apt = any(apt in group.lower() for apt in APT_GROUPS)
        if is_apt:
            anomaly = 0.95
        elif is_critical:
            anomaly = 0.75
        else:
            anomaly = 0.45
        tags = ["ransomware", group.lower().replace(" ", "_")]
        if is_critical:
            tags.append("critical_infrastructure")
        if is_apt:
            tags.append("apt_group")

 
        # Build the Company entity that got attacked.
        entity = Entity(
            id=victim, type=EntityType.COMPANY,
            name=victim, country_code=country or None,
        )
 
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.RANSOMWARE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            headline=f"{group} ransomware: {victim} ({sector})",
            security_data=SecurityData(
                breach_type="ransomware",
                affected_org=victim,
                data_types=p.get("data_types", []),
            ),
            tags=tags,
            country_code=country or None,
            anomaly_score=anomaly,
        )
     
    # ── Credential Breach ─────────────────────────────────────────────────────
    def _enrich_breach(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses massive database leaks (e.g., a SQL database dumped on a forum).
        """
        # Extract the affected organization.
        org     = (p.get("org") or p.get("name") or "").lower()
        # Extract the number of rows/users leaked. Cast safely to integer.
        records = int(p.get("record_count") or 0)
        country = (p.get("country_code") or "")[:2].upper()
        
        if not org:
            return None
        
        is_critical = any(kw in org for kw in HIGH_VALUE_ORGS)
        
        # Dynamic Scoring: Start with 0.3 base score, add 0.4 for every 10 million records leaked.
        # Use min(1.0) so the score mathematically cannot exceed 1.0.
        anomaly     = min(1.0, 0.3 + (records / 10_000_000) * 0.4)
        if is_critical:
            # Multiply by 1.5 if it's a critical org (again, capping at 1.0 max).
            anomaly = min(1.0, anomaly * 1.5)
        
        entity = Entity(
            id=org, type=EntityType.COMPANY,
            name=org, country_code=country or None,
        )
        
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.BREACH_DETECTED,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            # Format the record count with commas for readability (e.g. 10,000,000)
            headline=f"Data breach: {org} — {records:,} records",
            security_data=SecurityData(
                breach_type="data_breach",
                affected_org=org,
                record_count=records,
                data_types=p.get("data_types", []),
            ),
            tags=["breach_detected", "data_breach"],
            country_code=country or None,
            anomaly_score=round(anomaly, 3),  
        )
    