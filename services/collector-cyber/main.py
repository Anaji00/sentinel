"""
services/collector-cyber/main.py
 
CYBER / INFRASTRUCTURE COLLECTOR — Phase 3
==========================================
Monitors: breach disclosures, exposed infrastructure, BGP anomalies,
ransomware victim postings.
Pushes RawEvents to Kafka topic: raw.cyber
 
Data sources:
  Ransomware.live   — aggregates ransomware group victim posts
    https://api.ransomware.live/recentvictims  (free, no auth)
 
  RIPE RIS BGP      — BGP route change stream
    https://ris-live.ripe.net/v1/stream/  (free WebSocket)
 
# Instead of Shodan, poll these three:
# 1. Censys Search API (250 queries/day free)
#    - query: "services.port:502 OR services.port:102"  (industrial protocols)
#    - cross-check against your watchlist of org names

# 2. CISA KEV JSON feed (free, no auth)
#    - https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
#    - poll daily, emit event when new CVE added

# 3. Shadowserver daily feeds (free, register your ASNs)
#    - automatic email reports → parse and ingest
 
  Breach feeds      — free RSS aggregators
"""
