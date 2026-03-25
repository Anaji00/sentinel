"""
services/correlation/main.py  —  run loop only.
 
Consumes enriched.events.
Runs every rule against every event.
Emits CorrelationCluster to correlations.detected when a rule fires.
 
Rules live in rules/:
  maritime.py     — MARITIME_001 (vessel dark + cross-domain)
  financial.py    — FINANCIAL_001 (options sweep + geo signals)
  aviation.py     — AVIATION_001 (emergency squawk)
  news.py         — NEWS_001 (sanctions headline + flagged vessel)
  cross_domain.py — CROSS_001 (catch-all high-anomaly cluster)
 
FIX (code review): consume loop wrapped in `while True` to guard against
  StopIteration. Although consumer_timeout_ms is no longer set (removed in
  shared/kafka), this is defensive — if something does exhaust the iterator,
  the service restarts the loop rather than silently dying.
"""