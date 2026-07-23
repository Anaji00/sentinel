import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=True)

os.environ["POSTGRES_HOST"] = "127.0.0.1"
os.environ["REDIS_HOST"] = "127.0.0.1"
os.environ["NEO4J_URI"] = "bolt://127.0.0.1:7687"

from shared.db import get_timescale, get_redis, get_neo4j

async def seed():
    print("🌱 Seeding Live Multi-Domain Events, Graph Nodes & Correlations into Sentinel Pipeline...")
    
    db = await get_timescale()
    neo4j = await get_neo4j()
    redis = await get_redis()
    
    now = datetime.now(timezone.utc).isoformat()

    # 1. Real Events across all 5 domains
    events_data = [
        {
            "event_id": "evt_maritime_101",
            "type": "ais_gap_dark_vessel",
            "source": "collector.ais",
            "occurred_at": now,
            "primary_entity_id": "MMSI_235091234",
            "primary_entity_name": "TANKER_HORMUZ_01",
            "region": "Strait of Hormuz",
            "anomaly_score": 0.88,
            "summary": "AIS Gap Detected: Crude Tanker TANKER_HORMUZ_01 went dark near Strait of Hormuz (26.55N, 56.25E)",
            "domain_data": {"latitude": 26.55, "longitude": 56.25, "name": "TANKER_HORMUZ_01", "mmsi": "235091234", "speed": 14.2, "flag": "PA"},
            "raw_payload": {}
        },
        {
            "event_id": "evt_maritime_102",
            "type": "vessel_position",
            "source": "collector.ais",
            "occurred_at": now,
            "primary_entity_id": "MMSI_636018992",
            "primary_entity_name": "VLCC_PERSIAN_GULF",
            "region": "Persian Gulf",
            "anomaly_score": 0.72,
            "summary": "VLCC Persian Gulf reported course deviation near Ras Tanura terminal (25.90N, 54.80E)",
            "domain_data": {"latitude": 25.90, "longitude": 54.80, "name": "VLCC_PERSIAN_GULF", "mmsi": "636018992", "speed": 11.8, "flag": "LBR"},
            "raw_payload": {}
        },
        {
            "event_id": "evt_cyber_201",
            "type": "bgp_anomaly",
            "source": "collector.cyber",
            "occurred_at": now,
            "primary_entity_id": "AS_45129",
            "primary_entity_name": "RIPE_BGP_NODE",
            "region": "Middle East",
            "anomaly_score": 0.76,
            "summary": "Suspicious BGP Route Announcement Hijack targeting telecom AS-45129",
            "domain_data": {"asn": "AS_45129", "from_coords": [-75.0, 38.0], "to_coords": [56.0, 26.0]},
            "raw_payload": {}
        },
        {
            "event_id": "evt_tradfi_301",
            "type": "option_sweep",
            "source": "collector.tradfi",
            "occurred_at": now,
            "primary_entity_id": "NVDA",
            "primary_entity_name": "NVIDIA Corp",
            "region": "US",
            "anomaly_score": 0.82,
            "summary": "NVDA Institutional Put Option Sweep $14.2M (25D IV Skew +420bps)",
            "domain_data": {"ticker": "NVDA", "notional": 14200000, "iv_skew_bps": 420},
            "raw_payload": {}
        },
        {
            "event_id": "evt_tradfi_302",
            "type": "volume_anomaly",
            "source": "collector.radar",
            "occurred_at": now,
            "primary_entity_id": "SMCI",
            "primary_entity_name": "Super Micro Computer",
            "region": "US",
            "anomaly_score": 0.89,
            "summary": "SMCI Volume Spike +3.84σ EWMA Z-Score detected by Alpaca US Scanner",
            "domain_data": {"ticker": "SMCI", "z_score": 3.84, "volume": 920000},
            "raw_payload": {}
        },
        {
            "event_id": "evt_crypto_401",
            "type": "liquidation_cascade",
            "source": "collector.crypto",
            "occurred_at": now,
            "primary_entity_id": "BTC",
            "primary_entity_name": "Bitcoin",
            "region": "Global",
            "anomaly_score": 0.68,
            "summary": "BTC Long Liquidation Cascade $48.5M across Binance & Bybit perps",
            "domain_data": {"symbol": "BTCUSDT", "liquidations_usd": 48500000},
            "raw_payload": {}
        },
        {
            "event_id": "evt_prediction_501",
            "type": "odds_spike",
            "source": "collector.prediction",
            "occurred_at": now,
            "primary_entity_id": "MARKET_HORMUZ_BLOCKADE",
            "primary_entity_name": "Polymarket Hormuz Blockade",
            "region": "Global",
            "anomaly_score": 0.91,
            "summary": "Polymarket 'Hormuz Shipping Blockade in July 2026' probability surged 18% to 64%",
            "domain_data": {"market": "Hormuz Shipping Blockade", "prob": 0.64, "prob_change_24h": 0.18},
            "raw_payload": {}
        }
    ]

    import json
    for e in events_data:
        await db.execute(
            """
            INSERT INTO events (event_id, type, source, occurred_at, primary_entity_id, primary_entity_name, region, anomaly_score, summary, domain_data, raw_payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (event_id) DO UPDATE SET
              occurred_at = EXCLUDED.occurred_at,
              anomaly_score = EXCLUDED.anomaly_score,
              summary = EXCLUDED.summary,
              domain_data = EXCLUDED.domain_data;
            """,
            e["event_id"], e["type"], e["source"], datetime.fromisoformat(e["occurred_at"]),
            e["primary_entity_id"], e["primary_entity_name"], e["region"], e["anomaly_score"],
            e["summary"], json.dumps(e["domain_data"]), json.dumps(e["raw_payload"])
        )
        # Broadcast to Redis PubSub for real-time WebSocket live feed
        await redis.raw.publish("sentinel:events:live", json.dumps({
            "event_id": e["event_id"],
            "type": e["type"],
            "occurred_at": e["occurred_at"],
            "source": e["source"],
            "anomaly_score": e["anomaly_score"],
            "region": e["region"],
            "headline": e["summary"],
            "primary_entity_name": e["primary_entity_name"],
            "domain_data": e["domain_data"]
        }))
    print("✅ Inserted & Broadcasted 7 Multi-Domain Events!")

    # 2. Neo4j Graph Entities & Co-occurrence Relationships
    cypher_entities = """
    MERGE (ofac:Entity {id: 'SANCTIONS_OFAC_TARGET_99'})
    SET ofac.name = 'OFAC Target 99', ofac.type = 'sanctioned_entity', ofac.risk = 0.98

    MERGE (nvda:Entity {id: 'NVDA'})
    SET nvda.name = 'NVIDIA Corp', nvda.type = 'equity', nvda.ticker = 'NVDA'

    MERGE (smci:Entity {id: 'SMCI'})
    SET smci.name = 'Super Micro Computer', smci.type = 'equity', smci.ticker = 'SMCI'

    MERGE (tsmc:Entity {id: 'TSMC'})
    SET tsmc.name = 'Taiwan Semiconductor', tsmc.type = 'supplier', tsmc.ticker = 'TSM'

    MERGE (vessel:Entity {id: 'MMSI_235091234'})
    SET vessel.name = 'TANKER_HORMUZ_01', vessel.type = 'vessel', vessel.mmsi = '235091234'

    MERGE (nvda)-[:PURCHASES_FROM]->(tsmc)
    MERGE (smci)-[:SUPPLIES]->(nvda)
    MERGE (ofac)-[:TRANSACTS_WITH]->(smci)
    MERGE (vessel)-[:OPERATES_IN]->(ofac)
    """
    await neo4j.query(cypher_entities)
    print("✅ Neo4j Graph Knowledge Nodes & Relationships Synced!")

    # 3. Dynamic Correlation Rules in Redis
    dynamic_rule = {
        "rule_id": "rule_geopolitical_maritime_market_cascade",
        "trigger_event_type": "ais_gap_dark_vessel",
        "conditions": {"min_anomaly": 0.5},
        "correlations": [{"event_types": ["option_sweep", "volume_anomaly", "bgp_anomaly"], "hours": 24, "min_anomaly": 0.5}],
        "alert_tier": "CRITICAL",
        "expires_at": int(time.time()) + 315360000
    }
    await redis.raw.hset("sentinel:correlation:dynamic_rules", dynamic_rule["rule_id"], json.dumps(dynamic_rule))
    print("✅ Redis Dynamic Correlation Rules Active!")

    print("🚀 Pipeline Seeding Completed Successfully!")

if __name__ == "__main__":
    asyncio.run(seed())
