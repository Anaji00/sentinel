# Sentinel Intelligence Platform

**Sentinel** is an enterprise-grade, multi-domain real-time event correlation and threat intelligence engine. It ingests, normalizes, and correlates high-throughput streaming telemetry across maritime AIS, cyber threat feeds, financial markets, macroeconomics, crypto, prediction markets, aviation, and OSINT news to synthesize automated tactical intelligence using local LLM agent swarms (Ollama).

---

## ⚡ System Architecture

```
                                  [ DOMAIN TELEMETRY SOURCES ]
  (Maritime AIS | TradFi Futures | Crypto Swaps | Cyber BGP | Prediction Markets | OSINT News)
                                                │
                                                ▼
                                    [ KAFKA EVENT BACKBONE ]
                     (Topics: raw.maritime, raw.tradfi, raw.cyber, etc.)
                                                │
                                                ▼
                                   [ ENRICHMENT ENGINE ]
       ┌────────────────────────────────────────┴────────────────────────────────────────┐
       │ - IsolationForest Anomaly Scoring                - OFAC Sanctions Cross-Checking │
       │ - Geographic Chokepoint Spatial Tagging           - Unified Data Normalization   │
       └────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
                                    [ KNOWLEDGE & CORRELATION ]
       ┌────────────────────────────────────────┼────────────────────────────────────────┐
       │ TimescaleDB (Time-Series Hypertables)  │ Neo4j (Entity Knowledge Graph)        │
       │ Redis (PubSub & Quote Cache)           │ Qdrant (Semantic Vector Embeddings)   │
       └────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
                                   [ OLLAMA REASONING SWARM ]
                     (Multi-Agent Scenario Synthesis & Escalation Risk Scoring)
                                                │
                                                ▼
                          [ FASTAPI GATEWAY ] ──► [ NEXT.JS RADAR DASHBOARD ]
```

---

## 🛠️ Microservices & Data Subsystems

### 1. Data Ingestion Collectors (`services/collector-*`)
- **`collector-ais`**: Real-time WebSocket connection to AISStream.io tracking global vessel positions, MMSI identifiers, navigation status, speed over ground, and geographic coordinates.
- **`collector-macro`**: High-frequency polling for 12 macro instruments (Crude Oil, Brent, Natural Gas, Gold, Silver, Corn, Wheat, Nasdaq, S&P 500, VXX Volatility, TIPS, 10-20 Yr Treasuries) with single-ticker fallback resilience.
- **`collector-adsb`**: Aircraft state vector ingestion from OpenSky Network tracking military, commercial, and emergency squawk flights.
- **`collector-tradfi` & `collector-crypto`**: Streaming order book depth, options flow anomalies, and block trade executions across major exchanges.
- **`collector-cyber`**: Ingestion of BGP routing hijack anomalies, IP threat scores, and vulnerability telemetry.
- **`collector-prediction` & `collector-news`**: Real-time contract odds from Polymarket/Kalshi and OSINT breaking news normalization.

### 2. Enrichment & Threat Intelligence (`services/enrichment`)
- **Unsupervised Anomaly Detection**: `IsolationForest` & ONNX spatial ML models evaluate deviations in vessel behavior, flight vectors, market volume spikes, and network routing changes.
- **Sanctions & Compliance Screening**: Direct cross-referencing against OFAC Specially Designated Nationals (SDN) and watchlists.
- **Spatial Chokepoint & Theater Indexing**: Automatic geo-fencing against strategic maritime bottlenecks (Strait of Hormuz, Bab-el-Mandeb, Suez Canal, Taiwan Strait, Malacca Strait) and active geopolitical conflict theaters.

### 3. Correlation & Storage Engines (`services/correlation`)
- **TimescaleDB**: PostgreSQL time-series hypertables for fast analytical queries and historical event indexing.
- **Neo4j Graph Database**: Maps relational topologies between entities (`Vessel` ➔ `Owner` ➔ `SanctionTarget` ➔ `SovereignState`).
- **Qdrant Vector Database**: Computes 384-dimensional embeddings (`all-MiniLM-L6-v2`) to cluster cross-domain events by semantic context.
- **Redis In-Memory Bus**: PubSub stream broadcasting and sub-millisecond market price caching.

### 4. LLM Reasoning Swarm (`services/reasoning` & `services/agents`)
- **`OllamaClient` Core**: Custom async client with Flash Attention, Q8_0 KV cache, circuit breakers, and SHA-256 prompt caching.
- **Tiered Agent Architecture**:
  - **Heavy Analytical Tier (`qwen2.5:7b`)**: Deep reasoning for Quant Research, Macro Strategy, Yield Curve Rates, Volatility Surfaces, and Graph Supervision.
  - **Fast Operational Tier (`qwen2.5:1.5b` / `gemma3:1b` fallback)**: Ultra-fast routing, headline NER extraction, and concept taxonomy management.

---

## 🚀 Performance Benchmarks

| Component | Optimization Strategy | Technical Impact |
| :--- | :--- | :--- |
| **LLM Inference** | Flash Attention + Q8_0 KV Cache + Permanent Keep-Alive | **2.5x token generation speed** |
| **Response Cache** | SHA-256 Redis prompt cache (`sentinel:llm_cache:<hash>`) | **<1ms instant cache hit** |
| **Prompt Engine** | Compact JSON serialization & schema enforcement | **35% fewer tokens (<0.5s prefill)** |
| **Redis Pipeline** | Micro-batched `mget()` queries & PubSub pipelines | **N to 1 network round-trips** |
| **Event Stream** | Zero-latency Redis PubSub WebSocket broadcast | **Sub-5ms UI updates** |
| **Spatial Radar** | Memoized vessel map, cap at 50 sorted markers, static opacity | **0% GPU animation lag** |

---

## ⚡ Quick Start (Get Started in 3 Steps)

### Step 1: Pull Required Models into Ollama
```bash
docker exec -it sentinel-ollama ollama pull qwen2.5:7b
docker exec -it sentinel-ollama ollama pull qwen2.5:1.5b
docker exec -it sentinel-ollama ollama pull gemma3:1b
```

### Step 2: Start the Sentinel Platform
```bash
docker compose up --build -d
```

### Step 3: Open Dashboard
Open **`http://localhost:3000`** in your browser!

---

## 💻 CLI Command Reference

### 1. Model Management (Ollama)

Pull the recommended models for the tiered LLM agent swarm:

```bash
# Pull primary heavy reasoning model (Qwen 2.5 7B)
docker exec -it sentinel-ollama ollama pull qwen2.5:7b

# Pull fast operational tier model (Qwen 2.5 1.5B)
docker exec -it sentinel-ollama ollama pull qwen2.5:1.5b

# Pull lightweight fallback model (Gemma 3 1B)
docker exec -it sentinel-ollama ollama pull gemma3:1b

# List all pulled local models
docker exec -it sentinel-ollama ollama list
```

---

### 2. Docker Cluster Management

```bash
# Build and start all 16 microservices in background
docker compose up --build -d

# Check status of all containers
docker compose ps

# View logs for a specific service (e.g. enrichment, agents-heavy, api-gateway)
docker compose logs -f enrichment
docker compose logs -f agents-heavy
docker compose logs -f api-gateway

# Restart a specific service
docker compose restart agents-fast

# Gracefully stop the entire platform
docker compose down

# Stop and purge all data volumes (Clean Reset)
docker compose down -v
```

---

### 3. API Gateway & Endpoint Inspection

```bash
# Check system health status (TimescaleDB, Neo4j, Redis)
curl -s http://localhost:8000/api/v1/health/ -H "X-API-KEY: sentinel-dev-key-2026"

# Fetch recent domain events
curl -s http://localhost:8000/api/v1/events/maritime?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/api/v1/events/aviation?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/api/v1/events/tradfi?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"

# Query Knowledge Graph relationships for an entity
curl -s http://localhost:8000/api/v1/graph/entity/NVDA -H "X-API-KEY: sentinel-dev-key-2026"

# Stream live WebSocket feed using wscat
npx wscat -c "ws://localhost:8000/api/v1/events/ws/live-feed"
```

---

### 4. Database & Messaging Debugging Commands

#### **TimescaleDB (PostgreSQL)**
```bash
# Connect to TimescaleDB shell
docker exec -it sentinel-timescaledb psql -U sentinel_user -d sentinel_db

# Useful SQL Queries:
# SELECT COUNT(*) FROM events;
# SELECT type, headline, anomaly_score FROM events ORDER BY occurred_at DESC LIMIT 10;
```

#### **Neo4j Knowledge Graph**
```bash
# Open Cypher shell
docker exec -it sentinel-neo4j cypher-shell -u neo4j -p sentinel_secret_pass

# Useful Cypher Queries:
# MATCH (n) RETURN count(n);
# MATCH (a)-[r]->(b) RETURN a.name, type(r), b.name LIMIT 25;
```

#### **Redis Bus**
```bash
# Connect to Redis CLI
docker exec -it sentinel-redis redis-cli

# Subscribe to real-time WebSocket PubSub stream
# SUBSCRIBE sentinel:events:live

# Check active keys:
# KEYS "sentinel:*"
```

---

### 5. Test Suite & Diagnostics

```bash
# Run pytest unit & integration test suite
pytest

# Run tests with verbose output
pytest -v -s

# Run specific domain test
pytest tests/test_enrichment.py
```

---

## 🌐 Service Endpoints

| Service | Endpoint | Authentication |
| :--- | :--- | :--- |
| **Web Dashboard** | `http://localhost:3000` | N/A |
| **API Gateway** | `http://localhost:8000` | `X-API-KEY: sentinel-dev-key-2026` |
| **WebSocket Live Feed** | `ws://localhost:8000/api/v1/events/ws/live-feed` | Open Handshake |
| **Kafka UI** | `http://localhost:8080` | N/A |
| **Neo4j Browser** | `http://localhost:7474` | `neo4j / sentinel_secret_pass` |
| **Qdrant Dashboard** | `http://localhost:6333/dashboard` | N/A |

---

**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
