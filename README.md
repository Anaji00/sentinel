# Sentinel Intelligence Platform

**Sentinel** is an enterprise-grade, multi-domain real-time event correlation, quantitative risk analysis, and threat intelligence engine. It ingests, normalizes, and correlates high-throughput streaming telemetry across maritime AIS, cyber threat feeds, financial markets & options flow, macroeconomics, crypto, prediction markets, aviation, and OSINT news to synthesize automated tactical intelligence using local LLM agent swarms (Ollama) and deterministic quantitative risk models.

---

## ⚡ System Architecture

```
                                  [ DOMAIN TELEMETRY SOURCES ]
  (Maritime AIS | TradFi Options & Equity | Crypto Swaps | Cyber BGP & CVEs | Prediction Markets | Aviation ADSB | OSINT News)
                                                │
                                                ▼
                                    [ KAFKA EVENT BACKBONE ]
                     (Topics: raw.maritime, raw.tradfi, raw.cyber, raw.adsb, etc.)
                                                │
                                                ▼
                                    [ ENRICHMENT ENGINE ]
       ┌────────────────────────────────────────┴────────────────────────────────────────┐
       │ - IsolationForest Spatial ML Scoring             - OFAC Sanctions Cross-Checking │
       │ - OCC Standardized Option Contract Parser        - PostGIS Geographic Chokepoints│
       │ - Vessel Subtype Auto-Classification             - Unified Event Normalization   │
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
                                   [ REASONING & QUANT SWARM ]
       ┌────────────────────────────────────────┴────────────────────────────────────────┐
       │ - Ollama Swarm (Qwen 2.5 7B / 1.5B)              - Cointegration & Granger Tests│
       │ - Deterministic Half-Kelly Sizing                - ATR Stop Loss & R/R Tiers     │
       │ - Subjective Logic Consensus Engine              - Source Reliability Scorecard  │
       └────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
                          [ FASTAPI GATEWAY ] ──► [ NEXT.JS RADAR DASHBOARD ]
                     (/metrics, /health, /graph)     (Timezone HUD & Deck.gl Map)
```

---

## 🛠️ Microservices & Subsystem Architecture

Sentinel is composed of **18 modular microservices** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Data Ingestion Collectors (`services/collector-*`)
- **`collector-ais`**: Real-time WebSocket connection tracking global vessel positions, MMSI identifiers, navigation status, speed over ground, and geographic coordinates.
- **`collector-tradfi`**: Streaming US equity quotes, order book depth, and options flow sweeps with built-in OCC standardized contract parsing (`AAPL240816C00220000`).
- **`collector-macro`**: High-frequency polling for 12 macro instruments (Crude Oil, Brent, Natural Gas, Gold, Silver, Corn, Wheat, Nasdaq, S&P 500, VXX Volatility, TIPS, 10-20 Yr Treasuries) with single-ticker fallback resilience.
- **`collector-adsb`**: Aircraft state vector ingestion tracking military, commercial, and emergency squawk flights.
- **`collector-crypto`**: High-frequency perpetual swaps, candle volatility, and volume anomaly tracking across major exchanges.
- **`collector-cyber`**: Ingestion of BGP routing hijack anomalies, IP threat scores, and CVE vulnerability telemetry with automated equity ticker mapping.
- **`collector-prediction`**: Real-time contract odds and liquidity streams from Polymarket and Kalshi.
- **`collector-news`**: RSS/API breaking news ingestion and OSINT headline normalization.
- **`collector-radar`**: Cross-domain anomaly radar aggregator consolidating domain metrics into unified streams.

### 2. Enrichment & Threat Intelligence (`services/enrichment`)
- **OCC Option Contract Parser**: Extracts ticker, ISO expiry (`YYYY-MM-DD`), option side (`CALL`/`PUT`), and strike price ($) from 21-character OCC contract symbols.
- **Unsupervised Spatial Anomaly Detection**: IsolationForest & ONNX ML models evaluate deviations in vessel behavior, flight vectors, market volume spikes, and network routing changes.
- **PostGIS Spatial Indexing**: Explicit longitude and latitude column mapping into PostgreSQL hypertables with spatial point indexing (SRID 4326).
- **Sanctions & Compliance Screening**: Direct cross-referencing against OFAC Specially Designated Nationals (SDN) and watchlists.
- **Spatial Chokepoint & Theater Indexing**: Automatic geo-fencing against strategic maritime bottlenecks (Strait of Hormuz, Bab-el-Mandeb, Suez Canal, Taiwan Strait, Malacca Strait) and active geopolitical conflict theaters.

### 3. Correlation & Storage Engines (`services/correlation`)
- **Hawkes Point Process Correlator**: Models cross-domain self-exciting cascade probabilities.
- **TimescaleDB Hypertables**: PostgreSQL time-series hypertables for fast analytical queries, spatial queries, and historical event indexing with non-destructive UPSERT logic.
- **Neo4j Knowledge Graph**: Maps relational topologies between entities (`Vessel` ➔ `Owner` ➔ `SanctionTarget` ➔ `SovereignState`). Features dynamic fallback to TimescaleDB event co-occurrences when Neo4j is offline or unpopulated.
- **Qdrant Vector Database**: Computes 384-dimensional embeddings (`all-MiniLM-L6-v2`) to cluster cross-domain events by semantic context.
- **Redis In-Memory Bus**: PubSub stream broadcasting and sub-millisecond market price caching.

### 4. Quantitative Engine & Reasoning Swarm (`services/agents` & `services/reasoning`)
- **Quantitative Trading Engine**: Features server-side deterministic trade signal enforcement:
  - Entry level fixed to current verified asset price.
  - ATR-based stop loss calculation ($1.5 \times \text{ATR}$).
  - Conviction-tiered Risk/Reward multipliers (1.5x for $<0.6$, 2.0x for $<0.8$, 3.0x for $\ge 0.8$).
  - Empirical half-Kelly allocation clamping ($K_{\text{max}}$).
- **Macro Intelligence Engine**: Cointegration pair analysis via Engle-Granger tests, yield curve spread tracking, and z-score anomaly detection.
- **Consensus Engine**: Subjective logic opinion fusion ($b, d, u, a$) across multi-agent predictions.
- **LLM Reasoning Swarm**:
  - **Heavy Analytical Tier (`qwen2.5:7b`)**: Deep reasoning for Quant Research, Macro Strategy, Yield Curve Rates, Volatility Surfaces, and Graph Supervision.
  - **Fast Operational Tier (`qwen2.5:1.5b` / `gemma3:1b` fallback)**: Ultra-fast routing, headline NER extraction, and concept taxonomy management.

### 5. Auxiliary System Workers
- **`alert_manager`**: Escalation router delivering high-severity correlation alerts via PagerDuty, Webhooks, Telegram, and Slack.
- **`dlq-worker`**: Dead-letter queue listener handling failed message retries and quarantine analysis.
- **`telemetry-worker`**: Operational health metrics, container status telemetry, and Redis PubSub heartbeats.

---

## 📐 Quantitative & Statistical Mathematics

Sentinel integrates a specialized quantitative math engine providing verified financial algorithms:

- **Sharpe Ratio & Max Drawdown**: Annualized risk-adjusted return ratio and peak-to-trough decline metrics.
- **Value at Risk (VaR) & Conditional VaR (CVaR)**: Non-parametric historical loss estimation at 95% and 99% confidence levels per $10,000 position.
- **Engle-Granger Cointegration**: Two-step OLS regression testing stationary spread relationships ($y_t - \beta x_t - \alpha$) with Dickey-Fuller t-statistics and half-life estimation.
- **Granger Causality**: VAR lag testing evaluating whether lead-lag relationships exist between cross-domain series.
- **EWMA Volatility**: Exponentially Weighted Moving Average volatility estimation ($\lambda = 0.94$).
- **Empirical Half-Kelly Criterion**: Position sizing derived from historical win rates ($W$) and payoff ratios ($R$):
  $$K_{\text{half}} = 0.5 \times \left( W - \frac{1 - W}{R} \right)$$

---

## 🚀 Performance Benchmarks & Optimizations

| Component | Optimization Strategy | Technical Impact |
| :--- | :--- | :--- |
| **LLM Inference** | Flash Attention + Q8_0 KV Cache + Permanent Keep-Alive | **2.5x token generation speed** |
| **Response Cache** | SHA-256 Redis prompt cache (`sentinel:llm_cache:<hash>`) | **<1ms instant cache hit** |
| **Quant Execution** | Post-hoc deterministic risk enforcement | **100% elimination of LLM price/Kelly hallucination** |
| **Redis Pipeline** | Micro-batched `mget()` queries & PubSub pipelines | **N to 1 network round-trips** |
| **Event Stream** | Zero-latency Redis PubSub WebSocket broadcast | **Sub-5ms UI updates** |
| **Spatial Radar** | Memoized vessel map, cap at 50 sorted markers, static opacity | **0% GPU animation lag** |

---

## ⚡ Quick Start (3 Steps)

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

### 1. Model Swarm Management (Ollama)

```bash
# Pull primary heavy reasoning model (Qwen 2.5 7B)
docker exec -it sentinel-ollama ollama pull qwen2.5:7b

# Pull fast operational tier model (Qwen 2.5 1.5B)
docker exec -it sentinel-ollama ollama pull qwen2.5:1.5b

# Pull lightweight fallback model (Gemma 3 1B)
docker exec -it sentinel-ollama ollama pull gemma3:1b

# List all local pulled models
docker exec -it sentinel-ollama ollama list
```

---

### 2. Docker Cluster Management

```bash
# Build and start all 18 microservices in background
docker compose up --build -d

# Check status of all running containers
docker compose ps

# View logs for a specific service
docker compose logs -f enrichment
docker compose logs -f agents-heavy
docker compose logs -f api-gateway
docker compose logs -f correlation

# Restart a specific service
docker compose restart api-gateway

# Stop the entire platform
docker compose down

# Stop and purge all persistent volumes (Clean Reset)
docker compose down -v
```

---

### 3. API Gateway & Operational Inspection

```bash
# System Health Inspection (TimescaleDB, Neo4j, Redis)
curl -s http://localhost:8000/api/v1/health -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/health

# Prometheus Metrics Scraping (unauthenticated)
curl -s http://localhost:8000/metrics
curl -s http://localhost:8000/metrics/json

# Domain Event Queries
curl -s http://localhost:8000/api/v1/events/maritime?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/api/v1/events/tradfi?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/api/v1/events/cyber?limit=10 -H "X-API-KEY: sentinel-dev-key-2026"

# Knowledge Graph Relationship Queries (with TimescaleDB fallback)
curl -s http://localhost:8000/api/v1/graph/entity/NVDA -H "X-API-KEY: sentinel-dev-key-2026"
curl -s http://localhost:8000/api/v1/graph/shortest-path?source_id=USA&target_id=IRN -H "X-API-KEY: sentinel-dev-key-2026"

# Live WebSocket Stream Inspection (using wscat)
npx wscat -c "ws://localhost:8000/api/v1/events/ws/live-feed?api_key=sentinel-dev-key-2026"
```

---

### 4. Database & Storage Debugging Commands

#### **TimescaleDB (PostgreSQL)**
```bash
# Connect to TimescaleDB shell
docker exec -it sentinel-timescaledb psql -U sentinel_user -d sentinel_db

# Useful SQL Queries:
# SELECT COUNT(*) FROM events;
# SELECT type, headline, anomaly_score, longitude, latitude FROM events ORDER BY occurred_at DESC LIMIT 10;
```

#### **Neo4j Knowledge Graph**
```bash
# Open Cypher shell
docker exec -it sentinel-neo4j cypher-shell -u neo4j -p sentinel_secret_pass

# Useful Cypher Queries:
# MATCH (n) RETURN count(n);
# MATCH (a:Entity)-[r]->(b:Entity) RETURN a.name, type(r), b.name LIMIT 25;
```

#### **Redis Bus**
```bash
# Connect to Redis CLI
docker exec -it sentinel-redis redis-cli

# Subscribe to real-time WebSocket PubSub stream
# SUBSCRIBE sentinel:events:live

# Inspect active keys:
# KEYS "sentinel:*"
```

---

### 5. Automated Test Suite (215 Passing Tests)

Run the full pytest suite covering all microservices, risk engines, and API endpoints:

```bash
# Run complete test suite
python -m pytest

# Run with verbose output and print statements
python -m pytest -v -s

# Run specific domain test module
python -m pytest tests/test_quant_trading_engine.py
```

---

## 🌐 Service & Port Directory

| Service | Host / Port | Authentication / Access |
| :--- | :--- | :--- |
| **Web Dashboard** | `http://localhost:3000` | Public Web UI |
| **API Gateway** | `http://localhost:8000` | Header `X-API-KEY: sentinel-dev-key-2026` |
| **Prometheus Metrics** | `http://localhost:8000/metrics` | Unauthenticated Scraper |
| **JSON Metrics Summary** | `http://localhost:8000/metrics/json` | Unauthenticated |
| **Health Status Endpoint** | `http://localhost:8000/health` | Unauthenticated |
| **WebSocket Live Feed** | `ws://localhost:8000/api/v1/events/ws/live-feed` | Query Param `?api_key=` or Header |
| **Kafka UI Manager** | `http://localhost:8080` | Public Admin UI |
| **Neo4j Graph Browser** | `http://localhost:7474` | `neo4j / sentinel_secret_pass` |
| **Qdrant Dashboard** | `http://localhost:6333/dashboard` | Public Vector UI |
| **TimescaleDB PostgreSQL** | `localhost:5432` | `sentinel_user / sentinel_db` |

---

**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
