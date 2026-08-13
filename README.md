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
       │ - Ollama Swarm (Qwen 2.5 7B / 1.5B / Gemma)     - Cointegration & Granger Tests │
       │ - Deterministic Half-Kelly Sizing                - ATR Stop Loss & R/R Tiers     │
       │ - Subjective Logic Consensus Engine              - Source Reliability Scorecard  │
       │ - StockCorrelation & Sympathy Engine             - Financial Advisor & Exec      │
       └────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
                         [ FASTAPI GATEWAY ] ──► [ NEXT.JS RADAR DASHBOARD ]
                    (/metrics, /health, /graph)     (Timezone HUD & Deck.gl 3D Map)
                                                │
                                                ▼
                                   [ OBSERVABILITY & METRICS ]
                         (Prometheus Time-Series & Grafana Dashboards)
```

---

## 🛠️ Microservices & Subsystem Architecture

Sentinel is composed of **29 modular services & containers** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Data Ingestion Collectors (`services/collector-*`)
- **`collector-ais`**: Real-time WebSocket connection tracking global vessel positions, MMSI identifiers, navigation status, speed over ground, and geographic coordinates.
- **`collector-tradfi`**: Streaming US equity quotes, order book depth, and options flow sweeps with built-in OCC standardized contract parsing (`AAPL240816C00220000`).
- **`collector-macro`**: High-frequency polling for 12 macro instruments (Crude Oil, Brent, Natural Gas, Gold, Silver, Corn, Wheat, Nasdaq, S&P 500, VXX Volatility, TIPS, 10-20-30 Yr Treasuries) with single-ticker fallback resilience.
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

### 3. Correlation & Storage Engines (`services/correlation` & `shared/db`)
- **Hawkes Point Process Correlator**: Models cross-domain self-exciting cascade probabilities.
- **TimescaleDB Hypertables**: PostgreSQL time-series hypertables for fast analytical queries, spatial queries, and historical event indexing with non-destructive UPSERT logic.
- **Neo4j Knowledge Graph**: Maps relational topologies between entities (`Vessel` ➔ `Owner` ➔ `SanctionTarget` ➔ `SovereignState`). Features dynamic fallback to TimescaleDB event co-occurrences when Neo4j is offline or unpopulated.
- **Qdrant Vector Database**: Computes 384-dimensional embeddings (`all-MiniLM-L6-v2`) to cluster cross-domain events by semantic context.
- **Redis In-Memory Bus**: PubSub stream broadcasting, rate-limiting token buckets, and sub-millisecond market price caching.

### 4. Quantitative Engine & Reasoning Swarm (`services/agents` & `services/reasoning`)
- **Quantitative Trading Engine**: Features server-side deterministic trade signal enforcement:
  - Entry level fixed to current verified asset price.
  - ATR-based stop loss calculation ($1.5 \times \text{ATR}$).
  - Conviction-tiered Risk/Reward multipliers (1.5x for $<0.6$, 2.0x for $<0.8$, 3.0x for $\ge 0.8$).
  - Empirical half-Kelly allocation clamping ($K_{\text{max}}$).
- **Financial Advisor & Order Execution Bridge**: AI Financial Advisor brief generation with VaR 95%, CVaR 99%, Sharpe ratio metrics, technical indicators (RSI, EMA 12/26, ATR, Fibonacci retracement levels), and Alpaca paper order execution.
- **StockCorrelationAgent**: Identifies dynamic equities correlations, sympathy movers, and inter-market transmission channels stored in Redis.
- **Macro Intelligence Engine**: Cointegration pair analysis via Engle-Granger tests, yield curve spread tracking, and z-score anomaly detection.
- **Consensus Engine**: Subjective logic opinion fusion ($b, d, u, a$) across multi-agent predictions.
- **LLM Reasoning Swarm (`agents-heavy` & `agents-fast`)**:
  - **Heavy Analytical Tier (`agents-heavy` - `qwen2.5:7b`)**: Deep reasoning for Quant Research, Macro Strategy, Yield Curve Rates, Volatility Surfaces, and Graph Supervision.
  - **Fast Operational Tier (`agents-fast` - `qwen2.5:1.5b` / `gemma3:1b` fallback)**: Ultra-fast routing, headline NER extraction, and concept taxonomy management.

### 5. Auxiliary Workers & Tactical Radar Dashboard (`frontend` & `services/gateway`)
- **`api-gateway`**: High-performance FastAPI gateway handling REST queries, rate limiting, authentication, Prometheus metrics, and real-time WebSockets.
- **`frontend` (Sentinel Radar Dashboard)**: Tactical radar interface built with **Next.js 14**, **React**, **Deck.gl**, **Recharts**, and **TailwindCSS**. Renders real-time streaming telemetry, 3D geographic threat visualizations, quantitative financial risk models, AI agent swarm telemetry, and interactive graph exploration.
  - **`GlobalMap.tsx`**: Deck.gl 3D Globe & Map supporting real-time AIS vessel positions, ADSB flight vectors, strategic trade chokepoints, and active geopolitical conflict theaters.
  - **`FinancialAdvisorAdvice.tsx`**: Interactive AI Financial Advisor dashboard with quarter-Kelly position sizing, VaR 95% / CVaR 99% portfolio metrics, RSI / EMA / ATR technical indicators, and single-click paper order execution via Alpaca execution bridge.
  - **`QuantRadarPanel.tsx`**: Real-time volume anomaly radar sweeps and watchlist management.
  - **`CryptoAnalytics.tsx`**: Perpetual swaps volatility and candle analytics.
  - **`DarkPoolFlowPanel.tsx`**: Options sweep flow and institutional dark pool tracking.
  - **`CyberIntelligencePanel.tsx`**: BGP hijacking alerts, IP threat scoring, and CVE mapping.
  - **`PredictionMarketPanel.tsx`**: Polymarket and Kalshi real-time odds probability streams.
  - **`OsintThreatMatrix.tsx`**: OSINT news intelligence and cross-domain entity correlation matrix.
  - **`GraphExplorer.tsx`**: Neo4j & TimescaleDB 2D/3D entity relationship graph viewer.
  - **`AgentSwarmTelemetry.tsx`**: Real-time agent decision log inspector and LLM model status HUD.
  - **`CommandCenterGrid.tsx`**: Multi-panel tactical command center grid.
  - **`SystemHealthHUD.tsx`**: Real-time WebSocket connection status, database latency, and time-zone HUD.
- **`alert_manager`**: Escalation router delivering high-severity correlation alerts via PagerDuty, Webhooks, Telegram, and Slack.
- **`dlq-worker`**: Dead-letter queue listener handling failed message retries and quarantine analysis.
- **`telemetry-worker`**: Operational health metrics, container status telemetry, and Redis PubSub heartbeats.

### 6. Observability & Infrastructure
- **`prometheus`**: Time-series metrics collection server scraping `/metrics` endpoints.
- **`grafana`**: Operational monitoring dashboards for infrastructure visualization.
- **`kafka-ui`**: Web UI for inspecting Kafka topics, partitions, and message offsets.

---

## 📐 Quantitative & Statistical Mathematics

Sentinel integrates a specialized quantitative math engine providing verified financial algorithms:

- **Sharpe Ratio & Max Drawdown**: Annualized risk-adjusted return ratio and peak-to-trough decline metrics.
- **Value at Risk (VaR) & Conditional VaR (CVaR)**: Non-parametric historical loss estimation at 95% and 99% confidence levels per position.
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
# Build and start all 29 services in background
docker compose up --build -d

# Check status of all running containers
docker compose ps

# View logs for specific services
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

### 3. Frontend Local Development & Configuration

```bash
# Navigate to frontend directory and install dependencies
cd frontend
npm install

# Start local Next.js development server (runs on http://localhost:3000)
npm run dev
```

#### **Frontend Environment Variables (`frontend/.env.local`)**
| Environment Variable | Description | Default Value |
| :--- | :--- | :--- |
| `NEXT_PUBLIC_API_URL` | Fast API Gateway REST & WS Endpoint | `http://localhost:8000` |
| `API_GATEWAY_KEY` | Development API Authentication Key | `sentinel-dev-key-2026` |

---

### 4. API Gateway & Operational Inspection

```bash
# System Health Inspection (TimescaleDB, Neo4j, Redis)
curl -s http://localhost:8000/api/v1/health -H "X-API-KEY: {APIKEY}"
curl -s http://localhost:8000/health

# Prometheus Metrics Scraping (unauthenticated)
curl -s http://localhost:8000/metrics
curl -s http://localhost:8000/metrics/json

# Domain Event Queries
curl -s http://localhost:8000/api/v1/events/maritime?limit=10 -H "X-API-KEY: {APIKEY}"
curl -s http://localhost:8000/api/v1/events/tradfi?limit=10 -H "X-API-KEY: {APIKEY}"
curl -s http://localhost:8000/api/v1/events/cyber?limit=10 -H "X-API-KEY: {APIKEY}"

# AI Financial Advisor Brief & Trade Recommendations
curl -s http://localhost:8000/api/v1/financial/advice -H "X-API-KEY: {APIKEY}"

# Execute Paper Trade Order via Execution Bridge
curl -X POST http://localhost:8000/api/v1/trading/orders/execute \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: {APIKEY}" \
  -d '{"ticker":"NVDA","action":"BUY","order_type":"Limit","entry_price":219.16,"target_price":242.00,"stop_loss":208.50,"position_size_usd":10000,"kelly_allocation_pct":6.8}'

# Quantitative Radar Anomalies & Intraday Market Series
curl -s http://localhost:8000/api/v1/radar/anomalies -H "X-API-KEY: {APIKEY}"
curl -s "http://localhost:8000/api/v1/radar/market-series?symbols=SPY,QQQ,BTCUSD" -H "X-API-KEY: {APIKEY}"

# Agentic Swarm Telemetry & Decision Logs
curl -s http://localhost:8000/api/v1/agents/processes -H "X-API-KEY: {APIKEY}"

# Knowledge Graph Relationship Queries (with TimescaleDB fallback)
curl -s http://localhost:8000/api/v1/graph/entity/NVDA -H "X-API-KEY: {APIKEY}"
curl -s http://localhost:8000/api/v1/graph/shortest-path?source_id=USA&target_id=IRN -H "X-API-KEY: {APIKEY}"

# Live WebSocket Stream Inspection (using wscat)
npx wscat -c "ws://localhost:8000/api/v1/events/ws/live-feed?api_key={APIKEY}"
```

---

### 5. Database & Storage Debugging Commands

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

### 6. Automated Test Suite (116 Passing Tests Across 7 Consolidated Modules)

Run the full pytest suite covering all microservices, risk engines, and API endpoints:

```bash
# Run complete consolidated test suite
python -m pytest

# Run specific domain test module
python -m pytest tests/test_quant_and_trading.py
python -m pytest tests/test_domain_enrichers.py
python -m pytest tests/test_anomaly_detection.py
python -m pytest tests/test_correlation_and_graph.py
python -m pytest tests/test_reasoning_swarm.py
python -m pytest tests/test_api_gateway.py
python -m pytest tests/test_system_and_security.py
```

---

## 🌐 Service & Port Directory

| Service | Host / Port | Authentication / Access |
| :--- | :--- | :--- |
| **Web Dashboard** | `http://localhost:3000` | Public Web UI |
| **Grafana Monitoring** | `http://localhost:3001` | Admin (`admin / admin`) |
| **API Gateway** | `http://localhost:8000` | Header `X-API-KEY: {APIKEY}` |
| **Prometheus Server** | `http://localhost:9090` | Public Monitoring UI |
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
