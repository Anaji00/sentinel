# Sentinel Intelligence Platform

**Sentinel** is an enterprise-grade, multi-domain real-time event correlation, quantitative risk analysis, and threat intelligence engine. It ingests, normalizes, and correlates high-throughput streaming telemetry across maritime AIS, cyber threat feeds, financial markets & options flow, macroeconomic indicators & economic calendar releases, crypto perpetuals, prediction markets, aviation ADSB, and OSINT news to synthesize automated tactical intelligence using local LLM agent swarms (Ollama) and deterministic quantitative risk models.

---

## ⚡ System Architecture

```
                                  [ DOMAIN TELEMETRY SOURCES ]
  (Maritime AIS | TradFi Equities & Options | Macro Calendar & SOFR | Crypto Swaps | Cyber BGP | Prediction Markets | Aviation ADSB | OSINT)
                                                │
                                                ▼
                                    [ KAFKA EVENT BACKBONE ]
                     (Topics: raw.maritime, raw.tradfi, raw.macro, raw.cyber, raw.adsb, etc.)
                                                │
                                                ▼
                                    [ ENRICHMENT ENGINE ]
        ┌───────────────────────────────────────┴───────────────────────────────────────┐
        │ - IsolationForest Spatial ML Scoring            - OFAC Sanctions Cross-Checking │
        │ - OCC Standardized Option Contract Parser       - PostGIS Geographic Boundaries │
        │ - Vessel Subtype Auto-Classification            - Macro Surprise & Bias Calc    │
        │ - Reference Data Graph Promotion                - Statistical Correlation ID Tag│
        └───────────────────────────────────────┬───────────────────────────────────────┘
                                                │
                                                ▼
                                   [ STORAGE & KNOWLEDGE LAYER ]
        ┌───────────────────────────────────────┼───────────────────────────────────────┐
        │ TimescaleDB (tradfi_bars & CAGGs)     │ Neo4j (Entity & Empirical Graph)     │
        │ Redis (PubSub, Caches, Thresholds)    │ Qdrant (Semantic Vector Embeddings)   │
        └───────────────────────────────────────┬───────────────────────────────────────┘
                                                │
                                                ▼
                                  [ CORRELATION & STAT ENGINE ]
        ┌───────────────────────────────────────┴───────────────────────────────────────┐
        │ - Topology-Guided Pairwise Discovery            - Bidirectional Granger Tests   │
        │ - Rolling Pearson / Spearman Correlations       - Cross-Sector Hawkes Contagion │
        │ - Engle-Granger Cointegration & Z-Scores        - 30-Day Exponential Edge Decay │
        └───────────────────────────────────────┬───────────────────────────────────────┘
                                                │
                                                ▼
                                   [ REASONING & QUANT SWARM ]
        ┌───────────────────────────────────────┴───────────────────────────────────────┐
        │ - Ollama Swarm (Qwen 2.5 7B / 1.5B / Gemma)     - 1-Hop Graph Context Checks    │
        │ - Black-Scholes Greeks & IV Root Finding        - Covered Call Overlays         │
        │ - Deterministic Kelly Criterion Allocation      - VaR 95% & CVaR 99% Risk Engine│
        │ - Subjective Logic Multi-Agent Consensus        - Alpaca Paper Execution Bridge │
        └───────────────────────────────────────┬───────────────────────────────────────┘
                                                │
                                                ▼
                          [ FASTAPI GATEWAY ] ──► [ NEXT.JS RADAR DASHBOARD ]
                       (HMAC/JWT Auth, /graph)      (3D Deck.gl, Graph & Radar HUDs)
                                                │
                                                ▼
                                   [ OBSERVABILITY & ML-OPS ]
                       (Prometheus, Grafana, Model Drift PSI/KS Scheduler)
```

---

## 🛠️ Microservices & Subsystem Architecture

Sentinel is composed of **29 modular services & containers** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Ingestion Collectors (`services/collector-*`)
- **`collector-tradfi`**: Streaming US equity quotes, order book depth, dark pool prints, and options flow with OCC standardized contract parsing (`AAPL240816C00220000`).
- **`collector-macro`**: Live Federal Reserve SOFR risk-free rate polling, 12 macro proxy instruments (Crude, Brent, NatGas, Gold, Silver, Yields, Indexes), and **Economic Calendar Collector** ingesting scheduled releases (CPI, Core CPI, NFP, Unemployment, FOMC Rate Decisions, GDP, ISM Manufacturing PMI, PCE) with automated surprise and directional bias calculations (`EventType.MACRO_RELEASE`).
- **`collector-ais`**: Real-time WebSocket tracking global maritime vessel positions, MMSI identifiers, navigation status, SOG, and coordinates.
- **`collector-adsb`**: Aircraft state vector ingestion tracking military, commercial, and emergency squawk flights.
- **`collector-crypto`**: High-frequency perpetual swaps, candle volatility, and volume anomaly tracking across major crypto exchanges.
- **`collector-cyber`**: BGP routing hijack anomalies, IP threat scores, and CVE vulnerability telemetry with equity ticker resolution.
- **`collector-prediction`**: Real-time contract odds and liquidity streams from Polymarket and Kalshi.
- **`collector-news`**: RSS/API breaking news ingestion and OSINT headline normalization.
- **`collector-radar`**: Cross-domain anomaly radar aggregator consolidating domain metrics into unified streams.

### 2. Enrichment & Threat Intelligence (`services/enrichment`)
- **OCC Option Contract Parser**: Extracts ticker, ISO expiry (`YYYY-MM-DD`), option side (`CALL`/`PUT`), and strike price ($) from 21-character OCC contract symbols.
- **Unsupervised Spatial Anomaly Detection**: IsolationForest & ONNX ML models evaluate deviations in vessel behavior, flight vectors, volume spikes, and network routes.
- **PostGIS Spatial Indexing**: Explicit longitude and latitude column mapping into PostgreSQL hypertables with spatial point indexing (SRID 4326).
- **Sanctions & Compliance Screening**: Direct cross-referencing against OFAC Specially Designated Nationals (SDN) and watchlists.
- **Spatial Chokepoint Indexing**: Automatic geo-fencing against strategic maritime bottlenecks (Strait of Hormuz, Bab-el-Mandeb, Suez Canal, Taiwan Strait, Malacca Strait) and active geopolitical conflict theaters.
- **Graph & Payload Promotion**: Reference data (sector, industry, index membership) is promoted to Neo4j via `GraphWriter` into `Topics.ONTOLOGY_PROPOSALS`, and active statistical correlation IDs (`corr:stat:{A}:{B}`, `granger:{A}:{B}:lag{L}`) are stamped onto enriched events.

### 3. Correlation & Storage Engines (`services/correlation` & `shared/db`)
- **TimescaleDB Multi-Timeframe Continuous Aggregates**: Durable `tradfi_bars` hypertable with automated continuous aggregates across `5m`, `10m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`, and `1mo` buckets and rolling 20-period continuous Z-score views.
- **Statistical Correlation Discovery Engine**: Scheduled topology-guided pairwise discovery computing rolling Pearson ($r, p$), Spearman, bidirectional Granger causality ($F$-statistic, optimal lag), and Engle-Granger cointegration.
- **Cross-Sector Hawkes Point Process**: `IntraTradFiHawkesCorrelator` modeling mutual cross-sector volatility contagion across 11 GICS sector ETFs.
- **Dynamic Threshold Calibration**: Real-time percentile calibration harness updating empirical significance cutoffs in Redis (zero hardcoded thresholds).
- **Unified Graph Governance & 30-Day Staleness Decay**: Centralized `VALID_PREDICATES` whitelist, single-write path via `GraphWriter` $\rightarrow$ `Topics.ONTOLOGY_PROPOSALS`, and continuous exponential edge decay ($w_{\text{effective}} = w_{\text{base}} \cdot e^{-\lambda \Delta t}$, $t_{1/2}=30\text{d}$).
- **Qdrant Vector Database**: Computes 384-dimensional embeddings (`all-MiniLM-L6-v2`) to cluster cross-domain events by semantic context.
- **Redis In-Memory Bus**: PubSub stream broadcasting, rate-limiting token buckets, quote caching, and active correlation ID indexing.

### 4. Quantitative Engine & Reasoning Swarm (`services/agents` & `services/reasoning`)
- **Quantitative Trading Engine**: Server-side deterministic trade signal enforcement:
  - Entry level verified against current market quote.
  - ATR-based stop loss calculation ($1.5 \times \text{ATR}$) and conviction-tiered Risk/Reward multipliers.
  - Empirical half-Kelly allocation clamping ($K_{\text{max}}$).
  - 1-hop Neo4j topology pre-checks (`sector`, `indices`, `supply_chain`, `correlations`) attached to advisory briefs.
- **Options & Quant Math Engine**: Black-Scholes pricing and analytical Greeks ($\Delta, \Gamma, \Theta, \mathcal{V}, \rho$), Brent-Dekker numerical IV root finding, and covered call overlay optimization.
- **Grounded StockCorrelationAgent**: Queries empirical correlation matrices and Granger causality statistics from Neo4j/Redis first, prompting the LLM exclusively to explain economic transmission mechanisms.
- **Macro Intelligence Engine**: Cointegration pair analysis via Engle-Granger tests, yield curve spread tracking, and z-score anomaly detection.
- **ContextBuilder**: Non-exclusion bidirectional 3-hop graph traversal capturing statistical, structural, and regulatory relationships with recency decay weighting.
- **Consensus Engine**: Subjective logic opinion fusion ($b, d, u, a$) across multi-agent predictions.
- **LLM Reasoning Swarm (`agents-heavy` & `agents-fast`)**:
  - **Heavy Analytical Tier (`agents-heavy` - `qwen2.5:7b`)**: Deep reasoning for Quant Research, Macro Strategy, Yield Curve Rates, Volatility Surfaces, and Graph Supervision.
  - **Fast Operational Tier (`agents-fast` - `qwen2.5:1.5b` / `gemma3:1b` fallback)**: Ultra-fast routing, headline NER extraction, and concept taxonomy management.

### 5. API Gateway & Tactical Radar Dashboard (`frontend` & `services/api_gateway`)
- **`api-gateway`**: High-performance FastAPI gateway with **cryptographic HMAC-SHA256 & JWT session-cookie verification**, rate limiting, Prometheus metrics, and multi-hop graph query endpoints (`/api/v1/graph/entity/{id}?hops=1|2`).
- **`frontend` (Sentinel Radar Dashboard)**: Next.js 14 interface with React, Deck.gl, Recharts, and TailwindCSS across 11 dedicated operational surfaces:
  - **`/` (Command Center)**: Unified situational intelligence HUD.
  - **`/graph` (Knowledge Graph & Empirical Correlation Explorer)**: 1-hop and 2-hop subgraph explorer with **epistemic edge differentiation** (dashed animated lines for empirical statistical edges vs solid neon lines for structural/index edges vs solid red for sanctions) and an **Edge Epistemic Inspector Drawer** detailing raw vs decayed weight, $r$, $p$, lag, $F$-stat, and Hawkes $\eta$.
  - **`/intelligence`**: Multi-domain real-time event intelligence feed.
  - **`/osint`**: Global geopolitical OSINT threat matrix.
  - **`/flow`**: Institutional dark pool prints and options sweeps.
  - **`/agents`**: AI agent swarm decision logs and telemetry status HUD.
  - **`/macro`**: Macroeconomic indicators, yield curve spreads, and economic calendar releases.
  - **`/options`**: Options flow analytics and volatility surfaces.
  - **`/crypto`**: Crypto perpetuals, funding rates, and volume spikes.
  - **`/charts`**: High-frequency multi-asset technical charts.
  - **`/map`**: Deck.gl 3D global globe and tactical map.

### 6. Observability, ML-Ops & Infrastructure
- **`ModelDriftScheduler` (`services/telemetry-worker`)**: Computes Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) statistics on rolling 24-hour ONNX anomaly distributions, automatically dispatching retrain triggers to `Topics.MODEL_RETRAIN_REQUESTS` when $\text{PSI} \ge 0.25$ or $\text{KS} \ge 0.05$.
- **`alert_manager`**: Escalation router delivering alerts via PagerDuty, Webhooks, Telegram, and Slack.
- **`prometheus` & `grafana`**: Time-series metrics collection server and operational monitoring dashboards.
- **`kafka-ui`**: Web UI for inspecting Kafka topics, partitions, and message offsets.

---

## 📐 Quantitative & Statistical Mathematics

Sentinel integrates a specialized quantitative math library in `shared/utils/quant_calc.py`:

- **Black-Scholes Greeks & Pricing**: Analytical Call/Put pricing with closed-form Greeks ($\Delta, \Gamma, \Theta, \mathcal{V}, \rho$).
- **Implied Volatility (IV) Root Finding**: Hybrid Newton-Raphson and Brent-Dekker solver with volatility smile boundary clamping ($0.001 \le \sigma \le 10.0$).
- **Covered Call Optimization**: Evaluates strike delta selection ($\Delta \approx 0.30$), annualized option yield, downside protection percentage, and max return.
- **Value at Risk (VaR) & Conditional VaR (CVaR)**: Non-parametric historical loss estimation at 95% and 99% confidence levels per position.
- **Engle-Granger Cointegration**: Two-step OLS regression testing stationary spread relationships ($y_t - \beta x_t - \alpha$) with Dickey-Fuller t-statistics and half-life estimation.
- **Granger Causality**: Vector Auto-Regression (VAR) $F$-test evaluating lead-lag causal directionality across time series.
- **Hawkes Point Process Branching Ratio**: Cross-domain and intra-sector volatility contagion excitation ratio ($\eta = \int \alpha(t) dt$).
- **30-Day Exponential Edge Recency Decay**: Continuous decay for graph confidence and weights:
  $$w_{\text{effective}}(t) = w_{\text{base}} \times \exp\left(-\frac{\ln 2}{30 \times 86400} \cdot \Delta t\right)$$
- **Empirical Half-Kelly Criterion**: Position sizing derived from historical win rates ($W$) and payoff ratios ($R$):
  $$K_{\text{half}} = 0.5 \times \left( W - \frac{1 - W}{R} \right)$$

---

## ⚡ Quick Start

### Prerequisites
- **Docker Engine** 24.0+ & **Docker Compose** v2+
- **Python** 3.11+ (for local development and test runner)
- **Node.js** 18+ & **npm** 9+ (for frontend development)

### 1. Environment Setup
Copy and customize the `.env` configuration file:
```bash
cp .env.example .env
```

### 2. Pull Local LLM Models (Ollama)
```bash
docker exec -it sentinel-ollama ollama pull qwen2.5:7b
docker exec -it sentinel-ollama ollama pull qwen2.5:1.5b
docker exec -it sentinel-ollama ollama pull gemma3:1b
```

### 3. Start Sentinel Platform
```bash
docker compose up --build -d
```

Access the dashboard at **`http://localhost:3000`**.

---

## 💻 CLI & API Operations

### 1. Docker Cluster Management
```bash
# Start all 29 services in background
docker compose up -d

# Inspect health and container states
docker compose ps

# Follow service logs
docker compose logs -f enrichment correlation api-gateway agents-heavy telemetry-worker

# Stop all services
docker compose down
```

### 2. API Gateway & Subgraph Queries
```bash
# Health Check
curl -s http://localhost:8000/api/v1/health -H "X-API-KEY: ${API_KEY}"

# Prometheus Metrics
curl -s http://localhost:8000/metrics

# 1-Hop & 2-Hop Epistemic Graph Query
curl -s "http://localhost:8000/api/v1/graph/entity/NVDA?hops=1" -H "X-API-KEY: ${API_KEY}"
curl -s "http://localhost:8000/api/v1/graph/entity/NVDA?hops=2" -H "X-API-KEY: ${API_KEY}"

# Dijkstra Shortest Relationship Path
curl -s "http://localhost:8000/api/v1/graph/shortest-path?source_id=NVDA&target_id=TSM" -H "X-API-KEY: ${API_KEY}"

# AI Financial Advisor Brief
curl -s http://localhost:8000/api/v1/financial/advice -H "X-API-KEY: ${API_KEY}"

# Real-Time WebSocket Feed
npx wscat -c "ws://localhost:8000/api/v1/events/ws/live-feed?api_key=${API_KEY}"
```

### 3. Automated Test Suite (241 Passing Tests)
```bash
# Run entire repository test suite
python -m pytest tests/

# Run targeted domain modules
python -m pytest tests/test_system_and_security.py
python -m pytest tests/test_quant_and_trading.py
python -m pytest tests/test_candles_and_registry.py
python -m pytest tests/test_correlation_and_graph.py
python -m pytest tests/test_statistical_discovery.py
python -m pytest tests/test_macro_calendar.py
python -m pytest tests/test_drift_scheduler.py
python -m pytest tests/test_agent_graph_integration.py
python -m pytest tests/test_payload_and_graph_promotion.py
```

---

## 🌐 Service & Port Directory

| Service | Host / Port | Authentication / Access |
| :--- | :--- | :--- |
| **Web Dashboard** | `http://localhost:3000` | Public Web UI |
| **Knowledge Graph Explorer** | `http://localhost:3000/graph` | Interactive 1–2 Hop Graph Explorer |
| **Grafana Monitoring** | `http://localhost:3001` | Configured via `.env` |
| **API Gateway** | `http://localhost:8000` | Header `X-API-KEY: ${API_KEY}` / HMAC Session |
| **Prometheus Server** | `http://localhost:9090` | Configured via `.env` |
| **Prometheus Metrics** | `http://localhost:8000/metrics` | Unauthenticated Scraper |
| **WebSocket Live Feed** | `ws://localhost:8000/api/v1/events/ws/live-feed` | Query Param `?api_key=` or Header |
| **Ollama LLM Engine** | `http://localhost:11434` | Local LLM REST API |
| **Kafka UI Manager** | `http://localhost:8080` | Configured via `.env` |
| **Kafka Broker** | `localhost:9092` | Configured via `.env` |
| **Neo4j Graph Browser** | `http://localhost:7474` (Bolt: `7687`) | Configured via `.env` |
| **Qdrant Dashboard** | `http://localhost:6333/dashboard` | Configured via `.env` |
| **TimescaleDB PostgreSQL** | `localhost:5432` | Configured via `.env` |
| **Redis Bus** | `localhost:6379` | Configured via `.env` |

---

**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
