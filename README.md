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

## 🛠️ Microservices & Component Directory

Sentinel is composed of **29 modular services & containers** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Ingestion Collectors (`services/collector-*`)
| Collector | Source / Protocol | Ingested Telemetry & Responsibilities |
| :--- | :--- | :--- |
| **`collector-tradfi`** | WebSocket / REST | Streaming US equity quotes, order book depth, dark pool block prints, and options flow sweeps with OCC symbol parsing (`AAPL240816C00220000`). |
| **`collector-macro`** | Polling / NY Fed / BLS / BEA | Live Federal Reserve SOFR risk-free rate, 12 macro proxy instruments (Crude, Brent, Gold, Yields, Indexes), and **Economic Calendar Collector** (`economic_calendar.py`) capturing CPI, NFP, FOMC, GDP, and PMI with surprise & bias calculations (`EventType.MACRO_RELEASE`). |
| **`collector-ais`** | WebSocket (AISStream) | Global maritime vessel positions, MMSI identifiers, navigation status, speed over ground (SOG), and geographic coordinates. |
| **`collector-adsb`** | REST / Beast Protocol | Aircraft state vectors tracking military, commercial, emergency squawks (7700/7600/7500), and flight route deviations. |
| **`collector-crypto`** | WebSocket (Binance/Coinbase) | High-frequency perpetual swaps, funding rates, open interest, and liquidation volume spikes. |
| **`collector-cyber`** | REST (BGPStream / NVD / IPQS) | BGP routing hijack anomalies, IP threat scores, and CVE vulnerability disclosures mapped to affected public tickers. |
| **`collector-prediction`** | REST / WS (Polymarket / Kalshi) | Real-time event contract probability odds, market liquidity, and rapid sentiment shifts. |
| **`collector-news`** | RSS / NewsAPI | Breaking global news headlines, OSINT feeds, and geopolitical dispatches. |
| **`collector-radar`** | Internal Stream Aggregator | Cross-domain anomaly radar aggregator consolidating domain metrics into normalized real-time telemetry. |

---

### 2. Enrichment & Threat Intelligence Subsystem (`services/enrichment`)
| Module | File | Core Capabilities & Algorithms |
| :--- | :--- | :--- |
| **Enrichment Engine** | `main.py` | High-throughput Kafka consumer routing raw events through specialized domain enrichers and anomaly scorers. |
| **Anomaly Scorer** | `anomaly_scorer.py` | Multi-domain `IsolationForest` ML scoring, Hawkes cross-domain intensity boosting, and rolling microstructure estimators. |
| **Reference Data** | `ref_data.py` | Caches company fundamentals (sector, industry, market cap tier, index co-membership) and promotes nodes/edges to Neo4j via `GraphWriter`. |
| **Entity Resolution** | `entity_resolver.py` | Cross-domain entity resolution linking MMSI, ICAO, Ticker, ASN, and OFAC identifiers into unified `Entity` objects. |
| **Graph Writer** | `graph_writer.py` | Universal write path emitting `MERGE_ONTOLOGY_NODE`, `LINK_ENTITY`, `upsert_equity()`, and `upsert_index()` to `Topics.ONTOLOGY_PROPOSALS`. |
| **AIS Gap Detector** | `gap_detector.py` | Tracks maritime transponder disabling ("dark ship" anomalies) near sanctions zones and strategic trade bottlenecks. |
| **Aviation Gap Detector** | `aviation_gap_detector.py` | Detects emergency transponder dropouts and suspicious flight pattern deviations in contested airspaces. |
| **Sanctions Engine** | `ofac_sync.py` | Direct synchronization with OFAC Specially Designated Nationals (SDN) watchlists with fuzzy name matching. |
| **Database Writer** | `db_writer.py` | Asynchronous batch writer persisting normalized events and PostGIS spatial points (SRID 4326) into TimescaleDB. |

#### **Domain Enrichers (`services/enrichment/enrichers/`)**
- **`tradfi.py`**: Microstructure calculations (Kyle's $\lambda$, Amihud illiquidity, Order Flow Imbalance, VWAP), SOFR risk-free rate caching, active statistical `correlation_ids` linking, and durable `tradfi_bars` hypertable persistence.
- **`crypto.py`**: Volume-to-open-interest ratios, funding rate dislocation, and perpetual contract liquidation cascades.
- **`cyber.py`**: ASN-to-equity ticker resolution, BGP route hijack classification, and CVE exploitability scoring.
- **`maritime.py`**: Geofencing against strategic chokepoints (Hormuz, Bab-el-Mandeb, Suez, Malacca, Taiwan Strait) and speed anomaly detection.
- **`aviation.py`**: Emergency squawk categorization and restricted airspace boundary crossing detection.
- **`prediction.py`**: Probability swing momentum and liquidity-adjusted sentiment shift estimation.
- **`news.py`**: NLP named entity recognition (NER), geopolitical tag resolution, and sentiment polarity scoring.

---

### 3. Correlation & Statistical Discovery Engine (`services/correlation`)
| Module | File | Responsibilities & Mathematical Models |
| :--- | :--- | :--- |
| **Statistical Discovery** | `statistical_discovery.py` | Scheduled topology-guided pairwise discovery reading from TimescaleDB CAGGs: rolling Pearson ($r, p$), Spearman, bidirectional Granger causality ($F$-statistic, optimal lag), and active Redis ID indexing (`sentinel:correlation:active_ids:{ticker}`). |
| **Hawkes Point Process** | `hawkes_correlator.py` | Multivariate self-exciting point process modeling cross-domain contagion and calculating Hawkes branching ratios ($\eta$). |
| **Sector Hawkes** | `sector_hawkes.py` | `IntraTradFiHawkesCorrelator` tracking mutual volatility excitation across 11 GICS sector ETFs. |
| **Semantic Correlator** | `soft_correlator.py` | Qdrant vector database client generating 384-dimensional embeddings (`all-MiniLM-L6-v2`) for cross-domain semantic narrative clustering. |
| **Cascade Engine** | `cascade.py` | Evaluates downstream propagation time windows and systemic contagion risks across correlated entities. |
| **Event Store** | `event_store.py` | In-memory rolling event ring buffers for high-frequency analytical window queries. |

---

### 4. AI Reasoning Swarm & Autonomous Agents (`services/agents` & `services/reasoning`)
| Agent / Module | File | Operational Role & AI Tier |
| :--- | :--- | :--- |
| **Quant Trading Engine** | `quant_trading_engine.py` | Deterministic trade signal validation: current quote verification, ATR stop loss ($1.5 \times \text{ATR}$), conviction Risk/Reward tiers, empirical half-Kelly sizing, 1-hop Neo4j topology checks, and Alpaca paper order execution. |
| **Macro Intelligence** | `macro_intelligence_engine.py` | Engle-Granger cointegration testing ($y_t - \beta x_t - \alpha$), yield curve inversion monitoring (2Y/10Y/30Y), and dynamic decoupling z-score detection. |
| **Stock Correlation Agent**| `stock_correlation_agent.py` | Empirically grounded agent: queries Neo4j/Redis correlation matrices first and uses LLMs exclusively to synthesize causal transmission rationales. |
| **Adversarial Wargamer** | `adversarial_wargamer.py` | Geopolitical red-teaming, black swan simulation, and supply chain fragility stress testing. |
| **Knowledge Graph Engine** | `knowledge_graph_engine.py` | Evaluates graph topology, synthesizes entity connections, and routes proposals through Kafka. |
| **Consensus Engine** | `consensus_engine.py` | Subjective logic opinion fusion ($b, d, u, a$) reconciling multi-agent confidence and uncertainty. |
| **Edge Validator** | `edge_validator.py` | Validates proposed graph edges against the centralized `VALID_PREDICATES` whitelist before persistence. |
| **Radar Agent** | `radar_agent.py` | Cross-domain threshold monitoring, multi-stream anomaly synthesis, and priority alerting. |
| **Context Builder** | `context_builder.py` | Non-exclusion bidirectional 3-hop graph traversal (`MATCH (v)-[r*1..3]-(n)`) with 30-day exponential recency decay weighting. |
| **Scenario Generator** | `scenario_generator.py` | LLM-driven predictive scenario synthesis modeling strategic and market outcomes. |
| **Calibration Harness** | `calibration_harness.py` | Dynamic percentile threshold calibration updating Redis cutoffs (zero hardcoded static numbers). |

#### **LLM Reasoning Swarm Tiers**
- **Heavy Analytical Tier (`agents-heavy` - `qwen2.5:7b`)**: Complex reasoning for Quant Research, Macro Strategy, Yield Curve Analysis, and Graph Supervision.
- **Fast Operational Tier (`agents-fast` - `qwen2.5:1.5b` / `gemma3:1b` fallback)**: High-speed triage, routing, headline NER extraction, and concept taxonomy management.

---

### 5. Shared Infrastructure & Quantitative Math (`shared/`)
| Directory / Module | Core Functionality |
| :--- | :--- |
| **`shared/utils/quant_calc.py`** | Black-Scholes analytical Greeks ($\Delta, \Gamma, \Theta, \mathcal{V}, \rho$), Brent-Dekker numerical IV root finding, covered call overlays, dynamic Kelly criterion, Kyle's $\lambda$, Amihud illiquidity, Order Flow Imbalance (OFI), Parkinson volatility, and VaR 95% / CVaR 99%. |
| **`shared/utils/streaming_detectors.py`**| Welford online variance, CUSUM change-point detectors, EWMA volatility ($\lambda=0.94$), and rolling z-score monitors. |
| **`shared/utils/model_registry.py`** | Scikit-Learn and ONNX model registry managing IsolationForest anomaly detector pipelines. |
| **`shared/utils/ollama.py`** | Resilient Ollama HTTP client with SHA-256 prompt response caching, automatic retries, and fallback model tier routing. |
| **`shared/utils/regions.py`** | Geographic definitions for strategic trade chokepoints and active geopolitical conflict theaters. |
| **`shared/utils/cyber_mapper.py`** | Autonomous System Number (ASN) to public equity ticker lookup and mapping engine. |
| **`shared/utils/sanctions.py`** | OFAC SDN name normalization and fuzzy identity matching algorithms. |
| **`shared/utils/candles.py`** | Multi-timeframe OHLCV candle aggregator and resampler (`5m` to `1mo`). |
| **`shared/models/`** | Strongly-typed Pydantic schemas: `events.py` (`NormalizedEvent`, `FinancialData`, `MacroReleaseData`), `financial.py`, `graph.py`, `reasoning.py`. |
| **`shared/db/`** | Database pools and migration runners for TimescaleDB (PostgreSQL), Neo4j (Cypher), Qdrant, and Redis. |
| **`shared/kafka/`** | Kafka topic definitions (`Topics.RAW_*`, `Topics.ENRICHED_EVENTS`, `Topics.ONTOLOGY_PROPOSALS`), producer and consumer wrappers. |

---

### 6. Auxiliary Workers, Observability & ML-Ops
- **`telemetry-worker` (`services/telemetry-worker`)**: Container metrics, Redis heartbeats, and **`ModelDriftScheduler`** computing Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) drift statistics on rolling 24-hour ONNX distributions, triggering retraining via `Topics.MODEL_RETRAIN_REQUESTS`.
- **`dlq-worker` (`services/dlq-worker`)**: Dead-letter queue consumer handling poison-pill message quarantine and automated retries.
- **`alert_manager` (`services/alert_manager`)**: Multi-channel escalation router dispatching alerts via PagerDuty, Slack, Telegram, and Webhooks.
- **`api-gateway` (`services/api_gateway`)**: FastAPI gateway with cryptographic HMAC/JWT session auth, rate limiting, Prometheus metrics, and multi-hop graph endpoints (`/api/v1/graph/entity/{id}?hops=1|2`).
- **`prometheus` & `grafana`**: Infrastructure monitoring, operational latency dashboards, and scrape metrics.

---

### 7. Frontend Radar Dashboard (`frontend/`)
Built with **Next.js 14**, **React**, **Deck.gl**, **Recharts**, and **TailwindCSS** across 11 dedicated operational surfaces:
- **`/` (Command Center)**: Consolidated situational awareness and real-time alerts HUD.
- **`/graph` (Knowledge Graph & Empirical Correlation Explorer)**: Interactive 1-hop and 2-hop graph explorer with **epistemic edge differentiation** (dashed animated lines for empirical statistical edges vs solid neon lines for structural/index edges vs solid red for sanctions) and an **Edge Epistemic Inspector Drawer** detailing raw vs decayed weight, $r$, $p$, lag, $F$-stat, and Hawkes $\eta$.
- **`/intelligence`**: High-frequency streaming multi-domain event intelligence feed.
- **`/osint`**: Global geopolitical OSINT threat matrix and cross-domain entity correlations.
- **`/flow`**: Institutional dark pool block prints and options sweeps flow.
- **`/agents`**: AI agent swarm telemetry, decision audit logs, and LLM health inspector.
- **`/macro`**: Macroeconomic indicator matrix, yield curve inversion tracker, and economic calendar releases.
- **`/options`**: Options flow analytics and volatility surface visualizations.
- **`/crypto`**: Crypto perpetuals volatility, funding rates, and liquidation metrics.
- **`/charts`**: High-frequency multi-asset technical charts with volume indicators.
- **`/map`**: Deck.gl 3D global globe rendering real-time AIS vessel vectors, ADSB flights, and strategic chokepoints.

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
| **Grafana Monitoring** | `http://localhost:3001` | Admin (`admin / admin`) |
| **API Gateway** | `http://localhost:8000` | Header `X-API-KEY: ${API_KEY}` / HMAC Session |
| **Prometheus Server** | `http://localhost:9090` | Public Monitoring UI |
| **Prometheus Metrics** | `http://localhost:8000/metrics` | Unauthenticated Scraper |
| **WebSocket Live Feed** | `ws://localhost:8000/api/v1/events/ws/live-feed` | Query Param `?api_key=` or Header |
| **Ollama LLM Engine** | `http://localhost:11434` | Local LLM REST API |
| **Kafka UI Manager** | `http://localhost:8080` | Public Admin UI |
| **Kafka Broker** | `localhost:9092` | Plaintext (`PLAINTEXT://localhost:9092`) |
| **Neo4j Graph Browser** | `http://localhost:7474` (Bolt: `7687`) | Configured via `.env` |
| **Qdrant Dashboard** | `http://localhost:6333/dashboard` | Public Vector UI |
| **TimescaleDB PostgreSQL** | `localhost:5432` | Configured via `.env` |
| **Redis Bus** | `localhost:6379` | Configured via `.env` |

---

**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
