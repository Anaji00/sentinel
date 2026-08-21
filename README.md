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
                             [ NGINX TLS INGRESS (port 443) ]
                    (HTTP→HTTPS 301, HSTS, TLSv1.2/1.3, Security Headers)
                                                 │
                                                 ▼
                                    [ OBSERVABILITY & ML-OPS ]
                        (Prometheus, Grafana, Model Drift PSI/KS Scheduler)
```

---

## 🛠️ Microservices & Subsystem Architecture

Sentinel is composed of **30 modular services & containers** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Ingestion Collectors (`services/collector-*`)
- **`collector-tradfi`**: Streaming US equity quotes, order book depth, dark pool prints, and options flow with OCC standardized contract parsing (`AAPL240816C00220000`).
- **`collector-filings`**: Real-time **SEC EDGAR Corporate Filings** (Form 8-K material corporate disclosures, S-1/424B offerings, 10-K/10-Q periodic financials) with direct inline document URLs, and **13F Institutional Holdings** with live SEC EDGAR XML InfoTable parsing, dynamic company ticker registry loading (`company_tickers.json`), and Redis-cached CIK→ticker resolution.
- **`collector-macro`**: Live Federal Reserve SOFR risk-free rate polling, 12 macro proxy instruments (Crude, Brent, NatGas, Gold, Silver, Yields, Indexes), **Economic Calendar Collector** ingesting scheduled releases (CPI, Core CPI, NFP, Unemployment, FOMC Rate Decisions, GDP, ISM Manufacturing PMI, PCE), **Supply Chain & Freight Index Poller** (Baltic Dry Index BDI, Container Freight Rates FBX/Harpex), and **Federal Register Regulatory Policy Tracker** (export controls, semiconductor policy, antitrust, Section 301 tariffs).
- **`collector-social`**: Primary-source real-time OSINT & social signal ingestion from curated **Telegram public channels** (geopolitical/maritime), **Reddit subreddits** (financial/geopolitical), and breaking **X/Twitter** squawk handles, tagging events with `source_type: "primary_social"` and epistemic reliability weights.
- **`collector-ais`**: Real-time WebSocket tracking global maritime vessel positions, MMSI identifiers, navigation status, SOG, and coordinates across coastal chokepoints.
- **`collector-adsb`**: Aircraft state vector ingestion tracking military, commercial, and emergency squawk flights via open academic sensor networks.
- **`collector-crypto`**: High-frequency perpetual swaps, Coinbase spot tape, **Multi-Chain On-Chain Whale Tracking** across Ethereum, Arbitrum, and Base (USDT, USDC, DAI, WBTC, WETH, ARB, cbBTC), and **Cross-Exchange Basis/Funding Divergence Engine** (Binance vs Bybit vs Kraken).
- **`collector-cyber`**: BGP routing hijack anomalies, IP threat scores, and CVE vulnerability telemetry with equity ticker resolution.
- **`collector-prediction`**: Real-time contract odds and liquidity streams from Polymarket and Kalshi.
- **`collector-news`**: RSS wire services (BBC, France24, NPR, AP, NYT) tagged with `source_type: "wire"`.
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
- **Strategy Backtester & Validation Gatekeeper**: Replays historical multi-timeframe CAGG price bars (`5m` to `1d`) through quantitative strategies, calculating realized Sharpe, Sortino, max drawdown ($\text{MDD}$), and empirical probability calibration curves (binned conviction vs win rate, Brier score) before signals are trusted live.
- **Feature Flags & Emergency Kill Switches**: Redis-backed `FeatureFlagManager` enabling zero-downtime platform-wide signal gating (`covered_calls`, `granger_causality`, `hawkes_contagion`, `crypto_liquidations`, `insider_clustering`), gradual rollout percentages ($0\text{–}100\%$), and ticker whitelists with instantaneous single-signal and master kill switches.
- **Grounded StockCorrelationAgent**: Queries empirical correlation matrices and Granger causality statistics from Neo4j/Redis first, prompting the LLM exclusively to explain economic transmission mechanisms.
- **Macro Intelligence Engine**: Cointegration pair analysis via Engle-Granger tests, yield curve spread tracking, and z-score anomaly detection.
- **ContextBuilder**: Non-exclusion bidirectional 3-hop graph traversal capturing statistical, structural, and regulatory relationships with recency decay weighting.
- **Consensus Engine**: Subjective logic opinion fusion ($b, d, u, a$) across multi-agent predictions.
- **LLM Reasoning Swarm (`agents-heavy` & `agents-fast`)**:
  - **Heavy Analytical Tier (`agents-heavy` - `qwen2.5:7b`)**: Deep reasoning for Quant Research, Macro Strategy, Yield Curve Rates, Volatility Surfaces, and Graph Supervision.
  - **Fast Operational Tier (`agents-fast` - `qwen2.5:1.5b` / `gemma3:1b` fallback)**: Ultra-fast routing, headline NER extraction, and concept taxonomy management.

### 5. API Gateway & Tactical Radar Dashboard (`frontend` & `services/api_gateway`)
- **`api-gateway`**: High-performance FastAPI gateway with **cryptographic HMAC-SHA256 & JWT session-cookie verification**, rate limiting, Prometheus metrics, multi-hop graph queries (`/api/v1/graph/entity/{id}?hops=1|2`), strategy backtesting (`/api/v1/backtest/run`), model explainability audit trails (`/api/v1/explain/event/{id}`), and feature flag kill switches (`/api/v1/flags`).
- **`frontend` (Sentinel Radar Dashboard)**: Next.js 14 interface with React, Deck.gl, Recharts, and TailwindCSS across 11 dedicated operational surfaces:
  - **`/` (Command Center)**: Unified situational intelligence HUD.
  - **`/graph` (Knowledge Graph & Empirical Correlation Explorer)**: 1-hop and 2-hop subgraph explorer with **epistemic edge differentiation** (dashed animated lines for empirical statistical edges vs solid neon lines for structural/index edges vs solid red for sanctions) and an **Edge Epistemic Inspector Drawer** detailing raw vs decayed weight, $r$, $p$, lag, $F$-stat, and Hawkes $\eta$.
  - **Model Cards & Explainability Modal**: One-click factor attribution waterfall, step-by-step score derivation timeline, data provenance, and drift stability inspector embedded across conviction plays and alert feeds.
  - **Signal Governance & Kill Switch Panel**: Real-time feature flag toggles, rollout percentage sliders, and emergency signal kill switches.
  - **`/intelligence`**: Multi-domain real-time event intelligence feed.
  - **`/osint`**: Global geopolitical OSINT threat matrix.
  - **`/flow`**: Institutional dark pool prints and options sweeps.
  - **`/agents`**: AI agent swarm decision logs and telemetry status HUD.
  - **`/macro`**: Macroeconomic indicators, yield curve spreads, and economic calendar releases.
  - **`/options`**: Options flow analytics and volatility surfaces.
  - **`/crypto`**: Crypto perpetuals, funding rates, and volume spikes.
  - **`/charts`**: High-frequency multi-asset technical charts.
  - **`/map`**: Deck.gl 3D global globe and tactical map.

### 6. Enterprise Governance, Security & Infrastructure
- **TLS Ingress Reverse Proxy**: Nginx-based TLS termination on port 443 with HTTP→HTTPS 301 redirect, `Strict-Transport-Security` (HSTS), `X-Frame-Options`, `X-Content-Type-Options`, modern TLSv1.2/1.3 cipher suites, and automated self-signed certificate bootstrapping for local development. All internal services are isolated behind the ingress with no direct host port exposure.
- **Fail-Closed Secrets Management**: `shared/utils/secrets.py` centralizes all environment configuration with `required=True` enforcement — the gateway **refuses to start** without `SESSION_SECRET`. `resolve_env_var` permits hardcoded dev fallbacks **only** when `SENTINEL_ENV` is in the `SAFE_DEV_ENVS` whitelist (`dev`, `test`, `local`). Automated token masking in logs and health audits (`/api/v1/health/secrets`).
- **Non-Root Container Execution**: All Python containers run as `sentinel` (UID 1001), preventing container-escape privilege escalation.
- **Hardened Session Cookies**: `sentinel_session` cookie enforces `HttpOnly: true`, `Secure: true`, `SameSite: Strict`, and `Path: /`.
- **Conditional CORS**: `allow_origin_regex` for localhost is restricted to `SAFE_DEV_ENVS` environments. In production, origins are explicitly enumerated via `CORS_ALLOWED_ORIGINS` with no open regex.
- **Container Resource Limits**: CPU and memory limits enforced across all 30 services (`deploy.resources.limits`) — from Ollama (4 CPU / 8G) down to collectors (0.5 CPU / 512M) — preventing host starvation.
- **Universal Heartbeat Coverage & Data Health Dashboard**: Structured 15s heartbeats across all 15 collectors and background workers aggregated at `GET /api/v1/health/data` for real-time cluster liveness and degradation tracking.
- **Dynamic Watchlist Governance**: Real-time ticker registration in Redis (`sentinel:watched:equities`) with a hard **Finnhub 50-ticker clamp** and instant WebSocket repointing via Redis Pub/Sub (`sentinel:collector:watchlist_sync`), eliminating static ticker hardcoding.
- **Role-Based Access Control (RBAC)**: Hierarchical access control (`ADMIN > ANALYST > VIEWER`) enforced across all API routes via the `require_role` FastAPI dependency, with cryptographic JWT session-cookie verification (no spoofable client headers).
- **Immutable SHA-256 Hash-Chained Audit Ledger**: Cryptographically chained audit log (`shared/utils/audit_ledger.py`) guaranteeing non-repudiation for watchlist modifications, flag toggles, manual trades, and case updates, with automated tamper verification (`POST /api/v1/audit/verify`).
- **Broker Abstraction Layer**: Clean separation of execution logic via `BrokerInterface` supporting `PaperBroker` (with live Redis quote resolution, realistic slippage, and commission simulation) and `AlpacaBroker` (Paper & Live API v2).
- **Investigation Case Management**: Analyst collaboration workspace (`shared/models/cases.py` & `/api/v1/cases`) enabling cross-domain evidence linking, severity escalation, and multi-agent notes.
- **Executive Intelligence Reporting**: Automated markdown and brief synthesis engine (`ReportGenerator` & `/api/v1/reports/generate`) summarizing high-confidence anomalies and portfolio risk.
- **`ModelDriftScheduler` (`services/telemetry-worker`)**: Computes Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) statistics on rolling 24-hour ONNX anomaly distributions, automatically dispatching retrain triggers to `Topics.MODEL_RETRAIN_REQUESTS` when $\text{PSI} \ge 0.25$ or $\text{KS} \ge 0.05$.
- **`alert_manager`**: Escalation router delivering alerts via PagerDuty, Webhooks, Telegram, and Slack.
- **`prometheus` & `grafana`**: Time-series metrics collection and operational monitoring dashboards (port-isolated behind ingress, accessed via internal `sentinel-network`).
- **`kafka-ui`**: Web UI for inspecting Kafka topics, partitions, and message offsets (internal network only).

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

Access the dashboard at **`https://localhost`** (Nginx TLS ingress auto-generates a self-signed certificate on first boot; mount production certs at `deploy/nginx/ssl/sentinel.crt` and `sentinel.key` for trusted HTTPS).

---

## 💻 CLI & API Operations

### 1. Docker Cluster Management
```bash
# Start all 30 services in background
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

### 3. Automated Test Suite (292 Passing Tests)
```bash
# Run entire repository test suite
python -m pytest tests/

# Run targeted security, domain expansion & integrity modules
python -m pytest tests/test_rbac_security.py
python -m pytest tests/test_telegram_formatting.py
python -m pytest tests/test_integrity_layer.py
python -m pytest tests/test_domain_expansion.py
python -m pytest tests/test_domain_coverage_gaps.py
python -m pytest tests/test_enterprise_features.py
python -m pytest tests/test_signal_validation_and_governance.py
python -m pytest tests/test_system_and_security.py
python -m pytest tests/test_quant_and_trading.py
python -m pytest tests/test_correlation_and_graph.py
python -m pytest tests/test_statistical_discovery.py
python -m pytest tests/test_macro_calendar.py
python -m pytest tests/test_drift_scheduler.py
python -m pytest tests/test_payload_and_graph_promotion.py
python -m pytest tests/test_tier2_hardening.py
```

---

## 🌐 Service & Port Directory

All public traffic is routed through the **Nginx TLS Ingress** on ports `80` (HTTP→HTTPS redirect) and `443` (TLS termination). Internal datastores and monitoring tools are isolated on the Docker `sentinel-network` with no direct host port exposure.

| Service | Access | Authentication |
| :--- | :--- | :--- |
| **Web Dashboard** | `https://localhost/` | Signed `sentinel_session` cookie |
| **Knowledge Graph Explorer** | `https://localhost/graph` | Interactive 1–2 Hop Graph Explorer |
| **API Gateway (REST)** | `https://localhost/api/v1/...` | Header `X-API-KEY` / HMAC Session Cookie |
| **WebSocket Live Feed** | `wss://localhost/ws/...` | Query Param `?api_key=` or Cookie |
| **Prometheus Metrics** | `https://localhost/api/v1/health/metrics` | Scraped via internal network |

### Internal Services (Docker `sentinel-network` only — no host port exposure)

| Service | Internal Address | Purpose |
| :--- | :--- | :--- |
| **Grafana** | `grafana:3000` | Operational monitoring dashboards |
| **Prometheus** | `prometheus:9090` | Time-series metrics collection |
| **Ollama LLM** | `ollama:11434` | Local LLM inference engine |
| **Kafka Broker** | `kafka:29092` | Event streaming backbone |
| **Kafka UI** | `kafka-ui:8080` | Topic inspection |
| **Neo4j** | `neo4j:7687` | Entity & correlation knowledge graph |
| **TimescaleDB** | `timescaledb:5432` | Time-series OHLCV and event storage |
| **Redis** | `redis:6379` | PubSub, caching, rate limiting |
| **Qdrant** | `qdrant:6333` | Semantic vector embeddings |

---

**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
