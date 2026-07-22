# Sentinel 

**Alessio Naji**

Sentinel is a multi-domain, real-time event-driven intelligence and threat correlation engine. It ingests, processes, normalizes, and correlates high-velocity streaming data across aviation, maritime, finance, macroeconomics, cybersecurity, prediction markets, and global news feeds to identify structural anomalies, track geopolitical shifts, and generate automated tactical intelligence.

Built on an event-driven microservices architecture, Sentinel routes streaming telemetry through Apache Kafka and leverages an autonomous network of local AI agents (powered by Ollama: Qwen 2.5, Gemma 2B, Llama 3) alongside TimescaleDB, Neo4j, Qdrant, and Redis for automated reasoning, semantic correlation, and graph synthesis.

---

## Code Structure

The repository is organized into the following components:

- `services/`: Self-contained Python microservices containerized via Docker:
  - `collector-*`: Domain-specific streaming collectors (ADS-B aviation, AIS maritime, TradFi, Crypto, Cyber, Macro, News, Prediction markets, Radar).
  - `enrichment/`: Normalizes raw events, calculates online Isolation Forest anomaly scores, performs OFAC sanctions checks, writes time-series to TimescaleDB, and updates Neo4j entity graphs.
  - `correlation/`: Evaluates enriched events against hot-reloaded dynamic rules from Redis (`sentinel:correlation:dynamic_rules`) and Qdrant vector embeddings.
  - `reasoning/`: Synthesizes correlation clusters into tactical scenarios using local LLM inference via Ollama.
  - `agents/`: Autonomous Python agent swarm executing specialized quantitative, rates, wargaming, and research intelligence tasks.
  - `alert_manager/`: Deduplicates and rate-limits outbound alerts delivered via Telegram and Webhooks.
  - `api_gateway/`: FastAPI REST interface locked down with global `X-API-KEY` security.
  - `dlq-worker/`: Dead-Letter Queue processor capturing corrupted payloads into Postgres `failed_events`.
  - `telemetry-worker/`: Ingests and persists LLM execution metrics (prompt lengths, token counts, model choice, latency).
- `shared/`: Common libraries used across microservices:
  - `db/`: Database connection clients for TimescaleDB (`asyncpg`), Redis (`redis.asyncio`), and Neo4j (`AsyncGraphDatabase`).
  - `kafka/`: Centralized Kafka producer/consumer wrappers (`aiokafka`) and topic registry (`Topics`).
  - `models/`: Pydantic v2 schemas (`RawEvent`, `NormalizedEvent`, `CorrelationCluster`, `Scenario`, `AlertTier`).
  - `utils/`: Core utilities including `ollama.py` (LLM client with circuit breakers), `candles.py` (structural candle building), `equities.py`, `sanctions.py`, `regions.py`, `websocket.py`, and `logging.py`.
- `frontend/`: Next.js web application for real-time visualization and interactive intelligence dashboards.
- `infrastructure/`: Kubernetes deployment manifests, Helm charts, and operational configs.
- `models/`: Exported machine learning models (e.g., spatial/temporal Isolation Forests).
- `sentinel_config.yaml`: Central configuration for static watchlists, geopolitical chokepoints, and high-value target assets.
- `docker-compose.yml`: Primary orchestration file to deploy the entire multi-container backend platform.

---

## Architecture

Sentinel uses [Apache Kafka](https://kafka.apache.org/) as a central event bus to fully decouple data collection from downstream enrichment, correlation, agent execution, and storage.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 DATA COLLECTORS (8 DOMAINS)                                  │
│ (ADS-B, AIS Maritime, TradFi, Crypto, Cyber, Macro Futures, RSS News, Prediction Markets)  │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               │ RawEvents (events.raw.*)
                                               ▼
                                      ┌────────────────┐
                                      │  Apache Kafka  │
                                      └────────┬───────┘
                                               │
                                               ▼
                                      ┌────────────────┐
                                      │  Enrichment    │◄─── OFAC Sanctions / Isolation Forest
                                      └────────┬───────┘
                                               │ Enriched Events (enriched.events)
                                               ▼
                               ┌───────────────┴───────────────┐
                               │                               │
                               ▼                               ▼
                      ┌────────────────┐              ┌────────────────┐
                      │  TimescaleDB   │              │  Correlation   │◄── Qdrant & Redis Rules
                      │  & Neo4j Graph │              └────────┬───────┘
                      └────────────────┘                       │ CorrelationCluster
                                                               ▼
                                                      ┌────────────────┐
                                                      │  Agent Swarm & │◄── Ollama Local LLMs
                                                      │   Reasoning    │    (Qwen 2.5, Gemma, Llama3)
                                                      └────────┬───────┘
                                                               │
                                                               ▼
                                                      ┌────────────────┐
                                                      │ Alert Manager  │──► Telegram & Webhooks
                                                      │ & API Gateway  │──► REST API / Web UI
                                                      └────────────────┘
```

### Data Pipeline Stages

1. **Collection**: Specialized `collector-*` microservices poll external APIs, websockets, and public feeds. Features built-in resiliency:
   - **AIS Maritime**: Emits synthetic vessel telemetry for key chokepoints (Hormuz, Suez, Malacca) when upstream streams are disconnected.
   - **ADS-B Aviation**: Tracks OpenSky API rate-limit headers (`OPENSKY_RATE_LIMITED_UNTIL`) to prevent redundant sleep loops.
   - **Macro Futures**: Single-ticker `fast_info` fallback for commodity and index futures (`NG=F`, `GC=F`, `SI=F`, etc.) when bulk downloads drop columns.
   - **Cyber**: Tracks CISA KEVs, Censys exposed ICS ports, Ransomware victims, and continuous RIPE RIS BGP route announcements with 20s WebSocket keepalive pings.
2. **Standardization & Publication**: Raw data is wrapped into standardized Pydantic `RawEvent` objects and published to Kafka raw event topics (`events.raw.*`).
3. **Enrichment**: The `enrichment` service normalizes events into `NormalizedEvent` objects, calculates dynamic Isolation Forest anomaly scores, performs fuzzy OFAC sanctions checks, writes time-series data to TimescaleDB, and updates Neo4j entity graph nodes/edges.
4. **Correlation**: The `correlation` service evaluates enriched events against dynamic correlation rules hot-reloaded from Redis (`sentinel:correlation:dynamic_rules`) and vector similarity embeddings in Qdrant. Seed baseline rules automatically populate Redis on cold start.
5. **Reasoning & Agent Swarm**:
   - The `reasoning` service feeds correlation clusters and graph subgraphs into local LLMs to generate tactical scenarios (`Scenario`).
   - Autonomous python agents react to specialized domain topics (e.g. `yield_curve_agent` computes 2Y-10Y spreads and TIPS real yield breakevens; `financial_advisor` evaluates multi-timeframe EMA/RSI confluence and Kelly Criterion capital sizing).
6. **Alerting & Delivery**: The `alert_manager` service deduplicates alerts (6-hour TTL per entity) and enforces non-blocking per-rule rate limits before delivering markdown alerts via Telegram and Webhooks.

---

## Shared Resilient Infrastructure

### Resilient LLM Inference (`OllamaClient`)
All local LLM calls across agents are unified in `shared/utils/ollama.py`:
- **Context Size Capping**: Explicitly sets `"num_ctx": 4096` in request options and truncates prompts >3,500 characters to fit within context limits without CPU timeouts.
- **Adaptive Circuit Breaker**: Tracks consecutive timeouts per model (threshold: 3). Opens circuit with a 15-second half-open recovery trial loop.
- **Fallback Chain**: Automatically fails over across model tiers (`qwen2.5:7b` → `gemma:2b` → `llama3:latest`) on error or timeout.

### Database & Storage Systems
- **TimescaleDB (PostgreSQL)**: Serves as the primary time-series hypertable with automated migration management (`shared/db/migrate.py`) and asyncpg connection pooling.
- **Redis**: Low-latency cache for latest quotes (`sentinel:quotes:latest:*`), sliding-window pricing candles, rate-limiting, watched tickers, and hot-reloaded dynamic rules.
- **Neo4j**: Graph database mapping ontological entity relationships (vessels, sanctions, IP addresses, companies, country nodes). APOC procedures are strictly scoped to safe algorithms (`apoc.meta.*`, `apoc.algo.*`, `apoc.spatial.*`, `apoc.coll.*`) to eliminate host RCE vectors.
- **Qdrant**: High-performance vector database storing event embeddings for soft-correlation searches.

---

## Agent Swarm Architecture

The Python agent swarm executes `asyncio`-native tasks without blocking the main event loops:

- **`YieldCurveMacroRatesAgent`**: Tracks Treasury yield curve dynamics (2Y, 10Y, 30Y yields), SOFR, credit ETF ratios (HYG/LQD), and computes normalized TIPS real yield breakeven inflation rates.
- **`FinancialAdvisor`**: Executes quantitative technical analysis across equity and crypto watchlists, evaluating multi-timeframe EMA/RSI confluence, support/resistance, and Kelly Criterion capital sizing.
- **`QuantResearcher`**: Investigates market volume and volatility anomalies across TradFi and Crypto data feeds.
- **`OntologyMaster`**: Manages the dynamic Neo4j knowledge graph, expands entity taxonomies, and processes unknown entity proposals.
- **`NewsIntel`**: Parses unstructured RSS news feeds into structured intelligence briefs.
- **`MacroStrategist` & `MacroCointegrationEngine`**: Tracks structural macroeconomic regime shifts using online Engle-Granger statistical cointegration tracking.
- **`RuleAgent`**: Evaluates static correlation rules.
- **`Supervisor`**: Synchronizes graph operations to prevent write conflicts in Neo4j.

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started) & [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.11+](https://www.python.org/downloads/)
- [Node.js 18+](https://nodejs.org/) (for the frontend application)

### Quickstart (Docker Compose)

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd sentinel
   ```

2. **Configure Environment Variables:**
   Create a `.env` file in the root directory (or use `.env.example` defaults):
   ```env
   POSTGRES_USER=sentinel
   POSTGRES_PASSWORD=sentinel_local_dev
   POSTGRES_DB=sentinel
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=sentinel_graph
   API_GATEWAY_KEY=sentinel-dev-key-2026
   ```

3. **Boot the Backend Stack:**
   ```bash
   docker-compose up --build -d
   ```

4. **Monitor Platform Logs:**
   ```bash
   docker-compose logs -f
   ```

5. **Run Test Suite:**
   ```bash
   python -m pytest
   ```

---

## Accessing Services & Interfaces

- **Backend REST API Gateway:** http://localhost:8000 (Requires `X-API-KEY: sentinel-dev-key-2026`)
- **Frontend Web UI:** http://localhost:3000
- **Kafka UI:** http://localhost:8080
- **Neo4j Browser:** http://localhost:7474
- **Qdrant Dashboard:** http://localhost:6333/dashboard

---

## License & Author

**Author:** Alessio Naji  
**License:** Proprietary / All Rights Reserved.
