# Sentinel

Sentinel is an event-driven data aggregation and analysis engine. It ingests, processes, and correlates streaming data across aviation, maritime, finance, and cyber domains to identify anomalies and track structural shifts.

Built on a microservices architecture, Sentinel routes high-throughput telemetry through Kafka and uses local LLM agents (Llama3) alongside TimescaleDB and Neo4j for automated reasoning and intelligence synthesis.

## Code Structure

The project is organized into the following directories:

-   `services/`: Contains the individual microservices (e.g., collectors, correlation, reasoning, agents).
-   `shared/`: Holds common code, such as database handlers (`db/`), Kafka utilities (`kafka/`), and business logic (`utils/`).
-   `frontend/`: Contains the Next.js web-based user interface.
-   `infrastructure/`: Includes Kubernetes deployment files and Helm charts.
-   `models/`: Stores exported ONNX machine learning models (e.g., spatial/temporal Isolation Forests).
-   `regions.geojson`: A geographic boundary file used to trigger geofencing alerts (e.g., Strait of Hormuz).
-   `sentinel_config.yaml`: Central file for static configuration (e.g., watchlists, keywords).
-   `docker-compose.yml`: Primary orchestration file to boot up the entire backend stack (Kafka, Redis, TimescaleDB, Neo4j, Microservices).

## Architecture

Sentinel uses [Apache Kafka](https://kafka.apache.org/) as a central event bus to decouple data collection from downstream enrichment, correlation, and agent execution.

### Data Flow

The data journey through Sentinel follows a staged pipeline:

1.  **Collection**: A suite of `collector-*` services ingests raw data from external sources (e.g., APIs, public feeds). Each collector is specialized for a specific domain.
2.  **Publication**: Once collected, data is standardized into a common event format and published to a "raw-events" topic in Kafka.
3.  **Correlation**: The `correlation` service consumes raw events, applies a set of configurable rules to identify relationships between them (e.g., a news event occurring near a maritime vessel's location), and publishes "correlated-events" back to Kafka.
4.  **Enrichment**: The `enrichment` service consumes correlated events. It augments the data with valuable context, resolving entities, scoring anomalies via localized Isolation Forests, and detecting gaps in data streams. High-throughput feeds (TradFi, Maritime, Cyber) utilize `enrich_batch` pipelines to process massive event arrays concurrently using Python's `asyncio.gather`. The enriched events are then published to a final "enriched-events" topic.
5.  **Reasoning & Agency**: A decentralized network of services and agents performs higher-level analysis.
    -   The `reasoning` service acts as the orchestrator. It feeds enriched data, historical patterns, and graph context into a local Llama3 model via Ollama to synthesize tactical scenarios.
    -   The `agents` are specialized, autonomous Python services that react to specific event types. They perform automated research, discover connections, and dynamically update watchlists (e.g., the `quant_researcher` agent investigates market anomalies and adds correlated stocks to the tracked entities list).
6.  **Storage & Visualization**: Downstream services consume enriched events and persist them in TimescaleDB (PostgreSQL) and Neo4j.

### Architecture Diagram

```
┌────────────┐   ┌─────────┐   ┌─────────────┐   ┌────────────┐   ┌─────────────┐   ┌──────────┐
│ Collectors │──►│  Kafka  │──►│ Correlation │──►│   Kafka    │──►│ Enrichment  │──►│  Kafka   │
└────────────┘   └─────────┘   └─────────────┘   └────────────┘   └─────────────┘   └──────────┘
                                                                                         │
                                                                                         ▼
                                                                        ┌──────────────────────────┐
                                                                        │ Reasoning & Agent Services │
                                                                        └──────────────────────────┘
                                                                                         │
                                                                                         ▼
                                                              ┌────────────────┬───┬──────────┐
                                                              │ Database       │   │ Frontend │
                                                              │ (SQL & Graph)  │◄──►│ & API    │
                                                              └────────────────┘   └──────────┘
```

### Key Components

*   **Microservices**: Each service is a self-contained Python application, containerized using Docker. This allows for independent development, deployment, and scaling.
*   **Kafka**: Acts as the central nervous system of the platform, decoupling producers of data from consumers. This enables fault tolerance and allows new services to be added without disrupting the existing flow.
*   **TimescaleDB (PostgreSQL)**: Serves as the primary timeseries database for fast, concurrent event ingestion, ML anomaly bounding, and context injection queries.
*   **Redis**: Used for high-speed state management, caching dynamic pub/sub correlation rules, deduplicating live feeds, and pipelined batch operations.
*   **Neo4j**: Graph database used for mapping ontological entity relationships and running pathfinding queries during adversarial wargaming.
*   **Shared Libraries**: The `shared/` directory contains common code used across services, including database models (`models/`), Kafka utilities (`kafka/`), and business logic (`utils/`). This promotes code reuse and consistency.
*   **Containerization**: The entire platform is designed to be run using Docker and `docker-compose`, which simplifies setup and ensures a consistent environment for development and production. For production deployments, the Kubernetes configurations in the `infrastructure/` directory can be used.

### Advanced Capabilities

*   **Geofencing & Spatial Intelligence**: The `regions.geojson` file maps critical geopolitical chokepoints (e.g., Strait of Hormuz, Malacca). The `shared/utils/regions.py` utility uses `shapely` to perform point-in-polygon checks against incoming maritime and aviation telemetry, dynamically tagging events that breach these high-risk zones.
*   **Machine Learning (Isolation Forest)**: Real-time high-throughput feeds (like Equities or Crypto block trades) bypass simple static thresholds. Instead, they are evaluated against an online `IsolationForest` (see `anomaly_scorer.py`). This detects multiscale, multi-timeframe structural anomalies (e.g., 1m, 5m, 1h window spikes) in live volume or volatility.
*   **OFAC Sanctions Synchronization**: The `enrichment` layer automatically synchronizes with the US Treasury OFAC Sanctions list, performing Levenshtein distance checks against maritime vessel owners, crypto wallets, and tradfi corporate entities.
*   **Entity-Aware Context Injection**: To prevent LLM context-window pollution, Sentinel uses a surgical `fetch_entity_context()` method. When an agent needs to evaluate an anomaly, it hits TimescaleDB with a parameterized query to fetch *only* the news and anomalies that co-occurred specifically with that entity over the last 24 hours.

## Agent Architecture

Python agents are entirely `asyncio` native, executing complex tasks (like database queries or LLM calls) without blocking the main event loops.

### Core Principles & Best Practices

*   **Asynchronous Operations**: Agents are built entirely on Python's `asyncio` framework.
    *   This allows an agent to handle multiple tasks concurrently, such as fetching news from a database, querying a graph, and calling an external API all at the same time. This dramatically reduces idle time and increases throughput.
    *   I/O-bound operations that don't natively support `asyncio` (like some database drivers or Redis calls) are safely offloaded to a background thread pool using `asyncio.to_thread` or `loop.run_in_executor`. This prevents a single slow operation from freezing the entire agent.

*   **Structured & Validated AI Output**: Agents use `Pydantic` to define strict data schemas for the expected output from Large Language Models (LLMs).
    *   LLMs can sometimes produce unpredictable or malformed JSON. By parsing the LLM's response into a Pydantic model, we guarantee that the data structure is 100% correct before it's processed or sent downstream. This eliminates a major source of potential bugs and data corruption.
    *   A `PeerDiscovery` schema in the `quant_researcher` defines the exact fields and types (e.g., `ticker: str`, `discovery_confidence: float`). If the LLM's output doesn't match this schema, Pydantic raises an error that can be caught and handled gracefully.

*   **Deterministic State Management**: Agents use Redis for fast, temporary state management, such as deduplicating events or tracking watchlists.
    *   To prevent processing the same event multiple times or creating fragmented state, Redis keys are generated by a single, deterministic function. This ensures consistency.
    *   The `_state_key()` method normalizes inputs (e.g., `str.lower()`, `str.strip()`) to create a standard key format (e.g., `sentinel:quant:volume:AAPL`). State is often set with an expiration time (`EX`) to ensure Redis doesn't fill up with stale data.

*   **Idempotent Database Writes**: When writing data to the graph database (Neo4j), agents use `MERGE` statements.
    *   `MERGE` ensures that a node or relationship is created *only if it doesn't already exist*. This makes the data writing process idempotent - it can be run multiple times with the same input without creating duplicate data or causing errors. This is critical for a resilient, distributed system where an event might be processed more than once.

*   **Clean Code & Safety**:
    *   **Guard Clauses**: Agents use "early returns" at the beginning of handlers to check for invalid conditions. This keeps the main logic clean, reduces nested `if` statements, and saves compute resources by exiting before performing expensive work.
    *   **Configuration-driven**: Key thresholds and parameters (e.g., `RESEARCH_TRIGGER_SCORE`) are defined as constants at the top of the file, making them easy to find and adjust.

## Services

The platform is composed of the following services:

*   **Collectors**:
    *   `collector-adsb`: Collects live aviation telemetry data (ADS-B).
    *   `collector-ais`: Collects real-time maritime vessel tracking data (AIS).
    *   `collector-crypto`: Monitors cryptocurrency networks and tracks key wallet activities.
    *   `collector-cyber`: Gathers cybersecurity threat intelligence and vulnerability data.
    *   `collector-news`: Concurrently scrapes and deduplicates global news articles from RSS feeds.
    *   `collector-prediction`: Ingests data from prediction markets to gauge event sentiment and probabilities.
    *   `collector-radar`: A quantitative engine that dynamically scans the US Equities market for volume and volatility anomalies.
    *   `collector-tradfi`: Collects traditional financial market data via Finnhub Websockets & SEC EDGAR, actively parsing out live up/down-tick block trades.
*   **Correlation**: Correlates events from different sources based on a set of rules.
*   **Enrichment**: Enriches the collected data with anomaly scores, ML threshold bounds, and live OFAC sanctions list synchronizations.
*   **Reasoning**: Synthesizes scenarios from correlated data via local Llama3. Executes Adversarial Wargaming simulations and features an autonomous feedback loop that adds extracted entities (e.g., cashtags, crypto wallets) back to collector watchlists in Redis.
*   **Agents (Local AI)**: Decentralized multi-agent framework powered by Ollama:
    *   **Adversarial Wargamer**: Simulates game-theory attack scenarios against the Neo4j context subgraph to isolate vulnerabilities.
    *   **Ontology Master**: Manages the dynamic Neo4j knowledge graph, expands taxonomies, and routes new entities.
    *   **News Intel**: Analyzes raw news for structured intelligence briefs.
    *   **Quant Researcher**: Investigates market volume anomalies across TradFi and Crypto.
    *   **Macro Cointegration Engine**: Tracks structural macroeconomic regime drifts using online Engle-Granger statistical tracking.
    *   **Macro Strategist**: Synthesizes cointegration data and macro indicators.
    *   **Rule Agent**: Processes static correlation rules.
    *   **Supervisor**: Centralized synchronization node managing batch writes to Neo4j to prevent race conditions.
*   **API**: Provides a RESTful API for interacting with the platform.
*   **Alert Manager**: Manages alerts generated by the system.
*   **DLQ Worker**: Processes messages from the Dead Letter Queue for auditing and retries.
*   **Telemetry Worker**: Captures and persists live LLM performance metrics (prompt lengths, latency, outcome) into TimescaleDB for automated cost and capacity analytics.
*   **Frontend**: A Next.js web-based user interface for the platform.

## Configuration

The `sentinel_config.yaml` file provides a centralized location for static, human-readable configuration used across multiple services. This includes lists of:

*   Geopolitical maritime chokepoints.
*   Financial instruments and ETFs to monitor for geopolitical or macroeconomic reasons.
*   High-value organizations to track for cybersecurity events.

Using a central YAML file makes it easy to update these watchlists without modifying Python code.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following tools installed:

*   [Docker](https://www.docker.com/get-started)
*   [Docker Compose](https://docs.docker.com/compose/install/)
*   [Python](https://www.python.org/downloads/) (version 3.x recommended)
*   [Node.js](https://nodejs.org/) (for the frontend application)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd sentinel
    ```

2.  **Install Python dependencies:**
    The project's Python dependencies are listed in `requirements-base.txt` and `requirements-ml.txt`.
    ```bash
    pip install -r requirements-base.txt
    pip install -r requirements-ml.txt
    ```

3.  **Download SpaCy model:**
    After installing the Python dependencies, you need to download the English language model for SpaCy.
    ```bash
    python -m spacy download en_core_web_sm
    ```

### Running the Application

The Sentinel platform is fully containerized for a seamless developer experience. The `docker-compose.yml` file is configured to spin up all necessary infrastructure (Kafka, Zookeeper, Redis, PostgreSQL, Neo4j) alongside the backend microservices.

To start the entire backend and infrastructure stack, run:

```bash
docker-compose up --build -d
```

To monitor the health and view real-time logs of the services:

```bash
docker-compose logs -f
```

To gracefully stop the platform:

```bash
docker-compose down
```

### Starting the Frontend

The frontend is a Next.js application located in the `frontend` directory. It runs independently of the backend docker-compose stack.

```bash
cd frontend
npm install
npm run dev
```

### Accessing the Services

*   **Kafka UI:** http://localhost:8080
*   **Neo4j Browser:** http://localhost:7474
*   **Backend API Gateway:** http://localhost:8000
*   **Frontend Web UI:** http://localhost:3000

## Deployment

The application can be deployed to a Kubernetes cluster using the configuration files in the `infrastructure/k8s` directory.

**Note:** This section is a placeholder. Detailed deployment instructions will be added soon.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
