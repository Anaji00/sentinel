# Sentinel

Sentinel is a comprehensive, multi-source intelligence and analysis platform designed to provide situational awareness and decision support. By ingesting, processing, and correlating data from a wide variety of domains—including aviation, maritime, finance, and cyber—Sentinel empowers analysts to uncover hidden connections, identify anomalies, and understand complex, evolving situations.

The platform is built for scalability and extensibility, allowing new data sources and analytical capabilities to be integrated with ease. Its primary goal is to transform raw data into actionable intelligence, presenting it through an intuitive interface that facilitates exploration and investigation.

## Code Structure

The project is organized into the following directories:

-   `services/`: Contains the individual microservices that make up the platform.
-   `shared/`: Holds common code, such as data models and utility functions, shared across the microservices.
-   `frontend/`: Contains the user interface of the platform.
-   `infrastructure/`: Includes infrastructure-as-code configurations, such as Kubernetes deployment files.
-   `docs/`: Contains additional documentation for the project.
-   `sentinel_config.yaml`: A central file for static configuration, such as lists of financial instruments or keywords for monitoring.

## Architecture

The Sentinel platform is built on a distributed, event-driven microservices architecture. This design promotes loose coupling, scalability, and resilience. Services communicate asynchronously using [Apache Kafka](https://kafka.apache.org/) as a central message bus, ensuring that data flows reliably through the processing pipeline.

### Data Flow

The data journey through Sentinel follows a staged pipeline:

1.  **Collection**: A suite of `collector-*` services ingests raw data from external sources (e.g., APIs, public feeds). Each collector is specialized for a specific domain.
2.  **Publication**: Once collected, data is standardized into a common event format and published to a "raw-events" topic in Kafka.
3.  **Correlation**: The `correlation` service consumes raw events, applies a set of configurable rules to identify relationships between them (e.g., a news event occurring near a maritime vessel's location), and publishes "correlated-events" back to Kafka.
4.  **Enrichment**: The `enrichment` service consumes correlated events. It augments the data with valuable context, such as resolving entities, scoring anomalies, and detecting gaps in data streams. The enriched events are then published to a final "enriched-events" topic.
5.  **Reasoning & Agency**: A decentralized network of services and agents performs higher-level analysis.
    -   The `reasoning` service acts as the Enterprise Reasoning Orchestrator. It feeds enriched data, historical patterns, and graph context into a local Llama3 model via Ollama to synthesize tactical scenarios.
    -   The `agents` are specialized, autonomous Python services that react to specific event types. They use local AI to perform deep research, discover new connections, and dynamically steer the platform's focus. For example, the `quant_researcher` agent investigates market anomalies and automatically adds correlated stocks to the watchlist.
6.  **Storage & Visualization**: Downstream services, including a `db_writer` and `graph_writer`, consume enriched events and persist them in a database (e.g., PostgreSQL) and a graph database, making the data available for querying and analysis via the **API** and **Frontend**.

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
*   **Shared Libraries**: The `shared/` directory contains common code used across services, including database models (`models/`), Kafka utilities (`kafka/`), and business logic (`utils/`). This promotes code reuse and consistency.
*   **Containerization**: The entire platform is designed to be run using Docker and `docker-compose`, which simplifies setup and ensures a consistent environment for development and production. For production deployments, the Kubernetes configurations in the `infrastructure/` directory can be used.

## Agent Architecture

The Python agents (e.g., `quant_researcher`) are built on a modern, high-performance architecture designed for asynchronous I/O and robust data processing. This allows them to perform complex, long-running tasks—like querying multiple databases and calling LLMs—without blocking and while maintaining data integrity.

### Core Principles & Best Practices

*   **Asynchronous Operations**: Agents are built entirely on Python's `asyncio` framework.
    *   **Why it matters**: This allows an agent to handle multiple tasks concurrently, such as fetching news from a database, querying a graph, and calling an external API all at the same time. This dramatically reduces idle time and increases throughput.
    *   **Implementation**: I/O-bound operations that don't natively support `asyncio` (like some database drivers or Redis calls) are safely offloaded to a background thread pool using `asyncio.to_thread` or `loop.run_in_executor`. This prevents a single slow operation from freezing the entire agent.

*   **Structured & Validated AI Output**: Agents use `Pydantic` to define strict data schemas for the expected output from Large Language Models (LLMs).
    *   **Why it matters**: LLMs can sometimes produce unpredictable or malformed JSON. By parsing the LLM's response into a Pydantic model, we guarantee that the data structure is 100% correct before it's processed or sent downstream. This eliminates a major source of potential bugs and data corruption.
    *   **Implementation**: A `PeerDiscovery` schema in the `quant_researcher` defines the exact fields and types (e.g., `ticker: str`, `discovery_confidence: float`). If the LLM's output doesn't match this schema, Pydantic raises an error that can be caught and handled gracefully.

*   **Deterministic State Management**: Agents use Redis for fast, temporary state management, such as deduplicating events or tracking watchlists.
    *   **Why it matters**: To prevent processing the same event multiple times or creating fragmented state, Redis keys are generated by a single, deterministic function. This ensures consistency.
    *   **Implementation**: The `_state_key()` method normalizes inputs (e.g., `str.lower()`, `str.strip()`) to create a standard key format (e.g., `sentinel:quant:volume:AAPL`). State is often set with an expiration time (`EX`) to ensure Redis doesn't fill up with stale data.

*   **Idempotent Database Writes**: When writing data to the graph database (Neo4j), agents use `MERGE` statements.
    *   **Why it matters**: `MERGE` ensures that a node or relationship is created *only if it doesn't already exist*. This makes the data writing process idempotent—it can be run multiple times with the same input without creating duplicate data or causing errors. This is critical for a resilient, distributed system where an event might be processed more than once.

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
    *   `collector-tradfi`: Collects traditional financial market data, including stocks and bonds.
*   **Correlation**: Correlates events from different sources based on a set of rules.
*   **Enrichment**: Enriches the collected data with additional information, such as anomaly scores and entity resolution.
*   **Reasoning (Enterprise)**: Powered by local Llama3 via Ollama, this service performs higher-level analysis by synthesizing tactical scenarios from correlated data. It features an autonomous feedback loop that connects to the Neo4j graph and dynamically updates collector watchlists (like prediction markets and news keywords) based on AI-derived recommendations to close the loop on intelligence gathering.
*   **Agents (Local AI)**: A decentralized multi-agent framework powered by a local **Ollama** LLM (Llama3). These specialized agents perform deep, autonomous research in response to specific triggers:
    *   **Ontology Master**: Manages the dynamic Neo4j knowledge graph, expands taxonomy, and routes new entities to collector watchlists.
    *   **News Intel**: Analyzes raw news to extract structured intelligence briefs and discovers hidden entity relationships for the graph.
    *   **Quant Researcher**: Investigates market volume anomalies across TradFi and Crypto, discovers correlated peers, and auto-injects them into watchlists.
    *   **Macro Cointegration Engine**: Tracks structural macroeconomic regime drifts and commodity-to-equity decoupling using online Engle-Granger statistical tracking.
    *   **Macro Strategist**: Synthesizes cointegration data and macro indicators to publish high-level tactical macro scenarios.
    *   **Rule Agent**: Processes static correlation rules and handles immediate deterministic alerting.
    *   **Supervisor**: Acts as a centralized synchronization node, safely managing batch writes to the Neo4j graph database to prevent race conditions.
*   **API**: Provides a RESTful API for interacting with the platform.
*   **Alert Manager**: Manages alerts generated by the system.
*   **DLQ Worker**: Processes messages from the Dead Letter Queue for auditing and retries.
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
*   **Neo4j Browser:** http://localhost:7474 (credentials: `neo4j` / `sentinel_graph`)
*   **Backend API Gateway:** http://localhost:8000
*   **Frontend Web UI:** http://localhost:3000

## Deployment

The application can be deployed to a Kubernetes cluster using the configuration files in the `infrastructure/k8s` directory.

**Note:** This section is a placeholder. Detailed deployment instructions will be added soon.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
