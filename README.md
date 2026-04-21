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

## Architecture

The Sentinel platform is built on a distributed, event-driven microservices architecture. This design promotes loose coupling, scalability, and resilience. Services communicate asynchronously using [Apache Kafka](https://kafka.apache.org/) as a central message bus, ensuring that data flows reliably through the processing pipeline.

### Data Flow

The data journey through Sentinel follows a staged pipeline:

1.  **Collection**: A suite of `collector-*` services ingests raw data from external sources (e.g., APIs, public feeds). Each collector is specialized for a specific domain.
2.  **Publication**: Once collected, data is standardized into a common event format and published to a "raw-events" topic in Kafka.
3.  **Correlation**: The `correlation` service consumes raw events, applies a set of configurable rules to identify relationships between them (e.g., a news event occurring near a maritime vessel's location), and publishes "correlated-events" back to Kafka.
4.  **Enrichment**: The `enrichment` service consumes correlated events. It augments the data with valuable context, such as resolving entities, scoring anomalies, and detecting gaps in data streams. The enriched events are then published to a final "enriched-events" topic.
5.  **Reasoning**: The `reasoning` service acts as the Enterprise Reasoning Orchestrator. It feeds enriched data, historical patterns, and graph context into Gemini 2.5 Pro to synthesize tactical scenarios. It also "closes the loop" by autonomously pushing new tracking recommendations (e.g., stock tickers, crypto wallets) back to the Redis watchlists, steering the Collectors dynamically.
6.  **Storage & Visualization**: Downstream services, including a `db_writer` and `graph_writer`, consume enriched events and persist them in a database (e.g., PostgreSQL) and a graph database, making the data available for querying and analysis via the **API** and **Frontend**.

### Architecture Diagram

```
┌────────────┐   ┌─────────┐   ┌─────────────┐   ┌────────────┐   ┌─────────────┐   ┌──────────┐
│ Collectors │──►│  Kafka  │──►│ Correlation │──►│   Kafka    │──►│ Enrichment  │──►│  Kafka   │
└────────────┘   └─────────┘   └─────────────┘   └────────────┘   └─────────────┘   └──────────┘
                                                                                         │
                                                                                         ▼
                                                                                 ┌───────────┐
                                                                                 │ Reasoning │
                                                                                 └───────────┘
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

## Services

The platform is composed of the following services:

*   **Collectors**:
    *   `collector-adsb`: Collects aviation data (ADS-B).
    *   `collector-ais`: Collects maritime data (AIS).
    *   `collector-crypto`: Collects cryptocurrency and wallet tracking data.
    *   `collector-cyber`: Collects cybersecurity-related data.
    *   `collector-news`: Collects news articles.
    *   `collector-prediction`: Collects prediction market data.
    *   `collector-tradfi`: Collects traditional financial data (stocks, bonds).
*   **Correlation**: Correlates events from different sources based on a set of rules.
*   **Enrichment**: Enriches the collected data with additional information, such as anomaly scores and entity resolution.
*   **Reasoning**: Powered by Gemini 2.5 Pro, this service performs higher-level analysis by synthesizing tactical scenarios from correlated data. It features an autonomous feedback loop that dynamically updates collector watchlists based on AI-derived recommendations.
*   **API**: Provides a RESTful API for interacting with the platform.
*   **Alert Manager**: Manages alerts generated by the system.
*   **Frontend**: A Next.js web-based user interface for the platform.

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
    The project's Python dependencies are listed in `requirements-shared.txt`.
    ```bash
    pip install -r requirements-shared.txt
    ```

3.  **Download SpaCy model:**
    After installing the Python dependencies, you need to download the English language model for SpaCy.
    ```bash
    python -m spacy download en_core_web_sm
    ```

### Running the Application

Running the Sentinel platform is a straightforward process. You can choose to run everything via Docker or run the python services locally.

**Option 1: Full Docker Deployment**

This is the easiest way to start the entire backend and infrastructure stack. The `docker-compose.yml` file is configured to spin up Kafka, Zookeeper, Redis, PostgreSQL, Neo4j, and all the backend microservices (Collectors, Correlation, Enrichment, Reasoning, API).

```bash
docker-compose up --build -d
```
You can monitor the health and logs of the services using `docker-compose logs -f`.

**Option 2: Local Python Services (Development)**

If you are developing the Python microservices, you can start only the infrastructure in Docker, and run the Python apps using `honcho`.

1. Start infrastructure only (Kafka, DBs, Redis):
```bash
docker-compose up -d zookeeper kafka kafka-ui timescaledb neo4j redis
```

2. Start the application services via `Procfile`:
```bash
honcho start
```
Alternatively, you can run a specific service. For example, to run only the AIS collector:
```bash
honcho start ais
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