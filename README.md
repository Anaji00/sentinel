# Sentinel

Sentinel is a comprehensive, multi-source intelligence and analysis platform designed to provide situational awareness and decision support. By ingesting, processing, and correlating data from a wide variety of domains—including aviation, maritime, finance, and cyber—Sentinel empowers analysts to uncover hidden connections, identify anomalies, and understand complex, evolving situations.

The platform is built for scalability and extensibility, allowing new data sources and analytical capabilities to be integrated with ease. Its primary goal is to transform raw data into actionable intelligence, presenting it through an intuitive interface that facilitates exploration and investigation.

## Architecture

The Sentinel platform is built on a distributed, event-driven microservices architecture. This design promotes loose coupling, scalability, and resilience. Services communicate asynchronously using [Apache Kafka](https://kafka.apache.org/) as a central message bus, ensuring that data flows reliably through the processing pipeline.

### Data Flow

The data journey through Sentinel follows a staged pipeline:

1.  **Collection**: A suite of `collector-*` services ingests raw data from external sources (e.g., APIs, public feeds). Each collector is specialized for a specific domain.
2.  **Publication**: Once collected, data is standardized into a common event format and published to a "raw-events" topic in Kafka.
3.  **Correlation**: The `correlation` service consumes raw events, applies a set of configurable rules to identify relationships between them (e.g., a news event occurring near a maritime vessel's location), and publishes "correlated-events" back to Kafka.
4.  **Enrichment**: The `enrichment` service consumes correlated events. It augments the data with valuable context, such as resolving entities, scoring anomalies, and detecting gaps in data streams. The enriched events are then published to a final "enriched-events" topic.
5.  **Storage & Visualization**: Downstream services, including a `db_writer` and `graph_writer`, consume enriched events and persist them in a database (e.g., PostgreSQL) and a graph database, making the data available for querying and analysis via the **API** and **Frontend**.

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
    *   `collector-cyber`: Collects cybersecurity-related data.
    *   `collector-financial`: Collects financial data.
    *   `collector-news`: Collects news articles.
*   **Correlation**: Correlates events from different sources based on a set of rules.
*   **Enrichment**: Enriches the collected data with additional information, such as anomaly scores and entity resolution.
*   **Reasoning**: Performs higher-level analysis on the data.
*   **API**: Provides a RESTful API for interacting with the platform.
*   **Alert Manager**: Manages alerts generated by the system.
*   **Frontend**: A web-based user interface for the platform.

## Getting Started

To get started with Sentinel, you will need to have Docker and `docker-compose` installed.

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    ```
2.  Navigate to the project directory:
    ```bash
    cd sentinel
    ```
3.  Start the services:
    ```bash
    docker-compose up
    ```

This will start all the services in the platform. You can then access the frontend at `http://localhost:3000`.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
