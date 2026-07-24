# Sentinel

Sentinel is a multi-domain, real-time event-driven intelligence engine. It ingests, normalizes, and correlates streaming telemetry across aviation, maritime, finance, macroeconomics, cyber, prediction markets, and news to generate automated tactical intelligence using local LLM agent swarms (Ollama).

---

## Core Stack & Services

- **Services (`services/`)**: `collector-*` (8 domain streaming ingestion), `enrichment` (Isolation Forest anomaly scoring & OFAC sanctions), `correlation` (Qdrant vector embeddings & Redis rules), `reasoning` (Ollama scenario synthesis), `agents` (autonomous quant/rates/OSINT agents), `alert_manager`, `api_gateway`, `dlq-worker`, `telemetry-worker`.
- **Shared (`shared/`)**: Unified database clients (`TimescaleDB`, `Redis`, `Neo4j`), Kafka wrappers, and resilient `OllamaClient`.
- **Frontend (`frontend/`)**: Next.js real-time spatial radar & financial intelligence dashboard.

---

## Performance Engine

| Component | Optimization | Impact |
| :--- | :--- | :--- |
| **LLM Inference** | Flash Attention + Q8_0 KV Cache + Permanent Keep-Alive | **2.5x token speed** |
| **Response Cache** | SHA-256 Redis prompt cache (`sentinel:llm_cache:<hash>`) | **<1ms instant cache hit** |
| **Prompt Engineering** | Compact JSON (`separators=(',', ':')`) & lean prompts | **35% fewer tokens (<0.5s prefill)** |
| **Redis Access** | Batched `mget()` pipeline queries for multi-ticker quotes | **N to 1 network round-trips** |
| **Logging** | Micro-batched Kafka log producer & throttled startup logs | **Zero I/O log blocking** |
| **Frontend Map UI** | D3 selection prototype `.interrupt()` polyfill | **Fixes SVG transform error** |
| **WebSockets** | Mounted state guards & backoff reconnects | **Clean connection lifecycle** |

---

## Quickstart

```bash
docker-compose up --build -d
python -m pytest
```

### Local Endpoints
- **API Gateway:** `http://localhost:8000` (`X-API-KEY: <your .env API_GATEWAY_KEY>`)
- **Frontend Web UI:** `http://localhost:3000`
- **Kafka UI:** `http://localhost:8080` | **Neo4j Browser:** `http://localhost:7474` | **Qdrant Dashboard:** `http://localhost:6333/dashboard`

---
**Author:** Alessio Naji | **License:** Proprietary / All Rights Reserved.
