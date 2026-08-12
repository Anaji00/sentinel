# Sentinel Radar Dashboard (Frontend)

The **Sentinel Frontend** is an enterprise, high-throughput tactical radar interface built with **Next.js 14**, **React**, **Deck.gl**, **Recharts**, and **TailwindCSS**. It renders real-time streaming telemetry, 3D geographic threat visualizations, quantitative financial risk models, AI agent swarm telemetry, and interactive graph exploration.

---

## 🚀 Getting Started

### 1. Installation & Local Development

Run the development server locally:

```bash
npm install
npm run dev
```

Open **`http://localhost:3000`** in your browser.

---

## 🎨 Component & Dashboard Architecture

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

---

## 🛠️ Environment Configuration

| Environment Variable | Description | Default |
| :--- | :--- | :--- |
| `NEXT_PUBLIC_API_URL` | Fast API Gateway REST URL | `http://localhost:8000` |
| `API_GATEWAY_KEY` | Development API Key | `sentinel-dev-key-2026` |
