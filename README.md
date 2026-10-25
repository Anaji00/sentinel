# Sentinel Intelligence Platform

**Sentinel** is an enterprise-grade, multi-domain real-time event correlation, quantitative risk analysis, and threat intelligence engine. It ingests, normalizes, and correlates high-throughput streaming telemetry across maritime AIS, financial markets & options flow, macroeconomic indicators & economic calendar releases, crypto perpetuals, prediction markets, aviation ADSB, and OSINT news to synthesize automated tactical intelligence using local LLM agent swarms (Ollama) and deterministic quantitative risk models.

> **New here?** [GUIDE.md](GUIDE.md) explains in plain English how to run Sentinel, what each page is for, and how the stock calls are made.
> **Model trials:** [MODEL_TRIALS.md](MODEL_TRIALS.md) records every change tested against the stock model, the rule it had to clear, and the result.
> **Data:** [DATA_PIPELINE.md](DATA_PIPELINE.md) documents where every stock and options input comes from, how the model reads it, and the latest accuracy checks.

---

## ⚡ System Architecture

```
                                  [ DOMAIN TELEMETRY SOURCES ]
  (Maritime AIS | TradFi Equities & Options | Macro Calendar & SOFR | Crypto Swaps | Prediction Markets | Aviation ADSB* | OSINT)
                     * emergency squawks and five watch zones.  Cyber BGP is retired: `make cyber`.
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
        │ - Ollama (qwen2.5:3b, one model resident)        - 1-Hop Graph Context Checks    │
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

Sentinel is composed of **35 modular services & containers** orchestrated via Docker Compose and communicating over Kafka and Redis:

### 1. Ingestion Collectors (`services/collector-*`)
- **`collector-tradfi`**: Streaming US equity quotes, order book depth, dark pool prints, and options flow with OCC standardized contract parsing (`AAPL240816C00220000`).
- **`collector-filings`**: Real-time **SEC EDGAR Corporate Filings** (Form 8-K material corporate disclosures, S-1/424B offerings, 10-K/10-Q periodic financials) with direct inline document URLs, and **13F Institutional Holdings** with live SEC EDGAR XML InfoTable parsing. Also: five years of 8-K history, SIC industry profiles, **FINRA short interest**, and **SEC company facts** — every financial line item a company filed, with its filing date (`company_facts.py`, append-only, restatements kept). Tickers resolve ticker → issuer from the SEC registry, common stock first, share classes included (`issuer_ticker`). Every bulk SEC loop shares one pace under SEC's fair-access limit (`shared/utils/sec_rate.py`).
- **`collector-macro`**: Live Federal Reserve SOFR risk-free rate polling, 12 macro proxy instruments (Crude, Brent, NatGas, Gold, Silver, Yields, Indexes), **Economic Calendar Collector** ingesting scheduled releases (CPI, Core CPI, NFP, Unemployment, FOMC Rate Decisions, GDP, ISM Manufacturing PMI, PCE), **Supply Chain & Freight Index Poller** (Baltic Dry Index BDI, Container Freight Rates FBX/Harpex), and **Federal Register Regulatory Policy Tracker** (export controls, semiconductor policy, antitrust, Section 301 tariffs).
- **`collector-social`**: Primary-source real-time OSINT & social signal ingestion from curated **Telegram public channels** (geopolitical/maritime), **Reddit subreddits** (financial/geopolitical), and breaking **X/Twitter** squawk handles, tagging events with `source_type: "primary_social"` and epistemic reliability weights.
- **`collector-ais`**: Real-time WebSocket tracking global maritime vessel positions, MMSI identifiers, navigation status, SOG, and coordinates across coastal chokepoints.
- **`collector-adsb`**: Aircraft state vector ingestion tracking military, commercial, and emergency squawk flights via open academic sensor networks.
- **`collector-crypto`**: High-frequency perpetual swaps, Coinbase spot tape, **Multi-Chain On-Chain Whale Tracking** across Ethereum, Arbitrum, and Base (USDT, USDC, DAI, WBTC, WETH, ARB, cbBTC), and **Cross-Exchange Basis/Funding Divergence Engine** (Binance vs Bybit vs Kraken).
- **`collector-cyber`** (retired): BGP routing anomalies and CVE telemetry. Off by default (`cyber` profile only) and removed from the interface entirely; the stored history and its read path are kept.
- **`collector-prediction`**: Real-time contract odds and liquidity streams from Polymarket, and Kalshi's economic series (Fed, CPI, PCE, jobless claims, payrolls, unemployment, GDP, recession, index levels, WTI) — the market's implied consensus for the releases equities trade around.
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
  - **One model, both tiers (`qwen2.5:3b`)**: a single model stays resident in Ollama (`OLLAMA_MAX_LOADED_MODELS=1`); `agents-heavy` and `agents-fast` differ in what they are asked, not which model answers. Reasoning writes scenarios only for INTELLIGENCE-tier clusters and above, and not for correlation rules that grading against prices has shown carry no information (see the Stock Research Pipeline).

### 5. API Gateway & Tactical Radar Dashboard (`frontend` & `services/api_gateway`)
- **`api-gateway`**: High-performance FastAPI gateway with **cryptographic HMAC-SHA256 & JWT session-cookie verification**, rate limiting, Prometheus metrics, multi-hop graph queries (`/api/v1/graph/entity/{id}?hops=1|2`), strategy backtesting (`/api/v1/backtest/run`), model explainability audit trails (`/api/v1/explain/event/{id}`), and feature flag kill switches (`/api/v1/flags`).
- **`frontend` (Sentinel Radar Dashboard)**: Next.js 16 interface with React 19, Deck.gl, Recharts, and TailwindCSS across 11 dedicated operational surfaces:
  - **`/` (Home)**: Opens on *Today's top calls*: the strongest sized buys and shorts as cards, taken from each side in turn, under a one-line market pulse (S&P 500 today, regime, momentum sizing), then **Who's watching you** at equal weight: plate-reader detections in 30 days, cameras mapped, agencies in 287(g) immigration agreements, federal contracts to surveillance vendors with a who-profits ranking, this week's civic news, and a town search that opens `/watch?town=…`. Then the model portfolio and the strongest evidence. A first-visit welcome asks what the person came for (their town, today's calls, or both) and home reorders around the answer; **Your stocks** and **Your town** cards follow the stocks and police department they chose, saved per account (`/api/v1/me/preferences`, migration 0067). The live operator console is folded away below.
  - **`/predictions` (Stock Calls)**: Today's calls first, by horizon (daily, weekly, monthly, event-led), in Buys and Sells columns with an independent-bets gauge each. Every card shows the live price and today's move, the expected move against the S&P 500 in large type, a quarter's sparkline, right-way odds on a bar centred on a coin flip, size / round-trip cost / target, the options trade in one line (win %, cost, max loss, EV), earnings and validation badges, and "Why this call" for the full reasoning. The track record follows.
  - **`/ticker` (Look Up a Stock)**: A hero (price, 90-session chart from the model's own daily history at `/api/v1/predictions/closes/{ticker}`, the model's rank in words, the evidence verdict, today's call, and a Follow button), then four tabs: What's happening, Signals & model (the model's view with what stands out about the stock in words, from `signal_plain.stand_out`), Who owns & trades it, and Options. For a listed surveillance company (PLTR, AXON, MSI, CLBT, SSTI, REKR, RELX, LHX) a **Surveillance business** card shows what it sells to police, its federal contracts, devices mapped, lobbying and biggest federal customers; the same companies carry ticker links wherever the civic pages name them (`lib/surveillanceCompanies.ts`).
  - **`/board` (Signal Board)**: Every anomalous ticker of the last three days. It opens on the four strongest bullish and bearish leans among tradeable names (over $5, under 0.5% to trade $1M), then a list filterable by lean. Verdicts are coloured by direction (green up, red down) and filled by confidence, and each row's counted votes show as a bar from proven to research-backed. Click a row for every signal in plain English, tiered by evidence (Proven here / Model-backed / Research-backed / Tested: no edge / Background).
  - **`/signals` (Trade Signals)**: Proven event signals, live. Signals sharing a reason, horizon and direction are grouped into one card with their stocks, the likelihood, and the expected move against its cost; the track record, the converging evidence, and the reaction study filterable by verdict follow.
  - **`/book` (Portfolio & Record)**: The model portfolio against the S&P 500, the portfolio lab, model health, the track record, the paper ledger and every producer's grade, then the paper account. The navigation groups pages as Overview, Who's Watching (second, in the civic colour), Stocks (calls, board, signals, look-up), Track Record (portfolio, backtests, what drives signals), Markets, Intelligence and Reference. Wording across the stock pages is plain English ("Chance it goes our way", "Most to put in", "Bet against"); statistics such as Sharpe ratios and q-values sit in tooltips, and the signal board's everyday names come from `shared/utils/signal_plain.everyday`.
  - **`/graph` (Knowledge Graph & Empirical Correlation Explorer)**: 1-hop and 2-hop subgraph explorer with **epistemic edge differentiation** (dashed animated lines for empirical statistical edges vs solid neon lines for structural/index edges vs solid red for sanctions) and an **Edge Epistemic Inspector Drawer** detailing raw vs decayed weight, $r$, $p$, lag, $F$-stat, and Hawkes $\eta$.
  - **Model Cards & Explainability Modal**: One-click factor attribution waterfall, step-by-step score derivation timeline, data provenance, and drift stability inspector embedded across conviction plays and alert feeds.
  - **Signal Governance & Kill Switch Panel**: Real-time feature flag toggles, rollout percentage sliders, and emergency signal kill switches.
  - **`/intelligence` (News & Intel)**: Two views of one page: the multi-domain live event feed (`?view=feed`, red reserved for scores of 0.9 and up) and the open-source threat matrix (`?view=osint`; the old `/osint` redirects here).
  - **`/agents` (AI Analysts)**: The analysts' roster, what they have concluded, and their bulletins in plain words.
  - **`/options` (Options & Flow)**: Big-money block trades and options sweeps (default view; the old `/flow` redirects here), the unusual-volume radar (`?view=radar`), and index flow (`?view=indices`).
  - **`/crypto`**: Crypto perpetuals, funding rates, and volume spikes.
  - **`/charts` (Markets & Economy)**: Two views: Prices (multi-asset cards, each line coloured by its own direction) and Economy (`?view=economy`: the yield curve, oil stockpiles and prediction markets; `/macro` redirects here).
  - **`/cases` (Investigations)**: Cases and Briefs as two views (`/reports` redirects to `?view=briefs`).
  - **`/strategies` (Research)**: Backtests and What drives signals as two views (`/attribution` redirects to `?view=drivers`).
  - **`/map`**: The world map of vessels, flights, chokepoints and conflict zones, inside the app shell. The civic surveillance map stays at `/watch`: different scale (US agencies, ~10,000 points) and a different renderer.

### 6. Enterprise Governance, Security & Infrastructure
- **TLS Ingress Reverse Proxy**: Nginx-based TLS termination on port 443 with HTTP→HTTPS 301 redirect, `Strict-Transport-Security` (HSTS), `X-Frame-Options`, `X-Content-Type-Options`, modern TLSv1.2/1.3 cipher suites, and automated self-signed certificate bootstrapping for local development. All internal services are isolated behind the ingress with no direct host port exposure.
- **Fail-Closed Secrets Management**: `shared/utils/secrets.py` centralizes all environment configuration with `required=True` enforcement — the gateway **refuses to start** without `SESSION_SECRET`. `resolve_env_var` permits hardcoded dev fallbacks **only** when `SENTINEL_ENV` is in the `SAFE_DEV_ENVS` whitelist (`dev`, `test`, `local`). Automated token masking in logs and health audits (`/api/v1/health/secrets`).
- **Non-Root Container Execution**: All Python containers run as `sentinel` (UID 1001), preventing container-escape privilege escalation.
- **Hardened Session Cookies**: `sentinel_session` cookie enforces `HttpOnly: true`, `Secure: true`, `SameSite: Strict`, and `Path: /`.
- **Conditional CORS**: `allow_origin_regex` for localhost is restricted to `SAFE_DEV_ENVS` environments. In production, origins are explicitly enumerated via `CORS_ALLOWED_ORIGINS` with no open regex.
- **Container Resource Limits**: CPU and memory limits enforced across all services (`deploy.resources.limits`) — from Ollama (4 CPU / 8G) down to collectors (0.5 CPU / 512M) — preventing host starvation.
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

## 📐 Quantitative & Statistical Models

The shared maths library lives in `shared/utils/quant_calc.py`. What it does, plainly:

**Options** — Black-Scholes pricing and Greeks, implied volatility solving, and covered-call
strike selection.

**Risk** — Value at Risk and Conditional VaR from the actual loss history, maximum drawdown,
and half-Kelly position sizing driven by measured win rates rather than assumed ones.

**Relationships between instruments** — Pearson correlation, Engle-Granger cointegration,
and Granger causality for lead-lag effects. Because testing many pairs at once produces
false positives by construction, results are corrected for multiple comparisons before
anything is reported.

**Market behaviour** — GARCH and EWMA volatility, a Hurst exponent for whether a series
trends or mean-reverts, and a Hawkes process for how activity in one domain excites another.

**Two notes on how these are used.** Every annualised figure is scaled to the timeframe it
was measured on, so a number from 5-minute bars is not treated as if it came from daily ones.
And statistical tests are run on stationary inputs — price levels are differenced first where
the test requires it — because regressions on raw prices find relationships that are not there.

Each of these is checked against data with a known answer: a random walk should score near
0.5 on the Hurst measure, and unrelated series should be flagged as causally linked about
5% of the time and no more.

---

## 📈 Stock Research Pipeline

What turns the data into stock ideas, and how each step is kept honest. Everything below
is measured out of sample and published as it accumulates; nothing is asserted in advance.

**Cross-sectional model** (`shared/utils/factor_model.py`, `services/agents/factor_engine.py`).
Each night four candidates (ridge, gradient boosting, a LightGBM ranker, plain momentum)
rank stocks at daily, weekly and monthly horizons on 29 features — momentum (12-1, 6-1,
overnight), reversal, volatility, beta, liquidity, 52-week high, lottery-like returns, days to
cover, earnings reaction, industry momentum and reversal, off-exchange short volume, quality
and earnings yield, and option-implied measures. They read **eleven years of daily bars**
(from January 2016, `FACTOR_LOOKBACK_YEARS`; the service has 6 GB for it). Validation is
walk-forward with purging and an embargo; the deflated Sharpe counts every candidate tried.
The walk-forward runs on the names ever tradable, as every trial does.

The monthly ranker, out of sample over 101 months and averaged over four offsets of the test
calendar (trials P1–P3, 24 October 2026): rank IC 0.037, IC t 3.34, net Sharpe 0.97, net beta
−0.06, about +1.0% a month long-short after costs. Its shorts carry it (+1.6% a month); its buys
still trail the market after their extra risk (−0.6%). A single run swings by about 0.4 in net
Sharpe with the test calendar alone, so trials are judged on several offsets. The nightly run
publishes every horizon's current figures on the model-health panel.

The rules that keep it tradable:

- **Universe**: the 1,000 most liquid common stocks by consolidated dollar volume each day,
  from data up to that day. Across every name over the floor the weekly book showed a net
  Sharpe of 4.3; inside the top thousand, with real costs, about 1 — the rest lived in
  small names whose spreads and borrow no desk gets.
- **Shorts** only where the broker marks a name easy to borrow.
- **Legs measured apart**: each side's return is reported against SPY and against the
  average eligible stock. Shorts vote when they beat SPY; buys when they beat the average
  stock — stock selection — and every buy says how it compared with the S&P 500 too. On
  9 October the buys beat the average stock by 0.31% a week (t 2.1) while the average
  stock trailed SPY by 0.30%: a few very large companies carried the index.
- **Market regime and rates** interactions (beta × trend, rate sensitivity × rate moves, momentum
  × volatility, beta × credit) were adopted on five years (trial E) and removed on eleven (trial
  P1): they held a quarter of the ranker's splits and added noise. Idiosyncratic volatility,
  residual momentum and Amihud illiquidity went too, as near-duplicates. All are still computed
  for the stock pages.
- **Event evidence can overrule the model** in either direction (`shared/utils/recommendations.py`):
  insider filings, filings and event families weigh at their evidence tier, more when they
  pour in; outweighing the model takes a full vote; strong one-sided evidence carries an
  "event-led" idea graded apart. Block trades carry a tick-test side, and both sides
  measured no edge here, so they are shown and not counted.
- **Fundamentals** (from filing-dated SEC facts, trailing twelve months, each value from
  the session after its filing) are computed and shown on the board; the model trains on
  a feature only after a pre-registered trial shows it helps. Adopted that way on
  9 October: overnight momentum and the intraday month (Lou, Polk & Skouras), quality
  against industry peers, and earnings yield — together weekly net Sharpe 0.71 → 0.86,
  monthly 0.67 → 1.13. Rejected: averaging the candidates (better IC, worse after costs).
- **Option-implied features** (trial L, adopted 18 October): the call–put IV spread
  (Cremers & Weinbaum), skew (Xing, Zhang & Zhao), put/call volume (Pan & Poteshman),
  realised minus implied volatility, and the month's change in implied volatility. They are
  rebuilt from daily option bars back to February 2024 (`shared/utils/option_iv.py`,
  `option_iv_history`): each contract's close with the stock's *unadjusted* close, inverted
  through Black–Scholes. On the 595 names with options the long–short spread rose from 14
  to 28 bps a week and from 113 to 232 bps a month; the daily horizon did not improve.
- **Daily company news tone** (trial J, rejected 18 October): FinBERT tone and article
  counts over 788,000 Benzinga articles since September 2023 left the monthly rank IC at
  0.068 and lowered the weekly one. News is studied as event families instead (below).
- **News embeddings** (trial M, adopted 18 October): the same 624,349 single-company
  headlines embedded with `all-mpnet-base-v2`, summed per stock and session, and a ridge
  walked forward by month. Against other stocks in the news it predicted the next week
  (rank IC 0.027, t 4.31 on non-overlapping weeks; top decile beat bottom by 128 bps
  before costs); the next day fell just short of its bar (IC 0.018). The correlation service
  now embeds each live headline, and weekly forecasts add the read
  (`shared/utils/news_embed.py`; the fitted model ships as `news_embed_model.json`).
  Every stock-day's embedding sum is kept (`news_embed_daily`), and the factor-model
  service refits monthly on summary statistics (`services/agents/news_model_refit.py`),
  keeping each stock-session's out-of-sample score (`news_embed_oos`) and publishing only
  a refit that still passes. As a ranking-model feature (trial N) it fell just short:
  weekly IC 0.0427 → 0.0467 against a +0.005 bar.
- **Trials O1–O8** (22–23 October, rules set in advance; [MODEL_TRIALS.md](MODEL_TRIALS.md)): price
  history back to 2016 was adopted — the monthly ranker's walk-forward grew from 44 to 101 months,
  IC t 2.00 → 3.12, net Sharpe 0.79 → 1.01, net beta 0.10. Rejected on the same bar: training on
  beta-adjusted or industry-relative returns, fundamentals and share issuance, averaging the two
  tree models, a two-sided ranker, insider and 8-K features, and commodity exposure.
- **Trials P1–P3** (24 October, each averaged over four offsets of the test calendar): the four regime
  interactions and three near-duplicate inputs (idiosyncratic volatility, residual momentum, Amihud
  illiquidity) were removed — 29 inputs, mean monthly net Sharpe 0.68 → 0.97, IC t 2.40 → 3.34, better on
  every offset. Gross profitability added nothing. The paper account now holds only the model's extremes
  (book B1: bottom tenth short, 90th–99th percentile long, equal weight).
- **Evidence tiers**: *validated* passes every gate including the deflated Sharpe;
  *supported* passes all but that one. Only these vote.

**Reaction study** (`shared/utils/reaction_study.py`). Every event family (8-K items,
insider clusters, sweeps, news families, earnings reactions…) is tested against what the
instrument did next, net of SPY, with a winsorised t-test and Benjamini-Hochberg
false-discovery control at q ≤ 0.10, against a size-matched ETF (SPY, IJH, IJR or IWC by the
stock's dollar volume) so small-cap drift does not pass for an edge. A family whose result
contradicts its stated prior is never promoted. Proven as of 18 October 2026 (17 cells):
8-K equity sales (3.02) at every horizon, −10% over 30 days; delisting notices (3.01) at
1, 5 and 30 days; auditor changes and impairments; earnings drops continuing in small caps
and large-cap earnings jumps fading; opportunistic insider sales at 30 days; and clearly
negative company news, −1.3% over five days.

Clearly *positive* company news is followed by a fall too: −0.5% the next day and −1.3%
over five (t −5.6, the held-out half agreeing), the opposite of its prior. So it is not
traded. Instead `stock_news_fade` was registered on 18 October: positive news, expected
down, judged only on articles from that date (`NEWS_FADE_REGISTERED`), so history cannot
vote for it.

Option flow is studied as bought calls and puts (prints above the quote's midpoint with
$50k+ of extrinsic premium, deep in-the-money prints set aside) and as opening trades
(confirmed by next morning's open interest), after Pan & Poteshman. On the 15-minute
delayed feed the snapshot's quote is often minutes from the print and history keeps no
option quotes, so a quote up to five minutes old classifies only prints at or through it
(`shared/utils/options_flow.py`). Measured live, the feed's option quotes are real-time
while its trades arrive 15 minutes late, so the poller keeps a 25-minute tape of each
active contract's quotes (polled every minute) and reads each print against the quote of
its own moment. These families collect from 19 October 2026.

**Signal Board** (`services/agents/dossier_engine.py`, `shared/utils/signal_matrix.py`).
For every anomalous ticker: price, model, options (skew and call–put spread ranked against
the day's cross-section), short interest, insiders, filings, news, fundamentals and trading
cost — each with one plain-English sentence and its evidence tier, then a verdict.

**Paper ledger** (`shared/utils/paper_ledger.py`). Every trade idea is traded on paper as a
desk would: filled at the next session's open, $250,000 a trade, half the spread and
square-root market impact both ways, out at its own stop or its horizon's last close, net
of SPY; open positions are marked daily, and each horizon reports closed and open trades
together, since a horizon younger than its hold can only have closed what its stops cut. Stops
widen with the hold (`shared/utils/recommendations.py` `HORIZON_STOP_K`): two daily ATRs had sat
inside a month's ordinary swing and stopped 68% of monthly ideas. Each trade also records its
result under volatility-managed sizing (Moreira & Muir), so the ledger itself tests whether
scaling helps.

**Idea cards** (`services/agents/recommendation_engine.py`). Ideas are the strongest and
weakest names *within each industry*, as the headline book ranks them, so no one sector
fills a side; the card shows both ranks (in industry and overall). Industry codes miss
supply chains (on 19 October 2026 six "different" monthly longs were one semiconductor
theme, two effective bets of six), so each idea must also stay under a 0.6 correlation of
63-day returns with every idea already kept on its side. Each idea carries its reasons
in words (the ridge model's exact per-feature contributions), a warning when results are
likely due inside the hold, the Signal Board's evidence for and against, and what $10,000
in it risks: the loss at the stop, the cost a side, and the size that risks 1% on a typical
day. Names wanted at more than one horizon are listed once, together.

**Forecasts, options and sizing** (`shared/utils/expected_returns.py`,
`shared/utils/option_ev.py`). Every idea says how far, not only which way. Its expected move
against SPY is Grinold–Kahn alpha (the model's IC × the stock's 63-day volatility × the
normal quantile of its rank across all names; the industry rank chooses the idea, the
overall rank forecasts it). That is mapped through the measured out-of-sample record of the
model that ranks the horizon: bucket means, made monotone and held flat past the most
extreme bucket. Any active proven event family is added. The tails are not symmetric: the
bottom 1% of monthly scores lost 19.6% against SPY while the top 1% gained 2.1%, and the
daily model's top names showed no edge at all, so daily ideas are shorts only.

- **Volatility**: a HAR blend of month, quarter and year, pulled halfway toward implied
  volatility less a 15% premium. The stock's own earnings move is added only to option
  expiries after the release, spread over that option's life.
- **Costs at the size taken** (`shared/utils/trading_costs.py`): the stock's own spread (its
  liquidity tier, or for names under $50M a day the Abdi–Ranaldo estimate from daily close,
  high and low when wider) plus square-root impact, σ × √(position ÷ daily dollar volume),
  in and out, plus quoted borrow on a short. A flat 0.2% flattered thin, volatile names
  and overcharged large ones.
- **Sizing**: half-Kelly with costs that grow with the position, solved jointly, up to 5%
  of a $1M reference account; each idea also states its capacity, the position at which
  costs would take the whole edge.
- **Options**: every long call or put, debit spread and credit spread 7–70 days out is
  priced under that distribution, bought at the ask and sold at the bid, with no leg past
  0.85 delta. The edge counts only up to the horizon, the market's drift after it. The pick
  has the best expected return on the capital at risk (at least 5%, with at least a 25%
  chance of profit), or none. Five- and 30-day event signals get the same search, and every
  option idea is settled at expiry on the stock's close (`recommendation_log.option_pnl`).

**Model portfolio: beating the S&P 500** (`shared/utils/model_portfolio.py`,
`services/agents/portfolio_engine.py`). An actively managed paper portfolio: SPY as the
core plus a market-neutral overlay, long the top fifth and short the bottom fifth of every
industry by the monthly model's rank (borrowable only), 30% of the portfolio a side. That
industry-neutral book has been the headline since 16 October 2026; the universe-wide book
runs beside it in shadow. Over the lab's out-of-sample months it earned more per unit of
risk (Sharpe 2.69 against 2.20 on 18 October), with industry bets explaining 0.5% of its
variance against 31%. It issues BUY / SELL / SHORT / COVER
orders, each with its reason; it rebalances every 21 sessions with the model's hold buffer;
between rebalances it sells names that leave the ranked universe and covers shorts that
can no longer be borrowed; and it closes the overlay if the monthly model loses its
evidence tier. Fills come at the next open, net of trading costs and a borrow fee, and the
portfolio is marked against SPY every session (return, tracking error, information ratio,
worst stretch behind SPY). Why this shape, from out-of-sample trials on 9 October 2026:

| Construction (2023–26, net of costs) | vs SPY | Beta | Alpha after beta |
|---|---|---|---|
| Equal-weight top 100 / 200 liquid names, no model | −6 / −16 bps a week | 1.4 / 1.2 | −22 / −25 bps |
| Model's best 20 of the top 200, weekly, hold buffer | +32 bps a week (t 1.4) | 1.7 | +1 bp |
| Model's best 50, hedged to beta one | −18 bps a week | 0.9 | −16 bps |
| Monthly long/short book (the overlay) | +185 bps a month | ≈ 0 | **+191 bps (t 2.3)** |
| ↳ its short leg | −200 bps a month | 1.3 | **−248 bps (t −3.9)** |

The picks beat random picks from the same names every time, but a book of only the buys
"beats" SPY by owning more market. The sells are the reliable part. A pre-registered trial
of training on beta-adjusted returns (trial F) was rejected: it improved the weekly
beta-one book from −18 to +8 bps a week, short of its t ≥ 1.5 rule. Thirty-two months is
thin evidence; the portfolio is the live test.

**Paper auto-trader** (`services/agents/auto_trader.py`, `shared/utils/auto_trade.py`). The
model's strongest calls are traded in an Alpaca **paper** account each session a few minutes
after the open — never a live venue, whatever `BROKER_TYPE` says, and off unless
`AUTO_TRADE_ENABLED` is set. Book B1 (adopted 24 October 2026 after a pre-registered trial of
seven constructions on the same forecasts): short the bottom tenth of the monthly ranks that can
be borrowed cheaply, buy the 90th–99th percentile (the very top 1% has lost money in the model's
own record), equal weight, half the capital each side, rebuilt every 21 sessions, no index fund.
Over 98 out-of-sample months it beat the top-and-bottom-fifth book (net Sharpe 0.82 against
0.71); weighting by calibrated expected return over risk, beta-neutral legs, stress scaling and
stops on shorts all did worse. Shorts are whole shares, so the short leg's shares are allocated
together to keep its dollar size; the trader only touches positions it opened. Orders and the
account are on the Portfolio page (`/api/v1/predictions/auto-trade`). Still to build: an
automatic pause when live skill turns negative, per-name caps, opening-auction or limit orders,
and a live-versus-backtest drift alarm.

**Everything else graded on prices** (`shared/utils/producer_grades.py`, daily). Correlation
rules and scenarios that name a stock are asked whether it then moved more than it usually
does (about 1.0× means no information); agent calls are graded net of SPY; entity claims
are set against a placebo — was it already active the day before? Equity rules graded
uninformative on fifty or more alerts no longer take a model-written scenario.

**Changes go live on the lab's verdict** (`shared/utils/portfolio_lab.py` `adoptions`). The
weekly lab tests candidate changes on the monthly model's out-of-sample history, and the
model portfolio and the stock buy/sell ideas both apply only what its latest run says beats
what it replaces (Sharpe up 0.10, worst period no deeper). As of 21 October 2026: scaling by
the momentum factor's own volatility (Barroso & Santa-Clara; Sharpe 2.77 against 2.49, worst
month −2.9% against −5.7%) is live, scaling idea sizes and the overlay; ranking within
statistical return themes (2.22 against 2.36) and a sentiment-led short leg (2.45 against
2.49) are not. Sizing by each pick's modelled chance of paying (meta-labelling) is built for the
monthly ideas and the model portfolio's legs, and switches on only when two consecutive runs
both show a gain; the sentiment short tilt, once adopted, sizes the portfolio's short leg
apart from its long leg, and each day is marked with the weights it was rebalanced at. Each horizon's
buys and sells, and the model portfolio's books, report their independent bets (the
participation ratio of their return correlations).

**Session guards.** A daily bar for a session still open is never stored (the vendor
serves today's bar as it stands; one stored at 09:39 New York carried nine minutes of
trading as the close), a post-close top-up writes every name at 16:35 New York before the
22:00 UTC model run, and a one-hour signal is raised only inside the regular session for its
whole hour.

**Look-ahead guards.** Fundamentals, short interest and earnings reactions are read point
in time; a Freqtrade-style truncation test checks that arrays built from data filed up to a
date equal, on every earlier row, arrays built from everything. Features are tested the
same way.

---

## 👁️ Civic: Watching the Watchers

The same collection and evidence machinery, turned on surveillance itself. **Civic →
Surveillance Watch** (`/watch`) maps who watches the public and helps people protect their
data. Agencies and vendors are the subject: nothing looks up a plate or a person, and a
place someone clicks is not stored.

| Layer | Source (free, public) | Refresh |
|---|---|---|
| Licence-plate readers (maker, operator, facing) | DeFlock's national file, built from OpenStreetMap (ODbL): ~144,000 readers, streamed and upserted in batches | Weekly |
| Gunshot detectors | OpenStreetMap via Overpass, 1.5° tiles, backing off when the public servers are busy | Weekly |
| Which agency uses which technology, with a source link per deployment | EFF Atlas of Surveillance (~15,000 deployments, ~9,500 agencies) | Weekly |
| Agency locations | Census gazetteer: city centre, else county centre | Weekly |

- **Agency report cards** (`/api/v1/civic/agency`): each technology in plain words, its vendors,
  the evidence with links, devices mapped within 15 km, and an intensity score weighting mass,
  persistent, location-revealing tools (plate readers, face recognition, cell-site simulators,
  real-time crime centres) highest.
- **What watches here** (`/api/v1/civic/near`): devices within a radius, nearest first, and the
  agencies centred nearby with their tools.
- **Protect yourself**: free, ordered steps — California's DROP portal, the Big Ass Data Broker
  Opt-Out List, phone ad ID and location, Ring sharing, Have I Been Flocked, records requests,
  EFF's Rayhunter.

**Is it legal here?** (`shared/utils/civic_law.py`, `/api/v1/civic/legal`). Each agency's
published Flock practice — retention and the full list of agencies it shares plate data with,
from Flock transparency portals as aggregated by EyesOnFlock (CC BY-SA 4.0; 1,528 agencies,
39,499 cameras) — is checked against its state's licence-plate-reader law, transcribed with
citations from NCSL's 50-state summary (September 2026) and, for California, the Attorney
General's reading of SB 34. ICE's official 287(g) list (2,650 agreements, 2,278 agencies) is
matched to the same agencies. Checks: retention over the state limit; out-of-state or federal
sharing where the state restricts it; sharing that needs signed declarations; a published
policy that omits immigration where the law bars it; a 287(g) agreement beside a Flock network.
Every finding carries its evidence and citation and is worded as a question ("possible
violation" or "ask for records"): statutes carry exceptions published data cannot show, and a
finding resting only on an ambiguous name code ('LA', 'CO', 'SD') is only a question. Each
agency page drafts a public-records request for the records that would settle its findings;
the reader sends it themselves (Sentinel never files on anyone's behalf).

**More views** (`/api/v1/civic/audits`, `/departures`, `/drones`, `/money`, `/brokers`):
- *Search logs* — 533 agencies' released Flock search logs (EyesOnFlock), last 90 days, summarised
  to counts: nationwide lookups (72% reached 100+ networks), searches with no reason (47%), and
  reasons matching a published keyword list (immigration; protests and press). Reason text is
  never stored. Searches citing immigration are flagged where state law restricts it.
- *Dropped portals* — a portal missing from the weekly list is marked as a likely cancellation.
- *Drone waivers* — 963 FAA drone-as-first-responder waivers from EFF's FOIA release, mapped.
- *Federal money* — USAspending contracts to surveillance vendors (Palantir, Axon, Dataminr,
  Cellebrite, Clearview, Babel Street, location brokers…) and grants naming a surveillance
  technology; ICE alone paid these vendors $608M.
- *Data brokers* — California's registry: what 603 brokers declare they collect and who they
  sold to (28 to law enforcement without a subpoena), each with its opt-out link.

**Your privacy plan** (`shared/utils/privacy_plan.py`, `POST /api/v1/civic/plan`). A few
answers — state, phone, car, doorbell camera, and optional concerns (immigration, reproductive
care, activism or press, someone who may try to find you) — return a ranked list of free steps:
California's DROP portal for Californians; opt-out links for every broker that declared selling
to police; phone advertising-ID and location settings; car data and plate searches; doorbell
sharing; a records request to your own police department; concern-specific protections; a
credit freeze. Answers travel in the request body (not the URL, which access logs keep), are
validated by a Pydantic schema, and are never stored.

**Watch feed** (`shared/utils/watch_feed.py`, `/api/v1/civic/feed`). One timeline of what is
changing: surveillance news — the general news feeds judged on the headline, plus seven
civil-liberties sources (EFF, 404 Media, The Record, The Markup, Techdirt, ACLU, EPIC) judged
on headline and summary — classified by precise topic (plate readers, face recognition, gunshot
detection, police drones, camera networks, phone tracking, data brokers, federal surveillance,
privacy law); and changes in the civic data after each table's first load: new legal findings,
dropped or new Flock portals, drone waivers, federal awards of $100k or more, and newly
documented deployments. The classifier was tuned on 30 days of collected news: "flock to the
EU", the Navy's Stingray drone, Palantir's stock and a stolen bodega cat no longer match.

**Courts** (`shared/utils/courts.py`, `/api/v1/civic/courts`). Federal cases over plate
readers, face recognition, gunshot detection, location data and phone tracking (CourtListener
RECAP search), split civil and criminal with multi-defendant prosecutions collapsed to one row;
the assigned judges; and what those judges disclosed holding in surveillance vendors (Ethics in
Government Act disclosures). A holding is context; a *possible* 28 U.S.C. §455(b)(4) conflict is
flagged only when the vendor held is itself a party to the case. Needs `COURTLISTENER_TOKEN`;
calls are budgeted per day (cases/judges 70, vendor holdings 45) and the holdings scan resumes
where it stopped, rarest vendors first.

**Lobbying** (`shared/utils/lobbying.py`, `/api/v1/civic/lobbying`, Federal money tab). Every
Lobbying Disclosure Act filing for Flock, Axon, Motorola Solutions, Palantir, SoundThinking,
Clearview, Rekor, Cellebrite, Skydio, Dataminr, Babel Street, LexisNexis Risk and Fusus (lda.gov,
no key, weekly): amount by year, the lobbying firm, the issues and the agencies and chambers
lobbied. Firms report income from the client, in-house lobbying reports expenses; trade-group
spending is not included.

Service: `collector-civic` (`services/collector-civic/main.py`, 160M; parsing in
`shared/utils/surveillance.py`, tables from migration 0045). Map tiles are fetched by the
gateway from OpenStreetMap and cached on disk (`/api/v1/civic/tiles`), so a viewer's
browser never tells a tile server where they are looking; the CSP stays `'self'`. Next: an
oversight (CCOPS) layer, more state data-broker registries, and court filings in the watch feed.

---

## 📊 Market Data

Price bars are stored as one-minute records and rolled up automatically into
**5m, 15m, 30m, 1h, 4h, 1d, 1w and 1mth** views.

**Daily history is consolidated (every exchange, SIP)** — from January 2015 for every ticker
the model can rank, and for 1,078 stocks that no longer trade where the vendor still serves them,
split- and dividend-adjusted, each row recording its feed (`shared/utils/bar_feed.py`).
Until 8 October 2026 it came from IEX alone, and an IEX daily bar is a different object:
its close is IEX's last trade (25–40 bps from the official close on low-priced names), its
volume is 2–7% of the tape by stock, and it has no bar at all on a day a thin stock did not
trade on IEX. Every dollar-volume floor and cost estimate reads the feed setting. Minute
bars stay on IEX, the feed the live stream continues them with.

**Free short-side and earnings data** (migrations 0043–0044):

| Source | What is kept | History | Where |
|---|---|---|---|
| FINRA daily short sale volume (`cdn.finra.org`) | Off-exchange short and total volume per stock per day | From 2021, then daily | `finra_short_volume` |
| Interactive Brokers public borrow file (guest FTP) | Borrow fee, rebate, shares available | One snapshot a day from 9 Oct 2026 | `borrow_rates` |
| Finnhub earnings calendar (free tier) | Consensus EPS and revenue estimate, snapshotted daily | From 9 Oct 2026; revisions from our own copies | `earnings_estimates` |
| Finnhub `/stock/earnings` | Actual vs estimate, last four quarters, top 1,000 names | Weekly refresh | `earnings_surprises` |
| SEC filing-dated facts (already kept) | Standardized earnings surprise (`sue`) from quarterly net income | Full history | model feature candidate |
| Alpaca news (Benzinga) | Company articles with the stocks they name, FinBERT tone | From Sept 2023, live every 2 minutes | `stock_news` |
| Alpaca option bars and contracts | ATM IV, skew, call–put IV spread, put/call volume, rebuilt per stock per day | From Feb 2024 for the 600 most traded names, updated each evening | `option_iv_history` |
| Cboe delayed option chains (free, keyless) | Call/put volume and open interest, put/call ratios, 30-day IV, ATM IV, 25-delta skew | Each evening from 22 Oct 2026, 600 names plus the live focus | `option_chain_daily` |
| Yahoo futures, DRAMeXchange spot, RunPod GPU rates | 24 futures and bitcoin; DRAM, NAND flash and GDDR spot; per-GPU hourly rental | Futures from 2015; memory and GPU from 22 Oct 2026 | `commodity_prices` |

The model portfolio charges each short its quoted borrow fee and will not short a name
costing more than 5% a year. Pre-registered trial G (9 Oct 2026) adopted `svr_20` and
`svr_chg` (FINRA short share, 20-day level and change): weekly net Sharpe 0.90 → 1.00,
deflated 0.53 → 0.59; monthly 1.24 → 1.25, a marginal gain. The standardized earnings
surprise (`sue`) lowered both horizons and stays board context.

Without adjustment, a 4-for-1 split looks like a 75% crash — the largest move the system
would ever see, carrying no information — so history is always requested adjusted.

**Live ticks on free slots** (`shared/utils/live_focus.py`). The free plans allow 30 IEX
trade-and-quote subscriptions, 50 Finnhub tick symbols and 200 indicative option quotes. A live
focus list ranks the names the paper account trades today, then names entering or leaving the
book, held names reporting earnings, held names by how extreme their rank is, then the radar's
watched names; the IEX stream stores trades and quotes for its top 15 (`tick_trades`,
`tick_quotes`), the option stream quotes and trades for the nearest-the-money calls and puts of
its first 50 (`option_ticks`, MessagePack), and Finnhub's discovery half follows it. Each
commodity price is linked to the stocks that make or buy it (`shared/utils/commodity_links.py`),
and every domain's per-stock values go into one point-in-time table read by the model's trials
(`stock_features_daily`, `shared/utils/stock_features.py`): insider measures since 2016, adverse
8-K events, commodity exposure. Sources, cadences and accuracy checks: [DATA_PIPELINE.md](DATA_PIPELINE.md).

Two limits worth knowing. Delisted companies are included only where the broker still
serves their history (1,078 names), so backtests still lean somewhat toward survivors.
And adjusted prices are adjusted as of today: fine for research, not for claiming past
returns.

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

### 2. Start Sentinel Platform

The stack is split across Compose **profiles**, and a bare `docker compose up`
starts only the 14 services that have no profile -- the databases, Kafka, the
gateway, enrichment, correlation and the frontend. No collectors, no agents, no
observability. That is why a plain `up` looks like "not all the containers
started": the rest were never selected.

Use the Makefile, which knows the combinations:

```bash
make up
```

`make up` runs preflight checks and starts the **collectors** profile alongside
the core: ingestion plus the dashboard. This is the default mode.

```bash
make reasoning
```

`make reasoning` stops the collectors and starts the **agents** profile -- the
LLM swarm and Ollama. The two modes are deliberately exclusive: on a single
host, twelve collectors and a local inference tier do not fit in memory
together, and running both is how the OOM killer gets involved.

```bash
make obs
```

`make obs` adds Prometheus, Grafana and Kafka UI to whichever mode is running.

To drive Compose directly, name the profiles explicitly:

```bash
docker compose --profile collectors up -d --build
```

| Profile | Services | When |
| :--- | :--- | :--- |
| *(none)* | ingress, zookeeper, kafka, timescaledb, neo4j, redis, qdrant, migrator, enrichment, correlation, api-gateway, frontend, dlq-worker, telemetry-worker | Always started |
| `collectors` | 12 domain collectors | Ingestion — `make up` |
| `agents` | reasoning, ollama, agents-fast, agents-heavy, alert-manager | Inference — `make reasoning` |
| `obs` | prometheus, grafana, kafka-ui | Monitoring — `make obs` |
| `test` | integration-tests | CI only |

### 3. Pull Local LLM Models (Ollama)

Only needed for `make reasoning`; Ollama does not run in the default mode.

```bash
docker exec -it sentinel-ollama ollama pull qwen2.5:3b
```

Access the dashboard at **`https://localhost`** (Nginx TLS ingress auto-generates a self-signed certificate on first boot; mount production certs at `deploy/nginx/ssl/sentinel.crt` and `sentinel.key` for trusted HTTPS).

---

## 💻 CLI & API Operations

### 1. Docker Cluster Management
```bash
# Start the default mode (core + collectors) in the background
make up

# Inspect health and container states, with memory headroom
make ps

# Follow service logs
docker compose logs -f enrichment correlation api-gateway agents-heavy telemetry-worker

# Stop every profile's containers, keeping volumes
make stop
```

`docker compose down` without `--profile` leaves the collector and agent
containers running, because Compose only acts on the services the selected
profiles name. `make down` passes all three.

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

### 3. Automated Test Suite (5,194 Python tests, 178 frontend)
```bash
# Everything
python -m pytest tests/

# One area — the suite is split across 393 files by subject
python -m pytest tests/test_quant_and_trading.py
python -m pytest tests/test_correlation_and_graph.py
python -m pytest tests/test_rbac_security.py

# Anything matching a keyword
python -m pytest tests/ -k "backtest or calibration"
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
