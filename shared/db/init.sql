-- SENTINEL — TimescaleDB Schema
-- Runs automatically on first container start via docker-entrypoint-initdb.d

-- VECTOR: Enables storing and querying high-dimensional ML embeddings (pgvector).
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- POSTGIS: Adds geospatial types and functions (e.g., ST_Distance, ST_DWithin).
CREATE EXTENSION IF NOT EXISTS postgis;
-- PG_TRGM: Adds Trigram matching for fuzzy text search (e.g., finding misspelled vessel names).
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- UUID-OSSP: Generates unique IDs ensuring no collision across distributed systems.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── EVENTS (The Core Fact Table) ──────────────────────────────────────────────
-- PURPOSE: Records meaningful occurrences (anomalies, zone entries, transactions).
-- STRUCTURE: Polymorphic. Uses JSONB to store data for different domains (Maritime, Air, Finance) in one timeline.
-- RELATIONSHIPS: The central hub. 'primary_entity_id' links to vessels/people. 'correlation_ids' links to alerts.
-- INTERACTION: This table is the "Source of Truth." Other tables (Correlations) point TO this table.
--              It does NOT point to others with hard constraints to ensure ingestion never fails.
CREATE TABLE IF NOT EXISTS events (
    -- PRIMARY KEY (UUID): Essential for distributed systems. Unlike SERIAL (1, 2, 3), UUIDs can be generated 
    -- by multiple collectors simultaneously without checking a central database for the "next number."
    event_id UUID DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL, -- TIMESTAMPTZ converts to UTC on storage. Critical for coordinating global events across timezones.
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT,
    source_reliability FLOAT DEFAULT 1.0,

    -- "Soft Foreign Key" (Logical Link): 
    -- We link loosely to strings (MMSI) rather than using 'REFERENCES vessels(id)'.
    -- NECESSITY: High-speed ingestion. If we enforced a hard FK, we would have to reject an event 
    -- if the vessel wasn't in our registry yet. We prefer to capture the data first, reconcile later.
    primary_entity_id TEXT,
    primary_entity_type TEXT,
    primary_entity_name TEXT,
    primary_entity_flags TEXT[],

    -- GEOGRAPHY(POINT, 4326): Uses WGS84 (GPS standard). 
    -- Calculations (ST_Distance) return meters on a curved earth, unlike GEOMETRY which uses planar degrees.
    coordinates GEOGRAPHY(POINT, 4326),
    region TEXT,
    country_code TEXT,
    headline TEXT,
    summary TEXT,
    url TEXT,
    -- POLYMORPHISM: JSONB allows "NoSQL" flexibility within SQL. 
    -- We can query specific keys (e.g., WHERE vessel_data->>'imo' = '123') without schema migrations for every new field.
    vessel_data JSONB,
    flight_data JSONB,
    financial_data JSONB,
    security_data JSONB,
    prediction_market_data JSONB,
    crypto_data JSONB,
    cyber_data JSONB,

    tags TEXT[],
    named_entities TEXT[],
    sentiment FLOAT,
    anomaly_score FLOAT DEFAULT 0.0,
    correlation_ids UUID[],
    trace_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, occurred_at) -- Composite PK: UUID + Time. This allows efficient time-range queries while ensuring uniqueness.

);  

-- HYPERTABLE: Partitions the table into "chunks" by time.
-- Benefit: Recent data stays in RAM (hot), old data moves to disk (cold). Queries on time ranges only scan relevant chunks.
SELECT create_hypertable('events', 'occurred_at', if_not_exists => TRUE);

-- GIST (Generalized Search Tree): Optimized for nearest-neighbor (KNN) and bounding-box spatial queries.
CREATE INDEX IF NOT EXISTS events_geo_idx ON events USING GIST(coordinates);
CREATE INDEX IF NOT EXISTS events_entity_time_idx ON events(primary_entity_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_type_time_idx ON events(type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_anomaly_idx ON events(anomaly_score DESC, occurred_at DESC) WHERE anomaly_score > 0.5;
-- GIN (Generalized Inverted Index): Essential for fast searching within Arrays (tags) and JSONB documents.
CREATE INDEX IF NOT EXISTS events_tags_idx ON events USING GIN(tags);
CREATE INDEX IF NOT EXISTS events_entities_idx ON events USING GIN(named_entities);
CREATE INDEX IF NOT EXISTS events_region_time_idx ON events(region, occurred_at DESC) WHERE region IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_source_time_idx ON events(source, occurred_at DESC);

-- RETENTION: Automatically drop event chunks older than 90 days to prevent unbounded disk growth.
-- Adjust the interval based on operational needs. Historical data should be exported to cold storage first.
SELECT add_retention_policy('events', INTERVAL '90 days', if_not_exists => TRUE);

-- ── VESSEL POSITIONS (high-frequency, separate table) ─────────────────────────
-- PURPOSE: Stores raw AIS telemetry (pings every few seconds).
-- STRATEGY: Separated from 'events' to handle high write volume without bloating the main ledger.
-- OPTIMIZATION: Uses heavy compression because we rarely query individual historical pings, usually just aggregates or tracks.
-- LOGICAL JOIN: mmsi -> events.primary_entity_id
CREATE TABLE IF NOT EXISTS vessel_positions (
    mmsi TEXT NOT NULL, -- Maritime Mobile Service Identity. The unique ID for AIS.
    occurred_at TIMESTAMPTZ NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    speed_knots FLOAT,
    heading INT, 
    nav_status TEXT,
    source TEXT DEFAULT 'AIS',
    -- COMPOSITE PRIMARY KEY (id + time):
    -- NECESSITY: TimescaleDB requires the partitioning column ('occurred_at') to be part of the Primary Key.
    -- This allows the database to instantly know which "chunk" (file) a specific row lives in.
    PRIMARY KEY (mmsi, occurred_at)
);

-- CHUNKING: Creates a new file partition every 1 day. 
-- This makes "DROP" operations (removing old data) instant by just unlinking a file, rather than deleting rows one by one.
SELECT create_hypertable('vessel_positions', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
-- COMPRESSION: Converts Row-Oriented storage (standard DB) to Columnar storage (Arrays) after 7 days.
-- Reduces disk usage by ~90-95% for historical GPS data, at the cost of making older rows Read-Only.
ALTER TABLE vessel_positions SET (timescaledb.compress, timescaledb.compress_segmentby = 'mmsi');
SELECT add_compression_policy('vessel_positions', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS vpo_mmsi_time_idx ON vessel_positions(mmsi, occurred_at DESC);

-- ── CORRELATIONS ──────────────────────────────────────────────────────────────
-- PURPOSE: The "Alert" layer. Groups multiple events into a detected pattern.
-- EXAMPLE: "Dark Rendezvous" = Event A (Vessel 1 Dark) + Event B (Vessel 2 Dark) + Event C (Proximity).
-- RELATIONSHIPS: Links one-to-many with 'events' (trigger + supporting).
-- INTERACTION: Acts as a "Wrapper" around multiple Event rows.
CREATE TABLE IF NOT EXISTS correlations (
    correlation_id       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    trace_id             UUID,
    rule_id              TEXT,
    rule_name            TEXT,
    alert_tier           INT,
    detected_at          TIMESTAMPTZ DEFAULT NOW(),
    -- LOGICAL KEYS: These point back to events.event_id.
    -- We do not use "REFERENCES events(event_id)" because 'events' is a Hypertable (partitioned).
    -- Foreign Keys pointing TO a Hypertable from a regular table are restricted in some PG versions and incur perf costs.
    trigger_event_id     UUID,
    supporting_event_ids UUID[],
    entity_ids           TEXT[],
    description          TEXT,
    tags                 TEXT[],
    scenario             JSONB,
    status               TEXT DEFAULT 'active'
);

CREATE INDEX IF NOT EXISTS corr_tier_time_idx ON correlations(alert_tier, detected_at DESC);
CREATE INDEX IF NOT EXISTS corr_tags_idx      ON correlations USING GIN(tags);

-- ── SCENARIOS (Phase 2) ───────────────────────────────────────────────────────
-- PURPOSE: The "Investigation" layer. Represents a human analyst's workspace.
-- FLOW: Correlations (Auto) -> Scenarios (Manual).
-- RELATIONSHIPS: Strong FK to 'correlations'.
-- INTERACTION: This allows analysts to add "State" (Status, Notes) to a read-only Correlation.
CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id            UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    trace_id               UUID,
    -- HARD FOREIGN KEY: This enforces integrity. A Scenario MUST belong to a valid Correlation.
    -- NECESSITY: Unlike the high-speed 'events' table, this is low-volume human data. Integrity is more important than raw speed here.
    correlation_id         UUID        REFERENCES correlations(correlation_id),
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW(),
    status                 TEXT DEFAULT 'hypothesis',
    headline               TEXT,
    narrative_summary      TEXT,
    embedding              vector(768),
    significance           TEXT,
    hypotheses             JSONB,
    recommended_monitoring TEXT[],
    confidence_overall     INT,
    confidence_rationale   TEXT,
    confidence_history     JSONB       DEFAULT '[]',
    supporting_event_ids   UUID[]
);
 
CREATE INDEX IF NOT EXISTS scenario_status_idx ON scenarios(status, created_at DESC);


-- HNSW INDEX: This is the "Tier 1" secret. It builds a graph-based index for vectors.
-- vector_cosine_ops tells it to optimize specifically for Cosine Similarity (<->).
CREATE INDEX IF NOT EXISTS scenario_embedding_idx ON scenarios USING hnsw (embedding vector_cosine_ops);




-- shared/db/init.sql (Append this to the bottom)

CREATE TABLE IF NOT EXISTS failed_events (
    id SERIAL PRIMARY KEY,
    failed_at TIMESTAMPTZ DEFAULT NOW(),
    original_topic TEXT NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    retry_count INT DEFAULT 0,
    permanently_failed BOOLEAN DEFAULT FALSE
);

ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS retry_count INT DEFAULT 0;
ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS permanently_failed BOOLEAN DEFAULT FALSE;

-- Index for fast querying by topic and resolution status
CREATE INDEX IF NOT EXISTS idx_failed_events_topic ON failed_events(original_topic, resolved);

-- ── AUDIT LEDGER ──────────────────────────────────────────────────────────────
-- Durable, append-only, SHA-256 hash-chained audit trail. This is the system of
-- record for trade execution, governance changes, and watchlist mutations.
-- Redis holds a read cache of the same entries; it is NOT the source of truth
-- (it runs allkeys-lru and is therefore evictable).
CREATE TABLE IF NOT EXISTS audit_ledger (
    seq           BIGSERIAL PRIMARY KEY,
    hash          TEXT NOT NULL UNIQUE,
    -- UNIQUE makes chain forks structurally impossible: a given entry can have at
    -- most one successor, so two concurrent appends reading the same head cannot
    -- both commit. The loser retries against the new head.
    prev_hash     TEXT NOT NULL UNIQUE,
    timestamp     TIMESTAMPTZ NOT NULL,
    actor         TEXT NOT NULL,
    action        TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT,
    ip_address    TEXT,
    details       JSONB
);

-- Chain traversal + chronological reads.
CREATE INDEX IF NOT EXISTS audit_ledger_time_idx     ON audit_ledger(timestamp DESC);
CREATE INDEX IF NOT EXISTS audit_ledger_actor_idx    ON audit_ledger(actor, timestamp DESC);
CREATE INDEX IF NOT EXISTS audit_ledger_resource_idx ON audit_ledger(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS audit_ledger_prev_idx     ON audit_ledger(prev_hash);

-- Append-only enforcement: the ledger's tamper-evidence depends on rows never
-- being mutated or removed in place.
CREATE OR REPLACE FUNCTION audit_ledger_forbid_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_ledger is append-only; % is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_ledger_no_update ON audit_ledger;
CREATE TRIGGER audit_ledger_no_update
    BEFORE UPDATE OR DELETE ON audit_ledger
    FOR EACH ROW EXECUTE FUNCTION audit_ledger_forbid_mutation();

-- ── VIEWS ─────────────────────────────────────────────────────────────────────

-- VIEW (Virtual Table): INTERACTION
-- This abstracts the complex logic of finding the "latest" row.
-- Instead of the App writing "SELECT DISTINCT ON...", the App just does "SELECT * FROM vessel_latest_positions".
-- This decouples the Frontend code from the Database structure.
CREATE OR REPLACE VIEW vessel_latest_positions AS
-- DISTINCT ON: A PostgreSQL power-feature. It sorts the bucket by (mmsi, time), then keeps ONLY the first row per mmsi.
-- This is significantly faster than using "GROUP BY mmsi" with aggregate MAX() functions for high-cardinality data.
SELECT DISTINCT ON (mmsi)
    mmsi, occurred_at, lat, lon, speed_knots, heading, nav_status
FROM vessel_positions
ORDER BY mmsi, occurred_at DESC;
 
-- VIEW: Quick dashboard access to high-severity recent events.
CREATE OR REPLACE VIEW active_anomalies AS
SELECT * FROM events
WHERE occurred_at > NOW() - INTERVAL '48 hours'
  AND anomaly_score > 0.6
ORDER BY anomaly_score DESC, occurred_at DESC;

-- ── TRADFI BARS HYPERTABLE & MULTI-TIMEFRAME CONTINUOUS AGGREGATES (§2.1, §2.3, §2.5) ──
CREATE TABLE IF NOT EXISTS tradfi_bars (
    ticker TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    session TEXT DEFAULT 'REGULAR',
    PRIMARY KEY (ticker, time)
);

SELECT create_hypertable('tradfi_bars', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS tradfi_bars_ticker_time_idx ON tradfi_bars(ticker, time DESC);

-- 1. 5-Minute Continuous Aggregate (Materialized View)
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_5m
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('5 minutes', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('5 minutes', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_5m',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- 2. 15-Minute Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_15m
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('15 minutes', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('15 minutes', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_15m',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE);

-- 3. 1-Hour Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_1h
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('1 hour', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('1 hour', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_1h',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- 4. 1-Day Daily Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_1d
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('1 day', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('1 day', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_1d',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- 5. 1-Week Weekly Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_1w
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('1 week', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('1 week', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_1w',
    start_offset => INTERVAL '180 days',
    end_offset => INTERVAL '1 week',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- 6. 1-Month Monthly Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_1mth
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('30 days', time) AS bucket_time,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM tradfi_bars
GROUP BY ticker, time_bucket('30 days', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('tradfi_bars_1mth',
    start_offset => INTERVAL '365 days',
    end_offset => INTERVAL '30 days',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- 7. Rolling 20-Bucket Z-Score View over 5-Minute CAGG (§2.3)
-- Frame strictly uses 'ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING' to exclude the current bucket from its own baseline
CREATE OR REPLACE VIEW tradfi_bars_5m_zscore AS
WITH rolling_stats AS (
    SELECT
        ticker,
        bucket_time,
        open,
        high,
        low,
        close,
        volume,
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY bucket_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_mean_20,
        STDDEV_SAMP(close) OVER (
            PARTITION BY ticker 
            ORDER BY bucket_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_std_20,
        COUNT(close) OVER (
            PARTITION BY ticker 
            ORDER BY bucket_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS sample_count
    FROM tradfi_bars_5m
)
SELECT
    ticker,
    bucket_time,
    open,
    high,
    low,
    close,
    volume,
    rolling_mean_20,
    rolling_std_20,
    sample_count,
    CASE
        WHEN rolling_std_20 IS NOT NULL AND rolling_std_20 > 1e-6 AND sample_count >= 10 THEN
            (close - rolling_mean_20) / rolling_std_20
        ELSE 0.0
    END AS z_score
FROM rolling_stats;