-- SENTINEL — TimescaleDB Schema
-- Runs automatically on first container start via docker-entrypoint-initdb.d
 
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
    country_code CHAR(2),
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
    created_at TIMESTAMPTZ DEFAULT NOW()
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

-- ── VESSEL POSITIONS (high-frequency, separate table) ─────────────────────────
-- PURPOSE: Stores raw AIS telemetry (pings every few seconds).
-- STRATEGY: Separated from 'events' to handle high write volume without bloating the main ledger.
-- OPTIMIZATION: Uses heavy compression because we rarely query individual historical pings, usually just aggregates or tracks.
-- LOGICAL JOIN: mmsi -> events.primary_entity_id
CREATE TABLE IF NOT EXISTS vessel_positions (
    id BIGSERIAL,
    mmsi VARCHAR(20) NOT NULL, -- Maritime Mobile Service Identity. The unique ID for AIS.
    occurred_at TIMESTAMPTZ NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    speed_knots FLOAT,
    heading INT, 
    nav_status VARCHAR(50),
    source VARCHAR(50) DEFAULT 'AIS',
    -- COMPOSITE PRIMARY KEY (id + time):
    -- NECESSITY: TimescaleDB requires the partitioning column ('occurred_at') to be part of the Primary Key.
    -- This allows the database to instantly know which "chunk" (file) a specific row lives in.
    PRIMARY KEY (id, occurred_at)
);

-- CHUNKING: Creates a new file partition every 1 day. 
-- This makes "DROP" operations (removing old data) instant by just unlinking a file, rather than deleting rows one by one.
SELECT create_hypertable('vessel_positions', 'occurred_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
-- COMPRESSION: Converts Row-Oriented storage (standard DB) to Columnar storage (Arrays) after 7 days.
-- Reduces disk usage by ~90-95% for historical GPS data, at the cost of making older rows Read-Only.
SELECT add_compression_policy('vessel_positions', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS vpo_mmsi_time_idx ON vessel_positions(mmsi, occurred_at DESC);

-- ── CORRELATIONS ──────────────────────────────────────────────────────────────
-- PURPOSE: The "Alert" layer. Groups multiple events into a detected pattern.
-- EXAMPLE: "Dark Rendezvous" = Event A (Vessel 1 Dark) + Event B (Vessel 2 Dark) + Event C (Proximity).
-- RELATIONSHIPS: Links one-to-many with 'events' (trigger + supporting).
-- INTERACTION: Acts as a "Wrapper" around multiple Event rows.
CREATE TABLE IF NOT EXISTS correlations (
    correlation_id       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id              VARCHAR(50),
    rule_name            VARCHAR(200),
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
    status               VARCHAR(20) DEFAULT 'active'
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
    -- HARD FOREIGN KEY: This enforces integrity. A Scenario MUST belong to a valid Correlation.
    -- NECESSITY: Unlike the high-speed 'events' table, this is low-volume human data. Integrity is more important than raw speed here.
    correlation_id         UUID        REFERENCES correlations(correlation_id),
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW(),
    status                 VARCHAR(20) DEFAULT 'hypothesis',
    headline               TEXT,
    significance           TEXT,
    hypotheses             JSONB,
    recommended_monitoring TEXT[],
    confidence_overall     INT,
    confidence_rationale   TEXT,
    confidence_history     JSONB       DEFAULT '[]',
    supporting_event_ids   UUID[]
);
 
CREATE INDEX IF NOT EXISTS scenario_status_idx ON scenarios(status, created_at DESC);

-- ── ENTITY WATCHLIST ──────────────────────────────────────────────────────────
-- PURPOSE: Configuration table for high-interest entities (Sanctioned vessels, VIPs).
-- USAGE: Ingestion logic checks this table to elevate priority/severity of incoming events.
CREATE TABLE IF NOT EXISTS entity_watchlist (
    id            SERIAL       PRIMARY KEY,
    entity_id     VARCHAR(100) NOT NULL UNIQUE,
    entity_type   VARCHAR(50),
    entity_name   VARCHAR(200),
    added_at      TIMESTAMPTZ  DEFAULT NOW(),
    notes         TEXT,
    alert_tier_min INT         DEFAULT 1,
    active        BOOLEAN      DEFAULT TRUE
);

-- shared/db/init.sql (Append this to the bottom)

CREATE TABLE IF NOT EXISTS failed_events (
    id SERIAL PRIMARY KEY,
    failed_at TIMESTAMPTZ DEFAULT NOW(),
    original_topic VARCHAR(255) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    resolved BOOLEAN DEFAULT FALSE
);

-- Index for fast querying by topic and resolution status
CREATE INDEX IF NOT EXISTS idx_failed_events_topic ON failed_events(original_topic, resolved);
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