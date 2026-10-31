import asyncio
import sys
import logging
from shared.db import get_timescale

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("db.migrate")

# Centralized migrations list.
# 0001 is handled by init.sql on fresh container startup.
MIGRATIONS = [
    {
        "version": "0001_initial_schema",
        "sql": None,
        "transactional": True
    },
    {
        "version": "0002_add_trace_id",
        "sql": """
            ALTER TABLE events ADD COLUMN IF NOT EXISTS trace_id UUID;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS trace_id UUID;
            ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS trace_id UUID;
        """,
        "transactional": True
    },
    {
        "version": "0003_add_lat_lon_columns",
        "sql": """
            ALTER TABLE events ADD COLUMN IF NOT EXISTS latitude FLOAT;
            ALTER TABLE events ADD COLUMN IF NOT EXISTS longitude FLOAT;
        """,
        "transactional": True
    },
    {
        "version": "0004_create_tradfi_bars_hypertable",
        "sql": """
            -- ── TRADFI BARS HYPERTABLE (§2.1) ───────────────────────────────────
            -- Durable equity-bar history across sessions
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
        """,
        "transactional": True
    },
    {
        "version": "0005_create_tradfi_bars_continuous_aggregates",
        "sql": """
            -- ─────────────────────────────────────────────────────────────────────────────
            -- RECONCILIATION OF MULTI-TIMEFRAME CANDLE ARCHITECTURE (§2.5):
            -- Redis / Lua multi-timeframe candle aggregator serves exclusively as the low-latency
            -- hot path for UI/chart rendering.
            -- TimescaleDB Continuous Aggregates (tradfi_bars_5m, 15m, 1h, 1d, 1w, 1mth) and the
            -- rolling Z-score view (tradfi_bars_5m_zscore) serve as the authoritative, durable,
            -- and replayable source of truth for alerts, risk signals, and statistical significance gating.
            -- ─────────────────────────────────────────────────────────────────────────────

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
        """,
        "transactional": False
    },
    {
        "version": "0006_account_email_verification",
        "sql": """
            -- Open signup means the address on an account is a claim until it is
            -- proven. An unverified account still gets the free tier immediately
            -- -- the whole analyst platform -- but cannot be charged and cannot
            -- reset a password, because both of those trust the mailbox.
            ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ;

            -- Existing accounts predate verification. The operator seeded from
            -- environment credentials is trusted by construction; marking it
            -- verified avoids locking the owner out of their own deployment.
            UPDATE users SET email_verified = TRUE, email_verified_at = NOW()
             WHERE role = 'ADMIN' AND email_verified = FALSE;

            -- One table for both purposes. Tokens are stored as a SHA-256 digest
            -- and never in the form that was emailed: a database read must not
            -- yield a working password-reset link for every account.
            CREATE TABLE IF NOT EXISTS auth_tokens (
                id           BIGSERIAL PRIMARY KEY,
                user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash   TEXT NOT NULL UNIQUE,
                purpose      TEXT NOT NULL CHECK (purpose IN ('verify_email', 'reset_password')),
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ
            );

            CREATE INDEX IF NOT EXISTS auth_tokens_user_idx ON auth_tokens(user_id, purpose);
            -- Expiry sweeps scan on this.
            CREATE INDEX IF NOT EXISTS auth_tokens_expiry_idx ON auth_tokens(expires_at)
                WHERE consumed_at IS NULL;

            -- Interest in the paid tier while billing is switched off. Kept
            -- separate from `users` so that turning payments on later is a
            -- config change and not a data migration.
            CREATE TABLE IF NOT EXISTS pro_waitlist (
                id          BIGSERIAL PRIMARY KEY,
                user_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
                email       TEXT NOT NULL UNIQUE,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                notified_at TIMESTAMPTZ
            );
        """,
        "transactional": True
    },
    {
        "version": "0007_event_corroboration",
        "sql": """
            -- Independent corroboration of a claim across sources, for the
            -- events where that is meaningful (news, OSINT). Deliberately
            -- separate from source_reliability: that is a source's historical
            -- record, this is whether anyone else is reporting the same thing
            -- right now. A trusted outlet reporting alone and four outlets
            -- agreeing are different situations, and conflating them is how a
            -- single source comes to look like consensus.
            ALTER TABLE events ADD COLUMN IF NOT EXISTS corroboration JSONB;

            -- Single-sourced claims are the ones worth surfacing to an analyst,
            -- so they get an index rather than a scan.
            CREATE INDEX IF NOT EXISTS events_single_sourced_idx
                ON events ((corroboration->>'is_single_sourced'))
                WHERE corroboration IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0008_domain_payload_indexes",
        "sql": """
            -- One partial index per domain payload column.
            --
            -- /events/{domain} filters on "<column> IS NOT NULL" and then takes
            -- the most recent rows. Nothing indexed that predicate, so finding
            -- the newest matching rows meant scanning back through the whole
            -- hypertable ordered by time. Measured on a 50-row request against
            -- ~700k events: tradfi 15.6s, cyber 18.2s, prediction 9.6s -- the
            -- sparse domains, where matches are rare and the scan runs longest.
            -- The dense ones (crypto, maritime) came back in ~0.1s because a
            -- match turns up almost immediately.
            --
            -- Partial and ordered by time, because that is exactly the access
            -- pattern: "the newest N rows carrying this payload".
            CREATE INDEX IF NOT EXISTS events_financial_time_idx
                ON events (occurred_at DESC) WHERE financial_data IS NOT NULL;
            CREATE INDEX IF NOT EXISTS events_crypto_time_idx
                ON events (occurred_at DESC) WHERE crypto_data IS NOT NULL;
            CREATE INDEX IF NOT EXISTS events_security_time_idx
                ON events (occurred_at DESC) WHERE security_data IS NOT NULL;
            CREATE INDEX IF NOT EXISTS events_prediction_time_idx
                ON events (occurred_at DESC) WHERE prediction_market_data IS NOT NULL;
            CREATE INDEX IF NOT EXISTS events_vessel_time_idx
                ON events (occurred_at DESC) WHERE vessel_data IS NOT NULL;
            CREATE INDEX IF NOT EXISTS events_flight_time_idx
                ON events (occurred_at DESC) WHERE flight_data IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0009_entity_case_insensitive_lookup",
        "sql": """
            -- Case-insensitive entity lookup, without rewriting the hypertable.
            --
            -- primary_entity_id is not one identifier space. tradfi writes
            -- upper-cased tickers; crypto upper-cases the asset on most paths
            -- and not on one; wallet addresses arrive EIP-55 mixed-case
            -- straight from the RPC; Polymarket slugs are lowercase kebab;
            -- maritime writes a numeric MMSI; and correlation writes the
            -- literals 'HAWKES' and 'CORRELATION'. Six namespaces, one TEXT
            -- column, and no rule saying which casing wins.
            --
            -- So any reader comparing with `=` matches or misses depending on
            -- which collector happened to write the row. /market-series
            -- returned an empty series -- a blank chart -- for every symbol
            -- stored in a casing other than the caller's.
            --
            -- Matching on upper(column) settles it at read time. The index is
            -- what makes that affordable: events_entity_time_idx cannot serve a
            -- function-wrapped predicate, so the obvious fix would otherwise
            -- turn the query behind every chart into a scan of the hypertable.
            --
            -- An expression index and not a GENERATED ALWAYS ... STORED column,
            -- deliberately: a stored generated column rewrites every chunk, and
            -- nothing needs to select this key -- only match on it. The key
            -- stays derived either way, so it cannot drift out of agreement
            -- with the column it comes from, and no collector has to be changed
            -- for it to hold.
            CREATE INDEX IF NOT EXISTS events_entity_id_upper_time_idx
                ON events (upper(primary_entity_id), occurred_at DESC);

            -- The same query ORs against the name. An OR needs both sides
            -- indexed, or Postgres falls back to scanning whichever chunks the
            -- time predicate admits.
            CREATE INDEX IF NOT EXISTS events_entity_name_upper_time_idx
                ON events (upper(primary_entity_name), occurred_at DESC);
        """,
        "transactional": True
    },
    {
        "version": "0010_oidc_identity",
        "sql": """
            -- Federated identity, alongside passwords rather than instead of
            -- them. Both columns null on every existing row, so a deployment
            -- with no OIDC_ISSUER configured is unaffected by this migration.
            ALTER TABLE users ADD COLUMN IF NOT EXISTS oidc_issuer  TEXT;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS oidc_subject TEXT;

            -- password_hash becomes nullable, because an account that only ever
            -- signs in through the IdP has no password and should not be given
            -- a placeholder one. A placeholder is a credential: it either
            -- verifies against something, or it is a row that looks like it has
            -- a password and silently cannot be used to sign in. Null says
            -- exactly what is true -- there is no password on this account --
            -- and verify_password already returns False for a null hash, so the
            -- password login path rejects these accounts without a change.
            ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL;

            -- The pair is the identity, not the subject alone: `sub` is only
            -- promised to be unique within an issuer, so two providers can
            -- legitimately both call someone "12345". Unique, because two
            -- accounts answering to one federated identity means whichever row
            -- the query happens to return decides who you are signed in as.
            CREATE UNIQUE INDEX IF NOT EXISTS users_oidc_identity_idx
                ON users (oidc_issuer, oidc_subject)
                WHERE oidc_subject IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0011_canonical_entity_ids",
        "sql": """
            -- Backfills primary_entity_id to the spelling Entity now enforces.
            --
            -- shared/models/events.canonical_entity_id() canonicalises at
            -- construction, so every row written from here on is already
            -- correct. This is the history: rows written before the rule
            -- existed, by collectors that each chose their own casing.
            --
            -- The predicate is "differs from its canonical form", NOT "is of
            -- this type". That distinction is the whole cost of this migration.
            -- Measured here: 1.4M rows carry one of these types and exactly
            -- 5,728 of them are spelled wrongly, so the type-based predicate
            -- rewrote 1.4M rows to change 5,728 -- it had not finished after
            -- seven minutes and held a lock on a live hypertable the whole
            -- time. Scanning for the 5,728 and writing only those is the same
            -- result in a fraction of the work.
            --
            -- primary_entity_name is deliberately untouched. It is what a
            -- person reads, and several of these rows carry the same string in
            -- both columns -- "kreysler & associates" -- so folding the display
            -- copy would turn a company name into shouting to fix an
            -- identifier nobody looks at.

            -- Tickers, CVE ids, ISO country codes, AS numbers.
            UPDATE events SET primary_entity_id = upper(btrim(primary_entity_id))
            WHERE primary_entity_type IN
                  ('instrument','company','vulnerability','country','infrastructure')
              AND primary_entity_id IS NOT NULL
              AND primary_entity_id <> upper(btrim(primary_entity_id));

            -- Wallet and ICAO24 addresses: the same hex, one spelling. EIP-55
            -- checksum casing is a validation artifact, not an identity.
            UPDATE events SET primary_entity_id = lower(btrim(primary_entity_id))
            WHERE primary_entity_type IN ('wallet','aircraft')
              AND primary_entity_id IS NOT NULL
              AND primary_entity_id <> lower(btrim(primary_entity_id));

            -- MMSI/IMO are numeric; some feeds decorate them. A value with no
            -- digits at all is not one of those and is left alone rather than
            -- reduced to an empty identifier.
            UPDATE events SET primary_entity_id =
                   regexp_replace(primary_entity_id, '[^0-9]', '', 'g')
            WHERE primary_entity_type = 'vessel'
              AND primary_entity_id IS NOT NULL
              AND regexp_replace(primary_entity_id, '[^0-9]', '', 'g') <> ''
              AND primary_entity_id <> regexp_replace(primary_entity_id, '[^0-9]', '', 'g');
        """,
        "transactional": True
    },
    {
        "version": "0012_prediction_market_identity",
        "sql": """
            -- Repairs what 0011 broke, and stops it recurring.
            --
            -- Prediction-market contracts were typed 'instrument' because
            -- EntityType had no member for them, so 0011's ticker rule folded
            -- them upward -- turning a Polymarket slug into
            -- WILL-THERE-BE-7-9-KNIFE-KILLS-AT-BLAST-BOUNTY-... That is not a
            -- cosmetic problem: the venue's own lowercase slug is the key this
            -- deployment writes to Redis (sentinel:prediction:outcomes:{slug})
            -- and the agents' categorical resolver reads back, so upper-casing
            -- the database's copy split one contract's identity across two
            -- stores -- the precise failure 0011 exists to close.
            --
            -- The original survives on the row, in
            -- prediction_market_data->>'market_id', so this restores from
            -- evidence rather than guessing at the previous casing. Only rows
            -- that differ from it by case alone are touched; a row whose id
            -- genuinely differs from its market_id is left alone, because that
            -- is a different problem and not one 0011 caused.
            UPDATE events
            SET primary_entity_id = prediction_market_data->>'market_id',
                primary_entity_type = 'prediction_market'
            WHERE type = 'prediction_market_trade'
              AND prediction_market_data->>'market_id' IS NOT NULL
              AND primary_entity_id <> prediction_market_data->>'market_id'
              AND upper(primary_entity_id) = upper(prediction_market_data->>'market_id');

            -- The rest keep their identifier and gain the correct type, so the
            -- casing rule never applies to them again. Measured: 239 rows.
            UPDATE events SET primary_entity_type = 'prediction_market'
            WHERE type = 'prediction_market_trade'
              AND primary_entity_type = 'instrument';
        """,
        "transactional": True
    },
    {
        "version": "0013_correlation_published_fields",
        "sql": """
            -- A correlation is published with twenty fields and persisted with
            -- thirteen. The eight below were dropped in silence on every one of
            -- 373,887 rows, because the writer names the columns it knows about
            -- and nothing raises for the ones it does not.
            --
            -- confidence_score is the costly one: the distribution measured on
            -- the wire -- 79% of clusters on one of two values, 193 published
            -- at exactly 1.0 -- exists only in Kafka, so nothing reading a
            -- correlation from the database can see how confident it was. The
            -- calibration harness had to reach through the trigger event for an
            -- anomaly score for exactly this reason.
            --
            -- summary_headline is the line the alert is identified by; a reader
            -- working from the database had to reconstruct it from description,
            -- which is a different sentence written for a different purpose.
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS confidence_score    DOUBLE PRECISION;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS primary_domain      TEXT;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS summary_headline    TEXT;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS supporting_headlines TEXT[];
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS metrics_summary     JSONB;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS primary_entity_id   TEXT;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS primary_entity_name TEXT;
            ALTER TABLE correlations ADD COLUMN IF NOT EXISTS entity_names        TEXT[];

            -- 2,587 distinct rule_ids across 373,887 rows and no index on the
            -- column, so "what has this rule been doing" -- the natural question
            -- once the namespace grew -- was a sequential scan every time.
            CREATE INDEX IF NOT EXISTS corr_rule_time_idx
                ON correlations(rule_id, detected_at DESC);

            -- The join back to the event that caused the cluster.
            CREATE INDEX IF NOT EXISTS corr_trigger_event_idx
                ON correlations(trigger_event_id)
                WHERE trigger_event_id IS NOT NULL;

            -- Ranking by confidence is the query the new column exists for.
            CREATE INDEX IF NOT EXISTS corr_confidence_time_idx
                ON correlations(confidence_score DESC, detected_at DESC)
                WHERE confidence_score IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0014_drop_indexes_covering_no_rows",
        "sql": """
            -- Two partial indexes were built for fields the enrichment layer was
            -- expected to fill and never did: corroboration is null on 100% of
            -- events and named_entities is empty on 99.9%. An index is a claim
            -- that a column will be queried and will have values in it; these
            -- two recorded an intention, cost write amplification on every
            -- insert, and covered nothing.
            --
            -- They are dropped rather than kept empty because a reader who finds
            -- them assumes the fields are populated. If either field starts
            -- being written, the index is one statement to restore.
            DROP INDEX IF EXISTS events_single_sourced_idx;
            DROP INDEX IF EXISTS events_entities_idx;
        """,
        "transactional": True
    },
    {
        "version": "0015_agent_prediction_outcomes",
        "sql": """
            -- agent_predictions had no column in which an outcome could be
            -- recorded. Resolution happened in Redis and the durable store kept
            -- only the claim, so even with the scorer working there was no
            -- persisted history of what was predicted and what happened -- which
            -- is the corpus a calibration needs.
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS agent_name    TEXT;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS ticker        TEXT;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS direction     TEXT;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS entry_price   DOUBLE PRECISION;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS horizon_hours INT;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS resolved_at   TIMESTAMPTZ;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS outcome_correct BOOLEAN;
            ALTER TABLE agent_predictions ADD COLUMN IF NOT EXISTS brier_score   DOUBLE PRECISION;

            -- Its only index was the primary key, so it could not be queried by
            -- prediction, by cause or by time without a scan.
            CREATE INDEX IF NOT EXISTS agent_pred_prediction_idx ON agent_predictions(prediction_id);
            CREATE INDEX IF NOT EXISTS agent_pred_corr_idx       ON agent_predictions(correlation_id);
            CREATE INDEX IF NOT EXISTS agent_pred_time_idx       ON agent_predictions(occurred_at DESC);
            CREATE INDEX IF NOT EXISTS agent_pred_unresolved_idx
                ON agent_predictions(occurred_at DESC)
                WHERE resolved_at IS NULL;
        """,
        "transactional": True
    },
    {
        "version": "0016_events_supply_chain_data",
        "sql": """
            -- Every other event sub-model has a column: vessel_data,
            -- flight_data, financial_data, security_data,
            -- prediction_market_data, crypto_data, cyber_data. supply_chain_data
            -- did not, so the freight feed had nowhere durable to land even once
            -- the enricher built it -- it would have travelled on Kafka and been
            -- absent from every query written against the events table.
            ALTER TABLE events ADD COLUMN IF NOT EXISTS supply_chain_data JSONB;

            -- The queries this column exists to serve ask "which freight
            -- indices moved, and when", so the index is on time within the
            -- rows that have one. Partial, because supply-chain events are a
            -- small fraction of the table and a full index would mostly store
            -- nulls.
            CREATE INDEX IF NOT EXISTS events_supply_chain_idx
                ON events(occurred_at DESC)
                WHERE supply_chain_data IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0017_failed_events_created_at",
        "sql": """
            -- The table that records permanently-failed messages had no
            -- queryable timestamp, so "what has been failing since the deploy"
            -- could not be asked of it -- which is the only question anyone
            -- asks of a dead-letter table.
            ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
            CREATE INDEX IF NOT EXISTS failed_events_created_idx ON failed_events(created_at DESC);
        """,
        "transactional": True
    },
    {
        "version": "0018_events_filing_data",
        "sql": """
            -- Every SEC filing's structured detail was being discarded on write.
            --
            -- The enricher builds a full FilingData for each one -- form_type,
            -- cik, accession_number, items, primary_doc_url, report_date,
            -- is_material_8k -- and events had no column to put it in, so the
            -- object was constructed on every filing and dropped by the writer.
            -- 466 filings over 48 hours were stored with the form type
            -- recoverable only by parsing it back out of the headline text.
            --
            -- This is why the filing stream could not be filtered by form: the
            -- field existed in the model, in the collector's payload and in the
            -- enricher's output, and nowhere in the table.
            ALTER TABLE events ADD COLUMN IF NOT EXISTS filing_data JSONB;

            -- The question this column exists to answer is "which filings of
            -- this form, recently", so the index is on form type and time.
            CREATE INDEX IF NOT EXISTS events_filing_form_idx
                ON events((filing_data->>'form_type'), occurred_at DESC)
                WHERE filing_data IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0019_tradfi_bars_30m_4h_aggregates",
        "sql": """
            -- Two timeframes the platform evaluates but never stored.
            --
            -- TIMEFRAMES_MINUTES is [1, 5, 15, 30, 60, 240]: the streaming
            -- candle evaluator scores 30-minute and 4-hour windows on every
            -- tick. The durable side had 5m, 15m, 1h, 1d, 1w and 1mth, so
            -- those two existed only as Redis lists -- capped, volatile, and
            -- gone on restart. Any consumer wanting 30-minute or 4-hour
            -- history had nowhere to read it, and the fallback chain in the
            -- radar agent (1h, then 15m, then 5m) skipped straight past both.
            --
            -- 1-minute needs no aggregate: tradfi_bars IS the minute bar, so
            -- it is the base of every rollup here rather than a gap in them.
            --
            -- Offsets follow the convention already established: start_offset
            -- scaled to the bucket, end_offset one bucket back so a partially
            -- filled bucket is never materialised, and schedule_interval at
            -- the bucket width.
            CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_30m
            WITH (timescaledb.continuous) AS
            SELECT
                ticker,
                time_bucket('30 minutes', time) AS bucket_time,
                FIRST(open, time) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, time) AS close,
                SUM(volume) AS volume
            FROM tradfi_bars
            GROUP BY ticker, time_bucket('30 minutes', time)
            WITH NO DATA;

            SELECT add_continuous_aggregate_policy('tradfi_bars_30m',
                start_offset => INTERVAL '7 days',
                end_offset => INTERVAL '30 minutes',
                schedule_interval => INTERVAL '30 minutes',
                if_not_exists => TRUE);

            CREATE MATERIALIZED VIEW IF NOT EXISTS tradfi_bars_4h
            WITH (timescaledb.continuous) AS
            SELECT
                ticker,
                time_bucket('4 hours', time) AS bucket_time,
                FIRST(open, time) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, time) AS close,
                SUM(volume) AS volume
            FROM tradfi_bars
            GROUP BY ticker, time_bucket('4 hours', time)
            WITH NO DATA;

            SELECT add_continuous_aggregate_policy('tradfi_bars_4h',
                start_offset => INTERVAL '30 days',
                end_offset => INTERVAL '4 hours',
                schedule_interval => INTERVAL '4 hours',
                if_not_exists => TRUE);
        """,
        "transactional": False
    },
    {
        "version": "0020_retention_and_compression",
        "sql": """
            -- Nothing bounded the growth of any hypertable.
            --
            -- No add_retention_policy, no compression and no drop_chunks
            -- appeared anywhere in these migrations, against 121,370 crypto
            -- transfer events in a single 24 hours, on a host whose entire
            -- allocation is 26 GiB and whose database is capped at 1.5 GiB of
            -- memory. The raw tables are also the least valuable thing in the
            -- system once they have been rolled up: the continuous aggregates
            -- carry the history the statistics actually read.
            --
            -- Compression first, retention well behind it. Chunks older than a
            -- week compress -- typically an order of magnitude on OHLCV, which
            -- is highly repetitive per ticker -- and only much older raw data
            -- is dropped, always long after every aggregate has been
            -- materialised from it.
            ALTER TABLE tradfi_bars SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'ticker',
                timescaledb.compress_orderby = 'time DESC'
            );
            SELECT add_compression_policy('tradfi_bars', INTERVAL '7 days', if_not_exists => TRUE);
            SELECT add_retention_policy('tradfi_bars', INTERVAL '400 days', if_not_exists => TRUE);

            ALTER TABLE events SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'type',
                timescaledb.compress_orderby = 'occurred_at DESC'
            );
            SELECT add_compression_policy('events', INTERVAL '14 days', if_not_exists => TRUE);
            -- 90 days, matching init.sql, because that is what is running.
            --
            -- This asked for 400 and got 90. init.sql already declares a
            -- 90-day retention on events, and `if_not_exists => TRUE` keeps the
            -- existing policy rather than replacing it -- so the deployment has
            -- run 90 days for the life of this migration while the line above
            -- said 400 and called itself "deliberately generous". Verified
            -- against timescaledb_information.jobs on the running database:
            -- {"drop_after": "90 days"}.
            --
            -- 90 is also the defensible number on this host. The events
            -- hypertable is 5,856 MB against a 26 GiB VM; compression at 14
            -- days is what makes even that fit, and 400 days of raw events is
            -- not something this allocation can hold. A correlation citing an
            -- event older than 90 days dangles, and that is a real cost -- it
            -- is a smaller one than the database filling.
            SELECT add_retention_policy('events', INTERVAL '90 days', if_not_exists => TRUE);
        """,
        "transactional": False
    },
    {
        "version": "0021_failed_events_resolution",
        "sql": """
            -- `failed_events.resolved` had no writer.
            --
            -- The column is declared in init.sql, is half of
            -- idx_failed_events_topic, and no statement anywhere in the tree
            -- ever set it. 11,539 rows sat at FALSE, which is the same thing as
            -- the column not existing -- except that an index was being
            -- maintained on it and an operator reading the table would take
            -- "0 resolved" as a finding rather than as an absence.
            --
            -- There was also no way to act on a dead letter. A dead-letter table
            -- with no replay path is a log file with a schema: the whole point
            -- of keeping the payload is being able to put it back.
            --
            -- These columns are what a replay writes when it succeeds.
            ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS resolved_at  TIMESTAMPTZ;
            ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS resolved_by  TEXT;
            ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS replay_count INT DEFAULT 0;
            ALTER TABLE failed_events ADD COLUMN IF NOT EXISTS last_replay_error TEXT;

            -- The question the replay path asks is "what is still outstanding
            -- on this topic, oldest first", and the existing
            -- (original_topic, resolved) index cannot order it.
            CREATE INDEX IF NOT EXISTS failed_events_unresolved_idx
                ON failed_events (original_topic, failed_at DESC)
                WHERE resolved = FALSE;
        """,
        "transactional": True
    },
    {
        "version": "0022_correlations_index_and_retention",
        "sql": """
            -- The only query this table serves could not use either of its
            -- indexes.
            --
            -- services/api_gateway/routes/scenarios.py runs
            --   WHERE alert_tier >= $1 ORDER BY detected_at DESC LIMIT $2
            -- against corr_tier_time_idx (alert_tier, detected_at DESC). A
            -- range predicate on the leading column leaves the second column
            -- unordered across the matched range, so the planner cannot walk
            -- the index to satisfy the ORDER BY -- it reads every qualifying
            -- row and sorts. Measured at 385,000 rows on the live table, for a
            -- request that wants twenty.
            --
            -- An index on the sort column alone is what a top-N-by-time query
            -- wants: walk it backwards, drop rows failing the tier filter, stop
            -- at LIMIT. The composite index stays, because a query pinned to a
            -- single tier still uses it.
            CREATE INDEX IF NOT EXISTS corr_detected_at_idx
                ON correlations (detected_at DESC);

            -- And nothing bounded the table.
            --
            -- `events` is dropped at 90 days by migration 0020. A correlation
            -- older than that cites trigger and supporting event ids that no
            -- longer resolve, so it is not merely stale, it is unreadable --
            -- while still being scanned, indexed and backed up. Matching the
            -- events window keeps the two consistent.
            --
            -- `correlations` is a plain table, not a hypertable, so
            -- add_retention_policy does not apply; TimescaleDB's job scheduler
            -- runs the delete instead. Rows an analyst has opened an
            -- investigation on are exempt: scenarios holds a hard foreign key
            -- to correlation_id, and a human workspace outliving the raw
            -- detection is the whole point of that table.
            CREATE OR REPLACE PROCEDURE sentinel_prune_correlations(
                job_id INT, config JSONB
            )
            LANGUAGE plpgsql AS $$
            DECLARE
                keep_days INT := COALESCE((config->>'keep_days')::INT, 90);
            BEGIN
                DELETE FROM correlations c
                WHERE c.detected_at < NOW() - (keep_days || ' days')::INTERVAL
                  AND NOT EXISTS (
                      SELECT 1 FROM scenarios s WHERE s.correlation_id = c.correlation_id
                  );
            END;
            $$;

            -- Dead letters too. The table had 11,539 rows, no retention and,
            -- until migration 0021, nothing that could ever mark one done.
            -- Only resolved rows are dropped: an outstanding failure is still
            -- work to do however old it is.
            CREATE OR REPLACE PROCEDURE sentinel_prune_failed_events(
                job_id INT, config JSONB
            )
            LANGUAGE plpgsql AS $$
            DECLARE
                keep_days INT := COALESCE((config->>'keep_days')::INT, 30);
            BEGIN
                DELETE FROM failed_events
                WHERE resolved = TRUE
                  AND resolved_at IS NOT NULL
                  AND resolved_at < NOW() - (keep_days || ' days')::INTERVAL;
            END;
            $$;

            -- Registered once. add_job has no if_not_exists, so re-running this
            -- migration body outside the schema_migrations guard would schedule
            -- duplicates; the guard is what prevents that, and the lookup below
            -- makes it safe anyway.
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM timescaledb_information.jobs
                    WHERE proc_name = 'sentinel_prune_correlations'
                ) THEN
                    PERFORM add_job(
                        'sentinel_prune_correlations', INTERVAL '1 day',
                        config => '{"keep_days": 90}'::jsonb
                    );
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM timescaledb_information.jobs
                    WHERE proc_name = 'sentinel_prune_failed_events'
                ) THEN
                    PERFORM add_job(
                        'sentinel_prune_failed_events', INTERVAL '1 day',
                        config => '{"keep_days": 30}'::jsonb
                    );
                END IF;
            END
            $$;
        """,
        "transactional": False
    },
    {
        "version": "0023_scenarios_primary_entity",
        "sql": """
            -- The subject of a scenario, carried on the model since it was
            -- written and never persisted.
            --
            -- `Scenario` resolves `primary_entity_id` and
            -- `primary_entity_name` from `entity_ids`/`entity_names` in a
            -- validator that runs on every scenario built, and the table had
            -- no column for either -- so `_save_scenario` could not have
            -- inserted them if it had tried. The feed's scenario card and its
            -- detail modal both read `primary_entity_name`, so every scenario
            -- in the product was labelled "Multi-Entity", including the ones
            -- about a single vessel or a single ticker.
            --
            -- TEXT rather than UUID: an entity id here is an MMSI, an ICAO24,
            -- a ticker or an AS number, never a generated key.
            ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS primary_entity_id TEXT;
            ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS primary_entity_name TEXT;

            -- "What has this platform said about NVDA" is the question the
            -- column exists to answer, so the index is on subject and time.
            CREATE INDEX IF NOT EXISTS scenarios_entity_time_idx
                ON scenarios(primary_entity_id, created_at DESC)
                WHERE primary_entity_id IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0024_events_score_provenance",
        "sql": """
            -- How a score was arrived at, discarded at the database boundary.
            --
            -- `NormalizedEvent` carries `anomaly_breakdown` -- the spatial,
            -- temporal, volume and volatility sub-scores, the coverage fraction
            -- and which estimator produced the number -- and
            -- `score_adjustments`, the ordered (reason, delta) steps the scorer
            -- records as it builds the composite. Both are populated on scored
            -- events. Neither had a column, so the writer dropped them and the
            -- only thing reaching the table was the single float at the end.
            --
            -- The cost landed on /explain/event/{id}, the endpoint whose whole
            -- purpose is to show that derivation: with no real steps to read,
            -- it printed an invented four-step waterfall instead.
            ALTER TABLE events ADD COLUMN IF NOT EXISTS anomaly_breakdown JSONB;
            ALTER TABLE events ADD COLUMN IF NOT EXISTS score_adjustments JSONB;

            -- Which estimator backed a score, over time. A 0.4 from a warm-up
            -- curve and a 0.4 from a full percentile window are not the same
            -- claim, and this is the column that can tell them apart.
            CREATE INDEX IF NOT EXISTS events_score_basis_idx
                ON events((anomaly_breakdown->>'coverage_basis'), occurred_at DESC)
                WHERE anomaly_breakdown IS NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0025_macro_column_entity_index_and_ais_retention",
        "sql": """
            -- 1. The macro payload had nowhere to land.
            --
            -- `events` carried nine payload columns -- vessel, flight,
            -- financial, security, prediction_market, crypto, cyber,
            -- supply_chain, filing -- and db_writer inserted all nine. There
            -- was no macro_data column and none in the INSERT, while
            -- collector-macro/economic_calendar.py fills the field on every
            -- release. A macro release persisted as a headline and a type with
            -- a null payload: surprise_pct, actual, forecast and previous
            -- survived the 48-hour Redis window and were gone from the durable
            -- store, so no backtest, no calibration corpus and no deep-window
            -- clause could ever see them. supply_chain_data got its column in
            -- 0016 for exactly this reason; macro was missed.
            ALTER TABLE events ADD COLUMN IF NOT EXISTS macro_data JSONB;

            -- 2. Restoring the entity index, on the condition 0014 named.
            --
            -- 0014 dropped events_entities_idx because named_entities was empty
            -- on 99.9% of events, and said: "If either field starts being
            -- written, the index is one statement to restore." It started being
            -- written -- the news enricher now writes confirmed tickers plus
            -- the affected_tickers the regulatory collector already resolved,
            -- which is what made a news-to-mover join possible at all.
            --
            -- Without it, /radar/movers runs an unindexed array-overlap scan
            -- across a ninety-day hypertable on every page load, on the request
            -- path. This is that one statement.
            CREATE INDEX IF NOT EXISTS events_entities_idx
                ON events USING GIN(named_entities);

            -- 3. Bounding the one hypertable nothing reads.
            --
            -- vessel_positions takes every AIS position report and appears in
            -- the tree only in writes -- there is no SELECT against it
            -- anywhere. It has columnar compression after seven days and, alone
            -- among the hypertables, no retention policy: 0020 set 400 days on
            -- tradfi_bars and 90 on events and did not name this one.
            --
            -- Ninety days matches `events`, which is the table a reader would
            -- join it to if one is ever written. Retention rather than a drop:
            -- the table is cheap to keep bounded and the positions are the
            -- record a track reconstruction would need.
            SELECT add_retention_policy('vessel_positions', INTERVAL '90 days', if_not_exists => TRUE);
        """,
        "transactional": False
    },
    {
        "version": "0026_daily_aggregate_is_current_and_backfilled",
        "sql": """
            -- The per-ticker standing reads tradfi_bars_1d, whose policy is
            -- `end_offset => INTERVAL '1 day'` with a daily schedule -- so it
            -- deliberately excludes the current day and refreshes once. A
            -- watchlist row therefore carried a live day_pct from the snapshot
            -- sweep beside a change_pct_week that ended a session or two
            -- earlier, with nothing saying so.
            --
            -- Real-time aggregation closes the gap without touching the
            -- materialisation policy: the view unions the materialised region
            -- with a live read of the bars past it, so a query sees today.
            ALTER MATERIALIZED VIEW tradfi_bars_1d
                SET (timescaledb.materialized_only = false);

            -- And the history behind it. `start_offset => INTERVAL '30 days'`
            -- means each scheduled refresh only rewrites the trailing month,
            -- the view was created WITH NO DATA, and nothing in this repository
            -- calls refresh_continuous_aggregate -- so on any database younger
            -- than 220 sessions the 50- and 200-day distances stayed null
            -- permanently, which is the same answer the code gives for "not
            -- enough history" and therefore indistinguishable from outside.
            --
            -- One full pass, once. tradfi_bars is retained for 400 days
            -- precisely so a 200-day average is reachable; this is what makes
            -- it reachable.
            CALL refresh_continuous_aggregate('tradfi_bars_1d', NULL, NULL);
        """,
        "transactional": False
    },
    {
        "version": "0027_headline_trigram_index",
        "sql": """
            -- Scenario resolution reads headlines with ILIKE and nothing could
            -- serve it.
            --
            -- `_match_signals` asks "has any event in the last 48 hours got a
            -- headline containing all of these keywords". `events` already
            -- carries GIN indexes on `tags` and `named_entities`, but those two
            -- conditions sit in an OR beside `headline ILIKE ...`, and an
            -- unindexable branch in a disjunction forces a sequential scan of
            -- the whole window regardless of what the other branches could
            -- have used. Measured on this deployment: 21.2 seconds per signal
            -- over 917,000 rows, returning nothing. The tracker runs up to ten
            -- signals for each of a hundred active scenarios, so the work it
            -- asked for was hours per cycle; what actually happened is that the
            -- client cancelled at sixty seconds, 51 times, logging an
            -- asyncpg QueryCanceledError whose str() is empty -- so the log
            -- read "Signal match query failed for 'X': " with no reason after
            -- the colon.
            --
            -- pg_trgm is already installed here, so this adds no dependency.
            -- The index takes roughly two and a half minutes to build across
            -- the hypertable's 24 chunks, 16 of which are compressed.
            --
            -- The index on its own does nothing: a trigram index cannot serve
            -- `headline ILIKE ALL($1)`, because a ScalarArrayOp is not an
            -- indexable operator. Measured on a 200,000-row copy, that form
            -- stays a sequential scan at 229ms while the equivalent AND-chain
            -- drops to 0.281ms on the same index. scenario_tracker.py issues
            -- the AND-chain for exactly this reason; changing it back to ALL()
            -- would silently return this query to a sequential scan.
            CREATE EXTENSION IF NOT EXISTS pg_trgm;

            CREATE INDEX IF NOT EXISTS events_headline_trgm_idx
                ON events USING GIN (headline gin_trgm_ops);
        """,
        "transactional": False,
        # Measured at 2m19s against the live hypertable; the pool's default
        # command_timeout of 60 cancels it less than half way through.
        "timeout": 900
    },
    {
        "version": "0028_vessel_region_recency_index",
        "sql": """
            -- "The newest vessels in each watched region" had no index behind it.
            --
            -- The global map asks /events/maritime for the newest 250 rows.
            -- Measured on this deployment, those 250 span 184 seconds and
            -- contain seven regions: Taiwan Territorial, Singapore Approach,
            -- Suez, the Black Sea, the Turkish Straits, the South China Sea and
            -- Malacca. Singapore and Taiwan alone produce roughly 4,400 vessel
            -- events an hour, so a chokepoint reporting four a day cannot
            -- appear in a three-minute window however large the limit gets --
            -- which is why the Strait of Hormuz and Bab-el-Mandeb were absent
            -- from the map even in the hours when they did report.
            --
            -- Reserving a few rows per region is the fix, and it needs an index
            -- that can seek to one region and walk backwards in time.
            -- events_region_time_idx is (region, occurred_at DESC) but carries
            -- no vessel predicate, so the planner preferred
            -- events_vessel_time_idx and filtered a rare region out of the whole
            -- stream: 82.7 seconds for twelve regions. With this index the same
            -- query is 5.6ms, and it builds in 16 seconds.
            CREATE INDEX IF NOT EXISTS events_vessel_region_time_idx
                ON events (region, occurred_at DESC)
                WHERE vessel_data IS NOT NULL;

            -- The aviation layer of the same map has the same shape and the
            -- same gap.
            CREATE INDEX IF NOT EXISTS events_flight_region_time_idx
                ON events (region, occurred_at DESC)
                WHERE flight_data IS NOT NULL;
        """,
        "transactional": False,
        "timeout": 900
    },
    {
        "version": "0029_tradfi_bars_volume_nullable",
        "sql": """
            -- Volume is not always known, and NOT NULL forced the writer to
            -- choose between inventing a number and dropping the bar. It had
            -- been dropping the bar.
            --
            -- The eleven GICS sector ETFs are served from Finnhub's quote
            -- endpoint, which returns a price and no volume. The collector was
            -- corrected once already for answering that with a hardcoded
            -- 1000.0 -- the five-minute aggregate SUMs this column, so every
            -- macro bar read exactly 4000 or 5000, the poll count rather than
            -- the market. It now sends NULL, which is true. But the enricher
            -- tested `volume <= 0` before persisting, so the honest null became
            -- a dropped row: XLK published at $188.00 roughly once a minute for
            -- hours and tradfi_bars held not one sector ETF bar, which is why
            -- the sector arm of the Hawkes work had nothing to read.
            --
            -- SUM() skips nulls and returns NULL for an all-null bucket, so the
            -- aggregates report "unknown" instead of a fabricated zero. No
            -- existing row changes: this only permits a value that could not be
            -- written before.
            ALTER TABLE tradfi_bars ALTER COLUMN volume DROP NOT NULL;
        """,
        "transactional": True
    },
    {
        "version": "0030_signal_event_archive",
        "sql": """
            -- The signal history, kept permanently.
            --
            -- `events` drops chunks after 90 days, and it is the only record of
            -- every headline, filing, sweep, block and cluster the reaction study
            -- learns from -- so the study could never see past 90 days, and a
            -- family that fires a few times a month could never reach the twenty
            -- independent units it needs to be judged at all. Raising the events
            -- retention would keep two gigabytes of vessel pings and telemetry
            -- with it. This keeps only what the study reads, in the columns it
            -- reads, with no retention policy: it only grows.
            CREATE TABLE IF NOT EXISTS signal_event_archive (
                event_id          TEXT NOT NULL,
                type              TEXT NOT NULL,
                source            TEXT,
                created_at        TIMESTAMPTZ NOT NULL,
                occurred_at       TIMESTAMPTZ,
                headline          TEXT,
                summary           TEXT,
                tags              TEXT[],
                sentiment         DOUBLE PRECISION,
                primary_entity_id TEXT,
                financial_data    JSONB,
                region            TEXT,
                PRIMARY KEY (event_id, created_at)
            );
            SELECT create_hypertable('signal_event_archive', 'created_at',
                chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS signal_event_archive_type_time
                ON signal_event_archive (type, created_at DESC);
            ALTER TABLE signal_event_archive SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'type',
                timescaledb.compress_orderby = 'created_at DESC'
            );
            SELECT add_compression_policy('signal_event_archive', INTERVAL '30 days', if_not_exists => TRUE);

            -- Everything still held in events, so nothing more is lost from now.
            INSERT INTO signal_event_archive
            SELECT event_id::text, type, source, created_at, occurred_at, headline, summary,
                   tags, sentiment, primary_entity_id, financial_data, region
            FROM events
            WHERE type IN ('headline', 'social_signal', 'vessel_dark', 'vessel_sts', 'supply_chain_metric',
                           'filing', 'regulatory_event', 'insider_cluster', 'earnings_surprise', 'options_flow',
                           'market_anomaly', 'equity_block', 'thirteen_f')
              AND NOT (type = 'market_anomaly' AND source NOT IN ('alpaca_quant_radar', 'finra_short_volume'))
              AND NOT (type = 'options_flow' AND COALESCE((financial_data->>'premium_usd')::float, 0) < 50000)
            ON CONFLICT DO NOTHING;
        """,
        # Idempotent throughout (IF NOT EXISTS, ON CONFLICT), and the seed reads
        # ninety days of compressed chunks: longer than the pool's 60 seconds.
        "transactional": False,
        "timeout": 1800,
    },
    {
        "version": "0031_daily_bars",
        "sql": """
            -- Adjusted daily bars, kept permanently, years deep.
            --
            -- tradfi_bars holds one-minute bars for 400 days and began on
            -- 2026-07-13, so nothing could be measured over more than a few
            -- months: not a 200-day average from vendor history, not a daily
            -- correlation over a year, not a five-day reaction with enough
            -- episodes. Daily bars are small -- five years of 2,000 names is ten
            -- million rows at most -- and split- and dividend-adjusted from the
            -- vendor, so a split is not read as a crash.
            CREATE TABLE IF NOT EXISTS daily_bars (
                ticker  TEXT NOT NULL,
                day     DATE NOT NULL,
                open    DOUBLE PRECISION,
                high    DOUBLE PRECISION,
                low     DOUBLE PRECISION,
                close   DOUBLE PRECISION NOT NULL,
                volume  DOUBLE PRECISION,
                source  TEXT NOT NULL DEFAULT 'alpaca_adjusted',
                PRIMARY KEY (ticker, day)
            );
            CREATE INDEX IF NOT EXISTS daily_bars_day ON daily_bars (day DESC);
        """,
        "transactional": True
    },
    {
        "version": "0032_aggregates_cover_backfilled_history",
        "sql": """
            -- Roll every backfilled minute into the intraday aggregates.
            --
            -- Their refresh policies rewrite only a trailing window (1h: 7 days,
            -- 4h: 30), so 1.47M minute bars backfilled for older weeks never
            -- reached them: the median ticker had 28 hourly bars in 60 days
            -- while its minute bars held enough for 300+. 0026 did this once for
            -- the daily view; the backfill loop now refreshes what it writes.
            CALL refresh_continuous_aggregate('tradfi_bars_5m', NULL, NOW() - INTERVAL '1 hour');
            CALL refresh_continuous_aggregate('tradfi_bars_15m', NULL, NOW() - INTERVAL '1 hour');
            CALL refresh_continuous_aggregate('tradfi_bars_30m', NULL, NOW() - INTERVAL '1 hour');
            CALL refresh_continuous_aggregate('tradfi_bars_1h', NULL, NOW() - INTERVAL '2 hours');
            CALL refresh_continuous_aggregate('tradfi_bars_4h', NULL, NOW() - INTERVAL '4 hours');
            CALL refresh_continuous_aggregate('tradfi_bars_1d', NULL, NOW() - INTERVAL '1 day');
        """,
        "transactional": False,
        "timeout": 3600,
    },
    {
        "version": "0033_option_daily",
        "sql": """
            -- One row per ticker per session of option-implied measures, kept
            -- permanently (shared/utils/options_positioning.py).
            --
            -- Put/call volume, IV skew and the call-put IV spread were read
            -- live and held for 36 hours in Redis, and this feed sells no
            -- history of them: every day not written down was a day that could
            -- never be studied. Upserted through the session; the last write is
            -- the close.
            CREATE TABLE IF NOT EXISTS option_daily (
                ticker            TEXT NOT NULL,
                day               DATE NOT NULL,
                call_volume       DOUBLE PRECISION,
                put_volume        DOUBLE PRECISION,
                put_call_ratio    DOUBLE PRECISION,
                iv_skew           DOUBLE PRECISION,
                atm_iv            DOUBLE PRECISION,
                skew_expiry_days  INTEGER,
                cp_iv_spread      DOUBLE PRECISION,
                cp_pairs          INTEGER,
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, day)
            );
            CREATE INDEX IF NOT EXISTS option_daily_day ON option_daily (day DESC);
        """,
        "transactional": True
    },
    {
        "version": "0034_short_interest",
        "sql": """
            -- FINRA consolidated short interest, one row per ticker per
            -- settlement, since 2020 (services/collector-filings/short_interest.py).
            -- Days-to-cover is recomputed from the parts: FINRA's own figure is
            -- rounded to whole days.
            CREATE TABLE IF NOT EXISTS short_interest (
                ticker            TEXT NOT NULL,
                settlement_date   DATE NOT NULL,
                short_qty         DOUBLE PRECISION NOT NULL,
                prev_short_qty    DOUBLE PRECISION,
                avg_daily_volume  DOUBLE PRECISION,
                days_to_cover     DOUBLE PRECISION,
                change_pct        DOUBLE PRECISION,
                PRIMARY KEY (ticker, settlement_date)
            );
            CREATE INDEX IF NOT EXISTS short_interest_date ON short_interest (settlement_date DESC);
        """,
        "transactional": True
    },
    {
        "version": "0035_company_profile",
        "sql": """
            -- Each listed company's SEC industry code, from its EDGAR
            -- submissions file, for industry features in the cross-sectional
            -- model (services/collector-filings/history.py).
            CREATE TABLE IF NOT EXISTS company_profile (
                ticker           TEXT PRIMARY KEY,
                cik              TEXT,
                sic              INTEGER,
                sic_description  TEXT,
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """,
        "transactional": True
    },
    {
        "version": "0036_recommendation_log",
        "sql": """
            -- Every trade idea, once per day per horizon, ticker and direction,
            -- with the price it was given at (services/agents/recommendation_engine.py),
            -- so the ideas can be graded against what prices did.
            CREATE TABLE IF NOT EXISTS recommendation_log (
                day                  DATE NOT NULL,
                horizon              TEXT NOT NULL,
                ticker               TEXT NOT NULL,
                direction            SMALLINT NOT NULL,
                validated            BOOLEAN NOT NULL,
                entry_price          DOUBLE PRECISION,
                stop_price           DOUBLE PRECISION,
                expected_excess_bps  DOUBLE PRECISION,
                model                TEXT,
                structure            TEXT,
                option_idea          JSONB,
                as_of                DATE,
                created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (day, horizon, ticker, direction)
            );
        """,
        "transactional": True
    },
    {
        "version": "0037_track_record",
        "sql": """
            -- What the platform said, graded against what prices did
            -- (shared/utils/track_record.py, services/agents/grader.py).
            ALTER TABLE recommendation_log
                ADD COLUMN IF NOT EXISTS spy_entry_price DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS graded_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS exit_day DATE,
                ADD COLUMN IF NOT EXISTS exit_price DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS ret DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS spy_ret DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS excess DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS signed_excess DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS hit BOOLEAN;

            -- The first board snapshot of each ticker each day, so the board's
            -- net vote can be tested as a predictor rather than trusted as one.
            CREATE TABLE IF NOT EXISTS signal_matrix_log (
                day              DATE NOT NULL,
                ticker           TEXT NOT NULL,
                net              DOUBLE PRECISION,
                lean             DOUBLE PRECISION,
                votes            DOUBLE PRECISION,
                proven_net       INTEGER,
                supported_net    INTEGER,
                published_net    INTEGER,
                anomaly_score    DOUBLE PRECISION,
                price            DOUBLE PRECISION,
                spy_price        DOUBLE PRECISION,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                excess_1d        DOUBLE PRECISION,
                excess_5d        DOUBLE PRECISION,
                graded_1d_at     TIMESTAMPTZ,
                graded_5d_at     TIMESTAMPTZ,
                PRIMARY KEY (day, ticker)
            );

            -- Every model run kept, so drift in what it finds is visible.
            CREATE TABLE IF NOT EXISTS factor_model_runs (
                run_at           TIMESTAMPTZ NOT NULL PRIMARY KEY,
                as_of            DATE,
                primary_horizon  TEXT,
                universe         INTEGER,
                tradable         INTEGER,
                report           JSONB NOT NULL
            );
        """,
        "transactional": True
    },
    {
        "version": "0038_fundamentals",
        "sql": """
            -- Reported financials for every filer, from SEC XBRL frames
            -- (services/collector-filings/fundamentals.py): one row per company,
            -- line item and period. Read point in time by the factor model --
            -- a period's figures count only from 90 days after it ends.
            CREATE TABLE IF NOT EXISTS fundamentals (
                cik           TEXT NOT NULL,
                ticker        TEXT,
                concept       TEXT NOT NULL,
                frame         TEXT NOT NULL,
                period_start  DATE,
                period_end    DATE NOT NULL,
                value         DOUBLE PRECISION NOT NULL,
                accn          TEXT,
                PRIMARY KEY (cik, concept, frame)
            );
            CREATE INDEX IF NOT EXISTS fundamentals_ticker ON fundamentals (ticker, concept, period_end);
        """,
        "transactional": True
    },
    {
        "version": "0039_fundamental_facts",
        "sql": """
            -- Every reported value with the date it was filed, from SEC company
            -- facts (services/collector-filings/company_facts.py). Append-only,
            -- keyed by accession: a restatement is a new row with a later
            -- filing date, so "what was known when" is a query. Frames (0038)
            -- carry only the latest filing for each period and no filing date.
            CREATE TABLE IF NOT EXISTS fundamental_facts (
                cik           TEXT NOT NULL,
                concept       TEXT NOT NULL,
                unit          TEXT NOT NULL,
                period_start  DATE,
                period_end    DATE NOT NULL,
                value         DOUBLE PRECISION NOT NULL,
                fy            INTEGER,
                fp            TEXT,
                form          TEXT,
                filed         DATE NOT NULL,
                accn          TEXT NOT NULL
            );
            -- The value is part of the key: a cover page reports one share
            -- count per class under the same accession and date.
            CREATE UNIQUE INDEX IF NOT EXISTS fundamental_facts_key ON fundamental_facts
                (cik, concept, accn, period_end, (COALESCE(period_start, DATE '1900-01-01')), value);
            CREATE INDEX IF NOT EXISTS fundamental_facts_cik ON fundamental_facts (cik, concept, filed);
            -- Ticker -> issuer. Keyed by ticker, so a company with two classes
            -- (GOOGL, GOOG) maps both, and a ticker can carry an issuer's old
            -- registration as well as its new one.
            CREATE TABLE IF NOT EXISTS issuer_ticker (
                ticker      TEXT NOT NULL,
                cik         TEXT NOT NULL,
                source      TEXT NOT NULL,
                fetched_at  TIMESTAMPTZ,
                last_filed  DATE,
                PRIMARY KEY (ticker, cik)
            );
            CREATE INDEX IF NOT EXISTS issuer_ticker_cik ON issuer_ticker (cik);
        """,
        "transactional": True
    },
    {
        "version": "0040_paper_trades",
        "sql": """
            -- Every trade idea traded on paper (shared/utils/paper_ledger.py):
            -- filled at the next session's open, costed both ways, exited at
            -- its stop or its horizon's last close, net of SPY.
            CREATE TABLE IF NOT EXISTS paper_trades (
                idea_day      DATE NOT NULL,
                horizon       TEXT NOT NULL,
                ticker        TEXT NOT NULL,
                direction     SMALLINT NOT NULL,
                idea_at       TIMESTAMPTZ NOT NULL,
                model         TEXT,
                stop_price    DOUBLE PRECISION,
                notional_usd  DOUBLE PRECISION NOT NULL,
                status        TEXT NOT NULL DEFAULT 'pending',
                entry_day     DATE,
                entry_price   DOUBLE PRECISION,
                spy_entry     DOUBLE PRECISION,
                cost_bps      DOUBLE PRECISION,
                adv_usd       DOUBLE PRECISION,
                exit_day      DATE,
                exit_price    DOUBLE PRECISION,
                spy_exit      DOUBLE PRECISION,
                exit_reason   TEXT,
                gross_excess  DOUBLE PRECISION,
                net_excess    DOUBLE PRECISION,
                pnl_usd       DOUBLE PRECISION,
                PRIMARY KEY (idea_day, horizon, ticker, direction)
            );
            CREATE INDEX IF NOT EXISTS paper_trades_status ON paper_trades (status, horizon);
        """,
        "transactional": True
    },
    {
        "version": "0041_paper_trades_vol_scale",
        "sql": """
            -- Volatility-managed sizing beside fixed size (shared/utils/paper_ledger.py).
            ALTER TABLE paper_trades
                ADD COLUMN IF NOT EXISTS scale DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS pnl_scaled_usd DOUBLE PRECISION;
        """,
        "transactional": True
    },
    {
        "version": "0042_model_portfolio",
        "sql": """
            -- The actively managed model portfolio (shared/utils/model_portfolio.py):
            -- its current books, every order with its reason, and its value
            -- each session beside SPY's.
            CREATE TABLE IF NOT EXISTS model_portfolio_state (
                portfolio       TEXT PRIMARY KEY,
                long_book       JSONB NOT NULL DEFAULT '[]',
                short_book      JSONB NOT NULL DEFAULT '[]',
                pending         JSONB,
                rebalanced_on   DATE,
                scores_as_of    DATE,
                marked_through  DATE,
                started_on      DATE,
                nav             DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                spy_nav         DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS model_portfolio_orders (
                portfolio   TEXT NOT NULL,
                decided_on  DATE NOT NULL,
                ticker      TEXT NOT NULL,
                action      TEXT NOT NULL,
                kind        TEXT NOT NULL,
                reason      TEXT NOT NULL,
                percentile  DOUBLE PRECISION,
                fill_day    DATE,
                fill_price  DOUBLE PRECISION,
                PRIMARY KEY (portfolio, decided_on, ticker, action)
            );
            CREATE TABLE IF NOT EXISTS model_portfolio_nav (
                portfolio   TEXT NOT NULL,
                day         DATE NOT NULL,
                nav         DOUBLE PRECISION NOT NULL,
                spy_nav     DOUBLE PRECISION NOT NULL,
                ret         DOUBLE PRECISION NOT NULL,
                spy_ret     DOUBLE PRECISION NOT NULL,
                long_ret    DOUBLE PRECISION,
                short_ret   DOUBLE PRECISION,
                cost        DOUBLE PRECISION,
                borrow      DOUBLE PRECISION,
                n_long      INTEGER,
                n_short     INTEGER,
                missing     INTEGER,
                PRIMARY KEY (portfolio, day)
            );
        """,
        "transactional": True
    },
    {
        "version": "0043_short_data",
        "sql": """
            -- Free short-side data (shared/utils/short_data.py): FINRA's daily
            -- off-exchange short sale volume (history from 2021), and Interactive
            -- Brokers' public borrow file, kept one snapshot a day from first run.
            CREATE TABLE IF NOT EXISTS finra_short_volume (
                day            DATE NOT NULL,
                ticker         TEXT NOT NULL,
                short_volume   REAL NOT NULL,
                exempt_volume  REAL,
                total_volume   REAL NOT NULL,
                PRIMARY KEY (day, ticker)
            );
            CREATE TABLE IF NOT EXISTS borrow_rates (
                day          DATE NOT NULL,
                ticker       TEXT NOT NULL,
                fee_rate     REAL,
                rebate_rate  REAL,
                available    BIGINT,
                taken_at     TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (day, ticker)
            );
        """,
        "transactional": True
    },
    {
        "version": "0044_earnings_estimates",
        "sql": """
            -- Finnhub's free earnings data, kept (services/collector-tradfi/
            -- earnings_estimates.py): the calendar's consensus snapshotted daily,
            -- so estimate revisions can be measured from our own copies, and
            -- reported quarters' actual against estimate.
            CREATE TABLE IF NOT EXISTS earnings_estimates (
                snap_day          DATE NOT NULL,
                ticker            TEXT NOT NULL,
                report_date       DATE NOT NULL,
                fiscal_year       INTEGER,
                fiscal_quarter    INTEGER,
                hour              TEXT,
                eps_estimate      DOUBLE PRECISION,
                revenue_estimate  DOUBLE PRECISION,
                eps_actual        DOUBLE PRECISION,
                revenue_actual    DOUBLE PRECISION,
                PRIMARY KEY (snap_day, ticker, report_date)
            );
            CREATE TABLE IF NOT EXISTS earnings_surprises (
                ticker        TEXT NOT NULL,
                period        DATE NOT NULL,
                eps_actual    DOUBLE PRECISION,
                eps_estimate  DOUBLE PRECISION,
                surprise_pct  DOUBLE PRECISION,
                first_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, period)
            );
        """,
        "transactional": True
    },
    {
        "version": "0045_civic_surveillance",
        "sql": """
            -- Watching the watchers (shared/utils/surveillance.py): mapped
            -- licence-plate readers and gunshot detectors from OpenStreetMap,
            -- and which agency uses which surveillance technology (EFF Atlas).
            CREATE TABLE IF NOT EXISTS surveillance_devices (
                osm_id        BIGINT PRIMARY KEY,
                lat           DOUBLE PRECISION NOT NULL,
                lon           DOUBLE PRECISION NOT NULL,
                kind          TEXT NOT NULL,
                manufacturer  TEXT,
                operator      TEXT,
                direction     TEXT,
                tags          JSONB NOT NULL DEFAULT '{}',
                first_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS surveillance_devices_geo ON surveillance_devices (lat, lon);
            CREATE TABLE IF NOT EXISTS surveillance_agency_tech (
                aos_id      TEXT PRIMARY KEY,
                agency      TEXT NOT NULL,
                city        TEXT,
                county      TEXT,
                state       TEXT,
                lea_type    TEXT,
                juris_type  TEXT,
                technology  TEXT NOT NULL,
                vendor      TEXT,
                summary     TEXT,
                link        TEXT,
                link_date   DATE,
                lat         DOUBLE PRECISION,
                lon         DOUBLE PRECISION,
                geo_level   TEXT,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS surveillance_agency_state ON surveillance_agency_tech (state, agency);
            CREATE INDEX IF NOT EXISTS surveillance_agency_geo ON surveillance_agency_tech (lat, lon);
        """,
        "transactional": True
    },
    {
        "version": "0046_civic_legal",
        "sql": """
            -- "Is it legal here?" (shared/utils/civic_law.py): each agency's
            -- published Flock practice (transparency portals via EyesOnFlock,
            -- CC BY-SA 4.0), ICE 287(g) agreements, and the findings that
            -- check one against the state's licence-plate-reader law.
            CREATE TABLE IF NOT EXISTS flock_portals (
                slug                 TEXT PRIMARY KEY,
                portal_url           TEXT,
                city                 TEXT,
                county               TEXT,
                state                TEXT,
                agency_type          TEXT,
                population           BIGINT,
                total_cameras        INTEGER,
                total_searches       BIGINT,
                data_retention       DOUBLE PRECISION,
                vehicles_captured    BIGINT,
                hotlist_hits         BIGINT,
                hotlist_hit_rate     DOUBLE PRECISION,
                shared_with_count    INTEGER,
                received_from_count  INTEGER,
                shared_with          JSONB NOT NULL DEFAULT '[]',
                prohibited_uses      TEXT,
                public_search_audit  BOOLEAN,
                data_last_updated    TEXT,
                lat                  DOUBLE PRECISION,
                lon                  DOUBLE PRECISION,
                updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS flock_portals_state ON flock_portals (state);
            CREATE TABLE IF NOT EXISTS ice_287g_agreements (
                state         TEXT NOT NULL,
                agency        TEXT NOT NULL,
                agency_type   TEXT,
                county        TEXT,
                support_type  TEXT NOT NULL,
                signed        DATE,
                moa           TEXT,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (state, agency, support_type)
            );
            CREATE TABLE IF NOT EXISTS civic_findings (
                slug        TEXT NOT NULL,
                code        TEXT NOT NULL,
                severity    TEXT NOT NULL,
                state       TEXT,
                agency      TEXT,
                title       TEXT NOT NULL,
                detail      TEXT NOT NULL,
                citation    TEXT,
                source      TEXT,
                evidence    JSONB NOT NULL DEFAULT '{}',
                first_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (slug, code)
            );
            CREATE INDEX IF NOT EXISTS civic_findings_state ON civic_findings (state, severity);
        """,
        "transactional": True
    },
    {
        "version": "0047_geo_places",
        "sql": """
            -- Census gazetteer centroids by state and normalised name (shared/utils/
            -- surveillance.py norm_place / norm_county), written by the Atlas
            -- pass so later passes place agencies with a lookup instead of
            -- re-parsing the gazetteers in a 160M service.
            CREATE TABLE IF NOT EXISTS geo_places (
                state  TEXT NOT NULL,
                norm   TEXT NOT NULL,
                kind   TEXT NOT NULL,
                lat    DOUBLE PRECISION NOT NULL,
                lon    DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (state, norm, kind)
            );
        """,
        "transactional": True
    },
    {
        "version": "0048_unwrap_double_encoded_jsonb",
        "sql": """
            -- The pool's jsonb codec already JSON-encodes parameters; values
            -- written as json.dumps(...) were stored as JSON *strings* holding
            -- the document, invisible to SQL operators. Unwrapped here for the
            -- tables added 9-10 Oct 2026, whose writers now pass Python values.
            -- (Older tables share the pattern and are left to their own fix.)
            UPDATE model_portfolio_state SET long_book = (long_book #>> '{}')::jsonb WHERE jsonb_typeof(long_book) = 'string';
            UPDATE model_portfolio_state SET short_book = (short_book #>> '{}')::jsonb WHERE jsonb_typeof(short_book) = 'string';
            UPDATE model_portfolio_state SET pending = (pending #>> '{}')::jsonb WHERE jsonb_typeof(pending) = 'string';
            UPDATE surveillance_devices SET tags = (tags #>> '{}')::jsonb WHERE jsonb_typeof(tags) = 'string';
            UPDATE flock_portals SET shared_with = (shared_with #>> '{}')::jsonb WHERE jsonb_typeof(shared_with) = 'string';
            UPDATE civic_findings SET evidence = (evidence #>> '{}')::jsonb WHERE jsonb_typeof(evidence) = 'string';
        """,
        "transactional": True
    },
    {
        "version": "0049_flock_audits_and_departures",
        "sql": """
            -- Per-agency summaries of released Flock search logs (EyesOnFlock):
            -- counts by reason category and matched term, never reason text.
            CREATE TABLE IF NOT EXISTS flock_audit_stats (
                slug                   TEXT PRIMARY KEY,
                days                   INTEGER NOT NULL,
                searches               INTEGER NOT NULL,
                first_search           TIMESTAMPTZ,
                last_search            TIMESTAMPTZ,
                immigration_hits       INTEGER NOT NULL DEFAULT 0,
                immigration_terms      JSONB NOT NULL DEFAULT '{}',
                first_amendment_hits   INTEGER NOT NULL DEFAULT 0,
                first_amendment_terms  JSONB NOT NULL DEFAULT '{}',
                wide_searches          INTEGER NOT NULL DEFAULT 0,
                no_reason              INTEGER NOT NULL DEFAULT 0,
                updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            -- A portal missing from a full pass is marked, not deleted: a
            -- likely contract cancellation (or a gap in the aggregator).
            ALTER TABLE flock_portals ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW();
            ALTER TABLE flock_portals ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;
        """,
        "transactional": True
    },
    {
        "version": "0050_drone_waivers_and_federal_money",
        "sql": """
            -- FAA drone-as-first-responder waivers (EFF's FOIA release, a public
            -- Google Sheet) and federal awards for surveillance (USAspending):
            -- contracts to named vendors, grants whose descriptions name a technology.
            CREATE TABLE IF NOT EXISTS drone_waivers (
                applicant    TEXT NOT NULL,
                city         TEXT,
                state        TEXT NOT NULL,
                category     TEXT,
                status       TEXT,
                received     DATE,
                completed    DATE,
                start_date   DATE,
                altitude     TEXT,
                lat          DOUBLE PRECISION,
                lon          DOUBLE PRECISION,
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (applicant, state)
            );
            CREATE TABLE IF NOT EXISTS federal_awards (
                award_id      TEXT PRIMARY KEY,
                kind          TEXT NOT NULL,          -- 'contract' or 'grant'
                matched       TEXT NOT NULL,          -- vendor name or technology keyword
                recipient     TEXT,
                amount        DOUBLE PRECISION,
                agency        TEXT,
                sub_agency    TEXT,
                start_date    DATE,
                description   TEXT,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS federal_awards_matched ON federal_awards (kind, matched);
        """,
        "transactional": True
    },
    {
        "version": "0051_data_brokers",
        "sql": """
            -- California's data broker registry (CPPA): what each broker
            -- declares it collects and who it sold data to, with its opt-out page.
            CREATE TABLE IF NOT EXISTS data_brokers (
                name         TEXT PRIMARY KEY,
                dba          TEXT,
                website      TEXT,
                privacy_url  TEXT,
                email        TEXT,
                city         TEXT,
                state        TEXT,
                flags        JSONB NOT NULL DEFAULT '{}',
                registry     TEXT NOT NULL DEFAULT 'CA',
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """,
        "transactional": True
    },
    {
        "version": "0052_unwrap_double_encoded_jsonb_platform",
        "sql": """
            -- The pool's jsonb codec JSON-encodes parameters; writers that also
            -- called json.dumps stored the document as a JSON *string*, which
            -- SQL's ->> cannot read (the dossier and macro engines' ticker and
            -- sector lookups returned NULL for 264k archived filings and
            -- earnings reactions). Writers fixed 10 Oct 2026; rows unwrapped
            -- here, a year at a time on the compressed hypertable. Left as is:
            -- audit_ledger.details (the hash chain covers the exact string) and
            -- the *_jsonb_backup_20260902 tables (backups).
            -- Compressed chunks: one year of archive exceeds the default cap on
            -- rows a statement may decompress; lifted for this session only.
            SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
            -- Only strings that parse as a JSON object or array are unwrapped;
            -- anything else (a genuinely textual payload) is left untouched.
            CREATE OR REPLACE FUNCTION sentinel_try_jsonb(t text) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $f$
            DECLARE j jsonb;
            BEGIN
                j := t::jsonb;
                IF jsonb_typeof(j) IN ('object', 'array') THEN RETURN j; END IF;
                RETURN NULL;
            EXCEPTION WHEN others THEN RETURN NULL;
            END $f$;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2015-01-01' AND occurred_at < '2016-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2016-01-01' AND occurred_at < '2017-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2017-01-01' AND occurred_at < '2018-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2018-01-01' AND occurred_at < '2019-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2019-01-01' AND occurred_at < '2020-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2020-01-01' AND occurred_at < '2021-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2021-01-01' AND occurred_at < '2022-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2022-01-01' AND occurred_at < '2023-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2023-01-01' AND occurred_at < '2024-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2024-01-01' AND occurred_at < '2025-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2025-01-01' AND occurred_at < '2026-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE signal_event_archive SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE occurred_at >= '2026-01-01' AND occurred_at < '2027-01-01' AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE events SET financial_data = sentinel_try_jsonb(financial_data #>> '{}')
                WHERE type = 'earnings_reaction' AND occurred_at >= '2026-09-01'
                  AND jsonb_typeof(financial_data) = 'string' AND sentinel_try_jsonb(financial_data #>> '{}') IS NOT NULL;
            UPDATE failed_events SET raw_payload = sentinel_try_jsonb(raw_payload #>> '{}') WHERE jsonb_typeof(raw_payload) = 'string' AND sentinel_try_jsonb(raw_payload #>> '{}') IS NOT NULL;
            UPDATE recommendation_log SET option_idea = sentinel_try_jsonb(option_idea #>> '{}') WHERE jsonb_typeof(option_idea) = 'string' AND sentinel_try_jsonb(option_idea #>> '{}') IS NOT NULL;
            UPDATE factor_model_runs SET report = sentinel_try_jsonb(report #>> '{}') WHERE jsonb_typeof(report) = 'string' AND sentinel_try_jsonb(report #>> '{}') IS NOT NULL;
            DROP FUNCTION IF EXISTS sentinel_try_jsonb(text);
        """,
        "transactional": False,
        "timeout": 1800
    },
    {
        "version": "0053_courts",
        "sql": """
            -- Surveillance in the federal courts (shared/utils/courts.py,
            -- CourtListener): the cases, the judges' disclosures, and disclosed
            -- holdings in surveillance vendors.
            CREATE TABLE IF NOT EXISTS court_cases (
                docket_id        BIGINT PRIMARY KEY,
                case_name        TEXT NOT NULL,
                court_id         TEXT,
                date_filed       DATE,
                date_terminated  DATE,
                judge            TEXT,
                judge_id         BIGINT,
                suit_nature      TEXT,
                docket_number    TEXT,
                url              TEXT NOT NULL,
                topics           TEXT[] NOT NULL DEFAULT '{}',
                first_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS court_cases_judge ON court_cases (judge_id);
            CREATE TABLE IF NOT EXISTS judge_disclosures (
                disclosure_id  BIGINT PRIMARY KEY,
                person_id      BIGINT NOT NULL,
                year           INTEGER
            );
            CREATE INDEX IF NOT EXISTS judge_disclosures_person ON judge_disclosures (person_id);
            CREATE TABLE IF NOT EXISTS judge_checked (
                person_id   BIGINT PRIMARY KEY,
                checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS vendor_holdings (
                investment_id     BIGINT PRIMARY KEY,
                disclosure_id     BIGINT NOT NULL,
                vendor            TEXT NOT NULL,
                description       TEXT,
                gross_value_code  TEXT,
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS vendor_holdings_disclosure ON vendor_holdings (disclosure_id);
        """,
        "transactional": True
    },
    {
        "version": "0054_lobbying",
        "sql": """
            -- Federal lobbying by surveillance vendors (shared/utils/lobbying.py, lda.gov).
            CREATE TABLE IF NOT EXISTS lobbying_filings (
                filing_uuid   TEXT PRIMARY KEY,
                vendor        TEXT NOT NULL,
                client_name   TEXT,
                registrant    TEXT,
                year          INTEGER,
                period        TEXT,
                filing_type   TEXT,
                amount        DOUBLE PRECISION,
                in_house      BOOLEAN NOT NULL DEFAULT FALSE,
                issues        TEXT[] NOT NULL DEFAULT '{}',
                entities      TEXT[] NOT NULL DEFAULT '{}',
                description   TEXT,
                url           TEXT,
                posted        DATE,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS lobbying_vendor_year ON lobbying_filings (vendor, year);
        """,
        "transactional": True
    },
    {
        "version": "0055_tradfi_bars_source",
        "sql": """
            -- Where a bar came from, apart from which session it belongs to.
            -- Vendor rows were written with session = 'vendor_backfill', so a
            -- backfilled 06:59 bar could not say it was pre-market, and a
            -- reader could not tell an IEX bar from a consolidated one.
            ALTER TABLE tradfi_bars ADD COLUMN IF NOT EXISTS source TEXT;
        """,
        "transactional": False,
        "timeout": 600
    },
    {
        "version": "0056_factor_scores_and_ic",
        "sql": """
            -- Each night's factor scores, kept, and their grade once the horizon
            -- has passed (shared/utils/ic_monitor.py): the live IC beside the
            -- IC the walk-forward promised.
            CREATE TABLE IF NOT EXISTS factor_scores_daily (
                as_of       DATE NOT NULL,
                horizon     TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                percentile  DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (as_of, horizon, ticker)
            );
            CREATE TABLE IF NOT EXISTS factor_ic (
                as_of       DATE NOT NULL,
                horizon     TEXT NOT NULL,
                ic          DOUBLE PRECISION,
                n           INTEGER NOT NULL,
                end_day     DATE,
                graded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (as_of, horizon)
            );
        """,
        "transactional": True
    },
    {
        "version": "0057_form144_and_eia",
        "sql": """
            -- Insiders' notices of proposed sales (shared/utils/form144.py).
            CREATE TABLE IF NOT EXISTS form144_notices (
                accession         TEXT PRIMARY KEY,
                issuer_cik        TEXT,
                ticker            TEXT,
                issuer_name       TEXT,
                seller            TEXT,
                relationship      TEXT,
                security          TEXT,
                shares            DOUBLE PRECISION,
                market_value      DOUBLE PRECISION,
                pct_outstanding   DOUBLE PRECISION,
                approx_sale_date  DATE,
                acquired_how      TEXT,
                notice_date       DATE,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS form144_ticker ON form144_notices (ticker, notice_date DESC);
            -- EIA's weekly petroleum stocks (shared/utils/eia.py): one row per
            -- series per week, in thousand barrels.
            CREATE TABLE IF NOT EXISTS eia_weekly (
                series       TEXT NOT NULL,
                week_ending  DATE NOT NULL,
                value        DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (series, week_ending)
            );
        """,
        "transactional": True
    },
    {
        "version": "0058_macro_first_release",
        "sql": """
            -- Macro series as first published (FRED output_type=4): point in
            -- time, before the revisions today's history carries.
            CREATE TABLE IF NOT EXISTS macro_first_release (
                series_id       TEXT NOT NULL,
                period          DATE NOT NULL,
                value           DOUBLE PRECISION NOT NULL,
                realtime_start  DATE,
                PRIMARY KEY (series_id, period)
            );
        """,
        "transactional": True
    },
    {
        "version": "0059_vendor_shares",
        "sql": """
            -- Shares outstanding from Finnhub for ranked stocks the SEC frames
            -- leave out (services/collector-filings/vendor_shares.py).
            CREATE TABLE IF NOT EXISTS vendor_shares (
                ticker   TEXT NOT NULL,
                as_of    DATE NOT NULL,
                shares   DOUBLE PRECISION NOT NULL,
                source   TEXT NOT NULL,
                PRIMARY KEY (ticker, as_of)
            );
        """,
        "transactional": True
    },
    {
        "version": "0060_overlay_sizing",
        "sql": """
            -- The model portfolio's overlay size and SPY core weight, decided
            -- each night (volatility target, equity-regime gauge, beta hedge),
            -- and what each marked session used.
            ALTER TABLE model_portfolio_state ADD COLUMN IF NOT EXISTS sizing JSONB;
            ALTER TABLE model_portfolio_nav ADD COLUMN IF NOT EXISTS gross DOUBLE PRECISION;
            ALTER TABLE model_portfolio_nav ADD COLUMN IF NOT EXISTS core DOUBLE PRECISION;
        """,
        "transactional": True
    },
    {
        "version": "0061_insider_transactions",
        "sql": """
            -- Insider open-market trades from the SEC's quarterly Form 3/4/5
            -- data sets, each classified routine or opportunistic
            -- (shared/utils/insider_history.py).
            CREATE TABLE IF NOT EXISTS insider_transactions (
                accession      TEXT NOT NULL,
                line           TEXT NOT NULL,
                owner_cik      TEXT NOT NULL,
                filed          DATE NOT NULL,
                traded         DATE NOT NULL,
                issuer_cik     TEXT,
                ticker         TEXT NOT NULL,
                owner_name     TEXT,
                relationship   TEXT,
                title          TEXT,
                code           TEXT NOT NULL,
                shares         DOUBLE PRECISION,
                price          DOUBLE PRECISION,
                planned        BOOLEAN NOT NULL DEFAULT FALSE,
                insider_class  TEXT,
                PRIMARY KEY (accession, line, owner_cik)
            );
            CREATE INDEX IF NOT EXISTS insider_transactions_owner ON insider_transactions (owner_cik, traded);
            CREATE INDEX IF NOT EXISTS insider_transactions_ticker ON insider_transactions (ticker, filed);
        """,
        "transactional": True
    },
    {
        "version": "0062_stock_news",
        "sql": """
            -- Company news from Alpaca's feed (Benzinga), tagged with its
            -- stocks, and FinBERT's tone for each headline
            -- (shared/utils/stock_news.py).
            CREATE TABLE IF NOT EXISTS stock_news (
                id          BIGINT PRIMARY KEY,
                created_at  TIMESTAMPTZ NOT NULL,
                symbols     TEXT[] NOT NULL DEFAULT '{}',
                headline    TEXT NOT NULL,
                summary     TEXT,
                source      TEXT,
                url         TEXT,
                positive    REAL,
                negative    REAL,
                neutral     REAL,
                scored_at   TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS stock_news_created ON stock_news (created_at DESC);
            CREATE INDEX IF NOT EXISTS stock_news_symbols ON stock_news USING GIN (symbols);
            CREATE INDEX IF NOT EXISTS stock_news_unscored ON stock_news (created_at DESC) WHERE scored_at IS NULL;
        """,
        "transactional": True
    },
    {
        "version": "0063_option_idea_grades",
        "sql": """
            -- Each logged option idea's result at expiry: profit or loss per
            -- contract from the stock's close, and on the capital at risk.
            ALTER TABLE recommendation_log ADD COLUMN IF NOT EXISTS option_pnl DOUBLE PRECISION;
            ALTER TABLE recommendation_log ADD COLUMN IF NOT EXISTS option_return_on_risk DOUBLE PRECISION;
            ALTER TABLE recommendation_log ADD COLUMN IF NOT EXISTS option_graded_at TIMESTAMPTZ;
        """,
        "transactional": True
    },
    {
        "version": "0064_option_iv_history",
        "sql": """
            -- Option-implied measures rebuilt from daily option bars
            -- (shared/utils/option_iv.py), apart from the live option_daily
            -- so a study never mixes the two definitions.
            CREATE TABLE IF NOT EXISTS option_iv_history (
                ticker            TEXT NOT NULL,
                day               DATE NOT NULL,
                atm_iv            DOUBLE PRECISION,
                iv_skew           DOUBLE PRECISION,
                skew_expiry_days  INTEGER,
                cp_iv_spread      DOUBLE PRECISION,
                cp_pairs          INTEGER,
                call_volume       DOUBLE PRECISION,
                put_volume        DOUBLE PRECISION,
                put_call_ratio    DOUBLE PRECISION,
                contracts         INTEGER,
                spot              DOUBLE PRECISION,
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, day)
            );
            CREATE INDEX IF NOT EXISTS option_iv_history_day ON option_iv_history (day);
        """,
        "transactional": True
    },
    {
        "version": "0065_news_embed",
        "sql": """
            -- Each stock's company-news embeddings summed per news day
            -- (shared/utils/news_embed.py): 768 float16 values packed in v.
            -- The monthly refit reads it; the live scorer adds to it.
            CREATE TABLE IF NOT EXISTS news_embed_daily (
                day     DATE NOT NULL,
                ticker  TEXT NOT NULL,
                n       INTEGER NOT NULL,
                v       BYTEA NOT NULL,
                PRIMARY KEY (day, ticker)
            );
            -- The walk-forward's out-of-sample score for each stock and
            -- session, from a model trained only on earlier months.
            CREATE TABLE IF NOT EXISTS news_embed_oos (
                day       DATE NOT NULL,
                ticker    TEXT NOT NULL,
                score     DOUBLE PRECISION NOT NULL,
                fitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (day, ticker)
            );
        """,
        "transactional": True
    },
    {
        "version": "0066_issuer_kind",
        "sql": """
            -- What kind of issuer stands behind a ticker, from its EDGAR
            -- submissions: a domestic operating company, a foreign private
            -- issuer (annual report on 20-F or 40-F: traded here as ADRs or
            -- foreign ordinary shares), or a partnership (L.P. units).
            ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS issuer_kind TEXT;
            ALTER TABLE company_profile ADD COLUMN IF NOT EXISTS state_of_incorporation TEXT;
        """,
        "transactional": True
    },
    {
        "version": "0067_user_preferences",
        "sql": """
            -- Each signed-in person's own settings: the stocks they follow, their
            -- town, and what home leads with. Keyed by the gateway-verified
            -- identity (the session's email), one row per person.
            CREATE TABLE IF NOT EXISTS user_preferences (
                owner       TEXT PRIMARY KEY,
                prefs       JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """,
        "transactional": True
    },
    {
        "version": "0068_auto_trade_orders",
        "sql": """
            -- Every order the auto-trader (services/agents/auto_trader.py) sent to
            -- the paper account to mirror the model portfolio, with the venue's
            -- answer. Also the record of which names it manages: it never trades
            -- a position it did not open.
            CREATE TABLE IF NOT EXISTS auto_trade_orders (
                id            BIGSERIAL PRIMARY KEY,
                day           DATE NOT NULL,
                portfolio     TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                side          TEXT NOT NULL,
                qty           DOUBLE PRECISION NOT NULL,
                reason        TEXT NOT NULL,
                order_id      TEXT,
                status        TEXT NOT NULL,
                error         TEXT,
                submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS auto_trade_orders_day ON auto_trade_orders (portfolio, day);
        """,
        "transactional": True
    },
    {
        "version": "0069_tick_trades_quotes",
        "sql": """
            -- Every trade and top-of-book change for the names the portfolio
            -- holds or is about to trade (shared/utils/live_focus.py), from the
            -- free IEX stream: one venue's prints, a few percent of the tape,
            -- but tick by tick. Compressed after a day, kept 90 days -- enough
            -- to study order flow around the book's own trades.
            CREATE TABLE IF NOT EXISTS tick_trades (
                ticker      TEXT NOT NULL,
                time        TIMESTAMPTZ NOT NULL,
                price       DOUBLE PRECISION NOT NULL,
                size        DOUBLE PRECISION NOT NULL,
                trade_id    BIGINT,
                exchange    TEXT,
                conditions  TEXT[],
                source      TEXT NOT NULL
            );
            SELECT create_hypertable('tick_trades', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS tick_trades_ticker_time ON tick_trades (ticker, time DESC);
            ALTER TABLE tick_trades SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'ticker',
                timescaledb.compress_orderby = 'time DESC'
            );
            SELECT add_compression_policy('tick_trades', INTERVAL '1 day', if_not_exists => TRUE);
            SELECT add_retention_policy('tick_trades', INTERVAL '90 days', if_not_exists => TRUE);

            CREATE TABLE IF NOT EXISTS tick_quotes (
                ticker      TEXT NOT NULL,
                time        TIMESTAMPTZ NOT NULL,
                bid         DOUBLE PRECISION,
                bid_size    DOUBLE PRECISION,
                ask         DOUBLE PRECISION,
                ask_size    DOUBLE PRECISION,
                source      TEXT NOT NULL
            );
            SELECT create_hypertable('tick_quotes', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS tick_quotes_ticker_time ON tick_quotes (ticker, time DESC);
            ALTER TABLE tick_quotes SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'ticker',
                timescaledb.compress_orderby = 'time DESC'
            );
            SELECT add_compression_policy('tick_quotes', INTERVAL '1 day', if_not_exists => TRUE);
            SELECT add_retention_policy('tick_quotes', INTERVAL '90 days', if_not_exists => TRUE);
        """,
        "transactional": False
    },
    {
        "version": "0070_option_ticks",
        "sql": """
            -- Quotes and trades on the options of the live focus, from the free
            -- plan's indicative option stream (200 contracts; not OPRA, so
            -- prices are indicative, not executable). Compressed after a day,
            -- kept 90 days.
            CREATE TABLE IF NOT EXISTS option_ticks (
                contract    TEXT NOT NULL,
                underlying  TEXT NOT NULL,
                time        TIMESTAMPTZ NOT NULL,
                kind        TEXT NOT NULL,
                bid         DOUBLE PRECISION,
                bid_size    DOUBLE PRECISION,
                ask         DOUBLE PRECISION,
                ask_size    DOUBLE PRECISION,
                price       DOUBLE PRECISION,
                size        DOUBLE PRECISION,
                source      TEXT NOT NULL
            );
            SELECT create_hypertable('option_ticks', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS option_ticks_underlying_time ON option_ticks (underlying, time DESC);
            ALTER TABLE option_ticks SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'underlying',
                timescaledb.compress_orderby = 'time DESC'
            );
            SELECT add_compression_policy('option_ticks', INTERVAL '1 day', if_not_exists => TRUE);
            SELECT add_retention_policy('option_ticks', INTERVAL '90 days', if_not_exists => TRUE);
        """,
        "transactional": False
    },
    {
        "version": "0071_option_chain_daily",
        "sql": """
            -- Each evening's option chain per stock, summarised
            -- (shared/utils/option_chain.py) from Cboe's free delayed chains:
            -- volume and open interest by side, Cboe's 30-day IV, ATM IV and
            -- 25-delta skew. Open interest day over day shows positions being
            -- opened, which Alpaca's option bars cannot.
            CREATE TABLE IF NOT EXISTS option_chain_daily (
                ticker       TEXT NOT NULL,
                day          DATE NOT NULL,
                call_volume  DOUBLE PRECISION,
                put_volume   DOUBLE PRECISION,
                call_oi      DOUBLE PRECISION,
                put_oi       DOUBLE PRECISION,
                pcr_volume   DOUBLE PRECISION,
                pcr_oi       DOUBLE PRECISION,
                iv30         DOUBLE PRECISION,
                atm_iv       DOUBLE PRECISION,
                skew_25d     DOUBLE PRECISION,
                expiry_days  INTEGER,
                contracts    INTEGER,
                spot         DOUBLE PRECISION,
                source       TEXT NOT NULL DEFAULT 'cboe_delayed',
                fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, day)
            );
        """,
        "transactional": True
    },
    {
        "version": "0072_commodity_prices_and_stock_features",
        "sql": """
            -- Daily prices of what companies make and buy: futures, memory-chip
            -- spot prices, GPU rental rates (services/collector-tradfi/commodities.py).
            CREATE TABLE IF NOT EXISTS commodity_prices (
                series  TEXT NOT NULL,
                day     DATE NOT NULL,
                value   DOUBLE PRECISION NOT NULL,
                unit    TEXT,
                source  TEXT NOT NULL,
                PRIMARY KEY (series, day)
            );

            -- One table every domain writes per-stock, per-day values into:
            -- the integration contract between the platform's domains and the
            -- stock model. Point in time (a value is dated the day it became
            -- known); read by the model only through a pre-registered trial.
            CREATE TABLE IF NOT EXISTS stock_features_daily (
                ticker   TEXT NOT NULL,
                day      DATE NOT NULL,
                feature  TEXT NOT NULL,
                value    DOUBLE PRECISION,
                source   TEXT NOT NULL,
                PRIMARY KEY (ticker, day, feature)
            );
            CREATE INDEX IF NOT EXISTS stock_features_feature_day ON stock_features_daily (feature, day);
        """,
        "transactional": True
    },
    {
        "version": "0073_macro_indices_and_customer_links",
        "sql": """
            -- Published risk indices, daily or monthly (a monthly value is dated
            -- the first of its month): the Caldara-Iacoviello geopolitical risk
            -- index and its country series (services/collector-tradfi/commodities.py).
            CREATE TABLE IF NOT EXISTS macro_indices (
                series  TEXT NOT NULL,
                day     DATE NOT NULL,
                value   DOUBLE PRECISION NOT NULL,
                source  TEXT NOT NULL,
                PRIMARY KEY (series, day)
            );

            -- A supplier's major customers as its 10-K names them (Regulation
            -- S-K requires any customer over 10% of revenue). Dated by the
            -- filing, so reading it forward is point in time.
            CREATE TABLE IF NOT EXISTS customer_links (
                supplier         TEXT NOT NULL,
                customer_name    TEXT NOT NULL,
                customer_ticker  TEXT,
                pct              DOUBLE PRECISION,
                filed            DATE NOT NULL,
                accession        TEXT NOT NULL,
                method           TEXT NOT NULL,
                PRIMARY KEY (supplier, accession, customer_name)
            );
            CREATE INDEX IF NOT EXISTS customer_links_customer ON customer_links (customer_ticker);
        """,
        "transactional": True
    },
    {
        "version": "0074_stock_news_llm",
        "sql": """
            -- A language model's read of each single-stock headline: good (1),
            -- bad (-1) or unclear (0) for the price (services/agents/llm_news_job.py).
            -- Scored forward only, as headlines arrive, with the model and its
            -- training cutoff on every row: a model may remember what prices did
            -- before its cutoff, so only headlines after it can test the score.
            CREATE TABLE IF NOT EXISTS stock_news_llm (
                news_id     BIGINT NOT NULL,
                ticker      TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL,
                score       SMALLINT NOT NULL,
                model       TEXT NOT NULL,
                cutoff      DATE,
                scored_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (news_id, model)
            );
            CREATE INDEX IF NOT EXISTS stock_news_llm_ticker_time ON stock_news_llm (ticker, created_at);
        """,
        "transactional": True
    },
    {
        "version": "0075_auto_trade_equity",
        "sql": """
            -- The traded book's equity and SPY's close once a session, for the
            -- auto-trader's halts: day loss, drawdown, and drift against SPY.
            CREATE TABLE IF NOT EXISTS auto_trade_equity (
                day        DATE NOT NULL,
                portfolio  TEXT NOT NULL,
                equity     DOUBLE PRECISION NOT NULL,
                spy_close  DOUBLE PRECISION,
                PRIMARY KEY (day, portfolio)
            );
        """,
        "transactional": True
    },
    {
        "version": "0076_session_aligned_rollups",
        "sql": """
            -- Daily, weekly and monthly bars of the regular session on the New
            -- York calendar, and 4-hour bars on New York hours. These bucketed
            -- by UTC day over every session: measured 29 Oct 2026 against SIP
            -- daily bars, the 1d view's close was off by more than 0.2% on 45%
            -- of stock-days, its open on 59% and its range outside SIP's on
            -- 38%; over the regular session from the same minutes, 8%, and the
            -- range 0.9%. 09:30-16:00 inclusive by clock, not by the session
            -- label: the 16:00 minute carries the closing auction and is
            -- labelled AFTER_HOURS. The month view bucketed 30 days, not
            -- calendar months. Nothing depends on these views.
            DROP MATERIALIZED VIEW IF EXISTS tradfi_bars_1d CASCADE;
            DROP MATERIALIZED VIEW IF EXISTS tradfi_bars_1w CASCADE;
            DROP MATERIALIZED VIEW IF EXISTS tradfi_bars_1mth CASCADE;
            DROP MATERIALIZED VIEW IF EXISTS tradfi_bars_4h CASCADE;

            CREATE MATERIALIZED VIEW tradfi_bars_1d WITH (timescaledb.continuous) AS
            SELECT ticker, time_bucket('1 day', time, 'America/New_York') AS bucket_time,
                   FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low,
                   LAST(close, time) AS close, SUM(volume) AS volume
            FROM tradfi_bars WHERE (time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
            GROUP BY ticker, time_bucket('1 day', time, 'America/New_York') WITH NO DATA;
            ALTER MATERIALIZED VIEW tradfi_bars_1d SET (timescaledb.materialized_only = false);
            SELECT add_continuous_aggregate_policy('tradfi_bars_1d', start_offset => INTERVAL '30 days',
                end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day', if_not_exists => TRUE);

            CREATE MATERIALIZED VIEW tradfi_bars_1w WITH (timescaledb.continuous) AS
            SELECT ticker, time_bucket('1 week', time, 'America/New_York') AS bucket_time,
                   FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low,
                   LAST(close, time) AS close, SUM(volume) AS volume
            FROM tradfi_bars WHERE (time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
            GROUP BY ticker, time_bucket('1 week', time, 'America/New_York') WITH NO DATA;
            SELECT add_continuous_aggregate_policy('tradfi_bars_1w', start_offset => INTERVAL '180 days',
                end_offset => INTERVAL '1 week', schedule_interval => INTERVAL '1 day', if_not_exists => TRUE);

            CREATE MATERIALIZED VIEW tradfi_bars_1mth WITH (timescaledb.continuous) AS
            SELECT ticker, time_bucket('1 month', time, 'America/New_York') AS bucket_time,
                   FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low,
                   LAST(close, time) AS close, SUM(volume) AS volume
            FROM tradfi_bars WHERE (time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
            GROUP BY ticker, time_bucket('1 month', time, 'America/New_York') WITH NO DATA;
            SELECT add_continuous_aggregate_policy('tradfi_bars_1mth', start_offset => INTERVAL '365 days',
                end_offset => INTERVAL '1 month', schedule_interval => INTERVAL '1 day', if_not_exists => TRUE);

            CREATE MATERIALIZED VIEW tradfi_bars_4h WITH (timescaledb.continuous) AS
            SELECT ticker, time_bucket('4 hours', time, 'America/New_York') AS bucket_time,
                   FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low,
                   LAST(close, time) AS close, SUM(volume) AS volume
            FROM tradfi_bars
            GROUP BY ticker, time_bucket('4 hours', time, 'America/New_York') WITH NO DATA;
            SELECT add_continuous_aggregate_policy('tradfi_bars_4h', start_offset => INTERVAL '30 days',
                end_offset => INTERVAL '4 hours', schedule_interval => INTERVAL '4 hours', if_not_exists => TRUE);

            CALL refresh_continuous_aggregate('tradfi_bars_1d', NULL, NOW() - INTERVAL '1 day');
            CALL refresh_continuous_aggregate('tradfi_bars_1w', NULL, NOW() - INTERVAL '1 week');
            CALL refresh_continuous_aggregate('tradfi_bars_1mth', NULL, NOW() - INTERVAL '1 month');
            CALL refresh_continuous_aggregate('tradfi_bars_4h', NULL, NOW() - INTERVAL '4 hours');
        """,
        "transactional": False
    },
    {
        "version": "0077_macro_bars_and_sympathy_peers",
        "sql": """
            -- Daily closes for macro instruments the stock bars never carry:
            -- Treasury par yields (US02Y, US10Y, US30Y, from Treasury.gov) and
            -- futures, the dollar and VIX (Yahoo's chart API). Exposure edges
            -- point at 11 futures and only CL=F and GC=F had any stored bars
            -- (9 sessions each); US10Y had none anywhere. A yield's close is
            -- the yield in percent, not a price.
            CREATE TABLE IF NOT EXISTS macro_daily_bars (
                symbol  TEXT NOT NULL,
                day     DATE NOT NULL,
                close   DOUBLE PRECISION NOT NULL,
                source  TEXT NOT NULL,
                PRIMARY KEY (symbol, day)
            );
            -- Measured sympathy peers (shared/utils/sympathy.py): stocks whose
            -- daily moves net of SPY tracked each other over 120 sessions.
            -- Rebuilt nightly; the stock correlation agent picks movers only
            -- from here, where the model used to name them.
            CREATE TABLE IF NOT EXISTS sympathy_peers (
                ticker  TEXT NOT NULL,
                peer    TEXT NOT NULL,
                corr    DOUBLE PRECISION NOT NULL,
                n       INTEGER NOT NULL,
                as_of   DATE NOT NULL,
                PRIMARY KEY (ticker, peer)
            );
        """,
        "transactional": True
    },
    {
        "version": "0078_macro_exposures",
        "sql": """
            -- Measured exposures of stocks to macro instruments
            -- (shared/utils/macro_exposure.py): the instrument's term in a
            -- daily regression with the market, |t| >= 3 over 252 sessions.
            -- The graph's exposure edges were sector priors at weight 1.0;
            -- the stock correlation agent reads these instead.
            CREATE TABLE IF NOT EXISTS macro_exposures (
                ticker        TEXT NOT NULL,
                macro         TEXT NOT NULL,
                beta          DOUBLE PRECISION NOT NULL,
                t             DOUBLE PRECISION NOT NULL,
                partial_corr  DOUBLE PRECISION NOT NULL,
                n             INTEGER NOT NULL,
                as_of         DATE NOT NULL,
                PRIMARY KEY (ticker, macro)
            );
        """,
        "transactional": True
    },
    {
        "version": "0079_headline_event_type",
        "sql": """
            -- The kind of event each scored headline reports, from a fixed list
            -- (shared/utils/llm_news.EVENT_TYPES), read in the same call as its
            -- good/bad score: return effects differ by event type (Li, Qiao &
            -- Zheng 2025). Null for headlines scored before 30 Oct 2026.
            ALTER TABLE stock_news_llm ADD COLUMN IF NOT EXISTS event_type TEXT;
        """,
        "transactional": True
    },
    {
        "version": "0080_text_peers",
        "sql": """
            -- Peers from 10-K business descriptions (shared/utils/text_peers.py),
            -- after Hoberg & Phillips' text-based industries; rebuilt nightly.
            CREATE TABLE IF NOT EXISTS text_peers (
                ticker  TEXT NOT NULL,
                peer    TEXT NOT NULL,
                sim     DOUBLE PRECISION NOT NULL,
                as_of   DATE NOT NULL,
                PRIMARY KEY (ticker, peer)
            );
        """,
        "transactional": True
    }
]

async def apply_migrations():
    db = await get_timescale()

    # 1. Ensure the schema_migrations tracking table exists
    await db.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR(255) PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)

    # 2. Fetch already applied migrations
    rows = await db.query("SELECT version FROM schema_migrations;")
    applied_versions = {row["version"] for row in rows}

    logger.info("Checking database migration status...")

    # 3. Apply pending migrations sequentially
    for migration in MIGRATIONS:
        version_name = migration["version"]
        sql_script = migration["sql"]
        is_transactional = migration.get("transactional", True)

        if version_name in applied_versions:
            logger.info(f"Migration {version_name} is already applied. Skipping.")
            continue

        logger.info(f"Applying pending migration: {version_name} (transactional={is_transactional})...")

        if not sql_script:
            # Mark migrations with no SQL script (like initial setup) as applied
            await db.execute(
                "INSERT INTO schema_migrations (version) VALUES ($1);", 
                version_name
            )
            logger.info(f"Registered baseline migration {version_name} successfully.")
            continue

        # Execute migration script in transaction or direct execution path
        try:
            if is_transactional:
                await db.execute(sql_script)
            else:
                await db.execute_without_transaction(
                    sql_script, timeout=migration.get("timeout")
                )

            await db.execute(
                "INSERT INTO schema_migrations (version) VALUES ($1);", 
                version_name
            )
            logger.info(f"✅ Migration {version_name} applied successfully.")
        except Exception as e:
            logger.error(f"🚨 Migration failed for {version_name} | Error: {e}", exc_info=True)
            raise

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(apply_migrations())