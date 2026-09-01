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

    logger.info(f"Checking database migration status...")

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
                await db.execute_without_transaction(sql_script)

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