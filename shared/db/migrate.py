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