-- Remove live minute-bar rows that duplicate a vendor (SIP/IEX backfill) bar.
--
-- Until 15 Oct 2026 the live aggregator stamped bars at flush time
-- (09:29:14.385) instead of the minute they cover, so a vendor bar for the same
-- minute (09:29:00) was stored beside it and every rollup summed the minute
-- twice. A live row is removed only when vendor bars exist for BOTH minutes its
-- trades can fall in (its own minute and the one before), so nothing it held is
-- lost. Measured on 15 Oct: 108,900 rows across 679 tickers, 24 Aug - 14 Oct.
--
-- The rows are copied to a backup table first; to undo:
--   INSERT INTO tradfi_bars SELECT * FROM tradfi_bars_dupes_backup_20261015;
--
-- Run:
--   docker exec -i sentinel-timescaledb psql -U sentinel -d sentinel -v ON_ERROR_STOP=1 < scripts/dedupe_tradfi_bars.sql

SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
SET statement_timeout = 0;

BEGIN;

CREATE TABLE tradfi_bars_dupes_backup_20261015 AS
SELECT l.* FROM tradfi_bars l
WHERE l.session <> 'vendor_backfill'
  AND l.time <> date_trunc('minute', l.time)
  AND EXISTS (SELECT 1 FROM tradfi_bars v
              WHERE v.ticker = l.ticker AND v.session = 'vendor_backfill'
                AND v.time = date_trunc('minute', l.time))
  AND EXISTS (SELECT 1 FROM tradfi_bars v
              WHERE v.ticker = l.ticker AND v.session = 'vendor_backfill'
                AND v.time = date_trunc('minute', l.time) - INTERVAL '1 minute');

SELECT count(*) AS backed_up FROM tradfi_bars_dupes_backup_20261015;

DELETE FROM tradfi_bars t
USING tradfi_bars_dupes_backup_20261015 b
WHERE t.ticker = b.ticker AND t.time = b.time AND t.session = b.session;

COMMIT;

-- Rebuild the rollups over the cleaned window.
CALL refresh_continuous_aggregate('tradfi_bars_5m',   '2026-08-23', now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('tradfi_bars_15m',  '2026-08-23', now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('tradfi_bars_30m',  '2026-08-23', now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('tradfi_bars_1h',   '2026-08-23', now() - INTERVAL '2 hours');
CALL refresh_continuous_aggregate('tradfi_bars_4h',   '2026-08-23', now() - INTERVAL '4 hours');
CALL refresh_continuous_aggregate('tradfi_bars_1d',   '2026-08-23', now() - INTERVAL '1 day');
