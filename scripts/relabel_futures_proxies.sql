-- Move ETF-proxy prices stored under futures symbols to the ETF symbols they are.
--
-- Until 15 Oct 2026 the macro collector quoted futures through ETF proxies and
-- stored the ETF's price under the futures symbol: CL=F held USO ($153 against
-- crude at $96.60), GC=F held GLD, ES=F held SPY, NQ=F held QQQ. Real contract
-- prices replace them from now on (MACRO_REAL_FUTURES=1, collector-macro). The
-- old rows move to the proxy's own symbol, with source 'macro_proxy', so no
-- price is lost and no futures series jumps between the two levels.
--
-- Rows the ETF already has at the same instant are kept on the ETF side.
-- Backed up first; to undo:
--   DELETE FROM tradfi_bars t USING tradfi_bars_futures_proxy_backup_20261015 b
--     WHERE t.ticker = b.proxy AND t.time = b.time AND t.source = 'macro_proxy';
--   INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session, source)
--     SELECT ticker, time, open, high, low, close, volume, session, source
--     FROM tradfi_bars_futures_proxy_backup_20261015 ON CONFLICT DO NOTHING;
--
-- Run, then switch real futures on:
--   docker exec -i sentinel-timescaledb psql -U sentinel -d sentinel -v ON_ERROR_STOP=1 < scripts/relabel_futures_proxies.sql
--   (add MACRO_REAL_FUTURES=1 to .env)  docker compose up -d --force-recreate --no-build collector-macro

SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
SET statement_timeout = 0;

BEGIN;

CREATE TEMP TABLE proxy_map (fut text PRIMARY KEY, proxy text NOT NULL);
INSERT INTO proxy_map VALUES
  ('CL=F', 'USO'), ('BZ=F', 'BNO'), ('NG=F', 'UNG'), ('GC=F', 'GLD'), ('SI=F', 'SLV'),
  ('ZC=F', 'CORN'), ('ZW=F', 'WEAT'), ('NQ=F', 'QQQ'), ('ES=F', 'SPY');

CREATE TABLE tradfi_bars_futures_proxy_backup_20261015 AS
SELECT t.*, m.proxy FROM tradfi_bars t JOIN proxy_map m ON m.fut = t.ticker;

SELECT ticker, count(*) AS rows_moving FROM tradfi_bars_futures_proxy_backup_20261015 GROUP BY 1 ORDER BY 1;

INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session, source)
SELECT proxy, time, open, high, low, close, volume, session, 'macro_proxy'
FROM tradfi_bars_futures_proxy_backup_20261015
ON CONFLICT (ticker, time) DO NOTHING;

DELETE FROM tradfi_bars t USING proxy_map m WHERE t.ticker = m.fut;

COMMIT;

CALL refresh_continuous_aggregate('tradfi_bars_5m',  '2026-08-21', now() - INTERVAL '5 minutes');
CALL refresh_continuous_aggregate('tradfi_bars_15m', '2026-08-21', now() - INTERVAL '15 minutes');
CALL refresh_continuous_aggregate('tradfi_bars_30m', '2026-08-21', now() - INTERVAL '30 minutes');
CALL refresh_continuous_aggregate('tradfi_bars_1h',  '2026-08-21', now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('tradfi_bars_4h',  '2026-08-21', now() - INTERVAL '4 hours');
CALL refresh_continuous_aggregate('tradfi_bars_1d',  '2026-08-21', now() - INTERVAL '1 day');
