"""Bar quality fixes (29 Oct 2026), from a backtest against SIP daily bars:
Redis 1d candles had closes off by more than 0.2% on 50% of stock-days and
0.3% of real volume; the 1d aggregate 45%, because both cut days at midnight
UTC over every session; live bars could overwrite settled SIP minutes; and 118
streamed-only stocks were never settled."""
import importlib.util
import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _src(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _bucket_fn():
    """candle_bucket_start, pulled out of the collector without importing it whole."""
    src = _src("services/collector-tradfi/main.py")
    body = src[src.index("NY_TZ = ZoneInfo"):src.index("class OHLCVAggregator")]
    ns: dict = {}
    exec("from datetime import datetime, timedelta, timezone\nfrom zoneinfo import ZoneInfo\n" + body, ns)
    return ns["candle_bucket_start"]


def test_daily_candles_take_the_new_york_session_date():
    f = _bucket_fn()
    ten_am_et = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
    nine_pm_et = datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc)   # still 29 Oct in New York
    ny_midnight = datetime(2026, 10, 29, 4, 0, tzinfo=timezone.utc)
    assert f(ten_am_et, 1440, "1d") == ny_midnight
    assert f(nine_pm_et, 1440, "1d") == ny_midnight, "a UTC cut would put this in 30 Oct"
    assert f(ten_am_et, 10080, "1w") == datetime(2026, 10, 26, 4, 0, tzinfo=timezone.utc), "Monday, New York"
    assert f(datetime(2026, 10, 29, 15, 30, tzinfo=timezone.utc), 240, "4h") == \
        datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc), "11:30 ET falls in the 08:00-12:00 ET bucket"


def test_daily_and_weekly_candles_take_regular_session_prints_only():
    src = _src("services/collector-tradfi/main.py")
    assert 'if tf in ("1d", "1w") and session_name != "REGULAR":' in src
    assert "now.replace(hour=0, minute=0, second=0, microsecond=0)" not in src.split("class OHLCVAggregator")[1].split("def flush")[1][:4000]


def test_every_candle_list_gets_an_expiry_from_its_first_push():
    lua = _src("services/collector-tradfi/main.py").split('LUA_AGGREGATE_SCRIPT = """')[1].split('"""')[0]
    first = lua.split("if #latest == 0 then")[1].split("else")[0]
    assert 'redis.call("EXPIRE", key, 604800)' in first


def test_live_bars_never_overwrite_a_consolidated_one():
    src = _src("services/enrichment/enrichers/tradfi.py")
    block = src.split("INSERT INTO tradfi_bars")[1].split('"""')[0]
    assert "WHERE tradfi_bars.source IS DISTINCT FROM 'alpaca_sip'" in block


def test_the_settle_pass_includes_every_provisionally_written_stock():
    src = _src("services/collector-tradfi/main.py")
    assert "source IS DISTINCT FROM 'alpaca_sip'" in src.split("_PROVISIONAL_SQL = ")[1].split("\n")[0]
    loop = src.split("async def _settle_minute_bars_loop")[1].split("\n# ")[0]
    assert "_PROVISIONAL_SQL" in loop and "provisional" in loop


def test_daily_charts_come_from_the_consolidated_table_first():
    spec = importlib.util.find_spec("services.api_gateway.routes.radar")
    assert spec is not None
    src = _src("services/api_gateway/routes/radar.py")
    assert 'DAILY_BUCKETS = {"1d": "1 day", "1w": "1 week", "1M": "1 month"}' in src
    body = src.split("async def get_candles")[1]
    assert body.index("_DAILY_SERIES_SQL") < body.index("# 1. Hot path"), "daily_bars must be read before Redis"
    assert "if redis and not candles:" in body


def test_rollups_are_rebuilt_on_the_new_york_session():
    src = _src("shared/db/migrate.py")
    m = src.split('"0076_session_aligned_rollups"')[1].split('"transactional"')[0]
    for v in ("tradfi_bars_1d", "tradfi_bars_1w", "tradfi_bars_1mth", "tradfi_bars_4h"):
        assert f"CREATE MATERIALIZED VIEW {v}" in m
    assert m.count("'America/New_York')::time BETWEEN '09:30' AND '16:00'") == 3
    assert "time_bucket('1 month', time, 'America/New_York')" in m and "'30 days'" not in m.split("tradfi_bars_1mth")[1][:300]
