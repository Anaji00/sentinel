"""History is kept: an archive the study reads, and deep daily bars."""

import asyncio
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-tradfi"))

import historical_backfill as hb  # noqa: E402


class _Db:
    def __init__(self):
        self.rows = []

    async def execute_many(self, sql, rows):
        assert "INSERT INTO daily_bars" in sql
        self.rows.extend(rows)


def test_daily_bars_are_written_adjusted_and_bad_prices_skipped(monkeypatch) -> None:
    monkeypatch.setattr(hb, "_credentials", lambda: {"k": "v"})
    monkeypatch.setattr(hb, "INTER_REQUEST_SLEEP_SEC", 0)

    async def fake_page(session, headers, symbols, timeframe, s, e, token):
        assert timeframe == "1Day"
        return {"NVDA": [
            {"t": "2026-10-01T04:00:00Z", "o": 230, "h": 236, "l": 229, "c": 235, "v": 1e6},
            {"t": "2026-10-02T04:00:00Z", "o": 235, "h": 236, "l": 233, "c": 0, "v": 1e6},
        ]}, None

    monkeypatch.setattr(hb, "_fetch_page", fake_page)
    db = _Db()

    class _S:
        async def close(self):
            pass

    report = asyncio.run(hb.backfill_daily_bars(db, ["nvda"], lookback_days=30, session=_S()))
    assert report["bars_written"] == 1 and report["tickers_with_data"] == 1
    assert db.rows[0][0] == "NVDA" and str(db.rows[0][1]) == "2026-10-01" and db.rows[0][5] == 235


def test_no_credentials_writes_nothing(monkeypatch) -> None:
    monkeypatch.setattr(hb, "_credentials", lambda: None)
    report = asyncio.run(hb.backfill_daily_bars(_Db(), ["NVDA"], lookback_days=30))
    assert report["bars_written"] == 0 and report["errors"]


def test_the_study_reads_the_permanent_archive() -> None:
    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert 'FROM signal_event_archive' in src.replace('"FROM signal_event_archive"', 'FROM signal_event_archive')
    assert "_STUDY_EVENTS_SQL, day, nxt" in src
    mig = (ROOT / "shared/db/migrate.py").read_text(encoding="utf-8")
    assert "0030_signal_event_archive" in mig and "0031_daily_bars" in mig
    # The archive has no retention policy: it only grows.
    assert "add_retention_policy('signal_event_archive'" not in mig


def test_daily_history_is_prefixed_and_never_overlaps_intraday() -> None:
    from array import array
    from services.agents.signal_engine import merge_daily

    daily = (array("d", [100.0, 200.0, 300.0]), array("d", [1.0, 2.0, 3.0]))
    intraday = (array("d", [250.0, 260.0]), array("d", [2.5, 2.6]))
    t, c = merge_daily(daily, intraday)
    assert list(t) == [100.0, 200.0, 250.0, 260.0] and list(c) == [1.0, 2.0, 2.5, 2.6]
    assert merge_daily(None, intraday) is intraday and merge_daily(daily, None) is daily


def test_a_daily_close_is_stamped_at_four_pm_new_york() -> None:
    from datetime import date, datetime, timezone
    from services.agents.signal_engine import _close_ts

    summer = datetime.fromtimestamp(_close_ts(date(2026, 7, 15)), timezone.utc)
    winter = datetime.fromtimestamp(_close_ts(date(2026, 1, 15)), timezone.utc)
    assert (summer.hour, winter.hour) == (20, 21)


def test_the_hourly_horizon_never_reads_daily_points() -> None:
    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert "use = series if h >= timedelta(days=1) else intraday" in src


def test_the_study_reads_filings_before_its_one_year_window() -> None:
    """EDGAR history is stamped at acceptance; a one-year window read one year of five."""
    import asyncio
    from services.agents import signal_engine as se

    class Raw:
        async def set(self, *a, **k): return True
        async def delete(self, *a, **k): return 0
        async def sadd(self, *a, **k): return 0

    class Redis:
        raw = Raw()

    eng = se.SignalEngine(None, Redis())
    calls = []

    async def load(start, end, types=None, step_days=1):
        calls.append((start, end, types, step_days))
        return []

    async def nothing(*a, **k):
        return None

    eng._load_observations = load
    eng._archive_recent = nothing
    eng._series = nothing
    eng._daily_series = nothing
    asyncio.run(eng.run_study())
    deep = [c for c in calls if c[2] == se.DEEP_HISTORY_TYPES]
    assert deep, "no deep pass over filings"
    start, end, _t, step = deep[0]
    assert (end - start).days >= se.HISTORY_LOOKBACK_DAYS - se.STUDY_LOOKBACK_DAYS - 1 and step == 30
