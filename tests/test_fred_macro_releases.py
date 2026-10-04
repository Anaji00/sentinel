"""
tests/test_fred_macro_releases.py

Macro releases from FRED.

The economic calendar polled a Finnhub endpoint that is premium on this key and
answers 403, so the platform had never ingested a CPI, payrolls, PCE, GDP or Fed
funds release. FRED publishes the official values minutes after each release.
It carries no consensus forecast, so a release is measured against the prior
reading and says so rather than calling the change a surprise.

Fixtures are the values FRED returned on 2 October 2026.
"""

import asyncio
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO = ROOT / "services" / "collector-macro"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


fred = _load("fred_releases_for_test", MACRO / "fred_releases.py")
cal = _load("economic_calendar_for_test", MACRO / "economic_calendar.py")

NOW = datetime(2026, 10, 2, 14, 0, tzinfo=timezone.utc)


def _obs(*pairs):
    return [{"date": d, "value": v} for d, v in pairs]


PAYEMS = _obs(("2026-09-01", "159044"), ("2026-08-01", "159015"), ("2026-07-01", "158882"))
UNRATE = _obs(("2026-09-01", "4.2"), ("2026-08-01", "4.1"))
DFEDTARU_FLAT = _obs(*[(f"2026-09-{d:02d}", "4") for d in range(30, 16, -1)])
DFEDTARU_CUT = _obs(("2026-10-02", "3.75"), *[(f"2026-09-{d:02d}", "4") for d in range(30, 17, -1)])


def _series(sid):
    return next(s for s in fred.FRED_SERIES if s.series_id == sid)


def test_payrolls_are_the_monthly_change_in_thousands():
    r = fred.release_from_observations(_series("PAYEMS"), PAYEMS)
    assert r == {"period": "2026-09-01", "actual": 29.0, "previous": 133.0, "detail": ""}


# FRED's CPIAUCSL on 2 October 2026, newest first, October 2025 missing.
CPI = _obs(("2026-08-01", "334.131"), ("2026-07-01", "332.813"), ("2026-06-01", "332.568"),
           ("2026-05-01", "333.979"), ("2026-04-01", "332.407"), ("2026-03-01", "330.293"),
           ("2026-02-01", "327.460"), ("2026-01-01", "326.588"), ("2025-12-01", "326.031"),
           ("2025-11-01", "325.063"), ("2025-10-01", "."), ("2025-09-01", "324.245"),
           ("2025-08-01", "323.291"), ("2025-07-01", "322.169"))


def test_cpi_is_year_on_year_by_calendar_month():
    r = fred.release_from_observations(_series("CPIAUCSL"), CPI)
    assert r["actual"] == round((334.131 / 323.291 - 1) * 100, 2)
    assert r["previous"] == round((332.813 / 322.169 - 1) * 100, 2)
    assert r["detail"] == f"m/m {(334.131 / 332.813 - 1) * 100:+.2f}%"


def test_a_missing_month_does_not_shift_the_comparison():
    """Counting back twelve rows across the October 2025 gap would have
    compared August 2026 with July 2025."""
    r = fred.release_from_observations(_series("CPIAUCSL"), CPI)
    assert r["actual"] != round((334.131 / 322.169 - 1) * 100, 2)


def test_no_year_ago_value_means_no_release():
    assert fred.release_from_observations(_series("CPIAUCSL"), CPI[:5]) is None


def test_too_little_history_is_none_not_a_guess():
    assert fred.release_from_observations(_series("CPIAUCSL"), CPI[:1]) is None
    assert fred.release_from_observations(_series("UNRATE"), UNRATE[:1]) is None


def test_missing_values_are_skipped():
    obs = [{"date": "2026-09-01", "value": "."}] + UNRATE
    assert fred.release_from_observations(_series("UNRATE"), obs)["actual"] == 4.2


def test_a_cold_start_announces_only_a_recent_release():
    s = _series("PAYEMS")
    r = fred.release_from_observations(s, PAYEMS)
    assert fred.is_new_release(s, r, None, NOW - timedelta(hours=1), NOW)
    assert not fred.is_new_release(s, r, None, NOW - timedelta(days=20), NOW)


def test_a_seen_period_is_not_announced_again():
    s = _series("PAYEMS")
    r = fred.release_from_observations(s, PAYEMS)
    assert not fred.is_new_release(s, r, "period:2026-09-01", NOW, NOW)
    assert fred.is_new_release(s, r, "period:2026-08-01", NOW, NOW)


def test_fed_funds_is_a_release_only_when_the_target_moves():
    s = _series("DFEDTARU")
    flat = fred.release_from_observations(s, DFEDTARU_FLAT)
    cut = fred.release_from_observations(s, DFEDTARU_CUT)
    assert not fred.is_new_release(s, flat, "value:4.0", NOW, NOW)
    assert not fred.is_new_release(s, flat, None, NOW, NOW)
    assert cut["actual"] == 3.75 and cut["previous"] == 4.0
    assert fred.is_new_release(s, cut, "value:4.0", NOW, NOW)


def test_last_updated_parses_freds_offset():
    assert fred.parse_last_updated("2026-10-02 08:24:33-05") == datetime(2026, 10, 2, 13, 24, 33, tzinfo=timezone.utc)
    assert fred.parse_last_updated("junk") is None


def test_a_release_without_consensus_does_not_claim_a_surprise():
    c = cal.EconomicCalendarCollector()
    e = c.build_macro_release_event("NFP", 29.0, forecast=None, previous=133.0,
                                    period="2026-09-01", source="fred")
    assert "vs prior 133.0k" in e.headline
    assert "Forecast" not in e.headline and "Surprise" not in e.headline
    assert "no_consensus" in e.tags


def test_a_release_with_consensus_is_unchanged():
    c = cal.EconomicCalendarCollector()
    e = c.build_macro_release_event("CPI", 3.1, forecast=2.9, previous=2.8)
    assert "vs Forecast 2.9%" in e.headline and "no_consensus" not in e.tags


def test_poll_fred_publishes_once_and_records_state(monkeypatch):
    c = cal.EconomicCalendarCollector(producer=MagicMock(), redis_client=MagicMock())
    c.producer.send = AsyncMock()
    state = {}
    c.redis.raw.hget = AsyncMock(side_effect=lambda k, f: state.get(f))
    c.redis.raw.hset = AsyncMock(side_effect=lambda k, f, v: state.__setitem__(f, v))
    c._get_session = AsyncMock(return_value=MagicMock())
    data = {"PAYEMS": PAYEMS, "UNRATE": UNRATE}

    async def fetch(session, series):
        return data.get(series.series_id, []), datetime.now(timezone.utc) - timedelta(minutes=30)

    monkeypatch.setattr(cal.fred, "fetch_series", fetch)
    first = asyncio.run(c.poll_fred())
    c._fred_polled_at = 0.0
    second = asyncio.run(c.poll_fred())
    assert sorted(e.macro_data.indicator for e in first) == ["NFP", "UNEMPLOYMENT"]
    assert second == []
    assert state["PAYEMS"] == "period:2026-09-01"


def test_finnhub_is_not_polled_once_fred_is_configured():
    src = (MACRO / "economic_calendar.py").read_text(encoding="utf-8")
    body = src.split("async def poll_and_publish")[1]
    assert body.index("return await self.poll_fred()") < body.index("fetch_finnhub_calendar()")


def test_a_release_goes_through_enrichment_and_survives_unchanged():
    """Sent straight to ENRICHED_EVENTS, no macro release was ever persisted:
    the agents read that topic and nothing writes it to the events table."""
    from shared.kafka import Topics
    from shared.models import RawEvent
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    c = cal.EconomicCalendarCollector(producer=MagicMock())
    c.producer.send = AsyncMock()
    event = c.build_macro_release_event("NFP", 29.0, previous=133.0, period="2026-09-01", source="fred")
    asyncio.run(c._publish(event))
    topic, payload = c.producer.send.await_args.args[:2]
    assert topic == Topics.RAW_TRADFI

    enricher = TradFiEnricher.__new__(TradFiEnricher)
    out = asyncio.run(enricher.enrich(RawEvent.model_validate(payload)))
    assert out.event_id == event.event_id
    assert out.headline == event.headline
    assert out.macro_data.actual == 29.0 and "no_consensus" in out.tags


# BAMLH0A0HYM2 as FRED returned it on 3 October: +44bp in a week.
HY = _obs(("2026-10-01", "3.24"), ("2026-09-30", "3.12"), ("2026-09-29", "3.08"),
          ("2026-09-28", "3.02"), ("2026-09-25", "2.93"), ("2026-09-24", "2.80"))


def _ctx(ind):
    return next(s for s in fred.FRED_CONTEXT_SERIES if s.indicator == ind)


def test_a_44bp_week_in_high_yield_is_notable():
    r = fred.context_reading(_ctx("HY_OAS"), HY)
    assert r["value"] == 3.24 and r["week_ago"] == 2.80
    assert round(r["change"], 2) == 0.44 and r["notable"]


def test_a_quiet_week_is_context_only():
    quiet = _obs(*[(f"2026-09-{d:02d}", "3.10") for d in range(30, 24, -1)])
    assert not fred.context_reading(_ctx("HY_OAS"), quiet)["notable"]


def test_a_curve_crossing_zero_is_notable_however_small():
    obs = _obs(("2026-10-02", "-0.02"), ("2026-10-01", "0.01"), ("2026-09-30", "0.02"),
               ("2026-09-29", "0.03"), ("2026-09-28", "0.02"), ("2026-09-25", "0.04"))
    r = fred.context_reading(_ctx("CURVE_10Y2Y"), obs)
    assert r["crossed_zero"] and r["notable"]


def test_the_context_line_is_ordered_and_compact():
    line = fred.context_line({"HY_OAS": fred.context_reading(_ctx("HY_OAS"), HY)})
    assert line == "HY OAS 3.24 (+0.44 1w)"


def test_agents_read_the_context_line():
    src = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert "self.redis.raw.get(FRED_CONTEXT_KEY)" in src
