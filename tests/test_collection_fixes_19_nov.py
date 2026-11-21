"""Collection-layer fixes from the 18 Nov layer audit, section 1, each pinned.

1. Reddit answered http_404 for every subreddit and `upstream_errors` stayed 0.
2. `collector_rejected_total` was 0 everywhere: `dropped()` was called, the
   reject counter was not.
3. The starvation alarm fired 14,682 times outside the US session and stayed
   at 1 for collector-ais while data flowed (two CollectorMetrics instances).
4. 424B2 structured-note supplements were 1,310 of 3,081 weekly sec_edgar events.
"""
from __future__ import annotations

import asyncio
import ast
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

from shared.utils import collector_metrics as cm
from shared.utils.filing_forms import is_low_value_form
from shared.utils.metrics import _COUNTER_METRICS, _GAUGE_METRICS

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ROOT / "services"


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── 1. Reddit 404 is an upstream error, and the dead leg is visible ──────────

class _Resp:
    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self) -> str:
        return ""


class _Session:
    def __init__(self, status: int):
        self._status = status

    def get(self, *a, **k):
        return _Resp(self._status)


def test_reddit_404_counts_an_upstream_error():
    social = _load("services/collector-social/main.py", "collector_social_under_test")
    key = "collector_upstream_errors_total:collector-social"
    src_key = "collector_upstream_error_source:collector-social:reddit"
    before, before_src = _COUNTER_METRICS[key], _COUNTER_METRICS[src_key]

    got = asyncio.run(social.poll_reddit_subreddit(_Session(404), None, None, "wallstreetbets", "retail", 0.5))

    assert got == 0
    assert _COUNTER_METRICS[key] == before + 1
    assert _COUNTER_METRICS[src_key] == before_src + 1


def test_per_source_freshness_shows_a_leg_that_never_delivered():
    m = cm.CollectorMetrics("collector-freshness-test")
    m.expect_sources("telegram", "reddit")
    m.ingested(1, source="telegram")
    ages = m.source_freshness()
    # Telegram keeps the collector-level last-success fresh; reddit must not hide behind it.
    assert ages["reddit"] is None
    assert ages["telegram"] is not None and ages["telegram"] < 5
    assert _GAUGE_METRICS["collector_last_success_epoch:collector-freshness-test"] > 0
    later = m.source_freshness(now=m._source_last_at["telegram"] + 600)
    assert later["telegram"] == 600.0 and later["reddit"] is None


def test_social_heartbeat_publishes_the_source_ages():
    src = (SERVICES / "collector-social" / "main.py").read_text(encoding="utf-8")
    assert "source_freshness()" in src and "expect_sources(" in src


# ── 2. Every collector module that drops a record also counts the reject ─────

def _collector_modules():
    return sorted(p for d in SERVICES.glob("collector-*") for p in d.glob("*.py"))


def test_every_collector_module_that_imports_dropped_references_metrics_rejected():
    checked, offenders = 0, []
    for path in _collector_modules():
        src = path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(src)
        imports_dropped = any(
            isinstance(n, ast.ImportFrom) and any(a.name == "dropped" for a in n.names)
            for n in ast.walk(tree)
        )
        if not imports_dropped:
            continue
        checked += 1
        if ".rejected(" not in src:
            offenders.append(f"{path.parent.name}/{path.name}")
    assert checked >= 8, f"scan saw only {checked} modules that import dropped"
    assert not offenders, f"{offenders} import dropped() and never call metrics.rejected()"


def test_rejected_reasons_reach_the_exported_counters():
    m = cm.CollectorMetrics("collector-reject-test")
    m.rejected("form4_unlisted_issuer")
    assert _COUNTER_METRICS["collector_rejected_total:collector-reject-test"] == 1
    assert _COUNTER_METRICS["collector_rejected_reason:collector-reject-test:form4_unlisted_issuer"] == 1


# ── 3. The starvation watcher respects the session and clears on a record ────

async def _run_watch(m, clock, is_open, steps):
    """One watcher through `steps`: (clock_value, session_open) pairs."""
    task = asyncio.create_task(
        m.watch_for_starvation(expect_within_sec=300.0, check_interval_sec=0.01,
                               window_open=lambda now: is_open[0], clock=lambda: clock[0])
    )
    await asyncio.sleep(0)  # let the watcher read its start time at clock[0]
    seen = []
    for value, open_now in steps:
        clock[0], is_open[0] = value, open_now
        await asyncio.sleep(0.06)
        seen.append(_GAUGE_METRICS[f"collector_starved:{m.name}"])
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return seen


def test_starvation_alarm_is_silent_outside_the_session_and_fires_inside_it():
    m = cm.CollectorMetrics("collector-window-test")
    m._ingested, m._last_ingest_at = 50, 1000.0
    night = 1000.0 + 20 * 3600
    seen = asyncio.run(_run_watch(m, [1000.0], [True], [
        (night, False),        # closed 20 h after the last record: no alarm
        (night + 100, True),   # session reopens: idle counts from the open
        (night + 450, True),   # 350 s into the session with nothing: alarm
        (night + 460, False),  # session closes again: flag clears
    ]))
    assert seen == [0.0, 0.0, 1.0, 0.0]


def test_the_next_record_clears_the_flag_without_waiting_for_the_watcher():
    m = cm.CollectorMetrics("collector-clear-test")
    _GAUGE_METRICS["collector_starved:collector-clear-test"] = 1.0
    m.ingested(1)
    assert _GAUGE_METRICS["collector_starved:collector-clear-test"] == 0.0


def test_us_equity_window_follows_the_cash_session():
    def ts(y, mo, d, h):  # UTC
        return datetime(y, mo, d, h, tzinfo=timezone.utc).timestamp()

    assert cm.us_equity_window_open(ts(2026, 11, 18, 15))        # Wed 10:00 ET
    assert not cm.us_equity_window_open(ts(2026, 11, 18, 3))     # Wed 22:00 ET Tue
    assert not cm.us_equity_window_open(ts(2026, 11, 21, 15))    # Saturday


def test_radar_and_ais_use_the_session_window_and_a_single_instance():
    radar = (SERVICES / "collector-radar" / "main.py").read_text(encoding="utf-8")
    assert "window_open=us_equity_window_open" in radar
    ais = (SERVICES / "collector-ais" / "main.py").read_text(encoding="utf-8")
    # Two CollectorMetrics objects left main()'s watcher at zero arrivals forever.
    assert 'CollectorMetrics("collector-ais")' not in ais
    assert ais.count('_collector_metrics("collector-ais")') == 2


# ── 4. 424B2 pricing supplements are skipped; everything else is kept ────────

def test_only_424b2_is_low_value():
    for form in ("424B2", "424b2", " 424B2 "):
        assert is_low_value_form(form)
    for form in ("8-K", "8-K/A", "424B3", "424B4", "424B5", "S-1", "SC 13D", "13F-HR", "4", "", None):
        assert not is_low_value_form(form)


def test_the_firehose_skips_424b2_and_still_publishes_the_rest():
    fh = _load("services/collector-filings/edgar_firehose.py", "edgar_firehose_collection_test")

    def rec(acc, form):
        return {"accession_number": acc, "form": form, "company_name": "ACME CORP", "cik": "1",
                "items": ["2.02"] if form.startswith("8-K") else [], "filed_at": "2026-11-18T14:00:00-05:00",
                "index_url": "https://www.sec.gov/x"}

    records = [rec("a-1", "424B2"), rec("a-2", "8-K"), rec("a-3", "424B3"), rec("a-4", "424B5"), rec("a-5", "424B2")]

    async def fake_feed(session, headers, prefix):
        return records if prefix == "8-K" else []

    class Dedup:
        def __init__(self):
            self.seen = set()

        async def is_seen(self, a):
            return a in self.seen

        async def mark_seen(self, a):
            self.seen.add(a)

    class Producer:
        def __init__(self):
            self.sent = []

        async def send(self, topic, payload, key=None):
            self.sent.append(payload["raw_payload"]["form_type"])

    fh._fetch_form_feed = fake_feed
    producer, dedup = Producer(), Dedup()
    key = "collector_rejected_reason:collector-filings:424b2_low_value"
    before = _COUNTER_METRICS[key]

    n = asyncio.run(fh.poll_edgar_firehose(None, producer, dedup, {}, {}, lambda name: "ACME"))

    assert n == 3 and sorted(producer.sent) == ["424B3", "424B5", "8-K"]
    assert _COUNTER_METRICS[key] == before + 2
    assert not ({"a-1", "a-5"} & dedup.seen), "a skipped filing must not be marked seen"


# ── 5. Live SEC pollers take a sec_slot (CLAUDE.md: SEC requests go through it) ─

def test_the_live_edgar_pollers_wait_for_a_sec_slot():
    for rel, marker in (
        ("services/collector-filings/main.py", "data.sec.gov/submissions"),
        ("services/collector-filings/edgar_firehose.py", "EDGAR_CURRENT_URL.format"),
    ):
        src = (ROOT / rel).read_text(encoding="utf-8")
        assert "from shared.utils.sec_rate import sec_slot" in src, rel
        head, _, tail = src.partition(marker)
        assert "await sec_slot()" in tail[:400], f"{rel}: request not preceded by sec_slot()"
