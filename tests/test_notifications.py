"""Telling people about the stocks they follow: what counts, and how much."""

from shared.utils import notifications as nt

DAY = "2026-11-04"


def _recs(validated=True, direction=1):
    return {"horizons": {"monthly": {"validated": validated, "ideas": [{"ticker": "goog", "direction": direction}]}}}


def test_only_validated_horizons_can_alert():
    assert nt.call_snapshot(_recs()) == {"monthly:GOOG": 1}
    assert nt.call_snapshot(_recs(validated=False)) == {}


def test_a_new_call_and_a_reversal_notify_but_an_ending_does_not():
    new = nt.call_notices({}, {"monthly:GOOG": 1}, DAY)
    assert [n.title for n in new] == ["New call on GOOG: buy"]
    flip = nt.call_notices({"monthly:GOOG": 1}, {"monthly:GOOG": -1}, DAY)
    assert [n.title for n in flip] == ["Call reversed on GOOG: now short"]
    assert nt.call_notices({"monthly:GOOG": 1}, {"monthly:GOOG": 1}, DAY) == []
    assert nt.call_notices({"monthly:GOOG": 1}, {}, DAY) == []


def test_verdicts_need_higher_confidence():
    board = [
        {"ticker": "A", "net": 5, "confidence": "higher"},
        {"ticker": "B", "net": -5, "confidence": "moderate"},
        {"ticker": "C", "net": 0, "confidence": "higher"},
    ]
    assert nt.verdict_snapshot(board) == {"A": 1}
    assert nt.verdict_notices({}, {"A": 1}, DAY)[0].title == "Evidence on A now leans bullish"
    assert nt.verdict_notices({"A": 1}, {"A": -1}, DAY)[0].title == "Evidence on A flipped to bearish"
    assert nt.verdict_notices({"A": 1}, {"A": 1}, DAY) == []


def test_only_filing_events_become_notices():
    filing = nt.event_notice({"event_id": "e1", "type": "filing", "primary_entity_id": "alkt", "summary": "SCHEDULE 13D/A filed"})
    assert (filing.kind, filing.ticker, filing.dedupe_key) == ("filing", "ALKT", "event:e1")
    assert nt.event_notice({"event_id": "e3", "type": "headline", "primary_entity_id": "X", "summary": "s"}) is None
    assert nt.event_notice({"event_id": "e4", "type": "filing", "primary_entity_id": "X", "summary": ""}) is None


def test_holder_moves_need_several_institutions_and_notify_once():
    # The six active managers count; index funds moving is just index flow.
    add = {"kind": "add", "change": 0.5}
    per_filer = {"FMR LLC": {"NVDA": add, "LONE": add}, "T. Rowe Price Associates": {"NVDA": add},
                 "Wellington Management Group": {"NVDA": add}, "Vanguard Group": {"LONE": add},
                 "BlackRock, Inc.": {"LONE": add}}
    flows = nt.holder_flows(per_filer, "2026-06-30")
    assert list(flows) == ["2026-06-30:NVDA"]
    first = nt.holder_notices({}, flows)
    assert first[0].title == "Institutions are buying NVDA" and "quarter ending 2026-06-30" in first[0].body
    assert nt.holder_notices(flows, flows) == []


def test_long_summaries_are_cut():
    n = nt.event_notice({"event_id": "e", "type": "filing", "primary_entity_id": "X", "summary": "w " * 400})
    assert len(n.body) <= nt.BODY_MAX and n.body.endswith("…")


def _notice(kind, ticker, key):
    return nt.Notice(kind, ticker, f"t {key}", "b", "/u", key)


def test_address_keeps_followed_stocks_and_wanted_kinds():
    notices = [_notice("call", "A", "1"), _notice("filing", "A", "2"), _notice("call", "B", "3")]
    assert [n.dedupe_key for n in nt.address(notices, ["a"], None, 0, DAY)] == ["1", "2"]
    assert [n.dedupe_key for n in nt.address(notices, ["A"], {"filing": False}, 0, DAY)] == ["1"]


def test_the_cap_rolls_the_excess_into_one_summary_and_keeps_calls_first():
    notices = [_notice("filing", "A", f"f{i}") for i in range(5)] + [_notice("call", "A", "c")]
    out = nt.address(notices, ["A"], None, 0, DAY, cap=3)
    assert [n.dedupe_key for n in out] == ["c", "f0", f"overflow:{DAY}"]
    assert out[-1].title == "4 more updates on stocks you follow"
    assert len(out) == 3


def test_a_full_day_only_gets_the_one_summary():
    out = nt.address([_notice("call", "A", "c")], ["A"], None, 20, DAY)
    assert [n.dedupe_key for n in out] == [f"overflow:{DAY}"]


def test_digest_names_every_update_and_how_to_stop():
    subject, body = nt.digest([{"title": "New call on A: buy", "body": "b"}], "https://x")
    assert subject == "Sentinel: 1 update on stocks you follow"
    assert "New call on A: buy" in body and "Turn this email off" in body and "https://x" in body


# ── the job: first runs, restarts and repeats must not mislead anyone ───────

import asyncio
import json
from datetime import datetime, timezone

from services.alert_manager import notifications_job as job
from shared.utils.signal_store import RECOMMENDATIONS_KEY, SIGNAL_MATRIX_INDEX_KEY

NOW = datetime(2026, 11, 4, 9, 0, tzinfo=timezone.utc)  # before the digest hour


class _Raw:
    def __init__(self, **kv):
        self.kv = kv

    async def get(self, k):
        return self.kv.get(k)

    async def set(self, k, v):
        self.kv[k] = v

    async def hgetall(self, k):
        return {}

    async def scan_iter(self, *a, **k):
        for x in ():
            yield x


class _Db:
    def __init__(self, follows=("GOOG",), notify=None, filings=()):
        self.prefs = {"tickers": list(follows), "notify": notify or {}}
        self.filings, self.rows, self.sql = list(filings), {}, []

    async def query(self, sql, *a):
        self.sql.append(sql)
        if "user_preferences" in sql:
            return [{"owner": "a@b.c", "prefs": self.prefs}]
        if "FROM events" in sql:
            return self.filings
        return []

    async def query_one(self, sql, *a):
        if "count(*)" in sql:
            return {"n": len(self.rows)}
        key = (a[0], a[6])
        if key in self.rows:
            return None
        self.rows[key] = a
        return {"id": len(self.rows)}


def _run(raw, db, now=NOW):
    return asyncio.run(job.run_once(raw, db, now))


def _with_call(direction=1):
    return json.dumps(_recs(direction=direction))


def test_the_first_run_records_and_tells_nobody():
    raw, db = _Raw(**{RECOMMENDATIONS_KEY: _with_call()}), _Db()
    assert _run(raw, db) == 0 and db.rows == {}
    assert json.loads(raw.kv[job.STATE_KEY])["calls"] == {"monthly:GOOG": 1}


def test_a_change_after_that_reaches_the_follower_once():
    raw, db = _Raw(**{RECOMMENDATIONS_KEY: _with_call(1)}), _Db()
    _run(raw, db)
    raw.kv[RECOMMENDATIONS_KEY] = _with_call(-1)
    assert _run(raw, db) == 1
    assert [r[3] for r in db.rows.values()] == ["Call reversed on GOOG: now short"]
    assert _run(raw, db) == 0


def test_a_restart_emptying_the_sources_reports_nothing_and_keeps_the_snapshot():
    raw, db = _Raw(**{RECOMMENDATIONS_KEY: _with_call(1)}), _Db()
    _run(raw, db)
    del raw.kv[RECOMMENDATIONS_KEY]
    raw.kv[SIGNAL_MATRIX_INDEX_KEY] = json.dumps({"board": []})
    assert _run(raw, db) == 0
    assert json.loads(raw.kv[job.STATE_KEY])["calls"] == {"monthly:GOOG": 1}
    # The same call rebuilt afterwards is not news either.
    raw.kv[RECOMMENDATIONS_KEY] = _with_call(1)
    assert _run(raw, db) == 0


def test_people_only_hear_about_what_they_follow_and_want():
    raw = _Raw(**{RECOMMENDATIONS_KEY: json.dumps({"horizons": {"monthly": {"validated": True, "ideas": []}}})})
    raw.kv[job.STATE_KEY] = json.dumps({"calls": {"monthly:ZZZ": 1}})
    raw.kv[RECOMMENDATIONS_KEY] = _with_call(1)
    db = _Db(notify={"call": False})
    assert _run(raw, db) == 0 and db.rows == {}


def test_new_filings_are_read_from_the_cursor():
    raw = _Raw(**{job.STATE_KEY: json.dumps({"filings_since": "2026-11-04T08:00:00+00:00"})})
    db = _Db(filings=[{"event_id": "e1", "type": "filing", "primary_entity_id": "GOOG", "summary": "13D filed"}])
    assert _run(raw, db) == 1
    assert list(db.rows)[0][1] == "event:e1"
    assert json.loads(raw.kv[job.STATE_KEY])["filings_since"] == NOW.isoformat()


def test_one_persons_failure_does_not_stop_the_run():
    class Broken(_Db):
        async def query_one(self, sql, *a):
            raise RuntimeError("database away")

    raw = _Raw(**{job.STATE_KEY: json.dumps({"calls": {}}), RECOMMENDATIONS_KEY: _with_call(1)})
    assert _run(raw, Broken()) == 0
    assert job.STATE_KEY in raw.kv
