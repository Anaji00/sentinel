"""8-K history rebuilt point-in-time, in the live firehose's own format."""

import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-filings"))

import history  # noqa: E402
from shared.utils.signal_families import observations  # noqa: E402

SUBS = {"name": "Acme Corp", "filings": {"recent": {
    "form": ["8-K", "10-Q", "8-K", "8-K"],
    "acceptanceDateTime": ["2025-03-01T21:05:12.000Z", "2025-02-01T12:00:00.000Z",
                           "2019-01-01T12:00:00.000Z", "2025-04-02T13:30:00.000Z"],
    "accessionNumber": ["0001-25-000001", "0001-25-000002", "0001-19-000003", "0001-25-000004"],
    "items": ["4.02,9.01", "", "5.02", ""],
    "filingDate": ["2025-03-01", "2025-02-01", "2019-01-01", "2025-04-02"],
}}}
SINCE = datetime(2021, 1, 1, tzinfo=timezone.utc)


def test_only_recent_8ks_with_items_are_archived_at_acceptance_time() -> None:
    rows = history.eight_k_rows("ACME", "Acme Corp", SUBS, {"4.02": "Non-Reliance"}, SINCE)
    assert len(rows) == 1
    event_id, accepted, headline, summary, tags, ticker, fd = rows[0]
    assert event_id == "edgar8k:0001-25-000001"
    assert accepted == datetime(2025, 3, 1, 21, 5, 12, tzinfo=timezone.utc)
    assert summary.startswith("Material 8-K Event for ACME: 4.02: Non-Reliance; 9.01:")


def test_the_signal_families_read_history_like_live_filings() -> None:
    _id, accepted, headline, summary, tags, ticker, _fd = history.eight_k_rows(
        "ACME", "Acme Corp", SUBS, {"4.02": "Non-Reliance"}, SINCE)[0]
    row = {"event_id": _id, "type": "filing", "source": "sec_edgar_history", "created_at": accepted,
           "summary": summary, "tags": tags, "primary_entity_id": ticker, "financial_data": {"ticker": ticker}}
    fams = {(o.family, o.instrument, o.direction) for o in observations(row)}
    assert ("8k_nonreliance", "ACME", -1) in fams


def test_the_universe_maps_tickers_to_ciks() -> None:
    assert history.universe_ciks({"320193": "AAPL", "1045810": "NVDA"}, ["aapl", "ZZZZ"]) == {"AAPL": "320193"}


def test_the_industry_code_is_read_from_the_submissions_file() -> None:
    # With the issuer's kind (no annual report on file reads as domestic) and
    # its state of incorporation, when EDGAR gives one.
    assert history.profile_row("acme", "123", {"sic": "3674", "sicDescription": "Semiconductors"}) == (
        "ACME", "123", 3674, "Semiconductors", "domestic", None)
    assert history.profile_row("acme", "123", {"sic": ""}) is None


# --- deep history: the window opens at 2016 and older submissions pages are read ---

import asyncio  # noqa: E402

OLD_PAGE = {   # an older submissions page: the same columns, at the top level
    "form": ["8-K", "8-K", "10-K", "8-K"],
    "acceptanceDateTime": ["2016-02-23T23:38:35.000Z", "2017-06-01T12:00:00.000Z", "2017-03-01T12:00:00.000Z",
                           "2015-12-30T20:00:00.000Z"],
    "accessionNumber": ["0001-16-000001", "0001-17-000002", "0001-17-000003", "0001-15-000004"],
    "items": ["7.01,9.01", "5.02", "", "2.02"],
    "filingDate": ["2016-02-24", "2017-06-01", "2017-03-01", "2015-12-30"],
}
SUBS_WITH_FILES = {"name": "Acme Corp", "filings": {"recent": SUBS["filings"]["recent"], "files": [
    {"name": "CIK1-submissions-001.json", "filingFrom": "2016-01-04", "filingTo": "2023-01-01"},
    {"name": "CIK1-submissions-002.json", "filingFrom": "2005-01-01", "filingTo": "2015-12-31"}]}}


def test_the_window_opens_at_the_first_price_bar() -> None:
    assert history.HISTORY_START == datetime(2016, 1, 1, tzinfo=timezone.utc)


def test_only_pages_reaching_into_the_window_are_fetched() -> None:
    assert history.older_page_names(SUBS_WITH_FILES, history.HISTORY_START) == ["CIK1-submissions-001.json"]
    assert history.older_page_names(SUBS_WITH_FILES, datetime(2010, 1, 1, tzinfo=timezone.utc)) == [
        "CIK1-submissions-001.json", "CIK1-submissions-002.json"]
    assert history.older_page_names({}, history.HISTORY_START) == []


def test_older_pages_add_rows_in_the_same_format_and_stop_at_the_window() -> None:
    rows = history.eight_k_rows("ACME", "Acme Corp", SUBS, {"4.02": "Non-Reliance"}, history.HISTORY_START, [OLD_PAGE])
    ids = [r[0] for r in rows]
    assert "edgar8k:0001-16-000001" in ids and "edgar8k:0001-17-000002" in ids
    assert "edgar8k:0001-15-000004" not in ids                              # accepted before 2016
    assert "edgar8k:0001-17-000003" not in ids                              # a 10-K
    old = next(r for r in rows if r[0] == "edgar8k:0001-16-000001")
    assert old[1] == datetime(2016, 2, 23, 23, 38, 35, tzinfo=timezone.utc)  # acceptance, not the next filing date
    assert old[3] == "Material 8-K Event for ACME: 7.01: Corporate Disclosure; 9.01: Corporate Disclosure. Filed on 2016-02-24."
    assert old[6] == {"ticker": "ACME", "items": ["7.01", "9.01"], "accession_number": "0001-16-000001"}


def test_a_2016_officer_change_reads_like_a_current_one_to_the_families() -> None:
    row = next(r for r in history.eight_k_rows("ACME", "Acme Corp", {}, {"5.02": "Departure of Directors"},
                                               history.HISTORY_START, [OLD_PAGE]) if r[0].endswith("17-000002"))
    obs = observations({"event_id": row[0], "type": "filing", "source": "sec_edgar_history", "created_at": row[1],
                        "summary": row[3], "tags": row[4], "primary_entity_id": row[5],
                        "financial_data": {"ticker": row[5]}})
    assert {(o.family, o.instrument) for o in obs} == {("8k_officer_change", "ACME")}


class _Resp:
    def __init__(self, status: int, body=None):
        self.status, self._body = status, body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        return self._body


class _Session:
    def __init__(self, routes):
        self.routes, self.urls = routes, []

    def get(self, url, headers=None):
        self.urls.append(url)
        return self.routes[url.rsplit("/", 1)[1]]()


class _Raw:
    def __init__(self):
        self.sets, self.hashes = {}, {}

    async def smembers(self, k):
        return set(self.sets.get(k, ()))

    async def sadd(self, k, v):
        self.sets.setdefault(k, set()).add(v)

    async def hincrby(self, k, f, n):
        self.hashes.setdefault(k, {}).setdefault(f, 0)
        self.hashes[k][f] += n
        return self.hashes[k][f]


class _Db:
    def __init__(self):
        self.rows = {}

    async def execute_many(self, sql, rows):
        for r in rows:
            self.rows.setdefault(r[0], r)           # ON CONFLICT DO NOTHING


def _run_backfill(session, raw, db):
    history.REQUEST_SPACING_SEC = 0
    return asyncio.run(history.backfill_8k_history(
        session, {}, db, raw, {"ACME": "1"}, {}, "done", history.HISTORY_START))


def test_backfill_reads_older_pages_marks_done_and_is_idempotent(monkeypatch) -> None:
    async def no_wait():
        return None
    monkeypatch.setattr(history, "sec_slot", no_wait)
    routes = {"CIK0000000001.json": lambda: _Resp(200, SUBS_WITH_FILES),
              "CIK1-submissions-001.json": lambda: _Resp(200, OLD_PAGE)}
    raw, db = _Raw(), _Db()
    session1 = _Session(routes)
    s1 = _run_backfill(session1, raw, db)
    assert s1["pages"] == 1 and s1["filings"] == 4 and "ACME" in raw.sets["done:from2016"]
    assert not any(u.endswith("002.json") for u in session1.urls)           # the pre-2016 page is never fetched
    n = len(db.rows)
    session2 = _Session(routes)
    s2 = _run_backfill(session2, raw, db)
    assert s2["companies"] == 0 and session2.urls == [] and len(db.rows) == n   # a rerun fetches nothing


def test_a_failed_older_page_is_retried_then_parked(monkeypatch) -> None:
    async def no_wait():
        return None
    monkeypatch.setattr(history, "sec_slot", no_wait)
    routes = {"CIK0000000001.json": lambda: _Resp(200, SUBS_WITH_FILES),
              "CIK1-submissions-001.json": lambda: _Resp(503)}
    raw, db = _Raw(), _Db()
    for attempt in range(history.MAX_ATTEMPTS):
        assert "ACME" not in raw.sets.get("done:from2016", set())
        stats = _run_backfill(_Session(routes), raw, db)
        assert stats["errors"] >= 1
    assert "ACME" in raw.sets["done:from2016"]                              # parked, not retried forever
