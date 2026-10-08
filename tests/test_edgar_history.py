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
    assert history.profile_row("acme", "123", {"sic": "3674", "sicDescription": "Semiconductors"}) == (
        "ACME", "123", 3674, "Semiconductors")
    assert history.profile_row("acme", "123", {"sic": ""}) is None
