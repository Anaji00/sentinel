"""A split is not institutional accumulation: 13F share counts are unadjusted, so the feed's split dates gate the flows."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import ownership as ow  # noqa: E402
from shared.utils.corporate_actions import split_events  # noqa: E402

FILERS = ["Berkshire", "Pershing", "Third Point", "Elliott", "Appaloosa", "Greenlight"]


def doubled(n: int = 5):
    return {f: ow.holding_changes([{"ticker": "XYZ", "shares": 100 * (i + 1)}], [{"ticker": "XYZ", "shares": 200 * (i + 1) + i}])
            for i, f in enumerate(FILERS[:n])}


def test_without_the_feeds_split_a_doubling_by_five_holders_reads_as_accumulation() -> None:
    """The defect, kept as a test of what the aggregation alone cannot know."""
    assert ow.aggregate_changes(doubled(), filers=None)["XYZ"]["flow"] == 1


def test_the_history_path_skips_a_ticker_and_quarter_the_live_path_already_archived() -> None:
    published = ow.published_flow_keys([
        {"ticker": "AMAT", "tags": ["institutional_accumulation", "exposes:AMAT", "period:2026-06-30"]},
        {"ticker": "dell", "tags": ["institutional_distribution", "period:2026-06-30"]},
        {"ticker": "NOPERIOD", "tags": ["institutional_accumulation"]},
        {"ticker": None, "tags": ["period:2026-06-30"]},
    ])
    assert published == {("AMAT", "2026-06-30"), ("DELL", "2026-06-30")}
    rows = [("id1", None, "h", "s", ["institutional_accumulation", "period:2026-06-30"], "AMAT", {}),
            ("id2", None, "h", "s", ["institutional_accumulation", "period:2026-03-31"], "AMAT", {}),
            ("id3", None, "h", "s", ["institutional_distribution", "period:2026-06-30"], "DELL", {}),
            ("id4", None, "h", "s", ["institutional_accumulation", "period:2026-06-30"], "WAT", {})]
    assert [r[0] for r in ow.unpublished_flows(rows, published)] == ["id2", "id4"]
    assert ow.unpublished_flows(rows, set()) == rows


def test_a_split_in_the_quarter_removes_the_adds_and_cuts() -> None:
    skip = ow.split_tickers_in({"XYZ": ["2026-05-14"]}, "2026-03-31", "2026-06-30")
    assert skip == {"XYZ"}
    assert ow.aggregate_changes(doubled(), filers=None, split_tickers=skip) == {}
    reverse = {f: ow.holding_changes([{"ticker": "XYZ", "shares": 1000}], [{"ticker": "XYZ", "shares": 100}]) for f in FILERS[:4]}
    assert ow.aggregate_changes(reverse, filers=None, split_tickers=skip) == {}


def test_a_split_outside_the_two_quarter_ends_does_not_count() -> None:
    d = {"XYZ": ["2026-03-31", "2026-07-01"]}                      # on the start date (already in the earlier count), after the end
    assert ow.split_tickers_in(d, "2026-03-31", "2026-06-30") == frozenset()
    assert ow.split_tickers_in({"XYZ": ["2026-06-30"]}, "2026-03-31", "2026-06-30") == {"XYZ"}


def test_real_adds_in_a_name_without_a_split_still_fire_and_new_lines_survive_a_split() -> None:
    per = {f: ow.holding_changes([{"ticker": "XYZ", "shares": 100}], [{"ticker": "XYZ", "shares": 150}]) for f in FILERS[:3]}
    assert ow.aggregate_changes(per, filers=None, split_tickers=frozenset({"OTHER"}))["XYZ"]["flow"] == 1
    new = {f: ow.holding_changes([], [{"ticker": "XYZ", "shares": 100}]) for f in FILERS[:4]}
    assert ow.aggregate_changes(new, filers=None, split_tickers=frozenset({"XYZ"}))["XYZ"]["flow"] == 1


def test_previous_period_steps_back_one_quarter_end() -> None:
    assert ow.previous_period("2026-06-30") == "2026-03-31"
    assert ow.previous_period("2026-03-31") == "2025-12-31"
    assert ow.previous_period("2026-09-30") == "2026-06-30"
    assert ow.previous_period("2026-12-31") == "2026-09-30"


def test_only_share_count_actions_become_split_dates() -> None:
    body = {"corporate_actions": {
        "forward_splits": [{"symbol": "DXJ", "ex_date": "2026-10-09", "new_rate": 3, "old_rate": 1}],
        "reverse_splits": [{"symbol": "ABC", "ex_date": "2026-09-01"}, {"symbol": "ABC", "ex_date": "2026-01-02"}],
        "cash_dividends": [{"symbol": "KO", "ex_date": "2026-09-01"}],
        "spin_offs": [{"source_symbol": "GE", "ex_date": "2026-04-02"}],
        "forward_splits_future": [],
    }}
    assert split_events(body, date(2026, 11, 11)) == {"DXJ": ["2026-10-09"], "ABC": ["2026-01-02", "2026-09-01"]}
    assert split_events({"corporate_actions": {"forward_splits": [{"symbol": "NEW", "ex_date": "2027-01-01"}]}}, date(2026, 11, 11)) == {}


def test_the_history_and_live_paths_both_pass_the_splits() -> None:
    hist = (ROOT / "services/collector-filings/ownership_history.py").read_text(encoding="utf-8")
    live = (ROOT / "services/collector-filings/ownership.py").read_text(encoding="utf-8")
    tradfi = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "split_tickers_in(splits, prev_p, p)" in hist and "flows(per_period, splits)" in hist
    assert "split_tickers=in_split" in live and "previous_period(period)" in live
    assert "SPLIT_DATES_KEY" in tradfi and "split_events(body, today)" in tradfi
