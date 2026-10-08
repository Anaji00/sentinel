"""Quarter-on-quarter 13F changes, and flows only where institutions agree."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ownership import aggregate_changes, holding_changes  # noqa: E402


def test_small_rebalancing_is_ignored():
    ch = holding_changes([{"ticker": "A", "shares": 100}], [{"ticker": "A", "shares": 105}])
    assert ch == {}


def test_new_exit_add_cut():
    prev = [{"ticker": "A", "shares": 100}, {"ticker": "B", "shares": 100}, {"ticker": "C", "shares": 100}]
    curr = [{"ticker": "A", "shares": 150}, {"ticker": "C", "shares": 50}, {"ticker": "D", "shares": 10}]
    ch = holding_changes(prev, curr)
    assert ch["A"]["kind"] == "add" and ch["B"]["kind"] == "exit"
    assert ch["C"]["kind"] == "cut" and ch["D"]["kind"] == "new"


ACTIVE = ["FMR LLC", "T. Rowe Price Associates", "Capital International Investors",
          "Wellington Management Group", "Berkshire Hathaway Inc", "Pershing Square Capital Management"]


def test_a_flow_needs_three_active_managers_moving_together():
    two = aggregate_changes({ACTIVE[0]: {"X": {"kind": "add", "change": 0.5}}, ACTIVE[1]: {"X": {"kind": "new"}}})
    assert two["X"]["flow"] == 0
    three = aggregate_changes({a: {"X": {"kind": "add", "change": 0.5}} for a in ACTIVE[:3]})
    assert three["X"]["flow"] == 1


def test_a_split_vote_is_not_a_flow():
    """Five buyers against three sellers was being called accumulation."""
    mixed = aggregate_changes({
        **{a: {"X": {"kind": "add", "change": 0.5}} for a in ACTIVE[:4]},
        **{a: {"X": {"kind": "cut", "change": -0.5}} for a in ACTIVE[4:6]},
    })
    assert mixed["X"]["flow"] == 1  # 4 vs 2: two to one
    mixed = aggregate_changes({
        **{a: {"X": {"kind": "add", "change": 0.5}} for a in ACTIVE[:3]},
        **{a: {"X": {"kind": "cut", "change": -0.5}} for a in ACTIVE[3:5]},
    })
    assert mixed["X"]["flow"] == 0  # 3 vs 2


def test_index_and_dealer_books_do_not_count():
    churn = aggregate_changes({n: {"X": {"kind": "add", "change": 0.5}} for n in (
        "BlackRock, Inc.", "Vanguard Group", "Citadel Advisors", "Goldman Sachs Group", "JPMorgan Chase & Co")})
    assert churn["X"]["flow"] == 0 if "X" in churn else True


def test_the_collector_publishes_flows_once_per_ticker_and_period():
    src = (ROOT / "services/collector-filings/ownership.py").read_text(encoding="utf-8")
    assert 'f"sentinel:ownership:flow_published:{period}:{ticker}"' in src
    assert "nx=True" in src.split("async def publish_flows")[1]


def test_a_small_add_does_not_count_toward_a_flow():
    small = aggregate_changes({a: {"X": {"kind": "add", "change": 0.12}} for a in ACTIVE[:4]})
    assert small.get("X", {"flow": 0})["flow"] == 0


def test_passive_and_systematic_books_do_not_count():
    churn = aggregate_changes({n: {"X": {"kind": "new"}} for n in (
        "Invesco Ltd.", "Bridgewater Associates", "Geode Capital Management", "Renaissance Technologies")})
    assert churn.get("X", {"flow": 0})["flow"] == 0
