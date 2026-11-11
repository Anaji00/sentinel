"""The rebuilt insider-cluster rule is the live agent's: 2+ distinct buyers, $250k net, 14 days, one per window."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.insider_clusters import FormFourTrade as T, event_id, find_clusters, trade_usd  # noqa: E402


def buy(day, who, usd, planned=False):
    return T(date(2024, 3, day), "P", planned, who, usd)


def sell(day, who, usd, planned=False):
    return T(date(2024, 3, day), "S", planned, who, usd)


def test_two_buyers_and_enough_dollars_fire_on_the_second_filing_day():
    out = find_clusters("ABC", [buy(1, "A", 100_000), buy(5, "B", 200_000)])
    assert [(c.day, c.buyers, c.net_buy_usd) for c in out] == [(date(2024, 3, 5), ["A", "B"], 300_000.0)]


def test_one_buyer_or_too_little_money_does_not_fire():
    assert find_clusters("ABC", [buy(1, "A", 400_000), buy(2, "A", 400_000)]) == []
    assert find_clusters("ABC", [buy(1, "A", 100_000), buy(2, "B", 100_000)]) == []


def test_sales_in_the_window_reduce_the_net_and_planned_trades_count_neither_way():
    assert find_clusters("ABC", [sell(1, "C", 200_000), buy(2, "A", 200_000), buy(3, "B", 200_000)]) == []
    assert find_clusters("ABC", [sell(1, "C", 200_000, planned=True), buy(2, "A", 200_000), buy(3, "B", 200_000)])
    assert find_clusters("ABC", [buy(1, "A", 200_000), buy(2, "B", 200_000, planned=True)]) == []


def test_an_unsized_buy_counts_as_a_buyer_but_adds_no_dollars():
    out = find_clusters("ABC", [buy(1, "A", None), buy(2, "B", 300_000)])
    assert len(out) == 1 and out[0].net_buy_usd == 300_000.0


def test_trades_older_than_14_days_fall_out_of_the_window():
    assert find_clusters("ABC", [buy(1, "A", 200_000), buy(16, "B", 200_000)]) == []
    assert find_clusters("ABC", [buy(1, "A", 200_000), buy(15, "B", 200_000)])


def test_one_cluster_per_window_then_another_after_14_days():
    trades = [buy(1, "A", 200_000), buy(2, "B", 200_000), buy(3, "C", 200_000), buy(10, "D", 200_000),
              buy(17, "E", 300_000), buy(18, "F", 300_000)]
    assert [c.day.day for c in find_clusters("ABC", trades)] == [2, 17]


def test_helpers():
    assert trade_usd(100, 2.5) == 250.0
    assert trade_usd(None, 2.5) is None and trade_usd(100, 0) is None
    assert event_id("ABC", date(2024, 3, 5)) == "g3cluster:ABC:2024-03-05"
