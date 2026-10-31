"""10-K customer links: what gets read, what is kept, and the customer-momentum feature."""

import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import customer_links as cl  # noqa: E402


def test_names_followed_by_a_share_of_revenue_are_candidates() -> None:
    text = ("We design chips. During fiscal 2025, fiscal 2024, and fiscal 2023, Apple, through sales to multiple "
            "distributors and contract manufacturers, in the aggregate accounted for 67 %, 69 %, and 66 % of our net "
            "revenue. Net sales to the Company's largest customer, Walmart Stores, Inc. and its affiliates, were 26%, "
            "27% and 25% of consolidated net sales. Customer A accounted for 14% of revenue. "
            "Cleaning products 44 % 44 % of net sales by customer. Our headcount grew 4%.")
    found = dict(x for p in cl.passages(text) for x in cl.candidates(p))
    assert found["Apple"] == 67.0 and found["Walmart Stores, Inc"] == 26.0
    assert not any(n.startswith(("Customer", "Cleaning", "During", "Net")) for n in found)
    pg = ("Sales to Walmart Inc. and its affiliates represent approximately 16% of our total sales. Our largest "
          "customer, Walmart Inc. and its affiliates, accounted for consolidated net sales of approximately 16 % in 2026.")
    assert all(pct == 16.0 for p in cl.passages(pg) for _, pct in cl.candidates(p)) and cl.candidates(pg)
    assert cl.candidates("Revenue from Taiwan accounted for 18% of total revenue.") == []      # a place, not a customer
    assert cl.variants("Walmart Stores, Inc") == ["Walmart Stores, Inc", "Walmart Stores", "Walmart"]
    assert cl.variants("United Parcel Service")[-1] == "United Parcel"          # never the bare "United"


def test_the_models_names_must_be_in_the_passage() -> None:
    p = "Revenue from Lowe's Companies and The Home Depot together was 31% of net sales."
    got = cl.grounded([{"name": "Lowe's Companies", "pct": 16}, {"name": "Menards", "pct": 15},
                       {"name": "Customer B", "pct": 11}], p)
    # The name is in the passage; the 16% is not (the passage gives 31% for the
    # two together), so since 30 Oct 2026 the share is dropped, not believed.
    assert got == [("Lowe's Companies", None)]


def test_customer_momentum_follows_the_latest_filing() -> None:
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(500)]
    up = [100.0 * (1.001 ** i) for i in range(500)]
    flat = [50.0] * 500
    closes = {"WMT": (days, up), "TGT": (days, flat)}
    links = [(date(2025, 3, 1), "WMT", 20.0), (date(2025, 3, 1), "TGT", 20.0)]
    rows = cl.momentum_rows("SUPP", links, closes, [date(2025, 2, 1), date(2025, 4, 1), date(2026, 5, 1)])
    assert [r[1] for r in rows] == [date(2025, 4, 1)]              # none before filing, none once it is stale
    expected = 0.5 * (1.001 ** 21 - 1)
    assert abs(rows[0][3] - expected) < 1e-6 and rows[0][2] == cl.FEATURE


def test_customers_signals_carry_to_suppliers_by_revenue_share() -> None:
    d0, d1 = date(2025, 2, 1), date(2026, 2, 1)
    links = {"SUP": [(d0, "OLD", 50.0), (d1, "AAA", 30.0), (d1, "BBB", 10.0)]}
    values = {("AAA", "llm_news_30"): 0.5, ("BBB", "llm_news_30"): -0.5, ("OLD", "llm_news_30"): 1.0,
              ("BBB", "fil_change"): 0.01}
    rows = {(f, v) for _, _, f, v in cl.propagated_rows(links, values, date(2026, 10, 30))}
    # The latest filing's customers only (OLD dropped), weighted 30:10.
    assert rows == {("cust_news_30", 0.25), ("cust_fil_change", 0.01)}
    assert cl.propagated_rows(links, values, date(2024, 1, 1)) == []            # no filing yet
