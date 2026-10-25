"""Financials as they were known on each filing date: TTM, restatements, classes."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.fundamentals_pit import Fact, series, ttm  # noqa: E402


def F(concept, start, end, value, filed, accn="a"):
    return Fact(concept, start, end, value, filed, accn)


# An Apple-shaped fiscal year: quarters end late Dec, Mar, Jun; the year late Sep.
Q1 = (date(2024, 9, 29), date(2024, 12, 28))
Q2 = (date(2024, 12, 29), date(2025, 3, 29))
Q3 = (date(2025, 3, 30), date(2025, 6, 28))
FY = (date(2024, 9, 29), date(2025, 9, 27))
NINE = (date(2024, 9, 29), date(2025, 6, 28))
PRIOR_Q4 = (date(2024, 6, 30), date(2024, 9, 28))


def test_ttm_derives_the_fourth_quarter_from_the_year_less_nine_months() -> None:
    known = {Q1: 36.0, Q2: 25.0, Q3: 23.0, NINE: 84.0, FY: 112.0}
    end, value = ttm(known)
    assert end == FY[1] and value == 112.0          # Q1 + Q2 + Q3 + (112 - 84)
    known[(date(2025, 9, 28), date(2025, 12, 27))] = 42.0
    end, value = ttm(known)
    assert end == date(2025, 12, 27) and value == 25.0 + 23.0 + 28.0 + 42.0


def test_a_flow_is_known_from_its_filing_date_and_restated_only_from_the_restatement() -> None:
    facts = [
        F("NetIncomeLoss", *PRIOR_Q4, 14.0, date(2024, 11, 1), "k24"),
        F("NetIncomeLoss", *Q1, 36.0, date(2025, 1, 31), "q1"),
        F("NetIncomeLoss", *Q2, 25.0, date(2025, 5, 2), "q2"),
        F("NetIncomeLoss", *Q3, 23.0, date(2025, 8, 1), "q3"),
        # Q1 restated in the Q3 10-Q's comparatives would be a different
        # filing date; here a 10-Q/A on 2025-08-20 lowers it.
        F("NetIncomeLoss", *Q1, 30.0, date(2025, 8, 20), "q1a"),
    ]
    pts = dict(series(facts)["net_income"])
    assert pts[date(2025, 8, 1)] == 14.0 + 36.0 + 25.0 + 23.0
    assert pts[date(2025, 8, 20)] == 14.0 + 30.0 + 25.0 + 23.0
    assert date(2025, 7, 1) not in pts                # nothing new was filed then


def test_revenue_tags_are_chosen_per_period_across_a_tag_change() -> None:
    facts = [
        F("SalesRevenueNet", *PRIOR_Q4, 60.0, date(2024, 11, 1)),
        F("RevenueFromContractWithCustomerExcludingAssessedTax", *Q1, 120.0, date(2025, 1, 31)),
        F("RevenueFromContractWithCustomerExcludingAssessedTax", *Q2, 90.0, date(2025, 5, 2)),
        F("Revenues", *Q2, 95.0, date(2025, 5, 2)),          # preferred tag wins for that period
        F("RevenueFromContractWithCustomerExcludingAssessedTax", *Q3, 85.0, date(2025, 8, 1)),
    ]
    assert dict(series(facts)["revenue"])[date(2025, 8, 1)] == 60.0 + 120.0 + 95.0 + 85.0


def test_cover_page_shares_add_up_across_classes_and_assets_grow_year_on_year() -> None:
    facts = [
        F("EntityCommonStockSharesOutstanding", None, date(2025, 7, 20), 5.8, date(2025, 7, 25), "x"),
        F("EntityCommonStockSharesOutstanding", None, date(2025, 7, 20), 0.86, date(2025, 7, 25), "x"),
        F("EntityCommonStockSharesOutstanding", None, date(2025, 7, 20), 5.4, date(2025, 7, 25), "x"),
        F("Assets", None, date(2024, 6, 29), 100.0, date(2024, 8, 2), "y1"),
        F("Assets", None, date(2025, 6, 28), 110.0, date(2025, 8, 1), "y2"),
    ]
    s = series(facts)
    assert abs(dict(s["shares_out"])[date(2025, 7, 25)] - 12.06) < 1e-9
    assert abs(dict(s["asset_growth"])[date(2025, 8, 1)] - 0.10) < 1e-12
    assert dict(s["assets"])[date(2024, 8, 2)] == 100.0


def test_truncation_never_changes_the_past() -> None:
    """Freqtrade-style look-ahead check: arrays built from facts filed up to a
    date equal, on every row up to that date, arrays built from all facts."""
    import random
    from datetime import timedelta

    from services.agents.factor_engine import facts_arrays, facts_points

    rng = random.Random(4)
    days = [date(2024, 1, 1) + timedelta(days=k) for k in range(0, 700)]
    rows = []
    for t in ("AAA", "BBB", "CCC"):
        end = date(2023, 3, 31)
        for q in range(9):
            start, end = end + timedelta(days=1), end + timedelta(days=91)
            filed = end + timedelta(days=rng.randint(25, 60))
            for concept, v in (("NetIncomeLoss", rng.uniform(-5, 20)), ("Revenues", rng.uniform(50, 90))):
                rows.append({"ticker": t, "concept": concept, "period_start": start, "period_end": end,
                             "value": v, "filed": filed, "accn": f"{t}{q}"})
            rows.append({"ticker": t, "concept": "Assets", "period_start": None, "period_end": end,
                         "value": rng.uniform(100, 200), "filed": filed, "accn": f"{t}{q}"})
            if q == 5:   # a restatement of an earlier quarter, filed later
                rows.append({"ticker": t, "concept": "NetIncomeLoss", "period_start": start - timedelta(days=91),
                             "period_end": end - timedelta(days=91), "value": 1.0,
                             "filed": filed + timedelta(days=20), "accn": f"{t}{q}a"})
    tickers = ["AAA", "BBB", "CCC"]
    full = facts_arrays(days, tickers, facts_points(rows))
    for cut in (date(2024, 6, 1), date(2024, 11, 15), date(2025, 6, 30)):
        part = facts_arrays(days, tickers, facts_points([r for r in rows if r["filed"] <= cut]))
        k = sum(1 for d in days if d <= cut)
        for key, arr in full.items():
            other = part.get(key)
            if other is None:
                assert not (arr[:k] == arr[:k]).any(), key          # nothing known yet either way
                continue
            same = (arr[:k] == other[:k]) | (arr[:k] != arr[:k]) & (other[:k] != other[:k])
            assert same.all(), (key, cut)
