"""The industry-neutral shadow book: the same scores, ranked within industry."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import model_portfolio as mp  # noqa: E402


def test_each_industry_supplies_its_own_top_and_bottom() -> None:
    # Ten chip makers all ranked high, ten REITs all ranked low: the live book
    # would be long only chips and short only REITs.
    pct = {f"CHIP{i}": 0.8 + i * 0.01 for i in range(10)}
    pct.update({f"REIT{i}": 0.1 + i * 0.01 for i in range(10)})
    ind = {t: ("Semiconductors" if t.startswith("CHIP") else "REIT") for t in pct}
    rel = mp.industry_relative(pct, ind)
    long_book = mp.book(rel, [], 1)
    short_book = mp.book(rel, [], -1)
    assert sum(t.startswith("CHIP") for t in long_book) == sum(t.startswith("REIT") for t in long_book) == 2
    assert sum(t.startswith("CHIP") for t in short_book) == sum(t.startswith("REIT") for t in short_book) == 2
    assert mp.book(pct, [], 1) == sorted(f"CHIP{i}" for i in range(6, 10))       # the live construction


def test_small_and_unknown_industries_are_ranked_together() -> None:
    pct = {"A": 0.9, "B": 0.2, "C": 0.5}
    rel = mp.industry_relative(pct, {"A": "Tiny", "B": None})
    assert rel == {"B": 0.5 / 3, "C": 1.5 / 3, "A": 2.5 / 3}
    assert mp.industry_relative({}, {}) == {}
