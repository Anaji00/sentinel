"""Ideas add up to a book with measured beta, risk, concentration, liquidity and cost."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.book import build_book  # noqa: E402


def _ideas():
    return [
        {"ticker": "A", "direction": 1, "rv": 0.20, "beta": 1.2, "adv_usd": 5e8, "industry": "Semis"},
        {"ticker": "B", "direction": 1, "rv": 0.40, "beta": 1.5, "adv_usd": 2e7, "industry": "Semis"},
        {"ticker": "C", "direction": -1, "rv": 0.30, "beta": 0.8, "adv_usd": 1e8, "industry": "Banks"},
    ]


def test_inverse_vol_weights_and_balanced_sides() -> None:
    rng = np.random.default_rng(1)
    rets = {t: list(rng.normal(0, 0.01, 40)) for t in "ABC"}
    b = build_book(_ideas(), rets)
    w = {p["ticker"]: p["weight"] for p in b["positions"]}
    assert abs(w["A"] - 2 * w["B"]) < 1e-3          # half the volatility, twice the weight
    assert abs(b["gross"] - 1.0) < 1e-6 and abs(b["net"]) < 1e-6
    assert abs(b["net_beta"] - (w["A"] * 1.2 + w["B"] * 1.5 + w["C"] * 0.8)) < 1e-3
    assert b["spy_hedge_usd"] == round(-b["net_beta"] * 10_000_000)
    assert b["ex_ante_vol"] is not None and b["ex_ante_vol"] > 0
    assert b["top_industry"] == "Semis" and abs(b["top_industry_share"] - 0.5) < 1e-6


def test_thin_names_cost_more_and_take_longer() -> None:
    b = build_book(_ideas(), {})
    pos = {p["ticker"]: p for p in b["positions"]}
    assert pos["B"]["cost_bps"] > pos["A"]["cost_bps"]
    assert pos["B"]["days_at_cap"] > pos["A"]["days_at_cap"]
    assert b["ex_ante_vol"] is None                      # no aligned returns, no risk number


def test_one_idea_is_not_a_book() -> None:
    assert build_book(_ideas()[:1], {}) is None
