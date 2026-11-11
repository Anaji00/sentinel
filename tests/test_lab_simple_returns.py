"""The portfolio lab scores every book on simple returns, as walk_forward does for B1 (MODEL_TRIALS.md, 5 Nov 2026)."""

import inspect
import math
import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import portfolio_lab as pl  # noqa: E402


def test_the_lab_converts_collected_log_returns_before_scoring_any_book() -> None:
    src = inspect.getsource(pl.run)
    collect = src.index("fm.walk_forward(panel, model_name")
    convert = src.index("np.expm1(r)")
    first_book = src.index("res: Dict[str, Any] = compare(")
    assert collect < convert < first_book
    assert "math.expm1(f)" in src                                          # the sizing layer's picks too
    assert "ic_by_regime(log_preds" in src and "alpha_calibration(panel, log_preds" in src


def test_a_volatile_short_is_not_credited_with_half_its_variance() -> None:
    """Shorts that fall 50% then rise 50% on alternate periods: logs average to a gain, simple returns to a loss."""
    n, dates = 100, 12
    rng = np.random.default_rng(3)
    preds_log, preds_simple = [], []
    for d in range(dates):
        pred = np.arange(n, dtype=float)
        move = 0.405465 if d % 2 else -0.693147           # +50% then -50%, as log excess returns
        raw = np.zeros(n)
        raw[:20] = move                                    # the bottom fifth, which the book shorts
        raw += rng.normal(0, 1e-6, n)
        preds_log.append((d * 21, np.arange(n), pred, raw))
        preds_simple.append((d * 21, np.arange(n), pred, np.expm1(raw)))
    industry = np.zeros(n, dtype=int)
    on_logs = pl.compare(preds_log, industry, None, 21, 0.0)["live"]["gross_bps"]
    on_simple = pl.compare(preds_simple, industry, None, 21, 0.0)["live"]["gross_bps"]
    assert on_logs > 1000                                   # the log average books about +14% a period on the short
    assert abs(on_simple) < 1                               # +50% then -50% averages to nothing in simple returns


def test_hedged_books_add_the_market_back_as_a_simple_return() -> None:
    n = 100
    beta = np.ones((22, n))
    pred = np.arange(n, dtype=float)
    raw = np.zeros(n)                                       # every stock moves with SPY exactly
    market = np.full(22, np.nan)
    market[0] = math.log(1.10)                              # SPY +10%
    out = pl.compare([(0, np.arange(n), pred, raw)] * 4, np.zeros(n, dtype=int), beta, 21, 0.0, market)
    # Stocks equal SPY, betas are 1, so a book hedged to SPY earns exactly nothing, in simple returns.
    assert abs(out["short_hedged"]["gross_bps"]) < 1e-6 and abs(out["beta_neutral"]["gross_bps"]) < 1e-6
