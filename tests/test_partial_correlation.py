"""Direct links survive; the market and the sector do not."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.partial_correlation import (  # noqa: E402
    aligned_returns, ledoit_wolf_covariance, partial_network,
)


def _universe(n=500, seed=7):
    rng = np.random.default_rng(seed)
    m, sec, link = rng.normal(size=(3, n))
    eps = lambda: rng.normal(size=n) * 0.8  # noqa: E731
    rets = {
        "SPY": m,
        "XLK": 0.9 * m + sec + 0.2 * rng.normal(size=n),
        "A": m + sec + link + eps(),
        "B": m + sec + link + eps(),
        "C": m + sec + eps(),
        "D": m + sec + eps(),
        "E": m + eps(),
    }
    closes = {}
    for name, r in rets.items():
        p = 100 * np.cumprod(1 + 0.005 * r)
        closes[name] = {t: float(v) for t, v in enumerate(p)}
    return closes


def test_only_the_direct_link_survives() -> None:
    edges, stats = partial_network(_universe(), exclude_from_edges={"XLK"})
    found = {frozenset((e.a, e.b)) for e in edges}
    assert frozenset(("A", "B")) in found
    assert frozenset(("C", "D")) not in found, "a shared sector is not a direct link"
    assert not any("E" in pair for pair in found), "the market alone is not a link"
    assert not any("XLK" in pair for pair in found)
    ab = next(e for e in edges if {e.a, e.b} == {"A", "B"})
    assert ab.partial > 0.3 and ab.q_value <= 0.05


def test_raw_correlation_would_have_linked_everything() -> None:
    names, R = aligned_returns(_universe())
    c = np.corrcoef(R, rowvar=False)
    i, j = names.index("C"), names.index("D")
    assert c[i, j] > 0.5   # the false link pairwise discovery reports


def test_series_are_aligned_on_shared_timestamps_not_position() -> None:
    u = _universe(n=200)
    u["THIN"] = {t: v for t, v in u["A"].items() if t % 2 == 0}   # half the bars
    names, R = aligned_returns(u)
    assert "THIN" in names and R.shape[0] == 99   # 100 shared bars -> 99 returns


def test_shrinkage_fallback_matches_sklearn_shape() -> None:
    X = np.random.default_rng(1).normal(size=(80, 6))
    cov = ledoit_wolf_covariance(X)
    assert cov.shape == (6, 6) and np.all(np.linalg.eigvalsh(cov) > 0)


def test_short_gaps_are_carried_on_the_market_calendar() -> None:
    u = _universe(n=300)
    # A thin name missing one bar in five: kept, not allowed to shrink the panel.
    u["GAPPY"] = {t: v for t, v in u["A"].items() if t % 5 != 0}
    # A name missing most of the calendar: dropped.
    u["SPARSE"] = {t: v for t, v in u["A"].items() if t % 2 == 0 and t < 100}
    names, R = aligned_returns(u, reference="SPY")
    assert "GAPPY" in names and "SPARSE" not in names
    assert R.shape[0] >= 290
