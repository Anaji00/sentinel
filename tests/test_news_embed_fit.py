"""The news read's refit on monthly sufficient statistics."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import news_embed_fit as nf  # noqa: E402


def _months(k: int, rows: int, beta: np.ndarray, noise: float, seed: int = 1):
    rng = np.random.default_rng(seed)
    out = {}
    for m in range(k):
        X = rng.normal(size=(rows, nf.DIM)).astype(np.float32)
        X /= np.linalg.norm(X, axis=1, keepdims=True)
        y = X @ beta + rng.normal(0, noise, rows)
        s = nf.Stats()
        s.add(X, y)
        out[f"2025-{m + 1:02d}"] = (s, X, y)
    return out


def test_statistics_reproduce_a_direct_ridge_and_its_error() -> None:
    rng = np.random.default_rng(0)
    X = rng.normal(size=(500, nf.DIM)).astype(np.float32)
    y = rng.normal(size=500)
    a, b = nf.Stats(), nf.Stats()
    a.add(X[:200], y[:200])
    b.add(X[200:], y[200:])
    s = nf.total([a, b])
    beta = nf.solve(s, 10.0)
    direct = np.linalg.solve(X.T.astype(np.float64) @ X + 10.0 * np.eye(nf.DIM), X.T.astype(np.float64) @ y)
    assert np.allclose(beta, direct, atol=1e-8)
    assert abs(nf.sse(s, beta) - float(((X @ beta) - y) @ ((X @ beta) - y))) < 1e-6
    assert nf.unpack(nf.pack([0.5, -1.25])).tolist() == [0.5, -1.25]
    p = X @ beta
    assert abs(nf.corr(s, beta) - float(np.corrcoef(p, y)[0, 1])) < 1e-9


def test_folds_skip_the_adjacent_month_and_recover_a_signal() -> None:
    beta = np.zeros(nf.DIM)
    beta[:5] = 0.5
    data = _months(10, 4000, beta, noise=0.05)
    by = {m: v[0] for m, v in data.items()}
    months = sorted(by)
    got = nf.folds(months, by)
    assert sorted(got) == months[nf.MIN_TRAIN_MONTHS + 1:]
    ics = []
    for m, (b, _lam) in got.items():
        _s, X, y = data[m]
        ics.append(nf.rank_ic(X @ b, y))
    summ = nf.summary(ics, [0.01] * len(ics), 1)
    assert summ["mean_ic"] > 0.3 and nf.passes(summ)
    b, lam, sd = nf.final(by, [lam for _b, lam in got.values()])
    assert lam in nf.LAMBDAS and sd > 0 and float(b[:5].min()) > 10 * float(np.abs(b[5:]).max())
    assert nf.passes({"mean_ic": 0.01, "ic_t": 5.0}) is False
