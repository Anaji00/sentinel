"""News embeddings into next-session returns, walked forward."""

import pathlib
import sys
from datetime import date, datetime, timezone

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import news_model as nm  # noqa: E402


def test_the_walk_forward_finds_a_planted_signal_and_rejects_noise() -> None:
    rng = np.random.default_rng(0)
    n, k = 6000, 32
    X = rng.normal(0, 1, (n, k)).astype(np.float32)
    day = np.repeat(np.arange(60), n // 60)
    y_sig = (0.004 * X[:, 0] + rng.normal(0, 0.01, n)).astype(np.float32)
    y_noise = rng.normal(0, 0.01, n).astype(np.float32)
    sig, noise = nm.walk_forward(day, X, y_sig), nm.walk_forward(day, X, y_noise)
    assert sig["validated"] and sig["mean_ic"] > 0.2
    assert not noise["validated"] and abs(noise["mean_ic"]) < 0.05
    w = nm.ridge(X, y_sig)
    assert len(w) == k + 1 and w[0] > 0


def test_samples_take_the_next_session_after_the_news_and_net_spy() -> None:
    from services.correlation import news_model_job as job

    days = [date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)]
    closes = [100.0, 102.0, 103.02]
    spy = {days[0]: 500.0, days[1]: 505.0, days[2]: 505.0}
    # Known at 11:00 New York on the 14th: the first close after is the 14th's.
    got = job.next_session_return(days, closes, spy, datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc))
    assert got[0] == days[1] and abs(got[1] - 0.01) < 1e-9
    assert job.next_session_return(days, closes, spy, datetime(2026, 10, 15, 21, 0, tzinfo=timezone.utc)) is None
    assert job.instruments_of("x", ["exposes:NVDA", "exposes:CL=F", "tradfi"]) >= {"NVDA"}
    assert "CL=F" not in job.instruments_of("x", ["exposes:CL=F"])
