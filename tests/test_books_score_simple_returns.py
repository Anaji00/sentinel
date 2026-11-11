"""A book is scored on the simple return it earns; log returns flatter volatile shorts by about half their variance."""

import math
import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import factor_model as fm  # noqa: E402

SOURCE = (ROOT / "shared" / "utils" / "factor_model.py").read_text(encoding="utf-8")


def test_a_short_earns_less_than_its_log_return_says() -> None:
    # A stock that halves or doubles has a mean log return of zero, but a short loses money on average.
    logs = np.array([math.log(.5), math.log(2.0)])
    assert abs(float(-logs.mean())) < 1e-12
    assert float(-np.expm1(logs).mean()) == -0.25


def test_walk_forward_scores_books_on_simple_returns_and_keeps_the_log_figure() -> None:
    assert "raw = np.expm1(raw)" in SOURCE
    assert SOURCE.index("ls_log.append") < SOURCE.index("raw = np.expm1(raw)") < SOURCE.index("ls.append(float")
    assert '"net_sharpe_on_log_returns"' in SOURCE


def test_ranks_and_training_are_untouched() -> None:
    # IC is taken before the conversion, and the target stays a log return.
    assert SOURCE.index("ic = _spearman(pred, raw)") > SOURCE.index("target = forward_excess(panel, horizon)")
    assert "Next-`horizon`-day log return" in SOURCE
    assert hasattr(fm, "walk_forward")
