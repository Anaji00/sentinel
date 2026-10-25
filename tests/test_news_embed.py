"""The news read: headline embeddings summed per stock and day, scored by trial M's ridge."""

import math
import pathlib
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import news_embed as ne  # noqa: E402


def test_an_article_counts_toward_the_close_it_was_known_by() -> None:
    # 15:59 New York (19:59 UTC in October) counts that day; 16:01 the next.
    assert ne.news_day(datetime(2026, 10, 16, 19, 59, tzinfo=timezone.utc)) == date(2026, 10, 16)
    assert ne.news_day(datetime(2026, 10, 16, 20, 1, tzinfo=timezone.utc)) == date(2026, 10, 17)
    # Friday's close known: Saturday through Monday's news all feed Monday's read.
    assert ne.days_since(date(2026, 10, 16), date(2026, 10, 19)) == [
        date(2026, 10, 17), date(2026, 10, 18), date(2026, 10, 19)]


def test_sums_merge_and_score_as_the_normalised_mean() -> None:
    a = ne.add(None, [1.0, 0.0])
    b = ne.add(a, [0.0, 1.0])
    assert b == {"n": 2, "v": [1.0, 1.0]}
    both = ne.merge([b, None, ne.add(None, [1.0, 0.0])])
    assert both == {"n": 3, "v": [2.0, 1.0]}
    beta = [0.02, -0.01]
    assert abs(ne.score(both, beta) - (0.04 - 0.01) / math.sqrt(5)) < 1e-12
    assert ne.score(both, [0.1]) is None and ne.score(None, beta) is None


def test_expected_bps_uses_the_out_of_sample_scale_and_clips() -> None:
    model = {"beta": [0.02, -0.01], "slope": 0.8, "pred_sd": 0.005}
    up = ne.expected_bps(ne.add(None, [1.0, 0.0]), model)
    assert up == round(0.8 * 0.01 * 1e4, 1)                       # 0.02 clipped to 2 sd = 0.01
    wild = ne.expected_bps(ne.add(None, [1.0, 0.0]), {**model, "beta": [0.5, 0.0]})
    assert wild == round(0.8 * 2 * 0.005 * 1e4, 1)                # clipped at two standard deviations
    assert ne.expected_bps(ne.add(None, [1.0, 0.0]), None) is None


def test_the_packaged_model_is_trial_ms_final_fit() -> None:
    m = ne.packaged_model()
    assert m is not None and len(m["beta"]) == 768 and m["horizon_days"] == ne.HORIZON_DAYS
    assert m["oos_ic_t"] >= 2.0 and 0 < m["slope"] < 2
