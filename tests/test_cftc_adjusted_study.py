"""Amendment DS-1: an instrument that drifts against SPY on every week must not look like a positioning signal once its own baseline is removed."""
import importlib.util
import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("study_cftc_cot_under_test", ROOT / "scripts" / "study_cftc_cot.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def _world():
    start = date(2020, 1, 7)
    weekly = [(start + timedelta(weeks=i), start + timedelta(weeks=i, days=3), 0.01 * (i % 7)) for i in range(260)]
    days = [date(2020, 1, 1) + timedelta(days=i) for i in range(1900) if (date(2020, 1, 1) + timedelta(days=i)).weekday() < 5]
    # the ETF loses 0.2% a session against a flat SPY: pure drift, the same in every week
    etf = (list(days), [100.0 * (0.998 ** i) for i in range(len(days))])
    spy = (list(days), [100.0] * len(days))
    return weekly, {"USO": etf, "SPY": spy}, dict(zip(*spy))


def test_pure_instrument_drift_is_removed_by_the_baseline():
    weekly, prices, bench = _world()
    raw = mod.build_events({"067411": weekly}, prices, bench)
    assert raw and sum(r for _, _, r in raw) / len(raw) < -0.005            # unadjusted, every event is 'significantly' negative
    adj, base = mod.build_adjusted({"067411": weekly}, prices, bench)
    assert base["energy"] < -0.005
    assert abs(sum(r for _, _, r in adj) / len(adj)) < 1e-3                  # adjusted, the drift is gone


def test_the_contrast_needs_100_events_a_side_and_reports_sign():
    rows = [("energy:crowded_long", date(2020, 1, 1) + timedelta(days=i), -0.01) for i in range(50)]
    assert mod.contrast(rows)[0]["verdict"].startswith("under 100 events")
    long_ = [("energy:crowded_long", date(2020, 1, 1) + timedelta(days=i), -0.02 + 0.001 * (i % 5)) for i in range(150)]
    short = [("energy:crowded_short", date(2020, 1, 1) + timedelta(days=i), 0.02 - 0.001 * (i % 5)) for i in range(150)]
    c = mod.contrast(long_ + short)[0]
    assert c["long_minus_short_bps"] < 0 and c["passes"] is False            # the wrong direction for the registered rule is never a pass
