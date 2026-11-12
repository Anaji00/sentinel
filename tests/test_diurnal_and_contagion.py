"""Same-time-of-day robust scoring and damped multi-hop contagion (shared/utils/diurnal.py, shared/utils/contagion.py)."""

import pathlib
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import contagion as cg  # noqa: E402
from shared.utils import diurnal as dn  # noqa: E402

ET = ZoneInfo("America/New_York")


def test_the_seven_buckets_cover_the_day_in_eastern_time() -> None:
    cases = {(9, 0): "before the open", (9, 45): "opening", (10, 30): "morning", (12, 0): "midday", (15, 0): "afternoon", (15, 45): "closing", (17, 0): "after the close"}
    for (h, m), want in cases.items():
        assert dn.bucket_of(datetime(2026, 11, 11, h, m, tzinfo=ET)) == want
    assert dn.bucket_of(datetime(2026, 11, 11, 15, 30, tzinfo=timezone.utc)) == "morning"        # 15:30 UTC is 10:30 Eastern


def test_robust_z_ignores_outliers_in_the_baseline_and_needs_enough_history() -> None:
    base = [100.0 + (i % 5) for i in range(40)] + [10_000.0]                 # one wild value does not move a median/MAD scale
    z = dn.robust_z(130.0, base)
    assert z is not None and 3 < z < 40                    # the scale floor (5% of the median) governs a tight baseline
    assert dn.robust_z(100.0, [100.0] * 10) is None
    flat = dn.robust_z(101.0, [100.0] * 40)
    assert flat is not None and flat < 1.0                                    # the scale floor stops a flat baseline exploding


def test_the_slot_baseline_scores_before_adding_so_it_never_sees_itself() -> None:
    sb = dn.SlotBaseline()
    t = datetime(2026, 11, 11, 11, 0, tzinfo=ET)
    zs = [sb.score_then_add("AAPL", t, 100.0 + (i % 4)) for i in range(35)]
    assert zs[0] is None and zs[29] is None and zs[30] is not None            # nothing until MIN_BASELINE earlier values exist
    spike = sb.score_then_add("AAPL", t, 500.0)
    assert spike is not None and spike > 10
    assert sb.size("AAPL", t) == 36 and sb.size("AAPL", datetime(2026, 11, 11, 15, 0, tzinfo=ET)) == 0      # buckets are separate


def test_edge_weight_is_a_noisy_or_bounded_by_one() -> None:
    assert cg.edge_weight() == 0.0 and cg.edge_weight(0.5) == 0.5
    assert abs(cg.edge_weight(0.5, 0.5) - 0.75) < 1e-9 and cg.edge_weight(2.0, 5.0, -3.0) == 1.0
    assert abs(cg.edge_weight(partial_corr=-0.4) - 0.4) < 1e-9


def test_a_shock_decays_over_hops_keeps_its_path_and_cycles_do_not_amplify() -> None:
    edges = {"MINE": {"SUP": 0.8}, "SUP": {"MAKER": 0.6, "MINE": 0.9}, "MAKER": {"RETAIL": 0.5, "SUP": 0.9}, "RETAIL": {}}
    out = cg.propagate(edges, {"MINE": -1.0}, gamma=0.5, hops=3)
    assert set(out) == {"SUP", "MAKER", "RETAIL"}                             # the origin is not returned
    assert abs(out["SUP"]["score"] - (-1.0 * 0.5 * 0.8)) < 1e-9 and out["SUP"]["hops"] == 1
    assert abs(out["MAKER"]["score"]) < abs(out["SUP"]["score"]) and abs(out["RETAIL"]["score"]) < abs(out["MAKER"]["score"])
    assert out["RETAIL"]["hops"] == 3 and [(a, b) for a, b, _ in out["RETAIL"]["path"]] == [("MINE", "SUP"), ("SUP", "MAKER"), ("MAKER", "RETAIL")]
    assert cg.propagate(edges, {"MINE": -1.0}, hops=1).keys() == {"SUP"}


def test_only_names_that_have_not_yet_moved_become_second_order_candidates() -> None:
    edges = {"OIL": {"AIRLINE": 0.8, "REFINER": 0.8}}
    out = cg.propagate(edges, {"OIL": 0.10}, gamma=1.0, hops=1)                # +10% oil: airline and refiner exposed with the same sign here
    rows = cg.unadjusted(out, {"AIRLINE": 0.07, "REFINER": 0.005}, min_score=0.05)
    assert [r["ticker"] for r in rows] == ["REFINER"]                          # the airline already moved 88% of it
    assert rows[0]["path"] == [("OIL", "REFINER", 0.8)] and cg.unadjusted(out, {}, min_score=0.5) == []


def test_derived_back_filled_events_are_not_hawkes_arrivals_but_fast_real_streams_are_untouched() -> None:
    import asyncio
    import json

    from services.correlation.hawkes_correlator import CrossDomainHawkesCorrelator as HawkesCorrelator

    class _Raw:
        async def zrange(self, key, hi, lo, **kw):
            if kw.get("offset"):
                return []
            rows = [{"type": "earnings_reaction", "occurred_at": 1000.0 + i / 1000} for i in range(50)]          # a 50-row batch
            rows += [{"type": "crypto_liquidation", "occurred_at": 1000.0 + i * 0.2} for i in range(30)]          # a genuine fast stream
            return [json.dumps(r) for r in rows]

    class _Redis:
        raw = _Raw()

    h = HawkesCorrelator.__new__(HawkesCorrelator)
    h._redis, h._db = _Redis(), None
    h.FIT_WINDOW_HOURS = 1
    streams = asyncio.run(h._load_historical_events())
    assert sum(len(v) for v in streams.values()) == 30
    assert "earnings_reaction" in HawkesCorrelator.DERIVED_TYPES
