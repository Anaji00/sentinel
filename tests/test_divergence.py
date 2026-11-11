"""A stock against its peers on its own news (shared/utils/divergence.py). The NVDA case of 9 Nov 2026 is the spec."""

import math
import pathlib
import random
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import divergence as dv  # noqa: E402


def quiet_history(n: int = 60, sd: float = 0.02, seed: int = 4) -> list:
    rnd = random.Random(seed)
    return [rnd.gauss(0, sd) for _ in range(n)]


SEMIS = ("AMD", "AVGO", "MRVL", "TSM", "MU")


def test_the_nvda_case_fires_as_elevated_news_divergence() -> None:
    refs = {"industry": (0.077, quiet_history(), SEMIS), "sympathy": (0.048, quiet_history(sd=0.0324, seed=9), ("TSM", "AAOI"))}
    d = dv.detect("NVDA", -0.0427, refs, news=True)
    assert d is not None and d.reference == "industry" and d.kind == "news"
    assert d.tier == "ELEVATED" and d.z < -4
    text = d.headline()
    assert "NVDA -4.3%" in text and "+7.7%" in text and "news naming it" in text
    assert "AMD" in d.description() and "gap of -11.97%" in d.description()


def test_it_needs_opposite_directions_and_real_moves() -> None:
    refs = {"industry": (0.077, quiet_history(), SEMIS)}
    assert dv.detect("X", 0.04, refs, True) is None                      # same direction: the group moved with it
    assert dv.detect("X", -0.015, refs, True) is None                    # the stock barely moved
    assert dv.detect("X", -0.04, {"industry": (0.004, quiet_history(), SEMIS)}, True) is None    # the group barely moved
    assert dv.detect("X", -0.04, {"industry": (0.077, quiet_history(sd=0.2), SEMIS)}, True) is None  # a gap this size is normal for it


def test_the_larger_gap_of_two_references_is_reported_and_unexplained_is_an_alert_not_elevated() -> None:
    refs = {"sympathy": (0.03, quiet_history(), ("A", "B", "C")), "industry": (0.09, quiet_history(), SEMIS)}
    d = dv.detect("X", -0.05, refs, news=False)
    assert d is not None and d.reference == "industry" and d.kind == "unexplained" and d.tier == "ALERT"


def test_short_flat_or_broken_input_never_fires_and_never_raises() -> None:
    assert dv.gap_z(-0.1, quiet_history(10)) is None
    assert dv.gap_z(-0.1, [0.0] * 60) is None
    assert dv.gap_z(float("nan"), quiet_history()) is None
    assert dv.detect("X", float("nan"), {}, True) is None
    assert dv.detect("X", -0.05, {"industry": (None, quiet_history(), SEMIS)}, True) is None
    assert dv.detect("X", -0.05, {"industry": (float("inf"), quiet_history(), SEMIS)}, True) is None
    assert dv.mean_return([0.01, None, float("nan"), 0.03]) is None            # two usable members is not a reference
    assert math.isclose(dv.mean_return([0.01, 0.02, 0.03, None]) or 0.0, 0.02)


def test_the_watch_job_is_wired_into_the_heavy_tier() -> None:
    src = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
    assert "DivergenceWatch" in src and src.index("DivergenceWatch(") > src.index("SignalEngine(")
    job = (ROOT / "services/agents/divergence_watch.py").read_text(encoding="utf-8")
    assert "AlertTier.ELEVATED" in job and "AlertTier.INTELLIGENCE" not in job        # no 7B scenario synthesis for a price gap
    assert "save_correlation" in job and job.index("save_correlation") < job.index("Topics.CORRELATIONS")    # persist before publish


# ── the job's own pure parts and its publish path, with fakes ────────────────────────────────

import asyncio  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from services.agents import divergence_watch as dw  # noqa: E402


def test_news_window_starts_at_the_previous_weekdays_close_and_weekends_do_not_scan() -> None:
    mon = datetime(2026, 11, 9, 9, 40, tzinfo=timezone.utc)                    # Monday, pre-market
    assert dw.previous_close_cutoff(mon) == datetime(2026, 11, 6, 21, 0, tzinfo=timezone.utc)      # Friday 16:00 New York (EST)
    tue = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)
    assert dw.previous_close_cutoff(tue) == datetime(2026, 11, 9, 21, 0, tzinfo=timezone.utc)
    assert dw.in_scan_window(mon) and not dw.in_scan_window(datetime(2026, 11, 8, 15, 0, tzinfo=timezone.utc))
    assert not dw.in_scan_window(datetime(2026, 11, 9, 23, 0, tzinfo=timezone.utc))


def test_industry_groups_need_enough_other_members() -> None:
    sic = {f"S{i}": 3674 for i in range(8)}
    sic.update({"LONE": 9999, "A": 1111, "B": 1111})
    g = dw.group_members(sic, list(sic))
    assert "S0" in g and "S0" not in g["S0"] and len(g["S0"]) == 7
    assert "LONE" not in g and "A" not in g


class _Raw:
    def __init__(self) -> None:
        self.h: dict = {}

    async def hgetall(self, key: str) -> dict:
        return dict(self.h.get(key, {}))

    async def hset(self, key: str, mapping: dict) -> None:
        self.h.setdefault(key, {}).update({k: str(v) for k, v in mapping.items()})

    async def expire(self, *_: object) -> None:
        return None


class _Redis:
    def __init__(self) -> None:
        self.raw = _Raw()


class _Producer:
    def __init__(self) -> None:
        self.sent: list = []

    async def send(self, topic: str, payload: dict, key: str = "") -> None:
        self.sent.append((topic, payload, key))


def _div(z: float = -5.0, news: bool = True) -> dv.Divergence:
    return dv.Divergence("NVDA", "industry", -0.0427, 0.077, -0.1197, z, news, SEMIS)


def test_one_alert_a_day_unless_the_gap_grows_and_it_is_saved_before_it_is_sent(monkeypatch) -> None:
    saved: list = []

    async def fake_save(self, cluster) -> None:       # noqa: ANN001
        saved.append(cluster)
        assert len(prod.sent) == len(saved) - 1, "published before it was persisted"

    monkeypatch.setattr(dw.EventStore, "save_correlation", fake_save)
    watch = dw.DivergenceWatch(None, _Redis())
    prod = _Producer()
    watch._producer = prod                                              # type: ignore[assignment]
    now = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)
    art = {"headline": "Nvidia's Huang Admits Affair", "n": 22}
    assert asyncio.run(watch._publish(_div(-5.0), now, art)) is True
    assert asyncio.run(watch._publish(_div(-5.2), now, art)) is False               # same gap: held back
    assert asyncio.run(watch._publish(_div(-7.0), now, art)) is True                # grew by more than the step
    assert asyncio.run(watch._publish(_div(-9.5), now, art)) is True
    assert asyncio.run(watch._publish(_div(-12.0), now, art)) is False              # three a day at most
    assert len(prod.sent) == 3 and len(saved) == 3
    c = saved[0]
    assert c.alert_tier.value == "ELEVATED" and c.rule_id == "DIVERGENCE_NEWS" and c.primary_entity_id == "NVDA"
    assert c.metrics_summary["gap"] == -0.1197 and c.supporting_headlines == ["Nvidia's Huang Admits Affair"]
    assert prod.sent[0][0] == dw.Topics.CORRELATIONS and prod.sent[0][2] == "NVDA"


def test_a_failed_save_is_counted_not_sent(monkeypatch) -> None:
    async def boom(self, cluster) -> None:            # noqa: ANN001
        raise RuntimeError("database down")

    monkeypatch.setattr(dw.EventStore, "save_correlation", boom)
    watch = dw.DivergenceWatch(None, _Redis())
    prod = _Producer()
    watch._producer = prod                                              # type: ignore[assignment]
    assert asyncio.run(watch._publish(_div(), datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc), None)) is False
    assert prod.sent == []


def test_stale_daily_bars_stop_the_scan_and_the_numpy_work_runs_off_the_loop(monkeypatch) -> None:
    watch = dw.DivergenceWatch(None, _Redis())
    watch._day = datetime(2026, 11, 9, tzinfo=timezone.utc).date()
    watch._tickers = ["A"]
    import numpy as np
    watch._rets = np.zeros((60, 1))
    watch._last_bar = datetime(2026, 10, 30, tzinfo=timezone.utc).date()             # ten days old
    out = asyncio.run(watch.scan(datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)))
    assert out.get("stale") == 1 and out["fired"] == 0
    src = (ROOT / "services/agents/divergence_watch.py").read_text(encoding="utf-8")
    assert "asyncio.to_thread(self._detect_all" in src


def test_detection_over_a_universe_finds_the_diverging_stock_only() -> None:
    import numpy as np
    watch = dw.DivergenceWatch(None, _Redis())
    tickers = ["NVDA", "AMD", "AVGO", "MRVL", "TSM", "MU"]
    rng = np.random.default_rng(1)
    base = rng.normal(0, 0.01, (60, 1))
    watch._rets = base + rng.normal(0, 0.005, (60, len(tickers)))
    watch._col = {t: i for i, t in enumerate(tickers)}
    watch._refs = {t: {"industry": [x for x in tickers if x != t]} for t in tickers}
    ret = {"NVDA": -0.043, "AMD": 0.133, "AVGO": 0.088, "MRVL": 0.095, "TSM": 0.035, "MU": 0.0625}
    found = watch._detect_all(ret, frozenset({"NVDA"}))
    assert [d.ticker for d in found] == ["NVDA"] and found[0].news and found[0].reference == "industry"
