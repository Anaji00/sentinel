"""The four-stage situation and the seven-section brief of a divergence (shared/utils/situation.py, intel_brief.py)."""

import asyncio
import json
import pathlib
import random
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import divergence as dv  # noqa: E402
from shared.utils import intel_brief as ib  # noqa: E402
from shared.utils import situation as sit  # noqa: E402


def test_a_first_flag_develops_a_second_session_confirms_and_a_wider_gap_escalates() -> None:
    s = sit.on_detection(None, -3.4, True, "2026-11-09")
    assert s["stage"] == sit.DEVELOPING and s["number"] == 1 and s["sessions"] == 1
    s = sit.on_detection(s, -3.6, True, "2026-11-09")                 # same session, little change: unchanged
    assert s["stage"] == sit.DEVELOPING and s["sessions"] == 1
    s = sit.on_detection(s, -3.9, True, "2026-11-10")
    assert s["stage"] == sit.CONFIRMED and s["sessions"] == 2
    s = sit.on_detection(s, -5.6, True, "2026-11-10")                 # 5.6 >= peak 3.9 + 1.5
    assert s["stage"] == sit.ESCALATING and s["peak_z"] == 5.6


def test_a_closed_gap_resolves_and_a_return_starts_a_new_situation() -> None:
    s = sit.on_detection(None, -4.0, False, "2026-11-09")
    assert sit.on_quiet(s, -2.0, False, "2026-11-10", 1) is None        # still wide: not resolved
    done = sit.on_quiet(s, -0.4, False, "2026-11-10", 1)
    assert done and done["stage"] == sit.RESOLVED and done["resolved_session"] == "2026-11-10"
    assert sit.on_quiet(done, -0.4, False, "2026-11-11", 2) is None     # nothing more to change
    again = sit.on_detection(done, -3.2, True, "2026-11-12")
    assert again["stage"] == sit.DEVELOPING and again["number"] == 2
    assert sit.on_quiet(s, None, True, "2026-11-10", 1)["stage"] == sit.RESOLVED     # moved with its reference
    assert sit.on_quiet(s, None, False, "2026-11-20", 11)["stage"] == sit.RESOLVED   # expired unseen
    assert sit.on_quiet(None, 0.0, True, "2026-11-10", 0) is None


def _nvda() -> dv.Divergence:
    rnd = random.Random(4)
    hist = [rnd.gauss(0, 0.02) for _ in range(60)]
    d = dv.detect("NVDA", -0.0427, {"industry": (0.077, hist, ("AMD", "AVGO", "MRVL", "TSM", "MU"))}, True)
    assert d is not None
    return d


def test_the_brief_has_seven_sections_in_order_and_says_what_it_does_not_know() -> None:
    d = _nvda()
    state = sit.on_detection(None, d.z, True, "2026-11-09")
    b = ib.build(d, {"headline": "Nvidia CEO admits affair; board opens inquiry", "n": 21}, state,
                 [("AMD", 0.133), ("AVGO", 0.088)])
    assert tuple(b) == ib.SECTIONS
    assert "21 article(s)" in b["catalyst"] and "Nvidia CEO" in b["catalyst"]
    assert b["peers"]["members"] == ["AMD +13.3%", "AVGO +8.8%"]
    assert b["positioning"].startswith(ib.NOT_MEASURED)                       # no flow is joined; it must not be invented
    assert any("Escalating" in w for w in b["watch"]) and any("Resolved" in w for w in b["watch"])
    assert "Situation developing" in " ".join(b["limits"])
    text = ib.render(b)
    assert text.count("\n") == 6 and text.startswith("What happened:")


def test_an_unexplained_gap_is_not_called_news_driven() -> None:
    d = dv.detect("X", -0.05, {"industry": (0.09, [random.Random(1).gauss(0, 0.02) for _ in range(60)], ("A", "B", "C"))}, False)
    assert d is not None
    b = ib.build(d, None, None)
    assert ib.NOT_MEASURED in b["catalyst"] and "close within a week" in b["catalyst"]
    assert b["peers"]["returns_known"] is False and "Stage: new" in b["limits"]


def test_the_watch_job_records_the_stage_closes_quiet_situations_and_survives_a_store_failure() -> None:
    from services.agents import divergence_watch as dw

    class Raw:
        def __init__(self, broken=False):
            self.kv, self.sets, self.broken = {}, set(), broken

        async def get(self, k):
            if self.broken:
                raise ConnectionError("down")
            return self.kv.get(k)

        async def set(self, k, v, ex=None, nx=False):
            self.kv[k] = v
            return True

        async def sadd(self, k, v):
            self.sets.add(v)

        async def srem(self, k, v):
            self.sets.discard(v)

        async def smembers(self, k):
            return set(self.sets)

    class R:
        def __init__(self, raw):
            self.raw = raw

    raw = Raw()
    w = dw.DivergenceWatch(None, R(raw))
    d = _nvda()
    state = asyncio.run(w._situation(d, "2026-11-09"))
    assert state and state["stage"] == "DEVELOPING" and "NVDA" in raw.sets
    assert json.loads(raw.kv["sentinel:situation:NVDA"])["number"] == 1
    # NVDA is no longer flagged, and a scan finds nothing to measure it against: expiry only.
    w._rets, w._col, w._refs = None, {}, {}
    assert asyncio.run(w._close_quiet(set(), {}, __import__("datetime").datetime(2026, 11, 12, 15, tzinfo=__import__("datetime").timezone.utc))) == 0
    broken = dw.DivergenceWatch(None, R(Raw(broken=True)))
    assert asyncio.run(broken._situation(d, "2026-11-09")) is None       # counted, not raised
