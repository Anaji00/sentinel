"""The registered rule P1 live in the divergence watch (services/agents/peer_residual_scan.py)."""

import asyncio
import pathlib
import sys
from datetime import date, datetime, timedelta, timezone

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import divergence_watch as dw  # noqa: E402
from services.agents import peer_residual_scan as ps  # noqa: E402
from shared.utils import divergence as dv  # noqa: E402
from shared.utils import intel_brief as ib  # noqa: E402

SEMIS = ["AMD", "AVGO", "MRVL", "TSM", "MU"]
UNIVERSE = ["NVDA"] + SEMIS


def _watch(seed: int = 3):
    rng = np.random.default_rng(seed)
    w = dw.DivergenceWatch(None, None)
    base = rng.normal(0, 0.012, (60, 1))
    w._rets = base + rng.normal(0, 0.006, (60, len(UNIVERSE)))
    w._col = {t: i for i, t in enumerate(UNIVERSE)}
    w._refs = {t: {"industry": [x for x in UNIVERSE if x != t]} for t in UNIVERSE}
    return w


NVDA_DAY = {"NVDA": -0.043, "AMD": 0.133, "AVGO": 0.088, "MRVL": 0.095, "TSM": 0.035, "MU": 0.0625}


def test_stage_one_finds_the_nvda_day_and_nothing_in_a_broad_rally() -> None:
    w = _watch()
    cands = ps.stage_one(w, NVDA_DAY, frozenset({"NVDA"}))
    assert [c["ticker"] for c in cands] == ["NVDA"] and cands[0]["breadth"] == 1.0 and cands[0]["news"] is True
    rally = {t: 0.03 for t in UNIVERSE}
    assert ps.stage_one(w, rally, frozenset()) == []


def _slot_rows(ticker_ret, sigma_scale=1.0, volumes=1_000_000.0, days=20):
    """Synthetic earlier sessions at the same clock time: small moves, so a 4% gap is large against them."""
    rng = np.random.default_rng(11)
    rows, closes = [], []
    base = date(2026, 10, 1)
    for k in range(days):
        d = base + timedelta(days=k)
        common = rng.normal(0, 0.004 * sigma_scale)
        for t in UNIVERSE:
            prev = 100.0
            px = prev * (1 + common + rng.normal(0, 0.002 * sigma_scale))
            rows.append({"ticker": t, "d": d, "px": px, "cumvol": volumes})
            closes.append({"ticker": t, "day": d - timedelta(days=1), "close": prev})
    return rows, closes


def test_stage_two_confirms_with_volume_and_refuses_without_it_or_with_thin_history() -> None:
    w = _watch()
    cands = ps.stage_one(w, NVDA_DAY, frozenset({"NVDA"}))
    rows, closes = _slot_rows(NVDA_DAY)
    hits = ps.confirm_session(cands, rows, closes, {"NVDA": 3_000_000.0})
    assert len(hits) == 1 and hits[0].horizon == "session" and hits[0].z < -2.5 and hits[0].rvol == 3.0
    assert hits[0].tier == "ELEVATED" or hits[0].tier == "ALERT"
    assert ps.confirm_session(cands, rows, closes, {"NVDA": 1_000_000.0}) == []             # volume does not confirm (1.0x)
    assert ps.confirm_session(cands, rows, closes, {}) == []                                 # no volume figure: not a pass
    assert ps.confirm_session(cands, rows[:8 * len(UNIVERSE)], closes, {"NVDA": 3e6}) == []  # too few earlier sessions


def test_the_five_day_horizon_fires_on_a_stock_that_lagged_its_group_all_week() -> None:
    w = _watch()
    w._rets[-4:, 0] = -0.012                                    # NVDA down about 5% over four sessions
    w._rets[-4:, 1:] = 0.012                                    # its group up
    day = {"NVDA": -0.01, **{t: 0.012 for t in SEMIS}}
    hits = ps.five_day(w, day, frozenset())
    assert [h.ticker for h in hits] == ["NVDA"] and hits[0].horizon == "5d" and hits[0].rvol is None
    assert hits[0].ret < -0.04 and hits[0].ref_ret > 0.04
    assert ps.five_day(_watch(), {t: 0.0 for t in UNIVERSE}, frozenset()) == []              # an ordinary week fires nothing


def test_merge_prefers_the_registered_reading_one_alert_per_stock() -> None:
    d2 = dv.Divergence("NVDA", "industry", -0.043, 0.077, -0.12, -5.0, True, ("AMD",))
    other = dv.Divergence("ZZZ", "sympathy", -0.05, 0.03, -0.08, -3.2, False, ("A",))
    p1a = dv.PeerDivergence("NVDA", "industry", -0.043, 0.077, -0.09, -4.1, True, ("AMD",), horizon="session", breadth=1.0, beta=0.9, rvol=3.0)
    p1b = dv.PeerDivergence("NVDA", "industry", -0.06, 0.05, -0.1, -5.5, True, ("AMD",), horizon="5d", breadth=0.9, beta=1.0)
    out = {d.ticker: d for d in ps.merge([d2, other], [p1a, p1b])}
    assert set(out) == {"NVDA", "ZZZ"} and out["NVDA"].horizon == "5d" and out["ZZZ"] is other


def test_the_text_states_the_horizon_beta_and_volume_and_the_brief_follows_it() -> None:
    p = dv.PeerDivergence("NVDA", "industry", -0.043, 0.077, -0.09, -4.1, True, ("AMD", "AVGO"), horizon="session", breadth=0.83, beta=0.9, rvol=3.9)
    assert "today" in p.headline() and "beta" in p.headline() and "-4.3%" in p.headline()
    assert "83%" in p.description() and "3.9 times" in p.description()
    five = dv.PeerDivergence("NVDA", "industry", -0.06, 0.05, -0.1, -5.5, False, ("AMD",), horizon="5d", breadth=0.9, beta=1.0)
    b = ib.build(five, None, None)
    assert "last five sessions" in b["what_happened"] and "unexplained after its beta" in b["what_happened"]


def test_a_p1_failure_is_counted_and_the_d2_detection_goes_on(monkeypatch) -> None:
    w = _watch()

    def boom(*a, **k):
        raise RuntimeError("history query failed")

    monkeypatch.setattr(ps, "five_day", boom)
    monkeypatch.setattr(ps, "in_session", lambda now: False)
    assert asyncio.run(w._peer_residual(NVDA_DAY, frozenset(), datetime(2026, 11, 11, 16, 0, tzinfo=timezone.utc))) == []


def test_it_is_wired_into_the_scan_and_the_session_form_is_regular_hours_only() -> None:
    src = (ROOT / "services/agents/divergence_watch.py").read_text(encoding="utf-8")
    assert "found = peer_residual_scan.merge(found, p1)" in src and "horizon:" in src
    assert ps.in_session(datetime(2026, 11, 11, 10, 0)) and not ps.in_session(datetime(2026, 11, 11, 9, 40))
    assert not ps.in_session(datetime(2026, 11, 14, 11, 0))                               # Saturday


def test_the_brief_states_the_bar_of_the_rule_that_fired_not_a_fixed_three() -> None:
    p = dv.PeerDivergence("IRM", "industry", -0.03, 0.02, -0.04, -2.6, False, ("A", "B", "C"), horizon="session", breadth=0.8, beta=1.0, rvol=1.5)
    text = ib.build(p, None, None)["evidence"]
    assert "2.6 times" in text and "requires 2.5" in text and "requires 3" not in text and "not been measured" in text
    five = dv.PeerDivergence("X", "industry", -0.06, 0.05, -0.1, -2.8, False, ("A",), horizon="5d", breadth=0.9, beta=1.0)
    assert "five-session rule requires 2.5" in ib.build(five, None, None)["evidence"]
    d2 = dv.Divergence("NVDA", "industry", -0.043, 0.077, -0.12, -5.0, True, ("AMD",))
    old = ib.build(d2, None, None)["evidence"]
    assert "requires 3." in old and "t 1.6, 0.2" in old
