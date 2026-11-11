"""Alert-based weights are fixed at the rebuild; only a hard risk-off veto acts between rebuilds (approved 11 Nov 2026)."""

import asyncio
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import auto_trader as atr  # noqa: E402
from shared.utils import auto_trade as at  # noqa: E402


def test_an_8k_with_a_delisting_bankruptcy_or_non_reliance_item_vetoes_a_book_name() -> None:
    v = at.veto_names(["AAA"], ["BBB"], [{"ticker": "AAA", "items": ["3.01", "9.01"]}, {"ticker": "BBB", "items": ["7.01"]},
                                         {"ticker": "ZZZ", "items": ["1.03"]}], [])
    assert v == {"AAA": "8-K item 3.01"}                     # BBB's item is routine; ZZZ is not in the book


def test_a_divergence_vetoes_only_when_three_sigma_and_against_the_position() -> None:
    divs = [{"ticker": "L1", "ret": -0.05, "z": -4.0},       # long that fell against its peers: adverse
            {"ticker": "L2", "ret": 0.05, "z": 4.0},         # long that rose: not adverse
            {"ticker": "S1", "ret": 0.05, "z": 4.0},         # short that rose: adverse
            {"ticker": "S2", "ret": -0.05, "z": -4.0},       # short that fell: not adverse
            {"ticker": "L3", "ret": -0.05, "z": -2.9},       # under 3 sigma
            {"ticker": "L4", "ret": None, "z": -5.0}]        # unreadable
    v = at.veto_names(["L1", "L2", "L3", "L4"], ["S1", "S2"], [], divs)
    assert set(v) == {"L1", "S1"} and "4.0 sigma" in v["L1"]


def test_a_veto_cuts_the_name_and_leaves_the_rest_of_the_leg_alone() -> None:
    w = at.target_weights(["A", "B", "C", "D"], ["X", "Y"], 0.5)
    cut = at.apply_vetoes(w, {"A": "8-K item 3.01"}, 0.0)
    assert cut["A"] == 0.0 and cut["B"] == w["B"] and cut["X"] == w["X"]        # nothing is spread over the others
    assert sum(abs(x) for x in cut.values()) < sum(abs(x) for x in w.values())  # gross exposure fell
    half = at.apply_vetoes(w, {"A": "x"}, 0.5)
    assert half["A"] == w["A"] / 2 and at.apply_vetoes(w, {}, 0.0) == w


def test_the_veto_scale_defaults_to_exit_and_is_bounded(monkeypatch) -> None:
    monkeypatch.delenv("B1_VETO_SCALE", raising=False)
    assert atr.veto_scale() == 0.0
    monkeypatch.setenv("B1_VETO_SCALE", "0.3")
    assert atr.veto_scale() == 0.3
    monkeypatch.setenv("B1_VETO_SCALE", "7")
    assert atr.veto_scale() == 1.0
    monkeypatch.setenv("B1_VETO_SCALE", "junk")
    assert atr.veto_scale() == 0.0


class Raw:
    def __init__(self, book) -> None:
        self.d = {atr.BOOK_KEY: json.dumps(book)}

    async def get(self, k):
        return self.d.get(k)

    async def set(self, k, v, ex=None):
        self.d[k] = v


class Db:
    def __init__(self, eight=(), divs=()) -> None:
        self.eight, self.divs = list(eight), list(divs)

    async def query(self, sql, *a):
        return self.eight if "events" in sql else self.divs


def _trader(book, eight=(), divs=()):
    t = atr.AutoTrader.__new__(atr.AutoTrader)
    t.raw, t.db = Raw(book), Db(eight, divs)
    return t


def test_weights_are_fixed_once_into_the_stored_book_and_not_recomputed_after() -> None:
    stored = {"rebalanced_on": "2026-10-29", "long": ["A", "B", "C"], "short": ["X", "Y"]}
    t = _trader(stored)
    calls = []

    async def fake_risk(names):
        calls.append(names)
        return {"A": 0.5, "B": 1.0, "C": 1.0}

    t._risk_weights = fake_risk                                    # type: ignore[assignment]
    filtered = {**stored, "long": ["A", "B"]}                      # the day's copy, picks and large caps only
    out = asyncio.run(t._freeze_alert_weights(filtered))
    assert out["risk_weights"] == {"A": 0.5} and len(calls) == 1
    saved = json.loads(t.raw.d[atr.BOOK_KEY])
    assert saved["risk_weights"] == {"A": 0.5} and saved["long"] == ["A", "B", "C"]      # the stored names were not overwritten
    again = asyncio.run(t._freeze_alert_weights({**saved}))
    assert again["risk_weights"] == {"A": 0.5} and len(calls) == 1                         # not recomputed until a rebuild


def test_a_veto_is_found_saved_to_the_book_and_stays_until_the_rebuild() -> None:
    book = {"rebalanced_on": "2026-10-29", "long": ["A", "B"], "short": ["X"], "risk_weights": {}, "vetoes": {}}
    t = _trader(book, eight=[{"ticker": "A", "summary": "Material 8-K Event for A: 3.01: Notice of Delisting. Filed on 2026-11-10."}])
    v = asyncio.run(t._vetoes(book))
    assert v == {"A": "8-K item 3.01"}
    assert json.loads(t.raw.d[atr.BOOK_KEY])["vetoes"] == {"A": "8-K item 3.01"}
    # the 8-K ages out of the window: the veto is still in force, so the name is not bought back
    t.db = Db()
    book["vetoes"] = {"A": "8-K item 3.01"}
    assert asyncio.run(t._vetoes(book)) == {"A": "8-K item 3.01"}


def test_a_failed_veto_lookup_keeps_the_vetoes_already_held_and_trading_goes_on() -> None:
    book = {"long": ["A"], "short": [], "vetoes": {"A": "8-K item 1.03"}}
    t = _trader(book)

    async def boom(sql, *a):
        raise RuntimeError("db down")

    t.db.query = boom                                              # type: ignore[assignment]
    assert asyncio.run(t._vetoes(book)) == {"A": "8-K item 1.03"}


def test_run_once_freezes_before_it_weighs_and_vetoes_before_it_plans() -> None:
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    run_once = src[src.index("async def run_once"):]
    assert "weights=await self._risk_weights(" not in run_once                      # no per-session recomputation
    assert (run_once.index("_freeze_alert_weights(book)") < run_once.index("at.target_weights(")
            < run_once.index("self._vetoes(book)") < run_once.index("at.apply_vetoes(")
            < run_once.index("at.cap_weights(") < run_once.index("at.plan_orders("))
