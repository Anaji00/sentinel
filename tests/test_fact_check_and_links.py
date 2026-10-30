"""The fact checker beyond the analyst, the analyst's macro context, and
connected-firm momentum through news co-mentions."""
import asyncio
import json
import pathlib
import sys
from datetime import date, datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import comention as cm  # noqa: E402
from shared.utils import fact_check as fc  # noqa: E402


# ── fact checker ─────────────────────────────────────────────────────────────

SOURCE = "XOM last $104.20; Brent 0.82 correlation, p-val: 0.0130, lag: 2. Universe: XOM, CVX, OXY."


def test_numbers_in_the_data_pass_and_invented_ones_fail():
    good = fc.check("A 0.82 correlation with Brent and XOM at $104.20 explain it.", SOURCE)
    assert good.ok and good.numbers_checked == 2
    bad = fc.check("XOM fell 7.4% after a 0.55 beta and 31% margin squeeze.", SOURCE)
    assert not bad.numbers_ok and bad.unsupported_numbers


def test_percent_and_fraction_match_and_trivial_numbers_are_skipped():
    assert fc.supported(82.0, [0.82]) and fc.supported(0.013, [1.3])
    rep = fc.check("Within 2 sessions of the 2026 meeting, 3 peers moved.", SOURCE)
    assert rep.numbers_checked == 0 and rep.ok


def test_tickers_must_be_in_the_data_or_known():
    rep = fc.check("", SOURCE, ["CVX", "ZZZZ", "BP"], known={"BP"})
    assert rep.unknown_tickers == ["ZZZZ"] and not rep.ok


def test_one_unsupported_number_alone_does_not_fail():
    assert fc.check("A 0.55 beta.", SOURCE).numbers_ok


def test_both_free_text_agents_are_checked():
    s = (ROOT / "services/agents/stock_correlation_agent.py").read_text(encoding="utf-8")
    assert "fact_check.check(" in s and 'brief.expected_direction = "neutral"' in s
    g = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    assert "fact_check.check(_scenario_body_text(output), source)" in g
    assert g.count("self._to_scenario(cluster, output, source=user_prompt)") == 2


def test_an_unsupported_scenario_keeps_a_capped_confidence():
    import services.reasoning.scenario_generator as sg

    class _H:
        label, mechanism, beneficiaries, probability = "a", "Shares fall 37% on a 12.5% tariff", "x", 0.5

        def dict(self):
            return {"label": self.label, "probability": 50, "mechanism": self.mechanism, "beneficiaries": [],
                    "watch_signals": [], "deny_signals": [], "time_horizon": "1w"}

    class _Out:
        headline, significance = "Tariff shock", "Margins drop 41% and revenue 19%"
        hypotheses = [_H()]
        recommended_monitoring, confidence_overall, confidence_rationale = [], 80, "strong"

    class _Cluster:
        correlation_id, trigger_event_id, supporting_event_ids = "00000000-0000-0000-0000-000000000001", None, []
        entity_names, entity_ids, primary_entity_id, primary_entity_name = [], [], None, None

    gen = sg.ScenarioGenerator.__new__(sg.ScenarioGenerator)
    s = gen._to_scenario(_Cluster(), _Out(), source="A tariff was announced on steel imports.")
    assert s.confidence_overall <= sg.UNSUPPORTED_CONFIDENCE_CAP and "Fact check" in s.confidence_rationale


# ── analyst macro context ────────────────────────────────────────────────────

class _Raw:
    def __init__(self, snaps):
        self.snaps = snaps

    async def hgetall(self, _key):
        return self.snaps


def test_the_analyst_adds_market_medians_for_releases_in_its_horizon():
    from services.agents import analyst_agent as aa
    now = datetime.now(timezone.utc)
    snaps = {"CPI": json.dumps({"indicator": "CPI", "close_time": (now + timedelta(days=2)).isoformat(), "median": 3.1}),
             "NFP": json.dumps({"indicator": "NFP", "close_time": (now + timedelta(days=30)).isoformat(), "median": 140}),
             "U3": json.dumps({"indicator": "UNEMPLOYMENT", "close_time": (now - timedelta(days=1)).isoformat(), "median": 4.2})}
    a = aa.AnalystAgent.__new__(aa.AnalystAgent)
    a.logger = __import__("logging").getLogger("t")
    lines = asyncio.run(a._market_consensus(_Raw(snaps)))
    assert len(lines) == 1 and "cpi release" in lines[0] and "3.1" in lines[0]


def test_the_analyst_and_the_collector_share_one_key():
    src = (ROOT / "services/collector-macro/economic_calendar.py").read_text(encoding="utf-8")
    assert "from shared.utils.kalshi_consensus import KALSHI_CONSENSUS_KEY" in src
    a = (ROOT / "services/agents/analyst_agent.py").read_text(encoding="utf-8")
    assert "from shared.utils.kalshi_consensus import KALSHI_CONSENSUS_KEY" in a


# ── co-mention momentum ──────────────────────────────────────────────────────

def test_pairs_come_from_small_headlines_only():
    d = date(2026, 1, 5)
    ev = cm.pair_events([(d, ["aapl", "MSFT"]), (d, ["A", "B", "C", "D", "E", "F"]), (d, ["NVDA"])])
    assert ev == [(d, "AAPL", "MSFT")]


def test_links_are_point_in_time():
    ev = cm.pair_events([(date(2025, 1, 1), ["A", "B"]), (date(2026, 1, 9), ["A", "C"]), (date(2026, 1, 10), ["A", "D"])])
    links = cm.links_asof(ev, date(2026, 1, 10))
    assert links["A"] == {"C": 1.0}, "older than a year and same-day mentions are excluded"


def test_feature_weights_peers_by_mentions_and_needs_enough_of_them():
    links = {"A": {"B": 3.0, "C": 1.0}, "B": {"A": 3.0}}
    out = cm.feature(links, {"B": 0.10, "C": -0.10, "A": 0.5})
    assert out == {"A": round((3 * 0.10 - 0.10) / 4, 6)}, "B has one peer and is not published"


def test_month_ends_are_the_last_session_of_each_month():
    from services.agents.comention_job import month_ends
    s = [date(2026, 1, 29), date(2026, 1, 30), date(2026, 2, 2), date(2026, 2, 27)]
    assert month_ends(s) == [date(2026, 1, 30), date(2026, 2, 27)]


def test_the_analyst_skips_alerts_from_retired_families():
    from services.agents import analyst_agent as aa

    class _R:
        async def get(self, _k):
            return json.dumps({"correlation_families_1d": [{"group": "Dead Family", "n": 99, "verdict": "no difference"}]})

    a = aa.AnalystAgent.__new__(aa.AnalystAgent)
    a.logger = __import__("logging").getLogger("t")
    a.redis = type("X", (), {"raw": _R()})()
    msg = {"primary_entity_id": "AAPL", "rule_name": "Dead Family (Aapl)"}
    assert asyncio.run(a.analyse(msg)) is None
    assert "Dead Family" in asyncio.run(a._retired(_R()))
