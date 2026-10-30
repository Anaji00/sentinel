"""Case files, scenarios on request and for graded families, and the 13D reader
(29 Oct 2026)."""
import asyncio
import json
import pathlib
import sys
from datetime import datetime, timezone

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import activist_13d as a13  # noqa: E402
from shared.utils import case_file as cf  # noqa: E402
from shared.utils import scenario_requests as sr  # noqa: E402


# ── case file ────────────────────────────────────────────────────────────────

def test_a_case_file_numbers_every_fact_with_its_source_and_date():
    case = cf.build("XOM", rank=(0.93, "2026-10-28"), closes=[110.0, 100.0, 1, 1, 1, 105.0],
                    news={"n": 12, "mean": 0.25, "as_of": "2026-10-29"},
                    headlines=[("2026-10-29 14:00", "Exxon beats estimates", 1)],
                    alerts=[("2026-10-29 13:00", "Geopolitical Cascade", True)],
                    features={"cmdty_exposure_21": (0.031, "2026-10-28", "Signed 21-session move...")},
                    customers=[("SHEL", 12.0)], now=datetime(2026, 10, 29, tzinfo=timezone.utc))
    ids = [f["id"] for f in case["facts"]]
    assert ids == [f"F{k}" for k in range(1, len(ids) + 1)]
    kinds = {f["kind"] for f in case["facts"]}
    assert {"rank", "move", "news", "alert", "feature", "customer"} <= kinds
    assert all(f["source"] and "as_of" in f for f in case["facts"])
    assert any("graded to precede bigger moves" in f["text"] for f in case["facts"])
    assert "+10.0%" in case["facts"][1]["text"], "1-session move from closes newest first"


def test_missing_sources_are_absent_not_zero():
    case = cf.build("ZZZ")
    assert case["facts"] == []
    assert cf.fact_lines(None) == [] and not cf.cited_ids_exist(["F1"], case)


def test_citations_must_exist():
    case = cf.build("XOM", rank=(0.5, "d"))
    assert cf.cited_ids_exist(["F1"], case) and not cf.cited_ids_exist(["F1", "F9"], case)


def test_the_analyst_reads_the_case_file():
    src = (ROOT / "services/agents/analyst_agent.py").read_text(encoding="utf-8")
    assert "await self._case_facts(raw, ticker)" in src and "cf.CASE_KEY.format(ticker=ticker)" in src


# ── scenarios ────────────────────────────────────────────────────────────────

def test_a_request_is_validated_and_becomes_a_persistable_cluster():
    req = sr.ScenarioRequest(topic="  Ceasefire   in Ukraine ", tickers=["lmt", "RTX", "lmt"])
    assert req.topic == "Ceasefire in Ukraine" and req.tickers == ["LMT", "RTX"]
    rec = sr.queued(req, "alice")
    fields = sr.cluster_fields(rec)
    assert fields["rule_id"] == sr.RULE_ID and fields["summary_headline"] == "Ceasefire in Ukraine"
    assert "exposes:LMT" in fields["tags"]
    from shared.models import AlertTier, CorrelationCluster
    CorrelationCluster(alert_tier=AlertTier.INTELLIGENCE, **fields)
    with pytest.raises(Exception):
        sr.ScenarioRequest(topic="ok topic", tickers=["not a ticker!"])


def test_requests_are_persisted_before_synthesis_and_forced_past_the_gates():
    src = (ROOT / "services/reasoning/main.py").read_text(encoding="utf-8")
    loop = src.split("async def _scenario_request_loop")[1].split("async def main")[0]
    assert loop.index("store.save_correlation(cluster)") < loop.index("process_cluster(")
    assert "force=True" in loop
    proc = src.split("async def process_cluster")[1].split("context = await")[0]
    assert "if not force:" in proc and "family_warrants_synthesis" in proc
    assert "SELECT 1 FROM correlations" in proc, "the persisted-row check is never skipped"


class _Raw:
    def __init__(self, grades):
        self.grades, self.n = grades, 0

    async def get(self, _k):
        return json.dumps(self.grades)

    async def incr(self, _k):
        self.n += 1
        return self.n

    async def expire(self, *_):
        return True


def test_graded_families_get_scenarios_within_a_daily_cap(monkeypatch):
    import services.reasoning.main as rm
    from shared.models import AlertTier
    grades = {"correlation_families_1d": [{"group": "Geopolitical Cascade", "n": 99, "verdict": "moves more"}],
              "family_lift_1d": [{"group": "Geopolitical Cascade", "n": 99, "verdict": "beats a headline"}]}
    monkeypatch.setattr(rm, "SCENARIO_DAILY_CAP", 2)
    raw = _Raw(grades)

    class C:
        alert_tier = AlertTier.ALERT
        rule_name = "Geopolitical Cascade (Black Sea)"

    got = [asyncio.run(rm.family_warrants_synthesis(C(), raw)) for _ in range(3)]
    assert got == [True, True, False]
    C.rule_name = "Hawkes Excitation"
    assert not asyncio.run(rm.family_warrants_synthesis(C(), _Raw(grades)))


def test_scenarios_can_use_a_hosted_model():
    src = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    assert 'os.getenv("SCENARIO_MODEL") or os.getenv("REASONING_MODEL")' in src


# ── 13D ──────────────────────────────────────────────────────────────────────

FILING = ("Item 3. Source of funds... Item 4. Purpose of Transaction. The Reporting Persons acquired the Shares "
          "because they believe them undervalued. The Reporting Persons intend to seek representation on the "
          "Board of Directors and to urge the Issuer to explore a sale of the company. Item 5. Interest in "
          "Securities of the Issuer. 9.8% ...")


def test_item4_is_cut_between_its_heading_and_item5():
    body = a13.item4(FILING)
    assert body.startswith("The Reporting Persons acquired") and "Interest in Securities" not in body
    assert a13.item4("no items here") is None


def test_an_activist_reading_must_quote_item4():
    body = a13.item4(FILING)
    ok = a13.Intent(activist=True, intents=["board_seats", "sale_or_merger", "bogus"],
                    quote="intend to seek representation on the Board of Directors")
    assert a13.grounded(ok, body) and a13.clean_intents(ok.intents) == ["board_seats", "sale_or_merger"]
    made_up = a13.Intent(activist=True, quote="will launch a proxy fight against the CEO this quarter")
    assert not a13.grounded(made_up, body)
    assert a13.grounded(a13.Intent(activist=False), body), "boilerplate needs no quote"


def test_the_job_skips_13g_and_is_hosted_only():
    src = (ROOT / "services/agents/activist_13d_job.py").read_text(encoding="utf-8")
    assert "hosted_llm.is_hosted(MODEL)" in src and "13G" not in "".join(a13.FORMS)


def test_item4_is_read_from_the_structured_xml():
    xml = ("<edgarSubmission><item4><transactionPurpose>The Reporting Person intends to nominate directors "
           "&amp; seek a sale.</transactionPurpose></item4></edgarSubmission>")
    assert a13.item4_from_xml(xml) == "The Reporting Person intends to nominate directors & seek a sale."
    assert a13.item4_from_xml("<edgarSubmission/>") is None


def test_an_unavailable_model_never_parks_a_filing():
    src = (ROOT / "services/agents/activist_13d_job.py").read_text(encoding="utf-8")
    loop = src.split("async def run_once")[1].split("async def _read")[0]
    assert loop.index("except InferenceError") < loop.index("except Exception")
    assert "break" in loop.split("except InferenceError")[1].split("except Exception")[0]


def test_scenario_requests_are_not_alerts_to_any_reader():
    for f in ("services/agents/case_file_job.py", "services/agents/auto_trader.py", "services/agents/alert_move_job.py",
              "services/agents/grader.py"):
        src = (ROOT / f).read_text(encoding="utf-8")
        assert f"rule_id IS DISTINCT FROM '{sr.RULE_ID}'" in src, f
