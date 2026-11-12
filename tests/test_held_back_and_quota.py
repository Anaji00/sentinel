"""The two dashboard read routes: why a signal was held back (shared/utils/held_back.py) and the free-tier model quota
(shared/utils/model_quota.py)."""

import asyncio
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import held_back as hb  # noqa: E402
from shared.utils import hosted_llm as hl  # noqa: E402
from shared.utils import model_quota as mq  # noqa: E402


def test_each_recorded_reason_reads_as_a_sentence_with_its_numbers() -> None:
    liq = hb.explain_reason("trailing dollar volume $38M is under $140.8M")
    assert liq["held_back_by"] == "liquidity bar" and liq["measured"] == 38.0 and liq["required"] == 140.8
    assert hb.explain_reason("proven cell not cleared by G4")["held_back_by"] == "tradability test"
    assert hb.explain_reason("not shortable and easy to borrow")["held_back_by"] == "borrow check"
    assert hb.explain_reason("no borrow data for a short")["held_back_by"] == "borrow check"
    assert hb.explain_reason("no dollar-volume history")["held_back_by"] == "liquidity bar"
    assert hb.explain_reason("something new")["explanation"] == "something new"
    for r in ("proven cell not cleared by G4", "not shortable and easy to borrow"):
        text = hb.explain_reason(r)["explanation"]
        assert "G4" not in text and "cell" not in text


def test_an_entry_uses_plain_words_and_the_ticker_filter_keeps_newest_first_and_skips_junk() -> None:
    e = {"family": "insider_cluster", "horizon": "5d", "instrument": "usar", "direction": -1, "why": "not shortable and easy to borrow",
         "at": "2026-11-11T15:00:00+00:00"}
    v = hb.entry_view(e, {"insider_cluster": "Several insiders bought within weeks"})
    assert v["signal"] == "Several insiders bought within weeks" and v["expected_move"] == "fall" and v["window"] == "next five days"
    raw = [json.dumps({**e, "instrument": "AAPL"}), b"not json", json.dumps(e), json.dumps({**e, "at": "older"})]
    rows = hb.for_ticker(raw, "USAR", None, limit=5)
    assert [r["when"] for r in rows] == ["2026-11-11T15:00:00+00:00", "older"]
    assert hb.for_ticker(raw, "ZZZ") == [] and len(hb.for_ticker(raw, "USAR", None, limit=1)) == 1


def test_quota_status_ranks_no_key_cooling_at_limit_near_limit_and_ok() -> None:
    q = hl.Quota(rpm=30, rpd=1000, tpm=6000, tpd=100000)
    base = {"rm": 1, "rd": 10, "tm": 100, "td": 1000}
    assert mq.view("groq/m", q, base, 0, True)["status"] == "ok"
    assert mq.view("groq/m", q, base, 0, False)["status"] == "no key set"
    assert mq.view("groq/m", q, base, 12, True)["status"] == "cooling down"
    assert mq.view("groq/m", q, {**base, "rm": 30}, 0, True)["status"] == "at limit"
    near = mq.view("groq/m", q, {**base, "td": 85000}, 0, True)
    assert near["status"] == "near limit" and near["share_of_limit_used"] == 0.85 and near["model"] == "m" and near["provider"] == "groq"
    assert mq.view("groq/m", q, base, -2, True)["cooling_down_seconds"] == 0
    open_ended = mq.view("groq/m", hl.Quota(rpm=30, rpd=0, tpm=6000, tpd=0), base, 0, True)
    assert open_ended["requests_per_day_limit"] is None and open_ended["tokens_per_day_limit"] is None and open_ended["status"] == "ok"


class _Raw:
    def __init__(self, counts, ttl):
        self.counts, self.ttl_value = counts, ttl

    async def mget(self, keys):
        return [self.counts.get(k) for k in keys]

    async def ttl(self, key):
        return self.ttl_value


def test_the_snapshot_reads_the_buckets_and_orders_most_used_first(monkeypatch) -> None:
    monkeypatch.setenv("LLM_QUOTAS", "groq/a=30,1000,6000,100000;groq/b=30,1000,6000,100000")
    monkeypatch.setenv("GROQ_API_KEY", "x")
    now = 1_800_000_000.0
    w = hl.windows(now)
    counts = {hl.BUCKET_KEY.format(step="groq/b", unit="r", window=w["d"]): "900"}
    rows = asyncio.run(mq.snapshot(_Raw(counts, -2), now))
    assert [r["model"] for r in rows] == ["b", "a"] and rows[0]["requests_today"] == 900 and rows[0]["share_of_limit_used"] == 0.9
    assert rows[0]["status"] == "near limit" and rows[1]["status"] == "ok"
    monkeypatch.delenv("LLM_QUOTAS")
    assert asyncio.run(mq.snapshot(_Raw({}, -2), now)) == []


def test_the_two_routes_are_registered() -> None:
    from services.api_gateway.routes import signals, system

    assert "/api/v1/signals/held-back/{ticker}" in {r.path for r in signals.router.routes}
    assert "/api/v1/health/model-quota" in {r.path for r in system.router.routes}
