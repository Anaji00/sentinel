"""An insider cluster can fire on the Form 4s the platform actually receives.

INSIDER_CLUSTERS had carried zero messages in the platform's lifetime, and all
1,160 insider events in the fourteen days to 3 October read "Insider Other".
The collector parses every filing's XML and publishes each transaction and the
open-market totals; no reader turned them into a code. The enricher looked for
a `transaction_code` nobody writes and fell back to "J"; the quant agent did the
same and classed every trade OTHER, so the gate's two distinct buyers could
never exist. It also counted each filing twice -- raw and enriched -- and the
enricher read EDGAR's `Size: 4 KB` as a $4 trade.
"""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from shared.utils.form4 import code_value_usd, dominant_code


def _filing(owner, buy=0.0, sell=0.0, transactions=None, ticker="ACME"):
    return {
        "link": "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/x-index.htm",
        "title": f"4 - ACME CORP (0000000001) (Issuer)",
        "summary": "<b>Filed:</b> 2026-10-02 <b>AccNo:</b> 0000000001-26-000001 <b>Size:</b> 4 KB",
        "ticker": ticker, "issuer_name": "ACME CORP",
        "insider_names": [owner], "insider_name": owner,
        "transaction_value_usd": (buy - sell) or None,
        "open_market_buy_usd": buy, "open_market_sell_usd": sell,
        "has_open_market_activity": bool(buy or sell),
        "role": "Director",
        "transactions": transactions if transactions is not None else (
            [{"code": "P", "shares": 1000.0, "price_per_share": buy / 1000.0, "value_usd": buy,
              "acquired": True, "is_derivative": False}] if buy else []),
    }


def test_open_market_activity_decides_the_code():
    assert dominant_code(_filing("A", buy=300_000)) == "P"
    assert dominant_code(_filing("A", sell=90_000)) == "S"
    assert dominant_code(_filing("A", buy=10_000, sell=50_000)) == "S"
    assert code_value_usd(_filing("A", buy=300_000), "P") == 300_000


def test_without_open_market_activity_the_largest_transaction_decides():
    txns = [{"code": "A", "value_usd": None}, {"code": "M", "value_usd": 80_000.0}]
    assert dominant_code(_filing("A", transactions=txns)) == "M"
    assert dominant_code(_filing("A", transactions=[{"code": "g"}])) == "G"
    assert dominant_code({"title": "4 - X", "summary": ""}) is None


def _enricher():
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    e = TradFiEnricher.__new__(TradFiEnricher)
    e.redis_client = None
    e.scorer = SimpleNamespace(
        check_watchlist=AsyncMock(return_value=False),
        track_frequency=AsyncMock(return_value=0.0),
        get_hawkes_intensity=MagicMock(return_value=1.0),
    )
    e.graph = SimpleNamespace(producer=SimpleNamespace(send=AsyncMock()))
    return e


def test_the_enricher_reports_a_purchase_as_a_purchase():
    from services.enrichment.enrichers import tradfi

    raw = SimpleNamespace(source="sec_form4", event_id="e1", trace_id="t1",
                          occurred_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
                          raw_payload=_filing("JANE ROE", buy=300_000))
    enricher = _enricher()
    for name in ("_earnings_proximity", "_resolve_cik"):
        if hasattr(enricher, name):
            setattr(enricher, name, AsyncMock(return_value=None))
    event = asyncio.run(tradfi.TradFiEnricher._enrich_insider(enricher, raw, raw.raw_payload))
    assert event is not None
    assert "Open Market Buy" in event.headline
    assert "$0.3M" in event.headline


def test_filing_size_is_not_read_as_dollars():
    src = open("services/enrichment/enrichers/tradfi.py", encoding="utf-8").read()
    body = src[src.index("async def _enrich_insider"):]
    body = body[:body.index("\n    async def ", 10)]
    assert "Size:</b>|Size:" not in body


class _Redis:
    def __init__(self):
        self.z = {}
        self.kv = {}
        self.raw = self

    def pipeline(self):
        outer = self

        class P:
            def __init__(self):
                self.ops = []

            def zadd(self, key, mapping):
                self.ops.append(lambda: outer.z.setdefault(key, {}).update(mapping))

            def zremrangebyscore(self, key, lo, hi):
                self.ops.append(lambda: outer.z.setdefault(key, {}) and None)

            async def execute(self):
                for op in self.ops:
                    op()

        return P()

    async def zrange(self, key, start, stop):
        return list(self.z.get(key, {}))

    async def set(self, key, value, ex=None):
        self.kv[key] = value


def _agent():
    from services.agents.quant_trading_engine import QuantTradingEngine

    agent = QuantTradingEngine.__new__(QuantTradingEngine)
    agent.redis = _Redis()
    agent.flags = SimpleNamespace(is_enabled=AsyncMock(return_value=True))
    agent._producer = SimpleNamespace(send=AsyncMock())
    agent.publish_bulletin = AsyncMock()
    agent.name = "quant_trading_engine"
    return agent


def _raw_message(filing):
    return {"source": "sec_form4", "type": "raw", "raw_payload": filing}


def test_two_directors_buying_publish_a_cluster():
    from shared.kafka import Topics

    agent = _agent()

    async def run():
        await agent.handle(_raw_message(_filing("JANE ROE", buy=180_000)))
        return await agent.handle(_raw_message(_filing("JOHN DOE", buy=150_000)))

    result = asyncio.run(run())
    assert result and result["action"] == "INSIDER_CLUSTER_DETECTED"
    sent = [c for c in agent.producer.send.await_args_list if c.args[0] == Topics.INSIDER_CLUSTERS]
    assert len(sent) == 1
    assert sent[0].args[1]["insider_count"] == 2
    assert sent[0].args[1]["net_buy_usd"] == 330_000


def test_the_enriched_copy_is_not_counted_again():
    agent = _agent()
    enriched = {"source": "sec_form4", "type": "insider_trade", "headline": "Insider Open Market Buy: ACME",
                "financial_data": {"ticker": "ACME"}}
    assert asyncio.run(agent.handle(enriched)) is None
    assert agent.redis.z == {}


def test_grants_do_not_make_a_cluster():
    agent = _agent()
    grant = [{"code": "A", "value_usd": 900_000.0, "acquired": True}]

    async def run():
        await agent.handle(_raw_message(_filing("JANE ROE", transactions=grant)))
        return await agent.handle(_raw_message(_filing("JOHN DOE", transactions=grant)))

    assert asyncio.run(run()) is None
    stored = [json.loads(k) for k in agent.redis.z["sentinel:insider:trades:ACME"]]
    assert {t["tx_type"] for t in stored} == {"OTHER"}
