"""The crypto side, made to join up.

On 4 October the only live crypto feeds were Coinbase candles (named BTCUSDT)
and OKX funding (named BTC). No liquidation had ever arrived (Binance answers
451 here), the two crypto rules' evidence types had no live producer, their
same-entity join compared two spellings of one coin as two assets, and the
equity leg of the contagion rule asked for evidence from after its trigger.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import asyncio

from shared.utils import liquidations as liq
from shared.utils.equities import instrument_aliases, instrument_identity
from shared.utils.osint_bridges import crypto_exposure_tags


def test_one_name_per_coin_whichever_venue_spelled_it():
    assert {instrument_identity(s) for s in ("BTC", "BTCUSDT", "btc-usdt-swap", "BTC-USD")} == {"BTC"}
    assert instrument_identity("ETHUSDT") == "ETH"
    assert instrument_identity("AAPL") == "AAPL" and instrument_identity("CL=F") == "CL=F"
    assert "BTCUSDT" in instrument_aliases("BTC") and instrument_aliases("AAPL") == ["AAPL"]


def test_a_burst_is_judged_against_its_own_side():
    quiet = [100_000.0] * 60
    assert liq.burst_multiple(200_000.0, quiet) is None          # under the floor
    assert liq.burst_multiple(900_000.0, quiet) == 9.0
    assert liq.burst_multiple(900_000.0, quiet[:10]) is None     # unmeasured is not quiet
    busy = [2_000_000.0] * 60
    assert liq.burst_multiple(3_000_000.0, busy) is None         # busy ETH, ordinary bucket
    assert liq.burst_multiple(7_000_000.0, busy) == 3.5


def test_fills_are_summed_per_bucket_and_side_once():
    details = [
        {"ts": "1791102519384", "sz": "0.64", "bkPx": "85158.7", "posSide": "short"},
        {"ts": "1791102515010", "sz": "2.0", "bkPx": "85000", "posSide": "long"},
        {"ts": "1791102400000", "sz": "9.0", "bkPx": "85000", "posSide": "long"},   # already seen
    ]
    out = liq.aggregate(details, 0.01, after_ts=1791102500000)
    sides = {side: v for (_, side), v in out.items()}
    assert set(sides) == {"SHORT", "LONG"}
    assert round(sides["LONG"]["notional"]) == 1700 and sides["LONG"]["fills"] == 1


def test_scores_and_exposures():
    assert liq.burst_score(3) == 0.5 and liq.burst_score(5) == 0.7 and liq.burst_score(20) == 0.9
    assert "exposes:MSTR" in crypto_exposure_tags("BTCUSDT")
    assert crypto_exposure_tags("ETH") == ["exposes:COIN", "exposes:HOOD"]
    assert crypto_exposure_tags("SOL") == []


def test_an_okx_burst_is_enriched_and_scored_on_its_multiple():
    from services.enrichment.enrichers.crypto import CryptoEnricher

    e = CryptoEnricher.__new__(CryptoEnricher)
    e.flags = SimpleNamespace(is_enabled=AsyncMock(return_value=True))
    e.scorer = MagicMock()
    e.graph = SimpleNamespace(producer=SimpleNamespace(send=AsyncMock()))
    e.redis = SimpleNamespace(raw=SimpleNamespace(get=AsyncMock(return_value=None)))
    raw = SimpleNamespace(source="okx_swap", event_id="b1", trace_id="t",
                          occurred_at=datetime(2026, 10, 4, tzinfo=timezone.utc))
    p = {"asset": "BTC", "trade_type": "LIQUIDATION", "side": "LONG", "price": 85000.0,
         "size_tokens": 35.3, "notional_usd": 3_000_000.0, "baseline_multiple": 6.0, "window_minutes": 5}
    ev = asyncio.run(e._enrich_liquidation(raw, p))
    assert ev.anomaly_score == 0.8
    assert "exposes:COIN" in ev.tags and ev.primary_entity.id == "BTC"


def test_the_crypto_rules_can_see_live_evidence():
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "services/correlation/main.py").read_text(encoding="utf-8")
    stress = src[src.index('"rule_id": "rule_crypto_stress_cascade"'):]
    stress = stress[:stress.index('"definition_version"')]
    assert '"crypto_liquidation", "market_anomaly"' in stress
    contagion = src[src.index('"rule_id": "rule_crypto_equity_contagion"'):]
    contagion = contagion[:contagion.index('"definition_version"')]
    assert '"follows_trigger"' not in contagion and '"shared_tags": True' in contagion
    route = (pathlib.Path(__file__).resolve().parents[1] / "services/enrichment/enrichers/crypto.py").read_text(encoding="utf-8")
    assert 'elif trade_type == "LIQUIDATION":' in route
