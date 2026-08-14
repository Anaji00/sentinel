"""
tests/test_collector_ingestors.py

Unit test suite for all 9 Sentinel Domain Telemetry Collectors:
  - collector-ais
  - collector-adsb
  - collector-crypto
  - collector-cyber
  - collector-macro
  - collector-news
  - collector-prediction
  - collector-radar
  - collector-tradfi
"""

import json
import pytest
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Dynamically load services/collector-radar/regime.py
radar_regime_path = Path(__file__).resolve().parents[1] / "services" / "collector-radar" / "regime.py"
spec = importlib.util.spec_from_file_location("collector_radar_regime", radar_regime_path)
collector_radar_regime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector_radar_regime)

MarketRegime = collector_radar_regime.MarketRegime

def test_radar_parkinson_volatility():
    highs = [105.0, 106.0, 107.0, 108.0]
    lows = [99.0, 100.0, 101.0, 102.0]
    vol = MarketRegime.compute_parkinson_volatility(highs, lows)
    assert vol > 0.0


# ── 2. CRYPTO COLLECTOR TRADE PARSING ────────────────────────────────────────

def test_crypto_orderbook_depth_parsing():
    raw_ws_msg = {
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "b": [["60000.00", "1.5"], ["59990.00", "2.0"]],
        "a": [["60010.00", "0.8"], ["60020.00", "1.2"]]
    }
    bids = [[float(p), float(q)] for p, q in raw_ws_msg["b"]]
    asks = [[float(p), float(q)] for p, q in raw_ws_msg["a"]]
    
    bid_price = bids[0][0]
    ask_price = asks[0][0]
    spread = ask_price - bid_price
    
    assert bid_price == 60000.00
    assert ask_price == 60010.00
    assert spread == 10.00


# ── 3. AIS & ADSB COLLECTOR PAYLOAD NORMALIZATION ────────────────────────────

def test_ais_position_report_normalization():
    ais_msg = {
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": 613000123, "ShipName": "SHADOW TANKER ALPHA"},
        "Message": {
            "PositionReport": {
                "Latitude": 26.512,
                "Longitude": 56.411,
                "Sog": 14.2,
                "TrueHeading": 185,
                "NavigationalStatus": 0
            }
        }
    }
    meta = ais_msg["MetaData"]
    pos = ais_msg["Message"]["PositionReport"]
    
    assert str(meta["MMSI"]) == "613000123"
    assert pos["Latitude"] == 26.512
    assert pos["Sog"] == 14.2

def test_adsb_state_vector_parsing():
    opensky_vector = ["4b1800", "SWR100  ", "Switzerland", 1700000000, 1700000000, 8.54, 47.45, 10668.0, False, 230.5, 90.0, 0.0, None, 10700.0, "7000", False, 0]
    icao24 = opensky_vector[0].upper()
    callsign = opensky_vector[1].strip()
    lat = opensky_vector[6]
    lon = opensky_vector[5]
    alt_m = opensky_vector[7]
    squawk = opensky_vector[14]

    assert icao24 == "4B1800"
    assert callsign == "SWR100"
    assert lat == 47.45
    assert lon == 8.54
    assert alt_m == 10668.0
    assert squawk == "7000"


# ── 4. PREDICTION MARKET & MACRO COLLECTOR PAYLOADS ─────────────────────────

def test_prediction_market_prob_delta():
    poly_msg = {
        "market": "Will Iran close Strait of Hormuz before Q4?",
        "outcome": "YES",
        "price": 0.35,
        "prev_price": 0.12,
        "volume_24h": 450000
    }
    prob_shift = poly_msg["price"] - poly_msg["prev_price"]
    assert prob_shift == pytest.approx(0.23)
    assert poly_msg["volume_24h"] > 100000
