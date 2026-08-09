"""
tests/test_market_series_route.py

Unit tests for GET /api/v1/radar/market-series route telemetry endpoint.
"""

import pytest
from unittest.mock import AsyncMock
from services.api_gateway.routes.radar import get_market_series


import asyncio

def test_get_market_series_with_mock_db():
    async def _run():
        mock_db = AsyncMock()
        mock_db.query.return_value = [
            {
                "primary_entity_id": "BTCUSD",
                "primary_entity_name": "Bitcoin / USD",
                "occurred_at": "2026-08-05T12:00:00Z",
                "anomaly_score": 0.75,
                "financial_data": {"current_price": 67800.0, "volume": 12500},
                "crypto_data": None,
            },
            {
                "primary_entity_id": "TLT",
                "primary_entity_name": "iShares 20+ Year Treasury Bond ETF",
                "occurred_at": "2026-08-05T12:00:00Z",
                "anomaly_score": 0.40,
                "financial_data": {"current_price": 92.80, "volume": 45000},
                "crypto_data": None,
            }
        ]

        res = await get_market_series(symbols="BTCUSD,TLT", limit=20, db=mock_db)

        assert "BTCUSD" in res["symbols"]
        assert "TLT" in res["symbols"]
        assert "series" in res
        assert "BTCUSD" in res["series"]
        assert "TLT" in res["series"]
        assert len(res["series"]["BTCUSD"]) > 0
        assert len(res["series"]["TLT"]) > 0

    asyncio.run(_run())
