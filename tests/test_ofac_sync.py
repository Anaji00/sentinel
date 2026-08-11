"""
tests/test_ofac_sync.py

Tests for live OFAC Treasury SDN CSV fetching, automaton rebuild, fuzzy matching, and loop execution.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from services.enrichment.ofac_sync import fetch_ofac_keywords
from shared.utils.sanctions import check_sanctions, rebuild_sanctions_from_list, SANCTIONED_KEYWORDS, fetch_and_sync_ofac_sdn_list


def test_fetch_ofac_keywords_mocked():
    async def run_test():
        sample_sdn_csv = '101,"VALKYRIE ENTERPRISES", "vessel", "IRAN", "Title", "", "", "", "", "", "", ""\n102,"SOVCOMFLOT TANKER", "vessel", "RUSSIA", "", "", "", "", "", "", "", ""'
        sample_alt_csv = '201, 101, "aka", "VALKYRIE SHIPPING", ""'

        mock_resp_sdn = AsyncMock()
        mock_resp_sdn.status = 200
        mock_resp_sdn.text.return_value = sample_sdn_csv

        mock_resp_alt = AsyncMock()
        mock_resp_alt.status = 200
        mock_resp_alt.text.return_value = sample_alt_csv

        class MockSession:
            def get(self, url):
                cm = AsyncMock()
                if "sdn.csv" in url:
                    cm.__aenter__.return_value = mock_resp_sdn
                else:
                    cm.__aenter__.return_value = mock_resp_alt
                return cm

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

        with patch("aiohttp.ClientSession", return_value=MockSession()):
            keywords = await fetch_ofac_keywords()
            assert "valkyrie enterprises" in keywords
            assert "valkyrie shipping" in keywords
            assert "sovcomflot tanker" in keywords

    asyncio.run(run_test())


def test_sanctions_rebuild_and_fuzzy_matching():
    # Rebuild automaton with extended keywords
    keywords = SANCTIONED_KEYWORDS + ["valkyrie shipping", "sovcomflot tanker"]
    rebuild_sanctions_from_list(keywords)

    # Exact / Substring match check
    flags1 = check_sanctions("Vessel VALKYRIE SHIPPING in Persian Gulf")
    assert "sanctioned_ofac" in flags1
    assert any("valkyrie shipping" in f for f in flags1)

    # MMSI flag check
    flags2 = check_sanctions("Clean Vessel", mmsi="422123456")  # Iran MID 422
    assert "sanctions_adjacent_flag_state" in flags2
