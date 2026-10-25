"""A company's filings and financials belong to its common stock, not a preferred.

The SEC registry lists JPM first and JPM-PC ... JPM-PM after it under the same
CIK. The CIK map was last-writer-wins, so JPMorgan's filings and XBRL financials
were keyed to JPM-PM -- and so were 228 other issuers', most large banks among them.
"""
import asyncio
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "thirteen_f_registry", ROOT / "services" / "collector-filings" / "thirteen_f.py")
tf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tf)

# The live registry's order for three issuers, abbreviated.
REGISTRY = {
    "0": {"cik_str": 19617, "ticker": "JPM", "title": "JPMORGAN CHASE & CO"},
    "1": {"cik_str": 1067983, "ticker": "BRK-B", "title": "BERKSHIRE HATHAWAY INC"},
    "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
    "3": {"cik_str": 19617, "ticker": "JPM-PC", "title": "JPMORGAN CHASE & CO"},
    "4": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."},
    "5": {"cik_str": 1067983, "ticker": "BRK-A", "title": "BERKSHIRE HATHAWAY INC"},
    "6": {"cik_str": 19617, "ticker": "JPM-PM", "title": "JPMORGAN CHASE & CO"},
}


class _Resp:
    status = 200

    async def json(self, content_type=None):
        return REGISTRY

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def get(self, url, headers=None, timeout=None):
        return _Resp()


def test_a_cik_maps_to_the_first_listed_common_ticker() -> None:
    tf._DYNAMIC_TITLE_TO_TICKER.clear()
    tf._DYNAMIC_CIK_TO_TICKER.clear()
    asyncio.run(tf.load_sec_company_tickers(_Session(), None))
    m = tf._DYNAMIC_CIK_TO_TICKER
    assert m["19617"] == "JPM" and m["0000019617"] == "JPM"
    assert m["1067983"] == "BRK-B"
    assert m["1652044"] == "GOOGL"
