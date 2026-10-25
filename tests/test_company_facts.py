"""SEC company facts: every filing kept, with its date; tickers resolved ticker -> CIK."""

import importlib.util
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "company_facts", ROOT / "services" / "collector-filings" / "company_facts.py")
cf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cf)

PAYLOAD = {
    "cik": 320193,
    "facts": {
        "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
            {"end": "2025-07-18", "val": 14840000000, "accn": "q3", "fy": 2025, "fp": "Q3",
             "form": "10-Q", "filed": "2025-08-01"}]}}},
        "us-gaap": {
            "NetIncomeLoss": {"units": {"USD": [
                {"start": "2025-03-30", "end": "2025-06-28", "val": 23434000000, "accn": "q3",
                 "fy": 2025, "fp": "Q3", "form": "10-Q", "filed": "2025-08-01"},
                {"start": "2025-03-30", "end": "2025-06-28", "val": 23434000000, "accn": "q3b",
                 "fy": 2026, "fp": "Q3", "form": "10-Q", "filed": "2026-07-31"},
                {"start": "2010-01-01", "end": "2010-12-31", "val": 1, "accn": "old",
                 "form": "10-K", "filed": "2011-01-30"},
                {"start": "2025-03-30", "end": "2025-06-28", "val": 5, "accn": "8k",
                 "form": "8-K", "filed": "2025-07-31"}]}},
            "AccountsPayableCurrent": {"units": {"USD": [
                {"end": "2025-06-28", "val": 9, "accn": "q3", "form": "10-Q", "filed": "2025-08-01"}]}},
        },
    },
}


def test_extract_keeps_each_filing_of_a_period_and_drops_what_the_model_does_not_read() -> None:
    rows = cf.extract(PAYLOAD)
    ni = [r for r in rows if r[1] == "NetIncomeLoss"]
    assert sorted(r[9] for r in ni) == [date(2025, 8, 1), date(2026, 7, 31)]   # both filings, dated
    assert all(r[0] == "320193" for r in rows)
    assert not [r for r in rows if r[1] == "AccountsPayableCurrent"]            # not a tag we read
    assert not [r for r in rows if r[8] == "8-K"]                               # 10-K / 10-Q only
    shares = [r for r in rows if r[1] == "EntityCommonStockSharesOutstanding"]
    assert shares and shares[0][3] is None and shares[0][2] == "shares"


def test_every_ticker_of_an_issuer_resolves_to_it() -> None:
    reg = {"0": {"cik_str": 19617, "ticker": "JPM"}, "1": {"cik_str": 19617, "ticker": "JPM-PM"},
           "2": {"cik_str": 1652044, "ticker": "goog"}, "3": {"ticker": "BAD"}}
    assert cf.registry_pairs(reg) == [("JPM", "19617"), ("JPM-PM", "19617"), ("GOOG", "1652044")]


def test_a_share_class_matches_the_price_feeds_spelling() -> None:
    pairs = [("BRK-B", "1067983"), ("BRK-A", "1067983"), ("JPM", "19617"), ("JPM-PM", "19617")]
    got = cf.universe_pairs(pairs, {"BRK.B", "JPM"})
    assert got == [("BRK.B", "1067983", "sec_registry"), ("JPM", "19617", "sec_registry")]


def test_an_oversized_file_is_not_read_into_memory() -> None:
    """8 Oct: one large company-facts file killed the 384 MiB collector."""
    import asyncio

    class _Content:
        def __init__(self, n):
            self.n = n

        async def iter_chunked(self, size):
            sent = 0
            while sent < self.n:
                k = min(size, self.n - sent)
                sent += k
                yield b"x" * k

    class _Resp:
        def __init__(self, n):
            self.content = _Content(n)

    assert asyncio.run(cf._read_capped(_Resp(1000), 5000)) == b"x" * 1000
    assert asyncio.run(cf._read_capped(_Resp(200_000), 100_000)) is None
