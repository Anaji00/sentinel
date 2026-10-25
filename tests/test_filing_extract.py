"""What the small model and the patterns read out of filings."""

import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import filing_extract as fx  # noqa: E402
from shared.utils import signal_families as sf  # noqa: E402


def test_only_guidance_passages_reach_the_model() -> None:
    text = fx.text_of("<html><p>Revenue rose 12% to $1.2 billion in the third quarter.</p>"
                      "<p>The company raises its full-year 2026 outlook and now expects revenue of $4.9 billion.</p>"
                      "<script>var x = 1;</script><p>Net income was $210 million.</p></html>")
    passage = fx.guidance_passages(text)
    assert "raises its full-year 2026 outlook" in passage and "Net income" not in passage
    assert fx.guidance_passages("Revenue rose 12%. Net income was $210 million.") == ""


def test_an_equity_sale_is_sized_against_market_value() -> None:
    text = ("Item 3.02 Unregistered Sales of Equity Securities. On October 10 the Company issued 4,500,000 shares "
            "of common stock to the sellers, and 120,000 shares to the advisor.")
    assert fx.offered_shares(text) == 4_500_000
    assert fx.offering_tags(4_500_000, 10.0, 500e6) == ["offering_large"]          # $45M of $500M
    assert fx.offering_tags(4_500_000, 10.0, 5e9) == ["offering_small"]
    assert fx.offering_tags(None, 10.0, 5e9) == []


def test_extracted_events_become_their_own_families() -> None:
    row = {"type": "filing", "source": fx.EXTRACT_SOURCE, "event_id": "x", "financial_data": {"ticker": "CFG"},
           "created_at": datetime(2026, 10, 16, 11, tzinfo=timezone.utc),
           "tags": ["filing_extract", "guidance_lowered", "exposes:CFG"]}
    assert [(o.family, o.direction) for o in sf.observations(row)] == [("guidance_lowered", -1)]


def test_the_release_is_picked_over_the_presentation_and_index_pages() -> None:
    names = ["0000759944-26-000206-index-headers.html", "0000759944-26-000206-index.html", "0000759944-26-000206.txt",
             "a3q26earningspresentatio.htm", "a3q26earningsrelease.htm", "cfg-20261016.htm",
             "q326financialsupplement.htm", "R1.htm"]
    assert fx.pick_documents(names, "0000759944-26-000206") == {"release": "a3q26earningsrelease.htm",
                                                                 "body": "cfg-20261016.htm"}
    assert fx.pick_documents(["d123ex991.htm", "d123d8k.htm"], "0001-26-1")["release"] == "d123ex991.htm"
