"""Text-based peers from 10-K business descriptions (shared/utils/text_peers.py)."""
from shared.utils import text_peers as tp


def test_the_business_section_is_item_1_to_item_1a():
    text = ("Contents Item 1. Business 3 Item 1A. Risk Factors 9 ... Item 1. Business We design semiconductors "
            "and graphics processors for gaming. Item 1A. Risk Factors Our results may vary.")
    assert tp.business_section(text).strip() == "We design semiconductors and graphics processors for gaming."
    assert tp.business_section("no headings") == ""


def test_peers_share_distinctive_words_not_common_ones():
    chips = "semiconductor wafer foundry lithography graphics processor datacenter accelerator "
    banks = "deposits lending mortgage branches checking savings loans underwriting "
    docs = {"NVDA": tp.terms(chips * 5 + "revenue growth"), "AMD": tp.terms(chips * 4 + "revenue growth"),
            "JPM": tp.terms(banks * 5 + "revenue growth"), "WFC": tp.terms(banks * 3 + "revenue growth")}
    rows = {(t, u) for t, u, _ in tp.peers(docs, common_share=0.6)}
    assert ("NVDA", "AMD") in rows and ("JPM", "WFC") in rows
    assert not any({t, u} == {"NVDA", "JPM"} for t, u in rows)          # "revenue growth" is everyone's


def test_peer_momentum_averages_the_peers_returns():
    rows = [("NVDA", "AMD", 0.8), ("NVDA", "AVGO", 0.6), ("JPM", "WFC", 0.7)]
    assert tp.peer_momentum(rows, {"AMD": 0.10, "AVGO": 0.02}) == {"NVDA": 0.06}
