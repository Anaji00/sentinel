"""Subjects that are not companies, assets or places, and so must not anchor a correlation.

A news-entity extractor reads "AI" (artificial intelligence), "EPS" (earnings per share) and feed names as subjects: in seven days "AI" anchored 283
correlations, "EPS" 180 and "seekingalpha_currents" 166, and NVDA 12.7% of them (13 Nov 2026). "AI" and "EPS" are also real tickers (C3.ai; a
WisdomTree ETF), so the ticker check passes them; this list is the curated exception. A feed name is lower-case with underscores, which no ticker, currency pair, macro series id
or prediction-market slug (hyphenated, a legitimate subject) ever is. Pure.
"""
from __future__ import annotations

import re
from typing import Iterable, List, Optional

NON_COMPANY_TERMS = frozenset({
    # Words and abbreviations that are never the actor of a correlation. COO and CTO are not here: they are the Cooper Companies and CTO Realty. Regions, agencies, central banks and currencies (US, UK, EU, NATO, OPEC, FDA,
    # FOMC, ECB, USD...) are NOT here: they are legitimate subjects of macro and geopolitical correlations (US 238, UK 143, EU 42 in 30 days).
    "AI", "EPS", "ETF", "ETFS", "IPO", "CEO", "CFO", "SPAC", "ESG", "M&A", "Q1", "Q2", "Q3", "Q4", "FY", "YOY", "QOQ",
    "NEWS", "UPDATE", "MARKETS", "STOCKS", "STOCK", "SHARES", "TODAY", "WEEK", "TOP", "BEST", "WORST", "BIG", "GLOBAL",
})
_SOURCE_NAME = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+$")      # underscores only: prediction-market slugs are hyphenated and are subjects


def is_non_company_subject(entity_id: Optional[str]) -> bool:
    """True for a bare term from the list or a feed/source name; False for anything shaped like a company, asset, pair or series."""
    text = (entity_id or "").strip()
    if not text:
        return False
    return text.upper() in NON_COMPANY_TERMS or bool(_SOURCE_NAME.match(text))


def keep_subject_tokens(tokens: Iterable[str], cashtagged: Iterable[str] = ()) -> List[str]:
    """Tokens read from headline text that may become a subject: a bare "AI" or "EPS" is dropped, a cashtag ($AI) is kept because the writer named the ticker.

    Order is preserved. Layer audit 2.1: "EPS" was the primary entity on 497 headlines a week and "AI" on 335."""
    tagged = {str(t).upper() for t in cashtagged}
    return [t for t in tokens if str(t).upper() in tagged or not is_non_company_subject(t)]


__all__ = ["NON_COMPANY_TERMS", "is_non_company_subject", "keep_subject_tokens"]
