"""Layer audit 2.1: bare "AI", "EPS" and "IPO" in a headline are not subjects; a cashtag or a real ticker is kept."""
from shared.utils.subjects import keep_subject_tokens


def test_bare_terms_are_dropped_and_real_tickers_kept() -> None:
    assert keep_subject_tokens(["EPS", "NVDA", "AI", "IPO", "COO", "CTO"]) == ["NVDA", "COO", "CTO"]


def test_a_cashtag_keeps_the_ticker() -> None:
    assert keep_subject_tokens(["AI", "EPS"], {"AI"}) == ["AI"]
    assert keep_subject_tokens(["AI"], {"ai"}) == ["AI"]


def test_order_is_preserved_and_empty_is_empty() -> None:
    assert keep_subject_tokens(["TSLA", "EPS", "HD"]) == ["TSLA", "HD"]
    assert keep_subject_tokens([]) == []


def test_the_enricher_applies_it_before_ticker_confirmation() -> None:
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "services" / "enrichment" / "enrichers" / "news.py").read_text(encoding="utf-8")
    assert src.index("keep_subject_tokens(extracted_tickers") < src.index("await confirm_tickers(extracted_tickers")
