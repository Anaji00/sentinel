"""The Performance page leads with the answer; the explanations fold underneath."""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"


def test_the_verdict_comes_before_the_explanations() -> None:
    page = (SRC / "components/ModelPortfolio.tsx").read_text(encoding="utf-8")
    assert page.index("{verdict.word}") < page.index('<Disclosure label="how this portfolio works"')
    fold = page[page.index('<Disclosure label="how this portfolio works"'):page.index("</Disclosure>")]
    for part in ("A pretend portfolio", "Why it works this way", "sizingLine(", "SHADOW_LABEL"):
        assert part in fold, f"{part} should be folded away, not above the figures"
    head = page[:page.index('<Disclosure label="how this portfolio works"')]
    assert "A pretend portfolio" not in head


def test_a_short_record_is_never_presented_as_proof() -> None:
    verdict = (SRC / "lib/portfolioVerdict.ts").read_text(encoding="utf-8")
    assert "too early to tell skill from luck" in verdict and "MIN_SESSIONS" in verdict
