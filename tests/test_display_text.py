"""Text a person reads: headlines without decoration, and radar notes without rule syntax."""
from types import SimpleNamespace

from services.agents.radar_agent import radar_bulletin_summary
from services.enrichment.main import apply_display_text
from shared.utils.display_text import plain_headline


def test_the_leading_emoji_goes_and_the_words_stay() -> None:
    assert plain_headline("⚡ Heavy trading in AA: $0.54M, far above normal") == "Heavy trading in AA: $0.54M, far above normal"
    assert plain_headline("🏛️ 13F REPORT: Berkshire Hathaway Inc") == "13F REPORT: Berkshire Hathaway Inc"
    assert plain_headline("₿ BTC moved") == "BTC moved"


def test_a_score_already_on_the_badge_is_not_repeated() -> None:
    assert plain_headline("🐋 Block Trade | NVDA $0.20M | Anomaly: 0.91") == "Block Trade | NVDA $0.20M"


def test_ordinary_headlines_are_untouched() -> None:
    title = "Anomaly: the fund's odd year | $5M at stake 🐋"
    assert plain_headline(title) == title
    assert plain_headline("") == ""


def test_enrichment_applies_it_before_storing() -> None:
    event = SimpleNamespace(
        headline="🐋 Block Trade | NVDA $0.20M | Anomaly: 0.91",
        summary="A summary long enough to be left alone.",
        primary_entity=None,
        type="equity_block",
    )
    apply_display_text(event)
    assert event.headline == "Block Trade | NVDA $0.20M"


def test_a_radar_note_reads_as_a_sentence() -> None:
    note = radar_bulletin_summary("VST", 10.87, 1_860_000)
    assert note == (
        "VST moved to closer watch: trading volume is 10.9 standard deviations from normal, "
        "with $1.86M traded in a minute."
    )
    assert "primary surveillance" not in note and ">=" not in note
