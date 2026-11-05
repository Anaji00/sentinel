"""An option whose type cannot be read stays unknown, and the headline says what the bet was."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.enrichers import tradfi as tf  # noqa: E402

SOURCE = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")


def test_an_unreadable_type_is_not_a_call() -> None:
    assert 'str(option_type or "CALL")' not in SOURCE
    assert '"UNKNOWN"' in SOURCE


def test_the_headline_names_the_bet_in_words() -> None:
    assert tf._options_headline("AAPL", "CALL", 250_000, "buy") == "AAPL: A buyer paid $250k for calls (a bet it rises)"
    # A seller's position is the opposite of the option's bet, and an unread print
    # has no bet at all; both were worded as the buyer's until 5 Nov 2026.
    assert tf._options_headline("TSLA", "PUT", 1_400_000, "sell") == "TSLA: A seller collected $1.4M for puts (gains if it stays above the strike)"
    assert tf._options_headline("TSLA", "CALL", 80_000, "sell") == "TSLA: A seller collected $80k for calls (gains if it stays below the strike)"
    assert tf._options_headline("XYZ", "CALL", 90_000) == "XYZ: Someone traded $90k of calls (buyer or seller not known)"
    assert tf._options_headline("XYZ", "UNKNOWN", 90_000) == "XYZ: Someone traded $90k of options (buyer or seller not known)"
    assert all(ord(ch) < 0x2000 for ch in tf._options_headline("AAPL", "CALL", 1, "buy"))   # no emoji
