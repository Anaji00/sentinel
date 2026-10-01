"""A ticker with a price stuck to it is still that ticker.

696 claimed agents corroborate only when they spell a subject the same way.
Measured against the live resolver, that is mostly untrue:

    AAPL                  -> AAPL
    Apple Inc.            -> AAPL
    APPLE INC             -> AAPL
    Microsoft Corporation -> MSFT
    CPB ($21.53)          -> CPB 21 53     <- the one that failed

Genuine name variants already fold correctly. The failure was a *decorated*
ticker: a symbol with a price glued on became a subject of its own, unable to
corroborate or contradict the CPB every other agent published. That exact
string is recorded elsewhere in this audit as a bulletin's ticker.

The live bulletin board showed the larger truth behind 695: 26 bulletins, 25
distinct subjects, one shared by two agents. Consensus has little to fuse
because agents look at different things, not because grouping cannot see that
they agree.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.entity_resolution import canonical_key  # noqa: E402


# ── the decorated ticker ─────────────────────────────────────────────────────


def test_a_price_glued_to_a_ticker_is_stripped():
    assert canonical_key("CPB ($21.53)") == "CPB"
    assert canonical_key("GOOGL ( $180.22 )") == "GOOGL"


def test_a_parenthetical_name_beside_a_ticker_is_stripped():
    assert canonical_key("NVDA (NVIDIA)") == "NVDA"


def test_a_bare_ticker_is_untouched():
    assert canonical_key("AAPL") == "AAPL"


# ── what must not be stripped ────────────────────────────────────────────────


def test_a_company_name_does_not_become_its_parenthetical():
    """"SLTA V (GP), L.L.C." becoming ticker GP is the other half of this.

    Sixteen Form 4 filings landed under GP -- "General Partner" -- as though it
    were a company, because something took the parenthetical for a symbol.
    """
    assert canonical_key("SLTA V (GP), L.L.C.") != "GP"


def test_an_issuer_name_with_a_symbol_in_it_stays_a_name():
    assert canonical_key("Jewett Cameron (JCTC)") != "JCTC"


def test_text_with_no_parenthesis_is_returned_unchanged():
    for value in ("AAPL", "Apple Inc.", "", "   "):
        canonical_key(value)


def test_none_is_empty_not_an_error():
    assert canonical_key(None) == ""


# ── the two spellings now meet ───────────────────────────────────────────────


def test_a_decorated_and_a_bare_ticker_group_together():
    """The property consensus needs: one subject, not two."""
    assert canonical_key("CPB ($21.53)") == canonical_key("CPB")
