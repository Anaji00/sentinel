"""Resolving a 13F issuer name to the ticker a person would actually trade.

Measured on the ten institutional portfolios the platform holds, 100 holdings:
67 resolved before this, 78 after -- and eleven of the original 67 pointed at
the *wrong* security, which is worse than pointing at nothing:

    WHIRLPOOL CORP   POOL    -> WHR     (POOL is Pool Corporation)
    ALLSTATE CORP    ALL-PB  -> ALL     (a preferred line, not the common)
    ALPHABET INC     GOOGN   -> GOOGL   (on seven separate filers)
    KINROSS GOLD     KGCRF   -> KGC     (OTC rather than NYSE)
    BLOCK INC        BSQKZ   -> XYZ

No network: the registry fixture below is the real shape of SEC's
company_tickers.json, including the multiple-listing ordering that caused the
wrong answers.
"""

import importlib.util
import pathlib

import pytest

_SRC = (
    pathlib.Path(__file__).resolve().parents[1]
    / "services" / "collector-filings" / "thirteen_f.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("thirteen_f_under_test", _SRC)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Ordered exactly as SEC orders them: the primary listing first, then the other
# registered lines for the same company. That order is the whole basis of the
# "first listing wins" rule.
REGISTRY = [
    ("GOOGL", "Alphabet Inc."),
    ("GOOG", "Alphabet Inc."),
    ("GOOGM", "Alphabet Inc."),
    ("GOOGN", "Alphabet Inc."),
    ("HTZ", "HERTZ GLOBAL HOLDINGS, INC"),
    ("HTZWW", "HERTZ GLOBAL HOLDINGS, INC"),
    ("STM", "STMicroelectronics N.V."),
    ("STMEF", "STMicroelectronics N.V."),
    ("AMZN", "AMAZON COM INC"),
    ("VRSN", "VERISIGN INC/CA"),
    ("CRWD", "CrowdStrike Holdings, Inc."),
    ("TSM", "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD"),
    ("TSMWF", "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD"),
    ("WHR", "Whirlpool Corp"),
    ("POOL", "POOL CORP"),
    ("NVDA", "NVIDIA CORP"),
]


@pytest.fixture()
def resolver():
    m = _load_module()
    m._DYNAMIC_TITLE_TO_TICKER.clear()
    m._STRIPPED_TITLE_TO_TICKER.clear()
    for ticker, title in REGISTRY:
        upper = title.upper()
        m._DYNAMIC_TITLE_TO_TICKER.setdefault(m._registry_key(upper), ticker)
        m._DYNAMIC_TITLE_TO_TICKER.setdefault(upper, ticker)
    m._rebuild_stripped_index()
    return m


@pytest.mark.parametrize(
    "issuer,expected",
    [
        # Multiple registered lines: the primary one wins.
        ("ALPHABET INC", "GOOGL"),
        ("HERTZ GLOBAL HLDGS INC", "HTZ"),
        ("Stmicroelectronics N V", "STM"),
        # Punctuation used to be deleted rather than spaced, so "Amazon.com"
        # became AMAZONCOM and could not match "AMAZON COM INC".
        ("Amazon.com Inc", "AMZN"),
        # SEC appends the state of incorporation to the registered name.
        ("VERISIGN INC", "VRSN"),
        # A 13F abbreviates where the registry spells the word out.
        ("CROWDSTRIKE HLDGS INC", "CRWD"),
        # SEC caps nameOfIssuer, so long names arrive cut mid-word.
        ("Taiwan Semiconductor Manufac", "TSM"),
        # And the plain case must keep working.
        ("NVIDIA CORP", "NVDA"),
    ],
)
def test_issuer_resolves_to_its_primary_listing(resolver, issuer, expected):
    assert resolver.resolve_ticker_dynamically(issuer) == expected


def test_a_warrant_is_never_returned_for_the_common_stock(resolver):
    """HTZWW is a warrant on Hertz. It is not Hertz.

    Last-write-wins on the title map returned whichever line SEC enumerated
    last, so institutional holdings of the common stock were tagged with the
    warrant's symbol.
    """
    assert resolver.resolve_ticker_dynamically("HERTZ GLOBAL HLDGS INC") != "HTZWW"


def test_whirlpool_does_not_resolve_to_pool_corporation(resolver):
    """The worst of the eleven: two unrelated companies.

    A wrong listing of the right company is a bad answer. A different company
    is a different kind of wrong, and nothing downstream could detect it --
    the ticker is real, it prices, and it has a sector.
    """
    assert resolver.resolve_ticker_dynamically("WHIRLPOOL CORP") == "WHR"
    assert resolver.resolve_ticker_dynamically("POOL CORP") == "POOL"


def test_truncation_matching_requires_length_and_uniqueness(resolver):
    """A prefix match is otherwise a good way to resolve one company to another.

    Short prefixes are refused outright, and a prefix matching two different
    registry titles resolves to neither -- the name genuinely does not identify
    one company, and None is the honest answer.
    """
    # Well under the length floor: must not match anything by prefix.
    assert resolver.resolve_ticker_dynamically("TAIWAN") is None
    # Long enough, and unique to one registry title.
    assert resolver.resolve_ticker_dynamically("TAIWAN SEMICONDUCTOR MANUFAC") == "TSM"


def test_unknown_issuer_returns_none_rather_than_a_guess(resolver):
    """An ETF trust is not in SEC's operating-company registry.

    iShares, SPDR and Invesco trusts make up most of what stays unresolved, and
    that is correct. Returning a plausible ticker for one would put a holding
    on a company that does not hold it.
    """
    for name in ("Ishares Inc", "SPDR S&P 500 ETF TR", "VANECK ETF TRUST"):
        assert resolver.resolve_ticker_dynamically(name) is None
    assert resolver.resolve_ticker_dynamically("") is None
    assert resolver.resolve_ticker_dynamically(None) is None


def test_registry_key_is_used_on_both_sides_of_the_lookup(resolver):
    """The two sides were normalised differently, so they could not match.

    Registry titles were indexed from the raw string while issuer names went
    through a different strip. This pins that one function spells both.
    """
    assert resolver._registry_key("VeriSign, Inc.") == resolver._registry_key("VERISIGN INC")
    assert resolver._registry_key("Amazon.com Inc") == resolver._registry_key("AMAZON COM INC")


def test_state_suffix_pattern_is_a_real_regex():
    """This pattern was compiled with a literal backspace character.

    A heredoc turned `\\b` into 0x08, so `/[A-Z]{2,3}\\x08` never matched and
    every `/CA`-style suffix survived -- invisibly, because grep renders the
    backspace as nothing. Control characters do not belong in a pattern.
    """
    m = _load_module()
    assert all(ord(c) >= 32 for c in m._STATE_SUFFIX.pattern), (
        f"control character in pattern: {m._STATE_SUFFIX.pattern!r}"
    )
    assert m._STATE_SUFFIX.sub(" ", "VERISIGN INC/CA").strip() == "VERISIGN INC"


# ── holdings reaching the graph ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_holdings_become_owns_edges_and_unresolved_ones_do_not():
    """`InstitutionalFiler` had zero nodes and `OWNS` had thirteen edges.

    The positions to fill both arrive every quarter and stopped at the
    enricher, which read six scalars off the top of the payload. A fund's
    holdings are the one relationship that links two companies with no supply,
    sector or competitive tie -- they share a forced seller.
    """
    from unittest.mock import AsyncMock

    from shared.models.events import ThirteenFPosition
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    enricher = TradFiEnricher.__new__(TradFiEnricher)
    enricher.graph = AsyncMock()

    holdings = [
        ThirteenFPosition(ticker="AAPL", issuer_name="APPLE INC",
                          market_value_usd=65_950_296_923.0,
                          shares=227_917_808.0, weight_pct=22.04,
                          change_type="MAINTAINED"),
        ThirteenFPosition(ticker="NVDA", issuer_name="NVIDIA CORP",
                          market_value_usd=1_000.0, shares=10.0,
                          weight_pct=0.5, change_type="NEW"),
        # Never resolved to a ticker: an ETF trust is not an operating company.
        ThirteenFPosition(ticker=None, issuer_name="Ishares Inc",
                          market_value_usd=500.0, shares=5.0, weight_pct=0.1),
    ]

    written = await enricher._link_13f_holdings("BERKSHIRE", "Berkshire Hathaway", holdings)

    assert written == 2, "the unresolved holding should not have been written"
    targets = {c.kwargs["target_id"] for c in enricher.graph.link_entities.await_args_list}
    assert targets == {"AAPL", "NVDA"}

    for call in enricher.graph.link_entities.await_args_list:
        assert call.kwargs["relation_type"] == "OWNS"
        assert call.kwargs["source_label"] == "InstitutionalFiler"
        assert call.kwargs["target_label"] == "Company"

    apple = next(
        c for c in enricher.graph.link_entities.await_args_list
        if c.kwargs["target_id"] == "AAPL"
    )
    # Weight stored as a fraction, like every other weight in the graph.
    assert apple.kwargs["properties"]["weight"] == pytest.approx(0.2204)
    assert apple.kwargs["properties"]["source"] == "sec_edgar_13f"
    assert apple.kwargs["properties"]["change_type"] == "MAINTAINED"


@pytest.mark.asyncio
async def test_no_holdings_writes_nothing_and_does_not_raise():
    from unittest.mock import AsyncMock
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    enricher = TradFiEnricher.__new__(TradFiEnricher)
    enricher.graph = AsyncMock()
    assert await enricher._link_13f_holdings("X", "X", []) == 0
    assert await enricher._link_13f_holdings("X", "X", None) == 0
    enricher.graph.link_entities.assert_not_awaited()


def test_the_predicate_and_labels_are_in_the_ontology():
    """`OWNS` and `InstitutionalFiler` must survive the write path.

    An unrecognised predicate is silently rewritten to RELATED_TO and an
    unrecognised label to Entity, so a typo here would land these edges in the
    62% of the graph that means nothing.
    """
    from shared.models.ontology import (
        ALLOWED_NODE_LABELS, VALID_PREDICATES, resolve_node_label,
    )
    assert "OWNS" in VALID_PREDICATES
    assert "InstitutionalFiler" in ALLOWED_NODE_LABELS
    # And a filer id must not be re-typed as an equity by the symbol classifier.
    assert resolve_node_label(
        "InstitutionalFiler", "COATUE", source="test"
    ) == "InstitutionalFiler"
