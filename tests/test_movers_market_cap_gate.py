"""
tests/test_movers_market_cap_gate.py

A percentage move ranks by how small the denominator was.

The movers board is built from the radar's whole sweep -- 11,579 symbols on the
live deployment -- scored by the day's move and nothing else. Measured during a
regular session, that board's own top and bottom:

    gainers   MSAIW +18,800%   MGN +2,720%   SFWL +1,233%   JAGX +1,053%
              LCFYW +717%      AT +562%      IZM +453%      AEMD +384%
    losers    BBLGW -88.9%     RETO -88.4%   FGIWW -86.8%   SCAGW -85.6%

MSAIW is a warrant that went from $0.0001 to $0.0189. Six of those eleven end
in W, the warrant suffix. Not one recognisable company appears in either
direction.

With the gate on, from the same board and the same sweep:

    GNRC +20.61% $10.33B    WLYB +19.33% $2.67B
    ABSI +18.59%  $1.38B    SECZ +17.10% $1.27B

and the backfill's own resolutions show why the rest went: SFWL is a $49m
company, JAGX a $1.2m one, BRKL $975m -- just under -- and every W ticker
resolves to no market capitalisation at all.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

RADAR_ROUTE = ROOT / "services" / "api_gateway" / "routes" / "radar.py"
RADAR_COLLECTOR = ROOT / "services" / "collector-radar" / "main.py"
BOARD = ROOT / "frontend" / "src" / "components" / "MoversBoard.tsx"


# -- the cache ----------------------------------------------------------------


def test_unknown_is_not_small():
    """A gated board that admits unmeasured names cannot make its claim."""
    from shared.utils.market_cap import parse_market_cap, NOT_A_COMPANY

    assert parse_market_cap(None) is None
    assert parse_market_cap("") is None
    assert parse_market_cap(NOT_A_COMPANY) is None, (
        "a resolved warrant has no market cap, and must not read as zero"
    )
    assert parse_market_cap("0") is None
    assert parse_market_cap("1268574346.663739") == 1268574346.663739
    assert parse_market_cap(b"10330000000.0") == 10330000000.0


def test_the_default_floor_is_a_billion():
    from shared.utils.market_cap import DEFAULT_MIN_MARKET_CAP_USD

    assert DEFAULT_MIN_MARKET_CAP_USD == 1_000_000_000.0


def test_a_resolved_absence_is_remembered():
    """Otherwise every warrant on the board is re-asked on every pass."""
    code = (ROOT / "shared" / "utils" / "market_cap.py").read_text(encoding="utf-8")
    assert "has_been_resolved" in code
    assert "NOT_A_COMPANY" in code


# -- the gate -----------------------------------------------------------------


def test_the_endpoint_gates_and_says_so():
    code = RADAR_ROUTE.read_text(encoding="utf-8")
    assert "min_market_cap_usd" in code
    assert "DEFAULT_MIN_MARKET_CAP_USD" in code, "the floor must default to a billion"
    assert '"min_market_cap_usd": min_market_cap_usd' in code, (
        "a client must be able to tell a quiet board from a gate nothing passed"
    )


def test_the_gate_walks_past_the_first_page():
    """Filtering the first `limit` rows would return an almost empty board.

    The qualifying companies are hundreds of places down the ranking: of the
    top 400 gainers, 5 passed the gate, 38 were below it and the rest were
    unresolved. Taking 20 and filtering them would have returned nothing.
    """
    code = RADAR_ROUTE.read_text(encoding="utf-8")
    assert "MOVERS_WALK_CAP" in code, "the walk must be bounded"
    assert "while len(rows) < limit and walked < MOVERS_WALK_CAP" in code


def test_the_gate_can_be_turned_off():
    """0 returns the raw board, warrants included, for anyone who wants it."""
    code = RADAR_ROUTE.read_text(encoding="utf-8")
    assert "if min_market_cap_usd <= 0:" in code


# -- the backfill -------------------------------------------------------------


def test_the_backfill_is_bounded_and_off_the_sweep():
    """The sweep runs every 60s over 11,688 symbols; Finnhub prices one a call."""
    code = RADAR_COLLECTOR.read_text(encoding="utf-8")
    assert "MCAP_LOOKUPS_PER_PASS" in code
    assert "MCAP_CANDIDATE_DEPTH" in code, (
        "only the extremes of the ranking can ever reach the board"
    )
    # After the publish, so a provider problem cannot delay the board itself.
    body = code.split("await _publish_movers(")[1]
    assert "_backfill_market_caps" in body


def test_a_rate_limit_is_not_recorded_as_an_answer():
    """Caching a 429 as "no market cap" would exclude the company for a week."""
    code = RADAR_COLLECTOR.read_text(encoding="utf-8")
    assert "_MarketCapUnavailable" in code
    resolver = code.split("async def _resolve_market_cap")[1].split("class _MarketCapUnavailable")[0]
    assert "raise _MarketCapUnavailable" in resolver


# -- and the board says what it did -------------------------------------------


def test_the_board_shows_the_size_it_gated_on():
    code = BOARD.read_text(encoding="utf-8")
    assert "market_cap_usd" in code
    assert "min_market_cap_usd" in code, (
        "an empty gated board must explain itself, not read as 'no moves today'"
    )


# -- the currency the figure is denominated in --------------------------------


def test_a_non_usd_market_cap_is_not_a_usd_market_cap():
    """Finnhub reports the figure in the currency of the primary listing.

    TSM's profile2 resolves to the Taiwan Stock Exchange and reports
    61,719,038.554688 million TWD. Multiplied by a million and stored under a
    key whose own name says USD, that is $61.7 trillion -- fifty times the
    company and larger than world GDP. It sat in the live cache.

    ASML is the case that shows why an outlier filter is not the fix: 533,327
    million EUR stored as USD is wrong by the exchange rate alone, roughly nine
    per cent, and no plausibility check would ever flag it.
    """
    from shared.utils.market_cap import parse_market_cap, NOT_IN_USD

    assert parse_market_cap(NOT_IN_USD) is None
    assert parse_market_cap(NOT_IN_USD.encode()) is None


def test_non_usd_is_distinguishable_from_no_figure_at_all():
    """Three states, not two: a figure, no figure, and a figure we cannot read.

    Both refuse a size gate, which is the documented rule that unknown is not
    small. They are kept apart so the backfill can tell a warrant it has already
    resolved from a foreign listing it has already declined to convert -- and
    re-ask neither.
    """
    from shared.utils.market_cap import NOT_A_COMPANY, NOT_IN_USD

    assert NOT_A_COMPANY != NOT_IN_USD


def test_the_resolver_reads_the_currency_field():
    """The currency was returned by the provider and ignored by the caller."""
    source = RADAR_COLLECTOR.read_text(encoding="utf-8")
    assert '"currency"' in source or "'currency'" in source, (
        "the resolver must read the currency it is denominating in"
    )


def test_the_earnings_watchlist_floor_is_in_dollars():
    """`mcap_b >= mcap_floor_b` injects a symbol into the watched set.

    The floor is 500 billion dollars. TSM in TWD clears it by a factor of 123,
    so a foreign listing could add itself to the watchlist on its exchange rate
    rather than its size.
    """
    source = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    window = source[source.index("mcap_cache_key") : source.index("mcap_cache_key") + 3000]
    assert "currency" in window, (
        "the earnings market-cap gate must check the currency of the figure"
    )


def test_a_tier_is_not_claimed_for_a_currency_we_cannot_read():
    from services.enrichment.ref_data import _classify_market_cap_tier

    assert _classify_market_cap_tier(533_327.0, "USD") == "mega"
    assert _classify_market_cap_tier(533_327.0, "EUR") == "unknown"
    assert _classify_market_cap_tier(61_719_038.0, "TWD") == "unknown"
    # An absent currency keeps the previous behaviour rather than erasing every
    # tier the platform already has.
    assert _classify_market_cap_tier(533_327.0, "") == "mega"


# -- a ticker that is also an English word ------------------------------------


def test_the_news_counter_does_not_ask_about_ticker_shaped_words():
    """`named_entities` and the board's tickers are both bare uppercase tokens.

    So a mover called ON, IT, A, ALL, SO or ARE is indistinguishable from the
    preposition, and every one of those is a real listed company: ON
    Semiconductor, Gartner, Agilent, Allstate, Southern, Alexandria Real Estate.

    The historical corpus holds 23,173 rows of exactly this. The ticker
    extractor accepted any short alphabetic word until it was corrected on
    1 September, leaving THE (10,200), A (7,169) and ON (4,673) among the most
    frequent "named entities" the platform has ever stored. Matching ON
    Semiconductor against that counts every headline containing the word "on".

    It does not bite today only because the contamination stops on 1 September
    and the window caps at 168 hours. That is a fact about the date, not the
    code.
    """
    from services.api_gateway.routes.radar import _NON_SUBJECT_TICKERS

    for word in ("ON", "IT", "A", "ALL", "SO", "ARE", "THE", "NEW"):
        assert word in _NON_SUBJECT_TICKERS, f"{word} is a word before it is a ticker"
    for real in ("NVDA", "AAPL", "MU", "GM", "KO", "F", "T", "V"):
        assert real not in _NON_SUBJECT_TICKERS, f"{real} is a real ticker"

    # GO (Grocery Outlet) is deliberately absent from the curated list, and
    # appears 99 times in the corpus against THE's 10,200. The list is the
    # writer's own policy and is kept short on purpose -- widening it here
    # would change what the extractor accepts platform-wide, which is a
    # different decision from what this counter asks about.
    assert "GO" not in _NON_SUBJECT_TICKERS


def test_us_is_kept_because_it_is_a_real_subject():
    """The correlation engine records the same exception, for the same reason.

    spaCy correctly tags US as a GPE for "United States", and it is the entire
    residual after the writer was corrected. Stripping it would discard the one
    genuine subject in the set.
    """
    from services.api_gateway.routes.radar import _NON_SUBJECT_TICKERS

    assert "US" not in _NON_SUBJECT_TICKERS


def test_the_counter_answers_zero_rather_than_querying_nothing():
    """A board of only ticker-shaped words must not send an empty ANY()."""
    source = (
        ROOT / "services" / "api_gateway" / "routes" / "radar.py"
    ).read_text(encoding="utf-8")
    attach = source.index("async def _attach_news_counts")
    body = source[attach : attach + 3000]
    assert "if not askable:" in body
    assert "askable, window_hours" in body, (
        "the filtered list must be what is actually queried"
    )
