"""One owner for "is this prediction market worth watching".

There were two filters over a single Redis set and they disagreed.

`sentinel:polymarket:watched_slugs` decides which questions the collector
subscribes to. The enricher added to it under a reject-known-bad rule -- a regex
of sports and weather prefixes -- and the collector pruned it under a
require-positive-match rule against a vocabulary of actionable subjects. A slug
that passes one and fails the other is added on every enriched event and removed
on every sweep, forever. Measured on both:
`will-wti-dip-to-90-in-september-2026` and a European mayoral race were kept by
the enricher and pruned by the collector, so a genuinely on-domain crude market
oscillated in and out of the set and was never reliably polled.

Both sides now call `is_relevant_market`, and the collector already prunes with
`is_relevant_market({"slug": x})` -- slug alone -- so the enricher applying the
same predicate to the same slug makes the two agree by construction rather than
by coincidence.
"""
import re
from typing import Any, Dict

# Subjects this platform can actually act on. A prediction market is useful here
# when its resolution would move an instrument, a currency, a commodity or a
# border -- that is the only reason the correlation layer has to look at one.
#
# Matched on word boundaries against the question and slug, so "fed" does not
# match "federer" and "war" does not match "warriors" -- both of which are real
# Polymarket questions that a substring match would have admitted.
RELEVANT_MARKET_TERMS = frozenset({
    # Monetary policy and rates
    "fed", "fomc", "interest", "rate", "rates", "inflation", "cpi", "pce",
    "recession", "gdp", "unemployment", "jobs", "payrolls", "powell", "ecb",
    "boj", "treasury", "yield", "yields", "debt", "default", "shutdown",
    # Instruments and markets
    "stock", "stocks", "equity", "equities", "nasdaq", "sp500", "dow",
    "bitcoin", "btc", "ethereum", "eth", "crypto", "etf", "ipo", "earnings",
    # The majors by name, for the same reason "wti" is below: the vocabulary
    # reached for the asset class and Polymarket writes the ticker. Measured on
    # the live set, `will-avax-reach-16-by-december-31-2026` is a crypto price
    # market that matched none of the crypto words above.
    "solana", "sol", "avax", "xrp", "doge", "dogecoin", "bnb", "cardano", "ada",
    "oil", "opec", "gas", "gold", "commodity", "commodities", "dollar",
    "yuan", "euro", "yen", "currency",
    # The instruments those commodity words are actually traded as. "oil" was
    # here and "wti" was not, so `will-wti-dip-to-90-in-september-2026` -- a
    # crude market, on-domain by any reading -- failed the positive match and
    # was pruned on every sweep while the enricher kept re-adding it.
    "wti", "brent", "crude", "opec+", "natgas", "lng",
    "copper", "silver", "wheat", "corn", "soybeans", "uranium",
    # Geopolitics and conflict
    "war", "ceasefire", "invasion", "invade", "sanctions", "sanction",
    "nato", "ukraine", "russia", "china", "taiwan", "iran", "israel",
    "gaza", "korea", "strait", "blockade", "military", "strike", "missile",
    "nuclear", "treaty", "tariff", "tariffs", "trade", "embargo",
    # Government, insofar as it moves the above
    "election", "president", "presidential", "congress", "senate",
    "impeach", "cabinet", "resign", "coup", "referendum",
    # A head of government leaving is a rates and currency event before it is a
    # political one. `will-emmanuel-macron-be-the-next-leader-out-before-2027`
    # matched none of the words above -- the vocabulary reached for "president"
    # and "resign" and the market says "leader" and "out" -- so a filter that
    # keeps it and a filter that drops it could both look reasonable, which is
    # how the set came to oscillate.
    "leader", "leadership", "prime", "minister", "chancellor", "governor",
    "parliament", "government", "confidence", "dissolution",
})

# Subjects that are never actionable here, however liquid. Checked first, so a
# question that happens to contain "strike" in a sporting sense is still refused.
EXCLUDED_MARKET_TERMS = frozenset({
    "nfl", "nba", "mlb", "nhl", "ufc", "soccer", "football", "basketball",
    "baseball", "hockey", "tennis", "golf", "olympics", "superbowl",
    "worldcup", "premier", "champions", "playoff", "playoffs", "mvp",
    "grammy", "oscar", "oscars", "emmy", "billboard", "movie", "album",
    "netflix", "taylor", "swift", "kardashian", "celebrity", "rotten",
    "boxoffice", "eurovision", "meme",
})

# League and weather slug prefixes, carried over from the enricher's own filter
# so that folding the two together loses nothing. The positive match below would
# already refuse these; keeping them is belt and braces for a league whose name
# happens to collide with an actionable word.
OFF_DOMAIN_SLUG = re.compile(
    r"^(?:lol|cs2|mlb|nfl|nba|nhl|epl|uwcl|lec|cfb|val|itf|scoc|crickcl|aut|per\d|el\d|tur\d|"
    r"jap|egy|qat|por|cze|lal|sec|mgc|big\d)-"
    r"|rushing-yards|halftime|moneyline|first-half|-nrfi|exact-score"
    r"|highest-temperature|will-it-rain|where-will-it-rain",
    re.I,
)

_MARKET_WORD = re.compile(r"[a-z0-9+]+")


def is_relevant_market(market: Dict[str, Any]) -> bool:
    """True when a prediction market's resolution could move something we watch.

    Polymarket is ordered by volume, and its volume leaders are sports. This is
    the difference between a prediction feed and a scoreboard.

    Accepts a partial market: the collector prunes with `{"slug": x}` alone, and
    the enricher has only a slug, so both reach the same answer for the same
    slug. Passing the full question and description simply gives it more to
    read.
    """
    if not isinstance(market, dict):
        return False
    slug = str(market.get("slug") or "")
    if slug and OFF_DOMAIN_SLUG.search(slug):
        return False
    text = " ".join(
        str(market.get(k) or "")
        for k in ("question", "slug", "title", "description")
    ).lower()
    if not text.strip():
        return False

    words = set(_MARKET_WORD.findall(text))
    if words & EXCLUDED_MARKET_TERMS:
        return False
    return bool(words & RELEVANT_MARKET_TERMS)


def slug_is_relevant(slug: str) -> bool:
    """`is_relevant_market` for a caller that holds only the slug."""
    return is_relevant_market({"slug": slug})
