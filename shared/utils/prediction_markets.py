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
    # (Altcoin names removed on 3 October: the crypto scope is BTC and ETH, and
    # a SOL or AVAX price market has no instrument here to move.)
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
    # Government, insofar as it moves the above. US election-cycle words --
    # election, president, presidential, congress, senate -- were removed on
    # 3 October: the live feed was 2028 nominee markets and "Xi out before
    # 2027" at 97.5% NO, moving nothing an equity desk prices this year.
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
    # A league code with a division digit (chi2-, kor1-, bra2-): Chinese Super
    # League totals passed the sweep on "China" in their title and led the
    # tracked list on 31 Oct 2026, though the slug-only prune rejected them.
    r"|^[a-z]{3}\d-"
    r"|team-total|-total-\d|-spread-|both-teams-to-score|-btts|corners"
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


def devig(prices):
    """Prices of a field with exactly one winner, scaled to sum to 1 (proportional de-vig), or None.

    A market's quoted prices add to more than 1 by its overround, so each price overstates its outcome. Only for
    fields of mutually exclusive outcomes: independent contracts have no reason to add to 1, and scaling them would
    invent a constraint. None for a missing, negative or non-finite price, or a field summing to nothing."""
    import math as _m
    try:
        xs = [float(p) for p in prices]
    except (TypeError, ValueError):
        return None
    if len(xs) < 2 or any((not _m.isfinite(x)) or x < 0 for x in xs):
        return None
    total = sum(xs)
    return [x / total for x in xs] if total > 0 else None


def shrink_to_prior(price, volume, prior=0.5, strength=2000.0):
    """A price pulled toward `prior` in proportion to how little has traded: (v*price + k*prior) / (v + k).

    A contract that has traded $50 quotes a price that means little; $200,000 means a lot. `strength` is the volume
    (USD) at which the price and the prior weigh equally; 2,000 is a starting value to be calibrated against resolved
    markets, not a measured one. None when the price or volume is unusable."""
    import math as _m
    try:
        p, v = float(price), float(volume)
    except (TypeError, ValueError):
        return None
    if not (_m.isfinite(p) and _m.isfinite(v)) or not 0.0 <= p <= 1.0 or v < 0 or strength <= 0:
        return None
    return (v * p + strength * prior) / (v + strength)

def order_watch_list(dynamic_by_volume, stored, cap):
    """The slugs to poll: this sweep's relevant markets in the order the sweep returned them (volume, highest first), then the other stored
    slugs in a stable order, cut to `cap`. The sweep list is already filtered; only stored slugs that are not in it follow, so the
    cap keeps liquid markets before quiet ones. (It was alphabetical: the sort threw the volume order away.)"""
    seen = set()
    ordered = []
    wanted = set(stored)
    for slug in dynamic_by_volume:
        if slug in wanted and slug not in seen:
            seen.add(slug)
            ordered.append(slug)
    ordered.extend(sorted(wanted - seen))
    return ordered[:cap]


def belongs_to_slug(market, parent, queried):
    """Whether a market returned for `queried` really is that market or one of that event's markets. Gamma's /markets ignores an unknown
    filter such as event_slug and answers with its default 20 markets, which were then labelled with every queried slug (a bitcoin
    slug beside a Harris-nomination question, 13 Nov 2026)."""
    if not isinstance(market, dict):
        return False
    return market.get("slug") == queried or (isinstance(parent, dict) and parent.get("slug") == queried)



