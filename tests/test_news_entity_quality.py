"""
tests/test_news_entity_quality.py

The most frequently "named entities" in 48 hours of news were English stopwords.

    THE 1098 | TO 891 | IN 813 | OF 806 | A 742 | AND 738 | ON 548 | FOR 520

`is_valid_primary_equity()` answers a structural question -- could this be a
primary US common equity -- and 1-5 uppercase letters always could. It returns
True for THE, for AND, and for ZZZZZ. The news enricher used it to decide what
counted as a named entity, so every short word in a headline became a ticker.

The cost is not a noisy column. Those tokens land in `named_entities` and in
`tags`, and:

  * the scenario tracker matches signals against both with array containment,
    so a stopword in the array matches a stopword in a signal;
  * the anomaly scorer computes entity_boost = len(named_entities) * per_entity,
    so a headline scored *higher* for containing more ordinary English.

Two stages now, because they answer different questions at different costs. The
shape test is a denylist, runs in-process, and removes the closed-class words
for free -- but no denylist rejects ZZZZZ, because nothing about its shape is
wrong. Membership in the live universe is the allowlist, and it does.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.equities import (  # noqa: E402
    EQUITY_UNIVERSE_KEY, confirm_tickers, is_valid_primary_equity, looks_like_a_ticker,
)


class FakeRedis:
    def __init__(self, universe=None, missing=False):
        self.universe = set(universe or [])
        self.missing = missing
        self.calls = 0
        self.raw = self

    async def exists(self, key):
        return 0 if self.missing else 1

    async def smismember(self, key, members):
        self.calls += 1
        return [1 if m in self.universe else 0 for m in members]


UNIVERSE = {"NVDA", "AAPL", "F", "GM", "T", "KO", "V", "MU", "TSLA"}


# -- the shape test, and its limit ---------------------------------------------

@pytest.mark.parametrize("word", ["THE", "TO", "OF", "AND", "IS", "HAS", "FOR", "WITH", "AS"])
def test_the_structural_check_accepts_stopwords(word):
    """Kept as the reason the second stage exists."""
    assert is_valid_primary_equity(word) is True
    assert looks_like_a_ticker(word) is False


@pytest.mark.parametrize("ticker", ["F", "GM", "T", "KO", "V", "MU", "NVDA"])
def test_real_short_tickers_survive_the_denylist(ticker):
    """Ford, GM, AT&T, Coca-Cola, Visa, Micron. A stopword list that catches
    these has traded one problem for a worse one."""
    assert looks_like_a_ticker(ticker) is True


@pytest.mark.parametrize("junk", ["ZZZZZ", "QQQZ", "XYZAB"])
def test_the_denylist_cannot_reject_unlisted_symbols(junk):
    """Nothing about their shape is wrong, which is the whole argument for
    checking membership rather than form."""
    assert looks_like_a_ticker(junk) is True


# -- the allowlist -------------------------------------------------------------

@pytest.mark.anyio
async def test_only_listed_symbols_survive():
    redis = FakeRedis(UNIVERSE)
    got = await confirm_tickers(["NVDA", "THE", "ZZZZZ", "F", "AND"], redis)
    assert got == ["NVDA", "F"]


@pytest.mark.anyio
async def test_one_round_trip_regardless_of_candidate_count():
    """A bare-word scan considers 60-80 tokens per headline on a path handling
    ~400 events a minute. Awaiting each one is the wrong shape."""
    redis = FakeRedis(UNIVERSE)
    await confirm_tickers(["NVDA", "AAPL", "TSLA", "MU", "V"], redis)
    assert redis.calls == 1


@pytest.mark.anyio
async def test_the_prefilter_runs_before_the_round_trip():
    """Stopwords must never reach Redis -- they are the bulk of the candidates
    and the denylist settles them for free."""
    redis = FakeRedis(UNIVERSE)
    await confirm_tickers(["THE", "AND", "OF", "TO", "NVDA"], redis)
    assert redis.calls == 1


@pytest.mark.anyio
async def test_nothing_to_confirm_makes_no_call_at_all():
    redis = FakeRedis(UNIVERSE)
    assert await confirm_tickers(["THE", "AND", "OF"], redis) == []
    assert redis.calls == 0


# -- degradation ---------------------------------------------------------------

@pytest.mark.anyio
async def test_a_missing_universe_degrades_to_the_prefilter():
    """An empty set means reference data has not loaded, not that the world
    contains no equities. Returning nothing would silence news enrichment."""
    redis = FakeRedis(UNIVERSE, missing=True)
    assert await confirm_tickers(["NVDA", "THE", "ZZZZZ"], redis) == ["NVDA", "ZZZZZ"]


@pytest.mark.anyio
async def test_no_redis_at_all_still_returns_the_prefiltered_set():
    assert await confirm_tickers(["NVDA", "THE"], None) == ["NVDA"]


@pytest.mark.anyio
async def test_a_redis_failure_does_not_lose_the_headline():
    class Broken:
        raw = None

        def __init__(self):
            self.raw = self

        async def exists(self, key):
            raise ConnectionError("down")

    assert await confirm_tickers(["NVDA", "THE"], Broken()) == ["NVDA"]


@pytest.mark.anyio
async def test_duplicates_collapse():
    redis = FakeRedis(UNIVERSE)
    assert await confirm_tickers(["NVDA", "nvda", "NVDA "], redis) == ["NVDA"]


def test_the_enricher_confirms_before_publishing():
    source = (ROOT / "services/enrichment/enrichers/news.py").read_text(encoding="utf-8")
    assert "await confirm_tickers(extracted_tickers, self.redis)" in source
    assert "if is_valid_primary_equity(clean_t):" not in source


def test_the_universe_key_is_the_one_the_radar_maintains():
    assert EQUITY_UNIVERSE_KEY == "sentinel:equities:valid_set"


# -- homographs: real symbols that are also ordinary words ---------------------

def _bare_word_candidates(text: str) -> list:
    """The enricher's bare-word path, mirrored."""
    out = []
    for word in text.split():
        token = word.strip("$,.():;[]{}'\"")
        if not token.isupper() or len(token) < 2:
            continue
        clean = token.upper()
        if len(clean) <= 5 and clean.isalpha() and looks_like_a_ticker(clean):
            out.append(clean)
    return out


@pytest.mark.parametrize(
    "headline",
    [
        "Hong Kong stocks slid on the open",
        "Escalation amid war in the region",
        "Analysts say next year looks difficult",
        "Time is running short for a deal",
    ],
)
def test_prose_homographs_are_not_tickers(headline):
    """HONG, KONG, AMID, WAR, SAY, YEAR and TIME are all genuinely listed
    symbols, so no allowlist can reject them. Measured over 1,200 headlines they
    were the top "tickers" the news produced -- AI 60, WAR 51, HONG 46, TIME 45,
    SAY 44 -- pulled from "Hong Kong", "amid", "next year"."""
    assert _bare_word_candidates(headline) == []


@pytest.mark.parametrize(
    "headline,expected",
    [
        ("NVDA and AMD rallied", ["NVDA", "AMD"]),
        ("AI stocks led the tape", ["AI"]),
        ("MU guided lower", ["MU"]),
    ],
)
def test_symbols_written_as_symbols_survive(headline, expected):
    assert _bare_word_candidates(headline) == expected


def test_a_lone_capital_is_not_a_ticker():
    """A single capital in prose is an initial far more often than it is a
    symbol -- A is Agilent and F is Ford, but "A. Smith" and "F" in a sentence
    are neither. Both are dropped from the bare-word path; $A and $F still
    arrive through the cashtag pattern, where the author was explicit."""
    assert _bare_word_candidates("A. Smith said F is cheap") == []


def test_the_enricher_checks_case_before_uppercasing():
    """.upper() first destroys the only signal that separates AAPL from Aapl."""
    source = (ROOT / "services/enrichment/enrichers/news.py").read_text(encoding="utf-8")
    assert "if not token.isupper() or len(token) < 2:" in source
    assert 'clean_word = word.strip("$,.():;[]{}\'\\"").upper()' not in source
