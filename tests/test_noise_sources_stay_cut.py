"""Two sources measured as reaching nothing, and the guards that keep them cut.

SEMANTIC_001 published "Semantic Resemblance: BTCUSDT across 1 subject(s) in 1
domain(s)" 15,946 times in the thirty days to 4 October -- one subject's events
resembling each other -- and its crypto scenarios resolved 1 confirmed to 9
denied. The Ethereum whale stream produced ~7,000 events a day whose subject is
always a wallet address, which no remaining rule or agent can join.
"""

import importlib
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_a_semantic_cluster_needs_two_of_something():
    src = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    guard = src[src.index("skip_semantic = ("):]
    guard = guard[:guard.index("if skip_semantic:")]
    assert "distinct_subjects < 2 and semantic_domain_count < 2" in guard
    # And the inputs exist before the guard reads them.
    assert src.index("distinct_subjects = len(_named_subjects)") < src.index("skip_semantic = (")
    assert src.index("semantic_domain_count = len(semantic_domains)") < src.index("skip_semantic = (")


def test_the_onchain_stream_is_off_unless_asked_for(monkeypatch):
    path = ROOT / "services/collector-crypto/main.py"
    src = path.read_text(encoding="utf-8")
    assert 'ONCHAIN_WHALES_ENABLED = os.getenv("ONCHAIN_WHALES", "0") == "1"' in src
    gather = src[src.index("await asyncio.gather(\n            stream_coinbase_market_data"):]
    gather = gather[:gather.index(")\n    except")]
    assert "if ONCHAIN_WHALES_ENABLED" in gather
    assert "\n            stream_onchain_whales(producer, redis_client)," not in gather


def test_earnings_events_are_published_only_for_priced_instruments():
    """5 of 173 tickers with earnings events in thirty days had a quote. The
    context cache that the block-trade enricher reads stays unconditional."""
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    body = src[src.index("async def poll_finnhub_earnings"):]
    body = body[:body.index("\nasync def ", 10)]
    cache = body.index('entity_cache_key("sentinel:earnings", symbol)')
    gate = body.index("collector_tradfi.earnings_unpriced")
    publish = body.index('source="finnhub_earnings"')
    assert cache < gate < publish
    assert "quote_key(symbol)" in body[cache:gate]


def test_the_news_feeds_are_the_ones_that_answered():
    """Probed 4 October: ten feeds returned 403/404/500 or nothing for days, three
    had moved. The additions were chosen by their measured reach to an instrument."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("news_feeds", ROOT / "services/collector-news/main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["news_feeds"] = mod
    spec.loader.exec_module(mod)
    feeds = {name: url for name, url, *_ in mod.FEEDS}
    for dead in ("ap_world", "tanker_trackers", "seatrade", "lloyds_list", "rferl", "unusual_whales",
                 "us_state_dept", "cisa_alerts", "chatham_house", "cyber_defense_mag"):
        assert dead not in feeds
    assert feeds["ofac_news"] == "https://ofac.treasury.gov/rss.xml"
    assert feeds["maritime_exec"] == "https://maritime-executive.com/articles.rss"
    assert feeds["nikkei_asia"] == "https://asia.nikkei.com/rss/feed/nar"
    for added in ("seekingalpha_currents", "cnbc_tech", "cnbc_earnings", "cnbc_energy",
                  "eia_today", "oilprice", "splash247", "hellenic_shipping", "fed_press"):
        assert added in feeds
    assert len(feeds) == len(mod.FEEDS)
