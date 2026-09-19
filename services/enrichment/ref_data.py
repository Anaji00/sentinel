"""
services/enrichment/ref_data.py

Daily batch reference data manager for ticker metadata.
Queries Finnhub /stock/profile2 to cache sector, industry, exchange,
market_cap_tier, and asset_type per symbol in Redis with 24h TTL.

This module is designed to be called as a background job, NOT per-trade.
"""

import asyncio
import logging
import os
from typing import Optional, Dict, Any
from shared.utils.quiet_failures import swallowed
from shared.utils.watchlists import WATCHED_EQUITIES_KEY

logger = logging.getLogger("enrichment.ref_data")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
REFDATA_TTL = 86400  # 24 hours
REFDATA_PREFIX = "sentinel:refdata:"
ASSET_TYPE_PREFIX = "sentinel:asset_type:"
INDEX_MEMBERSHIP_PREFIX = "sentinel:index_membership:"
PEERS_PREFIX = "sentinel:peers:"

# Peers change on a corporate-action timescale, not a market one.
PEERS_TTL = 7 * 24 * 3600

# How many peers to keep per symbol.
#
# Finnhub returns the symbol itself first and then its comparables, longest
# lists running to a dozen. Ten is enough to describe a competitive set and
# short enough that one crowded sector cannot dominate the graph.
MAX_PEERS = 10

# Major US indices to track for index co-membership edges
INDEX_SYMBOLS = [
    ("^GSPC", "SPX"),   # S&P 500
    ("^NDX", "NDX"),    # Nasdaq 100
    ("^RUT", "RUT"),    # Russell 2000
    ("^DJI", "DJIA"),   # Dow Jones
]


def _classify_market_cap_tier(mcap_millions: float, currency: str = "USD") -> str:
    """Classify market cap in millions of USD into a human-readable tier.

    The currency matters and was not being checked. Finnhub reports this figure
    in the currency of the primary listing, so ASML arrives as 533,327 million
    EUR and TSM as 61,719,038 million TWD. The tiers here are coarse enough that
    a mega-cap stays a mega-cap through the error, which is why it survived --
    but the boundaries are absolute numbers, and a 400-million-yen company (about
    $2.7m, micro) lands in "small" on the same arithmetic.

    Unknown rather than converted: this module has no FX rate, and a tier is a
    claim about size that a wrong denomination cannot support.
    """
    if str(currency or "").strip().upper() not in ("", "USD"):
        return "unknown"
    if mcap_millions >= 200_000:
        return "mega"
    elif mcap_millions >= 10_000:
        return "large"
    elif mcap_millions >= 2_000:
        return "mid"
    elif mcap_millions >= 300:
        return "small"
    elif mcap_millions > 0:
        return "micro"
    return "unknown"


async def fetch_and_cache_reference_data(
    redis_client,
    symbol: str,
    session=None,
    graph_writer=None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch reference data for a single symbol from Finnhub /stock/profile2
    and cache it in Redis. Emits structural graph proposals (Company, Sector, Index)
    via GraphWriter when provided (§8.1).
    
    NOT designed for hot-path usage — call from daily batch jobs only.
    """
    import aiohttp

    if not FINNHUB_API_KEY:
        logger.debug("No FINNHUB_API_KEY set, skipping ref data fetch")
        return None

    cache_key = f"{REFDATA_PREFIX}{symbol}"

    # Check if already cached and fresh.
    #
    # A cache hit returns the reference data and still promotes it to the
    # graph. It used to return here outright, and that made the graph writes
    # below unreachable for any symbol seen before: with 547 symbols cached, a
    # refresh reported "400/400 symbols updated" and wrote nothing at all.
    #
    # The distinction is that this function does two things -- it caches a
    # vendor response, and it promotes that response into the knowledge graph --
    # and only the first is idempotent against Redis. Skipping the network call
    # is the point of the cache; skipping the graph write is a side effect of
    # where the `return` was put, and it meant OPERATES_IN, COMPETES_WITH and
    # the country edges could only ever be written the first time a symbol was
    # ever seen, so no later repair could reach them.
    try:
        cached = await redis_client.raw.get(cache_key)
        if cached:
            import json
            ref_data = json.loads(cached)
            await _promote_reference_data_to_graph(graph_writer, symbol, ref_data)
            return ref_data
    except Exception as _exc:
        swallowed("enrichment.ref_data.fetch_and_cache_reference_data", _exc, logger)

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        profile_url = "https://finnhub.io/api/v1/stock/profile2"
        async with session.get(
            profile_url, params={"symbol": symbol, "token": FINNHUB_API_KEY}
        ) as resp:
            if resp.status == 429:
                logger.debug(f"Finnhub rate-limited during ref data fetch for {symbol}")
                return None
            if resp.status != 200:
                logger.debug(f"Finnhub profile2 returned {resp.status} for {symbol}")
                return None

            profile = await resp.json()

        sector = profile.get("finnhubIndustry", "") or ""
        industry = profile.get("gicsSector", "") or profile.get("finnhubIndustry", "") or ""
        exchange = profile.get("exchange", "") or ""
        mcap_millions = float(profile.get("marketCapitalization", 0) or 0)
        listing_currency = str(profile.get("currency") or "").strip().upper()
        market_cap_tier = _classify_market_cap_tier(mcap_millions, listing_currency)
        asset_type = profile.get("type", "Common Stock") or "Common Stock"
        country = profile.get("country", "") or ""

        ref_data = {
            "symbol": symbol,
            "sector": sector,
            "industry": industry,
            "exchange": exchange,
            "market_cap_tier": market_cap_tier,
            "market_cap_millions": mcap_millions,
            # Carried so a reader can tell what the figure above is denominated
            # in, rather than assuming.
            "market_cap_currency": listing_currency,
            "asset_type": asset_type,
            "country": country,
            "index_membership": [],
        }

        # Merge cached index membership if available
        try:
            idx_cached = await redis_client.raw.get(f"{INDEX_MEMBERSHIP_PREFIX}{symbol}")
            if idx_cached:
                import json as _json
                ref_data["index_membership"] = _json.loads(idx_cached)
        except Exception as _exc:
            swallowed("enrichment.ref_data.fetch_and_cache_reference_data", _exc, logger)

        import json
        pipe = redis_client.raw.pipeline()
        pipe.set(cache_key, json.dumps(ref_data), ex=REFDATA_TTL)
        pipe.set(f"{ASSET_TYPE_PREFIX}{symbol}", asset_type, ex=REFDATA_TTL)
        # Also update the existing mcap cache for consistency
        if mcap_millions > 0:
            mcap_b = mcap_millions / 1000.0
            pipe.set(f"sentinel:mcap:{symbol}", str(mcap_b), ex=REFDATA_TTL)
        await pipe.execute()

        # Promote to Knowledge Graph via GraphWriter (§8.1)
        await _promote_reference_data_to_graph(graph_writer, symbol, ref_data)

        logger.debug(f"Cached ref data for {symbol}: sector={sector}, exchange={exchange}, type={asset_type}")
        return ref_data

    except Exception as e:
        logger.error(f"Failed to fetch ref data for {symbol}: {e}")
        return None
    finally:
        if owns_session:
            await session.close()


async def get_reference_data(redis_client, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve cached reference data for a symbol. Returns None if not cached.
    This is safe for hot-path usage — Redis read only, no HTTP calls.
    """
    import json
    try:
        cached = await redis_client.raw.get(f"{REFDATA_PREFIX}{symbol}")
        if cached:
            data = json.loads(cached)
            # Merge index membership from separate cache if not already present
            if not data.get("index_membership"):
                try:
                    idx_cached = await redis_client.raw.get(f"{INDEX_MEMBERSHIP_PREFIX}{symbol}")
                    if idx_cached:
                        data["index_membership"] = json.loads(idx_cached)
                except Exception as _exc:
                    swallowed("enrichment.ref_data.get_reference_data", _exc, logger)
            return data
    except Exception as _exc:
        swallowed("enrichment.ref_data.get_reference_data", _exc, logger)
    return None


async def fetch_and_cache_peers(
    redis_client, symbol: str, session=None, graph_writer=None
) -> list:
    """A symbol's competitive set, cached and promoted to COMPETES_WITH edges.

    /stock/peers answers on this key -- eleven names for NVDA -- and nothing in
    this repository has ever called it. The graph held five COMPETES_WITH edges
    in total, so the `competitors` field of every trade recommendation was
    empty, and the query asking for it was correct the whole time.

    Returns the peer list so a caller can use it without a second read. An empty
    list means the symbol has no comparables *or* the lookup failed, and the two
    are distinguished by the cache: a resolved absence is stored, a failure is
    not, so a failure is retried tomorrow and an absence is not re-asked.
    """
    import aiohttp
    import json

    symbol = str(symbol or "").upper().strip()
    if not symbol or not FINNHUB_API_KEY:
        return []

    cache_key = f"{PEERS_PREFIX}{symbol}"
    try:
        cached = await redis_client.raw.get(cache_key)
        if cached:
            raw = cached if isinstance(cached, str) else cached.decode("utf-8")
            return json.loads(raw)
    except Exception as exc:
        swallowed("enrichment.ref_data.peers_cache_read", exc, logger, detail=symbol)

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()
    try:
        async with session.get(
            "https://finnhub.io/api/v1/stock/peers",
            params={"symbol": symbol, "token": FINNHUB_API_KEY},
        ) as resp:
            if resp.status != 200:
                # Not cached: a rate limit is not a statement about the company.
                logger.warning(
                    "Finnhub /stock/peers returned %s for %s; competitor edges "
                    "will not be written this cycle.", resp.status, symbol,
                )
                return []
            payload = await resp.json()
    except Exception as exc:
        swallowed("enrichment.ref_data.peers_fetch", exc, logger, detail=symbol)
        return []
    finally:
        if owns_session:
            await session.close()

    # Finnhub returns the subject first; a company does not compete with itself.
    peers = [
        str(p).upper().strip() for p in (payload or [])
        if p and str(p).upper().strip() != symbol
    ][:MAX_PEERS]

    try:
        await redis_client.raw.set(cache_key, json.dumps(peers), ex=PEERS_TTL)
    except Exception as exc:
        swallowed("enrichment.ref_data.peers_cache_write", exc, logger, detail=symbol)

    if graph_writer and peers:
        for peer in peers:
            try:
                await graph_writer.link_entities(
                    source_id=symbol,
                    relation_type="COMPETES_WITH",
                    target_id=peer,
                    # Unrated rather than 1.0: this is a vendor's published
                    # comparable set, not a measurement, and the graph already
                    # records 272,040 edges that claimed certainty they did not
                    # have.
                    properties={"weight": 1.0, "source": "finnhub_peers"},
                    source_label="Company",
                    target_label="Company",
                )
            except Exception as exc:
                swallowed("enrichment.ref_data.peers_link", exc, logger, detail=symbol)

    return peers


async def fetch_index_constituents(redis_client, session=None, graph_writer=None):
    """
    Fetch constituents for major US indices from Finnhub /index/constituents,
    cache reverse map in Redis, and emit MEMBER_OF graph proposals (§8.1).
    
    Designed for daily batch usage only. Rate-limited with 500ms delay between calls.
    """
    import aiohttp
    import json

    if not FINNHUB_API_KEY:
        logger.debug("No FINNHUB_API_KEY set, skipping index constituents fetch")
        return

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    # Build reverse map: ticker → [index_label, ...]
    reverse_map: Dict[str, list] = {}

    try:
        for finnhub_symbol, label in INDEX_SYMBOLS:
            try:
                url = "https://finnhub.io/api/v1/index/constituents"
                async with session.get(
                    url, params={"symbol": finnhub_symbol, "token": FINNHUB_API_KEY}
                ) as resp:
                    if resp.status == 429:
                        logger.debug(f"Finnhub rate-limited during index constituents fetch for {finnhub_symbol}")
                        await asyncio.sleep(1.0)
                        continue
                    if resp.status == 403:
                        # Premium on Finnhub, and this key does not have it.
                        #
                        # Logged at DEBUG before, in a deployment that emits
                        # none -- so `index_membership` was empty for every
                        # symbol, MEMBER_OF held one edge in a graph of 253,000
                        # entities, and nothing said why. An unavailable source
                        # is a fact to state once, not a silence.
                        logger.warning(
                            "Finnhub /index/constituents is not available on "
                            "this key (403). Index membership will stay empty "
                            "and MEMBER_OF edges will not be written; the "
                            "`index_membership` field of every brief reflects "
                            "that rather than a company being in no index."
                        )
                        return
                    if resp.status != 200:
                        logger.warning(
                            "Finnhub index/constituents returned %s for %s.",
                            resp.status, finnhub_symbol,
                        )
                        continue

                    data = await resp.json()
                    constituents = data.get("constituents", [])
                    for ticker in constituents:
                        reverse_map.setdefault(ticker, []).append(label)

                    logger.debug(f"Fetched {len(constituents)} constituents for {label}")
            except Exception as e:
                logger.debug(f"Failed to fetch index constituents for {finnhub_symbol}: {e}")

            await asyncio.sleep(0.5)  # Rate limit between index calls

        # Cache each ticker's index membership in Redis & promote to graph
        if reverse_map:
            pipe = redis_client.raw.pipeline()
            for ticker, indices in reverse_map.items():
                pipe.set(
                    f"{INDEX_MEMBERSHIP_PREFIX}{ticker}",
                    json.dumps(sorted(set(indices))),
                    ex=REFDATA_TTL,
                )
            await pipe.execute()
            logger.info(f"Cached index membership for {len(reverse_map)} tickers across {len(INDEX_SYMBOLS)} indices")

            if graph_writer:
                for ticker, indices in reverse_map.items():
                    for idx in indices:
                        try:
                            await graph_writer.upsert_index(idx)
                            await graph_writer.link_entities(
                                source_id=ticker.upper(),
                                relation_type="MEMBER_OF",
                                target_id=str(idx).upper(),
                                properties={"weight": 1.0},
                                source_label="Company",
                                target_label="Index"
                            )
                        except Exception as ge:
                            logger.debug(f"Graph index link failed for {ticker} -> {idx}: {ge}")

    except Exception as e:
        logger.error(f"Index constituents batch fetch failed: {e}")
    finally:
        if owns_session:
            await session.close()


# How many symbols one daily pass will cover.
#
# Each symbol costs two Finnhub calls -- profile2 and peers -- spaced 250ms
# apart, so 400 symbols is about three and a half minutes of a 60-per-minute
# budget shared with the tradfi collector. Raise it and the pass takes longer;
# it does not take more per symbol.
REFERENCE_UNIVERSE_LIMIT = int(os.getenv("REFDATA_UNIVERSE_LIMIT", "400"))


async def _promote_reference_data_to_graph(graph_writer, symbol: str, ref_data) -> None:
    """Everything the graph should learn from one symbol's reference data.

    Extracted so the cache-hit path and the network path do the same thing.
    They did not: the promotion lived after the cache check, so it ran only on
    the first sighting of a symbol and every refresh afterwards was a no-op
    against the graph while reporting success.
    """
    if not graph_writer or not isinstance(ref_data, dict):
        return

    sector = ref_data.get("sector") or ""
    industry = ref_data.get("industry") or ""
    country = ref_data.get("country") or ""

    try:
        await graph_writer.upsert_equity(
            ticker=symbol,
            data={
                "sector": sector,
                "industry": industry,
                "indices": ref_data.get("index_membership", []),
            },
        )
    except Exception as exc:
        swallowed("enrichment.ref_data.upsert_equity", exc, logger, detail=symbol)

    await _link_commodity_exposure(graph_writer, symbol, sector, industry)

    # Where the company is registered.
    #
    # `country` has been fetched from /stock/profile2 and cached for every
    # symbol since this module was written, and `upsert_equity` passes sector,
    # industry and indices -- so `Company` had no geographic edge of any kind.
    # The platform watches twelve maritime chokepoints and could not answer
    # which listed companies sit on one.
    if country:
        try:
            await graph_writer.link_entities(
                source_id=symbol,
                relation_type="REGISTERED_IN",
                target_id=str(country).upper().strip(),
                properties={"source": "finnhub_profile2"},
                source_label="Company",
                target_label="Country",
            )
        except Exception as exc:
            swallowed(
                "enrichment.ref_data.company_country", exc, logger, detail=symbol,
            )


async def _link_commodity_exposure(
    graph_writer, symbol: str, sector: str, industry: str = ""
) -> int:
    """POSITIVE_EXPOSURE_TO / INVERSE_EXPOSURE_TO edges for one company.

    The sign is the reason this is worth writing. An oil producer and an
    airline are both exposed to crude, in opposite directions, and an unsigned
    edge from each to CL=F would tell a reader they respond to an oil shock the
    same way. Both predicates were already in the vocabulary and neither had
    ever been written.

    Returns the number of edges emitted, so the caller can report coverage
    rather than assume it.
    """
    if not graph_writer or not symbol:
        return 0

    from shared.utils.domain_bridges import exposures_for_sector, PRIOR_CONFIDENCE, PRIOR_SOURCE

    # Finnhub's `finnhubIndustry` is what lands in `sector` here, and its
    # vocabulary does not always match the graph's. Both fields are tried
    # rather than one, because a miss is silent and would look like a company
    # with no exposure.
    pairs = exposures_for_sector(sector) or exposures_for_sector(industry)
    if not pairs:
        return 0

    written = 0
    for commodity, predicate in pairs:
        try:
            await graph_writer.link_entities(
                source_id=symbol,
                relation_type=predicate,
                target_id=commodity,
                properties={
                    # Confidence, not weight, and well under 1.0: this is a
                    # structural prior and it must rank below anything the
                    # statistical pass actually measures against the same pair.
                    "confidence": PRIOR_CONFIDENCE,
                    "source": PRIOR_SOURCE,
                    "via_sector": sector or industry or "",
                },
                source_label="Company",
                target_label="Commodity",
            )
            written += 1
        except Exception as exc:
            swallowed("enrichment.ref_data.commodity_exposure", exc, logger, detail=symbol)
    return written


async def link_chokepoint_countries(graph_writer) -> int:
    """`(Region)-[:LOCATED_IN]->(Country)` for each watched chokepoint.

    The join that makes company geography reachable from the maritime side.
    With the company half written beside it the path is

        Vessel -> Region -> Country -> Company

    which answers "this strait is closing, which listed companies sit on it" --
    a different question from the commodity path, which answers what it prices.

    Coastal and transit states only. Japan depends on Hormuz and is not on it,
    and an edge saying otherwise would be a claim about trade flow wearing the
    clothes of geography.
    """
    if not graph_writer:
        return 0

    from shared.utils.domain_bridges import (
        CHOKEPOINT_COUNTRIES, PRIOR_CONFIDENCE, PRIOR_SOURCE,
    )

    written = 0
    for region, countries in CHOKEPOINT_COUNTRIES.items():
        for code in countries:
            try:
                await graph_writer.link_entities(
                    source_id=region,
                    relation_type="LOCATED_IN",
                    target_id=code,
                    properties={
                        "confidence": PRIOR_CONFIDENCE,
                        "source": PRIOR_SOURCE,
                    },
                    source_label="Region",
                    target_label="Country",
                )
                written += 1
            except Exception as exc:
                swallowed(
                    "enrichment.ref_data.chokepoint_country", exc, logger,
                    detail=f"{region}->{code}",
                )
    return written


async def link_chokepoint_adjacency(graph_writer) -> int:
    """ADJACENT_TO between chokepoints that sit on one corridor.

    `ADJACENT_TO` held 4 edges in the entire graph and none of them touched a
    Region, so the twelve watched chokepoints were twelve unrelated points. A
    chokepoint matters *because* it is on a route; without this the platform
    cannot say that a Bab-el-Mandeb closure and a Suez closure are the same
    disruption seen twice.
    """
    if not graph_writer:
        return 0

    from shared.utils.domain_bridges import (
        CHOKEPOINT_ADJACENCY, PRIOR_CONFIDENCE, PRIOR_SOURCE,
    )

    written = 0
    for source_region, target_region in CHOKEPOINT_ADJACENCY:
        try:
            await graph_writer.link_entities(
                source_id=source_region,
                relation_type="ADJACENT_TO",
                target_id=target_region,
                properties={
                    # Physical contiguity is not a probabilistic claim, but the
                    # confidence field is read as one by three separate queries,
                    # so it carries the same prior marker as its neighbours
                    # rather than a 1.0 that would outrank every measurement in
                    # the graph.
                    "confidence": PRIOR_CONFIDENCE,
                    "source": PRIOR_SOURCE,
                },
                source_label="Region",
                target_label="Region",
            )
            written += 1
        except Exception as exc:
            swallowed(
                "enrichment.ref_data.chokepoint_adjacency", exc, logger,
                detail=f"{source_region}~{target_region}",
            )
    return written


async def link_supply_chain(graph_writer) -> int:
    """SUPPLIER_TO edges for the chain whose geography this platform watches.

    SUPPLIER_TO, CUSTOMER_OF, SUPPLIES and PURCHASES_FROM hold zero edges
    between them. The graph-context query behind every trade recommendation
    reads `supply_chain`, and that field has been empty in every brief ever
    produced -- not because the query was wrong but because nothing had ever
    written one of these.

    Semiconductors specifically, because the Taiwan Strait is already one of the
    twelve watched chokepoints and until now a disruption there reached no
    equity by any path at all.
    """
    if not graph_writer:
        return 0

    from shared.utils.domain_bridges import (
        SUPPLY_CHAIN, PRIOR_CONFIDENCE, PRIOR_SOURCE,
    )

    written = 0
    for supplier, customer in SUPPLY_CHAIN:
        try:
            # One direction only. SUPPLIER_TO and CUSTOMER_OF are the same fact
            # read from two ends, and writing both would double-count every
            # supply relationship in any query that traverses undirected --
            # which is how COMPETES_WITH ended up being counted twice per pair
            # before the peers fetcher deduplicated it.
            await graph_writer.link_entities(
                source_id=supplier,
                relation_type="SUPPLIER_TO",
                target_id=customer,
                properties={
                    "confidence": PRIOR_CONFIDENCE,
                    "source": PRIOR_SOURCE,
                },
                source_label="Company",
                target_label="Company",
            )
            written += 1
        except Exception as exc:
            swallowed(
                "enrichment.ref_data.supply_chain", exc, logger,
                detail=f"{supplier}->{customer}",
            )
    return written


async def link_freight_exposures(graph_writer) -> int:
    """Freight indices, the equities they move, and the fuel that moves them.

    Closes the other half of the gap the chokepoint edges close. A freight
    event's primary entity is the index -- BDI, FBX_GLOBAL, HARPEX -- which is
    not a tradeable ticker, so the quant engine drops all 4,219 of them at its
    supported-asset gate. The equities the move actually reaches were named in
    the collector payload and had nowhere to go.

    With these edges the index is a node the graph can traverse *from*, so a
    freight spike reaches a shipowner without anything having to re-derive the
    relationship from a list.

    The sign is per index rather than per list. HARPEX is the clearest case:
    Danaos leases vessels out and ZIM charters them in, so one charter-rate
    spike is revenue for the first and cost for the second. A single
    "freight-sensitive" list says they respond alike.
    """
    if not graph_writer:
        return 0

    from shared.utils.domain_bridges import (
        FREIGHT_EXPOSURE, FREIGHT_INDEX_LABEL, PRIOR_CONFIDENCE, PRIOR_SOURCE,
        commodity_inputs_for_freight_index, exposures_for_freight_index,
    )

    written = 0
    for index_symbol in FREIGHT_EXPOSURE:
        for ticker, predicate in exposures_for_freight_index(index_symbol):
            try:
                await graph_writer.link_entities(
                    source_id=ticker,
                    relation_type=predicate,
                    target_id=index_symbol,
                    properties={
                        "confidence": PRIOR_CONFIDENCE,
                        "source": PRIOR_SOURCE,
                    },
                    source_label="Company",
                    target_label=FREIGHT_INDEX_LABEL,
                )
                written += 1
            except Exception as exc:
                swallowed(
                    "enrichment.ref_data.freight_exposure", exc, logger,
                    detail=f"{ticker}->{index_symbol}",
                )

        # Bunker fuel, which is why crude was in a list named for equities.
        for commodity in commodity_inputs_for_freight_index(index_symbol):
            try:
                await graph_writer.link_entities(
                    source_id=index_symbol,
                    relation_type="COMMODITY_EXPOSURE",
                    target_id=commodity,
                    properties={
                        "confidence": PRIOR_CONFIDENCE,
                        "source": PRIOR_SOURCE,
                    },
                    source_label=FREIGHT_INDEX_LABEL,
                    target_label="Commodity",
                )
                written += 1
            except Exception as exc:
                swallowed(
                    "enrichment.ref_data.freight_commodity", exc, logger,
                    detail=index_symbol,
                )
    if written:
        logger.info(
            "Linked %d freight-index edges across %d indices.",
            written, len(FREIGHT_EXPOSURE),
        )
    return written


async def link_chokepoint_commodities(graph_writer) -> int:
    """COMMODITY_EXPOSURE edges from each watched chokepoint to what transits it.

    Static geography, so this is idempotent and cheap -- thirteen regions and
    about thirty edges -- and it is re-asserted each pass rather than written
    once at startup, because a graph this platform rebuilds should not depend on
    having been up at the right moment.

    The Region nodes already exist and carry the traffic: STRAIT OF HORMUZ has
    1,959 edges today, every one of them to a vessel or an aircraft. These are
    the first edges it will have to anything priced.
    """
    if not graph_writer:
        return 0

    from shared.utils.domain_bridges import (
        CHOKEPOINT_COMMODITIES, PRIOR_CONFIDENCE, PRIOR_SOURCE,
    )

    written = 0
    for region, commodities in CHOKEPOINT_COMMODITIES.items():
        for commodity in commodities:
            try:
                await graph_writer.link_entities(
                    source_id=region,
                    relation_type="COMMODITY_EXPOSURE",
                    target_id=commodity,
                    properties={
                        "confidence": PRIOR_CONFIDENCE,
                        "source": PRIOR_SOURCE,
                    },
                    source_label="Region",
                    target_label="Commodity",
                )
                written += 1
            except Exception as exc:
                swallowed(
                    "enrichment.ref_data.chokepoint_commodity", exc, logger, detail=region,
                )
    if written:
        logger.info(
            "Linked %d chokepoint-to-commodity edges across %d watched regions.",
            written, len(CHOKEPOINT_COMMODITIES),
        )
    return written


async def _reference_universe(redis_client) -> list:
    """Which symbols are worth holding reference data for, today.

    The watchlist first, because those are the streamed names, then whatever the
    market-capitalisation backfill has resolved -- which is the platform's own
    record of the companies whose moves it has had to rank. That set grows as
    the market moves, so the graph follows the platform's attention rather than
    a list fixed at fifty.

    Symbols with no resolved market cap are not excluded here; `has_been_resolved`
    is about the movers gate. What is excluded is anything the cache has recorded
    as having no market capitalisation at all -- warrants, units and rights --
    because a competitive set for a warrant is not a thing.
    """
    from shared.utils.market_cap import MARKET_CAP_PREFIX, parse_market_cap

    out, seen = [], set()

    try:
        watched = await redis_client.raw.zrevrange(WATCHED_EQUITIES_KEY, 0, -1)
    except Exception as exc:
        swallowed("enrichment.ref_data.universe_watchlist", exc, logger)
        watched = []
    for item in watched or []:
        sym = (item.decode("utf-8") if isinstance(item, bytes) else str(item)).upper().strip()
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)

    # Then the resolved market-cap universe, largest first, so a pass that runs
    # out of budget has spent it on the companies that matter most.
    try:
        cursor, pairs = 0, []
        while True:
            cursor, keys = await redis_client.raw.scan(
                cursor=cursor, match=f"{MARKET_CAP_PREFIX}*", count=500
            )
            if keys:
                values = await redis_client.raw.mget(keys)
                for key, value in zip(keys, values):
                    usd = parse_market_cap(value)
                    if usd is None:
                        continue  # no market cap at all: not a company
                    name = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    pairs.append((usd, name.rsplit(":", 1)[-1].upper()))
            if not cursor:
                break
        for _usd, sym in sorted(pairs, reverse=True):
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
    except Exception as exc:
        swallowed("enrichment.ref_data.universe_marketcap", exc, logger)

    return out[:REFERENCE_UNIVERSE_LIMIT]


async def refresh_watchlist_reference_data(redis_client, session=None, graph_writer=None):
    """
    Batch-refresh reference data for all symbols on the active equity watchlist.
    Intended to be called once per day from a scheduled job.
    Respects Finnhub rate limits with a 250ms delay between calls.
    """
    import aiohttp

    try:
        # The static bridges first, because they depend on nothing.
        #
        # These were written below the `if not symbols: return` guard, which
        # gated the entire physical-to-financial bridge behind a non-empty
        # watchlist. On a fresh deployment, or any time the Redis universe is
        # cold, the chokepoint and freight edges would never have been written
        # at all -- and the symptom would have been precisely the defect they
        # exist to fix: a graph whose domains do not touch.
        #
        # They are also cheap and idempotent: about seventy MERGEs from tables
        # compiled into the image, with no network call between them.
        chokepoint_edges = await link_chokepoint_commodities(graph_writer)
        chokepoint_edges += await link_chokepoint_adjacency(graph_writer)
        chokepoint_edges += await link_chokepoint_countries(graph_writer)
        freight_edges = await link_freight_exposures(graph_writer)
        supply_edges = await link_supply_chain(graph_writer)

        symbols = await _reference_universe(redis_client)
        if not symbols:
            logger.info(
                "No equities to refresh reference data for; wrote %d "
                "chokepoint and %d freight-index and %d supply-chain edges.",
                chokepoint_edges, freight_edges, supply_edges,
            )
            return

        logger.info(
            "Refreshing reference data for %d symbols (watchlist plus resolved "
            "market-cap universe).", len(symbols),
        )

        owns_session = session is None
        if owns_session:
            session = aiohttp.ClientSession()

        try:
            # Fetch index constituents first (populates index_membership cache & graph)
            await fetch_index_constituents(redis_client, session, graph_writer=graph_writer)


            success_count = 0
            peer_edges = 0
            for symbol in symbols:
                result = await fetch_and_cache_reference_data(redis_client, symbol, session, graph_writer=graph_writer)
                if result:
                    success_count += 1
                # Rate limit: 250ms between Finnhub calls
                await asyncio.sleep(0.25)

                # The competitive set, in the same pass.
                #
                # Cached for a week, so on a steady universe this costs one call
                # per symbol per week rather than one per day.
                peers = await fetch_and_cache_peers(
                    redis_client, symbol, session, graph_writer=graph_writer
                )
                if peers:
                    peer_edges += len(peers)
                await asyncio.sleep(0.25)


            logger.info(
                "Reference data refresh complete: %d/%d symbols updated, "
                "%d competitor edges, %d chokepoint edges, "
                "%d freight-index edges, %d supply-chain edges.",
                success_count, len(symbols), peer_edges, chokepoint_edges,
                freight_edges, supply_edges,
            )
        finally:
            if owns_session:
                await session.close()

    except Exception as e:
        logger.error(f"Reference data batch refresh failed: {e}")
