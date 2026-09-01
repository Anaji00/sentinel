"""
shared/utils/equities.py

PRIMARY EQUITY VALIDATION & LIGHTWEIGHT ASSET CLASSIFICATION ENGINE
====================================================================
Sub-millisecond regex, structural, and blocklist classifier to distinguish 
clean primary US common equities from 2x/3x leveraged ETFs, single-stock 
leveraged funds, synthetic option yield ETFs, volatility ETNs, and derivatives.
"""

import re
from typing import Dict, Set, Tuple, Any, Optional

# ── ALLOWED CRYPTO EXCEPTION ──────────────────────────────────────────────────
ALLOWED_CRYPTO_TOKENS: Set[str] = {"BTC", "BTCUSDT", "BTCUSD"}

# Crypto assets this deployment actually collects a market surface for: OKX
# perpetual funding, mark and index price, basis and open interest, plus spot
# from Coinbase.
#
# Kept separate from ALLOWED_CRYPTO_TOKENS on purpose. That set answers "may this
# go in the *equity* watchlist", and the answer for ETH is still no. This one
# answers "does the platform hold data on this", which is a different question
# and the one an agent should be asking before it decides it has nothing to say.
# Conflating them meant the perpetual surface was collected, enriched, stored and
# then refused by every agent: QuantTradingEngine gates its whole handle path on
# the equity check while carrying a _fetch_funding_context() written for exactly
# these assets.
MAJOR_CRYPTO_ASSETS: Set[str] = {
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "MATIC",
    "LTC", "BCH", "ATOM", "UNI", "AAVE", "NEAR", "APT", "ARB", "OP", "SUI",
    "TON", "TRX", "ICP", "FIL", "INJ", "SEI", "TIA", "PEPE", "SHIB", "ENA",
    "HYPE", "ZEC", "PUMP", "TRUMP", "WIF", "BNB",
}


# Macro instruments: the other side of a cross-asset correlation. Commodities,
# rates, volatility and index futures -- what "oil down 5%, tech up 5%" is about.
#
# StockCorrelationAgent classified these with a substring list containing
# "BRENT", "WTI", "GLD", "VIX" and "US10Y". Live quote keys are ticker symbols,
# so BZ=F (Brent), GC=F (gold), SI=F (silver), NG=F (natural gas), ZC=F (corn),
# ZW=F (wheat), ES=F and NQ=F (index futures), VXX and TIP all failed to match
# and were classified as *equities* -- the very instruments the agent exists to
# correlate equities against ended up on the equity side of the comparison.
MACRO_SYMBOLS: Set[str] = {
    # Energy
    "CL=F", "BZ=F", "NG=F", "RB=F", "HO=F", "USO", "UNG", "BNO", "XLE",
    # Metals
    "GC=F", "SI=F", "HG=F", "PL=F", "PA=F", "GLD", "SLV", "IAU", "GDX", "CPER",
    # Agriculture
    "ZC=F", "ZW=F", "ZS=F", "KC=F", "SB=F", "CC=F", "CT=F", "LE=F", "DBA",
    # Rates and credit
    "ZN=F", "ZB=F", "ZF=F", "ZT=F", "TLT", "IEF", "SHY", "TIP", "LQD", "HYG",
    "AGG", "BND", "US10Y", "US02Y", "US2Y", "US30Y", "US05Y",
    # Volatility
    "VIX", "VXX", "UVXY", "VIXY", "SVXY", "VX=F",
    # Index and FX futures
    "ES=F", "NQ=F", "YM=F", "RTY=F", "DX=F", "6E=F", "6J=F", "6B=F", "DXY",
}

# Yahoo-style suffixes and prefixes that mark an instrument as macro regardless
# of whether the symbol itself is listed above: "=F" is a futures contract,
# "^" an index, "=X" an FX pair.
_MACRO_SUFFIXES = ("=F", "=X")
_MACRO_PREFIXES = ("^",)


def is_macro_asset(ticker: str) -> bool:
    """True for a commodity, rate, volatility, FX or index instrument."""
    if not ticker or not isinstance(ticker, str):
        return False
    sym = ticker.strip().upper()
    if not sym:
        return False
    if sym in MACRO_SYMBOLS:
        return True
    if sym.endswith(_MACRO_SUFFIXES) or sym.startswith(_MACRO_PREFIXES):
        return True
    # Treasury tenors arrive in several spellings across collectors.
    return bool(re.match(r"^US\d{1,2}Y$", sym))


def split_macro_and_equities(symbols) -> tuple:
    """Partition a symbol list into (macro instruments, equities)."""
    macro, equities = [], []
    for sym in symbols:
        (macro if is_macro_asset(sym) else equities).append(sym)
    return macro, equities


def is_major_crypto(ticker: str) -> bool:
    """True for a crypto asset this deployment collects market data on."""
    if not ticker or not isinstance(ticker, str):
        return False
    sym = ticker.strip().upper()
    # Instrument ids arrive as BTC-USDT-SWAP or BTCUSDT depending on the venue.
    for suffix in ("-USDT-SWAP", "-USD-SWAP", "-USDT", "-USD", "USDT", "USD"):
        if sym.endswith(suffix) and len(sym) > len(suffix):
            sym = sym[: -len(suffix)]
            break
    return sym in MAJOR_CRYPTO_ASSETS


def is_supported_asset(ticker: str) -> bool:
    """True when the platform holds data on this symbol at all.

    The union of primary US common equity and the crypto majors above. Use this
    to decide whether an agent has anything to reason about; use
    is_valid_primary_equity() when the question is specifically about equities,
    such as what may enter the equity watchlist.
    """
    return is_valid_primary_equity(ticker) or is_major_crypto(ticker)

# ── COMPILED REGEX PATTERNS ──────────────────────────────────────────────────

# OCC Options Contract Pattern (e.g. AAPL240816C00220000)
RE_OCC_OPTION = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d+$", re.IGNORECASE)

# Ticker structural noise: slashes, dots, dashes, digits, or non-alphabetics (e.g. BRK.A, TSLA/WS, AAPL-W)
RE_STRUCTURAL_DERIVATIVE = re.compile(r"[\.\/\-\=\+\~\d]", re.IGNORECASE)

# Class-share tickers: BRK.B, BF.B, CWEN.A, HEI.A. Ordinary common equity that
# happens to carry a share class, and among the largest companies listed. The
# structural-punctuation rule above rejects them for the dot, so they have to be
# recognised before it runs -- and PRIMARY_EQUITY_EXCEPTIONS cannot rescue them,
# because that check sits *after* the punctuation rule in the classifier.
#
# It has to clear the length gate as well. A four-letter root plus ".A" is six
# characters, so CWEN.A and LGF.A were rejected as INVALID_LENGTH two branches
# before this pattern was ever consulted -- which made the {1,4} in it dead for
# every root longer than three. The check therefore runs ahead of the length
# rule, not merely ahead of the punctuation one.
RE_CLASS_SHARE = re.compile(r"^[A-Z]{1,4}\.[A-Z]$")

# Non-leveraged index, sector and commodity funds. Distinct from
# ALL_DERIVATIVE_ETFS, which lists leveraged and inverse products only -- so the
# most heavily traded funds on the market (SPY, QQQ, GLD) matched no rule and
# fell through to "clean primary US common equity". That put them in the equity
# watchlist for agents to reason about as though they were companies, and it
# contradicted the async validator, which returns False for anything Finnhub
# types as ETF/ETP. Same ticker, opposite answers, depending on which function
# the caller happened to use.
BROAD_MARKET_ETFS: Set[str] = {
    # Broad market / index
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV", "VEA", "VWO", "EFA",
    "EEM", "VXUS", "ACWI", "RSP", "MDY", "SCHD", "VIG", "VYM", "IWF", "IWD",
    # Sector
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE",
    "XLC", "SMH", "XBI", "IBB", "XOP", "XME", "XRT", "KRE", "ITB", "JETS",
    # Commodity / rates / credit
    "GLD", "SLV", "USO", "UNG", "TLT", "IEF", "SHY", "LQD", "HYG", "AGG",
    "BND", "TIP", "GDX", "GDXJ", "SLX", "DBC", "PDBC", "IAU",
    # Popular active / thematic
    "ARKK", "ARKG", "ARKW", "ARKQ", "ARKF", "ICLN", "TAN", "LIT", "BOTZ",
    "ROBO", "HACK", "SKYY", "FDN", "IGV", "VNQ", "REET", "EWJ", "FXI", "INDA",
}

# Warrant, Right, Unit, Preferred Share, and Class suffixes (e.g. NVDAWS, AAPLRT, TSLAPR)
RE_DERIVATIVE_SUFFIX = re.compile(r"(WS|WT|RT|R|PR|P|UN|U|CL|CV)$", re.IGNORECASE)

# Crypto Symbols, Tokens, and Trading Pairs (Absolute Exclusion except BTC)
RE_CRYPTO = re.compile(
    r"^(ETH|SOL|XRP|ADA|DOGE|DOT|BCH|LINK|LTC|AVAX|MATIC|SHIB|UNI|ATOM|XLM|ETC|FIL|NEAR|APT|PEPE|WIF|BONK|FLOKI|INJ|TIA|SUI|SEI|RENDER|FET|AGIX|OP|ARB).*"
    r"|.*(USDT|USDC|BUSD|PERP)$",
    re.IGNORECASE
)

# Single-Stock Leveraged (2x/3x Bull/Bear), Short, & Synthetic Yield Suffixes
# Matches tickers ending in U, D, L, S, X, Z, Y, W on 4 or 5-letter symbols (e.g. IONZ, IONU, NVDL, NVDZ, TSLZ, MSTY)
RE_SINGLE_STOCK_LEVERAGED_SUFFIX = re.compile(
    r"^[A-Z]{3,4}[UDLSXYZW]$",
    re.IGNORECASE
)

# Active Single-Stock Ticker Roots for Single-Stock Leveraged/Yield ETFs (e.g. ION, NVD, TSL, AAP, MSF, AMZ, GOO, MET, NFL, CON, BIT, ETH, DIS, JPM, XOM, AMD, PYP, SMR, BAB, ARM, PLT, SMC, MST, HOD, MAR, SOF)
RE_LEVERAGED_ROOT_PATTERN = re.compile(
    r"^(ION|NVD|TSL|AAP|MSF|AMZ|GOO|MET|NFL|CON|BIT|ETH|DIS|JPM|XOM|AMD|PYP|SMR|BAB|ARM|PLT|SMC|MST|HOD|MAR|SOF|PLT)[UDLSXYZW]$",
    re.IGNORECASE
)

# ── COMPREHENSIVE DERIVATIVE, LEVERAGED & SYNTHETIC ETF BLOCKLIST ────────────────

ALL_DERIVATIVE_ETFS: Set[str] = {
    # Single-Stock Leveraged (IONZ, IONU, IOND, IONL, etc.)
    "IONZ", "IONU", "IOND", "IONL", "IONS", "IONX",
    
    # 1. YieldMax Single-Stock Option Income ETFs
    "NVDY", "CONY", "TSLY", "AMZY", "APLY", "MSFO", "GOOY", "FBY", "NFLY", 
    "OARK", "SMRY", "AMDY", "PYPY", "AIYY", "YMAX", "YMAG", "ULTY", "FIAT",
    "MSTY", "MRNY", "DISO", "XOMO", "JPMO", "ABNY", "BITO", "BITX", "ETHU",
    "SQY", "BAXY", "SLVY", "GDXY", "AIY", "CRSH", "DUMP", "SVO",

    # 2. Roundhill 0DTE & Option Income Derivatives
    "XDTE", "QDTE", "RDTE", "WEEK", "NVDW", "MAGS", "CHAT", "METV", "BIGT", 
    "KNGS", "MEME", "CHPY", "XPAY", "DEEP",

    # 3. Defiance 0DTE & Enhanced Options Income Derivatives
    "JEPY", "QQQT", "IWMY", "SPYT", "WDTE", "USOY", "HOOD", "SMCL", "TLTW", "LQDW", "HYGW",

    # 4. T-REX / REX Shares Single-Stock Leveraged (2x) & Inverse Derivatives
    "NVDU", "NVDD", "TSLT", "TSLZ", "AAPU", "AAPD", "GOOX", "GOOZ", 
    "MSFU", "MSFZ", "AMZU", "AMZD", "NVDX", "TSLX", "BKCH",

    # 5. GraniteShares Single-Stock Leveraged (2x/3x) & Short Derivatives
    "NVDL", "TSLR", "AAPB", "AMZL", "METU", "GOOL", "PLTE", "AMDL", "CONL", "NVDS",

    # 6. Kurv Yield Premium Single-Stock Derivatives
    "KAPL", "KGOOG", "KMSFT", "KAMZN", "KNVDA", "KTSLA", "KNFLX",

    # 7. Direxion, ProShares & Innovator 2x/3x Leveraged Bull & Bear ETFs
    "TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU", "SPXL", "SPXS", 
    "FNGU", "FNGD", "BULZ", "BERZ", "LABU", "LABD", "TECL", "TECS", 
    "FAS", "FAZ", "TNA", "TZA", "BOIL", "KOLD", "NUGT", "DUST", 
    "JNUG", "JDST", "ERX", "ERY", "DPST", "DRV", "WEBL", "WEBS", 
    "RETL", "WANT", "HIBS", "HIBL", "YINN", "YANG", "INDL", "CWEB", 
    "CHAU", "EDC", "EDZ", "MEXX", "EURL", "MIDU", "URTY", "SRTY", 
    "TARK", "SARK", "TSLL", "TSLS", "GGLL", "GGLS", "METD",

    # 8. Volatility ETNs, Short Volatility & Leveraged Commodity/Bond Funds
    "UVXY", "SVIX", "UVIX", "VIXY", "VXX", "SVXY", "XIV", "ZSL", 
    "AGQ", "UGL", "GLL", "UCO", "SCO", "UNL", "USL", "DIG", "DUG", 
    "USD", "SSG", "UYM", "SMN", "UYG", "SKF", "RXD", "RXL", "REK", 
    "URE", "SRS", "SH", "PSQ", "DOG", "RWM", "MYY", "MZZ", "SDK", 
    "SDD", "SZK", "SIJ", "SBM", "SBB", "EFU", "EFZ", "EEV", "EPV", 
    "FXP", "BZQ", "EUM", "SJNK", "HIGH", "TBT", "TMV", "TMF"
}

# Known Primary Common Equities ending in U/D/L/S/Z/Y that must be preserved
PRIMARY_EQUITY_EXCEPTIONS: Set[str] = {
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "GOOGL", "META", "TSLA", "INTC", 
    "AMD", "BABA", "NFLX", "PLTR", "DIS", "JPM", "XOM", "ARM", "DELL", "AVGO", 
    "SOFI", "HOOD", "MARA", "COIN", "MSTR", "SMCI", "IONQ", "DE", "CAT", "BA",
    "UNH", "LLY", "V", "MA", "PG", "HD", "JNJ", "ABBV", "BAC", "CVX", "COST",
    "WMT", "MRK", "TMO", "PEP", "AVGO", "CSCO", "ORCL", "ACN", "MCD", "LIN"
}


def fast_classify_equity(ticker: str, cached_asset_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Sub-millisecond lightweight classifier evaluating asset classification.
    Checks cached_asset_type (from Redis daily refdata) first, falling back to
    regex and blocklist heuristics.
    """
    if not ticker or not isinstance(ticker, str):
        return {
            "ticker": str(ticker),
            "is_primary_equity": False,
            "asset_class": "INVALID",
            "reason": "Null or non-string ticker"
        }

    sym = ticker.strip().upper()

    if cached_asset_type:
        cat_str = str(cached_asset_type).strip().upper()
        if cat_str in ("ETP", "ETF"):
            return {
                "ticker": sym,
                "is_primary_equity": False,
                "asset_class": "LEVERAGED_INVERSE_ETF",
                "reason": f"Redis cached asset type: {cached_asset_type}"
            }
        elif cat_str in ("COMMON STOCK", "ADR"):
            return {
                "ticker": sym,
                "is_primary_equity": True,
                "asset_class": "PRIMARY_COMMON_EQUITY",
                "reason": f"Redis cached asset type: {cached_asset_type}"
            }

    if sym in ALLOWED_CRYPTO_TOKENS:
        return {
            "ticker": sym,
            "is_primary_equity": True,
            "asset_class": "CRYPTO_TOKEN",
            "reason": "Explicit allowed crypto token exception (BTC)"
        }

    if RE_CLASS_SHARE.match(sym):
        return {
            "ticker": sym,
            "is_primary_equity": True,
            "asset_class": "PRIMARY_COMMON_EQUITY",
            "reason": "Class share of a primary common equity",
        }

    if not (1 <= len(sym) <= 5):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "INVALID_LENGTH",
            "reason": f"Length {len(sym)} out of bounds [1, 5]"
        }

    if RE_CRYPTO.match(sym):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "CRYPTO_TOKEN",
            "reason": "Crypto token match"
        }

    if RE_OCC_OPTION.match(sym):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "DERIVATIVE_OPTION",
            "reason": "OCC options symbol format"
        }

    if RE_STRUCTURAL_DERIVATIVE.search(sym):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "PREFERRED_RIGHT_WARRANT",
            "reason": "Non-alphabetic structural punctuation"
        }

    if sym in BROAD_MARKET_ETFS:
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "INDEX_SECTOR_ETF",
            "reason": "Index, sector or commodity fund, not a company",
        }

    if sym in ALL_DERIVATIVE_ETFS:
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "LEVERAGED_INVERSE_ETF",
            "reason": "Explicit leveraged/synthetic derivative ETF blocklist match"
        }

    if sym in PRIMARY_EQUITY_EXCEPTIONS:
        return {
            "ticker": sym,
            "is_primary_equity": True,
            "asset_class": "PRIMARY_COMMON_EQUITY",
            "reason": "Verified primary common equity exception"
        }

    if RE_LEVERAGED_ROOT_PATTERN.match(sym):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "LEVERAGED_INVERSE_ETF",
            "reason": f"Single-stock leveraged/yield root match ({sym[:-1]} + {sym[-1]})"
        }

    if len(sym) > 4 and RE_DERIVATIVE_SUFFIX.search(sym):
        return {
            "ticker": sym,
            "is_primary_equity": False,
            "asset_class": "PREFERRED_RIGHT_WARRANT",
            "reason": "5-letter warrant/right/preferred suffix"
        }

    return {
        "ticker": sym,
        "is_primary_equity": True,
        "asset_class": "PRIMARY_COMMON_EQUITY",
        "reason": "Clean primary US common equity"
    }


# Words that are shaped exactly like tickers and are not tickers.
#
# fast_classify_equity() answers a structural question -- "could this be a
# primary US common equity?" -- and 1-5 uppercase letters always could. It
# returns True for THE, TO, IN, OF, AND, and for ZZZZZ. The news enricher used
# it to decide what counts as a named entity, so 48 hours of headlines produced
# THE (1,098), TO (891), IN (813), OF (806), A (742), AND (738) as the most
# frequently "named entities" in the platform.
#
# The damage was not confined to a noisy column. Those tokens land in both
# `named_entities` and `tags`, which the scenario tracker matches against with
# array containment -- and the anomaly scorer computes
# `entity_boost = len(named_entities) * per_entity`, so a headline scored higher
# for containing more ordinary English.
#
# The real fix is membership in the live universe, which
# is_valid_primary_equity_async() already reads from Redis. It is empty,
# because refresh_watchlist_reference_data() has never been called. Until that
# populates, this list is the floor: a token has to survive both.
#
# Deliberately only the closed-class words -- articles, prepositions,
# conjunctions, auxiliaries, pronouns. Nothing here is a plausible issuer, and
# real one-to-three letter tickers (F, GM, T, KO, V, MU) are untouched.
EQUITY_UNIVERSE_KEY = "sentinel:equities:valid_set"

TICKER_SHAPED_STOPWORDS = frozenset({
    "A", "AN", "THE", "AND", "OR", "BUT", "NOR", "SO", "YET",
    "OF", "TO", "IN", "ON", "AT", "BY", "FOR", "FROM", "WITH", "INTO",
    "OVER", "UNDER", "UP", "OUT", "OFF", "AS", "THAN", "THEN",
    "IS", "ARE", "WAS", "WERE", "BE", "BEEN", "AM", "HAS", "HAVE", "HAD",
    "DO", "DOES", "DID", "WILL", "WOULD", "CAN", "COULD", "MAY", "MUST",
    "IT", "ITS", "HE", "SHE", "HIS", "HER", "THEY", "THEM", "WE", "US",
    "YOU", "I", "ME", "MY", "OUR", "WHO", "WHOM", "WHAT", "WHEN", "WHERE",
    "WHY", "HOW", "ALL", "ANY", "BOTH", "EACH", "MORE", "MOST", "SOME",
    "SUCH", "NO", "NOT", "ONLY", "OWN", "SAME", "TOO", "VERY", "JUST",
    "NEW", "NOW", "ONE", "TWO", "SAID", "SAYS", "ALSO", "AFTER", "BEFORE",
    "THIS", "THAT", "THESE", "THOSE", "THERE", "HERE", "IF", "ELSE",
})


def looks_like_a_ticker(token: str) -> bool:
    """Whether a bare word from prose may be treated as a ticker symbol.

    Structure alone is not enough -- see TICKER_SHAPED_STOPWORDS above.
    """
    clean = (token or "").strip().upper()
    if not clean or clean in TICKER_SHAPED_STOPWORDS:
        return False
    return is_valid_primary_equity(clean)


async def confirm_tickers(candidates, redis_client) -> list:
    """The candidates that are actually listed, in one round trip.

    Two stages, because they answer different questions at different costs.

    `looks_like_a_ticker` is a denylist and runs in-process: it rejects the
    closed-class English words that are shaped like symbols, which is most of
    the noise and costs nothing. It cannot reject ZZZZZ or QQQZ, because
    nothing about their shape is wrong.

    Membership in sentinel:equities:valid_set is the allowlist -- 11,821 live US
    symbols maintained by the radar collector -- and it rejects anything that is
    not actually listed. That is the correct test, and it is the reason this is
    async at all.

    Prefilter first, then one SMISMEMBER for the survivors. A bare-word scan
    considers 60-80 tokens per headline; awaiting each one would be 60-80 round
    trips on a path handling ~400 events a minute, to answer a question the
    denylist already settles for all but two or three of them.

    Degrades to the prefilter if the set is missing, rather than returning
    nothing: an empty universe means reference data has not loaded yet, not that
    the world contains no equities.
    """
    seen, prefiltered = set(), []
    for token in candidates or []:
        clean = str(token or "").strip().upper()
        if clean and clean not in seen and looks_like_a_ticker(clean):
            seen.add(clean)
            prefiltered.append(clean)

    if not prefiltered or redis_client is None:
        return prefiltered

    raw = getattr(redis_client, "raw", redis_client)
    try:
        if not await raw.exists(EQUITY_UNIVERSE_KEY):
            return prefiltered
        flags = await raw.smismember(EQUITY_UNIVERSE_KEY, prefiltered)
    except Exception:
        return prefiltered

    if not isinstance(flags, (list, tuple)) or len(flags) != len(prefiltered):
        return prefiltered
    return [sym for sym, listed in zip(prefiltered, flags) if listed]


def is_valid_primary_equity(ticker: str) -> bool:
    """
    Returns True ONLY if ticker is a clean primary US common equity (or BTC as sole crypto exception).
    Enforces sub-millisecond classification excluding all leveraged ETFs, single-stock funds, and derivatives.
    """
    res = fast_classify_equity(ticker)
    return res["is_primary_equity"]


async def is_valid_primary_equity_async(ticker: str, redis_client=None) -> bool:
    """
    Async validation against structural filters AND Redis dynamic US equities universe set.
    Uses cache-first Finnhub asset type classification when available, falling back to
    regex/blocklist heuristics for cache misses.
    """
    sym = ticker.strip().upper()
    if sym in ALLOWED_CRYPTO_TOKENS or sym in PRIMARY_EQUITY_EXCEPTIONS:
        return True

    # Cache-first: check Finnhub-sourced asset type from daily ref data refresh
    if redis_client and hasattr(redis_client, "raw"):
        try:
            cached_type = await redis_client.raw.get(f"sentinel:asset_type:{sym}")
            if cached_type:
                asset_type = cached_type.decode("utf-8") if isinstance(cached_type, bytes) else str(cached_type)
                # Finnhub types: "Common Stock", "ETP", "ADR", "ETF", etc.
                if asset_type in ("ETP", "ETF"):
                    return False
                if asset_type == "Common Stock":
                    return True
        except Exception:
            pass

    # Fallback: regex + blocklist classification
    if not is_valid_primary_equity(ticker):
        return False

    if redis_client and hasattr(redis_client, "raw"):
        try:
            exists = await redis_client.raw.exists("sentinel:equities:valid_set")
            if exists:
                is_valid = await redis_client.raw.sismember("sentinel:equities:valid_set", sym)
                return bool(is_valid)
        except Exception:
            pass

    return True


RE_OCC_OPTION_DETAILED = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$", re.IGNORECASE)


def parse_occ_option_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Parses an OCC option contract symbol (e.g. 'AAPL240816C00220000').
    Returns dict with ticker, expiry (YYYY-MM-DD), option_type ('CALL'|'PUT'), and strike (float),
    or None if invalid OCC symbol format.
    """
    if not symbol:
        return None
    m = RE_OCC_OPTION_DETAILED.match(symbol.strip().upper())
    if not m:
        return None
    ticker, date_str, type_char, strike_str = m.groups()
    expiry = f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
    option_type = "CALL" if type_char.upper() == "C" else "PUT"
    strike = float(strike_str) / 1000.0
    return {
        "ticker": ticker,
        "expiry": expiry,
        "option_type": option_type,
        "strike": strike,
    }
