"""
shared/utils/equities.py

PRIMARY EQUITY VALIDATION & LIGHTWEIGHT ASSET CLASSIFICATION ENGINE
====================================================================
Sub-millisecond regex, structural, and blocklist classifier to distinguish 
clean primary US common equities from 2x/3x leveraged ETFs, single-stock 
leveraged funds, synthetic option yield ETFs, volatility ETNs, and derivatives.
"""

import re
from typing import Dict, Set, Tuple, Any

# ── ALLOWED CRYPTO EXCEPTION ──────────────────────────────────────────────────
ALLOWED_CRYPTO_TOKENS: Set[str] = {"BTC", "BTCUSDT", "BTCUSD"}

# ── COMPILED REGEX PATTERNS ──────────────────────────────────────────────────

# OCC Options Contract Pattern (e.g. AAPL240816C00220000)
RE_OCC_OPTION = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d+$", re.IGNORECASE)

# Ticker structural noise: slashes, dots, dashes, digits, or non-alphabetics (e.g. BRK.A, TSLA/WS, AAPL-W)
RE_STRUCTURAL_DERIVATIVE = re.compile(r"[\.\/\-\=\+\~\d]", re.IGNORECASE)

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


def fast_classify_equity(ticker: str) -> Dict[str, Any]:
    """
    Sub-millisecond lightweight classifier evaluating asset classification:
    Returns dict:
      {
        "ticker": "IONZ",
        "is_primary_equity": False,
        "asset_class": "LEVERAGED_INVERSE_ETF",
        "reason": "Single-stock leveraged ETF pattern (ION + Z)"
      }
    """
    if not ticker or not isinstance(ticker, str):
        return {
            "ticker": str(ticker),
            "is_primary_equity": False,
            "asset_class": "INVALID",
            "reason": "Null or non-string ticker"
        }

    sym = ticker.strip().upper()

    if sym in ALLOWED_CRYPTO_TOKENS:
        return {
            "ticker": sym,
            "is_primary_equity": True,
            "asset_class": "CRYPTO_TOKEN",
            "reason": "Explicit allowed crypto token exception (BTC)"
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


def is_valid_primary_equity(ticker: str) -> bool:
    """
    Returns True ONLY if ticker is a clean primary US common equity (or BTC as sole crypto exception).
    Enforces sub-millisecond classification excluding all leveraged ETFs, single-stock funds, and derivatives.
    """
    res = fast_classify_equity(ticker)
    return res["is_primary_equity"]
