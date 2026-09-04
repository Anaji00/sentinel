"""The rates regime needs a curve, and nothing ever wrote one.

The macro engine reads sentinel:quotes:latest:US2Y and :US10Y and refuses to
publish a rates regime without both legs. Nothing in the codebase had ever
written either key, so the refusal was permanent -- correct behaviour on a
missing input, and a capability that could never come back. Its predecessor
was worse: hardcoded 4.25% and 4.15% yields, published hourly as measurement,
producing a -10bp inverted curve that was arithmetic on two typed constants.

The audit's standing note said this needed a data source rather than a code
change, and named FRED, which requires an API key nobody had issued. The
Treasury publishes the same constant-maturity series itself as CSV, with no key.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-macro"))

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "macro_collector_main", ROOT / "services" / "collector-macro" / "main.py"
)

# A verbatim excerpt of the published file, newest row first as it arrives.
LIVE_CSV = (
    'Date,"1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr","3 Yr",'
    '"5 Yr","7 Yr","10 Yr","20 Yr","30 Yr"\n'
    "09/01/2026,3.85,3.88,3.89,3.92,3.97,4.00,4.18,4.39,4.46,4.55,4.66,4.79,5.27,5.27\n"
    "08/31/2026,3.85,3.86,3.88,3.91,3.96,3.99,4.16,4.34,4.40,4.49,4.62,4.75,5.24,5.25\n"
)


def _mod():
    import sys as _sys
    if "macro_collector_main" in _sys.modules:
        return _sys.modules["macro_collector_main"]
    mod = importlib.util.module_from_spec(_spec)
    _sys.modules["macro_collector_main"] = mod
    _spec.loader.exec_module(mod)
    return mod


def test_the_latest_row_is_parsed():
    m = _mod()
    out = m._parse_treasury_csv(LIVE_CSV, m.TREASURY_COLUMNS)
    assert out["US2Y"] == 4.39
    assert out["US10Y"] == 4.79
    assert out["US30Y"] == 5.27
    assert out["__date__"] == "2026-09-01"


def test_the_newest_row_wins_regardless_of_file_order():
    """Published newest-first today. Relying on that would mean a change in the
    Treasury's ordering silently starts serving January's curve."""
    m = _mod()
    lines = LIVE_CSV.strip().split("\n")
    reversed_csv = "\n".join([lines[0]] + list(reversed(lines[1:]))) + "\n"
    out = m._parse_treasury_csv(reversed_csv, m.TREASURY_COLUMNS)
    assert out["__date__"] == "2026-09-01"
    assert out["US2Y"] == 4.39


def test_a_price_posted_to_a_yield_column_is_rejected():
    """The TIP ETF trades near 107 and was once written to a yield key, which
    produced a fabricated 230bp breakeven."""
    m = _mod()
    bad = 'Date,"2 Yr","10 Yr"\n09/01/2026,106.82,4.79\n'
    out = m._parse_treasury_csv(bad, {"US2Y": "2 Yr", "US10Y": "10 Yr"})
    assert "US2Y" not in out, "a price passed the yield range check"
    assert out["US10Y"] == 4.79


def test_a_negative_yield_is_accepted():
    """They have existed, and a range that excludes them would discard real data."""
    m = _mod()
    out = m._parse_treasury_csv('Date,"2 Yr"\n09/01/2026,-0.35\n', {"US2Y": "2 Yr"})
    assert out["US2Y"] == -0.35


def test_blank_and_malformed_cells_are_skipped_not_defaulted():
    m = _mod()
    out = m._parse_treasury_csv('Date,"2 Yr","10 Yr"\n09/01/2026,,N/A\n',
                                {"US2Y": "2 Yr", "US10Y": "10 Yr"})
    assert "US2Y" not in out and "US10Y" not in out


def test_an_empty_document_yields_nothing_rather_than_raising():
    m = _mod()
    assert m._parse_treasury_csv("", m.TREASURY_COLUMNS) == {}
    assert m._parse_treasury_csv("Date,\"2 Yr\"\n", m.TREASURY_COLUMNS) == {}


def test_the_keys_written_are_the_ones_the_macro_engine_reads():
    """The whole point of the exercise, and easy to get subtly wrong."""
    engine = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
    for key in ("US2Y", "US10Y"):
        assert f'"sentinel:quotes:latest:{key}"' in engine
    collector = (ROOT / "services/collector-macro/main.py").read_text(encoding="utf-8")
    assert 'f"sentinel:quotes:latest:{key}"' in collector
    assert '"US2Y":  "2 Yr"' in collector and '"US10Y": "10 Yr"' in collector


# ── The real curve ────────────────────────────────────────────────────────────
#
# TIPS_YIELD is the ten-year point of the inflation-indexed curve, and the macro
# engine subtracts it from the nominal ten-year to get the inflation breakeven.
# That breakeven was once fabricated at 230bp by treating the TIP ETF's price as
# a yield; the measured figure today is 4.79 - 2.45, or 234bp.

REAL_CSV = (
    'Date,"5 YR","7 YR","10 YR","20 YR","30 YR"\n'
    "09/02/2026,2.19,2.31,2.45,2.78,2.98\n"
    "09/01/2026,2.18,2.30,2.44,2.78,2.98\n"
)


def test_the_real_curve_column_names_differ_from_the_nominal_ones():
    """"10 Yr" on the nominal curve, "10 YR" on the real one. Sharing one map
    would have matched a single series and returned nothing for the other,
    silently -- which is the failure this collector exists to end."""
    m = _mod()
    assert m.TREASURY_COLUMNS["US10Y"] == "10 Yr"
    assert m.TREASURY_REAL_COLUMNS["TIPS_YIELD"] == "10 YR"
    assert m.TREASURY_COLUMNS["US10Y"] != m.TREASURY_REAL_COLUMNS["TIPS_YIELD"]


def test_the_ten_year_real_yield_is_parsed():
    m = _mod()
    out = m._parse_treasury_csv(REAL_CSV, m.TREASURY_REAL_COLUMNS)
    assert out["TIPS_YIELD"] == 2.45
    assert out["__date__"] == "2026-09-02"


def test_the_nominal_map_finds_nothing_in_the_real_csv():
    """Demonstrates the capitalisation trap rather than asserting it."""
    m = _mod()
    out = m._parse_treasury_csv(REAL_CSV, m.TREASURY_COLUMNS)
    assert "US10Y" not in out


def test_a_real_yield_stays_inside_the_plausible_range():
    """The macro engine rejects TIPS_YIELD outside -5 to 15 on read; the write
    side applies the same bound, and a real yield of 2.45 must survive it."""
    m = _mod()
    lo, hi = m.YIELD_PLAUSIBLE_RANGE
    assert lo < 2.45 < hi
    assert not (lo < 106.82 < hi), "the TIP ETF price would pass the range check"


def test_the_key_written_is_the_one_the_engine_reads():
    engine = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
    assert '"sentinel:quotes:latest:TIPS_YIELD"' in engine


def test_the_credit_etfs_the_engine_reads_are_tracked():
    """HYG and LQD are read for the credit spread and were tracked by nothing:
    the quote cache is filled from the tradfi collector's equity watchlist, so a
    macro instrument only landed there if it was already a watched equity."""
    m = _mod()
    assert "HYG" in m.MACRO_TICKERS
    assert "LQD" in m.MACRO_TICKERS
    engine = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
    for key in ("HYG", "LQD"):
        assert f'"sentinel:quotes:latest:{key}"' in engine


def test_the_macro_collector_writes_the_quote_cache():
    """It publishes to Kafka; it had never written the keys the engine reads."""
    collector = (ROOT / "services/collector-macro/main.py").read_text(encoding="utf-8")
    executable = "\n".join(
        line for line in collector.splitlines() if not line.strip().startswith("#")
    )
    # The key is built by the one shared helper now: six writers each had
    # their own literal and their own TTL, and the whole cache drained an
    # hour after the closing bell as a result.
    assert "quote_key(ticker)" in executable
    assert "QUOTE_CACHE_TTL_SEC" in executable
