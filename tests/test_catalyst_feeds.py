"""
tests/test_catalyst_feeds.py

FINRA short volume and FDA approvals, as equity catalysts.

Both free, both absent until now. Fixtures follow the formats the endpoints
returned on 3 October 2026.
"""

import asyncio
import importlib.util
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "catalysts_for_test", str(ROOT / "services" / "collector-filings" / "catalysts.py"))
cat = importlib.util.module_from_spec(spec)
sys.modules["catalysts_for_test"] = cat
spec.loader.exec_module(cat)

FINRA = """Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20261002|A|267855.710445|428|711868.938077|B,Q,N
20261002|AA|638122.239020|1797|1193988.524274|B,Q,N
20261002|ZZZ|0|0|0|Q
12465"""


def test_finra_parses_and_skips_the_trailer_and_empty_rows():
    rows = cat.parse_short_volume(FINRA)
    assert set(rows) == {"A", "AA"}
    assert rows["AA"] == (638122.239020, 1193988.524274)


def test_a_spike_needs_history_a_floor_and_a_jump():
    hist = [0.40] * 12
    assert cat.short_spike(0.60, hist)
    assert not cat.short_spike(0.50, hist)          # below the floor
    assert not cat.short_spike(0.58, [0.50] * 12)   # not far enough above norm
    assert not cat.short_spike(0.90, [0.40] * 5)    # too little history


def test_a_spike_event_names_the_ticker_and_its_norm():
    e = cat.short_spike_event("AA", "20261002", 0.62, [0.40] * 12, 1_193_988)
    assert e.source == "finra_short_volume" and "exposes:AA" in e.tags
    assert "62%" in e.headline and "40%" in e.headline
    assert 0.55 < e.anomaly_score <= 0.95


def _resolver(table):
    return lambda name: table.get(name)


def test_fda_sponsors_resolve_through_house_names_and_abbreviations():
    r = _resolver({"VERTEX PHARMACEUTICALS": "VRTX", "NUVATION BIO": "NUVB"})
    assert cat.sponsor_ticker("ELI LILLY AND CO", r) == "LLY"
    assert cat.sponsor_ticker("MERCK SHARP DOHME", r) == "MRK"
    assert cat.sponsor_ticker("JANSSEN BIOTECH", r) == "JNJ"
    assert cat.sponsor_ticker("VERTEX PHARMS INC", r) == "VRTX"
    assert cat.sponsor_ticker("NUVATION BIO INC", r) == "NUVB"
    assert cat.sponsor_ticker("LUPIN", r) is None


def test_only_original_nda_bla_approvals_in_the_window_count():
    results = [
        {"application_number": "NDA219713", "sponsor_name": "NUVATION",
         "products": [{"brand_name": "IBTROZI"}],
         "submissions": [{"submission_type": "ORIG", "submission_status": "AP",
                          "submission_status_date": "20260930", "submission_class_code": "TYPE 1"},
                         {"submission_type": "SUPPL", "submission_status": "AP",
                          "submission_status_date": "20261001"}]},
        {"application_number": "ANDA220657", "sponsor_name": "DIFGEN",
         "submissions": [{"submission_type": "ORIG", "submission_status": "AP",
                          "submission_status_date": "20260930"}]},
        {"application_number": "NDA220446", "sponsor_name": "LUPIN",
         "submissions": [{"submission_type": "ORIG", "submission_status": "TA",
                          "submission_status_date": "20260925"}]},
        {"application_number": "NDA215341", "sponsor_name": "BAYER",
         "submissions": [{"submission_type": "ORIG", "submission_status": "AP",
                          "submission_status_date": "20210709"}]},
    ]
    got = cat.parse_approvals(results, since=date(2026, 9, 26))
    assert [a["application"] for a in got] == ["NDA219713"]
    assert got[0]["brands"] == ["IBTROZI"]


def test_a_novel_approval_scores_above_a_routine_one():
    base = {"application": "NDA219713", "sponsor": "NUVATION", "date": date(2026, 9, 30), "brands": ["IBTROZI"]}
    novel = cat.approval_event(dict(base, **{"class": "TYPE 1"}), "NUVB")
    routine = cat.approval_event(dict(base, **{"class": "TYPE 5"}), "NUVB")
    assert novel.anomaly_score > routine.anomaly_score
    assert "exposes:NUVB" in novel.tags and novel.regulatory_data.affected_tickers == ["NUVB"]


def test_catalyst_events_pass_through_enrichment_unchanged():
    from shared.models import RawEvent
    from services.enrichment.enrichers.tradfi import PASSTHROUGH_SOURCES, TradFiEnricher
    assert {"finra_short_volume", "openfda"} <= PASSTHROUGH_SOURCES
    e = cat.short_spike_event("AA", "20261002", 0.62, [0.40] * 12, 1_193_988)
    raw = RawEvent(source=e.source, occurred_at=e.occurred_at,
                   raw_payload={"normalized_event": e.model_dump(mode="json")})
    out = asyncio.run(TradFiEnricher.__new__(TradFiEnricher).enrich(raw))
    assert out.event_id == e.event_id and out.headline == e.headline


def test_a_spike_is_measured_in_the_names_own_standard_deviations():
    steady = [0.40, 0.41, 0.39, 0.40, 0.41, 0.40, 0.39, 0.40, 0.41, 0.40, 0.40, 0.39]
    noisy = [0.30, 0.55, 0.35, 0.60, 0.32, 0.58, 0.31, 0.57, 0.36, 0.59, 0.33, 0.56]
    assert cat.short_spike(0.56, steady)      # far outside a steady name's range
    assert not cat.short_spike(0.62, noisy)   # inside a noisy name's normal swing


def test_fractional_finra_volume_still_builds_an_event():
    e = cat.short_spike_event("AA", "20261002", 0.62, [0.40] * 12, 600165.526789)
    assert e.financial_data.volume == 600166
