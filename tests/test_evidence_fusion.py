"""Independent evidence per instrument, weighted by each family's own record."""

import ast
import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.evidence_fusion import (  # noqa: E402
    EVIDENCE_EVENT_TYPES, build_dossiers, convergence, family_record,
)
from shared.utils.signal_families import Observation  # noqa: E402

T0 = datetime(2026, 10, 5, 14, tzinfo=timezone.utc)


def _o(fam, inst, d, i=0):
    return Observation(fam, inst, d, T0 + timedelta(minutes=i), f"e{fam}{i}", f"{fam}|{i}|{inst}")


CELLS = [
    {"family": "insider_cluster", "horizon": "5d", "verdict": "proven", "units": 31, "hit_rate": 0.61, "mean_bps": 85},
    {"family": "insider_cluster", "horizon": "1h", "verdict": "insufficient_data", "units": 4},
    {"family": "sector_news_positive", "horizon": "1d", "verdict": "no_edge", "units": 76},
]


def test_a_family_counts_once_however_many_events() -> None:
    obs = [_o("options_call_sweep", "WDC", 1, i) for i in range(53)]
    d = build_dossiers(obs, {})["WDC"]
    assert len(d.factors) == 1 and d.factors[0].events == 53
    assert d.agreeing == 1 and convergence({"WDC": d}) == []


def test_independent_families_converge_and_proven_ranks_first() -> None:
    obs = [
        _o("insider_cluster", "ACME", 1), _o("options_call_sweep", "ACME", 1),
        _o("options_call_sweep", "ZZZ", 1), _o("institutional_accumulation", "ZZZ", 1),
    ]
    ds = build_dossiers(obs, family_record(CELLS))
    ranked = convergence(ds)
    assert [d.instrument for d in ranked] == ["ACME", "ZZZ"]
    assert ds["ACME"].stance == "long" and ds["ACME"].proven == 1
    assert "proven evidence" in ds["ACME"].summary
    assert "unproven evidence only" in ds["ZZZ"].summary


def test_a_family_with_no_edge_is_shown_but_not_counted() -> None:
    obs = [_o("sector_news_positive", "SOXX", 1), _o("options_call_sweep", "SOXX", 1)]
    d = build_dossiers(obs, family_record(CELLS))["SOXX"]
    assert any(f.family == "sector_news_positive" and f.weight == 0.0 for f in d.factors)
    assert d.agreeing == 1


def test_disagreement_is_reported_as_mixed() -> None:
    obs = [
        _o("insider_cluster", "ACME", 1), _o("options_call_sweep", "ACME", 1),
        _o("institutional_distribution", "ACME", -1),
    ]
    d = build_dossiers(obs, family_record(CELLS))["ACME"]
    assert d.stance == "mixed" and d.opposing == 1


def test_price_directed_families_mark_attention_not_a_side() -> None:
    d = build_dossiers([_o("volume_spike", "TXT", 0)], {})["TXT"]
    assert d.stance == "attention" and d.attention == 1 and d.score == 0


def test_the_event_types_match_the_signal_engine() -> None:
    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    start = src.index("SIGNAL_EVENT_TYPES = [")
    listed = ast.literal_eval(src[start + len("SIGNAL_EVENT_TYPES = "): src.index("]", start) + 1])
    assert listed == EVIDENCE_EVENT_TYPES


def test_families_reading_one_source_are_one_vote() -> None:
    """The first live build ranked SOXX short on a dark vessel and an STS
    transfer in one zone -- one observation of shipping, not two."""
    obs = [_o("dark_vessel_in_zone", "SOXX", -1, i) for i in range(5)] + [_o("tanker_sts_in_zone", "SOXX", -1)]
    d = build_dossiers(obs, {})["SOXX"]
    assert d.agreeing == 1 and convergence({"SOXX": d}) == []
    obs.append(_o("options_put_sweep", "SOXX", -1))
    d = build_dossiers(obs, {})["SOXX"]
    assert d.agreeing == 2 and d.stance == "short"
