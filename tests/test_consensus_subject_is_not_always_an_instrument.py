"""A consensus signal's subject is called `ticker` and often is not one.

Measured on the running deployment 2026-09-20, the four most recent consensus
reports, 41 signals:

    contributing_agents   1     on all 41
    agreement_ratio       0.0   on all 41
    distinct subjects     ETD8MY, FDX10, JAL8664   flight callsigns
                          SAMANYOLU, RAGNAR        vessel names
                          TSLA, GOOGL, XOM, IBM    equities
                          SOLUSDT, ETHUSDT         crypto pairs

Five of eleven subjects are not instruments, and each arrived in exactly the
same shape as TSLA: `direction: "bearish"`, `consensus_score: -0.475`,
`weighted_conviction`. "Bearish ETD8MY at -0.475" is a statement about an
aircraft in the vocabulary of a trade, and nothing on the signal or on the API
response said which kind of thing it was.

`shared.utils.equities.asset_class` already answers this and its own docstring
insists the distinction be kept: None means "not a recognisable instrument
symbol", which is a different answer from "equity".
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.consensus_engine import ConsensusSignal  # noqa: E402
from shared.utils.equities import asset_class  # noqa: E402

FLIGHTS_AND_VESSELS = ["ETD8MY", "FDX10", "JAL8664", "SAMANYOLU", "RAGNAR"]
INSTRUMENTS = {"TSLA": "equity", "GOOGL": "equity", "XOM": "equity",
               "SOLUSDT": "crypto", "ETHUSDT": "crypto", "GC=F": "commodity"}


# ── the subjects actually observed ───────────────────────────────────────────


def test_an_aircraft_or_a_vessel_is_not_an_instrument():
    for subject in FLIGHTS_AND_VESSELS:
        assert asset_class(subject) is None, subject


def test_the_instruments_observed_beside_them_still_classify():
    for subject, expected in INSTRUMENTS.items():
        assert asset_class(subject) == expected, subject


# ── the signal carries the distinction ───────────────────────────────────────


def test_a_signal_records_what_kind_of_thing_its_subject_is():
    s = ConsensusSignal(ticker="TSLA", asset_class=asset_class("TSLA"), direction="bearish")
    assert s.asset_class == "equity"


def test_a_non_instrument_signal_says_so_rather_than_guessing():
    s = ConsensusSignal(ticker="ETD8MY", asset_class=asset_class("ETD8MY"), direction="bearish")
    assert s.asset_class is None, "an unrecognised subject must not become an equity"


def test_the_field_defaults_to_none_not_to_a_guess():
    assert ConsensusSignal(ticker="X", direction="mixed").asset_class is None


def test_both_construction_sites_populate_it():
    """Single-contributor and fused paths both emit signals."""
    src = (ROOT / "services/agents/consensus_engine.py").read_text(encoding="utf-8")
    assert src.count("asset_class=asset_class(ticker)") == 2, (
        "one of the two ConsensusSignal construction sites still omits it"
    )


def test_the_engine_imports_the_shared_answer_rather_than_rederiving_it():
    src = (ROOT / "services/agents/consensus_engine.py").read_text(encoding="utf-8")
    assert "from shared.utils.equities import asset_class" in src


# ── and the reader can see it ────────────────────────────────────────────────


def test_the_api_separates_signals_that_are_not_about_instruments():
    src = (ROOT / "services/api_gateway/routes/agents.py").read_text(encoding="utf-8")
    assert '"non_instrument_signals"' in src
    assert 'not s.get("asset_class")' in src


def test_the_existing_corroboration_split_is_untouched():
    """One agent is a lead, not a consensus -- that separation already existed."""
    src = (ROOT / "services/api_gateway/routes/agents.py").read_text(encoding="utf-8")
    assert '"corroborated_signals"' in src and '"single_agent_signals"' in src
