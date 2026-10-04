"""Government money to listed companies, and the people who sit on several boards.

USAspending's transaction search, dry-run over the five days to 4 October: 500
obligations, $8.8B, and 30 listed parents among the recipients -- Leidos $730M,
Maximus $404M, CACI $265M, Accenture $195M, General Dynamics $181M -- most of it
awarded to subsidiaries ("LEIDOS BIOMEDICAL RESEARCH INC", "ELECTRIC BOAT").
"""

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from shared.utils.contracts import aggregate_awards, award_score, normalize_recipient, parent_ticker

ROOT = Path(__file__).resolve().parents[1]


def test_subsidiaries_resolve_to_their_listed_parent():
    assert parent_ticker("LEIDOS BIOMEDICAL RESEARCH INC") == "LDOS"
    assert parent_ticker("CACI, INC. - FEDERAL") == "CACI"
    assert parent_ticker("ELECTRIC BOAT CORPORATION") == "GD"
    assert parent_ticker("THE BOEING COMPANY") == parent_ticker("BOEING COMPANY, THE") == "BA"
    assert parent_ticker("OPTUM PUBLIC SECTOR SOLUTIONS") == "UNH"


def test_near_misses_and_private_contractors_resolve_to_nothing():
    assert parent_ticker("JACOBSON CONSTRUCTION") is None          # not Jacobs
    assert parent_ticker("PERATON INC.") is None                   # private, stops at the table
    assert parent_ticker("TOTE SERVICES, LLC", lambda n: None) is None
    assert parent_ticker("ACME WIDGETS INC", lambda n: "ACME" if n == "ACME WIDGETS" else None) == "ACME"
    assert normalize_recipient("CACI, INC. - FEDERAL") == "CACI INC-FEDERAL"


def test_obligations_aggregate_per_company_and_size_against_it():
    rows = [
        {"Recipient Name": "LEIDOS, INC.", "Transaction Amount": 242e6, "Awarding Agency": "SSA",
         "Transaction Description": "OPTION YEAR", "Award ID": "A1"},
        {"Recipient Name": "LEIDOS BIOMEDICAL RESEARCH INC", "Transaction Amount": 342e6,
         "Awarding Agency": "HHS", "Award ID": "A2"},
        {"Recipient Name": "LEIDOS, INC.", "Transaction Amount": -5e6, "Awarding Agency": "SSA"},  # deobligation
        {"Recipient Name": "PERATON INC.", "Transaction Amount": 90e6, "Awarding Agency": "DOD"},
    ]
    agg = aggregate_awards(rows, parent_ticker)
    assert list(agg) == ["LDOS"]
    assert agg["LDOS"]["total"] == 584e6 and agg["LDOS"]["actions"] == 2
    assert agg["LDOS"]["largest"]["amount"] == 342e6
    assert award_score(584e6, 20e9) == 0.9 and award_score(100e6, 200e9) == 0.32
    assert award_score(1e6, None) == 0.35


def test_every_form4_owner_becomes_an_insider_link():
    spec = importlib.util.spec_from_file_location("tradfi_main_t", ROOT / "services/collector-tradfi/main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tradfi_main_t"] = mod
    spec.loader.exec_module(mod)
    graph = SimpleNamespace(link_entities=AsyncMock())
    mod._INSIDER_GRAPH = graph
    parsed = {"ticker": "ACME", "owners": [
        {"name": "Jane Roe", "is_director": True},
        {"name": "Big Fund II LP", "is_ten_percent_owner": True},
        {"name": "No Role Person"},
    ]}
    assert asyncio.run(mod._link_insiders(None, parsed)) == 2
    calls = graph.link_entities.await_args_list
    assert calls[0].args == ("JANE ROE", "INSIDER_OF", "ACME")
    assert calls[0].kwargs["source_label"] == "Person" and calls[0].kwargs["properties"]["role"] == "DIRECTOR"
    assert calls[1].kwargs["source_label"] == "InstitutionalFiler"


def test_the_new_predicates_are_allowed():
    from shared.models.ontology import is_valid_predicate
    for p in ("HOLDS_STAKE_IN", "LISTED_AS", "CONTRACTED_BY", "INSIDER_OF"):
        assert is_valid_predicate(p), p
