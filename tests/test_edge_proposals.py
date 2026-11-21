"""Neo4j audit 18 Nov, items c to f: replication stamp, default source, self-loop and non-symbol sympathy refusal, SUPPLIER_TO properties."""
import pathlib

from shared.utils import edge_proposals as ep

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_stamp_adds_replicated_and_last_check_without_mutating_the_original() -> None:
    p = {"entity_id": "A", "action": "LINK_ENTITY", "data": {"target_id": "B", "properties": {"source": "pearson"}}}
    out = ep.stamp_replication(p, 1_700_000_000_000)
    assert out["data"]["properties"] == {"source": "pearson", "replicated": True, "last_check": 1_700_000_000_000}
    assert "replicated" not in p["data"]["properties"]
    assert ep.stamp_replication({"entity_id": "A"}, 5)["data"]["properties"]["last_check"] == 5


def test_self_loops_and_non_symbol_sympathy_endpoints_are_refused() -> None:
    assert ep.refusal_reason("SYMPATHY_MOVER", "NVDA", "nvda") == "self_loop"
    assert ep.refusal_reason("SUPPLIER_TO", "MAT", "MAT") == "self_loop"
    assert ep.refusal_reason("SYMPATHY_MOVER", "NVDA", "$325.18") == "sympathy_endpoint_not_a_symbol"
    assert ep.refusal_reason("SYMPATHY_MOVER", "NVDA", "cryptocurrency intelligence for usdt") == "sympathy_endpoint_not_a_symbol"
    assert ep.refusal_reason("SYMPATHY_MOVER", "NVDA", "AMD") is None
    assert ep.refusal_reason("SYMPATHY_MOVER", "BRK.B", "000550.SZ") is None
    assert ep.refusal_reason("COMPETES_WITH", "NVDA", "AMD") is None


def test_default_source_is_never_blank() -> None:
    assert ep.default_source({"data": {"properties": {"source": "form4"}}}) == "form4"
    assert ep.default_source({"data": {"source": "ais"}}) == "ais"
    assert ep.default_source({"producer": "customer_links", "data": {}}) == "customer_links"
    assert ep.default_source({"action": "LINK_ENTITY", "data": {}}) == "proposal:LINK_ENTITY"


def test_the_supervisor_and_producers_are_wired() -> None:
    sup = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    assert "refusal_reason(relation, entity_id, target_id)" in sup and "default_source(p)" in sup
    cl = (ROOT / "services/agents/customer_links_job.py").read_text(encoding="utf-8")
    assert '"properties": {"source": "customer_links"' in cl and '"revenue_pct": pct' in cl
    sd = (ROOT / "services/correlation/statistical_discovery.py").read_text(encoding="utf-8")
    assert sd.count("stamp_replication(") >= 2
    wg = (ROOT / "services/agents/adversarial_wargamer.py").read_text(encoding="utf-8")
    assert "MATCH (a:Entity {id: $id})" not in wg
