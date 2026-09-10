"""The collector watched three chains and the enricher routed one.

`collector-crypto` holds RPC endpoints for ethereum, arbitrum and base, and
stamps every transfer `source=f"{chain}_rpc"`. The crypto enricher's dispatch
matched the literal `"ethereum_rpc"`, so whale transfers from the other two
chains fell through to no branch at all and were discarded on arrival.

Measured live, by the unrouted-source counter built earlier in this audit:
**8,081 dropped in 28 minutes**, against 8,320 ethereum transfers stored over
two hours and zero from arbitrum or base -- at a $250,000 whale threshold, on
two of the highest-volume L2s. Roughly half the on-chain traffic the platform
collects was being thrown away.

This is the third instance of the same defect here. Every pre-market and
after-hours equity bar was discarded for as long as only `finnhub_equities` was
routed; the OKX funding poller's output vanished after being collected
correctly because the branch named a venue rather than the thing it matched.
Both are recorded in comments next to the branches that now handle them.

The first two were found by accident. This one was found because the counter
said so, which is the argument for the counter.
"""
import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
COLLECTOR = (ROOT / "services" / "collector-crypto" / "main.py").read_text(encoding="utf-8")
ENRICHER = (ROOT / "services" / "enrichment" / "enrichers" / "crypto.py").read_text(encoding="utf-8")


def _configured_chains():
    """The chains the collector actually watches, read from its own config."""
    tree = ast.parse(COLLECTOR)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "RPC_FALLBACKS":
                    return [k.value for k in node.value.keys]
    return []


def test_the_collector_watches_more_than_one_chain():
    chains = _configured_chains()
    assert len(chains) >= 3, chains
    assert "arbitrum" in chains and "base" in chains


@pytest.mark.parametrize("chain", _configured_chains())
def test_every_configured_chain_is_routed(chain):
    """Derived from the collector's config, so adding a fourth chain is caught."""
    source = f"{chain}_rpc"
    # The dispatch must admit this source. A literal match on one chain is what
    # discarded the other two.
    assert re.search(r'source\.endswith\(\s*["\']_rpc["\']\s*\)', ENRICHER), (
        f"{source} has no branch; the dispatch matches a single literal chain"
    )


def test_the_dispatch_does_not_name_a_single_chain():
    assert 'source == "ethereum_rpc"' not in ENRICHER


def test_the_enricher_is_chain_agnostic():
    """It reads wallets, asset and notional -- nothing Ethereum-specific."""
    i = ENRICHER.index("async def _enrich_whale_transfer")
    body = ENRICHER[i:i + 2000]
    for chain_specific in ("etherscan", "mainnet", "chain_id == 1"):
        assert chain_specific not in body.lower(), chain_specific


def test_the_payload_carries_the_chain_so_a_consumer_can_tell_them_apart():
    """Routing them all is only right if the events stay distinguishable."""
    assert '"chain": chain_name' in COLLECTOR
