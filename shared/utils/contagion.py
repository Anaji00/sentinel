"""Damped multi-hop propagation of a shock through supplier, customer, commodity and chokepoint links.

S_j = sum over k = 1..K of (gamma W)^k applied to the shocks, with every path kept so a downstream signal names how it got there.
An edge weight combines three independent sources of dependence (revenue or cost share, commodity sensitivity, historical lead-lag
partial correlation) as a noisy-or, so no single source can push a weight past 1.

Shadow tool: it reports second-order candidates (a large expected shock on a name whose own price has not moved); it publishes nothing.
Pure; no I/O.
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Tuple

DEFAULT_GAMMA = 0.5
DEFAULT_HOPS = 3

Edge = Tuple[str, str, float]


def edge_weight(revenue_share: float = 0.0, commodity_sensitivity: float = 0.0, partial_corr: float = 0.0) -> float:
    """Noisy-or of the three dependence sources, each clipped to [0, 1]; 0 when none is known."""
    keep = 1.0
    for v in (revenue_share, commodity_sensitivity, abs(partial_corr)):
        keep *= 1.0 - min(1.0, max(0.0, float(v or 0.0)))
    return round(1.0 - keep, 6)


def propagate(edges: Mapping[str, Mapping[str, float]], shocks: Mapping[str, float], gamma: float = DEFAULT_GAMMA,
              hops: int = DEFAULT_HOPS) -> Dict[str, Dict[str, object]]:
    """{node: {"score": total expected shock, "hops": hops of the strongest path, "path": [(src, dst, weight), ...], "paths": n}}.

    `edges[src][dst]` is how strongly dst is exposed to src. A path never revisits a node, so cycles cannot amplify a shock. Origins are
    not returned. Contributions of different paths to the same node add (signed)."""
    frontier: List[Tuple[str, float, Tuple[Edge, ...]]] = [(n, float(x), ()) for n, x in shocks.items() if x]
    out: Dict[str, Dict[str, object]] = {}
    origins = set(shocks)
    for k in range(1, max(1, int(hops)) + 1):
        nxt: List[Tuple[str, float, Tuple[Edge, ...]]] = []
        for node, value, path in frontier:
            seen = {node} | {e[0] for e in path}
            for dst, w in (edges.get(node) or {}).items():
                if dst in seen:
                    continue
                contrib = value * gamma * float(w)
                if contrib == 0.0:
                    continue
                new_path = path + ((node, dst, float(w)),)
                nxt.append((dst, contrib, new_path))
                if dst in origins:
                    continue
                rec = out.setdefault(dst, {"score": 0.0, "hops": k, "path": new_path, "paths": 0, "_best": 0.0})
                rec["score"] = float(rec["score"]) + contrib
                rec["paths"] = int(rec["paths"]) + 1
                if abs(contrib) > float(rec["_best"]):
                    rec["_best"], rec["hops"], rec["path"] = abs(contrib), k, new_path
        frontier = nxt
    for rec in out.values():
        rec.pop("_best", None)
        rec["score"] = round(float(rec["score"]), 6)
        rec["path"] = list(rec["path"])
    return out


def unadjusted(propagated: Mapping[str, Mapping[str, object]], own_moves: Mapping[str, float], min_score: float,
               max_move_share: float = 0.25) -> List[Dict[str, object]]:
    """Second-order candidates: expected shock of at least `min_score` (absolute), and the name's own move, in that direction, is under
    `max_move_share` of it (it has not yet adjusted). Strongest first; each keeps its provenance path."""
    rows: List[Dict[str, object]] = []
    for node, rec in propagated.items():
        score = float(rec["score"])
        if abs(score) < min_score:
            continue
        moved = float(own_moves.get(node, 0.0))
        done = moved / score if score else 0.0                    # share of the expected shock already priced, same sign
        if done < max_move_share:
            rows.append({"ticker": node, "expected_shock": score, "own_move": moved, "hops": rec["hops"], "path": rec["path"], "paths": rec["paths"]})
    return sorted(rows, key=lambda r: -abs(float(r["expected_shock"])))


__all__ = ["DEFAULT_GAMMA", "DEFAULT_HOPS", "edge_weight", "propagate", "unadjusted"]
