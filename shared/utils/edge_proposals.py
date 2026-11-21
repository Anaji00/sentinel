"""Pure helpers for graph edge proposals (Neo4j audit 18 Nov, items c to f). No I/O.

A proposal is the dict sent on `sentinel.ontology.proposals`: {entity_id, action, data: {target_id, properties, ...}}. The supervisor stores scalar entries of
`data.properties` on the edge (`r += extra`), so a stamp added here reaches the graph without a schema change."""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, Optional

# A traded symbol as the graph names one: 1 to 10 of A-Z 0-9 . - = ^ with a letter or digit first ("BRK.B", "ES=F", "000550.SZ"); not "$325.18" or free text.
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.\-=^]{0,9}$")
_HAS_LETTER = re.compile(r"[A-Z]")


def stamp_replication(proposal: Dict[str, Any], checked_at_ms: int, replicated: bool = True) -> Dict[str, Any]:
    """A copy of `proposal` whose properties say the edge passed the survival re-test and when it was last checked (epoch milliseconds)."""
    out = copy.deepcopy(proposal)
    data = out.setdefault("data", {})
    props = data.get("properties")
    if not isinstance(props, dict):
        props = {}
        data["properties"] = props
    props["replicated"] = bool(replicated)
    props["last_check"] = int(checked_at_ms)
    return out


def looks_like_symbol(value: Any) -> bool:
    """True for a string shaped like a traded symbol; a price ("$325.18"), a sentence or an empty value is not."""
    text = str(value or "").strip().upper()
    return bool(_SYMBOL.match(text)) and bool(_HAS_LETTER.search(text))


def refusal_reason(relation: str, source_id: Any, target_id: Any) -> Optional[str]:
    """Why an edge must not be written, or None: a self-loop for any relation, or a SYMPATHY_MOVER endpoint that is not a symbol."""
    a, b = str(source_id or "").strip(), str(target_id or "").strip()
    if a and a.upper() == b.upper():
        return "self_loop"
    if relation == "SYMPATHY_MOVER" and not (looks_like_symbol(a) and looks_like_symbol(b)):
        return "sympathy_endpoint_not_a_symbol"
    return None


def default_source(proposal: Dict[str, Any]) -> str:
    """The edge `source` when the producer set none: the proposal's own producer or source field, else the action it asked for (never blank)."""
    data = proposal.get("data") if isinstance(proposal.get("data"), dict) else {}
    props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
    for candidate in (props.get("source"), data.get("source"), proposal.get("source"), proposal.get("producer"), proposal.get("service")):
        if candidate:
            return str(candidate)
    return f"proposal:{proposal.get('action') or 'unknown'}"


__all__ = ["default_source", "looks_like_symbol", "refusal_reason", "stamp_replication"]
