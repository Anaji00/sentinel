"""Every declared event type either has a producer, or is recorded as not having one.

Twenty of the forty-three members of `EventType` are never constructed anywhere
in the tree. That would be harmless if nothing depended on them -- an unused
enum member costs nothing. It is not harmless, because code has been written
downstream to handle several of them:

  * `services/alert_manager/telegram.py` formats VESSEL_SPOOF, VESSEL_STS,
    MARKET_CANDLE and CRYPTO_LIQUIDATION alerts that can never be sent.
  * `shared/utils/materiality.py` carries thresholds for DARK_POOL and
    PRICE_ANOMALY that nothing is ever measured against.
  * `services/correlation/cascade.py` encodes transitions out of
    FLIGHT_POSITION and CRYPTO_TRANSFER that no event can trigger.

Each of those reads, in review and in a docstring, as a working feature. The
platform looks like it alerts on ship-to-ship transfers. It cannot; nothing
emits the event.

This file makes the gap explicit and stops it growing. It is a ratchet, not a
prohibition: an event type may legitimately exist before its producer does. What
it may not do is exist unrecorded, and the list below may not silently get
longer.
"""
import ast
import pathlib

import pytest

from shared.models.events import EventType

ROOT = pathlib.Path(__file__).resolve().parents[1]
SEARCH_ROOTS = ("services", "shared")

# Names that hold an event's type on its way into a constructor.
_TYPE_TARGETS = frozenset({"type", "event_type", "etype", "ev_type"})


def _python_files():
    for root in SEARCH_ROOTS:
        for path in (ROOT / root).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path


def _members_referenced(node) -> set:
    """Every `EventType.X` anywhere in an expression."""
    return {
        n.attr for n in ast.walk(node)
        if isinstance(n, ast.Attribute)
        and isinstance(n.value, ast.Name)
        and n.value.id == "EventType"
    }


def _constructed_event_types() -> set:
    """Members that reach an event's `type`, read from the syntax tree.

    This was a regex for `type=EventType.X`, and it under-reported by four --
    each of which was then recorded in the table below as unproduced, with a
    confident sentence explaining why. It matches a name followed by `=`
    followed by the member, so it cannot see either of the two forms this tree
    actually uses in those places:

        event_type = EventType.FLIGHT_ANOMALY if ... else EventType.FLIGHT_POSITION
        type=getattr(EventType, "CRYPTO_TRANSFER", EventType.CRYPTO_TRANSFER)

    A conditional hides the second branch and a `getattr` wrapper hides both.
    HEADLINE, FLIGHT_POSITION, CRYPTO_TRANSFER and CRYPTO_LIQUIDATION were all
    being emitted the whole time, and `rule_news_financial_impact` triggers on
    HEADLINE -- so this file's own evidence said the platform's news-to-market
    rule could never fire, and it was wrong.

    Walking the value of any assignment or keyword whose target is a type name
    catches every form, because it reads the expression rather than its
    spelling.
    """
    found = set()
    for path in _python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            value = None
            if isinstance(node, ast.keyword) and node.arg in _TYPE_TARGETS:
                value = node.value
            elif isinstance(node, ast.Assign):
                names = {t.id for t in node.targets if isinstance(t, ast.Name)}
                names |= {t.attr for t in node.targets if isinstance(t, ast.Attribute)}
                if names & _TYPE_TARGETS:
                    value = node.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id in _TYPE_TARGETS:
                    value = node.value
            if value is not None:
                found |= _members_referenced(value)
    return found


# The table moved to shared/models/event_producers.py so the rule engine can
# read it too. It was complete and correct here and unreachable from the one
# place that most needed it: thirteen of fifteen shipped correlation rules
# selected on types recorded in it, and no check connected the two. The tests
# below are unchanged -- they still hold the table to the syntax tree.
from shared.models.event_producers import UNPRODUCED  # noqa: E402


def test_every_declared_event_type_is_either_produced_or_recorded():
    declared = {member.name for member in EventType}
    produced = _constructed_event_types()
    unaccounted = sorted(declared - produced - set(UNPRODUCED))
    assert not unaccounted, (
        "These event types are declared, never constructed, and not recorded in "
        f"UNPRODUCED: {unaccounted}. Either wire a producer or add an entry "
        "saying why there isn't one -- an event type nothing emits is a feature "
        "the platform appears to have and does not."
    )


def test_the_unproduced_list_does_not_describe_types_that_now_have_producers():
    # The list is a record of a gap. A stale entry makes it a record of nothing.
    produced = _constructed_event_types()
    stale = sorted(set(UNPRODUCED) & produced)
    assert not stale, (
        f"These types now have producers and should be removed from UNPRODUCED: {stale}"
    )


def test_the_unproduced_list_only_names_real_event_types():
    declared = {member.name for member in EventType}
    unknown = sorted(set(UNPRODUCED) - declared)
    assert not unknown, f"UNPRODUCED names types that no longer exist: {unknown}"


def test_the_gap_does_not_grow():
    # A ratchet. Adding an EventType without a producer is allowed; doing it
    # without noticing is what this stops.
    assert len(UNPRODUCED) <= 20, (
        f"{len(UNPRODUCED)} event types have no producer, up from 20. "
        "Wire one before declaring another."
    )


def test_every_entry_carries_a_reason():
    blank = sorted(name for name, why in UNPRODUCED.items() if not (why or "").strip())
    assert not blank, f"UNPRODUCED entries with no explanation: {blank}"
