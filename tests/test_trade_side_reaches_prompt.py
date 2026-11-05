"""The inferred side of a large trade reaches the reasoning prompt, and is never guessed."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reasoning.context_builder import _financial_facts  # noqa: E402

FIN = {"financial_data": {"sector": "Technology"}}


def test_both_tag_spellings_reach_the_prompt() -> None:
    assert "buyer-initiated (inferred)" in _financial_facts({**FIN, "tags": ["buyer_initiated"]})
    assert "buyer-initiated (inferred)" in _financial_facts({**FIN, "tags": ["aggressor_buy"]})
    assert "seller-initiated (inferred)" in _financial_facts({**FIN, "tags": ["seller_initiated"]})
    assert "seller-initiated (inferred)" in _financial_facts({**FIN, "tags": ["aggressor_sell"]})


def test_an_unread_side_says_nothing() -> None:
    assert "initiated" not in _financial_facts({**FIN, "tags": ["call"]})
    assert "initiated" not in _financial_facts(FIN)
