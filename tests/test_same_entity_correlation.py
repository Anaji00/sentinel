"""A single-name microstructure rule must correlate within one name.

"Equity Block & Options Convergence" is a claim about a block trade and options
activity in the same company. The rule triggered on an equity_block for one
ticker and correlated it with any options_flow in a 48-hour window, regardless
of ticker -- so it published a cluster headlined AAPL whose three supporting
headlines were MTZ, KKR and DELL.

With a 48-hour window at min_anomaly 0.25 the rule could not fail to find ten
correlates, which is why avg_evidence sat at exactly 10.0 across 2,220 clusters
in a day: the [:10] cap, every time.

same_entity is opt-in. Geographic and cross-domain rules legitimately correlate
across names -- a headline moves many tickers, and vessels in a strait are
related by the strait rather than by identity.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.correlation.event_store import EventStore  # noqa: E402


class _Raw:
    def __init__(self, members):
        self.members = members

    async def zrange(self, key, start, end, desc=False, byscore=False,
                     offset=None, num=None, **kw):
        rows = sorted(self.members, key=lambda m: m[1], reverse=desc)
        rows = [m for m in rows if m[1] >= float(end)]
        if offset is not None:
            rows = rows[offset: offset + (num or len(rows))]
        return [r[0] for r in rows]


class _Redis:
    def __init__(self, raw):
        self.raw = raw


def _store():
    now = time.time()
    members = []
    for i, ticker in enumerate(["AAPL", "MTZ", "KKR", "DELL", "AAPL"]):
        payload = json.dumps({
            "event_id": f"e{i}", "type": "options_flow", "domain": "equity",
            "anomaly_score": 0.9, "tags": ["equity"], "region": None,
            "entity_id": ticker, "headline": f"sweep {ticker}",
        })
        members.append((payload, now - i))
    s = EventStore.__new__(EventStore)
    s.cache_key = "events:recent_window"
    s._redis = _Redis(_Raw(members))
    return s


def test_without_the_filter_every_ticker_comes_back():
    rows = asyncio.run(_store().get_recent(None, hours=48, limit=50))
    assert {r["entity_id"] for r in rows} == {"AAPL", "MTZ", "KKR", "DELL"}


def test_the_filter_keeps_only_the_triggering_name():
    rows = asyncio.run(_store().get_recent(None, hours=48, limit=50, entity_id="AAPL"))
    assert rows, "the filter removed everything, including the matching events"
    assert {r["entity_id"] for r in rows} == {"AAPL"}
    assert len(rows) == 2


def test_matching_is_case_insensitive():
    rows = asyncio.run(_store().get_recent(None, hours=48, limit=50, entity_id="aapl"))
    assert len(rows) == 2


def test_an_unknown_entity_returns_nothing_rather_than_everything():
    rows = asyncio.run(_store().get_recent(None, hours=48, limit=50, entity_id="NOSUCH"))
    assert rows == []


def test_the_single_name_rules_ask_for_it():
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    for rule_id in ("rule_financial_block_volume_spike",
                    "rule_insider_options_convergence",
                    "rule_options_darkpool_surge"):
        block = source[source.index(rule_id):]
        block = block[:block.index("alert_tier")]
        assert '"same_entity": True' in block, rule_id


def test_the_cross_entity_rules_do_not():
    """A headline moves many names; constraining it would break the rule."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    block = source[source.index("rule_news_financial_impact"):]
    block = block[:block.index("alert_tier")]
    assert '"same_entity"' not in block


def test_the_flag_is_read_at_the_call_site():
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert 'corr.get("same_entity")' in source
    assert "entity_id=(entity_id if" in source
