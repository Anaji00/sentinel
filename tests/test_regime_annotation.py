"""The regime label is attached to divergence alerts from database facts, annotation only (services/agents/regime_annotation.py)."""

import asyncio
import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import regime_annotation as ra  # noqa: E402

AT = datetime(2026, 11, 12, 18, 0, tzinfo=timezone.utc)


class _Db:
    def __init__(self, filings=(), reactions=(), insiders=(), dtc=None, fail=()):
        self.filings, self.reactions, self.insiders, self.dtc, self.fail = filings, reactions, insiders, dtc, set(fail)

    async def query(self, sql, *args):
        key = "filings" if "type = 'filing'" in sql else "reactions" if "earnings_reaction" in sql else "insiders"
        if key in self.fail:
            raise RuntimeError("down")
        return list({"filings": self.filings, "reactions": self.reactions, "insiders": self.insiders}[key])

    async def query_one(self, sql, *args):
        if "short" in self.fail:
            raise RuntimeError("down")
        return {"days_to_cover": self.dtc} if self.dtc is not None else None


def test_helpers_read_reactions_and_insider_direction() -> None:
    assert ra.reaction_from_headline("HIMS earnings reaction -10.2% vs SPY") == -0.102 and ra.reaction_from_headline("none") is None
    assert ra.insider_net([]) is None and ra.insider_net([["insider_cluster", "open_market_buy"]]) == 1.0
    assert ra.insider_net([["open_market_sell"]]) == -1.0 and ra.insider_net([["open_market_buy"], ["open_market_sell"]]) == 0.0


def test_an_adverse_filing_and_a_drop_give_the_trap_label() -> None:
    db = _Db(filings=[{"items": ["4.02", "9.01"]}])
    out = asyncio.run(ra.annotate(db, "XYZ", AT, -3.1, 2))
    assert out["label"] == "FUNDAMENTAL_IMPAIRMENT_TRAP" and out["action"] == "veto_buy"


def test_nothing_found_with_every_lookup_working_is_an_unexplained_dislocation() -> None:
    out = asyncio.run(ra.annotate(_Db(), "XYZ", AT, 2.8, 0))
    assert out["label"] == "UNEXPLAINED_DISLOCATION" and out["basis"] == "measured"


def test_a_failed_lookup_leaves_the_input_unmeasured_so_no_catalyst_is_not_claimed() -> None:
    out = asyncio.run(ra.annotate(_Db(fail=["filings"]), "XYZ", AT, 2.8, 0))
    assert out["label"] == "CROWDED_NO_EDGE" and out["basis"] == "insufficient_evidence" and "adverse_8k_items" in out["missing"]


def test_the_watch_attaches_the_label_to_the_alert_and_its_tags() -> None:
    src = (ROOT / "services/agents/divergence_watch.py").read_text(encoding="utf-8")
    assert "regime_annotation.annotate(self.db" in src and '"regime": regime,' in src and 'f"regime:{regime[' in src
