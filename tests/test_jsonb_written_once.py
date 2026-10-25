"""JSONB parameters are encoded once.

The asyncpg pool registers a jsonb codec (shared/db/__init__.py) that
JSON-encodes parameters itself. Writers that also called json.dumps stored the
document as a JSON *string*: SQL's ->> then returned NULL (the dossier and
macro engines lost ticker and sector on 264k archived rows). Writers hand over
plain Python values; migration 0052 unwrapped the stored rows.
"""

import importlib.util
import json
import pathlib
import re
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str((ROOT / rel).parent))
    spec.loader.exec_module(mod)
    return mod


def test_the_pool_codec_encodes_jsonb() -> None:
    src = (ROOT / "shared" / "db" / "__init__.py").read_text(encoding="utf-8")
    assert re.search(r"set_type_codec\(\s*'jsonb',\s*encoder=json\.dumps", src)


def test_earnings_reaction_rows_carry_a_dict() -> None:
    from services.agents import earnings_reaction as er
    row = er._row({"ticker": "MU", "ear": 0.0412345, "release_day": date(2026, 9, 24),
                   "known_at": datetime(2026, 9, 24, 21, tzinfo=timezone.utc)})
    fd = row[-1]
    assert isinstance(fd, dict) and fd == {"ticker": "MU", "ear": 0.04123, "release_day": "2026-09-24"}
    json.dumps(fd)                                    # the codec can encode it


def test_8k_history_rows_carry_a_dict() -> None:
    h = _load("filings_history", "services/collector-filings/history.py")
    subs = {"filings": {"recent": {"form": ["8-K"], "acceptanceDateTime": ["2026-09-24T16:05:00.000Z"],
                                   "accessionNumber": ["0000-1"], "items": ["2.02,9.01"]}}}
    rows = h.eight_k_rows("MU", "Micron", subs, {"2.02": "Results"}, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert rows and isinstance(rows[0][-1], dict) and rows[0][-1]["ticker"] == "MU"


def test_no_writer_pre_encodes_a_jsonb_parameter() -> None:
    """A json.dumps(...) argument next to a ::jsonb placeholder, in the writers fixed here."""
    for rel in ("services/agents/earnings_reaction.py", "services/collector-filings/history.py",
                "services/agents/recommendation_engine.py", "services/agents/factor_engine.py",
                "services/agents/portfolio_engine.py", "services/collector-civic/main.py"):
        src = (ROOT / rel).read_text(encoding="utf-8")
        for m in re.finditer(r"execute(?:_many)?\((?:[^()]|\([^()]*\))*?\)", src, re.S):
            call = m.group(0)
            assert not ("::jsonb" in call and "json.dumps(" in call), f"{rel}: {call[:120]}"
