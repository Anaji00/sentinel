"""Daily bars keep being maintained for every name already tracked, not only those in the deep-history set (12 Nov 2026)."""

import ast
import asyncio
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")


def _function(name: str):
    tree = ast.parse(SRC)
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    ns = {"TRACKED_DAILY_DAYS": 45}
    exec(compile(ast.Module([node], []), "main.py", "exec"), ns)
    return ns[name]


class _Db:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    async def query(self, sql, *params):
        self.calls.append((sql, params))
        return self.rows


def test_tracked_names_come_from_recent_daily_bars_uppercased_and_blank_free() -> None:
    fn = _function("_tracked_daily_names")
    db = _Db([{"ticker": "sana"}, {"ticker": "GTR"}, {"ticker": None}, {"ticker": "SANA"}])
    assert asyncio.run(fn(db)) == {"SANA", "GTR"}
    assert "daily_bars" in db.calls[0][0] and db.calls[0][1] == (45,)
    assert asyncio.run(fn(_Db([]))) == set()


def test_both_top_ups_union_the_deep_set_with_the_tracked_names() -> None:
    assert SRC.count("await _tracked_daily_names(db)") == 2
    post_close = SRC[SRC.index("async def _post_close_daily_loop"):]
    assert "| await _tracked_daily_names(db))" in post_close and "backfill_daily_bars(db, names" in post_close
