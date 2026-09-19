"""The day's move, which the radar was downloading and throwing away.

Alpaca's snapshot endpoint returns `minuteBar`, `dailyBar` and `prevDailyBar`
for every symbol in the sweep. The collector bound all three and used the
daily bars only as the second and third terms of a price fallback, so
percent-on-the-day was one division away and was never performed -- for the
whole tradable universe, on every poll. An event was emitted only when the
volume z-score cleared its threshold, so nothing at all was stored for a symbol
that merely moved.

There was no gainers-or-losers concept anywhere in the tree: not a route, a
model field, a Redis key or a component.

Three defects sat in the way and are fixed here too. `/radar/sweeps` scanned
`sentinel:radar:mean:*` while the collector writes `sentinel:radar:1m_mean:` --
the third key-spelling defect this codebase has paid for -- and masked the
always-zero result with a constant. The same endpoint published a universe size of
4500 from a literal while the collector's own comments say 11,631. And the
latest-price cache stores a bare number that the paper broker parsed as JSON.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COLLECTOR_DIR = ROOT / "services" / "collector-radar"

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _code(path: Path) -> str:
    """Source with comments and docstrings removed.

    These files record the defects they fixed, quoting the old spelling in
    prose. An assertion that a bad string is absent has to read the code, or it
    matches the note explaining why the string is gone -- which has now caught
    me four times in this audit.
    """
    import ast

    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    docstrings.add((body[0].lineno, body[0].end_lineno))
    keep = []
    skip = set()
    for start, end in docstrings:
        skip.update(range(start, end + 1))
    for number, line in enumerate(source.splitlines(), 1):
        if number in skip:
            continue
        if line.lstrip().startswith("#"):
            continue
        keep.append(line)
    return chr(10).join(keep)


def _collector():
    """The radar collector, imported the way its container runs it."""
    if str(COLLECTOR_DIR) not in sys.path:
        sys.path.insert(0, str(COLLECTOR_DIR))
    spec = importlib.util.spec_from_file_location(
        "radar_collector_under_test", COLLECTOR_DIR / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# -- the arithmetic ----------------------------------------------------------


def test_two_names_up_six_percent_are_told_apart():
    """A gap that faded and a grind higher are not the same event.

    One number cannot distinguish them, which is why three are kept.
    """
    m = _collector()
    faded = m._percent_moves(prev_close=100.0, day_open=107.0, day_close=106.0)
    ground = m._percent_moves(prev_close=100.0, day_open=100.0, day_close=106.0)

    assert faded["day_pct"] == ground["day_pct"] == 6.0
    assert faded["gap_pct"] == 7.0
    assert faded["intraday_pct"] < 0, "gapped up and sold all session"
    assert ground["gap_pct"] == 0.0
    assert ground["intraday_pct"] == 6.0


def test_an_unmeasurable_move_is_absent_rather_than_zero():
    """Zero would rank an unmeasured symbol in the middle of the board."""
    m = _collector()
    assert m._percent_moves(prev_close=0.0, day_open=107.0, day_close=106.0) is None
    assert m._percent_moves(prev_close=100.0, day_open=107.0, day_close=0.0) is None
    # A missing open still yields the day's move; only the split is unknown.
    partial = m._percent_moves(prev_close=100.0, day_open=0.0, day_close=106.0)
    assert partial["day_pct"] == 6.0
    assert "gap_pct" not in partial


# -- the sweep keeps it ------------------------------------------------------


class _Pipe:
    def __init__(self, sink):
        self.sink = sink

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self.sink.append((name, args, kwargs))
            return self
        return record

    async def execute(self):
        return []


class _Raw:
    def __init__(self):
        self.calls = []
        self.values = {}

    def pipeline(self):
        return _Pipe(self.calls)

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **kw):
        self.values[key] = value


class _Redis:
    def __init__(self):
        self.raw = _Raw()


class _Producer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, data, key=None):
        self.sent.append(data)


class _Resp:
    def __init__(self, payload):
        self.status = 200
        self.headers = {"X-Rate-Limit-Remaining": "100"}
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, payload):
        self._payload = payload

    def get(self, *a, **k):
        return _Resp(self._payload)


def _snapshot(prev_close, day_open, day_close, minute_vol):
    return {
        "minuteBar": {"c": day_close, "v": minute_vol},
        "dailyBar": {"o": day_open, "c": day_close},
        "prevDailyBar": {"c": prev_close},
    }


async def test_a_symbol_that_only_moved_is_still_recorded():
    """The event is gated on a volume anomaly; the move must not be.

    Nothing at all used to be stored for a symbol that merely moved, so the
    platform could say what traded unusually much and nothing about what went
    up.
    """
    m = _collector()
    redis = _Redis()
    radar = m.QuantRadar(redis)
    producer = _Producer()
    state = {"total_evaluated": 0, "total_anomalies": 0, "polls": 0}

    await m.poll_alpaca_snapshots(
        _Session({"AAA": _snapshot(100.0, 100.0, 112.0, 1000)}),
        producer, radar, ["AAA"], 0.05, 3.0, state,
    )

    names = [call[0] for call in redis.raw.calls]
    assert "zadd" in names, "the move was not published"
    zadd = next(c for c in redis.raw.calls if c[0] == "zadd")
    mapping = zadd[1][1] if len(zadd[1]) > 1 else zadd[2].get("mapping")
    assert mapping["AAA"] == pytest.approx(12.0)
    # No volume anomaly on a single observation, and the move is kept anyway.
    assert producer.sent == []


async def test_the_board_is_replaced_rather_than_accumulated():
    """A ticker that stops reporting should leave, not sit at yesterday's number."""
    m = _collector()
    redis = _Redis()
    await m.poll_alpaca_snapshots(
        _Session({"AAA": _snapshot(100.0, 100.0, 112.0, 1000)}),
        _Producer(), m.QuantRadar(redis), ["AAA"], 0.05, 3.0,
        {"total_evaluated": 0, "total_anomalies": 0, "polls": 0},
    )
    names = [c[0] for c in redis.raw.calls]
    assert "rename" in names, "the staging set is never swapped in"
    assert names.index("delete") < names.index("zadd") < names.index("rename")


async def test_the_sweep_publishes_what_it_covered():
    """So the endpoint reporting it does not have to hold a literal."""
    m = _collector()
    redis = _Redis()
    await m.poll_alpaca_snapshots(
        _Session({"AAA": _snapshot(100.0, 100.0, 112.0, 1000)}),
        _Producer(), m.QuantRadar(redis), ["AAA"], 0.05, 3.0,
        {"total_evaluated": 0, "total_anomalies": 0, "polls": 0},
    )
    from shared.utils.radar_keys import RADAR_SWEEP_STATE_KEY

    hsets = [c for c in redis.raw.calls if c[0] == "hset" and c[1][0] == RADAR_SWEEP_STATE_KEY]
    assert hsets, "the sweep state was not written"
    mapping = hsets[0][2].get("mapping") or hsets[0][1][1]
    assert mapping["evaluated"] == "1"
    assert mapping["priced"] == "1"
    assert mapping["at"]


# -- the defects that were in the way ---------------------------------------


def test_the_sweep_endpoint_reads_the_prefix_the_collector_writes():
    """`sentinel:radar:mean:*` matched nothing, every request, forever.

    The third key-spelling defect here: the watchlist zrange, the calibration
    hashes, and this. It was masked by `mean_count or 1840`, so the endpoint
    reported a plausible baseline count that had never been measured.
    """
    from shared.utils.radar_keys import RADAR_BASELINE_PREFIX

    route = _code(ROOT / "services" / "api_gateway" / "routes" / "radar.py")
    collector = _code(COLLECTOR_DIR / "main.py")

    assert RADAR_BASELINE_PREFIX == "sentinel:radar:1m_mean:"
    assert 'match="sentinel:radar:mean:*"' not in route
    assert "RADAR_BASELINE_PREFIX" in route, "the route must use the shared prefix"
    assert "radar_baseline_key(" in collector, "and so must the writer"
    assert "mean_count or 1840" not in route, "a measurement with a constant behind it"


def test_the_universe_size_is_measured_rather_than_asserted():
    """4500 in the endpoint; 11,631 in the collector's own comments."""
    route = _code(ROOT / "services" / "api_gateway" / "routes" / "radar.py")
    assert "universe_size = 4500" not in route
    assert '"last_sweep_time": "Real-time 1-Bar Continuous"' not in route, (
        "a string where a timestamp belongs"
    )


def test_the_latest_price_cache_is_read_the_way_it_is_written():
    """Every writer stores a bare number; one reader parsed it as an object.

    `json.loads("93.23")` is a float, and `.get("price")` on it raises into
    whatever swallows the exception -- so the reader returned None for every
    ticker that existed. Found once in the agent tier, left in the paper
    broker, and then copied a third time into the mark-to-market added earlier
    in this audit, with a test fixture that invented the object format.
    """
    from shared.utils.quote_cache import parse_quote

    assert parse_quote("93.23") == 93.23
    assert parse_quote(b"93.23") == 93.23
    assert parse_quote('{"price": 12.5}') == 12.5
    assert parse_quote(None) is None
    assert parse_quote("0") is None

    broker = _code(ROOT / "shared" / "broker" / "paper.py")
    assert "parse_quote(" in broker
    assert 'quote.get("price")' not in broker


def test_no_quote_cache_reader_parses_it_as_an_object():
    """The pin above named one file, so the fourth copy landed somewhere else.

    `_underlying_spot` in the tradfi enricher read the cache with
    `json.loads(raw).get("price")`. The cache holds `1623.63` for ASML, so that
    raised AttributeError into a bare `except` and returned None for every
    ticker -- underlying_price populated on 0 of 603 options events, and
    otm_percentage with it, because the OTM calculation needs the spot.

    Checking every reader rather than one named file is the difference between
    a test that caught this and the one that did not.
    """
    import re

    read_sites = []
    for path in ROOT.rglob("*.py"):
        if any(part in {"node_modules", ".git", "__pycache__", "tests"} for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if "get(quote_key(" not in text:
            continue
        # Code only. Both readers carry a comment describing the old
        # object-form parse directly above the correct one, and a scan
        # that reads prose finds the explanation and calls it the defect.
        lines = [
            "" if ln.lstrip().startswith("#") else ln
            for ln in text.split('\n')
        ]
        for i, line in enumerate(lines):
            if "get(quote_key(" in line:
                window = "\n".join(lines[i:i + 18])
                read_sites.append((path.relative_to(ROOT), i + 1, window))

    assert read_sites, "the quote cache must have readers; this test found none"

    offenders = [
        f"{rel}:{ln}"
        for rel, ln, window in read_sites
        if re.search(r"""[.]get[(]\s*['\"](?:price|close|last)['\"]\s*[)]""", window)
    ]
    assert not offenders, (
        "these read the latest-price cache as an object, but every writer in "
        "the tree stores a bare number: " + ", ".join(offenders)
    )
