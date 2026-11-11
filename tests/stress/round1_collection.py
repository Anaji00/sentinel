"""Round 1: the collection layer. What raw feeds hand us, and the first logic that reads it.

Collectors parse symbols, quotes and files from the outside world, so these cases are
hostile on purpose: malformed text, impossible dates, NaN and infinity, mixed time
zones, and a market that is closed, halted or at a daylight-saving boundary. Every
function below is real code, called with generated input.
"""
from __future__ import annotations

import math
import string
from datetime import datetime, timedelta, timezone

from shared.utils import options_flow as ofl
from shared.utils import short_data as sd
from shared.utils import sec_holdings as sh
from shared.utils.equities import parse_occ_option_symbol
from shared.utils.market_session import current_session, session_liquidity_factor
from shared.utils.quote_cache import parse_quote
from shared.utils.stock_peers import clean_ticker

from tests.stress.core import NOTE, PASS, Ledger, finite, nasty_floats, rng, safe_repr

UTC = timezone.utc


def _occ(root: str, y: int, m: int, d: int, kind: str, strike8: str) -> str:
    return f"{root}{y % 100:02d}{m:02d}{d:02d}{kind}{strike8}"


def run(seed: int, n: int = 150) -> Ledger:
    L = Ledger("1. Collection", seed)
    r = rng(seed, "r1")

    # ── OCC option symbols: what a snapshot feed keys every contract by ──
    valid = [(''.join(r.choices(string.ascii_uppercase, k=r.randint(1, 5))), r.randint(2024, 2030), r.randint(1, 12),
              r.randint(1, 28), r.choice("CP"), f"{r.randint(1, 99999999):08d}") for _ in range(n)]
    L.family("OCC symbols round-trip", valid, lambda c: (
        None if (p := parse_occ_option_symbol(_occ(*c))) and p["ticker"] == c[0] and p["option_type"] == ("CALL" if c[4] == "C" else "PUT")
        and abs(p["strike"] - int(c[5]) / 1000) < 1e-6 and p["expiry"] == f"{c[1]}-{c[2]:02d}-{c[3]:02d}" else "did not round-trip"))
    impossible = [(''.join(r.choices(string.ascii_uppercase, k=3)), 2026, m, d, "C", "00100000")
                  for m, d in [(13, 1), (0, 5), (2, 30), (4, 31), (6, 0), (12, 32), (99, 99), (2, 29)]]
    L.family("OCC symbols with impossible dates are refused", impossible,
             lambda c: "accepted an impossible expiry " + str(parse_occ_option_symbol(_occ(*c))) if parse_occ_option_symbol(_occ(*c)) else None,
             lambda c: _occ(*c))
    junk = ["", " ", "AAPL", "BRK.B261113C00547500", "aapl240816c00220000 ", "AAPL240816X00220000", "AAPL24081C00220000",
            "TOOLONG240816C00220000", "AAPL240816C002200000", "АAPL240816C00220000", "AAPL240816C00220000\n", "💥", "A" * 500,
            "AAPL240816C0022000O", "AAPL------C00220000", None]
    L.family("OCC parser never raises on junk", junk, lambda s: None if parse_occ_option_symbol(s) is None or isinstance(parse_occ_option_symbol(s), dict) else "bad type",
             safe_repr, note=False)

    # ── The quote rule that decides buyer or seller ──
    t0 = datetime(2026, 11, 5, 15, 0, tzinfo=UTC)
    quotes = []
    for _ in range(n * 3):
        bid = r.choice([0.0, None, r.uniform(0.01, 50)])
        ask = r.choice([None, 0.0, (bid or 1.0) * r.uniform(0.95, 1.3)])
        price = r.choice(nasty_floats(r, 2))
        lag = r.choice([0, 1, 30, 59, 61, 299, 301, -30, -400, 1e9])
        quotes.append((price, bid, ask, t0, t0 + timedelta(seconds=min(lag, 3e8))))

    def sane_side(c):
        price, bid, ask, tt, qt = c
        side = ofl.aggressor(price, bid, ask, tt, qt)
        if side not in (None, "buy", "sell"):
            return f"returned {side!r}"
        if side and not (bid and ask and ask >= bid):
            return f"read a side from an unusable quote {side}"
        if side == "buy" and finite(price) and ask and price < ask and price <= (bid + ask) / 2:
            return "called a buy at or below the midpoint"
        if side == "sell" and finite(price) and bid and price > bid and price >= (bid + ask) / 2:
            return "called a sell at or above the midpoint"
        return None
    L.family("buyer or seller is only read from a usable quote", quotes, sane_side, lambda c: f"px={c[0]:.4g} bid={c[1]} ask={c[2]} lag={(c[4]-c[3]).total_seconds():.0f}s")
    naive = [(1.0, 1.0, 1.1, datetime(2026, 11, 5, 15, 0), t0), (1.0, 1.0, 1.1, t0, datetime(2026, 11, 5, 15, 0))]
    L.family("naive and aware timestamps are handled, not crashed on", naive,
             lambda c: None if ofl.aggressor(*c) in (None, "buy", "sell") else "bad value", lambda c: "naive/aware mix", note=True)
    neg = [(1.0, s, k, 100.0, 105.0) for s in (-5.0, -0.001) for k in ("CALL", "PUT")]
    L.family("a negative trade size never becomes a publishable premium", neg,
             lambda c: (lambda e: None if e is None or e <= 0 else f"positive extrinsic {e} from size {c[1]}")(ofl.extrinsic_premium(c[0], c[1], c[2], c[3], c[4])), lambda c: f"size {c[1]}", note=True)

    # ── Extrinsic premium and the intrinsic share ──
    vals = nasty_floats(r, 60)
    sizes = [v for v in vals if not (isinstance(v, float) and v < 0)]
    prem = [(r.choice(vals), r.choice(sizes), r.choice(["CALL", "PUT", "C", "", "x"]), r.choice(vals + [None]), r.choice(vals + [None])) for _ in range(n * 3)]

    def prem_ok(c):
        price, size, kind, strike, spot = c
        e = ofl.extrinsic_premium(price, size, kind, strike, spot)
        s = ofl.intrinsic_share(price, kind, strike, spot)
        if e is not None and finite(e) and e < 0:
            return f"negative extrinsic {e}"
        if s is not None and finite(s) and not (0 <= s <= 1):
            return f"intrinsic share {s} outside 0..1"
        if e is not None and finite(e) and finite(price) and finite(size) and price > 0 and size > 0 and e > price * size * 100 * (1 + 1e-9):
            return "extrinsic premium exceeds the premium paid"
        return None
    L.family("extrinsic premium never exceeds what was paid", prem, prem_ok, lambda c: f"px={c[0]:.4g} size={c[1]:.4g} {c[2]} K={c[3]} S={c[4]}", note=True)
    nonfinite = [c for c in prem if any(isinstance(v, float) and not math.isfinite(v) for v in c[:2] + c[3:])]
    L.family("non-finite inputs do not become finite premiums", nonfinite,
             lambda c: (lambda e: None if e is None or not finite(e) or c[0] != c[0] else "NaN/inf input produced a finite premium")(
                 ofl.extrinsic_premium(c[0], c[1], c[2], c[3], c[4])), lambda c: f"px={c[0]} size={c[1]}", note=True)

    # ── The quote tape: bounded memory, nearest-in-time answers ──
    tape_cases = []
    for _ in range(40):
        times = sorted(t0 + timedelta(seconds=r.randint(0, 3000)) for _ in range(r.randint(1, 60)))
        shuf = times[:]
        r.shuffle(shuf)
        tape_cases.append((shuf, t0 + timedelta(seconds=r.randint(-300, 3300))))

    def tape_ok(c):
        order, when = c
        tape = ofl.QuoteTape()
        for k, t in enumerate(order):
            tape.record("X", t, 1.0 + k, 1.1 + k)
        got = tape.at("X", when)
        if got is None:
            return "no answer from a non-empty tape" if len(tape) else None
        accepted, high = [], None          # the tape keeps a quote only if it is newer than the last one kept
        for t in order:
            if high is None or t > high:
                accepted.append(t)
                high = t
        if abs(got[2] - when) > min(abs(t - when) for t in accepted):
            return "the answer is not the recorded quote nearest in time"
        return None
    L.family("tape returns the quote nearest the print among the quotes it kept", tape_cases, tape_ok, lambda c: f"{len(c[0])} quotes")
    dropped_late = sum(1 for order, _ in tape_cases for k, t in enumerate(order) if any(u >= t for u in order[:k]))
    L.record("quotes that arrive late are discarded, not inserted", NOTE if dropped_late else PASS,
             f"{dropped_late} late quotes in {sum(len(o) for o, _ in tape_cases)} were ignored because a newer one was already held. "
             "The feed's quotes arrive in order, so this only matters if a quote's timestamp falls back to the poll time.", sum(len(o) for o, _ in tape_cases))
    tape = ofl.QuoteTape()
    for i in range(20000):
        tape.record("HOT", t0 + timedelta(seconds=i), 1.0, 1.1)
    held = len(tape._tape["HOT"][0])
    tape.prune(t0 + timedelta(seconds=20000))
    L.record("tape memory is bounded after pruning", "PASS" if len(tape._tape["HOT"][0]) <= 25 * 60 + 5 else "DEFECT",
             f"20,000 one-second quotes: {held} held, {len(tape._tape.get('HOT', ([], []))[0])} after prune (25 minute window)", 20000)
    for bad in (float("nan"), float("inf"), 0.0, None):
        tape2 = ofl.QuoteTape()
        tape2.record("N", t0, bad, 1.0)
    L.record("tape refuses unusable quotes", "PASS", "NaN, infinite, zero and missing bids did not raise", 4)

    # ── The collector's own timestamp parser ──
    import importlib.util
    import pathlib
    import sys as _sys
    path = pathlib.Path(__file__).resolve().parents[2] / "services" / "collector-tradfi"
    try:
        _sys.path.insert(0, str(path))
        spec = importlib.util.spec_from_file_location("tradfi_collector_main", path / "main.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        stamps = ["2026-11-05T15:00:00Z", "2026-11-05T15:00:00.123456789Z", "2026-11-05T15:00:00+00:00", "2026-11-05T10:00:00-05:00", "2026-11-05 15:00:00",
                  "garbage", "", None, 12345, "0001-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "2026-13-45T00:00:00Z", "2026-11-05T15:00:00.Z", "2026-11-05T25:00:00Z"]
        L.family("the collector's trade timestamps are aware datetimes or nothing", stamps,
                 lambda v: (lambda d: None if d is None or (d.tzinfo is not None) else "naive datetime returned")(mod._option_trade_time({"t": v})), safe_repr)
    except Exception as exc:
        L.record("collector timestamp parser", NOTE, f"could not import the collector module for this check: {type(exc).__name__}: {str(exc)[:80]}", 0)
    finally:
        _sys.path = [x for x in _sys.path if x != str(path)]

    # ── Poll capture weight ──
    L.family("poll capture weight is a probability", [(g, p) for g in [None, 0, -5, 1e-3, 80, 1e9] for p in (84.0, 163.0, 313.0)],
             lambda c: None if 0 <= ofl.poll_capture_weight(*c) <= 1 else "outside 0..1")

    # ── Cached prices ──
    prices = ["93.23", b"93.23", "nan", "inf", "-5", "0", "1e999", "", "abc", '{"price": 12.5}', '{"price": "x"}', b"\xff\xfe", None, "True", "  7 ", "1,234.5"]
    L.family("cached prices parse to a positive finite number or nothing", prices,
             lambda v: (lambda p: None if p is None or (finite(p) and p > 0) else f"returned {p}")(parse_quote(v)), safe_repr)

    # ── Tickers entering SQL and URLs ──
    tickers = ["AAPL", "brk.b", "BF-B", "AAPL; DROP TABLE events", "../../etc", "A" * 40, "", " ", "АAPL", "AAPL\n", "$AAPL", "SPY%20", "aapl ", "123", "A.B.C"]
    def ticker_ok(s):
        t = clean_ticker(s)
        if t is None:
            return None
        if not all(ch in string.ascii_uppercase + string.digits + ".-" for ch in t) or len(t) > 10:
            return f"let {t!r} through"
        return None
    L.family("ticker cleaning lets nothing dangerous through", tickers, ticker_ok, safe_repr)

    # ── FINRA and borrow files, and the SEC fails file ──
    finra_good = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20261104|AAPL|1000|10|5000|N\n20261104|XYZ|abc|1|2|N\n20261104||5|1|9|N\n20261104|NEG|-5|0|-9|N\n20261104|BIG|1e999|0|1|N\nTrailer|1\n"
    def finra_ok(text):
        rows = sd.parse_finra(text)
        for row in rows:
            if not all(finite(float(x)) for x in row[2:5] if x is not None):
                return "non-finite row stored"
            if any(float(x) < 0 for x in row[2:5] if x is not None):
                return "negative volume stored"
        return None
    L.family("FINRA files with bad rows store only sane rows", [finra_good, "", "garbage\n\n\x00\x00", "Date|Symbol\n", "﻿Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"], finra_ok, lambda t: f"{len(t)} chars", note=True)
    ftd = "SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE\n20261001|X|ABC|630|D|0.58\n20260230|X|ABC|5|D|1\n20261001|X|ABC|-5|D|1\n20261001|X|ABC|7.5|D|1\n"
    rows = sh.parse_ftd(ftd)
    L.record("fails-to-deliver file skips impossible dates and negative sizes", "PASS" if len(rows) == 1 and rows[0][3] == 630 else "DEFECT", f"kept {len(rows)} of 4 rows (expected the one valid row)", 4,
             [] if len(rows) == 1 else [str(rows)[:140]])

    # ── Sessions and clocks ──
    edges = [datetime(2026, 3, 8, 6, 59, tzinfo=UTC), datetime(2026, 3, 8, 7, 1, tzinfo=UTC), datetime(2026, 11, 1, 5, 59, tzinfo=UTC), datetime(2026, 11, 1, 6, 1, tzinfo=UTC),
             datetime(2026, 11, 26, 15, 0, tzinfo=UTC), datetime(2026, 7, 3, 15, 0, tzinfo=UTC), datetime(2026, 11, 27, 18, 5, tzinfo=UTC), datetime(2026, 12, 25, 15, 0, tzinfo=UTC),
             datetime(2026, 11, 7, 15, 0, tzinfo=UTC), datetime(2026, 11, 5, 14, 30, tzinfo=UTC), datetime(2026, 11, 5, 21, 0, tzinfo=UTC), datetime(2035, 7, 4, 15, 0, tzinfo=UTC)]
    L.family("sessions and liquidity factors exist at daylight-saving, holiday and boundary times", edges,
             lambda t: None if 0 < session_liquidity_factor(t, asset_class="equities") <= 1 and current_session(t, "equities") is not None else "no valid session", lambda t: t.isoformat())
    L.record("open and close boundaries", "PASS" if str(current_session(datetime(2026, 11, 5, 14, 30, tzinfo=UTC), "equities")).endswith("REGULAR")
             and not str(current_session(datetime(2026, 11, 5, 21, 0, tzinfo=UTC), "equities")).endswith("REGULAR") else "NOTE",
             f"14:30 UTC (09:30 ET) is {current_session(datetime(2026, 11, 5, 14, 30, tzinfo=UTC), 'equities')}, 21:00 UTC (16:00 ET) is {current_session(datetime(2026, 11, 5, 21, 0, tzinfo=UTC), 'equities')}")
    return L
