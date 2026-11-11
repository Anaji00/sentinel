"""G3 (reports/Event backtest scope.md): why are 9 signal families "unmeasurable", and why do 1,937 instruments have no bars?

Run inside an agents container (it needs the database and Redis):
    MSYS_NO_PATHCONV=1 docker exec -w /app sentinel-agents-heavy python scripts/audit_unmeasurable.py [--probe N]

Read-only: nothing is written to Redis or the database. Queries are batched and sequential. `--probe N` additionally
asks Alpaca's free data API (one GET per 100 symbols, two windows) whether the first N never-priced symbols have
any daily bars; it needs the Alpaca keys in the container's environment and never prints them.
"""
import argparse
import asyncio
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from shared.db import get_redis, get_timescale
from shared.utils.asset_universe import load_asset_meta
from shared.utils.reaction_study import HORIZONS
from shared.utils.signal_families import PRICE_DIRECTED, episodes
from shared.utils.signal_store import BARS_BACKFILLED_KEY, DAILY_DEEP_KEY, MAX_NEEDS_BARS, NEEDS_BARS_KEY, STUDY_KEY
from services.agents.signal_engine import DEEP_HISTORY_TYPES, HISTORY_LOOKBACK_DAYS, STUDY_LOOKBACK_DAYS, SignalEngine

VALID_EQUITIES_KEY = "sentinel:equities:valid_set"
DAILY_ILLIQUID_KEY = "sentinel:daily_bars:illiquid"
DATA_URL = "https://data.alpaca.markets/v2/stocks/bars"

OCC = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")
# Suffix forms for warrants, units, rights and preferreds as they appear in SEC and vendor tickers.
WARRANT = re.compile(r"(\.|-)?(WS|WT|WTS|W)$|\.WS[A-Z]?$|-WT$")
UNIT = re.compile(r"(\.|-)U$|\.UN$")
RIGHT = re.compile(r"(\.|-)R$|\.RT$")
PREFERRED = re.compile(r"(\.|-)P[A-Z]?$|\.PR[A-Z]?$|-P-[A-Z]$|^[A-Z]{1,5}P[A-Z]$")


def classify_symbol(sym: str, meta: dict, valid: set, deep: set, sip_listed_hint: bool = False) -> str:
    """The most specific reason a symbol cannot have a plain daily series; first match wins."""
    if OCC.match(sym):
        return "option contract (OCC symbol)"
    if sym.endswith("=F"):
        return "futures proxy (=F)"
    if sym.endswith("=X") or sym.startswith("^"):
        return "FX pair or index"
    if sym.endswith("-USD") or sym.endswith("USDT") or sym.endswith("-USDT"):
        return "crypto"
    if not sym.replace(".", "").replace("-", "").isalnum():
        return "symbol has odd characters"
    m = meta.get(sym)
    if m is not None:
        kind = m[0]
        if kind == "fund":
            return "ETF/fund (active, Alpaca-listed)"
        if kind == "other":
            return "warrant/unit/right/preferred (per Alpaca name)"
        return "active common stock (Alpaca-listed)"
    if UNIT.search(sym):
        return "unit (by suffix)"
    if RIGHT.search(sym):
        return "right (by suffix)"
    if WARRANT.search(sym) and len(sym) > 4:
        return "warrant (by suffix)"
    if "." in sym or "-" in sym:
        return "share-class or preferred form (dot or dash)"
    if len(sym) == 5 and sym[-1] in "FY":
        return "foreign ordinary / ADR OTC line (5 letters ending F or Y)"
    if len(sym) > 5:
        return "symbol longer than 5 letters"
    if sym in valid:
        return "in valid set but not in active asset list"
    return "not in Alpaca active list (delisted, OTC, renamed or non-US)"


def as_date(v):
    """A date from whatever the driver returned (date, datetime or ISO string)."""
    if isinstance(v, datetime):
        return v.date()
    if hasattr(v, "year"):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()


def fmt_table(header: list, rows: list) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


async def alpaca_presence(symbols: list, windows: list) -> dict:
    """{symbol: set(window labels with at least one daily bar)} from Alpaca's free data API."""
    import aiohttp

    key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not key or not secret:
        return {}
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    found: dict = defaultdict(set)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), headers=headers) as session:
        for k in range(0, len(symbols), 100):
            chunk = symbols[k:k + 100]
            for label, start, end in windows:
                params = {"symbols": ",".join(chunk), "timeframe": "1Day", "start": start, "end": end,
                          "limit": 10000, "adjustment": "all", "feed": os.getenv("ALPACA_DAILY_FEED", "sip")}
                async with session.get(DATA_URL, params=params) as resp:
                    if resp.status != 200:
                        print(f"  probe: HTTP {resp.status} for {label} chunk {k // 100}")
                        continue
                    body = await resp.json()
                for sym, bars in (body.get("bars") or {}).items():
                    if bars:
                        found[sym.upper()].add(label)
                await asyncio.sleep(0.4)
    return found


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", type=int, default=0, help="ask Alpaca about the first N never-priced symbols")
    args = ap.parse_args()

    db, redis = await get_timescale(), await get_redis()
    raw = getattr(redis, "raw", redis)
    dec = lambda m: (m.decode() if isinstance(m, bytes) else str(m)).upper()  # noqa: E731

    study = json.loads(await raw.get(STUDY_KEY))
    cells = study["cells"]
    print(f"study generated {study['generated_at']}: episodes {study['episodes']}, instruments {study['instruments']}, "
          f"not studied {study['instruments_not_studied']}, without bars {study['instruments_without_bars']}")
    print("verdicts", study["verdicts"])
    unm = [c for c in cells if c["verdict"] == "unmeasurable"]
    fams = sorted({c["family"] for c in unm})
    print("\n## Unmeasurable cells")
    print(fmt_table(["family", "horizon", "episodes", "units"], [[c["family"], c["horizon"], c["episodes"], c["units"]]
                                                                  for c in sorted(unm, key=lambda c: (c["family"], c["horizon"]))]))
    print("\nall horizons of the 9 families:")
    print(fmt_table(["family", "horizon", "verdict", "episodes", "units", "instruments measured"],
                    [[c["family"], c["horizon"], c["verdict"], c["episodes"], c["units"], len(c.get("instruments") or [])]
                     for c in sorted(cells, key=lambda c: (c["family"], list(HORIZONS).index(c["horizon"])))
                     if c["family"] in fams]))

    meta = await load_asset_meta(raw)
    valid = {dec(m) for m in (await raw.smembers(VALID_EQUITIES_KEY) or [])}
    deep = {dec(m) for m in (await raw.smembers(DAILY_DEEP_KEY) or [])}
    illiquid = {dec(m) for m in (await raw.smembers(DAILY_ILLIQUID_KEY) or [])}
    needed = {dec(m) for m in (await raw.smembers(NEEDS_BARS_KEY) or [])}
    done = {dec(m) for m in (await raw.smembers(BARS_BACKFILLED_KEY) or [])}
    print(f"\nasset_meta {len(meta)}, valid set {len(valid)}, daily-deep {len(deep)}, illiquid {len(illiquid)}, "
          f"needs_bars key {len(needed)} (cap {MAX_NEEDS_BARS}), bars_backfilled {len(done)}")

    # Rebuild the study's episodes exactly as run_study does (weekly reads instead of daily ones: same rows, fewer queries).
    eng = SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=STUDY_LOOKBACK_DAYS)
    # The container has a 1 GiB limit and the signal engine shares it, so each window is reduced to counters as it is
    # read. Only the 9 families keep full observations (to rebuild their episodes); the rest are counted per instrument.
    inst_n: Counter = Counter()
    intel: set = set()
    fam_inst: dict = defaultdict(Counter)
    obs9: list = []
    fam_obs: Counter = Counter()

    async def absorb(start: datetime, end: datetime, types: list, step: int) -> None:
        day = start
        while day < end:
            nxt = min(end, day + timedelta(days=step))
            for o in await eng._load_observations(day, nxt, types):
                inst_n[o.instrument] += 1
                fam_obs[o.family] += 1
                fam_inst[o.family][o.instrument] += 1
                if o.family not in PRICE_DIRECTED:
                    intel.add(o.instrument)
                if o.family in fams:
                    obs9.append(o)
            day = nxt

    await absorb(since, now, None, 1)
    await absorb(now - timedelta(days=HISTORY_LOOKBACK_DAYS), since, DEEP_HISTORY_TYPES, 30)
    eps = episodes(obs9)
    by_inst: dict = defaultdict(list)
    for e in eps:
        by_inst[e.instrument].append(e)
    print(f"read {sum(fam_obs.values())} observations on {len(inst_n)} instruments (study: {study['episodes']} episodes / "
          f"{study['instruments']} instruments); the 9 families rebuilt as {len(eps)} episodes")

    # Bars held per instrument: daily history span, and whether any intraday bars exist.
    names = sorted(inst_n)
    daily: dict = {}
    intraday: dict = {}
    for k in range(0, len(names), 500):
        chunk = names[k:k + 500]
        for r in await db.query("SELECT ticker, count(*) AS n, min(day) AS d0, max(day) AS d1 FROM daily_bars "
                                "WHERE ticker = ANY($1::text[]) AND close > 0 GROUP BY 1", chunk) or []:
            daily[r["ticker"]] = (int(r["n"]), as_date(r["d0"]), as_date(r["d1"]))
        for r in await db.query("SELECT ticker, count(*) AS n, min(day) AS d0 FROM (SELECT ticker, time_bucket('1 day', bucket_time) AS day "
                                "FROM tradfi_bars_1h WHERE ticker = ANY($1::text[]) AND bucket_time > now() - interval '400 days') x "
                                "GROUP BY 1", chunk) or []:
            intraday[r["ticker"]] = (int(r["n"]), as_date(r["d0"]))

    # The hourly rollup lags the raw minute table for names that stream only briefly; the study reads the raw table.
    # Ask it directly (one indexed EXISTS per ticker) for the names still without any series.
    lone = [i for i in names if i not in daily and i not in intraday]
    raw_hit = 0
    for k in range(0, len(lone), 300):
        chunk = lone[k:k + 300]
        for r in await db.query("SELECT t AS ticker FROM unnest($1::text[]) t WHERE EXISTS "
                                "(SELECT 1 FROM tradfi_bars b WHERE b.ticker = t AND b.time > now() - interval '366 days')", chunk) or []:
            intraday[r["ticker"]] = (1, now.date() - timedelta(days=366))
            raw_hit += 1
    print(f"names with no daily or hourly bars but raw minute bars in the last year: {raw_hit} of {len(lone)}")
    no_series = [i for i in names if i not in daily and i not in intraday]
    nb = [i for i in no_series if i in intel and not i.endswith("=F")]
    print(f"\ninstruments with no daily and no intraday bars: {len(no_series)}; of those 'intel' (what the study counts): {len(nb)}")

    # --- 1,937 classification
    cls = Counter(classify_symbol(i, meta, valid, deep) for i in nb)
    ep_by_cls: Counter = Counter()
    for i in nb:
        ep_by_cls[classify_symbol(i, meta, valid, deep)] += inst_n[i]
    print("\n## Instruments without bars, by cause (observations, not collapsed episodes)")
    print(fmt_table(["cause", "instruments", "observations"], [[c, n, ep_by_cls[c]] for c, n in cls.most_common()]))
    print("\nrequested (needs_bars key):", len(set(nb) & needed), " already backfilled once:", len(set(nb) & done),
          " in illiquid set:", len(set(nb) & illiquid), " in valid set:", len(set(nb) & valid), " in asset_meta:", len(set(nb) & set(meta)))
    top = sorted(nb, key=lambda i: -inst_n[i])[:40]
    print("top 40 by observations:", ", ".join(f"{i}({inst_n[i]})" for i in top))
    for i in top[:25]:
        print(f"  {i}: " + ", ".join(f"{f}={c[i]}" for f, c in fam_inst.items() if c.get(i)))
    for c in cls:
        ex = [i for i in sorted(nb, key=lambda i: -inst_n[i]) if classify_symbol(i, meta, valid, deep) == c][:8]
        print(f"  e.g. {c}: {', '.join(ex)}")

    # --- how many of the 1,937 each family touches
    nbs = set(nb)
    fam_nb = {f: Counter({i: n for i, n in c.items() if i in nbs}) for f, c in fam_inst.items()}
    print("\n## Families by observations sitting on bar-less instruments")
    rows = sorted(((f, sum(c.values()), len(c), fam_obs[f]) for f, c in fam_nb.items() if c), key=lambda r: -r[1])
    print(fmt_table(["family", "observations on no-bar instruments", "such instruments", "family observations"],
                    [list(r) for r in rows[:25]]))

    # --- each of the 9 families
    for fam in fams:
        fe = [e for e in eps if e.family == fam]
        insts = Counter(e.instrument for e in fe)
        print(f"\n## {fam}: {len(fe)} episodes, {len(insts)} instruments")
        buckets: Counter = Counter()
        ep_buckets: Counter = Counter()
        for i, n in insts.items():
            d, h = daily.get(i), intraday.get(i)
            if d is None and h is None:
                b = "no bars at all: " + classify_symbol(i, meta, valid, deep)
            elif d is None:
                b = "intraday only (no daily_bars)"
            else:
                b = "has daily bars"
            buckets[b] += 1
            ep_buckets[b] += n
        print(fmt_table(["instrument bars", "instruments", "episodes"], [[b, n, ep_buckets[b]] for b, n in buckets.most_common()]))
        # Episodes whose time falls outside the daily span, and horizons the intraday bars cannot reach.
        before = after = covered = 0
        for e in fe:
            d = daily.get(e.instrument)
            if d is None:
                continue
            day = e.known_at.date()
            if day < d[1]:
                before += 1
            elif day > d[2] - timedelta(days=1):
                after += 1
            else:
                covered += 1
        have = sum(1 for e in fe if e.instrument in daily)
        no_intraday_cover = sum(1 for e in fe if e.instrument not in intraday or e.known_at.date() < intraday[e.instrument][1])
        print(f"episodes on instruments with daily bars: {have} -> before first bar {before}, within span {covered}, "
              f"after last bar {after}; episodes with no 1h intraday bar at that date: {no_intraday_cover} of {len(fe)}")
        sample = ", ".join(f"{i}({n})" for i, n in insts.most_common(12))
        print("top instruments:", sample)
        times = [e.known_at for e in fe]
        print("episode span:", min(times).date(), "->", max(times).date())
        mature = {name: sum(1 for t in times if t <= now - h) for name, h in HORIZONS.items()}
        print("episodes old enough for each horizon's window to have closed:", mature)

    # --- optional probe of Alpaca, per cause: up to N plain-ticker symbols from each of the largest groups
    if args.probe:
        win = [("recent", (now - timedelta(days=12)).strftime("%Y-%m-%d"), (now - timedelta(days=2)).strftime("%Y-%m-%d")),
               ("2021", "2021-03-01", "2021-03-20"), ("2017", "2017-03-01", "2017-03-20")]
        print("\n## Alpaca probe (free daily bars) per cause")
        rows = []
        for c, _n in cls.most_common():
            cand = [i for i in sorted(nb, key=lambda i: -inst_n[i])
                    if classify_symbol(i, meta, valid, deep) == c and re.fullmatch(r"[A-Z]{1,5}", i)][:args.probe]
            if not cand:
                continue
            found = await alpaca_presence(cand, win)
            recent = sum(1 for s_ in cand if "recent" in found.get(s_, set()))
            hist = sum(1 for s_ in cand if s_ in found and "recent" not in found[s_])
            rows.append([c, len(cand), recent, hist, len(cand) - recent - hist])
        print(fmt_table(["cause", "probed", "recent bars served", "history only", "nothing"], rows))


if __name__ == "__main__":
    asyncio.run(main())
