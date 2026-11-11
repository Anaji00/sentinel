"""Connection audit C1/C2/C3 (reports/Intelligence audit.md). Rules were registered before this ran.

Usage: python -I scripts/audit_connections.py <folder of exported CSVs>. Read-only: exports of daily_bars,
company_profile, text_peers and customer_links."""
import sys
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from shared.utils.sympathy import top_peers  # noqa: E402

D = sys.argv[1] if len(sys.argv) > 1 else "."   # folder of exports: closes.csv, sic.csv, text.csv, cust.csv
rng = np.random.default_rng(7)

closes = pd.read_csv(f"{D}/closes.csv", parse_dates=["day"]).pivot(index="day", columns="ticker", values="close").sort_index()
spy = closes["SPY"]
logr = np.log(closes).diff().iloc[1:]
ex = logr.sub(np.log(spy).diff().iloc[1:], axis=0).drop(columns=["SPY"])
ex = ex.loc[:, ex.notna().mean() > 0.8]
T = 120
test, form = ex.iloc[-T:], ex.iloc[-2 * T:-T]
t0 = test.index[0]
print(f"sessions {len(ex)}, formation {form.index[0].date()}..{form.index[-1].date()}, test {test.index[0].date()}..{test.index[-1].date()}, names {ex.shape[1]}")

sic = pd.read_csv(f"{D}/sic.csv").dropna()
sic2 = {r.ticker: int(r.sic) // 100 for r in sic.itertuples()}
by_sic: dict = defaultdict(list)
for t, s in sic2.items():
    if t in ex.columns:
        by_sic[s].append(t)

corr = test.corr(min_periods=60)

# --- connection sets: name -> {ticker: [peers]}
sets: dict = {}
liquid = ex.columns
fx = form[liquid].dropna(axis=1, thresh=int(0.8 * len(form)))
sym = defaultdict(list)
for t, p, c, n in top_peers(fx):
    sym[t].append(p)
sets["sympathy (out of sample)"] = sym

tx = pd.read_csv(f"{D}/text.csv")
txt = defaultdict(list)
for r in tx.itertuples():
    txt[r.ticker].append(r.peer)
sets["text peers (10-K)"] = txt

cu = pd.read_csv(f"{D}/cust.csv", parse_dates=["filed"])
cu = cu[cu.filed < t0]
fwd, rev = defaultdict(list), defaultdict(list)
for r in cu.itertuples():
    fwd[r.supplier].append(r.customer_ticker)
    rev[r.customer_ticker].append(r.supplier)
sets["customer links: supplier -> customer"] = fwd
sets["customer links: customer -> supplier"] = rev

ok = set(ex.columns)


def clean(s: dict) -> dict:
    return {a: [b for b in ps if b in ok and b != a] for a, ps in s.items() if a in ok and any(b in ok and b != a for b in ps)}


def baseline_peers(s: dict, k: int = 5) -> dict:
    out = {}
    for a, ps in s.items():
        pool = [x for x in by_sic.get(sic2.get(a), []) if x != a and x not in ps]
        if len(pool) >= k:
            out[a] = list(rng.choice(pool, size=k * 1, replace=False))
    return out


def pair_corr(a: str, b: str) -> float:
    try:
        return float(corr.at[a, b])
    except KeyError:
        return float("nan")


def c1(s: dict, base: dict) -> dict:
    rows = []
    for a, ps in s.items():
        if a not in base:
            continue
        lc = np.nanmean([pair_corr(a, b) for b in ps])
        bc = np.nanmean([pair_corr(a, b) for b in base[a]])
        if np.isfinite(lc) and np.isfinite(bc):
            rows.append((lc, bc))
    if len(rows) < 10:
        return {"n": len(rows)}
    arr = np.array(rows)
    lift = arr[:, 0] - arr[:, 1]
    boots = [lift[rng.integers(0, len(lift), len(lift))].mean() for _ in range(2000)]
    return {"n": len(rows), "linked": arr[:, 0].mean(), "same-industry random": arr[:, 1].mean(), "lift": lift.mean(),
            "ci_lo": np.percentile(boots, 2.5), "ci_hi": np.percentile(boots, 97.5)}


def clustered_ols(y, X, g):
    XtX_inv = np.linalg.inv(X.T @ X)
    b = XtX_inv @ X.T @ y
    e = y - X @ b
    meat = np.zeros((X.shape[1],) * 2)
    for gi in np.unique(g):
        m = g == gi
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    V = XtX_inv @ meat @ XtX_inv
    return b, np.sqrt(np.diag(V))


def c2(s: dict) -> dict:
    ys, xo, xp, gs = [], [], [], []
    vals = test.values
    cols = {c: i for i, c in enumerate(test.columns)}
    for a, ps in s.items():
        ia = cols[a]
        ip = [cols[b] for b in ps if b in cols]
        if not ip:
            continue
        own = vals[:-1, ia]
        peer = np.nanmean(vals[:-1][:, ip], axis=1)
        nxt = vals[1:, ia]
        good = np.isfinite(own) & np.isfinite(peer) & np.isfinite(nxt)
        ys.append(nxt[good]); xo.append(own[good]); xp.append(peer[good]); gs.append(np.arange(len(own))[good])
    y, o, p, g = map(np.concatenate, (ys, xo, xp, gs))
    X = np.column_stack([np.ones_like(y), o, p])
    b, se = clustered_ols(y, X, g)
    return {"obs": len(y), "peer_coef": b[2], "t": b[2] / se[2], "bps_per_1pct": b[2] * 100}


print()
res = []
for name, s in sets.items():
    s = clean(s)
    base = baseline_peers(s)
    r1 = c1(s, base)
    sb = {a: base[a] for a in base}
    r2, r2b = c2(s), c2(sb)
    res.append((name, len(s), r1, r2, r2b))
    print(f"== {name}: {len(s)} source tickers")
    print("  C1", {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r1.items()})
    print("  C2 linked   ", {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r2.items()})
    print("  C2 same-SIC ", {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r2b.items()})


def c3(s: dict, h: int) -> dict:
    vals = test.values
    cols = {c: i for i, c in enumerate(test.columns)}
    ys, xo, xp, gs = [], [], [], []
    for a, ps in s.items():
        ia = cols[a]
        ip = [cols[b] for b in ps if b in cols]
        if not ip:
            continue
        n = len(vals) - h
        own = vals[:n, ia]
        peer = np.nanmean(vals[:n][:, ip], axis=1)
        fut = np.array([np.nansum(vals[i + 1:i + 1 + h, ia]) if np.isfinite(vals[i + 1:i + 1 + h, ia]).all() else np.nan for i in range(n)])
        good = np.isfinite(own) & np.isfinite(peer) & np.isfinite(fut)
        ys.append(fut[good]); xo.append(own[good]); xp.append(peer[good]); gs.append(np.arange(n)[good])
    y, o, p, g = map(np.concatenate, (ys, xo, xp, gs))
    cut = np.quantile(np.abs(p), 0.98)
    m = np.abs(p) >= cut
    X = np.column_stack([np.ones(m.sum()), o[m], p[m]])
    b, se = clustered_ols(y[m], X, g[m])
    return {"obs": int(m.sum()), "cut_pct": round(cut * 100, 2), "peer_coef": round(b[2], 3), "t": round(b[2] / se[2], 2)}


print("\n--- C3 largest peer days")
for name, s in sets.items():
    s = clean(s)
    print(f"{name}: h=1 {c3(s, 1)}  h=5 {c3(s, 5)}")


# --- C4: after a company-specific shock (news that day, own move >= 4% net of SPY, peers still quiet), do linked peers drift?
news = pd.read_csv(f"{D}/news.csv", parse_dates=["day"])
news = news.assign(pos=ex.index.searchsorted(news["day"].values))
news = news[news["pos"] < len(ex.index)]
shock_news = set(zip(news["pos"].tolist(), news["ticker"].tolist()))
ALL = {c: i for i, c in enumerate(ex.columns)}
VALS = ex.values


def c4(s: dict, base: dict, start: int) -> dict:
    ev = []
    for a, ps in s.items():
        if a not in base or a not in ALL:
            continue
        ia, ip, ib = ALL[a], [ALL[b] for b in ps if b in ALL], [ALL[b] for b in base[a] if b in ALL]
        if not ip or not ib:
            continue
        for d in range(start, len(ex.index) - 5):
            if (d, a) not in shock_news:
                continue
            own = VALS[d, ia]
            quiet = np.nanmean(VALS[d, ip]) if np.isfinite(VALS[d, ip]).any() else np.nan
            if not (np.isfinite(own) and abs(own) >= 0.04 and np.isfinite(quiet) and abs(quiet) < 0.01):
                continue
            sg = 1.0 if own > 0 else -1.0
            row = [d]
            for h in (1, 5):
                for grp in (ip, ib):
                    blk = VALS[d + 1:d + 1 + h][:, grp]
                    row.append(sg * float(np.nanmean(np.nansum(blk, axis=0))))
            ev.append(row)
    if len(ev) < 30:
        return {"events": len(ev)}
    arr = np.array(ev)
    days = arr[:, 0].astype(int)
    out = {"events": len(arr), "days": len(set(days))}
    udays = np.unique(days)
    for h, (cl, cb) in {1: (1, 2), 5: (3, 4)}.items():
        diff = arr[:, cl] - arr[:, cb]
        S = {d: diff[days == d].sum() for d in udays}
        N = {d: int((days == d).sum()) for d in udays}
        boots = []
        for _ in range(2000):
            pick = rng.choice(udays, size=len(udays), replace=True)
            boots.append(sum(S[d] for d in pick) / sum(N[d] for d in pick))
        sd = float(np.std(boots))
        out[f"h{h}"] = {"linked_bps": round(arr[:, cl].mean() * 1e4, 1), "random_bps": round(arr[:, cb].mean() * 1e4, 1),
                        "diff_bps": round(diff.mean() * 1e4, 1), "t": round(diff.mean() / sd, 2) if sd > 0 else None}
    return out


print("\n--- C4 shock-conditioned drift (signed by the shock's direction)")
for name, s in sets.items():
    s = clean(s)
    base = baseline_peers(s)
    start = len(ex.index) - T if name.startswith("sympathy") else 1
    print(f"{name}: {c4(s, base, start)}")


# --- Customer-link precision (P1 to P3): needs cust_full.csv (supplier, customer_name, customer_ticker, pct, filed, method)
from scipy.stats import spearmanr  # noqa: E402

WRONG_NAMES = {
    "Abbott Laboratories, Boston Scientific", "Emerging Growth Businesses", "Emerging Markets", "Freedom", "Intelligent Edge",
    "Northern California Region", "Southern California Region", "Southern Minnesota Energy Cooperative", "On", "Outdoor", "People",
    "Senior Notes", "Target, Walmart", "Target, Walmart, Amazon", "Target, Walmart, Ulta Beauty", "Walmart, Target",
    "Evernorth Health Services", "Gravity", "Fox",
}
cf = pd.read_csv(f"{D}/cust_full.csv", parse_dates=["filed"])
cf = cf[cf["customer_ticker"].notna()].copy()
n_sessions = pd.read_csv(f"{D}/closes.csv", usecols=["ticker"])["ticker"].value_counts()
cf["sessions"] = cf["customer_ticker"].map(n_sessions).fillna(0).astype(int)
cf["wrong"] = cf["customer_name"].isin(WRONG_NAMES) | cf["customer_name"].str.startswith("TJX Companies,")
cf["unusable"] = (~cf["wrong"]) & (cf["sessions"] < 200)
cf["clean"] = ~cf["wrong"] & ~cf["unusable"]
print("\n--- P1 precision of the", len(cf), "links with a ticker")
print({"clean": int(cf.clean.sum()), "wrong": int(cf.wrong.sum()), "unusable": int(cf.unusable.sum())})
print("clean with a stated share of 10% or more:", int((cf.clean & (cf.pct >= 10)).sum()))

sub = cf[cf.clean & (cf.pct >= 10) & (cf.filed < t0)]
fwd2, rev2 = defaultdict(list), defaultdict(list)
for r in sub.itertuples():
    fwd2[r.supplier].append(r.customer_ticker)
    rev2[r.customer_ticker].append(r.supplier)
print("\n--- P2 clean subset")
for name, s in {"clean supplier -> customer": fwd2, "clean customer -> supplier": rev2}.items():
    s = clean(s)
    base = baseline_peers(s)
    r1 = c1(s, base)
    print(name, len(s), "sources")
    print("  C1", {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r1.items()})
    print("  C2", {k: (round(v, 3) if isinstance(v, float) else v) for k, v in c2(s).items()})
    print("  C4", c4(s, base, 1))

rows = []
for r in sub.itertuples():
    if r.supplier in ok and r.customer_ticker in ok:
        bl = [pair_corr(r.supplier, x) for x in rng.choice([x for x in by_sic.get(sic2.get(r.supplier), []) if x not in (r.supplier, r.customer_ticker)] or [r.supplier], size=5)]
        c = pair_corr(r.supplier, r.customer_ticker)
        if np.isfinite(c) and np.isfinite(np.nanmean(bl)):
            rows.append((r.pct, c - float(np.nanmean(bl))))
if len(rows) > 10:
    rho, p = spearmanr([a for a, _ in rows], [b for _, b in rows])
    print("\n--- P3 dose and response: links", len(rows), "spearman(share, excess corr) =", round(float(rho), 3), "p =", round(float(p), 3))
