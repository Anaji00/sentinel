"""Why each study instrument without a price series has none. Reads /tmp/nobars.json (from investigate_barless.py) and
Alpaca's active and inactive asset lists. Read-only; run inside sentinel-tradfi (keys never printed)."""

import collections
import json
import os
import re
import urllib.request

KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
HEAD = {"APCA-API-KEY-ID": KEY or "", "APCA-API-SECRET-KEY": SECRET or ""}
LISTED = {"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "NYSEARCA"}


def get(url: str) -> object:
    with urllib.request.urlopen(urllib.request.Request(url, headers=HEAD), timeout=90) as r:
        return json.loads(r.read())


nobars = json.load(open("/tmp/nobars.json"))
active = {a["symbol"].upper(): a for a in get("https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity")}
inactive = {a["symbol"].upper(): a for a in get("https://paper-api.alpaca.markets/v2/assets?status=inactive&asset_class=us_equity")}
print("active", len(active), "| inactive", len(inactive))
untradeable = [s for s in nobars if s not in active]
print("no-series names", len(nobars), "| not in the active list", len(untradeable))

variants: dict = collections.defaultdict(list)
for sym in active:
    base = re.split(r"[.\-]", sym)[0]
    if base != sym:
        variants[base].append(sym)

classes: dict = collections.defaultdict(list)
for s in untradeable:
    if not re.fullmatch(r"[A-Z]{1,5}", s):
        if s in variants:
            classes["alias of a dotted share class"].append(s)
        elif re.search(r"[?()*\s]|^[0-9]", s):
            classes["mangled by the parser"].append(s)
        else:
            classes["other punctuation or digits"].append(s)
    elif s in variants:
        classes["alias of a dotted share class"].append(s)
    elif s in inactive and inactive[s].get("exchange") in LISTED:
        classes["delisted, was on an exchange"].append(s)
    elif s in inactive:
        classes["delisted, was OTC"].append(s)
    elif len(s) == 5 and s[-1] in "FY":
        classes["foreign OTC line (F/Y)"].append(s)
    elif len(s) == 5 and s[-1] == "X":
        classes["mutual fund (X)"].append(s)
    elif len(s) == 4:
        classes["plain 4-letter, not on any Alpaca list"].append(s)
    else:
        classes["plain, not on any Alpaca list"].append(s)

for k, v in sorted(classes.items(), key=lambda kv: -len(kv[1])):
    eps = sum(nobars[s][0] for s in v)
    print(f"{len(v):5d} names {eps:6d} episodes  {k}   e.g. {v[:8]}")

# Can the free feed serve history for the delisted exchange names?
delisted = classes["delisted, was on an exchange"]
served = set()
for i in range(0, len(delisted), 100):
    chunk = delisted[i:i + 100]
    url = ("https://data.alpaca.markets/v2/stocks/bars?symbols=" + ",".join(chunk)
           + "&timeframe=1Day&start=2016-01-01&limit=10000&feed=sip&adjustment=all")
    try:
        served |= {k.upper() for k, v in (get(url).get("bars") or {}).items() if v}
    except Exception as exc:
        print("bars chunk failed:", str(exc)[:100])
print("delisted exchange names:", len(delisted), "| Alpaca returns daily history for:", len(served))
al = classes["alias of a dotted share class"]
print("alias examples ->", {s: variants[re.split(r"[.\-]", s)[0]] if s in variants else variants.get(s) for s in al[:10]})
