"""How many no-series instruments are a symbol-cleaning problem rather than missing history. Run inside sentinel-tradfi."""

import json
import os
import re
import urllib.request

HEAD = {"APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "",
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY") or ""}
nobars = json.load(open("/tmp/nobars.json"))
req = urllib.request.Request("https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity", headers=HEAD)
active = {a["symbol"].upper() for a in json.load(urllib.request.urlopen(req, timeout=90))}

rec, rec_eps, ex = {}, 0, []
for s, (n, fam) in nobars.items():
    if s in active or re.fullmatch(r"[A-Z]{1,5}", s):
        continue
    c = re.sub(r"[^A-Z0-9.]", "", s.upper().replace("NYSE:", "").replace("NASDAQ:", ""))
    if c and c != s and c in active:
        rec[s] = c
        rec_eps += n
        ex.append((s, c, n))
print("symbols that clean to an active ticker:", len(rec), "| episodes", rec_eps)
print(sorted(ex, key=lambda x: -x[2])[:25])
junk = [s for s in nobars if s not in active and not re.fullmatch(r"[A-Z]{1,5}", s) and s not in rec]
print("punctuated/odd symbols that do not clean to anything:", len(junk), "| sample:", junk[:40])
quoted = [s for s in nobars if s.startswith('"')]
print("triple-quoted:", len(quoted), "| of which clean to active:", len([s for s in quoted if s in rec]))
fams = {}
for s in junk:
    fams[nobars[s][1]] = fams.get(nobars[s][1], 0) + nobars[s][0]
print("junk symbols' episodes by family:", sorted(fams.items(), key=lambda kv: -kv[1])[:6])
