"""Print the heat map compactly (run inside sentinel-api_gateway)."""
import json
import sys
import urllib.request

from services.api_gateway.dependencies import API_KEY as key

by = sys.argv[1] if len(sys.argv) > 1 else "sector"
req = urllib.request.Request(f"http://127.0.0.1:8000/api/v1/market/heatmap?by={by}", headers={"X-API-KEY": key})
d = json.loads(urllib.request.urlopen(req, timeout=120).read())
print({k: d[k] for k in ("as_of", "session", "by", "universe")}, "groups", len(d["groups"]))
for g in d["groups"][: 14 if by == "sector" else 8]:
    print(f'{g["label"][:34]:34} n={g["n"]:4} priced={g["n_priced"]:4} cap={g["market_cap"]/1e9:8.0f}bn chg={g["change_pct"]} med={g["change_pct_median"]} up={g["breadth_up_share"]} vol={g["volume_vs_20d_median"]} top={[t["ticker"] for t in g["top"]]}')
