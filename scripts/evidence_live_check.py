"""Print the status of each evidence-bundle section for some tickers (run inside sentinel-api_gateway)."""
import json
import sys
import urllib.request

from services.api_gateway.dependencies import API_KEY as key

for t in sys.argv[1:]:
    req = urllib.request.Request(f"http://127.0.0.1:8000/api/v1/stocks/{t}/evidence", headers={"X-API-KEY": key})
    d = json.loads(urllib.request.urlopen(req, timeout=60).read())
    print(t, {k: v["status"] for k, v in d["sections"].items()})
    for name in ("regime", "producer_grade", "model", "positioning"):
        s = d["sections"][name]
        print("  ", name, json.dumps(s["data"] if s["status"] == "ok" else s["note"])[:260])
