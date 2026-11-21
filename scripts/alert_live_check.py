"""Exercise the price-alert routes against the running gateway (run inside sentinel-api_gateway); leaves nothing behind."""
import json
import time
import urllib.error
import urllib.request

from services.api_gateway.dependencies import API_KEY as key


def call(method, path, body=None):
    req = urllib.request.Request("http://127.0.0.1:8000" + path, method=method,
                                 headers={"X-API-KEY": key, "Content-Type": "application/json"},
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200].decode()


made = []
st, a = call("POST", "/api/v1/alerts", {"ticker": "nvda", "direction": "above", "level": 100, "note": "test above"})
print(st, a)
made.append(a["id"])
st, b = call("POST", "/api/v1/alerts", {"ticker": "NVDA", "direction": "above", "level": 100000})
print(st, b)
made.append(b["id"])
print(call("POST", "/api/v1/alerts", {"ticker": "NVDA", "direction": "sideways", "level": 5}))
print(call("POST", "/api/v1/alerts", {"ticker": "NVDA", "direction": "below", "level": -5}))
time.sleep(5)
print("triggered:", call("GET", "/api/v1/alerts/triggered"))
st, lst = call("GET", "/api/v1/alerts")
print(st, [(x["id"], x["level"], x["status"], x["triggered_price"]) for x in lst["alerts"]])
for i in made:
    print("ack", i, call("POST", f"/api/v1/alerts/{i}/ack")[0], "delete", call("DELETE", f"/api/v1/alerts/{i}")[0])
print("after:", call("GET", "/api/v1/alerts"))
