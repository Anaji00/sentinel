import pytest
from fastapi.testclient import TestClient
from services.api_gateway.routes.main import app

def test_api_gateway_metrics_endpoints_bypass_auth():
    client = TestClient(app)
    
    # GET /metrics should return 200 without X-API-KEY
    res_metrics = client.get("/metrics")
    assert res_metrics.status_code == 200
    
    # GET /metrics/json should return 200 without X-API-KEY
    res_json = client.get("/metrics/json")
    assert res_json.status_code == 200
    assert isinstance(res_json.json(), dict)
    
    # GET /api/v1/health/metrics should return 200 without X-API-KEY
    res_health_metrics = client.get("/api/v1/health/metrics")
    assert res_health_metrics.status_code == 200

def test_api_gateway_protected_endpoint_requires_auth():
    client = TestClient(app)
    
    # GET /api/v1/events/recent without X-API-KEY should be 403
    res = client.get("/api/v1/events/recent")
    assert res.status_code == 403
