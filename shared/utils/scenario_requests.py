"""
shared/utils/scenario_requests.py

Scenarios on request: a person names a development ("a ceasefire in Ukraine",
"OPEC cuts output") and the reasoning service writes the hypotheses -- what
could follow, how likely each is, what would confirm or deny it, who gains.

The gateway validates the request and queues it; the reasoning service turns it
into a correlation row (persisted first, so the scenario's foreign key holds),
writes the scenario, and records the result for the gateway to return.

Pure: validation and the cluster's fields. Keys shared by both sides.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator

REQUESTS_KEY = "sentinel:scenarios:requests"          # list, newest pushed left
RESULT_KEY = "sentinel:scenarios:request:{request_id}"  # status JSON, kept RESULT_TTL_SEC
RESULT_TTL_SEC = 7 * 86400
MAX_PENDING = 50
RULE_ID = "USER_SCENARIO_REQUEST"
RULE_NAME = "Scenario Request"
_TICKER = re.compile(r"^[A-Z][A-Z0-9.=\-]{0,9}$")


class ScenarioRequest(BaseModel):
    topic: str = Field(min_length=3, max_length=300)
    tickers: List[str] = Field(default_factory=list, max_length=10)

    @field_validator("topic")
    @classmethod
    def _topic(cls, v: str) -> str:
        v = " ".join(v.split())
        if len(v) < 3:
            raise ValueError("topic is empty")
        return v

    @field_validator("tickers")
    @classmethod
    def _tickers(cls, v: List[str]) -> List[str]:
        out = []
        for t in v:
            s = str(t).strip().upper()
            if not _TICKER.match(s):
                raise ValueError(f"not a symbol: {t!r}")
            if s not in out:
                out.append(s)
        return out


def queued(req: ScenarioRequest, requested_by: str) -> Dict[str, Any]:
    """The queue record for a validated request."""
    return {"request_id": str(uuid.uuid4()), "topic": req.topic, "tickers": req.tickers,
            "requested_by": requested_by[:80], "requested_at": datetime.now(timezone.utc).isoformat()}


def cluster_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    """CorrelationCluster fields for a request: INTELLIGENCE tier so it reads as
    deliberate, the topic as the headline, the tickers as its entities."""
    tickers = [str(t).upper() for t in rec.get("tickers") or []]
    return {
        "rule_id": RULE_ID,
        "rule_name": RULE_NAME,
        "primary_domain": "geopolitical",
        "confidence_score": 0.5,
        "summary_headline": str(rec["topic"]),
        "supporting_headlines": [],
        "metrics_summary": {"requested_by": rec.get("requested_by"), "request_id": rec.get("request_id")},
        "trigger_event_id": str(uuid.uuid4()),
        "supporting_event_ids": [],
        "entity_ids": tickers,
        "entity_names": tickers,
        "description": f"Scenario requested: {rec['topic']}",
        "tags": ["scenario_request"] + [f"exposes:{t}" for t in tickers],
    }


__all__ = ["MAX_PENDING", "REQUESTS_KEY", "RESULT_KEY", "RESULT_TTL_SEC", "RULE_ID", "RULE_NAME", "ScenarioRequest",
           "cluster_fields", "queued"]
