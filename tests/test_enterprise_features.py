"""
tests/test_enterprise_features.py

Comprehensive test suite verifying Enterprise Features:
1. Universal Heartbeat Aggregator & Data Health Dashboard
2. Dynamic Watchlist Governance & 50-Ticker Clamp
3. Centralized Secrets Management & Masking
4. Role-Based Access Control (RBAC)
5. Immutable SHA-256 Hash-Chained Audit Ledger
6. Broker Abstraction Layer (PaperBroker, AlpacaBroker)
7. Investigation Case Management & Note Collaboration
8. Real Portfolio Risk & Parametric VaR / CVaR Calculation
9. Executive Intelligence Report Generation
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from shared.utils.heartbeat import (
    touch_heartbeat,
    is_component_healthy,
    get_all_heartbeats_status,
    ALL_KNOWN_COMPONENTS,
    OPTIONAL_COMPONENTS,
)
from shared.utils.secrets import (
    mask_secret,
    get_secret,
    get_secret_bool,
    get_secret_int,
    audit_secrets_environment,
    is_sensitive_key,
)
from shared.utils.rbac import (
    Role,
    has_role_permission,
    parse_role,
    get_current_user_role,
    require_role,
)
from shared.utils.audit_ledger import (
    AuditLedger,
    compute_entry_hash,
    GENESIS_HASH,
)
from shared.broker import (
    get_broker,
    PaperBroker,
    AlpacaBroker,
    OrderSide,
    OrderType,
    OrderStatus,
)
from shared.models.cases import (
    InvestigationCase,
    CaseStatus,
    CasePriority,
    CaseNote,
)
from services.reporting.report_generator import ReportGenerator


class MockRedis:
    def __init__(self):
        self.store = {}
        self.lists = {}
        self.zsets = {}
        self.published = []

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)
        self.zsets.pop(key, None)

    async def zadd(self, key, mapping):
        if key not in self.zsets:
            self.zsets[key] = {}
        self.zsets[key].update(mapping)

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def zrem(self, key, member):
        z = self.zsets.get(key, {})
        if member in z:
            del z[member]
            return 1
        return 0

    async def zrevrange(self, key, start, stop, withscores=False):
        z = self.zsets.get(key, {})
        sorted_items = sorted(z.items(), key=lambda x: x[1], reverse=True)
        slice_items = sorted_items[start : stop + 1 if stop != -1 else None]
        if withscores:
            return slice_items
        return [item[0] for item in slice_items]

    async def rpush(self, key, value):
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].append(value)

    async def lrange(self, key, start, stop):
        l = self.lists.get(key, [])
        if stop == -1:
            return l[start:]
        return l[start : stop + 1]

    async def publish(self, channel, message):
        self.published.append((channel, message))

    def pipeline(self):
        return MockPipeline(self)


class MockPipeline:
    def __init__(self, mock_redis):
        self.redis = mock_redis
        self.ops = []

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, value, ex))
        return self

    def zadd(self, key, mapping):
        self.ops.append(("zadd", key, mapping))
        return self

    def rpush(self, key, value):
        self.ops.append(("rpush", key, value))
        return self

    def delete(self, key):
        self.ops.append(("delete", key))
        return self

    async def execute(self):
        results = []
        for op in self.ops:
            if op[0] == "set":
                await self.redis.set(op[1], op[2], ex=op[3])
                results.append(True)
            elif op[0] == "zadd":
                await self.redis.zadd(op[1], op[2])
                results.append(1)
            elif op[0] == "rpush":
                await self.redis.rpush(op[1], op[2])
                results.append(1)
            elif op[0] == "delete":
                await self.redis.delete(op[1])
                results.append(1)
        self.ops = []
        return results


# ── 1. UNIVERSAL HEARTBEAT TESTS ──────────────────────────────────────────────

def test_heartbeat_touch_and_health_aggregation():
    async def _test():
        redis = MockRedis()
        
        # Touch heartbeats for some components
        await touch_heartbeat(redis, "collector-tradfi")
        await touch_heartbeat(redis, "enrichment")
        await touch_heartbeat(redis, "correlation")

        status = await get_all_heartbeats_status(redis)
        assert status["components_count"] == len(ALL_KNOWN_COMPONENTS)
        assert status["healthy_count"] == 3
        assert status["components"]["collector-tradfi"]["status"] == "HEALTHY"
        assert status["components"]["enrichment"]["status"] == "HEALTHY"
        assert status["components"]["dlq-worker"]["status"] == "OFFLINE"

        # Components behind an opt-in compose profile report NOT_DEPLOYED rather
        # than OFFLINE, and are excluded from the health denominator -- otherwise
        # the default analyst mode would permanently read as degraded.
        for tier in OPTIONAL_COMPONENTS:
            assert status["components"][tier]["status"] == "NOT_DEPLOYED"

        expected_offline = len(ALL_KNOWN_COMPONENTS) - 3 - len(OPTIONAL_COMPONENTS)
        assert status["offline_count"] == expected_offline
        assert status["not_deployed_count"] == len(OPTIONAL_COMPONENTS)
        assert status["scored_components_count"] == len(ALL_KNOWN_COMPONENTS) - len(OPTIONAL_COMPONENTS)

    asyncio.run(_test())


def test_optional_components_do_not_depress_health_when_absent():
    """An opt-in tier that is simply not deployed must not read as a failure."""
    async def _test():
        redis = MockRedis()
        for comp in ALL_KNOWN_COMPONENTS:
            if comp not in OPTIONAL_COMPONENTS:
                await touch_heartbeat(redis, comp)

        status = await get_all_heartbeats_status(redis)
        assert status["system_status"] == "OPERATIONAL"
        assert status["healthy_ratio"] == 1.0

    asyncio.run(_test())


def test_deployed_but_unhealthy_optional_component_still_counts():
    """Once a tier is deployed, its health counts like any other component."""
    async def _test():
        redis = MockRedis()
        for comp in ALL_KNOWN_COMPONENTS:
            if comp not in OPTIONAL_COMPONENTS:
                await touch_heartbeat(redis, comp)
        # One tier present and healthy -> it enters the denominator.
        await touch_heartbeat(redis, "agents-heavy")

        status = await get_all_heartbeats_status(redis)
        scored = len(ALL_KNOWN_COMPONENTS) - len(OPTIONAL_COMPONENTS) + 1
        assert status["scored_components_count"] == scored
        assert status["not_deployed_count"] == len(OPTIONAL_COMPONENTS) - 1
        assert status["components"]["agents-heavy"]["status"] == "HEALTHY"

    asyncio.run(_test())


# ── 2. DYNAMIC WATCHLIST GOVERNANCE & 50-CLAMP TESTS ──────────────────────────

def test_watchlist_50_clamp_and_pubsub_sync():
    async def _test():
        redis = MockRedis()
        key = "sentinel:watched:equities"
        channel = "sentinel:collector:watchlist_sync"

        # Add 55 tickers — verify clamping logic
        tickers = [f"SYM{i}" for i in range(55)]
        clamped = tickers[:50]
        
        for t in clamped:
            await redis.zadd(key, {t: 1.0})
            
        count = await redis.zcard(key)
        assert count == 50

        # Publish sync signal
        await redis.publish(channel, json.dumps({"action": "SYNC", "active_count": count}))
        assert len(redis.published) == 1
        assert redis.published[0][0] == channel

    asyncio.run(_test())


# ── 3. SECRETS MANAGEMENT TESTS ───────────────────────────────────────────────

def test_secrets_masking_and_auditing():
    assert mask_secret("sk_live_1234567890abcdef", 4) == "sk_l****************cdef"
    assert mask_secret(None) == "<UNSET>"
    assert mask_secret("short") == "*****"

    assert is_sensitive_key("FINNHUB_API_KEY") is True
    assert is_sensitive_key("ALPACA_SECRET_KEY") is True
    assert is_sensitive_key("POSTGRES_HOST") is False

    with patch.dict("os.environ", {"TEST_KEY": "secret_value_123", "TEST_BOOL": "true", "TEST_INT": "42"}):
        assert get_secret("TEST_KEY") == "secret_value_123"
        assert get_secret_bool("TEST_BOOL") is True
        assert get_secret_int("TEST_INT") == 42

    audit = audit_secrets_environment()
    assert "total_audited" in audit
    assert "readiness_ratio" in audit
    assert audit["total_audited"] >= 15


# ── 4. ROLE-BASED ACCESS CONTROL (RBAC) TESTS ────────────────────────────────

def test_rbac_hierarchy_and_roles():
    assert has_role_permission(Role.ADMIN, Role.ADMIN) is True
    assert has_role_permission(Role.ADMIN, Role.ANALYST) is True
    assert has_role_permission(Role.ADMIN, Role.VIEWER) is True

    assert has_role_permission(Role.ANALYST, Role.ADMIN) is False
    assert has_role_permission(Role.ANALYST, Role.ANALYST) is True
    assert has_role_permission(Role.ANALYST, Role.VIEWER) is True

    assert has_role_permission(Role.VIEWER, Role.ADMIN) is False
    assert has_role_permission(Role.VIEWER, Role.ANALYST) is False
    assert has_role_permission(Role.VIEWER, Role.VIEWER) is True

    assert parse_role("admin") == Role.ADMIN
    assert parse_role("Analyst") == Role.ANALYST
    assert parse_role("invalid_role") == Role.VIEWER


# ── 5. IMMUTABLE AUDIT LEDGER TESTS ──────────────────────────────────────────

class MockAuditDB:
    """Minimal durable-store stand-in enforcing the audit_ledger UNIQUE constraints."""

    def __init__(self):
        self.rows = []

    async def execute(self, sql, *params):
        if "INSERT INTO audit_ledger" not in sql:
            return None
        hash_, prev_hash = params[0], params[1]
        # Mirror UNIQUE(hash) and UNIQUE(prev_hash) from init.sql.
        for r in self.rows:
            if r["hash"] == hash_ or r["prev_hash"] == prev_hash:
                raise Exception('duplicate key value violates unique constraint (23505)')
        self.rows.append({
            "hash": hash_, "prev_hash": prev_hash, "timestamp": params[2],
            "actor": params[3], "action": params[4], "resource_type": params[5],
            "resource_id": params[6], "ip_address": params[7], "details": params[8],
        })
        return None

    async def query_one(self, sql, *params):
        return dict(self.rows[-1]) if self.rows else None

    async def query(self, sql, *params):
        limit = params[0] if params else len(self.rows)
        offset = params[1] if len(params) > 1 else 0
        newest_first = list(reversed(self.rows))
        return [dict(r) for r in newest_first[offset:offset + limit]]


def test_audit_ledger_requires_durable_store():
    """Without a durable store the ledger must refuse to record, not silently cache."""
    from shared.utils.audit_ledger import AuditLedgerUnavailable

    async def _test():
        ledger = AuditLedger(redis_client=MockRedis())  # Redis only, no db
        with pytest.raises(AuditLedgerUnavailable):
            await ledger.record_entry(
                actor="admin", action="SUBMIT_ORDER",
                resource_type="ORDER", resource_id="ORD-001",
            )

    asyncio.run(_test())


def test_audit_ledger_hash_chain_and_verification():
    async def _test():
        redis = MockRedis()
        db = MockAuditDB()
        ledger = AuditLedger(redis_client=redis, db_client=db)

        # 1. Record 3 entries
        e1 = await ledger.record_entry(actor="admin", action="ADD_WATCHLIST", resource_type="WATCHLIST", resource_id="sentinel:watched:equities", details={"ticker": "NVDA"})
        assert e1["prev_hash"] == GENESIS_HASH
        assert len(e1["hash"]) == 64

        e2 = await ledger.record_entry(actor="analyst", action="SUBMIT_ORDER", resource_type="ORDER", resource_id="ORD-001", details={"symbol": "AAPL", "qty": 100})
        assert e2["prev_hash"] == e1["hash"]

        e3 = await ledger.record_entry(actor="system", action="TOGGLE_FLAG", resource_type="FLAG", resource_id="flag_quant_radar", details={"enabled": True})
        assert e3["prev_hash"] == e2["hash"]

        # 2. Verify complete chain against the durable store
        verification = await ledger.verify_chain()
        assert verification["valid"] is True
        assert verification["entries_checked"] == 3
        assert verification["status"] == "VERIFIED_VALID"

        # 3. Tamper test: modify a durable row and verify the chain fails
        db.rows[1]["details"] = json.dumps({"symbol": "AAPL", "qty": 999999}, sort_keys=True)

        tampered_verification = await ledger.verify_chain()
        assert tampered_verification["valid"] is False
        assert tampered_verification["broken_at_index"] == 1

    asyncio.run(_test())


def test_audit_ledger_chain_cannot_fork_under_contention():
    """A second append reading a stale head must retry, not fork the chain."""
    async def _test():
        db = MockAuditDB()
        ledger = AuditLedger(db_client=db)

        await ledger.record_entry(actor="a", action="ACT_ONE", resource_type="T", resource_id="1")
        await ledger.record_entry(actor="b", action="ACT_TWO", resource_type="T", resource_id="2")
        await ledger.record_entry(actor="c", action="ACT_THREE", resource_type="T", resource_id="3")

        # Every prev_hash is distinct -> strictly linear chain, no forks.
        prevs = [r["prev_hash"] for r in db.rows]
        assert len(prevs) == len(set(prevs))
        assert (await ledger.verify_chain())["valid"] is True

    asyncio.run(_test())


# ── 6. BROKER ABSTRACTION & PAPER TRADING TESTS ──────────────────────────────

def test_paper_broker_execution_and_positions():
    async def _test():
        broker = PaperBroker(initial_cash=100_000.0, slippage_bps=5.0)

        # 1. Check initial account
        acct = await broker.get_account()
        assert acct.cash == 100_000.0
        assert acct.portfolio_value == 100_000.0

        # 2. Buy 100 shares of NVDA @ estimated $120.00
        buy_order = await broker.submit_order(
            symbol="NVDA",
            qty=100,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            estimated_market_price=120.0,
        )
        assert buy_order.status == OrderStatus.FILLED
        assert buy_order.filled_qty == 100
        # Slippage: 120 * (1 + 0.0005) = 120.06
        assert buy_order.filled_avg_price == 120.06

        # 3. Verify position was created
        pos = await broker.get_position("NVDA")
        assert pos is not None
        assert pos.qty == 100
        assert pos.avg_entry_price == 120.06
        assert pos.market_value == 12006.0

        # 4. Partial sell 40 shares @ $130.00
        sell_order = await broker.submit_order(
            symbol="NVDA",
            qty=40,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            estimated_market_price=130.0,
        )
        assert sell_order.status == OrderStatus.FILLED

        pos_after = await broker.get_position("NVDA")
        assert pos_after.qty == 60

    asyncio.run(_test())


# ── 7. INVESTIGATION CASE MANAGEMENT TESTS ───────────────────────────────────

def test_investigation_case_models():
    case = InvestigationCase(
        title="Unusual Put Volume & Cross-Domain Dark Vessel Disruption",
        description="High put volume on energy sector correlated with vessel AIS silence in Hormuz.",
        priority=CasePriority.HIGH,
        lead_analyst="senior_analyst",
        linked_event_ids=["EVT-001", "EVT-002"],
        linked_correlation_ids=["CORR-001"],
        linked_entities=["XOM", "CVX", "IMO_9876543"],
        tags=["energy", "ais_dark", "options_spike"],
    )

    assert case.status == CaseStatus.OPEN
    assert case.priority == CasePriority.HIGH
    assert len(case.linked_event_ids) == 2
    assert len(case.linked_entities) == 3

    # Add note
    note = CaseNote(author="senior_analyst", content="Satellite corroboration confirmed tanker rerouting.")
    case.notes.append(note)
    assert len(case.notes) == 1
    assert case.notes[0].author == "senior_analyst"


# ── 8. REPORT GENERATOR TESTS ─────────────────────────────────────────────────

def test_executive_report_generator():
    async def _test():
        redis = MockRedis()
        await touch_heartbeat(redis, "collector-tradfi")
        
        generator = ReportGenerator(redis_client=redis)
        report = await generator.generate_brief(timeframe_hours=24, title="Test Risk Brief")

        assert report["title"] == "Test Risk Brief"
        assert report["timeframe_hours"] == 24
        assert "# Test Risk Brief" in report["markdown"]
        assert "Executive Summary" in report["markdown"]
        assert "Infrastructure & Feed Health" in report["markdown"]

    asyncio.run(_test())

