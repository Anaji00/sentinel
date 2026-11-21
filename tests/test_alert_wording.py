"""Layer audit finding 1 (wording only): the peer-cointegration family says direction is not predicted."""
from datetime import datetime, timezone

from shared.models import AlertTier, CorrelationCluster
from shared.utils.alert_wording import NOTE, direction_note


def test_the_note_applies_to_the_peer_family_only() -> None:
    assert direction_note("Quantitative Equity Peer Cointegration & Decoupling") == NOTE
    assert direction_note("Stock Moving Against Its Peers on News") is None
    assert direction_note(None) is None


def _cluster(rule: str) -> CorrelationCluster:
    return CorrelationCluster(rule_id="r", rule_name=rule, description="NVDA decoupled from AMD", alert_tier=AlertTier.ALERT, tags=["a"], entity_ids=["NVDA"],
                              detected_at=datetime(2026, 11, 19, tzinfo=timezone.utc), correlation_id="abcdef123456", event_ids=["e1"], trigger_event_id="e1", confidence_score=0.9)


def test_formatters_carry_the_note_and_leave_other_rules_alone() -> None:
    from services.alert_manager.formatters import telegram, webhook
    peer = _cluster("Quantitative Equity Peer Cointegration & Decoupling")
    other = _cluster("Equity Block & Options Convergence")
    assert "Direction is not predicted" in telegram.format_correlation(peer).replace("\\", "")
    assert "Direction is not predicted" not in telegram.format_correlation(other)
    assert "Direction is not predicted" in webhook.format_correlation_slack(peer)["attachments"][0]["text"]
    assert webhook.format_correlation_slack(other)["attachments"][0]["text"] == "NVDA decoupled from AMD"
