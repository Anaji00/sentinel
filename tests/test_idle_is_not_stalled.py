"""Three small corrections from the 14 Nov sweep: an idle consumer with nothing unread is not STALLED; a feed host that does not resolve costs one log line, not a
traceback; and the news job waits for a full batch (or 30 minutes) instead of paying a request for one headline."""
import pathlib

from shared.utils.heartbeat import stall_seconds
from shared.utils.llm_news import BATCH, MAX_WAIT_SECONDS, ready_to_send

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_idle_with_nothing_unread_is_not_a_stall():
    assert stall_seconds(2368.0, 0) == 0.0


def test_idle_with_unread_messages_is_a_stall():
    assert stall_seconds(300.0, 12) == 300.0


def test_an_unmeasured_lag_keeps_the_old_reading():
    assert stall_seconds(300.0, None) == 300.0
    assert stall_seconds(300.0, "n/a") == 300.0
    assert stall_seconds(300.0, True) == 300.0


def test_a_hung_handler_is_a_stall_even_at_lag_zero():
    # F6 (review 19 Nov): the message of a hung handler was already read, so lag is 0 and the idle reading was hidden.
    assert stall_seconds(2400.0, 0, 2400.0) == 2400.0
    assert stall_seconds(2400.0, 0, 30.0) == 0.0            # a handler within the limit is just working
    assert stall_seconds(300.0, 12, 0.0) == 300.0


def test_heartbeat_uses_the_lag_before_it_logs_a_stall():
    src = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert "stall_seconds(idle_seconds, lag_report.get(\"lag\"), oldest_dispatch)" in src
    assert "self._dispatch_started.pop(token, None)" in src
    assert src.index("stall_seconds(idle_seconds") < src.index("stall_note = f\" STALLED")


def test_news_feed_network_errors_log_one_line():
    src = (ROOT / "services" / "collector-news" / "main.py").read_text(encoding="utf-8")
    i = src.index("except (aiohttp.ClientError, OSError)")
    assert "exc_info" not in src[i:src.index("except Exception as e:", i)]
    assert "upstream_error(feed_name)" in src[i:src.index("except Exception as e:", i)]


def test_a_partial_batch_waits_and_then_goes():
    assert not ready_to_send(0, 99999)
    assert not ready_to_send(1, 60)
    assert not ready_to_send(BATCH - 1, MAX_WAIT_SECONDS - 1)
    assert ready_to_send(BATCH, 0)
    assert ready_to_send(1, MAX_WAIT_SECONDS)


def test_the_job_applies_the_admission_rule():
    src = (ROOT / "services" / "agents" / "llm_news_job.py").read_text(encoding="utf-8")
    assert "ln.ready_to_send(len(rows), oldest_age)" in src


def test_only_a_dropped_connection_is_retried_soon():
    # Review L4 (18 Nov): a timeout means the host is saturated; the heavy study must not be re-run four times in an hour.
    from shared.utils.retry_sleep import is_connection_error
    assert is_connection_error(ConnectionResetError()) and is_connection_error(OSError("refused"))
    assert not is_connection_error(TimeoutError()) and not is_connection_error(ValueError("x"))
    assert is_connection_error(type("PostgresConnectionError", (Exception,), {})())


def test_a_zero_move_is_an_observation_and_a_poison_pill_is_counted():
    # Review L7 and L2.
    src = (ROOT / "shared" / "utils" / "grades_v2.py").read_text(encoding="utf-8")
    assert "if v is not None and c is not None:" in src
    base = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    assert "except (ValueError, AttributeError) as e:" in base and "type(e).__name__" in base
