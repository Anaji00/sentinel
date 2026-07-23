import logging
import time
import pytest
from shared.utils.logging import (
    suppress_noisy_loggers,
    ThrottledLogger,
    BatchLogger,
    NOISY_LOGGERS,
)
from shared.kafka import BatchKafkaLogger


def test_suppress_noisy_loggers():
    suppress_noisy_loggers(logging.WARNING)
    for name in NOISY_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.level == logging.WARNING
        assert lg.propagate is False


def test_throttled_logger():
    records = []

    class MockHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("test.throttled")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(MockHandler())

    throttled = ThrottledLogger(logger, default_interval_sec=5.0)

    # First log should be emitted
    throttled.info("key1", "First message")
    assert len(records) == 1
    assert records[0].getMessage() == "First message"

    # Immediate second log with same key should be suppressed
    throttled.info("key1", "Second message")
    assert len(records) == 1

    # Log with different key should be emitted
    throttled.info("key2", "Different key message")
    assert len(records) == 2


def test_batch_logger():
    records = []

    class MockHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("test.batch")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(MockHandler())

    batch = BatchLogger(logger, "test-batch", flush_interval_sec=10.0, max_items=5)

    for i in range(4):
        batch.add("topic_a", 1)
    assert len(records) == 0

    # 5th item reaches max_items threshold and triggers flush
    batch.add("topic_a", 1)
    assert len(records) == 1
    assert "BATCH SUMMARY [test-batch]" in records[0].getMessage()
    assert "topic_a=5" in records[0].getMessage()


def test_batch_kafka_logger():
    records = []

    class MockHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    klogger = BatchKafkaLogger("test_service", flush_interval_sec=0.05)
    klogger.logger.setLevel(logging.INFO)
    klogger.logger.handlers.clear()
    klogger.logger.addHandler(MockHandler())

    klogger.log_produced("events.raw.maritime", 10)
    klogger.log_consumed("events.raw.adsb", 5)
    klogger.log_error("events.raw.maritime", "TimeoutError")

    time.sleep(0.06)
    klogger._maybe_flush()

    assert len(records) == 1
    msg = records[0].getMessage()
    assert "KAFKA BATCH [test_service]" in msg
    assert "events.raw.maritime: 10" in msg
    assert "events.raw.adsb: 5" in msg
