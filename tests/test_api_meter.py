"""The API meter: which host, which service, and never anything from the URL but the host."""
from collections import Counter
from unittest.mock import MagicMock

import pytest

from shared.utils import api_meter as am


def test_only_the_host_is_read_from_a_url():
    assert am.host_of("https://finnhub.io/api/v1/quote?symbol=X&token=SECRET") == "finnhub.io"
    assert am.host_of("not a url") == ""


@pytest.mark.parametrize("host, external", [
    ("paper-api.alpaca.markets", True), ("data.sec.gov", True), ("redis", False), ("api_gateway", False),
    ("localhost", False), ("host.docker.internal", False), ("172.18.0.9", False), ("10.0.0.1", False),
    ("192.168.1.2", False), ("172.32.0.1", True), ("", False),
])
def test_the_platform_talking_to_itself_is_not_an_api_call(host, external):
    assert am.is_external(host) is external


def test_a_rate_limit_is_counted_apart_from_other_refusals():
    assert [am.status_class(s) for s in (200, 301, 404, 429, 503, None)] == ["2xx", "3xx", "4xx", "429", "5xx", "error"]


def test_service_names_come_from_the_script_path():
    assert am.service_name("services/collector-news/main.py") == "collector-news"
    assert am.service_name("/app/services/agents/factor_engine.py") == "agents/factor_engine"
    assert am.service_name("/tmp/of_run.py") == "of_run"


def test_counts_survive_a_failed_flush(monkeypatch):
    monkeypatch.setattr(am, "_counts", Counter({"s|h|2xx": 3}))
    client = MagicMock()
    client.pipeline.return_value.execute.side_effect = ConnectionError("down")
    with pytest.raises(ConnectionError):
        am._flush(client)
    assert am._counts["s|h|2xx"] == 3
    client.pipeline.return_value.execute.side_effect = None
    am._flush(client)
    assert not am._counts
    client.pipeline.return_value.hincrby.assert_any_call(am.DAY_KEY.format(day=am.datetime.now(am.timezone.utc).date().isoformat()), "s|h|2xx", 3)


def test_a_route_keeps_words_and_masks_anything_that_could_be_a_secret_or_a_ticker():
    assert am.route_of("https://data.alpaca.markets/v2/stocks/AAPL/bars?feed=sip&token=S") == "/v2/stocks/:x/bars"
    assert am.route_of("https://data.alpaca.markets/v1beta1/options/snapshots/SPY") == "/v1beta1/options/snapshots/:x"
    assert am.route_of("https://api.telegram.org/bot123456:ABCdef/sendMessage") == "/:x/:x"
    assert am.route_of("https://finnhub.io/api/v1/quote?symbol=X&token=SECRET") == "/api/v1/quote"
    assert am.route_of("https://x.io/a1b2c3d4e5/hook") == "/:x/hook"
