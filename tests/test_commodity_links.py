"""Commodity, memory and GPU prices, and which stocks each bears on."""

import math
import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import commodity_links as cl  # noqa: E402

PAGE = """
<tr><td>Item</td><td>Daily High</td><td>Daily Low</td><td>Session High</td><td>Session Low</td><td>Session Average</td></tr>
<tr><td class="x">DDR5 16Gb (2Gx8) 4800/5600</td><td>71.00</td><td>47.00</td><td>71.00</td><td>47.00</td><td>61.100</td><td>0.38 %</td></tr>
<tr><td>DDR5 RDIMM 32GB 4800/5600</td><td>2420.00</td><td>2130.00</td><td>2420.00</td><td>2130.00</td><td>2,185.000</td><td>0.41 %</td></tr>
<tr><td>512Gb TLC</td><td>21.00</td><td>16.80</td><td>21.00</td><td>16.80</td><td>18.833</td><td>0.00 %</td></tr>
<tr><td>GDDR6  8Gb</td><td>18.00</td><td>10.20</td><td>18.00</td><td>10.20</td><td>12.502</td><td>0.42 %</td></tr>
<tr><td>Samsung</td><td>PCIe 4.0 X4</td><td>990 Pro</td><td>1 TB</td><td>259.99</td><td>239.99</td><td>249.99</td></tr>
"""


def test_memory_spot_rows_parse_into_groups() -> None:
    got = cl.parse_dramexchange(PAGE)
    assert got == {
        "dram:DDR5 16Gb (2Gx8) 4800/5600": 61.1,
        "dram:DDR5 RDIMM 32GB 4800/5600": 2185.0,
        "nand:512Gb TLC": 18.833,
        "gddr:GDDR6 8Gb": 12.502,
    }                                                   # the retail SSD row is not a spot price


def test_gpu_rates_parse() -> None:
    body = {"data": {"gpuTypes": [
        {"displayName": "H100 SXM", "securePrice": 3.99, "lowestPrice": {"uninterruptablePrice": 2.69}},
        {"displayName": "MI300X", "securePrice": 2.39, "lowestPrice": {"uninterruptablePrice": None}},
        {"displayName": "Some Old Card", "securePrice": 0.1, "lowestPrice": {"uninterruptablePrice": 0.05}},
    ]}}
    assert cl.parse_runpod(body) == {"gpu:H100 SXM:secure": 3.99, "gpu:H100 SXM:lowest": 2.69,
                                     "gpu:MI300X:secure": 2.39}


def test_links_run_both_ways() -> None:
    assert ("dram", 1) in cl.links_for("mu") and ("nand", 1) in cl.links_for("MU")
    assert ("oil", -1) in cl.links_for("DAL")
    assert cl.group_of("CL=F") == "oil" and cl.group_of("gpu:H100 SXM:secure") == "gpu"
    assert cl.group_of("nope:x") is None
    assert "NVDA" in cl.linked_tickers()


def test_exposure_is_the_signed_move_of_what_a_stock_makes_or_buys() -> None:
    days = [date(2026, 1, 1) + timedelta(days=k) for k in range(30)]
    oil = [(d, 100.0 * (1.01 ** k)) for k, d in enumerate(days)]          # +1% a day
    closes = {"CL=F": oil, "BZ=F": oil}
    groups = {"oil": ["CL=F", "BZ=F"]}
    up = cl.exposure("XOM", days[-1], groups, closes, 21)
    down = cl.exposure("DAL", days[-1], groups, closes, 21)
    assert abs(up - 21 * math.log(1.01)) < 1e-9 and abs(down + 21 * math.log(1.01)) < 1e-9
    assert cl.exposure("AAPL", days[-1], groups, closes, 21) is None       # nothing linked
    assert cl.exposure("XOM", days[5], groups, closes, 21) is None         # not enough history


def test_the_collector_reads_yahoo_and_the_spot_tables_date() -> None:
    sys.path.insert(0, str(ROOT / "services" / "collector-tradfi"))
    import commodities as cm

    body = {"chart": {"result": [{"meta": {"currency": "USD"}, "timestamp": [1792699200, 1792785600],
                                  "indicators": {"quote": [{"close": [99.2, None]}]}}]}}
    rows = cm.yahoo_rows("CL=F", body)
    assert rows == [("CL=F", date(2026, 10, 22), 99.2, "USD", "yahoo")]      # 20:00 UTC = 16:00 New York
    assert cm.yahoo_rows("CL=F", {"chart": {"result": None}}) == []
    page = "<b>Last Update: Oct.22 2026    18:10 (GMT+8)</b>"
    assert cm.dram_day(page, date(2026, 10, 23)) == date(2026, 10, 22)
    assert cm.dram_day("nothing", date(2026, 10, 23)) == date(2026, 10, 23)
