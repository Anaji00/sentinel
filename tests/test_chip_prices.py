"""Memory and GPU prices as a page reads them."""
from datetime import date

from shared.utils.chip_prices import build_chip_prices

D = date


def _rows():
    return [
        {"series": "dram:DDR5 16Gb", "day": D(2026, 10, 22), "value": 50.0, "unit": "USD", "source": "dramexchange"},
        {"series": "dram:DDR5 16Gb", "day": D(2026, 11, 5), "value": 60.0, "unit": "USD", "source": "dramexchange"},
        {"series": "nand:256Gb TLC", "day": D(2026, 11, 5), "value": 18.0, "unit": "USD", "source": "dramexchange"},
        {"series": "gpu:H100 SXM:secure", "day": D(2026, 11, 5), "value": 3.99, "unit": "USD per GPU-hour", "source": "runpod"},
        {"series": "gpu:H100 SXM:lowest", "day": D(2026, 11, 5), "value": 2.69, "unit": "USD per GPU-hour", "source": "runpod"},
        {"series": "ZC=F", "day": D(2026, 11, 5), "value": 4.1, "unit": None, "source": "yahoo"},
    ]


def test_sections_hold_only_memory_and_gpu_series_in_a_fixed_order():
    out = build_chip_prices(_rows(), D(2026, 11, 5))
    assert [s["key"] for s in out["sections"]] == ["dram", "nand", "gpu"]
    assert all("ZC=F" not in i["name"] for s in out["sections"] for i in s["items"])


def test_change_is_measured_from_the_first_stored_day_and_absent_for_one_day():
    out = build_chip_prices(_rows(), D(2026, 11, 5))
    dram = out["sections"][0]["items"][0]
    assert dram["change_pct"] == 20.0 and dram["since"] == D(2026, 10, 22)
    nand = out["sections"][1]["items"][0]
    assert nand["change_pct"] is None, "one day of prices has no change"


def test_gpu_rows_carry_the_price_tier_in_words():
    gpu = build_chip_prices(_rows(), D(2026, 11, 5))["sections"][2]["items"]
    assert [i["variant"] for i in gpu] == ["lowest price", "secure cloud"]
    assert {i["name"] for i in gpu} == {"H100 SXM"}


def test_each_section_says_which_stocks_it_bears_on():
    out = build_chip_prices(_rows(), D(2026, 11, 5))
    dram = out["sections"][0]
    assert "MU" in dram["gain_when_up"] and "DELL" in dram["pay_when_up"]
    assert "NVDA" in out["sections"][2]["gain_when_up"]


def test_a_short_record_is_called_too_short_and_old_data_is_flagged_stale():
    out = build_chip_prices(_rows(), D(2026, 11, 5))
    assert out["record_days"] == 2 and "too few to call a trend" in out["note"]
    assert out["stale"] is False
    assert build_chip_prices(_rows(), D(2026, 11, 20))["stale"] is True


def test_nothing_stored_gives_an_empty_answer_not_an_error():
    out = build_chip_prices([], D(2026, 11, 5))
    assert out["sections"] == [] and out["as_of"] is None and out["stale"] is False
