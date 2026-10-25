"""Free data for the short side and earnings: FINRA daily short volume, IB's
borrow file, the standardized earnings surprise from SEC facts, and the model
features built on them."""

import pathlib
import sys
from datetime import date, datetime, timedelta, timezone

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import factor_model as fm  # noqa: E402
from shared.utils import short_data as sd  # noqa: E402
from shared.utils.fundamentals_pit import Fact, series, sue  # noqa: E402

FINRA = """Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
20261008|A|464495.367250|2|1505110.859693|B,Q,N
20261008|AAAA|212|0|418.412300|Q
20261008|BRK.B|300000|0|900000|B,Q,N
20261008|ZZZ-W|90000|0|200000|Q
"""

IBKR = """#BOF|2026.10.09|08:53:50
#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|FIGI|
049323AB4|USD|CB ATLAS FINL HLDGS|557991763|XXXX|3.6300|0.2500|2800000|BBG017QTG7P1|
AAPL|USD|APPLE INC|265598|XXXX|3.5260|0.3540|>10000000|BBG000B9XRY4|
BYND|USD|BEYOND MEAT INC|910444967|XXXX|-21.3388|25.2188|100000|BBG003CVJP50|
BRK B|USD|BERKSHIRE HATHAWAY INC-CL B|72063691|XXXX|3.5|0.3|>10000000|BBG000DWG505|
SAP|EUR|SAP SE|123|XXXX|1.0|0.5|1000|BBG|
#EOF|5
"""


def test_finra_rows_keep_liquid_common_tickers() -> None:
    rows = sd.parse_finra(FINRA)
    assert [r[1] for r in rows] == ["A", "BRK.B"]          # thin AAAA and the warrant dropped
    day, t, short, exempt, total = rows[0]
    assert day == date(2026, 10, 8) and abs(short / total - 0.3086) < 1e-3 and exempt == 2.0


def test_borrow_file_rows_fee_and_capped_availability() -> None:
    taken, rows = sd.parse_ibkr(IBKR)
    # IB stamps the file in New York time: 08:53:50 EDT is 12:53:50 UTC.
    assert taken == datetime(2026, 10, 9, 12, 53, 50, tzinfo=timezone.utc)
    by = {r[0]: r for r in rows}
    assert set(by) == {"AAPL", "BYND", "BRK.B"}             # CUSIP-coded bond and the EUR line dropped
    assert by["BYND"][1] == 25.2188 and by["BYND"][1] > sd.HARD_TO_BORROW_FEE
    assert by["AAPL"][3] == sd.IBKR_AVAILABLE_CAP and by["BYND"][3] == 100000


def _quarters(values, start=date(2021, 1, 1)):
    """Consecutive calendar quarters with the given values: {(start, end): v}."""
    out, s = {}, start
    for v in values:
        e = s + timedelta(days=89)
        out[(s, e)] = v
        s = e + timedelta(days=1)
    return out


def test_sue_is_the_seasonal_change_over_its_own_history() -> None:
    # Seasonal changes run 1, 2, 1, 2, 1 then jump to 6.
    vals = [10, 10, 10, 10, 11, 12, 11, 12, 12, 18]
    u = sue(_quarters(vals))
    hist = [1.0, 2.0, 1.0, 2.0, 1.0]
    m = sum(hist) / 5
    sdv = (sum((h - m) ** 2 for h in hist) / 4) ** 0.5
    assert abs(u - 6.0 / sdv) < 1e-9


def test_sue_needs_a_year_ago_quarter_and_history() -> None:
    assert sue(_quarters([10, 10, 10, 10, 11, 12, 11])) is None      # three changes
    assert sue({}) is None


def test_sue_is_known_only_from_the_filing_that_completes_it() -> None:
    q = _quarters([10, 10, 10, 10, 11, 12, 11, 12, 12, 18])
    facts = [Fact("NetIncomeLoss", s, e, v, e + timedelta(days=40), f"a{k}") for k, ((s, e), v) in enumerate(q.items())]
    pts = series(facts)["sue"]
    first_filed = sorted(q)[8][1] + timedelta(days=40)                 # the ninth quarter's filing: five changes
    assert pts[0][0] == first_filed and pts[-1][1] > 3


def test_short_volume_share_features_follow_the_finra_arrays() -> None:
    T, N = 300, 3
    rng = np.random.default_rng(1)
    close = np.cumprod(1 + rng.normal(0, 0.01, (T, N)), axis=0).astype(np.float32) * 50
    dates = [date(2025, 1, 1) + timedelta(days=k) for k in range(T)]
    panel = fm.Panel(dates, ["A", "B", "SPY"], close, close, np.full((T, N), 1e6, np.float32), {})
    total = np.full((T, N), 1e6, np.float32)
    share = np.array([0.6, 0.4, 0.5], np.float32)
    shortv = total * share
    shortv[-20:, 0] = total[-20:, 0] * 0.8                            # A's shorting rises lately
    panel.extra.update({"short_volume": shortv, "offex_volume": total})
    f = fm.features(panel)
    assert abs(f["svr_20"][-1, 1] - 0.4) < 1e-6
    assert abs(f["svr_20"][-1, 0] - 0.8) < 1e-6 and f["svr_chg"][-1, 0] > 0.1
    assert np.isnan(f["sue"]).all()                                    # no facts given
    # Trial G (9 Oct 2026): the short-volume pair adopted, the surprise kept as context.
    assert {"svr_20", "svr_chg"} <= set(fm.FEATURES) and {"svr_20", "svr_chg"} <= fm.OPTIONAL_FEATURES
    assert "sue" in fm.CONTEXT_FEATURES and "sue" not in fm.FEATURES
