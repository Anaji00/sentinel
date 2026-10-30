"""The researcher's screen: candidate grammar, rank ICs, the incremental IC, honest q, and the bandit."""

import pathlib
import random
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import researcher as rs  # noqa: E402


def test_only_known_features_and_operators_get_through() -> None:
    allowed = ["ins_npr_182", "ev_8k_dilution"]
    assert rs.valid({"op": "product", "a": "ins_npr_182", "b": "ev_8k_dilution"}, allowed) == (
        "product", "ins_npr_182", "ev_8k_dilution")
    assert rs.valid({"op": "exec", "a": "ins_npr_182", "b": "ev_8k_dilution"}, allowed) is None
    assert rs.valid({"op": "diff", "a": "ins_npr_182", "b": "ins_npr_182"}, allowed) is None
    assert rs.valid({"op": "gate", "a": "made_up", "b": "ev_8k_dilution"}, allowed) is None
    assert rs.key("raw", "x") == "x" and rs.key("gate", "a", "b") == "gate(a,b)"


def test_ranks_are_centred_and_keep_gaps() -> None:
    r = rs.cs_rank(np.array([[3.0, np.nan, 1.0, 2.0]]))
    assert np.allclose(r[0, [0, 2, 3]], [0.5, -0.5, 0.0]) and np.isnan(r[0, 1])
    tied = rs.cs_rank(np.array([[0.0, 0.0, 0.0, 1.0]]))
    assert np.allclose(tied[0], [-1 / 6, -1 / 6, -1 / 6, 0.5])     # ties share a rank, whatever the order
    g = rs.build("gate", np.array([[1.0, 2.0, 3.0]]), np.array([[1.0, 0.0, np.nan]]))
    assert g[0, 1] == 0.0 and np.isnan(g[0, 2])


def test_a_signal_that_is_only_momentum_adds_nothing_once_momentum_is_out() -> None:
    rng = np.random.default_rng(0)
    months, names = 60, 200
    mom = rng.normal(size=(months, names))
    fwd = 0.3 * mom + rng.normal(size=(months, names))
    copy = mom + 0.01 * rng.normal(size=(months, names))
    own = rng.normal(size=(months, names))
    fwd2 = fwd + 0.3 * own
    raw = rs.summarize(rs.ic_series(copy, fwd), rs.ic_series(copy, fwd, [mom]))
    assert raw["t"] > 5 and abs(raw["inc_t"]) < 3
    new = rs.summarize(rs.ic_series(own, fwd2), rs.ic_series(own, fwd2, [mom]))
    assert new["inc_t"] > 5 and new["inc_months"] == months


def test_every_test_ever_run_counts_against_the_survivors() -> None:
    def board() -> dict:
        return {"x": {"inc_t": 3.2, "inc_months": 100}, "y": {"inc_t": 0.4, "inc_months": 100},
                "z": {"inc_t": 5.0, "inc_months": 5}}
    b = board()
    rs.judge(b, tests_run=2)
    assert b["x"]["verdict"] == "queue for a trial" and b["y"]["verdict"] == "nothing new"
    assert b["z"]["verdict"] == "too few months"
    b2 = board()
    rs.judge(b2, tests_run=200)                      # the same t after 200 tries is not news
    assert b2["x"]["verdict"] == "promising" and b2["x"]["q"] > rs.QUEUE_Q


def test_the_bandit_follows_what_has_paid() -> None:
    rng = random.Random(1)
    record = {"insider": (9, 1), "filings": (0, 10)}
    picks = [rs.pick_family(record, ["filings", "insider"], rng) for _ in range(200)]
    assert picks.count("insider") > 180
    assert rs.nw_t([0.01, 0.02, 0.015, 0.012, 0.018]) > 3 and rs.nw_t([0.1]) == 0.0
    system, user = rs.prompt("insider", {"ins_npr_182": "Net insider purchase ratio"}, [])
    assert "insider family" in system and "ins_npr_182" in user
