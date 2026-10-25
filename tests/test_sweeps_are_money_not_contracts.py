"""A sweep is premium spent; 100 contracts at $0.05 is $500, not a sweep."""

import pathlib

SRC = (pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")


def test_the_sweep_filter_is_premium_only() -> None:
    # Money spent, never contract count; since 16 Oct 2026 the money paid above
    # intrinsic value where the spot is known (shared/utils/options_flow.py).
    from shared.utils import options_flow as ofl
    assert "if (extrinsic if extrinsic is not None else premium) >= ofl.MIN_EXTRINSIC_USD:" in SRC
    assert ofl.MIN_EXTRINSIC_USD == 50000.0
    assert "or size >= 100.0" not in SRC
