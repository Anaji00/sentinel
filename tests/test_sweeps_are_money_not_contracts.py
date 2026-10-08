"""A sweep is premium spent; 100 contracts at $0.05 is $500, not a sweep."""

import pathlib

SRC = (pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")


def test_the_sweep_filter_is_premium_only() -> None:
    assert "if premium >= 50000.0:" in SRC
    assert "or size >= 100.0" not in SRC
